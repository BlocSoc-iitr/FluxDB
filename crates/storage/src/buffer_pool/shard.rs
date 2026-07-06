//! # Buffer Pool Shard
//!
//! A shard manages a fixed-size subset of the buffer pool's frames. Sharding
//! reduces lock contention by allowing multiple threads to access different
//! partitions of the buffer pool simultaneously.

use crate::buffer_pool::replacer::ClockReplacer;
use crate::disk::DiskManager;
use crate::page::Lsn;
use crate::wal::Wal;
use common::BufferPoolError;
use common::{INVALID_FRAME_ID, MAX_PAGE_SIZE};
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

type Result<T> = std::result::Result<T, BufferPoolError>;

/// A fixed-size buffer for a single database page.
pub struct PageData(pub Box<[u8; MAX_PAGE_SIZE]>);

impl Default for PageData {
    fn default() -> Self {
        Self::new()
    }
}

impl PageData {
    /// Creates a new, zeroed `PageData`.
    pub fn new() -> Self {
        Self(
            vec![0u8; MAX_PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .unwrap(),
        )
    }
}

impl Deref for PageData {
    type Target = [u8; MAX_PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl DerefMut for PageData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut()
    }
}

/// An RAII guard for reading a page from the buffer pool.
pub struct PageReadGuard<'a> {
    pub(crate) shard: &'a BufferPoolShard,
    pub(crate) page_id: u64,
    pub(crate) guard: Option<RwLockReadGuard<'a, PageData>>,
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = [u8; MAX_PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("Guard should be present")
            .deref()
    }
}

impl<'a> Drop for PageReadGuard<'a> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            drop(guard);
        }
        self.shard.unpin_page(self.page_id, false);
    }
}

/// An RAII guard for writing to a page in the buffer pool.
pub struct PageWriteGuard<'a> {
    pub(crate) shard: &'a BufferPoolShard,
    pub(crate) page_id: u64,
    pub(crate) guard: Option<RwLockWriteGuard<'a, PageData>>,
    pub(crate) dirty: bool,
    pub(crate) record_lsn: Option<Lsn>,
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = [u8; MAX_PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("Guard should be present")
            .deref()
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        self.guard
            .as_mut()
            .expect("Guard should be present")
            .deref_mut()
    }
}

impl<'a> Drop for PageWriteGuard<'a> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            drop(guard);
        }
        let needs_dirty_unpin = self.dirty && self.record_lsn.is_none();
        self.shard.unpin_page(self.page_id, needs_dirty_unpin);
    }
}

/// Marks the page as dirty and associates it with the WAL record LSN that caused the change.
///
/// This reads the old page LSN before the mutation occurs and passes it to the shard.
/// Returns `true` if this is the first change since the last checkpoint, indicating
/// the caller must log a Full-Page Image (FPI).
impl<'a> PageWriteGuard<'a> {
    pub fn mark_dirty_with_lsn(&mut self, lsn: Lsn) -> bool {
        self.dirty = true;
        self.record_lsn = Some(lsn);

        let old_page_lsn = if let Some(guard) = &self.guard {
            crate::page::page_lsn(&guard[..])
        } else {
            0
        };

        self.shard.mark_dirty(self.page_id, lsn, old_page_lsn)
    }
}

/// Metadata for a single frame in a buffer pool shard.
pub struct FrameMetadata {
    pub page_id: u64,
    pub pin_count: u64,
    pub is_dirty: bool,
    pub loading: bool, // true while a load is in flight; frame not usable yet
    pub rec_lsn: Option<Lsn>, // Added a recovery lsn for each lsn, the lowest rec_lsn among all dirty page is selected for new redo point
}

/// Internal state of a buffer pool shard, protected by a mutex.
pub struct ShardInner {
    pub metadata: Vec<FrameMetadata>,
    pub page_table: FxHashMap<u64, usize>,
    pub free_list: Vec<usize>,
    pub replacer: ClockReplacer,
    pub min_rec_lsn: Option<Lsn>,
}

/// A shard of the buffer pool, managing a subset of the total frames.
pub struct BufferPoolShard {
    pub disk_manager: Arc<DiskManager>,
    pub pages: Vec<RwLock<PageData>>,
    pub inner: Mutex<ShardInner>,
    pub load_done: Condvar, // singalled when any load finishes(success or fail)
    pub wal: Arc<Wal>,
    pub last_checkpoint_redo_point: AtomicU64, // it contains the last checkpoint redo point
}

impl BufferPoolShard {
    /// Creates a new `BufferPoolShard` with the specified number of frames.
    pub fn new(disk_manager: Arc<DiskManager>, size: usize, wal: Arc<Wal>) -> Self {
        let mut metadata = Vec::with_capacity(size);
        let mut free_list = Vec::with_capacity(size);
        for frame_id in 0..size {
            metadata.push(FrameMetadata {
                page_id: INVALID_FRAME_ID,
                pin_count: 0,
                is_dirty: false,
                loading: false,
                rec_lsn: None,
            });
            free_list.push(size - 1 - frame_id);
        }

        Self {
            disk_manager,
            pages: (0..size).map(|_| RwLock::new(PageData::new())).collect(),
            inner: Mutex::new(ShardInner {
                metadata,
                page_table: FxHashMap::with_capacity_and_hasher(size, FxBuildHasher),
                free_list,
                replacer: ClockReplacer::new(size),
                min_rec_lsn: None,
            }),
            load_done: Condvar::new(),
            wal,
            last_checkpoint_redo_point: AtomicU64::new(0), //lsn 0 is null pageLsn
        }
    }

    /// Decrements the pin count of a page and marks it as dirty if requested.
    pub fn unpin_page(&self, page_id: u64, is_dirty: bool) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(&local_id) = inner.page_table.get(&page_id) {
            let meta = &mut inner.metadata[local_id];
            if meta.pin_count > 0 {
                meta.pin_count -= 1;
                meta.is_dirty |= is_dirty;
                if meta.pin_count == 0 {
                    inner.replacer.unpin(local_id);
                }
            }
        }
    }

    pub fn find_victim_frame_id(&self, inner: &mut ShardInner) -> Result<usize> {
        if let Some(id) = inner.free_list.pop() {
            Ok(id)
        } else {
            inner.replacer.victim()
        }
    }

    /// Acquire a frame for `page_id`.
    ///
    /// Returns `(frame_id, needs_load)`:
    /// - `false`: the page is resident and fully loaded; the frame is pinned and
    ///   ready to use.
    /// - `true`: the frame is reserved and published in a **loading** state; the
    ///   caller MUST load `page_id` into it and then call [`finish_load`]. Other
    ///   threads wanting this page block until then, so a page is never
    ///   observable before it has been loaded and verified.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if no frames can be evicted.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn acquire_frame(&self, page_id: u64) -> Result<(usize, bool)> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if let Some(&frame_id) = inner.page_table.get(&page_id) {
                if inner.metadata[frame_id].loading {
                    inner = self.load_done.wait(inner).unwrap(); // wait then re-check
                    continue;
                }
                // load cleared path, this is the path when load succeeded
                inner.metadata[frame_id].pin_count += 1;
                inner.replacer.pin(frame_id);
                return Ok((frame_id, false));
            }
            let frame_id = self.find_victim_frame_id(&mut inner)?;
            let old_page_id = inner.metadata[frame_id].page_id;
            let is_dirty = inner.metadata[frame_id].is_dirty;

            if is_dirty && old_page_id != INVALID_FRAME_ID {
                drop(inner);
                // Data-file durability is no longer this path's job: the checkpoint's
                // single batched fsync covers it, so eviction skips sync_data here.
                self.flush_page_without_sync(old_page_id)?;
                inner = self.inner.lock().unwrap();
                continue; // need to re-check the table now as someone might have loaded this frame while we were flushing
            }

            if old_page_id != INVALID_FRAME_ID {
                inner.page_table.remove(&old_page_id);
            }

            let meta = &mut inner.metadata[frame_id];
            meta.page_id = page_id;
            meta.pin_count = 1;
            meta.is_dirty = false;
            meta.loading = true;

            inner.page_table.insert(page_id, frame_id);
            inner.replacer.pin(frame_id);
            return Ok((frame_id, true));
        }
    }

    pub fn finish_load(&self, page_id: u64, frame_id: usize, success: bool) {
        {
            let mut inner = self.inner.lock().unwrap();
            if success {
                inner.metadata[frame_id].loading = false;
            } else {
                inner.page_table.remove(&page_id);
                let meta = &mut inner.metadata[frame_id];
                meta.page_id = INVALID_FRAME_ID;
                meta.pin_count = 0;
                meta.is_dirty = false;
                meta.loading = false;
                inner.replacer.pin(frame_id);
                inner.free_list.push(frame_id);
            }
        }
        self.load_done.notify_all();
    }
    /// Flushes a specific page to disk if it is dirty.
    ///
    /// This public shard path is used by explicit single-page flushes, so it
    /// syncs the data file before returning. Batch callers use
    /// [`Self::flush_page_without_sync`] and issue one shared sync after all
    /// page writes complete.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn flush_page(&self, page_id: u64) -> Result<bool> {
        self.flush_page_inner(page_id, true)
    }

    /// Writes a dirty page without syncing the data file.
    ///
    /// WAL-before-page is still enforced inside `write_frame_to_disk`; only the
    /// data-file sync is deferred so `flush_all_pages` can batch it.
    pub fn flush_page_without_sync(&self, page_id: u64) -> Result<bool> {
        self.flush_page_inner(page_id, false)
    }

    fn flush_page_inner(&self, page_id: u64, sync_data: bool) -> Result<bool> {
        let (pid, frame_id) = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&id) = inner.page_table.get(&page_id) {
                let meta = &mut inner.metadata[id];
                if meta.is_dirty && meta.page_id != INVALID_FRAME_ID {
                    meta.is_dirty = false;
                    meta.pin_count += 1;
                    (meta.page_id, id)
                } else {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        };

        let res = self.write_frame_to_disk(frame_id, pid).and_then(|()| {
            if sync_data {
                self.disk_manager.sync_data()?;
            }
            Ok(())
        });

        let mut inner = self.inner.lock().unwrap();
        let meta = &mut inner.metadata[frame_id];
        meta.pin_count -= 1;
        if meta.pin_count == 0 {
            inner.replacer.unpin(frame_id);
        }
        if let Err(e) = res {
            inner.metadata[frame_id].is_dirty = true;
            return Err(e);
        } else {
            let old_rec_lsn = inner.metadata[frame_id].rec_lsn; //saving before clearing it
            //When WRITE is succeded it is safe to clear the lsn
            inner.metadata[frame_id].rec_lsn = None;

            //when page with min_rec_lsn itself is flushed
            if old_rec_lsn == inner.min_rec_lsn {
                inner.min_rec_lsn = inner
                    .metadata
                    .iter()
                    .filter(|m| m.is_dirty)
                    .filter_map(|m| m.rec_lsn)
                    .min()
            }
        }

        Ok(true)
    }

    /// Writes every dirty frame in this shard to disk without syncing.
    pub fn flush_all_pages_no_sync(&self) -> Result<()> {
        let n_frames = self.pages.len();
        for frame_id in 0..n_frames {
            let (pid, is_dirty) = {
                let inner = self.inner.lock().unwrap();
                let meta = &inner.metadata[frame_id];
                (meta.page_id, meta.is_dirty)
            };
            if is_dirty && pid != INVALID_FRAME_ID {
                self.flush_page_without_sync(pid)?;
            }
        }
        Ok(())
    }

    pub fn write_frame_to_disk(&self, frame_id: usize, page_id: u64) -> Result<()> {
        let mut buf = vec![0u8; MAX_PAGE_SIZE];
        let page_lsn;
        {
            // Snapshot the page LSN and the page bytes under the SAME read guard
            // so the LSN we flush the WAL to matches exactly the bytes we write.
            // A concurrent writer cannot interleave between the two reads
            let data = self.pages[frame_id].read().unwrap();
            page_lsn = crate::page::page_lsn(&data[..]);
            buf.copy_from_slice(&data[..]);
        }

        // WAL-before-page: a page image carrying `page_lsn` must not reach disk
        // until the WAL is durable through `page_lsn`. No-op when already durable.
        // (`?` converts WalError → BufferPoolError via `#[from]`.)
        self.wal.flush_up_to(page_lsn)?;

        // Stamp the CRC32 on the outgoing copy so corruption is detectable on the next load.
        crate::page::stamp_checksum(&mut buf);

        // Only write bytes here. The caller decides whether to sync immediately
        // (`flush_page`) or after a group of writes (`flush_all_pages`).
        self.disk_manager.write_page(page_id, &buf)?;
        Ok(())
    }

    pub fn delete_page(&self, page_id: u64) -> Result<()> {
        loop {
            let mut inner = self.inner.lock().unwrap();
            let frame_id = match inner.page_table.get(&page_id) {
                Some(&id) => id,
                None => return Ok(()),
            };

            if inner.metadata[frame_id].pin_count > 0 {
                return Err(BufferPoolError::PinCountError);
            }

            if inner.metadata[frame_id].is_dirty {
                inner.metadata[frame_id].pin_count += 1;
                let pid = inner.metadata[frame_id].page_id;
                drop(inner);

                let res = self.write_frame_to_disk(frame_id, pid);

                let mut inner = self.inner.lock().unwrap();
                let meta = &mut inner.metadata[frame_id];
                meta.pin_count -= 1;
                if meta.pin_count == 0 {
                    inner.replacer.unpin(frame_id);
                }
                if res.is_err() {
                    inner.metadata[frame_id].is_dirty = true;
                    return Err(BufferPoolError::InternalError(
                        "Flush failed during delete".to_string(),
                    ));
                }
                inner.metadata[frame_id].is_dirty = false;
                drop(inner);
                continue;
            }

            inner.page_table.remove(&page_id);
            let meta = &mut inner.metadata[frame_id];
            meta.page_id = INVALID_FRAME_ID;
            meta.pin_count = 0;
            meta.is_dirty = false;
            inner.replacer.pin(frame_id);
            inner.free_list.push(frame_id);

            return Ok(());
        }
    }

    /// returns 'true' if the FPI is to be attached with WAL record
    /// page_lsn_before <= redo_point ensures that this is the first change in page after the last checkpoint
    pub fn mark_dirty(&self, page_id: u64, lsn: Lsn, page_lsn_before: Lsn) -> bool {
        let redo_point = self.last_checkpoint_redo_point.load(Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();

        if let Some(&frame_id) = inner.page_table.get(&page_id) {
            let meta = &mut inner.metadata[frame_id];
            let was_clean = !meta.is_dirty;
            meta.is_dirty = true;

            //Check if it is the first change after the checkpoint
            if was_clean {
                meta.rec_lsn = Some(lsn);

                //Comparing the recent lsn with min_rec_lsn of the shard and update it
                if inner.min_rec_lsn.is_none() || lsn < inner.min_rec_lsn.unwrap() {
                    inner.min_rec_lsn = Some(lsn);
                }
                return page_lsn_before <= redo_point;
            }
        }
        false
    }
}
