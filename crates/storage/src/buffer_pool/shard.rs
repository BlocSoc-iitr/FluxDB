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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    pub page_id: AtomicU64,
    pub pin_count: AtomicU64,
    pub is_dirty: AtomicBool,
    pub loading: AtomicBool,
    pub rec_lsn: Mutex<Option<Lsn>>,
}

/// A shard of the buffer pool, managing a subset of the total frames.
pub struct BufferPoolShard {
    pub disk_manager: Arc<DiskManager>,
    pub pages: Vec<RwLock<PageData>>,
    pub metadata: Vec<FrameMetadata>,
    pub page_table: RwLock<FxHashMap<u64, usize>>,
    pub free_list: Mutex<Vec<usize>>,
    pub replacer: Mutex<ClockReplacer>,
    pub min_rec_lsn: Mutex<Option<Lsn>>,
    pub load_done: Condvar, 
    pub wait_mutex: Mutex<()>,
    pub wal: Arc<Wal>,
    pub last_checkpoint_redo_point: AtomicU64,
}

impl BufferPoolShard {
    /// Creates a new `BufferPoolShard` with the specified number of frames.
    pub fn new(disk_manager: Arc<DiskManager>, size: usize, wal: Arc<Wal>) -> Self {
        let mut metadata = Vec::with_capacity(size);
        let mut free_list = Vec::with_capacity(size);
        for frame_id in 0..size {
            metadata.push(FrameMetadata {
                page_id: AtomicU64::new(INVALID_FRAME_ID),
                pin_count: AtomicU64::new(0),
                is_dirty: AtomicBool::new(false),
                loading: AtomicBool::new(false),
                rec_lsn: Mutex::new(None),
            });
            free_list.push(size - 1 - frame_id);
        }

        Self {
            disk_manager,
            pages: (0..size).map(|_| RwLock::new(PageData::new())).collect(),
            metadata,
            page_table: RwLock::new(FxHashMap::with_capacity_and_hasher(size, FxBuildHasher)),
            free_list: Mutex::new(free_list),
            replacer: Mutex::new(ClockReplacer::new(size)),
            min_rec_lsn: Mutex::new(None),
            load_done: Condvar::new(),
            wait_mutex: Mutex::new(()),
            wal,
            last_checkpoint_redo_point: AtomicU64::new(0), 
        }
    }

    /// Decrements the pin count of a page and marks it as dirty if requested.
    pub fn unpin_page(&self, page_id: u64, is_dirty: bool) {
        let table = self.page_table.read().unwrap();
        if let Some(&frame_id) = table.get(&page_id) {
            let meta = &self.metadata[frame_id];
            
            // Only unpin if it matches
            if meta.page_id.load(Ordering::Acquire) != page_id {
                return;
            }

            if is_dirty {
                meta.is_dirty.store(true, Ordering::Release);
            }
            
            let prev = meta.pin_count.fetch_sub(1, Ordering::SeqCst);
            if prev == 1 {
                self.replacer.lock().unwrap().unpin(frame_id);
            }
        }
    }

    pub fn find_victim_frame_id(&self) -> Result<usize> {
        if let Some(id) = self.free_list.lock().unwrap().pop() {
            Ok(id)
        } else {
            self.replacer.lock().unwrap().victim()
        }
    }

    pub fn acquire_frame(&self, page_id: u64) -> Result<(usize, bool)> {
        loop {
            // Fast path: read lock
            {
                let table = self.page_table.read().unwrap();
                if let Some(&frame_id) = table.get(&page_id) {
                    let meta = &self.metadata[frame_id];
                    if meta.loading.load(Ordering::Acquire) {
                        drop(table);
                        let guard = self.wait_mutex.lock().unwrap();
                        drop(self.load_done.wait(guard).unwrap());
                        continue;
                    }
                    if meta.page_id.load(Ordering::Acquire) == page_id {
                        let prev = meta.pin_count.fetch_add(1, Ordering::SeqCst);
                        if prev == 0 {
                            self.replacer.lock().unwrap().pin(frame_id);
                        }
                        return Ok((frame_id, false));
                    }
                }
            }
            
            // Slow path: write lock
            let mut table = self.page_table.write().unwrap();
            // Re-check
            if let Some(&frame_id) = table.get(&page_id) {
                let meta = &self.metadata[frame_id];
                if meta.loading.load(Ordering::Acquire) {
                    drop(table);
                    let guard = self.wait_mutex.lock().unwrap();
                    drop(self.load_done.wait(guard).unwrap());
                    continue;
                }
                if meta.page_id.load(Ordering::Acquire) == page_id {
                    let prev = meta.pin_count.fetch_add(1, Ordering::SeqCst);
                    if prev == 0 {
                        self.replacer.lock().unwrap().pin(frame_id);
                    }
                    return Ok((frame_id, false));
                }
            }

            let frame_id = self.find_victim_frame_id()?;
            let meta = &self.metadata[frame_id];
            
            let old_page_id = meta.page_id.load(Ordering::Acquire);
            let is_dirty = meta.is_dirty.load(Ordering::Acquire);

            if is_dirty && old_page_id != INVALID_FRAME_ID {
                drop(table);
                self.flush_page_without_sync(old_page_id)?;
                continue; 
            }

            if old_page_id != INVALID_FRAME_ID {
                table.remove(&old_page_id);
            }

            meta.page_id.store(page_id, Ordering::Release);
            meta.pin_count.store(1, Ordering::SeqCst);
            meta.is_dirty.store(false, Ordering::Release);
            meta.loading.store(true, Ordering::Release);

            table.insert(page_id, frame_id);
            self.replacer.lock().unwrap().pin(frame_id);
            return Ok((frame_id, true));
        }
    }

    pub fn finish_load(&self, page_id: u64, frame_id: usize, success: bool) {
        if success {
            self.metadata[frame_id].loading.store(false, Ordering::Release);
        } else {
            let mut table = self.page_table.write().unwrap();
            table.remove(&page_id);
            let meta = &self.metadata[frame_id];
            meta.page_id.store(INVALID_FRAME_ID, Ordering::Release);
            meta.pin_count.store(0, Ordering::SeqCst);
            meta.is_dirty.store(false, Ordering::Release);
            meta.loading.store(false, Ordering::Release);
            self.replacer.lock().unwrap().pin(frame_id);
            self.free_list.lock().unwrap().push(frame_id);
        }
        self.load_done.notify_all();
    }

    pub fn flush_page(&self, page_id: u64) -> Result<bool> {
        self.flush_page_inner(page_id, true)
    }

    pub fn flush_page_without_sync(&self, page_id: u64) -> Result<bool> {
        self.flush_page_inner(page_id, false)
    }

    fn flush_page_inner(&self, page_id: u64, sync_data: bool) -> Result<bool> {
        let (pid, frame_id) = {
            let table = self.page_table.read().unwrap();
            if let Some(&id) = table.get(&page_id) {
                let meta = &self.metadata[id];
                if meta.is_dirty.load(Ordering::Acquire) && meta.page_id.load(Ordering::Acquire) != INVALID_FRAME_ID {
                    meta.is_dirty.store(false, Ordering::Release);
                    meta.pin_count.fetch_add(1, Ordering::SeqCst);
                    (meta.page_id.load(Ordering::Acquire), id)
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

        let meta = &self.metadata[frame_id];
        let prev = meta.pin_count.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.replacer.lock().unwrap().unpin(frame_id);
        }
        
        if let Err(e) = res {
            meta.is_dirty.store(true, Ordering::Release);
            return Err(e);
        } else {
            let mut rec_lsn_lock = meta.rec_lsn.lock().unwrap();
            let old_rec_lsn = *rec_lsn_lock;
            *rec_lsn_lock = None;
            drop(rec_lsn_lock);

            let mut min_lock = self.min_rec_lsn.lock().unwrap();
            if old_rec_lsn == *min_lock {
                *min_lock = self.metadata
                    .iter()
                    .filter(|m| m.is_dirty.load(Ordering::Acquire))
                    .filter_map(|m| *m.rec_lsn.lock().unwrap())
                    .min();
            }
        }

        Ok(true)
    }

    pub fn flush_all_pages_no_sync(&self) -> Result<()> {
        let n_frames = self.pages.len();
        for frame_id in 0..n_frames {
            let meta = &self.metadata[frame_id];
            let pid = meta.page_id.load(Ordering::Acquire);
            let is_dirty = meta.is_dirty.load(Ordering::Acquire);
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
            let data = self.pages[frame_id].read().unwrap();
            page_lsn = crate::page::page_lsn(&data[..]);
            buf.copy_from_slice(&data[..]);
        }

        self.wal.flush_up_to(page_lsn)?;
        crate::page::stamp_checksum(&mut buf);
        self.disk_manager.write_page(page_id, &buf)?;
        Ok(())
    }

    pub fn delete_page(&self, page_id: u64) -> Result<()> {
        loop {
            let mut table = self.page_table.write().unwrap();
            let frame_id = match table.get(&page_id) {
                Some(&id) => id,
                None => return Ok(()),
            };

            let meta = &self.metadata[frame_id];

            if meta.pin_count.load(Ordering::Acquire) > 0 {
                return Err(BufferPoolError::PinCountError);
            }

            if meta.is_dirty.load(Ordering::Acquire) {
                meta.pin_count.fetch_add(1, Ordering::SeqCst);
                let pid = meta.page_id.load(Ordering::Acquire);
                drop(table);

                let res = self.write_frame_to_disk(frame_id, pid);

                let prev = meta.pin_count.fetch_sub(1, Ordering::SeqCst);
                if prev == 1 {
                    self.replacer.lock().unwrap().unpin(frame_id);
                }
                
                if res.is_err() {
                    meta.is_dirty.store(true, Ordering::Release);
                    return Err(BufferPoolError::InternalError(
                        "Flush failed during delete".to_string(),
                    ));
                }
                meta.is_dirty.store(false, Ordering::Release);
                continue;
            }

            table.remove(&page_id);
            meta.page_id.store(INVALID_FRAME_ID, Ordering::Release);
            meta.pin_count.store(0, Ordering::SeqCst);
            meta.is_dirty.store(false, Ordering::Release);
            self.replacer.lock().unwrap().pin(frame_id);
            self.free_list.lock().unwrap().push(frame_id);

            return Ok(());
        }
    }

    pub fn mark_dirty(&self, page_id: u64, lsn: Lsn, page_lsn_before: Lsn) -> bool {
        let redo_point = self.last_checkpoint_redo_point.load(Ordering::Acquire);
        
        let table = self.page_table.read().unwrap();
        if let Some(&frame_id) = table.get(&page_id) {
            let meta = &self.metadata[frame_id];
            
            // Atomically check and set is_dirty
            let was_clean = !meta.is_dirty.swap(true, Ordering::SeqCst);
            
            if was_clean {
                let mut rec_lsn = meta.rec_lsn.lock().unwrap();
                *rec_lsn = Some(lsn);

                let mut min_rec = self.min_rec_lsn.lock().unwrap();
                if min_rec.is_none() || lsn < min_rec.unwrap() {
                    *min_rec = Some(lsn);
                }
                return page_lsn_before <= redo_point;
            }
        }
        false
    }
}
