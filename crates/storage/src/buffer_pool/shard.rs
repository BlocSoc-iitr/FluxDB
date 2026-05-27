//! # Buffer Pool Shard
//!
//! A shard manages a fixed-size subset of the buffer pool's frames. Sharding
//! reduces lock contention by allowing multiple threads to access different
//! partitions of the buffer pool simultaneously.

use crate::buffer_pool::replacer::ClockReplacer;
use crate::disk::DiskManager;
use common::BufferPoolError;
use common::{INVALID_FRAME_ID, MAX_PAGE_SIZE};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

type Result<T> = std::result::Result<T, BufferPoolError>;

/// A fixed-size buffer for a single database page.
pub struct PageData(pub Box<[u8; MAX_PAGE_SIZE]>);

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
        self.shard.unpin_page(self.page_id, self.dirty);
    }
}

/// Metadata for a single frame in a buffer pool shard.
pub struct FrameMetadata {
    pub page_id: u64,
    pub pin_count: u64,
    pub is_dirty: bool,
}

/// Internal state of a buffer pool shard, protected by a mutex.
pub struct ShardInner {
    pub metadata: Vec<FrameMetadata>,
    pub page_table: HashMap<u64, usize>,
    pub free_list: Vec<usize>,
    pub replacer: ClockReplacer,
}

/// A shard of the buffer pool, managing a subset of the total frames.
pub struct BufferPoolShard {
    pub disk_manager: Arc<DiskManager>,
    pub pages: Vec<RwLock<PageData>>,
    pub inner: Mutex<ShardInner>,
}

impl BufferPoolShard {
    /// Creates a new `BufferPoolShard` with the specified number of frames.
    pub fn new(disk_manager: Arc<DiskManager>, size: usize) -> Self {
        let mut metadata = Vec::with_capacity(size);
        let mut free_list = Vec::with_capacity(size);
        for frame_id in 0..size {
            metadata.push(FrameMetadata {
                page_id: INVALID_FRAME_ID,
                pin_count: 0,
                is_dirty: false,
            });
            free_list.push(size - 1 - frame_id);
        }

        Self {
            disk_manager,
            pages: (0..size).map(|_| RwLock::new(PageData::new())).collect(),
            inner: Mutex::new(ShardInner {
                metadata,
                page_table: HashMap::with_capacity(size),
                free_list,
                replacer: ClockReplacer::new(size),
            }),
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

    /// Increments the pin count of a page and notifies the replacer.
    pub fn pin_page(&self, page_id: u64) -> Option<usize> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(&frame_id) = inner.page_table.get(&page_id) {
            inner.metadata[frame_id].pin_count += 1;
            inner.replacer.pin(frame_id);
            Some(frame_id)
        } else {
            None
        }
    }

    pub fn find_victim_frame_id(&self, inner: &mut ShardInner) -> Result<usize> {
        if let Some(id) = inner.free_list.pop() {
            Ok(id)
        } else {
            inner.replacer.victim()
        }
    }

    /// Finds an evictable frame and replaces its content with a new page.
    ///
    /// If the evicted page is dirty, it is flushed to disk first.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if no frames can be evicted.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn evict_and_replace(&self, page_id: u64) -> Result<(usize, bool)> {
        loop {
            let mut inner = self.inner.lock().unwrap();

            if let Some(&local_id) = inner.page_table.get(&page_id) {
                inner.metadata[local_id].pin_count += 1;
                inner.replacer.pin(local_id);
                return Ok((local_id, false));
            }

            let frame_id = self.find_victim_frame_id(&mut inner)?;
            let meta = &inner.metadata[frame_id];
            let old_page_id = meta.page_id;
            let is_dirty = meta.is_dirty;

            if is_dirty && old_page_id != INVALID_FRAME_ID {
                drop(inner);
                self.flush_page(old_page_id)?;
                continue;
            }

            if old_page_id != INVALID_FRAME_ID {
                inner.page_table.remove(&old_page_id);
            }

            let meta = &mut inner.metadata[frame_id];
            meta.page_id = page_id;
            meta.pin_count = 1;
            meta.is_dirty = false;

            inner.page_table.insert(page_id, frame_id);
            inner.replacer.pin(frame_id);

            return Ok((frame_id, true));
        }
    }

    /// Flushes a specific page to disk if it is dirty.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn flush_page(&self, page_id: u64) -> Result<()> {
        let (pid, frame_id) = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&id) = inner.page_table.get(&page_id) {
                let meta = &mut inner.metadata[id];
                if meta.is_dirty && meta.page_id != INVALID_FRAME_ID {
                    meta.is_dirty = false;
                    meta.pin_count += 1;
                    (meta.page_id, id)
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        };

        let res = self.write_frame_to_disk(frame_id, pid);

        let mut inner = self.inner.lock().unwrap();
        let meta = &mut inner.metadata[frame_id];
        meta.pin_count -= 1;
        if meta.pin_count == 0 {
            inner.replacer.unpin(frame_id);
        }
        if let Err(e) = res {
            inner.metadata[frame_id].is_dirty = true;
            return Err(e);
        }

        Ok(())
    }

    pub fn flush_all_pages(&self) -> Result<()> {
        let n_frames = self.pages.len();
        for frame_id in 0..n_frames {
            let (pid, is_dirty) = {
                let inner = self.inner.lock().unwrap();
                let meta = &inner.metadata[frame_id];
                (meta.page_id, meta.is_dirty)
            };
            if is_dirty && pid != INVALID_FRAME_ID {
                self.flush_page(pid)?;
            }
        }
        Ok(())
    }

    pub fn write_frame_to_disk(&self, frame_id: usize, page_id: u64) -> Result<()> {
        let mut buf = vec![0u8; MAX_PAGE_SIZE];
        {
            let data = self.pages[frame_id].read().unwrap();
            buf.copy_from_slice(&data[..]);
        }

        self.disk_manager.write_page(page_id, &buf)?;
        self.disk_manager.sync_data()?;
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
                if let Err(_) = res {
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
}
