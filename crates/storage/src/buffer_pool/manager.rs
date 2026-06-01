use crate::buffer_pool::shard::{BufferPoolShard, PageReadGuard, PageWriteGuard};
use crate::disk::DiskManager;
use common::{BufferPoolError, MAX_FRAMES, NUM_SHARDS, SHARD_MASK};
use std::sync::{Arc, Mutex};

pub type Result<T> = std::result::Result<T, BufferPoolError>;

/// The main manager for the buffer pool, providing a partitioned cache for disk pages.
pub struct BufferPoolManager {
    shards: Vec<BufferPoolShard>,
    next_page_id: Mutex<u64>,
}

impl BufferPoolManager {
    /// Creates a new `BufferPoolManager` with the given disk manager.
    ///
    /// It initializes the shards and sets the `next_page_id` based on the
    /// current number of pages in the disk file.
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let existing_pages = disk_manager.num_pages().unwrap_or(0);
        let shard_size = MAX_FRAMES / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| BufferPoolShard::new(disk_manager.clone(), shard_size))
            .collect();

        Self {
            shards,
            next_page_id: Mutex::new(existing_pages),
        }
    }

    #[inline]
    fn get_shard(&self, page_id: u64) -> &BufferPoolShard {
        &self.shards[(page_id & SHARD_MASK) as usize]
    }

    fn check_page_id(&self, page_id: u64) -> Result<()> {
        let next_id = *self.next_page_id.lock().unwrap();
        if page_id >= next_id {
            Err(BufferPoolError::PageNotFound(page_id))
        } else {
            Ok(())
        }
    }

    /// Creates a new page in the buffer pool.
    ///
    /// This will allocate a new `PageId`, find a free frame (potentially evicting
    /// an existing page), and return a `PageWriteGuard` for the new page.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if all frames are pinned.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs during eviction.
    pub fn new_page(&self) -> Result<PageWriteGuard<'_>> {
        let page_id = {
            let mut id = self.next_page_id.lock().unwrap();
            let pid = *id;
            *id += 1;
            pid
        };

        let shard = self.get_shard(page_id);
        let (frame_id, _) = shard.evict_and_replace(page_id)?;

        let mut data = shard.pages[frame_id].write().unwrap();
        data.fill(0);

        Ok(PageWriteGuard {
            shard,
            page_id,
            guard: Some(data),
            dirty: false,
        })
    }

    /// Fetches a page from the buffer pool for reading.
    ///
    /// If the page is not in memory, it will be loaded from disk. The returned
    /// `PageReadGuard` ensures the page remains pinned and allows read-only access.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::PageNotFound`] if the `page_id` is invalid.
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if a load is required but no frames are evictable.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn fetch_page(&self, page_id: u64) -> Result<PageReadGuard<'_>> {
        self.check_page_id(page_id)?;
        let shard = self.get_shard(page_id);

        if let Some(frame_id) = shard.pin_page(page_id) {
            let data = shard.pages[frame_id].read().unwrap();
            return Ok(PageReadGuard {
                shard,
                page_id,
                guard: Some(data),
            });
        }

        let (frame_id, needs_load) = shard.evict_and_replace(page_id)?;

        if needs_load {
            let mut data = shard.pages[frame_id].write().unwrap();
            shard
                .disk_manager
                .read_page(page_id, data.0.as_mut())?;
            if let Err((expected, actual)) = crate::page::verify_checksum(&data[..]) {
                drop(data); // drop is called to free the bytes for use by other thread. 
                shard.discard_frame(page_id);
                return Err(BufferPoolError::PageCorruption {
                    page_id,
                    expected,
                    actual,
                });
            }
        }

        let data = shard.pages[frame_id].read().unwrap();
        Ok(PageReadGuard {
            shard,
            page_id,
            guard: Some(data),
        })
    }

    /// Fetches a page from the buffer pool for writing.
    ///
    /// Similar to `fetch_page`, but returns a `PageWriteGuard` allowing
    /// mutable access to the page data.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::PageNotFound`] if the `page_id` is invalid.
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if a load is required but no frames are evictable.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn fetch_page_mut(&self, page_id: u64) -> Result<PageWriteGuard<'_>> {
        self.check_page_id(page_id)?;
        let shard = self.get_shard(page_id);

        if let Some(frame_id) = shard.pin_page(page_id) {
            let data = shard.pages[frame_id].write().unwrap();
            return Ok(PageWriteGuard {
                shard,
                page_id,
                guard: Some(data),
                dirty: false,
            });
        }

        let (frame_id, needs_load) = shard.evict_and_replace(page_id)?;

        let mut data = shard.pages[frame_id].write().unwrap();
        if needs_load {
            shard
                .disk_manager
                .read_page(page_id, data.0.as_mut())?;
            if let Err((expected, actual)) = crate::page::verify_checksum(&data[..]) {
                drop(data);
                shard.discard_frame(page_id);
                return Err(BufferPoolError::PageCorruption {
                    page_id,
                    expected,
                    actual,
                });
            }
        }

        Ok(PageWriteGuard {
            shard,
            page_id,
            guard: Some(data),
            dirty: false,
        })
    }

    /// Flushes a specific page to disk if it is dirty.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn flush_page(&self, page_id: u64) -> Result<()> {
        self.get_shard(page_id).flush_page(page_id)
    }

    /// Flushes all dirty pages in the buffer pool to disk.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn flush_all_pages(&self) -> Result<()> {
        for shard in &self.shards {
            shard.flush_all_pages()?;
        }
        Ok(())
    }

    /// Deletes a page from the buffer pool and disk.
    ///
    /// The page must not be pinned by any other process.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::PinCountError`] if the page is currently pinned.
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn delete_page(&self, page_id: u64) -> Result<()> {
        self.get_shard(page_id).delete_page(page_id)
    }
}
