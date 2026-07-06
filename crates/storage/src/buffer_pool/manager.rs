use crate::buffer_pool::shard::{BufferPoolShard, PageReadGuard, PageWriteGuard};
use crate::disk::DiskManager;
use crate::page::{Lsn, PAGE_SIZE, PageId};
use crate::wal::Wal;
use common::{BufferPoolError, MAX_FRAMES, NUM_SHARDS, SHARD_MASK};
use std::cmp::max;
use std::sync::{Arc, Mutex};

pub type Result<T> = std::result::Result<T, BufferPoolError>;

/// The main manager for the buffer pool, providing a partitioned cache for disk pages.
pub struct BufferPoolManager {
    pub(crate) shards: Vec<BufferPoolShard>,
    next_page_id: Mutex<u64>,
    pub(crate) free_pool: Mutex<Vec<PageId>>,
    pub(crate) free_page: Mutex<u64>,
}

impl BufferPoolManager {
    /// Creates a new `BufferPoolManager` with the given disk manager.
    ///
    /// It initializes the shards and sets the `next_page_id` based on the
    /// current number of pages in the disk file.
    pub fn new(disk_manager: Arc<DiskManager>, wal: Arc<Wal>) -> Self {
        let existing_pages = disk_manager.num_pages().unwrap_or(0);
        let shard_size = MAX_FRAMES / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| BufferPoolShard::new(disk_manager.clone(), shard_size, Arc::clone(&wal)))
            .collect();

        let pool = Self {
            shards,
            next_page_id: Mutex::new(existing_pages),
            free_pool: Mutex::new(Vec::new()),
            free_page: Mutex::new(0),
        };
        if existing_pages > 0
            && let Ok(meta) = pool.fetch_page(0)
        {
            *pool.free_page.lock().unwrap() = crate::page::meta::read_free_space(&meta[..]);
        }

        pool
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

    /// Returns the next page id that will be allocated.
    pub fn next_page_id(&self) -> u64 {
        *self.next_page_id.lock().unwrap()
    }


    fn claim_recycled(&self, pid: PageId) -> Result<()> {
        let free_space_id = *self.free_page.lock().unwrap();
        let mut fs_guard = self.fetch_page_mut(free_space_id)?;
        crate::page::free_space::clear_free(&mut fs_guard[..], pid);
        let image: &[u8; PAGE_SIZE] = (&fs_guard[..]).try_into().unwrap();
        let lsn = self.shards[0].wal.log_page_compact(0, free_space_id, image)?;
        crate::page::set_lsn(&mut fs_guard[..], lsn);
        Ok(())
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
        let recycled = self.free_pool.lock().unwrap().pop();
        let page_id = if let Some(pid) = recycled {
            // Recycle: claim the id in the bitmap page and journal the
            // allocation, so a replayed bitmap never re-offers it. On any
            // error the id would otherwise be lost — re-park it.
            match self.claim_recycled(pid) {
                Ok(()) => pid,
                Err(e) => {
                    self.free_pool.lock().unwrap().push(pid);
                    return Err(e);
                }
            }
        } else {
            let mut id = self.next_page_id.lock().unwrap();
            let pid = *id;
            *id += 1;
            pid
        };

        let shard = self.get_shard(page_id);
        // A fresh id is always a miss; a recycled id may hit a frame still
        // caching the page's previous life. Either way the fill(0) below
        // starts the page from a clean slate.
        let (frame_id, _needs_load) = shard.acquire_frame(page_id)?;

        let mut data = shard.pages[frame_id].write().unwrap();
        data.fill(0);
        // No disk read for a brand-new page; mark the frame ready (still holding
        // the write guard — page→inner lock order is safe).
        shard.finish_load(page_id, frame_id, true);

        Ok(PageWriteGuard {
            shard,
            page_id,
            guard: Some(data),
            dirty: false,
            record_lsn: None,
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

        let (frame_id, needs_load) = shard.acquire_frame(page_id)?;

        if needs_load {
            // Load + verify under the frame write lock. The mapping is published
            // in the `loading` state, so other threads wait rather than observe
            // these bytes before verification.
            let load = {
                let mut data = shard.pages[frame_id].write().unwrap();
                shard
                    .disk_manager
                    .read_page(page_id, data.0.as_mut())
                    .map_err(BufferPoolError::from)
                    .and_then(|()| {
                        crate::page::verify_checksum(&data[..]).map_err(|(expected, actual)| {
                            BufferPoolError::PageCorruption {
                                page_id,
                                expected,
                                actual,
                            }
                        })
                    })
            }; // write guard dropped here

            if let Err(e) = load {
                shard.finish_load(page_id, frame_id, false);
                return Err(e);
            }
            shard.finish_load(page_id, frame_id, true);
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

        let (frame_id, needs_load) = shard.acquire_frame(page_id)?;

        // Hold the write guard across the load so we can hand it straight back on
        // success without a re-lock window.
        let mut data = shard.pages[frame_id].write().unwrap();
        if needs_load {
            let load = shard
                .disk_manager
                .read_page(page_id, data.0.as_mut())
                .map_err(BufferPoolError::from)
                .and_then(|()| {
                    crate::page::verify_checksum(&data[..]).map_err(|(expected, actual)| {
                        BufferPoolError::PageCorruption {
                            page_id,
                            expected,
                            actual,
                        }
                    })
                });

            match load {
                // Still holding `data` (page write lock) while finish_load takes
                // the inner lock — page→inner order is deadlock-free.
                Ok(()) => shard.finish_load(page_id, frame_id, true),
                Err(e) => {
                    drop(data);
                    shard.finish_load(page_id, frame_id, false);
                    return Err(e);
                }
            }
        }

        Ok(PageWriteGuard {
            shard,
            page_id,
            guard: Some(data),
            dirty: false,
            record_lsn: None,
        })
    }

    /// Flushes a specific page to disk if it is dirty.
    ///
    /// This is an explicit durability boundary for one page: the target shard
    /// writes the page and syncs the data file before returning.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::InternalError`] if a disk I/O error occurs.
    pub fn flush_page(&self, page_id: u64) -> Result<()> {
        self.get_shard(page_id).flush_page(page_id)?;
        Ok(())
    }

    /// Flushes all dirty pages in the buffer pool to disk.
    ///
    /// Each shard writes its dirty pages first, then performs one data-file
    /// sync for that shard. This preserves WAL-before-page while avoiding an
    /// `fdatasync` per dirty page.
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

    /// Ensures a page exists in the buffer pool and returns it for writing.
    ///
    /// Recovery uses this for redo records whose target page may be beyond the
    /// current end of `data.db`. Existing pages are loaded and checksum-verified;
    /// missing pages are materialized as zeroed frames and `next_page_id` is
    /// advanced so later allocations cannot reuse recovered page IDs.
    ///
    /// # Errors
    ///
    /// * Returns [`BufferPoolError::NoEvictableFrames`] if no frame can be reserved.
    /// * Returns [`BufferPoolError::PageCorruption`] if an existing page fails checksum verification.
    /// * Returns [`BufferPoolError::Disk`] if the disk manager returns an I/O error.
    pub fn ensure_page(&self, page_id: u64) -> Result<PageWriteGuard<'_>> {
        let shard = self.get_shard(page_id);
        let (frame_id, needs_load) = shard.acquire_frame(page_id)?;
        let mut data = shard.pages[frame_id].write().unwrap();
        if needs_load {
            let num_pages = shard.disk_manager.num_pages()?;
            if num_pages > page_id {
                let load = shard
                    .disk_manager
                    .read_page(page_id, data.0.as_mut())
                    .map_err(BufferPoolError::from)
                    .and_then(|()| {
                        crate::page::verify_checksum(&data[..]).map_err(|(expected, actual)| {
                            BufferPoolError::PageCorruption {
                                page_id,
                                expected,
                                actual,
                            }
                        })
                    });
                match load {
                    // Still holding `data` (page write lock) while finish_load takes
                    // the inner lock — page→inner order is deadlock-free.
                    Ok(()) => shard.finish_load(page_id, frame_id, true),
                    Err(e) => {
                        drop(data);
                        shard.finish_load(page_id, frame_id, false);
                        return Err(e);
                    }
                }
            } else {
                data.fill(0);
                shard.finish_load(page_id, frame_id, true);
            }
        }
        {
            let mut id = self.next_page_id.lock().unwrap();
            *id = max(*id, page_id + 1);
        }
        Ok(PageWriteGuard {
            shard,
            page_id,
            guard: Some(data),
            dirty: false,
            record_lsn: None,
        })
    }

    /// Returns the min rec_lsn among all the frame by comparing minimun lsn of the shards
    ///This point is the redo point
    pub fn min_rec_lsn(&self) -> Option<Lsn> {
        self.shards
            .iter()
            .filter_map(|shard| shard.inner.lock().unwrap().min_rec_lsn)
            .min()
    }
}
