use crate::buffer_pool::manager::BufferPoolManager;
use crate::buffer_pool::replacer::ClockReplacer;
use crate::disk::DiskManager;
use common::{MAX_FRAMES, MAX_PAGE_SIZE};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_clock_replacer_eviction_order() {
    let mut replacer = ClockReplacer::new(4);
    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    let v1 = replacer.victim().unwrap();
    assert_eq!(v1, 0);
    let v2 = replacer.victim().unwrap();
    assert_eq!(v2, 1);
    replacer.pin(2);
    let v3 = replacer.victim().unwrap();
    assert_eq!(v3, 3);
    let result = replacer.victim();
    assert!(result.is_err());
}

#[test]
fn test_buffer_pool_manager_basic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let bpm = BufferPoolManager::new(disk_manager);
    let page_id;
    {
        let mut page = bpm.new_page().unwrap();
        page_id = page.page_id;
        page[0] = 1;
        page[1] = 2;
    }
    {
        let page = bpm.fetch_page(page_id).unwrap();
        assert_eq!(page[0], 1);
        assert_eq!(page[1], 2);
    }
    {
        let mut page = bpm.fetch_page_mut(page_id).unwrap();
        page[0] = 3;
    }
    {
        let page = bpm.fetch_page(page_id).unwrap();
        assert_eq!(page[0], 3);
    }
}

#[test]
fn test_buffer_pool_manager_eviction_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let bpm = BufferPoolManager::new(disk_manager);
    let mut page_ids = Vec::new();
    for i in 0..MAX_FRAMES + 1 {
        let mut page = bpm.new_page().unwrap();
        page[0] = i as u8;
        page_ids.push(page.page_id);
    }
    for i in 0..MAX_FRAMES + 1 {
        let page = bpm.fetch_page(page_ids[i]).unwrap();
        assert_eq!(page[0], i as u8);
    }
}

#[test]
fn test_buffer_pool_manager_full_lifecycle() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let bpm = BufferPoolManager::new(disk_manager);
    let mut page_ids = Vec::new();
    for i in 0..10 {
        let mut page = bpm.new_page().unwrap();
        let pid = page.page_id;
        page[0] = i as u8;
        page_ids.push(pid);
    }
    for i in 0..10 {
        let page = bpm.fetch_page(page_ids[i]).unwrap();
        assert_eq!(page[0], i as u8);
    }
    for i in 0..10 {
        let mut page = bpm.fetch_page_mut(page_ids[i]).unwrap();
        page[0] = (i + 10) as u8;
    }
    for i in 0..10 {
        bpm.flush_page(page_ids[i]).unwrap();
    }
    for i in 0..10 {
        let page = bpm.fetch_page(page_ids[i]).unwrap();
        assert_eq!(page[0], (i + 10) as u8);
    }
}

#[test]
fn test_buffer_pool_manager_concurrency() {
    use std::thread;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk_manager));
    let mut handles = Vec::new();
    for i in 0..10 {
        let bpm_clone = bpm.clone();
        handles.push(thread::spawn(move || {
            let mut page = bpm_clone.new_page().unwrap();
            page[0] = i as u8;
            let pid = page.page_id;
            drop(page);
            let fetched = bpm_clone.fetch_page(pid).unwrap();
            assert_eq!(fetched[0], i as u8);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_checksum_survives_reopen() {
    // A page flushed with a valid checksum must verify cleanly when loaded from
    // disk by a fresh buffer pool (no cache hit).
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());

    let pid;
    {
        let bpm = BufferPoolManager::new(disk_manager.clone());
        let mut page = bpm.new_page().unwrap();
        pid = page.page_id;
        page[0] = crate::page::LEAF; // valid page type → checksum applies
        page[100] = 42;
        drop(page);
        bpm.flush_page(pid).unwrap();
    }

    let bpm = BufferPoolManager::new(disk_manager);
    let page = bpm.fetch_page(pid).unwrap();
    assert_eq!(page[0], crate::page::LEAF);
    assert_eq!(page[100], 42);
}

#[test]
fn test_checksum_detects_corruption() {
    use common::BufferPoolError;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());

    let pid;
    {
        let bpm = BufferPoolManager::new(disk_manager.clone());
        let mut page = bpm.new_page().unwrap();
        pid = page.page_id;
        page[0] = crate::page::LEAF;
        page[100] = 42;
        drop(page);
        bpm.flush_page(pid).unwrap();
    }

    // Corrupt a byte on disk directly, bypassing the checksum-stamping flush path.
    let mut raw = vec![0u8; MAX_PAGE_SIZE];
    disk_manager.read_page(pid, &mut raw).unwrap();
    raw[100] ^= 0xFF;
    disk_manager.write_page(pid, &raw).unwrap();
    disk_manager.sync_data().unwrap();

    // A fresh pool must load from disk and reject the corrupted page. A failed
    // verify also discards the frame, so the bad bytes are never cached — both
    // fetch paths on the same pool keep reloading from disk and keep rejecting.
    let bpm = BufferPoolManager::new(disk_manager);
    assert!(matches!(
        bpm.fetch_page(pid),
        Err(BufferPoolError::PageCorruption { .. })
    ));
    assert!(matches!(
        bpm.fetch_page_mut(pid),
        Err(BufferPoolError::PageCorruption { .. })
    ));
}

#[test]
fn test_buffer_pool_manager_pin_count() {
    use common::BufferPoolError;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let disk_manager = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let bpm = BufferPoolManager::new(disk_manager);
    let mut pages = Vec::new();
    for _ in 0..MAX_FRAMES {
        pages.push(bpm.new_page().unwrap());
    }
    let res = bpm.new_page();
    assert!(matches!(res, Err(BufferPoolError::NoEvictableFrames)));
    drop(pages);
    assert!(bpm.new_page().is_ok());
}
