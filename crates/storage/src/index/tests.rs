use super::*;
use db_core::transaction_manager::TransactionStatus;
use crate::buffer_pool::manager::BufferPoolManager;
use crate::disk::DiskManager;
use crate::recovery::RecoveryManager;
use crate::wal::Wal;
use crate::wal::{WalIterator, WalRecordType};
use common::MAX_PAGE_SIZE;
use std::mem::forget;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use tempfile::tempdir;

type TM = db_core::transaction_manager::TransactionManager;
type Idx = BTreeIndex<&'static [u8], &'static [u8]>;

/// Open a throwaway WAL under `dir`. The index and pool must share one WAL,
/// so callers build it once here and clone the `Arc` to both.
fn make_wal(dir: &Path) -> Arc<Wal> {
    Arc::new(Wal::new(dir.join("wal")).unwrap())
}

/// Wrap `disk` in a pool backed by `wal` (required for WAL-before-page).
fn make_pool(disk: Arc<DiskManager>, wal: Arc<Wal>) -> Arc<BufferPoolManager> {
    Arc::new(BufferPoolManager::new(disk, wal))
}

fn make_index() -> BTreeIndex<&'static [u8], &'static [u8]> {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let wal = make_wal(dir.path());
    forget(dir);
    let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let pool = make_pool(disk, wal.clone());
    let (index, _) = BTreeIndex::create(pool, wal).unwrap();
    index
}

fn auto() -> Transaction {
    static TM_LOCK: OnceLock<Arc<db_core::transaction_manager::TransactionManager>> =
        OnceLock::new();
    let tm = TM_LOCK
        .get_or_init(|| Arc::new(db_core::transaction_manager::TransactionManager::new()))
        .clone();

    static TEST_TXN_ID: AtomicU64 = AtomicU64::new(1);
    Transaction::new(
        TEST_TXN_ID.fetch_add(1, Relaxed),
        db_core::transaction::Snapshot::latest(),
        tm,
    )
}

fn leak_bytes(b: &[u8]) -> &'static [u8] {
    Box::leak(b.to_vec().into_boxed_slice())
}

// `make_wal(dir)` opens dir/"wal" (segment dir); data lives at dir/"test.db".
fn build_db(dir: &Path) -> (Arc<BufferPoolManager>, Arc<Wal>, Arc<TM>, Idx) {
    let disk = Arc::new(DiskManager::new(dir.join("test.db"), MAX_PAGE_SIZE).unwrap());
    let wal = make_wal(dir);
    let pool = make_pool(disk, wal.clone());
    let tm = Arc::new(TM::new());
    let (index, _root) = BTreeIndex::create(pool.clone(), wal.clone()).unwrap();
    (pool, wal, tm, index)
}

/// Reopen = recover. Caller must drop the previous tuple first; only durable
/// (committed / flushed) records survive the drop.
fn reopen_db(dir: &Path) -> (Arc<BufferPoolManager>, Arc<Wal>, Arc<TM>, Idx) {
    let disk = Arc::new(DiskManager::new(dir.join("test.db"), MAX_PAGE_SIZE).unwrap());
    let wal = make_wal(dir);
    let pool = make_pool(disk, wal.clone());
    let tm = Arc::new(TM::new());
    RecoveryManager::new(pool.clone(), dir.join("wal"), tm.clone())
        .recover::<&[u8], &[u8]>()
        .unwrap();
    let index = BTreeIndex::open(pool.clone(), wal.clone()).unwrap();
    (pool, wal, tm, index)
}

// ── Basic get / insert ────────────────────────────────────────────────

#[test]
fn get_missing_key_returns_none() {
    let idx = make_index();
    assert!(idx.get(&(&b"hello"[..]), &auto()).unwrap().is_none());
}

#[test]
fn insert_and_get_single_entry() {
    let idx = make_index();
    idx.insert(&(&b"key"[..]), &(&b"value"[..]), &auto())
        .unwrap();
    let got = idx.get(&(&b"key"[..]), &auto()).unwrap().unwrap();
    assert_eq!(got, b"value");
}

#[test]
fn insert_duplicate_returns_error() {
    let idx = make_index();
    idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
    match idx.insert(&(&b"k"[..]), &(&b"v2"[..]), &auto()) {
        Err(IndexError::DuplicateKey) => {}
        other => panic!("expected DuplicateKey, got {:?}", other),
    }
}

// ── Update ───────────────────────────────────────────────────────────

#[test]
fn update_returns_new_value() {
    let idx = make_index();
    idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
    idx.update(&(&b"k"[..]), &(&b"v2"[..]), &auto()).unwrap();
    let got = idx.get(&(&b"k"[..]), &auto()).unwrap().unwrap();
    assert_eq!(got, b"v2");
}

#[test]
fn update_missing_key_returns_error() {
    let idx = make_index();
    match idx.update(&(&b"nope"[..]), &(&b"v"[..]), &auto()) {
        Err(IndexError::KeyNotFound) => {}
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

// ── Delete ───────────────────────────────────────────────────────────

#[test]
fn delete_missing_key_returns_error() {
    let idx = make_index();
    match idx.delete(&(&b"nope"[..]), &auto()) {
        Err(IndexError::KeyNotFound) => {}
        other => panic!("expected KeyNotFound, got {:?}", other),
    }
}

#[test]
fn insert_then_delete() {
    let idx = make_index();
    idx.insert(&(&b"k"[..]), &(&b"v"[..]), &auto()).unwrap();
    idx.delete(&(&b"k"[..]), &auto()).unwrap();
    assert!(idx.get(&(&b"k"[..]), &auto()).unwrap().is_none());
}

#[test]
fn insert_after_delete() {
    let idx = make_index();
    idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
    idx.delete(&(&b"k"[..]), &auto()).unwrap();
    // After delete, insert a new version — should succeed (old is dead).
    let r = idx.insert(&(&b"k"[..]), &(&b"v2"[..]), &auto());
    assert!(r.is_ok(), "insert after delete failed: {:?}", r.err());
    let got = idx.get(&(&b"k"[..]), &auto()).unwrap();
    assert!(got.is_some(), "get returned None after insert-after-delete");
    assert_eq!(got.unwrap(), b"v2");
}

// ── Reopen / metadata page ───────────────────────────────────────────

#[test]
fn reopen_recovers_root_and_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reopen.db");

    // Create, insert enough to force splits (incl. a root split), flush, close.
    {
        let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
        let wal = make_wal(dir.path());
        let pool = make_pool(disk, wal.clone());
        let (index, _root) =
            BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal).unwrap();
        for k in 0u32..300 {
            let key = leak_bytes(&k.to_be_bytes());
            let val = leak_bytes(&(k * 7).to_be_bytes());
            index.insert(&key, &val, &auto()).unwrap();
        }
        pool.flush_all_pages().unwrap();
    }

    // Reopen with NO root id — it must be recovered from the page-0 superblock.
    let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let wal = make_wal(dir.path());
    let pool = make_pool(disk, wal.clone());
    let index = BTreeIndex::<&'static [u8], &'static [u8]>::open(pool, wal).unwrap();
    for k in 0u32..300 {
        let key = leak_bytes(&k.to_be_bytes());
        let expected = (k * 7).to_be_bytes();
        let got = index.get(&key, &auto()).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(&expected[..]),
            "key {k} wrong after reopen"
        );
    }
}

#[test]
fn reopen_after_create_makes_root_durable() {
    // `create` alone (no inserts, no flush_all) must make BOTH the superblock
    // and the root page durable — otherwise a cold reopen can't find the root.
    let dir = tempdir().unwrap();
    let path = dir.path().join("fresh.db");
    {
        let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
        let wal = make_wal(dir.path());
        let pool = make_pool(disk, wal.clone());
        let _ = BTreeIndex::<&'static [u8], &'static [u8]>::create(pool, wal).unwrap();
    }
    let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let wal = make_wal(dir.path());
    let pool = make_pool(disk, wal.clone());
    let index = BTreeIndex::<&'static [u8], &'static [u8]>::open(pool, wal).unwrap();
    assert!(index.get(&(&b"anything"[..]), &auto()).unwrap().is_none());
}

// ── Sequential inserts / splits ──────────────────────────────────────

#[test]
fn sequential_inserts_all_readable() {
    let idx = make_index();
    for i in 0u64..200 {
        let k = i.to_be_bytes();
        let v = (i * 10).to_be_bytes();
        idx.insert(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
    }
    for i in 0u64..200 {
        let k = i.to_be_bytes();
        let expected = (i * 10).to_be_bytes();
        let got = idx
            .get(&(k.as_ref()), &auto())
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing", i));
        assert_eq!(got, expected.as_ref());
    }
}

#[test]
fn large_insert_all_keys_readable() {
    let idx = make_index();
    for i in 0u32..2000 {
        let k = i.to_be_bytes();
        let v = (i + 1).to_be_bytes();
        idx.insert(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
    }
    for i in 0u32..2000 {
        let k = i.to_be_bytes();
        let expected = (i + 1).to_be_bytes();
        let got = idx
            .get(&(k.as_ref()), &auto())
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing", i));
        assert_eq!(got, expected.as_ref());
    }
}

#[test]
fn reverse_order_inserts_all_readable() {
    let idx = make_index();
    for i in (0u32..500).rev() {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    for i in 0u32..500 {
        let k = i.to_be_bytes();
        let got = idx
            .get(&(k.as_ref()), &auto())
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing", i));
        assert_eq!(got, k.as_ref());
    }
}

#[test]
fn descent_finishes_incomplete_split() {
    let idx = make_index();
    // Build a multi-level tree with gaps (even keys) so an odd key routes
    // into an existing leaf.
    for i in (0u32..2000).step_by(2) {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }

    // Leftmost leaf — it has split, so it carries a rightlink + high key.
    let mut pid = idx.root_page_id();
    let leftmost = loop {
        let page = idx.pool.fetch_page(pid).unwrap();
        if page[0] == LEAF {
            break pid;
        }
        let child = InternalPageAccessor::<&[u8]>::new(&page[..]).child_page_at(0);
        drop(page);
        pid = child;
    };
    assert!(
        LeafPageAccessor::<&[u8], &[u8]>::new(&idx.pool.fetch_page(leftmost).unwrap()[..])
            .rightlink()
            .is_some(),
        "test needs a split tree",
    );

    // Simulate a recovered/raced incomplete split: flag set, downlink present.
    crate::page::set_incomplete_split(&mut idx.pool.fetch_page_mut(leftmost).unwrap()[..]);
    assert!(crate::page::is_incomplete_split(
        &idx.pool.fetch_page(leftmost).unwrap()[..]
    ));

    // An insert that descends through the leaf must finish the split.
    let k1 = 1u32.to_be_bytes();
    idx.insert(&(k1.as_ref()), &(k1.as_ref()), &auto()).unwrap();

    assert!(
        !crate::page::is_incomplete_split(&idx.pool.fetch_page(leftmost).unwrap()[..]),
        "descent should have cleared the flag",
    );

    // No duplicate downlink, no lost data.
    assert_eq!(
        idx.get(&(k1.as_ref()), &auto()).unwrap().unwrap(),
        k1.as_ref()
    );
    for i in (0u32..2000).step_by(2) {
        let k = i.to_be_bytes();
        assert!(
            idx.get(&(k.as_ref()), &auto()).unwrap().is_some(),
            "key {} missing",
            i
        );
    }
}

// ── Delete bulk ──────────────────────────────────────────────────────

#[test]
fn delete_half_keys_remaining_readable() {
    let idx = make_index();
    for i in 0u32..200 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    for i in (0u32..200).filter(|x| x % 2 == 0) {
        let k = i.to_be_bytes();
        idx.delete(&(k.as_ref()), &auto()).unwrap();
    }
    for i in (0u32..200).filter(|x| x % 2 == 1) {
        let k = i.to_be_bytes();
        idx.get(&(k.as_ref()), &auto())
            .unwrap()
            .unwrap_or_else(|| panic!("odd key {} missing", i));
    }
    for i in (0u32..200).filter(|x| x % 2 == 0) {
        let k = i.to_be_bytes();
        assert!(idx.get(&(k.as_ref()), &auto()).unwrap().is_none());
    }
}

// ── Update bulk ──────────────────────────────────────────────────────

#[test]
fn stress_update_all_keys() {
    let idx = make_index();
    let n = 200u32;
    for i in 0..n {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    for i in 0..n {
        let k = i.to_be_bytes();
        let v = (i + 1000).to_be_bytes();
        idx.update(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
    }
    for i in 0..n {
        let k = i.to_be_bytes();
        let expected = (i + 1000).to_be_bytes();
        let got = idx
            .get(&(k.as_ref()), &auto())
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing after update", i));
        assert_eq!(got, expected.as_ref());
    }
}

// ── Range scan ───────────────────────────────────────────────────────

#[test]
fn range_scan_full() {
    let idx = make_index();
    for i in 0u32..100 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    let results: Vec<_> = idx
        .range::<std::ops::RangeFull>(.., &auto())
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 100);
}

#[test]
fn range_scan_skips_deleted() {
    let idx = make_index();
    for i in 0u32..10 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    for &i in &[3u32, 5, 7] {
        let k = i.to_be_bytes();
        idx.delete(&(k.as_ref()), &auto()).unwrap();
    }
    let results: Vec<_> = idx
        .range::<std::ops::RangeFull>(.., &auto())
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 7);
}

#[test]
fn backward_range_scan_full() {
    let idx = make_index();
    for i in 0u32..100 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    let results: Vec<_> = idx
        .range_backward::<std::ops::RangeFull>(.., &auto())
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 100);

    // Keys must come out in strictly descending order.
    for w in results.windows(2) {
        assert!(
            w[0].0 > w[1].0,
            "expected descending order, got {:?} then {:?}",
            w[0].0,
            w[1].0
        );
    }
}

#[test]
fn backward_range_scan_skips_deleted() {
    let idx = make_index();
    for i in 0u32..10 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    for &i in &[3u32, 5, 7] {
        let k = i.to_be_bytes();
        idx.delete(&(k.as_ref()), &auto()).unwrap();
    }
    let results: Vec<_> = idx
        .range_backward::<std::ops::RangeFull>(.., &auto())
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 7);

    // Deleted keys must be absent.
    let keys: Vec<u32> = results
        .iter()
        .map(|(k, _)| u32::from_be_bytes(k[..4].try_into().unwrap()))
        .collect();
    assert!(!keys.contains(&3));
    assert!(!keys.contains(&5));
    assert!(!keys.contains(&7));

    for w in results.windows(2) {
        assert!(w[0].0 > w[1].0);
    }
}

#[test]
fn backward_range_scan_bounded() {
    let idx = make_index();
    for i in 0u32..100 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    let start: &'static [u8] = leak_bytes(&10u32.to_be_bytes());
    let end: &'static [u8] = leak_bytes(&20u32.to_be_bytes());
    let results: Vec<_> = idx
        .range_backward(start..end, &auto())
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 10);

    let first = u32::from_be_bytes(results.first().unwrap().0[..4].try_into().unwrap());
    let last = u32::from_be_bytes(results.last().unwrap().0[..4].try_into().unwrap());
    assert_eq!(first, 19);
    assert_eq!(last, 10);
}

#[test]
fn range_scan_bounded() {
    let idx = make_index();
    for i in 0u32..100 {
        let k = i.to_be_bytes();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
    }
    let start: &'static [u8] = leak_bytes(&10u32.to_be_bytes());
    let end: &'static [u8] = leak_bytes(&20u32.to_be_bytes());
    let results: Vec<_> = idx.range(start..end, &auto()).map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 10);
}

#[test]
fn range_scan_included_start_can_miss_only_visible_duplicate_before_position_result() {
    let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
    let idx = make_index();

    let duplicate_key: &'static [u8] = leak_bytes(&40u32.to_be_bytes());
    let after_duplicate_key: &'static [u8] = leak_bytes(&50u32.to_be_bytes());
    let after_duplicate_value: &'static [u8] = leak_bytes(&50u32.to_be_bytes());
    let value_1: &'static [u8] = leak_bytes(&1u32.to_be_bytes());
    let value_2: &'static [u8] = leak_bytes(&2u32.to_be_bytes());
    let value_3: &'static [u8] = leak_bytes(&3u32.to_be_bytes());

    let seed_txn = tm.begin();
    idx.insert(&after_duplicate_key, &after_duplicate_value, &seed_txn)
        .unwrap();
    tm.mark_committed(seed_txn.txn_id);

    let txn1 = tm.begin();
    idx.insert(&duplicate_key, &value_1, &txn1).unwrap();
    tm.mark_committed(txn1.txn_id);

    let txn2 = tm.begin();
    idx.update(&duplicate_key, &value_2, &txn2).unwrap();
    tm.mark_committed(txn2.txn_id);

    let reader = tm.begin();

    let txn3 = tm.begin();
    idx.update(&duplicate_key, &value_3, &txn3).unwrap();

    {
        let root = idx.root_page_id();
        let leaf = idx.pool.fetch_page(root).unwrap();
        let acc = LeafPageAccessor::<&'static [u8], &'static [u8]>::new(&leaf[..]);

        assert_eq!(acc.num_pairs(), 4);
        assert_eq!(acc.get_value(0), value_2);
        assert_eq!(acc.get_value(1), value_3);
        assert_eq!(acc.get_value(2), value_1);
        assert_eq!(acc.get_value(3), after_duplicate_value);

        assert!(reader.is_visible(acc.get_xmin(0), acc.get_xmax(0)));
        assert!(!reader.is_visible(acc.get_xmin(1), acc.get_xmax(1)));
        assert!(!reader.is_visible(acc.get_xmin(2), acc.get_xmax(2)));

        let acc = LeafPageAccessor::<&'static [u8], &'static [u8]>::new(&leaf[..]);
        assert_eq!(
            acc.position(&duplicate_key).0,
            2,
            "test setup expects position(40) to land after the visible duplicate"
        );
    }

    let results: Vec<_> = idx
        .range(duplicate_key..=duplicate_key, &reader)
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(
        results,
        vec![(duplicate_key.to_vec(), value_2.to_vec())],
        "range scan should return the only visible physical record for key 40"
    );
}

// ── Write conflict ───────────────────────────────────────────────────

#[test]
fn write_conflict_on_concurrent_delete() {
    let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
    let idx = make_index();

    // 1. Insert a key.
    let insert_txn = tm.begin();
    idx.insert(&(&b"k"[..]), &(&b"v"[..]), &insert_txn).unwrap();
    tm.mark_committed(insert_txn.txn_id);

    // 2. Start txn10 and delete the key.
    let txn10 = tm.begin();
    idx.delete(&(&b"k"[..]), &txn10).unwrap();

    // 3. Start txn20. It should see txn10 as active.
    let txn20 = tm.begin();

    match idx.delete(&(&b"k"[..]), &txn20) {
        Err(IndexError::WriteConflict) => {}
        other => panic!("expected WriteConflict, got {:?}", other),
    }
}

#[test]
fn vacuum_reclaims_space() {
    let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
    let idx = make_index();

    // 1. Insert 100 keys and commit.
    for i in 0u32..100 {
        let k = i.to_be_bytes();
        let txn = tm.begin();
        idx.insert(&(k.as_ref()), &(k.as_ref()), &txn).unwrap();
        tm.mark_committed(txn.txn_id);
    }

    // 2. Delete 50 keys and commit.
    for i in 0u32..50 {
        let k = i.to_be_bytes();
        let txn = tm.begin();
        idx.delete(&(k.as_ref()), &txn).unwrap();
        tm.mark_committed(txn.txn_id);
    }

    // 3. Run vacuum. Since all transactions committed, it should reclaim 50 records.
    let removed = idx.vacuum(&tm).unwrap();
    assert_eq!(removed, 50);

    // 4. Verify data is still visible for the 50 live keys.
    let results: Vec<_> = idx
        .range::<std::ops::RangeFull>(.., &tm.begin())
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 50);
}

#[test]
fn compact_prevents_split() {
    let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
    let idx = make_index();

    for i in 0u32..80 {
        let k = i.to_be_bytes();
        let v = vec![0xAA; 40]; // padding to fill page
        let txn = tm.begin();
        idx.insert(&(k.as_ref()), &(v.as_ref()), &txn).unwrap();
        tm.mark_committed(txn.txn_id);
    }

    let root_before = idx.root_page_id();

    for i in 0u32..40 {
        let k = i.to_be_bytes();
        let txn = tm.begin();
        idx.delete(&(k.as_ref()), &txn).unwrap();
        tm.mark_committed(txn.txn_id);
    }

    let txn = tm.begin();
    let k = 999u32.to_be_bytes();
    let v = vec![0xBB; 40];
    idx.insert(&(k.as_ref()), &(v.as_ref()), &txn).unwrap();
    tm.mark_committed(txn.txn_id);

    let root_after = idx.root_page_id();
    assert_eq!(
        root_before, root_after,
        "Split should have been avoided via compaction"
    );

    let result = idx.get(&(k.as_ref()), &tm.begin()).unwrap();
    assert!(result.is_some(), "Inserted record should be readable");
}
// ── ValueTooLarge guard ──────────────────────────────────────────────

#[test]
fn insert_and_update_reject_oversized_value() {
    let idx = make_index();
    let k: &[u8] = b"key";
    let oversized = vec![0xFFu8; MAX_VALUE_SIZE + 1];

    // insert should reject
    let err = idx.insert(&k, &oversized.as_slice(), &auto()).unwrap_err();
    assert!(matches!(err, IndexError::ValueTooLarge { size, max }
        if size == MAX_VALUE_SIZE + 1 && max == MAX_VALUE_SIZE));

    // insert a small value so we have something to update
    let v: &[u8] = b"small";
    idx.insert(&k, &v, &auto()).unwrap();

    // update should also reject
    let err = idx.update(&k, &oversized.as_slice(), &auto()).unwrap_err();
    assert!(matches!(err, IndexError::ValueTooLarge { size, max }
        if size == MAX_VALUE_SIZE + 1 && max == MAX_VALUE_SIZE));
}

// ── WAL: physiological Insert logging brings the flush gate to life ───────

#[test]
fn insert_logs_record_stamps_page_lsn_and_gate_flushes() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("wal_insert.db");
    let wal = make_wal(dir.path());
    let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
    let pool = make_pool(disk, wal.clone());
    let (index, root) =
        BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

    // `create` logs nothing, so the LSN counter starts at 1 (LSN 0 reserved).
    assert_eq!(wal.next_lsn(), 1);

    // Two in-place inserts on the same leaf → two Insert records (LSN 1, 2).
    index.insert(&(&b"a"[..]), &(&b"1"[..]), &auto()).unwrap();
    index.insert(&(&b"b"[..]), &(&b"2"[..]), &auto()).unwrap();
    assert_eq!(wal.next_lsn(), 3, "each insert appends a record");

    // The leaf page must carry the latest insert's LSN (set_lsn under the latch).
    let leaf = pool.fetch_page(root).unwrap();
    assert_eq!(crate::page::page_lsn(&leaf[..]), 2, "page LSN stamped");
    drop(leaf);

    // Flushing the dirty leaf must drive the WAL durable through that page LSN
    // (the WAL-before-page gate firing on a real, non-zero LSN).
    pool.flush_all_pages().unwrap();
    assert_eq!(wal.flushed_lsn(), Some(2), "gate flushed WAL to page LSN");
}

#[test]
fn delete_emits_one_setxmax() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("wal_insert.db");
    let wal = make_wal(dir.path());
    let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
    let pool = make_pool(disk, wal.clone());
    let (index, root) =
        BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

    let txn = auto();
    let key: &[u8] = b"a";
    let val: &[u8] = b"1";
    index.insert(&key, &val, &auto()).unwrap();

    let n = wal.next_lsn();
    assert_eq!(n, 2);
    index.delete(&key, &txn).unwrap();
    assert!(wal.next_lsn() == n + 1, "delete appends one record");

    // The leaf page must carry the latest delete's LSN (set_lsn under the latch).
    let leaf = pool.fetch_page(root).unwrap();
    assert_eq!(crate::page::page_lsn(&leaf[..]), 2, "page LSN stamped");
    drop(leaf);

    // Flushing the dirty leaf must drive the WAL durable through that page LSN
    // (the WAL-before-page gate firing on a real, non-zero LSN).
    pool.flush_all_pages().unwrap();
    let mut it = WalIterator::new(dir.path().join("wal")).unwrap();
    it.next_record();
    let rec = it.next_record().unwrap().unwrap();
    assert!(rec.entry_type == WalRecordType::SetXMax);
    assert!(rec.txn_id == txn.txn_id);
    assert!(rec.blocks.len() == 1);
    assert!(rec.blocks[0].page_id == root);
    let data = rec.blocks[0].data.unwrap();
    assert!(u16::from_le_bytes(data[0..2].try_into().unwrap()) == 0);
    assert!(u64::from_le_bytes(data[2..10].try_into().unwrap()) == txn.txn_id);
}

#[test]
fn update_emits_setxmax_then_insert_in_lsn_order() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("wal_insert.db");
    let wal = make_wal(dir.path());
    let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
    let pool = make_pool(disk, wal.clone());
    let (index, root) =
        BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

    let txn = auto();
    let key: &[u8] = b"a";
    let val: &[u8] = b"1";
    let new_val: &[u8] = b"2";
    index.insert(&key, &val, &auto()).unwrap();

    let n = wal.next_lsn();
    assert_eq!(n, 2);

    index.update(&key, &new_val, &txn).unwrap();
    assert!(wal.next_lsn() == n + 2, "update appends two records");

    // The leaf page must carry the latest delete's LSN (set_lsn under the latch).
    let leaf = pool.fetch_page(root).unwrap();
    assert_eq!(crate::page::page_lsn(&leaf[..]), n + 1, "page LSN stamped");
    drop(leaf);

    pool.flush_all_pages().unwrap();
    let mut it = WalIterator::new(dir.path().join("wal")).unwrap();
    it.next_record();

    let rec = it.next_record().unwrap().unwrap(); // SetXmax (LSN 2)
    assert_eq!(rec.entry_type, WalRecordType::SetXMax);
    assert_eq!(rec.txn_id, txn.txn_id);
    let xmax_lsn = rec.lsn;

    let rec = it.next_record().unwrap().unwrap(); // Insert (LSN 3)
    assert_eq!(rec.entry_type, WalRecordType::Insert);
    let ins_lsn = rec.lsn;

    assert!(
        xmax_lsn < ins_lsn,
        "SetXmax must be logged before the new Insert"
    );
}

// ── Crash Harness Recovery ──────────────────────────────────────
#[test]
fn crash_victim_uncommitted_insert_invisible_after_reopen() {
    let dir = tempdir().unwrap();
    {
        let (pool, _wal, tm, index) = build_db(dir.path());
        let victim = tm.begin();
        index
            .insert(&(&b"ghost"[..]), &(&b"boo"[..]), &victim)
            .unwrap();
        pool.flush_all_pages().unwrap();
        // crash: tuple drops; victim's Insert already durable via the gate, but no Commit record
    }
    let (_pool, _wal, tm, index) = reopen_db(dir.path());
    let reader = tm.begin();
    assert!(index.get(&(&b"ghost"[..]), &reader).unwrap().is_none());
    let all: Vec<(Vec<u8>, Vec<u8>)> = index.range(.., &reader).map(|r| r.unwrap()).collect();
    assert!(all.is_empty());
}

#[test]
fn clog_reconstructed_after_crash() {
    let dir = tempdir().unwrap();
    let committed_id;
    let aborted_id;
    let inflight_id;
    {
        let (pool, wal, tm, index) = build_db(dir.path());

        let c = tm.begin();                       // (1) committed — durable Commit
        committed_id = c.txn_id;
        index.insert(&(&b"c"[..]), &(&b"1"[..]), &c).unwrap();
        wal.log_commit(c.txn_id).unwrap();
        tm.mark_committed(c.txn_id);

        let a = tm.begin();                       // (2) explicitly aborted — durable Abort
        aborted_id = a.txn_id;
        index.insert(&(&b"a"[..]), &(&b"2"[..]), &a).unwrap();
        wal.log_abort(a.txn_id).unwrap();
        tm.mark_aborted(a.txn_id);

        let v = tm.begin();                       // (3) in-flight victim — Insert only
        inflight_id = v.txn_id;
        index.insert(&(&b"v"[..]), &(&b"3"[..]), &v).unwrap();

        wal.flush_up_to(wal.next_lsn()).unwrap();
        pool.flush_all_pages().unwrap();
        // crash
    }

    let (_pool, _wal, tm, _index) = reopen_db(dir.path());

    assert!(tm.is_committed(committed_id));
    assert!(tm.is_aborted(aborted_id));
    assert!(tm.is_aborted(inflight_id));          // in-flight presumed aborted
    assert!(!tm.is_committed(inflight_id));

    // presumed-commit: active set empty post-recovery, so every recovered id is below
    // global_xmin — not-explicitly-aborted ⇒ Committed; did-work-but-unsettled ⇒ Aborted.
    assert_eq!(tm.settled_status(committed_id), TransactionStatus::Committed);
    assert_eq!(tm.settled_status(inflight_id), TransactionStatus::Aborted);
}