use common::MAX_PAGE_SIZE;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use db_core::transaction::Snapshot;
use db_core::transaction::Transaction;
use db_core::transaction_manager::TransactionManager;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use storage::buffer_pool::manager::BufferPoolManager;
use storage::disk::DiskManager;
use storage::index::BTreeIndex;
use storage::wal::Wal;
use tempfile::TempDir;

fn make_wal(dir: &Path) -> Arc<Wal> {
    Arc::new(Wal::new(dir.join("wal.log")).unwrap())
}

fn make_pool(disk: Arc<DiskManager>, wal: Arc<Wal>) -> Arc<BufferPoolManager> {
    Arc::new(BufferPoolManager::new(disk, wal))
}

fn auto() -> Transaction {
    static TM_LOCK: OnceLock<Arc<TransactionManager>> = OnceLock::new();
    let tm = TM_LOCK
        .get_or_init(|| Arc::new(TransactionManager::new()))
        .clone();
    static TEST_TXN_ID: AtomicU64 = AtomicU64::new(1);
    Transaction::new(TEST_TXN_ID.fetch_add(1, Relaxed), Snapshot::latest(), tm)
}

fn setup_index(n: u32) -> (BTreeIndex<u32, u32>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let wal = make_wal(dir.path());
    let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
    let pool = make_pool(disk, wal.clone());
    let (index, _) = BTreeIndex::create(pool, wal).unwrap();
    for i in 0..n {
        index.insert(&i, &(i * 7), &auto()).unwrap();
    }
    (index, dir)
}

fn bench_get_random(c: &mut Criterion) {
    let (idx, _dir) = setup_index(4_000);
    let txn = auto();
    let mut i = 0u32;
    c.bench_function("get_random", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            let k = black_box(i.wrapping_mul(2654435761) % 4_000);
            black_box(idx.get(&k, &txn).unwrap())
        })
    });
}

fn bench_insert_sequential(c: &mut Criterion) {
    c.bench_function("insert_sequential", |b| {
        b.iter_batched(
            || setup_index(0),
            |(idx, _dir)| {
                for i in 0u32..1000 {
                    let k = black_box(i);
                    let v = i * 7;
                    black_box(idx.insert(&k, &v, &auto()).unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_insert_random(c: &mut Criterion) {
    c.bench_function("insert_random", |b| {
        b.iter_batched(
            || setup_index(0),
            |(idx, _dir)| {
                for i in 0u32..1000 {
                    let k = black_box(i.wrapping_mul(2654435761));
                    let v = i * 7;
                    black_box(idx.insert(&k, &v, &auto()).unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_range_full_scan(c: &mut Criterion) {
    let (idx, _dir) = setup_index(4_000);
    let txn = auto();
    c.bench_function("range_full_scan", |b| {
        b.iter(|| {
            let count = idx
                .range::<std::ops::RangeFull>(.., &txn)
                .map(|r| r.unwrap())
                .fold(0, |acc, (_k, _v)| black_box(acc + 1));
            black_box(count)
        })
    });
}

criterion_group!(
    benches,
    bench_get_random,
    bench_insert_sequential,
    bench_insert_random,
    bench_range_full_scan
);
criterion_main!(benches);
