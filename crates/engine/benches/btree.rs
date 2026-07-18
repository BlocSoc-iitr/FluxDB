//! B+Tree / engine benchmark suite, driven through the **high-level `Engine` API**
//! (auto-commit ops + `TxnHandle` batches), not the raw `BTreeIndex`.
//!
//! ## CPU vs fsync isolation (the load-bearing decision)
//!
//! A single `fsync` (~ms) dwarfs every CPU signal (~ns–µs), so the two are never
//! mixed in one number:
//!
//! - **`cpu/*` benches run on tmpfs (`/dev/shm`)** — `fsync` is memory-backed and
//!   effectively free, so what's left is descent / split / alloc / lock CPU.
//!   Writes are also batched under one `TxnHandle` (one commit) so even that one
//!   flush is amortized.
//! - **`durable/*` benches run on a real disk** and use **auto-commit** (one txn,
//!   one `fsync` per op) — they measure commit latency and group-commit batching.
//!
//! `cpu/insert_batched` vs `durable/insert_autocommit` is the same work under the
//! two regimes; the gap between them *is* the per-commit fsync tax.
//!
//! ## API gap
//!
//! `Engine` exposes no range/scan and no snapshot control, so the scan benches
//! (`cpu/range_full_scan`, `cpu/scan_active`) drop to the low-level
//! `BTreeIndex` directly. That gap is a finding, not a harness choice.

use common::{EngineError, Key, MAX_PAGE_SIZE, Value};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use db_core::transaction::{Snapshot, Transaction};
use db_core::transaction_manager::TransactionManager;
use engine::Engine;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use storage::buffer_pool::manager::BufferPoolManager;
use storage::disk::DiskManager;
use storage::index::BTreeIndex;
use storage::wal::Wal;
use tempfile::TempDir;

// ── Engine builders ──────────────────────────────────────────────────────────

/// Engine on tmpfs — fsync is memory-backed → CPU isolated.
fn cpu_engine<K: Key, V: Value>() -> (Engine<K, V>, TempDir) {
    let dir = tempfile::tempdir_in("/dev/shm").unwrap();
    let e = Engine::<K, V>::create(dir.path()).unwrap();
    (e, dir)
}

/// Engine on a real disk — fsync hits the device → durability measured.
fn disk_engine<K: Key, V: Value>() -> (Engine<K, V>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::<K, V>::create(dir.path()).unwrap();
    (e, dir)
}

/// tmpfs engine pre-loaded with `n` rows under one committed txn.
fn cpu_engine_committed(n: u32) -> (Engine<u32, u32>, TempDir) {
    let (e, dir) = cpu_engine::<u32, u32>();
    let mut t = e.begin();
    for i in 0..n {
        t.insert(&i, &(i.wrapping_mul(7))).unwrap();
    }
    t.commit().unwrap();
    (e, dir)
}

// ── Low-level index builder (scans only — no Engine range API) ───────────────

fn scan_index(n: u32) -> (Arc<BTreeIndex<u32, u32>>, Arc<TransactionManager>, TempDir) {
    let dir = tempfile::tempdir_in("/dev/shm").unwrap();
    let wal = Arc::new(Wal::new(dir.path().join("wal.log")).unwrap());
    let disk = Arc::new(DiskManager::new(dir.path().join("d.db"), MAX_PAGE_SIZE).unwrap());
    let pool = Arc::new(BufferPoolManager::new(disk, wal.clone()));
    let (idx, _) = BTreeIndex::<u32, u32>::create(pool, wal).unwrap();
    let tm = Arc::new(TransactionManager::new());
    let t = tm.begin();
    for i in 0..n {
        idx.insert(&i, &(i.wrapping_mul(7)), &t).unwrap();
    }
    tm.mark_committed(t.txn_id);
    (Arc::new(idx), tm, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// CPU-BOUND (tmpfs — fsync free)
// ═══════════════════════════════════════════════════════════════════════════

fn bench_get(c: &mut Criterion) {
    let (e, _dir) = cpu_engine_committed(4_000); // pool-fit → warm descent
    let mut group = c.benchmark_group("cpu");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("get_random_many", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            let mut k = 1u32;
            for _ in 0..1000 {
                k = k.wrapping_mul(2654435761) % 4_000;
                if e.get(&black_box(k)).unwrap().is_some() {
                    hits += 1;
                }
            }
            black_box(hits)
        })
    });
    group.finish();
}

fn bench_insert_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("insert_batched_1k", |b| {
        b.iter_batched(
            || cpu_engine::<u32, u32>(),
            |(e, _dir)| {
                let mut t = e.begin();
                for i in 0u32..1000 {
                    t.insert(&black_box(i), &(i.wrapping_mul(7))).unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    // Random key order — non-rightmost splits + scattered descent, unlike the
    // append-only pattern above. Odd multiplier is a bijection on u32 → keys distinct.
    group.bench_function("insert_rand_1k", |b| {
        b.iter_batched(
            || cpu_engine::<u32, u32>(),
            |(e, _dir)| {
                let mut t = e.begin();
                for i in 0u32..1000 {
                    let k = i.wrapping_mul(2654435761);
                    t.insert(&black_box(k), &(k.wrapping_mul(7))).unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_update_delete_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("update_batched_1k", |b| {
        b.iter_batched(
            || cpu_engine_committed(1500),
            |(e, _dir)| {
                let mut t = e.begin();
                for k in 0u32..1000 {
                    t.update(&k, &(k.wrapping_mul(11))).unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("delete_batched_1k", |b| {
        b.iter_batched(
            || cpu_engine_committed(1500),
            |(e, _dir)| {
                let mut t = e.begin();
                for k in 0u32..1000 {
                    t.delete(&k).unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_range_full_scan(c: &mut Criterion) {
    let (idx, tm, _dir) = scan_index(4_000);
    let txn = tm.begin();
    let mut group = c.benchmark_group("cpu");
    group.throughput(Throughput::Elements(4_000));
    group.bench_function("range_full_scan", |b| {
        b.iter(|| {
            let count = idx
                .range::<std::ops::RangeFull>(.., &txn)
                .map(|r| r.unwrap())
                .fold(0, |acc, (_k, _v)| black_box(acc + 1));
            black_box(count)
        })
    });
    group.finish();
}

/// Reader snapshot carrying `active_len` synthetic in-flight txn IDs (top half of
/// the u64 range so they never collide with the small insert IDs → records stay
/// visible but every `is_visible` searches the whole active set to a miss).
fn scan_txn(tm: &Arc<TransactionManager>, active_len: u64) -> Transaction {
    let active: Vec<u64> = (0..active_len).map(|i| u64::MAX / 2 + 1 + i).collect();
    let snap = Snapshot {
        xmin: 1,
        xmax: u64::MAX,
        active: Arc::from(active),
        aborted: Arc::from([]),
    };
    Transaction::new(u64::MAX - 1, snap, tm.clone())
}

fn bench_scan_active(c: &mut Criterion) {
    let (idx, tm, _dir) = scan_index(4_000);
    let mut group = c.benchmark_group("cpu/scan_active");
    for a in [0u64, 16, 64, 256] {
        let txn = scan_txn(&tm, a);
        group.bench_with_input(BenchmarkId::from_parameter(a), &a, |b, _| {
            b.iter(|| {
                let count = idx
                    .range::<std::ops::RangeFull>(.., &txn)
                    .map(|r| r.unwrap())
                    .fold(0, |acc, (_k, _v)| black_box(acc + 1));
                black_box(count)
            })
        });
    }
    group.finish();
}

// ── Variable-length (String) keys — the normalized-keys / `compare` axis ─────

fn vkey(i: u32) -> String {
    format!("user:session:token:prefix:{:020}", i)
}

fn bench_varkey(c: &mut Criterion) {
    // Engine path for get + insert.
    let (e, _dir) = {
        let (e, dir) = cpu_engine::<String, String>();
        let mut t = e.begin();
        for i in 0..3_000u32 {
            t.insert(&vkey(i), &format!("v{i}")).unwrap();
        }
        t.commit().unwrap();
        (e, dir)
    };
    let mut group = c.benchmark_group("cpu");
    group.throughput(Throughput::Elements(1000));
    // Keys pre-built: the loop must time the lookup, not `format!` allocation.
    let keys: Vec<String> = (0..3_000u32).map(vkey).collect();
    group.bench_function("varkey_get_many", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            let mut k = 1u32;
            for _ in 0..1000 {
                k = k.wrapping_mul(2654435761) % 3_000;
                if e.get(black_box(&keys[k as usize])).unwrap().is_some() {
                    hits += 1;
                }
            }
            black_box(hits)
        })
    });
    group.bench_function("varkey_insert_batched_1k", |b| {
        b.iter_batched(
            || cpu_engine::<String, String>(),
            |(e, _dir)| {
                let mut t = e.begin();
                for i in 0u32..1000 {
                    t.insert(&vkey(i.wrapping_mul(2654435761)), &format!("v{i}"))
                        .unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// ── Concurrent insert (tmpfs, auto-commit) — index/WAL-buffer contention ─────

const CONCURRENT_BATCH: u32 = 2_000;

fn bench_insert_concurrent(c: &mut Criterion) {
    let max_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut counts = Vec::new();
    let mut t = 1usize;
    while t < max_threads {
        counts.push(t);
        t *= 2;
    }
    counts.push(max_threads);
    counts.push(max_threads * 2);

    let mut group = c.benchmark_group("cpu/insert_concurrent");
    group.throughput(Throughput::Elements(CONCURRENT_BATCH as u64));
    for &nt in &counts {
        group.bench_with_input(BenchmarkId::from_parameter(nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = cpu_engine::<u32, u32>();
                    let e = Arc::new(e);
                    let nt = nt as u32;
                    let chunk = CONCURRENT_BATCH / nt;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for tid in 0..nt {
                            let e = Arc::clone(&e);
                            s.spawn(move || {
                                let lo = tid * chunk;
                                let hi = if tid == nt - 1 {
                                    CONCURRENT_BATCH
                                } else {
                                    lo + chunk
                                };
                                for k in lo..hi {
                                    e.insert(&k, &k.wrapping_mul(7)).unwrap();
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

// ── Contended update (tmpfs) — Wait-Die + conflict retry, no fsync noise ─────

const CONFLICT_BATCH: u32 = 1_000;

fn bench_update_contended(c: &mut Criterion) {
    let nt = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut group = c.benchmark_group("cpu/update_contended");
    group.throughput(Throughput::Elements(CONFLICT_BATCH as u64));
    group.sample_size(10);
    for &hot in &[1u32, 8, 64] {
        group.bench_with_input(BenchmarkId::from_parameter(hot), &hot, |b, &hot| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = cpu_engine_committed(hot);
                    let e = Arc::new(e);
                    let done = AtomicU64::new(0);
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for _ in 0..nt {
                            let e = Arc::clone(&e);
                            let done = &done;
                            s.spawn(move || {
                                loop {
                                    let n = done.fetch_add(1, Relaxed);
                                    if n >= CONFLICT_BATCH as u64 {
                                        break;
                                    }
                                    let key = (n as u32) % hot;
                                    let val = n as u32;
                                    // Retry on conflict (loser of first-writer-wins).
                                    // KeyNotFound is NOT retried: a live committed key
                                    // must never look missing (fixed as findings B2) —
                                    // the panic keeps this bench an invariant check.
                                    loop {
                                        let mut t = e.begin();
                                        match t.update(&key, &val) {
                                            Ok(()) => match t.commit() {
                                                Ok(()) => break,
                                                Err(_) => continue,
                                            },
                                            Err(EngineError::TransactionConflict) => {
                                                t.abort();
                                            }
                                            Err(e) => panic!("unexpected: {e:?}"),
                                        }
                                    }
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

// ── Concurrent reads + mixed read/write (tmpfs — contention, not fsync) ──────

const READ_BATCH: u32 = 20_000;
const MIXED_BATCH: u32 = 8_000;

fn thread_counts() -> Vec<usize> {
    let max = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut v = Vec::new();
    let mut t = 1;
    while t < max {
        v.push(t);
        t *= 2;
    }
    v.push(max);
    v.push(max * 2);
    v
}

// Pure reads, N threads sharing one Engine. Shows how (and whether) reads scale.
fn bench_concurrent_read(c: &mut Criterion) {
    let (e, _dir) = cpu_engine_committed(4_000);
    let e = Arc::new(e);
    let mut group = c.benchmark_group("cpu/concurrent_read");
    group.throughput(Throughput::Elements(READ_BATCH as u64));
    for &nt in &thread_counts() {
        group.bench_with_input(BenchmarkId::from_parameter(nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let chunk = READ_BATCH / nt as u32;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for tid in 0..nt as u32 {
                            let e = Arc::clone(&e);
                            s.spawn(move || {
                                let mut k = tid.wrapping_add(1);
                                for _ in 0..chunk {
                                    k = k.wrapping_mul(2654435761) % 4_000;
                                    black_box(e.get(&k).unwrap());
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

// Concurrent full scans: N threads each scan the whole tree at once, sharing
// one index + one read snapshot. Shows whether read-only scans scale, or whether
// concurrent readers contend (CLOG read-lock atomic, per-page shard Mutex).
fn bench_concurrent_scan(c: &mut Criterion) {
    let (idx, tm, _dir) = scan_index(4_000);
    let txn = tm.begin(); // one shared read snapshot for all threads
    let mut group = c.benchmark_group("cpu/concurrent_scan");
    for &nt in &[1usize, 2, 3, 4, 8, 16] {
        group.throughput(Throughput::Elements((nt as u64) * 4_000));
        group.bench_with_input(BenchmarkId::from_parameter(nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for _ in 0..nt {
                            let idx = Arc::clone(&idx);
                            let txn = &txn;
                            s.spawn(move || {
                                let count = idx
                                    .range::<std::ops::RangeFull>(.., txn)
                                    .map(|r| r.unwrap())
                                    .fold(0u64, |a, _| a + 1);
                                black_box(count);
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

// 90% reads / 10% writes. Two write modes:
//   - autocommit: every write is its own txn (begin+commit per write)
//   - longtxn:    each thread holds ONE txn for all its writes, commits once
// Writers insert fresh disjoint keys (no write conflict) so this measures the
// read/write interplay + txn overhead, not Wait-Die.
fn mixed(c: &mut Criterion, name: &str, longtxn: bool) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(MIXED_BATCH as u64));
    for &nt in &thread_counts() {
        group.bench_with_input(BenchmarkId::from_parameter(nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = cpu_engine_committed(4_000);
                    let e = Arc::new(e);
                    let next = AtomicU32::new(4_000);
                    let chunk = MIXED_BATCH / nt as u32;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for tid in 0..nt as u32 {
                            let e = Arc::clone(&e);
                            let next = &next;
                            s.spawn(move || {
                                let mut k = tid.wrapping_add(1);
                                if longtxn {
                                    let mut wt = e.begin();
                                    for i in 0..chunk {
                                        if i % 10 == 9 {
                                            let nk = next.fetch_add(1, Relaxed);
                                            wt.insert(&nk, &nk.wrapping_mul(7)).unwrap();
                                        } else {
                                            k = k.wrapping_mul(2654435761) % 4_000;
                                            black_box(e.get(&k).unwrap());
                                        }
                                    }
                                    wt.commit().unwrap();
                                } else {
                                    for i in 0..chunk {
                                        if i % 10 == 9 {
                                            let nk = next.fetch_add(1, Relaxed);
                                            e.insert(&nk, &nk.wrapping_mul(7)).unwrap();
                                        } else {
                                            k = k.wrapping_mul(2654435761) % 4_000;
                                            black_box(e.get(&k).unwrap());
                                        }
                                    }
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

fn bench_mixed_autocommit(c: &mut Criterion) {
    mixed(c, "cpu/mixed_autocommit", false);
}
fn bench_mixed_longtxn(c: &mut Criterion) {
    mixed(c, "cpu/mixed_longtxn", true);
}

// ═══════════════════════════════════════════════════════════════════════════
// DURABILITY (real disk — fsync measured)
// ═══════════════════════════════════════════════════════════════════════════

const DURABLE_N: u32 = 200; // per-op fsync is ~ms; keep the batch small

fn bench_autocommit(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable");
    group.throughput(Throughput::Elements(DURABLE_N as u64));
    group.sample_size(10);
    group.bench_function("insert_autocommit", |b| {
        b.iter_batched(
            || disk_engine::<u32, u32>(),
            |(e, _dir)| {
                for i in 0u32..DURABLE_N {
                    e.insert(&black_box(i), &(i.wrapping_mul(7))).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("update_autocommit", |b| {
        b.iter_batched(
            || {
                let (e, dir) = disk_engine::<u32, u32>();
                let mut t = e.begin();
                for i in 0..DURABLE_N {
                    t.insert(&i, &(i.wrapping_mul(7))).unwrap();
                }
                t.commit().unwrap();
                (e, dir)
            },
            |(e, _dir)| {
                for k in 0u32..DURABLE_N {
                    e.update(&black_box(k), &(k.wrapping_mul(11))).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Group commit: concurrent auto-commit inserts on disk. More threads should
// raise commits/s if leader/follower fsync batching coalesces flushes.
const COMMIT_BATCH: u32 = 400;

fn bench_commit_latency(c: &mut Criterion) {
    let max_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut group = c.benchmark_group("durable/commit_latency");
    group.throughput(Throughput::Elements(COMMIT_BATCH as u64));
    group.sample_size(10);
    let mut counts = vec![1usize];
    let mut t = 2;
    while t <= max_threads {
        counts.push(t);
        t *= 2;
    }
    for &nt in &counts {
        group.bench_with_input(BenchmarkId::from_parameter(nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = disk_engine::<u32, u32>();
                    let e = Arc::new(e);
                    let nt = nt as u32;
                    let chunk = COMMIT_BATCH / nt;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for tid in 0..nt {
                            let e = Arc::clone(&e);
                            s.spawn(move || {
                                let lo = tid * chunk;
                                let hi = if tid == nt - 1 {
                                    COMMIT_BATCH
                                } else {
                                    lo + chunk
                                };
                                for k in lo..hi {
                                    e.insert(&k, &k.wrapping_mul(7)).unwrap();
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

// Eviction fsync cliff: one big committed txn (one commit fsync) whose working
// set far exceeds the 80-frame pool, forcing the WAL-before-page flush gate to
// fsync per evicted dirty page. Isolates the *eviction* fsync from commit fsync.
fn bench_evict_throughput(c: &mut Criterion) {
    const N: u32 = 12_000;
    let mut group = c.benchmark_group("durable/evict_throughput");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);
    group.bench_function("insert_12k_evicting", |b| {
        b.iter_batched(
            || disk_engine::<u32, u32>(),
            |(e, _dir)| {
                let mut t = e.begin();
                for k in 0u32..N {
                    t.insert(&black_box(k), &k.wrapping_mul(7)).unwrap();
                }
                t.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_insert_batched,
    bench_update_delete_batched,
    bench_range_full_scan,
    bench_scan_active,
    bench_varkey,
    bench_insert_concurrent,
    bench_update_contended,
    bench_concurrent_read,
    bench_concurrent_scan,
    bench_mixed_autocommit,
    bench_mixed_longtxn,
    bench_autocommit,
    bench_commit_latency,
    bench_evict_throughput,
);
criterion_main!(benches);
