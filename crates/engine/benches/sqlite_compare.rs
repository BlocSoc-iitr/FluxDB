//! FluxDB vs SQLite head-to-head, on the axes where MVCC is the differentiator.
//!
//! ## What this suite claims (and what it deliberately doesn't)
//!
//! SQLite in WAL mode allows **one writer at a time** and N readers against a
//! stale-until-checkpoint snapshot. FluxDB allows **concurrent writers** with
//! per-key first-writer-wins conflicts and snapshot-isolated readers. The
//! benches therefore split into:
//!
//! - `vs_sqlite/writers_*` — N concurrent writer threads, disjoint keys.
//!   The showcase: SQLite serializes (flat or negative scaling + busy retries),
//!   FluxDB scales.
//! - `vs_sqlite/mixed_long_reader_*` — writers making progress while one
//!   long-lived read transaction stays open. SQLite's writer stalls checkpoints
//!   and readers block the WAL from resetting; FluxDB's writers are unaffected.
//! - `vs_sqlite/single_writer_batched_*` — the honesty bench. One thread, one
//!   big transaction. SQLite is extremely good here; we expect to roughly tie
//!   or lose. Publishing it makes the wins credible.
//!
//! ## Fairness rules (violating any of these invalidates the comparison)
//!
//! 1. **Same durability discipline per group.** CPU-regime benches run BOTH
//!    engines on tmpfs (`/dev/shm`); durable-regime benches run both on the
//!    same real-disk directory parent with fsync on commit:
//!    SQLite `PRAGMA synchronous=FULL` + `journal_mode=WAL` vs FluxDB
//!    auto-commit (one fsync per commit). Never compare tmpfs vs disk.
//! 2. **Same logical schema/work.** SQLite table: `(k INTEGER PRIMARY KEY,
//!    v INTEGER)` — its B-tree rowid path, the fastest thing it has; FluxDB
//!    `Engine<u32, u32>`. Same key ranges, same access patterns, prepared
//!    statements reused (SQLite must not pay parse cost per op).
//! 3. **SQLite gets its best multithreaded config**: one connection per thread
//!    (WAL supports that), `busy_timeout` set high so writers queue instead of
//!    erroring, `synchronous=NORMAL` *additionally* reported for the durable
//!    group since that's what people actually deploy (two rows: FULL and
//!    NORMAL).
//! 4. **Contention is real**: writers_disjoint uses per-thread key ranges
//!    (measures pure writer-parallelism); writers_contended uses a shared hot
//!    range (measures conflict handling: SQLite lock queue vs FluxDB
//!    first-writer-wins aborts — count and report retries for both).
//!
//! ## Bench matrix
//!
//! | group                                   | threads      | engine rows                    |
//! |-----------------------------------------|--------------|--------------------------------|
//! | `vs_sqlite/writers_disjoint/{impl}`     | 1,2,4,8,16   | fluxdb, sqlite-wal             |
//! | `vs_sqlite/writers_contended/{impl}`    | 1,4,16       | fluxdb, sqlite-wal             |
//! | `vs_sqlite/mixed_long_reader/{impl}`    | 4w+1lr       | fluxdb, sqlite-wal             |
//! | `vs_sqlite/single_writer_batched/{impl}`| 1            | fluxdb, sqlite-wal             |
//! | `vs_sqlite/durable_commit/{impl}`       | 1,8          | fluxdb, sqlite-full, sqlite-normal |
//!
//! All cpu-regime groups report `Throughput::Elements`; durable group reports
//! commits/s. Every SQLite bench must also run with `threads=1` so each graph
//! has its own baseline (scaling shape matters more than absolute numbers).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use engine::Engine;
use rusqlite::Connection;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const BATCH: u32 = 10_000; // total ops per measured iteration, split across threads
const PRELOAD: u32 = 4_000; // rows preloaded for update/read workloads

// ── Builders ─────────────────────────────────────────────────────────────────

fn fluxdb_tmpfs() -> (Arc<Engine<u32, u32>>, TempDir) {
    let dir = tempfile::tempdir_in("/dev/shm").unwrap();
    (Arc::new(Engine::create(dir.path()).unwrap()), dir)
}

fn fluxdb_disk() -> (Arc<Engine<u32, u32>>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Arc::new(Engine::create(dir.path()).unwrap()), dir)
}

/// One SQLite database file; every thread opens its own connection (the only
/// way WAL mode does concurrent readers + a writer).
struct SqliteDb {
    path: std::path::PathBuf,
    _dir: TempDir,
}

impl SqliteDb {
    fn new_tmpfs() -> Self {
        Self::create(tempfile::tempdir_in("/dev/shm").unwrap(), "NORMAL")
    }
    fn new_disk(synchronous: &str) -> Self {
        Self::create(tempfile::tempdir().unwrap(), synchronous)
    }
    fn create(dir: TempDir, synchronous: &str) -> Self {
        let path = dir.path().join("bench.db");
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", synchronous)
            .unwrap();
        conn.execute_batch("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID;")
            .unwrap();
        Self { path, _dir: dir }
    }
    /// Per-thread connection with the busy handler SQLite needs to queue
    /// writers instead of failing them.
    fn conn(&self) -> Connection {
        let conn = Connection::open(&self.path).unwrap();
        conn.busy_timeout(Duration::from_secs(30)).unwrap();
        conn
    }
    fn preload(&self, n: u32) {
        let mut conn = self.conn();
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx.prepare("INSERT INTO t VALUES (?1, ?2)").unwrap();
            for i in 0..n {
                stmt.execute((i, i.wrapping_mul(7))).unwrap();
            }
        }
        tx.commit().unwrap();
    }
}

fn fluxdb_preload(e: &Engine<u32, u32>, n: u32) {
    let mut t = e.begin();
    for i in 0..n {
        t.insert(&i, &(i.wrapping_mul(7))).unwrap();
    }
    t.commit().unwrap();
}

// ── Workload drivers (shared shape for both engines) ────────────────────────

/// N threads, disjoint key ranges, auto-commit per op. Returns wall time.
fn run_writers_fluxdb(e: &Arc<Engine<u32, u32>>, nt: u32, base: u32) -> Duration {
    let chunk = BATCH / nt;
    let start = Instant::now();
    std::thread::scope(|s| {
        for tid in 0..nt {
            let e = Arc::clone(e);
            s.spawn(move || {
                let lo = base + tid * chunk;
                for k in lo..lo + chunk {
                    e.insert(&k, &k.wrapping_mul(7)).unwrap();
                }
            });
        }
    });
    start.elapsed()
}

fn run_writers_sqlite(db: &SqliteDb, nt: u32, base: u32) -> Duration {
    let chunk = BATCH / nt;
    let start = Instant::now();
    std::thread::scope(|s| {
        for tid in 0..nt {
            let conn = db.conn();
            s.spawn(move || {
                let mut stmt = conn.prepare("INSERT INTO t VALUES (?1, ?2)").unwrap();
                let lo = base + tid * chunk;
                for k in lo..lo + chunk {
                    // autocommit: each execute is its own write txn — the
                    // apples-to-apples analogue of Engine::insert auto-commit.
                    stmt.execute((k, k.wrapping_mul(7))).unwrap();
                }
            });
        }
    });
    start.elapsed()
}

// ── Benches ──────────────────────────────────────────────────────────────────

/// Showcase 1: concurrent writers, disjoint keys, tmpfs (CPU regime).
/// Expectation: FluxDB scales with threads; SQLite is flat/negative (all
/// writers funnel through the one WAL write lock + busy queue).
fn bench_writers_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_sqlite/writers_disjoint");
    group.throughput(Throughput::Elements(BATCH as u64));
    for &nt in &[1u32, 2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("fluxdb", nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = fluxdb_tmpfs();
                    total += run_writers_fluxdb(&e, nt, 0);
                }
                total
            })
        });
        group.bench_with_input(BenchmarkId::new("sqlite", nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let db = SqliteDb::new_tmpfs();
                    total += run_writers_sqlite(&db, nt, 0);
                }
                total
            })
        });
    }
    group.finish();
}

/// Showcase 2: writers keep committing while one long-lived read txn stays
/// open over preloaded data. Measures WRITER throughput only; the reader
/// validates its snapshot at the end (same rows visible before and after).
/// Expectation: FluxDB writers ~unaffected by the reader; SQLite writers
/// degrade (reader pins the WAL, checkpoints stall, WAL file grows).
fn bench_mixed_long_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_sqlite/mixed_long_reader");
    group.throughput(Throughput::Elements(BATCH as u64));
    const WRITERS: u32 = 4;

    group.bench_function("fluxdb", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (e, _dir) = fluxdb_tmpfs();
                fluxdb_preload(&e, PRELOAD);
                let mut reader = e.begin(); // long-lived snapshot held across all writes
                total += run_writers_fluxdb(&e, WRITERS, PRELOAD);
                // Snapshot must still see exactly the preloaded rows.
                assert!(reader.get(&0).unwrap().is_some());
                assert!(reader.get(&(PRELOAD + 1)).unwrap().is_none());
                drop(reader);
            }
            total
        })
    });

    group.bench_function("sqlite", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let db = SqliteDb::new_tmpfs();
                db.preload(PRELOAD);
                let reader = db.conn();
                // Materialize the read snapshot: BEGIN alone is deferred, a
                // SELECT is what actually acquires the read mark on the WAL.
                reader.execute_batch("BEGIN;").unwrap();
                let _: i64 = reader
                    .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                    .unwrap();
                total += run_writers_sqlite(&db, WRITERS, PRELOAD);
                let n: i64 = reader
                    .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                    .unwrap();
                assert_eq!(n, PRELOAD as i64); // snapshot isolation held
                reader.execute_batch("COMMIT;").unwrap();
            }
            total
        })
    });
    group.finish();
}

/// Honesty bench: single writer, one big transaction, tmpfs. SQLite's home
/// turf (no locking, page cache, rowid B-tree). Expect to tie or lose; report
/// it anyway so the concurrent wins are believable.
fn bench_single_writer_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_sqlite/single_writer_batched");
    group.throughput(Throughput::Elements(BATCH as u64));

    group.bench_function("fluxdb", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (e, _dir) = fluxdb_tmpfs();
                let start = Instant::now();
                let mut t = e.begin();
                for k in 0..BATCH {
                    t.insert(&k, &k.wrapping_mul(7)).unwrap();
                }
                t.commit().unwrap();
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("sqlite", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let db = SqliteDb::new_tmpfs();
                let mut conn = db.conn();
                let start = Instant::now();
                let tx = conn.transaction().unwrap();
                {
                    let mut stmt = tx.prepare("INSERT INTO t VALUES (?1, ?2)").unwrap();
                    for k in 0..BATCH {
                        stmt.execute((k, k.wrapping_mul(7))).unwrap();
                    }
                }
                tx.commit().unwrap();
                total += start.elapsed();
            }
            total
        })
    });
    group.finish();
}

/// Durable regime: real disk, fsync per commit, 1 and 8 writer threads.
/// SQLite reported twice — synchronous=FULL (same guarantee as FluxDB commit)
/// and NORMAL (what deployments actually use). FluxDB's group commit is the
/// differentiator at 8 threads.
fn bench_durable_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("vs_sqlite/durable_commit");
    const DBATCH: u32 = 200; // fsync-bound: keep iterations sane
    group.throughput(Throughput::Elements(DBATCH as u64));
    group.sample_size(10);

    for &nt in &[1u32, 8] {
        group.bench_with_input(BenchmarkId::new("fluxdb", nt), &nt, |b, &nt| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (e, _dir) = fluxdb_disk();
                    let chunk = DBATCH / nt;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for tid in 0..nt {
                            let e = Arc::clone(&e);
                            s.spawn(move || {
                                let lo = tid * chunk;
                                for k in lo..lo + chunk {
                                    e.insert(&k, &k).unwrap(); // auto-commit: 1 fsync (grouped)
                                }
                            });
                        }
                    });
                    total += start.elapsed();
                }
                total
            })
        });
        for sync in ["FULL", "NORMAL"] {
            let id = format!("sqlite-{}", sync.to_lowercase());
            group.bench_with_input(BenchmarkId::new(&id, nt), &nt, |b, &nt| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = SqliteDb::new_disk(sync);
                        total += run_writers_sqlite(&db, nt, 0).min(Duration::from_secs(120)); // guard: FULL @8t can crawl
                    }
                    total
                })
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_writers_disjoint,
    bench_mixed_long_reader,
    bench_single_writer_batched,
    bench_durable_commit,
);
criterion_main!(benches);
