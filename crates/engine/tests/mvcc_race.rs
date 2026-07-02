//! Concurrency stress reproducer for the MVCC visibility race in `update`/`get`.
//!
//! ## What it catches
//!
//! Under first-writer-wins MVCC, a key that was inserted, committed, and never
//! deleted has **exactly one** visible version to any transaction. So `update`
//! and `get` on such a key must return `Ok`/`Some` or a legal `WriteConflict` —
//! never `KeyNotFound`/`None`. A miss means `find_visible_slot` saw *zero*
//! visible versions for a live key.
//!
//! ## Design fragility this workload stresses
//!
//! When one hot key accumulates so many versions that they fill a whole leaf,
//! `split_leaf_ly` has no key-change split point and falls back to splitting that
//! single key's version chain **across two pages** (smo.rs: `if split_at == 0`).
//! Readers of the key are then routed by rightlink correction (`high_key == KEY`)
//! along the chain of same-key pages. The concern: a version visible to a reader
//! but stranded on a page the correction walks past would read as missing.
//!
//! To make that state reachable, the test **pins `global_xmin` low** (one
//! long-lived read txn that never commits) so `compact()` cannot reclaim the
//! superseded versions — they pile up and force the cross-page split. Without the
//! pin, compaction keeps the page small and the split never happens.
//!
//! ## Why `std::thread`, not `loom`
//!
//! Probabilistic guard, not exhaustive proof: it runs the real `Engine`/WAL/
//! index under genuine contention. A pass means "this run kept the invariant,"
//! not "no interleaving can ever break it." `loom` would be exhaustive but
//! cannot run the real disk/WAL path.
//!
//! ## Status — currently GREEN (mechanism unconfirmed)
//!
//! On this branch the invariant **holds** in every run so far: the newly written
//! version marches to the rightmost same-key page, and rightlink correction is a
//! loop that follows the whole `high_key == KEY` chain to reach it — so nothing
//! is stranded in steady state. A `KeyNotFound` was observed once under the
//! Criterion `update_contended` harness, but this controlled reproducer has not
//! reproduced it; the likely remaining trigger is a narrow split-in-progress
//! window (INCOMPLETE_SPLIT, downlink not yet propagated) that steady-state load
//! doesn't reliably hit.
//!
//! Kept `#[ignore]` because it is a multi-minute stress run. Run explicitly:
//!
//! ```bash
//! cargo test -p engine --test mvcc_race -- --ignored --nocapture
//! ```
//!
//! Keep it: if a future change reintroduces a live-key visibility miss, this is
//! the guard positioned to catch it.

use engine::{Engine, EngineError, IndexError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use tempfile::TempDir;

/// Prefer tmpfs so per-commit fsync is memory-backed (millions of ops feasible);
/// fall back to a real temp dir if `/dev/shm` is unavailable.
fn fast_engine() -> (Engine<u32, u32>, TempDir) {
    let dir = tempfile::tempdir_in("/dev/shm")
        .or_else(|_| tempfile::tempdir())
        .unwrap();
    let e = Engine::<u32, u32>::create(dir.path()).unwrap();
    (e, dir)
}

const KEY: u32 = 0; // one hot key: max chain churn, widest scan window

#[test]
#[ignore = "multi-minute MVCC stress guard; run explicitly or in nightly CI"]
fn update_never_returns_keynotfound_for_live_key() {
    let ops: u64 = std::env::var("MVCC_RACE_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400_000);

    let (e, _dir) = fast_engine();
    // KEY is committed live and never deleted for the whole test → always live.
    e.insert(&KEY, &0).unwrap();
    let e = Arc::new(e);

    // ── The load-bearing ingredient: pin `global_xmin` low with one long-lived
    // read txn that never commits. This blocks `compact()` from reclaiming the
    // hot key's superseded versions, so they ACCUMULATE until they fill a whole
    // leaf. A leaf full of one key's versions has no key-change split point, so
    // `split_leaf_ly` takes its cross-page fallback (smo.rs) and strands part of
    // the chain on the left page — which rightlink correction (high_key == KEY)
    // then routes every reader past. Without this pin, compaction keeps the page
    // small and the pathological split never happens.
    let _pin = e.begin();

    let bug = Arc::new(AtomicBool::new(false));
    let ctr = Arc::new(AtomicU64::new(1));

    std::thread::scope(|s| {
        // ── Prober threads: observe the live key via update and get. Neither
        // may ever report the key missing.
        for tid in 0..8u32 {
            let e = Arc::clone(&e);
            let bug = Arc::clone(&bug);
            let ctr = Arc::clone(&ctr);
            s.spawn(move || {
                for i in 0..ops {
                    if bug.load(Relaxed) {
                        return;
                    }
                    // Alternate update-probe and get-probe.
                    if (i ^ tid as u64) & 1 == 0 {
                        let v = ctr.fetch_add(1, Relaxed) as u32;
                        match e.update(&KEY, &v) {
                            Ok(()) | Err(EngineError::TransactionConflict) => {}
                            Err(EngineError::Index(IndexError::KeyNotFound)) => {
                                bug.store(true, Relaxed);
                                return;
                            }
                            Err(other) => panic!("unexpected error from update: {other:?}"),
                        }
                    } else {
                        match e.get(&KEY) {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                // live key read as absent — same visibility race
                                bug.store(true, Relaxed);
                                return;
                            }
                            Err(other) => panic!("unexpected error from get: {other:?}"),
                        }
                    }
                }
            });
        }
    });

    assert!(
        !bug.load(Relaxed),
        "a live, committed, never-deleted key was reported missing (KeyNotFound / None) — \
         MVCC visibility race: is_aborted flip across the find_visible_slot scan"
    );
}
