//! Deterministic check of the first-writer-wins contract on a stale snapshot.
//!
//! Project model (CLAUDE.md): "first-writer-wins — if two transactions try to
//! write the same key, the second gets `WriteConflict`."
//!
//! Scenario:
//!   1. K = 0, committed.
//!   2. T begins (snapshot sees K=0).
//!   3. C updates K = 100 and commits — C started AFTER T's snapshot.
//!   4. T updates K = 200 on its now-stale snapshot.
//!
//! C committed first, so T is the second writer → T's update MUST fail with a
//! conflict, and the durable value MUST stay 100. If T's update returns `Ok`
//! and the final value is 200, C's committed write was silently lost.

use engine::{Engine, EngineError};
use tempfile::TempDir;

fn engine() -> (Engine<u32, u32>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::<u32, u32>::create(dir.path()).unwrap();
    (e, dir)
}

/// The decisive corruption check: after the scenario the key must have EXACTLY
/// one coherent committed value. It can be either:
///   - 100, if T correctly conflicted (first-writer-wins), or
///   - 200, if the engine chose last-writer-wins and T's commit truly applied.
/// The bug produces a THIRD, incoherent outcome: T's update returns `Ok` and
/// commits, yet the read returns 100 — proving T's committed write is a phantom
/// shadowed by a second live version left on the key.
#[test]
fn stale_snapshot_update_stays_coherent() {
    let (e, _dir) = engine();
    e.insert(&1, &0).unwrap();

    let mut t = e.begin();
    e.update(&1, &100).unwrap(); // C commits after T's snapshot

    let t_ok = matches!(t.update(&1, &200), Ok(())) && t.commit().is_ok();
    let final_val = u32::from_le_bytes(e.get(&1).unwrap().unwrap().try_into().unwrap());

    println!("T.update+commit ok = {t_ok}, final value = {final_val}");

    if t_ok {
        // T believes it committed 200 → read-your-committed-write demands 200.
        assert_eq!(
            final_val, 200,
            "CORRUPTION: T's update returned Ok and committed, but the key reads \
             {final_val}. Two live versions coexist (C's 100 shadows T's 200) — \
             the single-visible-version invariant is broken."
        );
    } else {
        // T conflicted → C's value stands.
        assert_eq!(final_val, 100, "T conflicted, so C's committed 100 must stand");
    }
}

/// Same setup, but assert on the returned error kind: the second writer should
/// see a conflict, never a silent success. Split out so the lost-update
/// assertion above and the API-contract assertion here fail independently.
#[test]
fn stale_snapshot_update_must_report_conflict() {
    let (e, _dir) = engine();
    e.insert(&1, &0).unwrap();

    let mut t = e.begin();
    e.update(&1, &100).unwrap();

    match t.update(&1, &200) {
        Err(EngineError::TransactionConflict) => { /* correct: first-writer-wins */ }
        Err(other) => panic!("expected TransactionConflict, got {other:?}"),
        Ok(()) => panic!(
            "stale second writer got Ok — first-writer-wins violated \
             (should be WriteConflict/TransactionConflict)"
        ),
    }
}
