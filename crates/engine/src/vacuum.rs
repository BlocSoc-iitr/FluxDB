//! Background autovacuum thread.
//!
//! One `std::thread` owned by [`crate::Db`]. Each tick it sweeps if enough
//! dead versions have piled up ([`AUTOVACUUM_DEAD_THRESHOLD`]). Exits on
//! `Shutdown` without a final sweep — vacuum is maintenance, not durability.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use common::{AUTOVACUUM_COST_DELAY, AUTOVACUUM_DEAD_THRESHOLD, Key, Value};

use crate::engine::Engine;

pub(crate) enum Msg {
    /// Stop after the current tick, then exit.
    Shutdown,
}

enum Wake {
    /// Interval elapsed with no message → run the trigger check.
    Tick,
    /// `Shutdown` received, or all senders dropped → exit.
    Shutdown,
}

fn next_wake(rx: &Receiver<Msg>, interval: Duration) -> Wake {
    match rx.recv_timeout(interval) {
        Err(RecvTimeoutError::Timeout) => Wake::Tick,
        // A dropped sender shouldn't happen with the Db design, but fold it into
        // Shutdown so we never busy-spin on repeated `Disconnected`.
        Ok(Msg::Shutdown) | Err(RecvTimeoutError::Disconnected) => Wake::Shutdown,
    }
}

/// The autovacuum loop. Owns a strong `Arc<Engine>` on purpose: `Db`'s
/// deterministic `join()` guarantees this returns and releases the Arc.
pub(crate) fn run<K, V>(engine: Arc<Engine<K, V>>, rx: Receiver<Msg>, interval: Duration)
where
    K: Key,
    V: Value,
{
    // Unlike the checkpointer there is no final pass on shutdown: a sweep is
    // pure maintenance, and the next open can always run one.
    while let Wake::Tick = next_wake(&rx, interval) {
        if engine.transaction_manager.dead_versions() >= AUTOVACUUM_DEAD_THRESHOLD {
            engine.transaction_manager.take_dead_versions();
            if !do_vacuum(&engine, &rx) {
                break;
            }
        }
    }

    // Returning here drops this thread's `Arc<Engine>` clone.
}

/// Runs one paced sweep, napping `AUTOVACUUM_COST_DELAY` per page the last
/// batch dirtied — busy batches earn longer naps, clean batches none.
/// Returns `false` if `Shutdown` arrived mid-sweep — the pacer consumed the
/// message, so the caller must exit instead of waiting for a second one.
/// Errors are logged, not propagated: a failed sweep must not kill the thread.
fn do_vacuum<K, V>(engine: &Engine<K, V>, rx: &Receiver<Msg>) -> bool
where
    K: Key,
    V: Value,
{
    let _span = tracing::info_span!("autovacuum").entered();
    let mut shutting_down = false;
    let result = engine.vacuum_paced(|touched| {
        let nap = AUTOVACUUM_COST_DELAY.saturating_mul(touched.try_into().unwrap_or(u32::MAX));
        match next_wake(rx, nap) {
            Wake::Tick => true,
            Wake::Shutdown => {
                shutting_down = true;
                false
            }
        }
    });
    match result {
        Ok(dead) => tracing::debug!(dead_versions = dead, "vacuum sweep complete"),
        Err(e) => tracing::error!(error = %e, "vacuum sweep failed"),
    }
    !shutting_down
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::AUTOVACUUM_DEAD_THRESHOLD;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Instant;
    use tempfile::TempDir;

    type TestEngine = Engine<&'static [u8], &'static [u8]>;

    /// Issue done-when (test 9): dead tuples get reclaimed with no `vacuum()`
    /// call anywhere — the worker must notice the threshold on its own tick.
    #[test]
    fn autovacuum_fires_without_manual_trigger() {
        let dir = TempDir::new().unwrap();
        let engine: Arc<TestEngine> = Arc::new(Engine::create(dir.path()).unwrap());

        // The worker under test, on a fast tick instead of the production one.
        let (tx, rx) = mpsc::channel();
        let worker_engine = Arc::clone(&engine);
        let worker = thread::spawn(move || run(worker_engine, rx, Duration::from_millis(25)));

        // Two batched transactions, not 2×N autocommits: dead versions are
        // counted at op time, so the deletes alone cross the threshold.
        let n = AUTOVACUUM_DEAD_THRESHOLD as usize + 1;
        let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i:05}").into_bytes()).collect();
        let mut txn = engine.begin();
        for key in &keys {
            txn.insert(&key.as_slice(), &b"v".as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let mut txn = engine.begin();
        for key in &keys {
            txn.delete(&key.as_slice()).unwrap();
        }
        txn.commit().unwrap();

        // The worker may consume the counter mid-delete (op-time counting), so
        // poll for both sweep signals: the counter back below the threshold AND
        // a published horizon, which only a completed full pass produces.
        let tm = &engine.transaction_manager;
        let deadline = Instant::now() + Duration::from_secs(20);
        while tm.dead_versions() >= AUTOVACUUM_DEAD_THRESHOLD || tm.vacuum_horizon() == 0 {
            assert!(
                Instant::now() < deadline,
                "autovacuum never swept: dead={}, horizon={}",
                tm.dead_versions(),
                tm.vacuum_horizon()
            );
            thread::sleep(Duration::from_millis(20));
        }

        tx.send(Msg::Shutdown).unwrap();
        worker.join().unwrap();
    }

    /// Issue done-when (test 10): one long-open transaction pins `global_xmin`
    /// — reclamation visibly stalls and `oldest_active_txn_age` names the
    /// culprit; finishing the transaction lets the same sweep reclaim it all.
    #[test]
    fn pinned_global_xmin_stalls_reclamation_and_is_observable() {
        let dir = TempDir::new().unwrap();
        let engine: TestEngine = Engine::create(dir.path()).unwrap();

        // The culprit: opened before the deletes, then "forgotten".
        let pin = engine.begin();

        const N: usize = 100;
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:03}").into_bytes()).collect();
        let mut txn = engine.begin();
        for key in &keys {
            txn.insert(&key.as_slice(), &b"v".as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let mut txn = engine.begin();
        for key in &keys {
            txn.delete(&key.as_slice()).unwrap();
        }
        txn.commit().unwrap();

        // Pinned: a full sweep completes but reclaims nothing — every dead
        // version postdates the culprit's snapshot — and the stat reports it.
        assert_eq!(engine.vacuum().unwrap(), 0);
        let stalled_horizon = engine.transaction_manager.vacuum_horizon();
        let age = engine
            .transaction_manager
            .oldest_active_txn_age()
            .expect("open transaction must be reported");
        assert!(age > Duration::ZERO);

        // Finish the culprit: the very same sweep now reclaims everything and
        // the horizon moves past where the stall held it.
        pin.commit().unwrap();
        assert_eq!(engine.transaction_manager.oldest_active_txn_age(), None);
        assert_eq!(engine.vacuum().unwrap(), N);
        assert!(engine.transaction_manager.vacuum_horizon() > stalled_horizon);
    }
}
