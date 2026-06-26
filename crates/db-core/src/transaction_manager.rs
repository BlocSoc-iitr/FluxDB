//! Thread-safe transaction lifecycle and snapshot state management.
//!
//! The `TransactionManager` acts as the global coordinator for MVCC architecture.
//! It issues sequentially increasing transaction IDs, builds accurate point-in-time
//! `Snapshot`s for read isolation, and maintains the Commit Log (CLOG) that tracks
//! whether a transaction is Active, Committed, or Aborted.
//!
//! ## Concurrency and Thread Safety
//!
//! Acquiring consistent snapshots in a multi-threaded codebase requires strict lock
//! ordering. When a transaction calls `begin()`, the manager acquires a write-lock
//! on the `active_txns` set *before* fetching the next global transaction ID.
//! This guarantees that if a concurrent transaction is busy acquiring a snapshot,
//! no new transaction ID can slip between the ID increment and insertion into the
//! active set, avoiding critical race conditions that would break snapshot isolation.
//!
//! ## MVCC State Transitions
//!
//! 1. **Active**: The transaction begins. It receives a `txn_id` and a `Snapshot` and
//!    is tracked inside `active_txns`. Its writes remain invisible to others.
//! 2. **Committed**: The transaction finishes successfully. It is recorded as `Committed`
//!    in the CLOG and safely removed from `active_txns`. Other new snapshots will see its writes.
//! 3. **Aborted**: The transaction is explicitly rolled back (or fails a conflict check).
//!    It is recorded as `Aborted` in the CLOG and removed from `active_txns`. Its writes
//!    will remain invisible to all future transactions and can be garbage collected.

use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Condvar, Mutex, RwLock},
};

use crate::transaction::{Snapshot, Transaction};

/// Represents the deterministic final state of a transaction.
///
/// A transaction starts as `Active`, and then transitions to either `Committed`
/// (success) or `Aborted` (failure/rollback). These states are tracked in
/// the Commit Log (CLOG).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

/// Global tracking for MVCC isolation rules and the commit log (CLOG).
///
/// the `TransactionManager` is the single source of truth for:
/// 1. **Transaction ID generation**: Monotonically increasing `u64` IDs.
/// 2. **Snapshot Creation**: Tracking which transactions are active to build
///    consistent point-in-time views.
/// 3. **Commit Log (CLOG)**: Recording the final status of every transaction
///    to resolve visibility during record scans.
//  Each entry is (settled: Mutex<bool>, Condvar). The condvar sleeps on its
//  own mutex so wait_until_settled never holds the waiters map lock while
//  sleeping — avoiding lock-order inversion with commit/abort.
type WaiterEntry = Arc<(Mutex<bool>, Condvar)>;

#[derive(Debug)]
pub struct TransactionManager {
    pub next_txn_id: AtomicU64,
    pub clog: RwLock<HashMap<u64, TransactionStatus>>,
    pub vacuum_horizon: AtomicU64,
    pub active_txns: RwLock<HashSet<u64>>,
    waiters: Mutex<HashMap<u64, WaiterEntry>>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    /// Creates a new, empty `TransactionManager`.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            clog: RwLock::new(HashMap::new()),
            vacuum_horizon: AtomicU64::new(0),
            active_txns: RwLock::new(HashSet::new()),
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the minimum `txn_id` currently in the active set (the global
    /// minimum transaction ID that is still executing).
    ///
    /// This is used to determine the lower bound for visibility checks in
    /// read operations (e.g., in B+-tree scans), ensuring that transactions
    /// which have not yet begun do not affect the visibility of committed data.
    pub fn global_xmin(&self) -> u64 {
        let active = self.active_txns.read().unwrap();
        active
            .iter()
            .min()
            .copied()
            .unwrap_or_else(|| self.next_txn_id.load(Acquire))
    }

    /// Publishes the horizon of a COMPLETED full vacuum sweep. `fetch_max` so a
    /// slower concurrent sweep can never move the horizon backward. (DESIGN §8.4)
    pub fn publish_vacuum_horizon(&self, horizon: u64) {
        self.vacuum_horizon.fetch_max(horizon, AcqRel);
    }

    /// The oldest-active-txn-id captured at the start of the most recent
    /// completed full sweep (0 until the first post-restart sweep completes).
    pub fn vacuum_horizon(&self) -> u64 {
        self.vacuum_horizon.load(Acquire)
    }

    /// Truncates the CLOG, removing entries older than `horizon`.
    /// Now the removal of entries has been made two-tier, such that the entries that are 
    /// committed and their txn_id lying below committed_horizon are deleted directly because
    /// they have already been committed into the tree. However, the transactions that aborted are removed
    /// once a successful sweep returns a vacuum_horizon value that can be used to check if the rows 
    /// concerning that aborted entry in the CLOG have been deleted or not. This physical vacuum confirms 
    /// that the records of aborted transactions are gone, allowing the truncation of aborted entries in CLOG. 
    pub fn truncate_clog(&self, committed_horizon: u64) {
        let aborted_horizon = self.vacuum_horizon();
        let mut clog = self.clog.write().unwrap();
        clog.retain(|&txn_id, status| match status {
            TransactionStatus::Active => true,
            TransactionStatus::Committed => txn_id >= committed_horizon,
            TransactionStatus::Aborted => txn_id >= aborted_horizon,
        });
    }

    /// Begins a new transaction synchronously, establishing its `Snapshot`.
    ///
    /// This method is thread-safe and enforces strict lock ordering to prevent
    /// race conditions. It acquires a write-lock on the active set *before*
    /// generating the new ID, ensuring that concurrent snapshot generators
    /// always see a consistent state.
    pub fn begin(self: &std::sync::Arc<Self>) -> Transaction {
        // Write-lock active_txns FIRST to prevent race conditions with get_snapshot.
        // We must lock before fetching TXN_ID to ensure that no snapshot is
        // generated in between ID creation and active set insertion.
        let mut active = self.active_txns.write().unwrap();

        let txn_id = self.next_txn_id.fetch_add(1, AcqRel);
        active.insert(txn_id);

        let xmin = *active.iter().min().unwrap_or(&txn_id);
        let xmax = self.next_txn_id.load(Acquire);
        let active_vec: Vec<u64> = active.iter().cloned().collect();

        drop(active);

        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Active);

        Transaction {
            txn_id,
            snapshot: Snapshot {
                xmin,
                xmax,
                active: active_vec,
            },
            tm: std::sync::Arc::clone(self),
            wrote_anything: false,
        }
    }

    /// Marks a transaction as committed in the CLOG and removes it from the active set.
    ///
    /// Once committed, the transaction's writes become eligible for visibility
    /// to new snapshots. Any threads waiting on this transaction via
    /// `wait_until_settled` are woken up.
    pub fn mark_committed(&self, txn_id: u64) {
        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Committed);
        self.active_txns.write().unwrap().remove(&txn_id);
        self.notify_waiters(txn_id);
    }

    /// Marks a transaction as aborted in the CLOG and removes it from the active set.
    ///
    /// Any threads waiting on this transaction via `wait_until_settled` are woken up.
    pub fn mark_aborted(&self, txn_id: u64) {
        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Aborted);
        self.active_txns.write().unwrap().remove(&txn_id);
        self.notify_waiters(txn_id);
    }

    /// Block the calling thread until `blocking_txn` commits or aborts.
    ///
    /// Returns immediately if the transaction has already settled. Otherwise
    /// sleeps on a per-transaction condvar that is woken by commit/abort.
    ///
    /// Lock ordering: never hold the `waiters` mutex while calling `is_active`
    /// (which locks `active_txns`). `commit`/`abort` lock `active_txns` and may
    /// subsequently lock `waiters` when notifying, so keeping `is_active` calls
    /// outside the `waiters` lock avoids potential lock-order inversion.
    pub fn wait_until_settled(&self, blocking_txn: u64) {
        // Fast path: already settled, nothing to do.
        if !self.is_active(blocking_txn) {
            return;
        }

        // Install the waiter entry. Do NOT call is_active while holding the
        // waiters mutex — that acquires active_txns and inverts the lock order
        // used by commit/abort (active_txns → waiters).
        let entry: WaiterEntry = {
            let mut waiters = self.waiters.lock().unwrap();
            Arc::clone(
                waiters
                    .entry(blocking_txn)
                    .or_insert_with(|| Arc::new((Mutex::new(false), Condvar::new()))),
            )
        }; // waiters lock released here — safe to call is_active again

        // Missed-wakeup guard: the transaction may have settled between the
        // fast-path check above and installing our entry. notify_waiters would
        // have found no entry at that point and done nothing. Check now:
        // - if settled is already true, wait_while returns immediately below
        // - if is_active is false but settled not yet true, clean up and return
        if !self.is_active(blocking_txn) {
            self.notify_waiters(blocking_txn);
            return;
        }

        // Sleep on the entry's own mutex — no map lock held during the wait.
        let (settled_lock, cv) = &*entry;
        let settled = settled_lock.lock().unwrap();
        drop(cv.wait_while(settled, |s| !*s).unwrap());
    }

    fn notify_waiters(&self, txn_id: u64) {
        let entry = self.waiters.lock().unwrap().remove(&txn_id);
        if let Some(entry) = entry {
            let (settled_lock, cv) = &*entry;
            *settled_lock.lock().unwrap() = true;
            cv.notify_all();
        }
    }

    /// Returns `true` if the transaction is recorded as `Committed` in the CLOG.
    pub fn is_committed(&self, txn_id: u64) -> bool {
        if txn_id == 0 {
            return true;
        }
        self.clog.read().unwrap().get(&txn_id) == Some(&TransactionStatus::Committed)
    }

    /// Returns `true` if the transaction is recorded as `Aborted` in the CLOG.
    pub fn is_aborted(&self, txn_id: u64) -> bool {
        self.clog.read().unwrap().get(&txn_id) == Some(&TransactionStatus::Aborted)
    }

    /// Returns `true` if the transaction is currently in the active set.
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_txns.read().unwrap().contains(&txn_id)
    }

    /// Generates a "latest" snapshot from the current manager state.
    ///
    /// This captures the current `xmin`, `xmax`, and active set. It is typically
    /// used for ad-hoc reads or by `begin()` to initialize a transaction's view.
    pub fn get_snapshot(&self) -> Snapshot {
        // Read lock active_txns to guarantee consistency
        let active = self.active_txns.read().unwrap();
        let xmax = self.next_txn_id.load(Acquire);
        Snapshot {
            xmin: *active.iter().min().unwrap_or(&xmax),
            xmax,
            active: active.iter().cloned().collect(),
        }
    }

    pub fn settled_status(&self, txn_id: u64) -> TransactionStatus {
        if self.is_committed(txn_id) {
            TransactionStatus::Committed
        } else if self.is_aborted(txn_id) {
            TransactionStatus::Aborted
        } else if txn_id < self.global_xmin() {
            TransactionStatus::Committed
        } else {
            TransactionStatus::Active
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();

        assert_eq!(txn.snapshot.active.len(), 1);
        assert_eq!(txn.snapshot.active[0], txn.txn_id);
        assert!(tm.is_active(txn.txn_id));
        assert!(!tm.is_committed(txn.txn_id));
        assert!(!tm.is_aborted(txn.txn_id));
    }

    #[test]
    fn test_commit_transaction() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();

        tm.mark_committed(txn.txn_id);

        assert!(!tm.is_active(txn.txn_id));
        assert!(tm.is_committed(txn.txn_id));
        assert!(!tm.is_aborted(txn.txn_id));
    }

    #[test]
    fn test_abort_transaction() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();

        tm.mark_aborted(txn.txn_id);

        assert!(!tm.is_active(txn.txn_id));
        assert!(!tm.is_committed(txn.txn_id));
        assert!(tm.is_aborted(txn.txn_id));
    }

    #[test]
    fn test_snapshot_empty_active() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let snap = tm.get_snapshot();

        // When empty, xmin should equal xmax
        assert_eq!(snap.xmin, snap.xmax);
        assert!(snap.active.is_empty());
    }

    #[test]
    fn test_snapshot_with_multiple_active() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn1 = tm.begin();
        let txn2 = tm.begin();

        let snap = tm.get_snapshot();

        assert_eq!(snap.xmin, txn1.txn_id); // txn1 is the oldest active
        assert!(snap.xmax > txn2.txn_id);
        assert!(snap.active.contains(&txn1.txn_id));
        assert!(snap.active.contains(&txn2.txn_id));
        assert_eq!(snap.active.len(), 2);
    }

    #[test]
    fn test_snapshot_with_commits_in_middle() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn1 = tm.begin();
        let txn2 = tm.begin();
        let txn3 = tm.begin();

        // Commit txn2 in the middle
        tm.mark_committed(txn2.txn_id);

        let snap = tm.get_snapshot();

        assert_eq!(snap.xmin, txn1.txn_id); // txn1 is still oldest
        assert!(!snap.active.contains(&txn2.txn_id)); // txn2 committed
        assert!(snap.active.contains(&txn1.txn_id));
        assert!(snap.active.contains(&txn3.txn_id));
        assert_eq!(snap.active.len(), 2);
    }

    // ── wait_until_settled ────────────────────────────────────────────────

    #[test]
    fn wait_until_settled_returns_immediately_if_already_committed() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();
        tm.mark_committed(txn.txn_id);
        // Must return without blocking — transaction already settled.
        tm.wait_until_settled(txn.txn_id);
    }

    #[test]
    fn wait_until_settled_returns_immediately_if_already_aborted() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();
        tm.mark_aborted(txn.txn_id);
        tm.wait_until_settled(txn.txn_id);
    }

    #[test]
    fn wait_until_settled_unblocks_on_commit() {
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tm = Arc::new(TransactionManager::new());
        let blocker = tm.begin();
        let blocker_id = blocker.txn_id;

        let tm_clone = Arc::clone(&tm);
        let waiter = thread::spawn(move || {
            tm_clone.wait_until_settled(blocker_id);
        });

        // Give the waiter thread time to reach wait_until_settled and sleep.
        thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished(), "waiter should be blocked");

        tm.mark_committed(blocker_id);

        let deadline = Instant::now() + Duration::from_secs(2);
        while !waiter.is_finished() {
            assert!(
                Instant::now() < deadline,
                "waiter did not unblock after commit"
            );
            thread::sleep(Duration::from_millis(5));
        }
        waiter.join().expect("waiter panicked");
    }

    #[test]
    fn wait_until_settled_unblocks_on_abort() {
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tm = Arc::new(TransactionManager::new());
        let blocker = tm.begin();
        let blocker_id = blocker.txn_id;

        let tm_clone = Arc::clone(&tm);
        let waiter = thread::spawn(move || {
            tm_clone.wait_until_settled(blocker_id);
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished(), "waiter should be blocked");

        tm.mark_aborted(blocker_id);

        let deadline = Instant::now() + Duration::from_secs(2);
        while !waiter.is_finished() {
            assert!(
                Instant::now() < deadline,
                "waiter did not unblock after abort"
            );
            thread::sleep(Duration::from_millis(5));
        }
        waiter.join().expect("waiter panicked");
    }

    #[test]
    fn wait_until_settled_multiple_waiters_all_unblock() {
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tm = Arc::new(TransactionManager::new());
        let blocker = tm.begin();
        let blocker_id = blocker.txn_id;

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let tm_clone = Arc::clone(&tm);
                thread::spawn(move || {
                    tm_clone.wait_until_settled(blocker_id);
                })
            })
            .collect();

        thread::sleep(Duration::from_millis(20));
        tm.mark_committed(blocker_id);

        let deadline = Instant::now() + Duration::from_secs(2);
        for handle in handles {
            while !handle.is_finished() {
                assert!(Instant::now() < deadline, "a waiter did not unblock");
                thread::sleep(Duration::from_millis(5));
            }
            handle.join().expect("waiter panicked");
        }
    }

    #[test]
    fn waiter_map_cleaned_up_after_settle() {
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tm = Arc::new(TransactionManager::new());
        let blocker = tm.begin();
        let blocker_id = blocker.txn_id;

        let tm_clone = Arc::clone(&tm);
        let waiter = thread::spawn(move || {
            tm_clone.wait_until_settled(blocker_id);
        });

        // Give the waiter time to register its entry in the map.
        thread::sleep(Duration::from_millis(20));

        tm.mark_committed(blocker_id);

        let deadline = Instant::now() + Duration::from_secs(2);
        while !waiter.is_finished() {
            assert!(Instant::now() < deadline, "waiter did not unblock");
            thread::sleep(Duration::from_millis(5));
        }
        waiter.join().expect("waiter panicked");

        // Entry must be removed from the map after settle — no memory leak.
        assert!(!tm.waiters.lock().unwrap().contains_key(&blocker_id));
    }

    #[test]
    fn test_truncate_clog_retains_aborted() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn_commit = tm.begin();
        let txn_abort = tm.begin();
        let txn_active = tm.begin();

        tm.mark_committed(txn_commit.txn_id);
        tm.mark_aborted(txn_abort.txn_id);

        // Truncate past all of them
        let horizon = txn_active.txn_id + 1;
        tm.truncate_clog(horizon);

        let clog = tm.clog.read().unwrap();
        // Committed should be dropped
        assert!(!clog.contains_key(&txn_commit.txn_id));
        // Aborted should be retained to prevent visible-aborted-data bug
        assert_eq!(
            clog.get(&txn_abort.txn_id),
            Some(&TransactionStatus::Aborted)
        );
        // Active is retained automatically because it's active
        assert_eq!(
            clog.get(&txn_active.txn_id),
            Some(&TransactionStatus::Active)
        );
    }
}
