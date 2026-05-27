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
    sync::RwLock,
};

use crate::transaction::{Snapshot, Transaction};
use concurrency::lock_manager::LockManager;
use std::sync::Arc;

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
#[derive(Debug)]
pub struct TransactionManager {
    pub next_txn_id: AtomicU64,
    pub clog: RwLock<HashMap<u64, TransactionStatus>>,
    pub active_txns: RwLock<HashSet<u64>>,
    pub lock_manager: Arc<LockManager>,
}

impl TransactionManager {
    /// Creates a new, empty `TransactionManager`.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            clog: RwLock::new(HashMap::new()),
            active_txns: RwLock::new(HashSet::new()),
            lock_manager: Arc::new(LockManager::new()),
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

    pub fn truncate_clog(&self, horizon: u64) {
        let mut clog = self.clog.write().unwrap();
        clog.retain(|&txn_id, status| txn_id >= horizon || *status == TransactionStatus::Active);
    }

    /// Begins a new transaction synchronously, establishing its `Snapshot`.
    ///
    /// This method is thread-safe and enforces strict lock ordering to prevent
    /// race conditions. It acquires a write-lock on the active set *before*
    /// generating the new ID, ensuring that concurrent snapshot generators
    /// always see a consistent state.
    pub fn begin(self: &Arc<Self>) -> Transaction {
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
            tm: self.clone(),
            lock_manager: self.lock_manager.clone(),
            locked_pages: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Marks a transaction as committed in the CLOG and removes it from the active set.
    ///
    /// Once committed, the transaction's writes become eligible for visibility
    /// to new snapshots.
    pub fn commit(&self, txn_id: u64) {
        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Committed);
        self.active_txns.write().unwrap().remove(&txn_id);
    }

    /// Marks a transaction as aborted in the CLOG and removes it from the active set.
    ///
    /// **Note**: The caller (e.g., storage engine) is responsible for rolling back
    /// any physical writes or undo logs associated with this transaction before
    /// or after calling this method.
    pub fn abort(&self, txn_id: u64) {
        // TODO: The storage/undo layer must rollback writes before calling this.
        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Aborted);
        self.active_txns.write().unwrap().remove(&txn_id);
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

        tm.commit(txn.txn_id);

        assert!(!tm.is_active(txn.txn_id));
        assert!(tm.is_committed(txn.txn_id));
        assert!(!tm.is_aborted(txn.txn_id));
    }

    #[test]
    fn test_abort_transaction() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let txn = tm.begin();

        tm.abort(txn.txn_id);

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
        tm.commit(txn2.txn_id);

        let snap = tm.get_snapshot();

        assert_eq!(snap.xmin, txn1.txn_id); // txn1 is still oldest
        assert!(!snap.active.contains(&txn2.txn_id)); // txn2 committed
        assert!(snap.active.contains(&txn1.txn_id));
        assert!(snap.active.contains(&txn3.txn_id));
        assert_eq!(snap.active.len(), 2);
    }
}
