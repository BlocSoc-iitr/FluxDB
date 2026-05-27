//! MVCC transaction primitives — snapshot-based visibility and conflict detection.
//!
//! Every read and write in the B+Tree operates against a [`Transaction`] that
//! bundles the transaction's identity (`txn_id`) with its point-in-time view
//! of the database ([`Snapshot`]). This ensures consistent visibility: a
//! transaction sees exactly the data that was committed before its snapshot
//! was taken, and nothing from in-progress or future transactions.
//!
//! ## Visibility Rule (PostgreSQL Semantics)
//!
//! A record with `(xmin, xmax)` is visible to a transaction holding `snap` if:
//!
//! 1. The creating transaction (`xmin`) is committed from `snap`'s perspective.
//! 2. The record has not been deleted/replaced (`xmax == 0`), OR the deleting
//!    transaction (`xmax`) has NOT committed from `snap`'s perspective.
//!
//! ## Conflict Detection (First-Writer-Wins)
//!
//! Before modifying a record (setting its `xmax`), the caller must check that
//! no other in-progress transaction has already claimed it. If `xmax` is set
//! by an active transaction, the current transaction loses (returns
//! `WriteConflict`). The first transaction to set `xmax` wins.

use crate::transaction_manager::TransactionManager;
use common::LockManagerError;
use concurrency::lock_manager::{LockManager, LockType};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

/// A transaction's identity and its point-in-time view of the database.
///
/// All B+Tree operations (`get`, `insert`, `delete`, `update`, `range`)
/// take a `&Transaction` to determine visibility and ownership.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub txn_id: u64,
    pub snapshot: Snapshot,
    pub tm: Arc<TransactionManager>,
    pub lock_manager: Arc<LockManager>,
    pub locked_pages: Arc<Mutex<HashSet<u64>>>,
}

impl Transaction {
    /// Determines whether a record with `(rec_xmin, rec_xmax)` is visible to
    /// this transaction.
    pub fn is_visible(&self, rec_xmin: u64, rec_xmax: u64) -> bool {
        is_visible(rec_xmin, rec_xmax, &self.snapshot, &self.tm)
    }

    /// Is the given transaction considered committed from this transaction's
    /// perspective?
    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.snapshot.is_committed(txn_id, &self.tm)
    }

    /// Is the given transaction still in-progress from this transaction's
    /// perspective?
    pub fn is_in_progress(&self, txn_id: u64) -> bool {
        self.snapshot.is_in_progress(txn_id, &self.tm)
    }

    /// Acquires a shared lock on the given page.
    pub fn acquire_shared_lock(&self, page_id: u64) -> Result<(), LockManagerError> {
        self.lock_manager
            .acquire_lock(self.txn_id, page_id, LockType::SharedLock)?;
        self.locked_pages.lock().unwrap().insert(page_id);
        Ok(())
    }

    /// Acquires an exclusive lock on the given page.
    pub fn acquire_exclusive_lock(&self, page_id: u64) -> Result<(), LockManagerError> {
        self.lock_manager
            .acquire_lock(self.txn_id, page_id, LockType::ExclusiveLock)?;
        self.locked_pages.lock().unwrap().insert(page_id);
        Ok(())
    }

    /// Upgrades an existing shared lock to an exclusive lock.
    pub fn upgrade_lock(&self, page_id: u64) -> Result<(), LockManagerError> {
        self.lock_manager.upgrade_lock(self.txn_id, page_id)?;
        self.locked_pages.lock().unwrap().insert(page_id);
        Ok(())
    }

    /// Commits the transaction and releases all held locks.
    pub fn commit(&self) {
        self.tm.commit(self.txn_id);
        let pages: Vec<u64> = {
            let mut guard = self.locked_pages.lock().unwrap();
            let p = guard.iter().copied().collect();
            guard.clear();
            p
        };
        for page_id in pages {
            let _ = self.lock_manager.release_lock(self.txn_id, page_id);
        }
    }

    /// Aborts the transaction and releases all held locks.
    pub fn abort(&self) {
        self.tm.abort(self.txn_id);
        let pages: Vec<u64> = {
            let mut guard = self.locked_pages.lock().unwrap();
            let p = guard.iter().copied().collect();
            guard.clear();
            p
        };
        for page_id in pages {
            let _ = self.lock_manager.release_lock(self.txn_id, page_id);
        }
    }
}

/// A point-in-time view of transaction state.
///
/// Captures which transactions were in-progress when the snapshot was taken.
/// Used to determine whether a record's `xmin`/`xmax` represent committed
/// or uncommitted changes.
///
/// # Fields
///
/// - `xmin`: All transactions with ID < `xmin` are guaranteed finished
///   (committed or aborted). Their effects are fully resolved.
/// - `xmax`: All transactions with ID >= `xmax` have not yet started.
///   Their effects are invisible.
/// - `active`: Transaction IDs between `xmin` and `xmax` that were still
///   in-progress when this snapshot was taken. Their uncommitted writes
///   are invisible to the holder of this snapshot.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub active: Vec<u64>,
}

impl Snapshot {
    /// A snapshot that sees all committed data and treats all deletions as
    /// visible. Equivalent to "read the latest state."
    ///
    /// With this snapshot:
    /// - Every `xmin < u64::MAX` → committed → creator visible
    /// - Every `xmax != 0` with `xmax < u64::MAX` → committed → record deleted
    /// - `xmax == 0` → record live
    pub fn latest() -> Self {
        Self {
            xmin: u64::MAX,
            xmax: u64::MAX,
            active: vec![],
        }
    }

    /// Is the given transaction considered committed from this snapshot's
    /// perspective?
    ///
    /// A transaction is committed if:
    /// - The CLOG records it as committed or aborted (authoritative), OR
    /// - Its ID is below `xmin` (finished before snapshot was taken), OR
    /// - Its ID is between `xmin` and `xmax` AND not in the active list.
    pub fn is_committed(&self, txn_id: u64, tm: &TransactionManager) -> bool {
        if txn_id == 0 {
            return true; // txn_id 0 is the "auto" transaction, always committed
        }
        if tm.is_aborted(txn_id) {
            return false;
        }
        if tm.is_committed(txn_id) {
            return true;
        }
        if txn_id < self.xmin {
            return true; // finished before snapshot
        }
        if txn_id >= self.xmax {
            return false; // not yet started
        }
        // Between xmin and xmax: committed if NOT in active list
        !self.active.contains(&txn_id)
    }

    /// Is the given transaction still in-progress from this snapshot's
    /// perspective?
    pub fn is_in_progress(&self, txn_id: u64, tm: &TransactionManager) -> bool {
        if txn_id == 0 {
            return false;
        }
        if tm.is_committed(txn_id) || tm.is_aborted(txn_id) {
            return false;
        }
        if txn_id < self.xmin {
            return false;
        }
        if txn_id >= self.xmax {
            return true; // not yet started → treat as in-progress
        }
        self.active.contains(&txn_id)
    }
}

/// Determines whether a record with `(rec_xmin, rec_xmax)` is visible to
/// a reader holding the given snapshot.
///
/// # Visibility semantics
///
/// A record is visible if:
/// 1. Its creator (`xmin`) has committed from the snapshot's perspective.
/// 2. It has not been deleted (`xmax == 0`), OR the deleter (`xmax`) has
///    NOT committed from the snapshot's perspective (the deletion hasn't
///    "happened" for this reader).
///
/// # Examples
///
/// ```text
/// snap = { xmin: 10, xmax: 20, active: [12, 15] }
///
/// Record (xmin=5,  xmax=0)  → visible   (creator committed, not deleted)
/// Record (xmin=5,  xmax=8)  → invisible (deleted by committed txn 8)
/// Record (xmin=12, xmax=0)  → invisible (creator in-progress, uncommitted)
/// Record (xmin=5,  xmax=15) → visible   (deleter in-progress, deletion not final)
/// Record (xmin=25, xmax=0)  → invisible (creator hasn't started yet)
/// ```
pub fn is_visible(rec_xmin: u64, rec_xmax: u64, snap: &Snapshot, tm: &TransactionManager) -> bool {
    // Step 1: The creating transaction must be committed.
    if !snap.is_committed(rec_xmin, tm) {
        return false;
    }

    // Step 2: If not deleted/replaced, the record is visible.
    if rec_xmax == 0 {
        return true;
    }

    // Step 3: If the deleting transaction committed, the record is gone.
    if snap.is_committed(rec_xmax, tm) {
        return false;
    }

    // The deleter hasn't committed → the deletion hasn't "happened" from
    // our perspective → the record is still visible.
    true
}

/// Determines whether a record version with `(xmin, xmax)` is "definitely dead"
/// and safe to be physically removed from storage.
///
/// A record is vacuumable if:
/// 1. Its creator (`xmin`) aborted (it was never valid).
/// 2. OR it was deleted/replaced by a transaction (`xmax`) that is:
///    - Committed
///    - AND older than the global horizon (no active txn can see the old state).
pub fn is_vacuumable(xmin: u64, xmax: u64, horizon: u64, tm: &TransactionManager) -> bool {
    // 1. Aborted records are always dead.
    if tm.is_aborted(xmin) {
        return true;
    }

    // 2. If not deleted (xmax=0) or the deleter hasn't committed yet, it's live.
    if xmax == 0 || !tm.is_committed(xmax) {
        return false;
    }

    // 3. Deleted by committed txn < horizon: no one will ever see it again.
    xmax < horizon
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(xmin: u64, xmax: u64, active: &[u64]) -> Snapshot {
        Snapshot {
            xmin,
            xmax,
            active: active.to_vec(),
        }
    }

    fn is_vis(xmin: u64, xmax: u64, s: &Snapshot) -> bool {
        let tm = TransactionManager::new();
        is_visible(xmin, xmax, s, &tm)
    }

    fn is_com(s: &Snapshot, txn_id: u64) -> bool {
        let tm = TransactionManager::new();
        s.is_committed(txn_id, &tm)
    }

    fn is_prog(s: &Snapshot, txn_id: u64) -> bool {
        let tm = TransactionManager::new();
        s.is_in_progress(txn_id, &tm)
    }

    // ── Snapshot::latest() ────────────────────────────────────────────────

    #[test]
    fn latest_sees_live_record() {
        let s = Snapshot::latest();
        // xmin=5 committed (5 < MAX), xmax=0 → visible
        assert!(is_vis(5, 0, &s));
    }

    #[test]
    fn latest_hides_deleted_record() {
        let s = Snapshot::latest();
        // xmin=5 committed, xmax=10 committed (10 < MAX) → invisible
        assert!(!is_vis(5, 10, &s));
    }

    #[test]
    fn latest_auto_txn_visible() {
        let s = Snapshot::latest();
        // xmin=0 (auto txn), xmax=0 → visible
        assert!(is_vis(0, 0, &s));
    }

    // ── Committed vs in-progress ─────────────────────────────────────────

    #[test]
    fn committed_creator_visible() {
        let s = snap(10, 20, &[12, 15]);
        // xmin=5 < 10 → committed → visible
        assert!(is_vis(5, 0, &s));
    }

    #[test]
    fn in_progress_creator_invisible() {
        let s = snap(10, 20, &[12, 15]);
        // xmin=12 in active list → not committed → invisible
        assert!(!is_vis(12, 0, &s));
    }

    #[test]
    fn future_creator_invisible() {
        let s = snap(10, 20, &[]);
        // xmin=25 >= xmax=20 → not started → invisible
        assert!(!is_vis(25, 0, &s));
    }

    #[test]
    fn committed_between_xmin_xmax_visible() {
        let s = snap(10, 20, &[12, 15]);
        // xmin=13: between 10 and 20, NOT in active → committed → visible
        assert!(is_vis(13, 0, &s));
    }

    // ── Deletion visibility ──────────────────────────────────────────────

    #[test]
    fn committed_deletion_hides_record() {
        let s = snap(10, 20, &[12, 15]);
        // xmin=5 committed, xmax=8 committed (8 < 10) → deleted → invisible
        assert!(!is_vis(5, 8, &s));
    }

    #[test]
    fn in_progress_deletion_keeps_record_visible() {
        let s = snap(10, 20, &[12, 15]);
        // xmin=5 committed, xmax=15 in active → deletion not final → visible
        assert!(is_vis(5, 15, &s));
    }

    #[test]
    fn future_deletion_keeps_record_visible() {
        let s = snap(10, 20, &[]);
        // xmin=5 committed, xmax=25 >= xmax → future → visible
        assert!(is_vis(5, 25, &s));
    }

    // ── is_committed / is_in_progress ────────────────────────────────────

    #[test]
    fn is_committed_below_xmin() {
        let s = snap(10, 20, &[]);
        assert!(is_com(&s, 5));
    }

    #[test]
    fn is_committed_in_active() {
        let s = snap(10, 20, &[12]);
        assert!(!is_com(&s, 12));
    }

    #[test]
    fn is_committed_between_not_active() {
        let s = snap(10, 20, &[12]);
        assert!(is_com(&s, 15)); // between 10 and 20, not in active
    }

    #[test]
    fn is_in_progress_in_active() {
        let s = snap(10, 20, &[12]);
        assert!(is_prog(&s, 12));
    }

    #[test]
    fn is_in_progress_future() {
        let s = snap(10, 20, &[]);
        assert!(is_prog(&s, 25)); // >= xmax → treat as in-progress
    }

    #[test]
    fn is_in_progress_committed() {
        let s = snap(10, 20, &[]);
        assert!(!is_prog(&s, 5)); // < xmin → finished
    }

    // ── is_vacuumable ───────────────────────────────────────────────────

    #[test]
    fn vacuumable_aborted_creator() {
        let tm = TransactionManager::new();
        tm.abort(5); // xmin=5 aborted
        // Creator aborted -> always dead
        assert!(is_vacuumable(5, 0, 100, &tm));
        assert!(is_vacuumable(5, 15, 10, &tm));
    }

    #[test]
    fn vacuumable_not_deleted_is_live() {
        let tm = TransactionManager::new();
        // xmin=5 committed, xmax=0 -> live
        assert!(!is_vacuumable(5, 0, 100, &tm));
    }

    #[test]
    fn vacuumable_committed_deleter_below_horizon() {
        let tm = TransactionManager::new();
        tm.commit(15); // xmax=15 committed
        // xmax=15 < horizon=20 -> dead
        assert!(is_vacuumable(5, 15, 20, &tm));
    }

    #[test]
    fn vacuumable_committed_deleter_above_horizon_is_live() {
        let tm = TransactionManager::new();
        tm.commit(15); // xmax=15 committed
        // xmax=15 >= horizon=10 -> live (someone might still see the old version)
        assert!(!is_vacuumable(5, 15, 10, &tm));
    }

    #[test]
    fn vacuumable_in_progress_deleter_is_live() {
        let tm = TransactionManager::new();
        // xmax=15 in-progress -> live
        assert!(!is_vacuumable(5, 15, 100, &tm));
    }
}
