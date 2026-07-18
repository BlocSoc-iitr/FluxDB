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

use crossbeam_utils::CachePadded;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
};
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, RwLock},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;

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
pub struct ActiveState {
    pub active: HashMap<u64, (u64, Instant)>,
    pub recent_aborts: BTreeSet<u64>,
}

/// Immutable transaction state published for lock-free snapshot reads.
///
/// Phase 1 publishes the active set and horizon metadata. `aborted` is present
/// but empty until the recent-aborts phase migrates abort ownership out of CLOG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnState {
    /// Sorted in-flight transaction IDs.
    pub active: Arc<[u64]>,
    /// Sorted live aborted transaction IDs. Empty in Phase 1.
    pub aborted: Arc<[u64]>,
    /// Oldest snapshot xmin among active transactions, or `xmax` if none.
    pub xmin: u64,
    /// `next_txn_id` at publish time.
    pub xmax: u64,
}

/// Lock ordering invariant (acquire in this order, never reversed):
///
///   `active_state` → overflow reader map → `clog`
///
/// - Writers (`begin`/`mark_committed`/`mark_aborted`) hold `active_state.write()`
///   while publishing `txn_state` via `ArcSwap::store`.
/// - `global_xmin` loads published state, scans fixed reader slots, and only
///   locks the overflow reader map when it is non-empty.
/// - `read_snapshot` is lock-free on `active_txns` (loads from `txn_state`),
///   then publishes into a fixed slot or the overflow reader map.
#[derive(Debug)]
pub struct TransactionManager {
    pub next_txn_id: AtomicU64,
    pub clog: RwLock<HashMap<u64, TransactionStatus>>,
    pub vacuum_horizon: AtomicU64,
    pub dead_versions: AtomicU64,
    /// In-flight `txn_id` → (its snapshot's `xmin`, when it began). The xmin
    /// feeds `global_xmin`; the timestamp feeds `oldest_active_txn_age`.
    pub active_state: RwLock<ActiveState>,
    /// Immutable transaction state. Every publisher must hold
    /// `active_state.write()` while building and storing a new value.
    pub txn_state: ArcSwap<TxnState>,

    slots: Box<[CachePadded<AtomicU64>]>,
    overflow_readers: Mutex<HashMap<u64, u64>>,
    overflow_nonempty: AtomicBool,
    /// Even when no reader is registering. Odd while a reader is between
    /// claiming capacity and publishing its snapshot xmin.
    reader_epoch: AtomicU64,
    next_reader_token: AtomicU64,
    next_reader_slot_probe: AtomicU64,

    waiters: Mutex<HashMap<u64, WaiterEntry>>,
}

const EMPTY_SLOT: u64 = u64::MAX;
const SETTING_UP_SLOT: u64 = 0;
const MAX_READER_SLOTS: usize = 128;
const OVERFLOW_READER_TOKEN_FLAG: u64 = 1 << 63;

thread_local! {
    static CACHED_READER_SLOT: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    /// Creates a new, empty `TransactionManager`.
    pub fn new() -> Self {
        let initial_state = TxnState {
            active: Arc::from([]),
            aborted: Arc::from([]),
            xmin: 1,
            xmax: 1,
        };
        let mut slots = Vec::with_capacity(MAX_READER_SLOTS);
        for _ in 0..MAX_READER_SLOTS {
            slots.push(CachePadded::new(AtomicU64::new(EMPTY_SLOT)));
        }
        Self {
            next_txn_id: AtomicU64::new(1),
            clog: RwLock::new(HashMap::new()),
            vacuum_horizon: AtomicU64::new(0),
            dead_versions: AtomicU64::new(0),
            active_state: RwLock::new(ActiveState {
                active: HashMap::new(),
                recent_aborts: BTreeSet::new(),
            }),
            txn_state: ArcSwap::from_pointee(initial_state),
            slots: slots.into_boxed_slice(),
            overflow_readers: Mutex::new(HashMap::new()),
            overflow_nonempty: AtomicBool::new(false),
            reader_epoch: AtomicU64::new(0),
            next_reader_token: AtomicU64::new(0),
            next_reader_slot_probe: AtomicU64::new(0),
            waiters: Mutex::new(HashMap::new()),
        }
    }

    fn build_txn_state(&self, active_state: &ActiveState) -> TxnState {
        let xmax = self.next_txn_id.load(Acquire);
        let mut active_ids: Vec<u64> = active_state.active.keys().copied().collect();
        active_ids.sort_unstable();

        let aborted_ids: Vec<u64> = active_state.recent_aborts.iter().copied().collect();

        let xmin = active_state
            .active
            .values()
            .map(|(xmin, _)| *xmin)
            .min()
            .unwrap_or(xmax);

        TxnState {
            active: Arc::from(active_ids),
            aborted: Arc::from(aborted_ids),
            xmin,
            xmax,
        }
    }

    fn publish_txn_state(&self, active_state: &ActiveState) -> Arc<TxnState> {
        let state = Arc::new(self.build_txn_state(active_state));
        self.txn_state.store(Arc::clone(&state));
        state
    }

    /// Re-publish `txn_state` from the current `active_txns` and `next_txn_id`.
    ///
    /// Called by recovery after restoring watermarks so that `get_snapshot()` and
    /// `read_snapshot()` see the correct `xmax`. Without this, the ArcSwap state
    /// would still have `xmax=1` from construction, making every recovered row
    /// invisible (txn_id ≥ xmax → "not yet started").
    pub fn refresh_txn_state(&self) {
        let active_state = self.active_state.read().unwrap();
        let state = self.build_txn_state(&active_state);
        self.txn_state.store(Arc::new(state));
    }

    fn snapshot_from_state(state: &TxnState) -> Snapshot {
        Snapshot::from_state(state)
    }

    fn reader_slot_key(&self) -> usize {
        self as *const Self as usize
    }

    fn cached_reader_slot(&self) -> Option<usize> {
        let key = self.reader_slot_key();
        CACHED_READER_SLOT.with(|slot| match slot.get() {
            Some((cached_key, idx)) if cached_key == key => Some(idx),
            _ => None,
        })
    }

    fn remember_reader_slot(&self, idx: usize) {
        let key = self.reader_slot_key();
        CACHED_READER_SLOT.with(|slot| slot.set(Some((key, idx))));
    }

    fn try_claim_reader_slot(&self, idx: usize) -> bool {
        self.slots[idx]
            .compare_exchange(EMPTY_SLOT, SETTING_UP_SLOT, AcqRel, Acquire)
            .is_ok()
    }

    fn claim_reader_slot(&self) -> Option<usize> {
        let cached = self.cached_reader_slot();
        if let Some(idx) = cached
            && self.try_claim_reader_slot(idx)
        {
            return Some(idx);
        }

        let start = cached
            .map(|idx| idx.wrapping_add(1))
            .unwrap_or_else(|| self.next_reader_slot_probe.fetch_add(1, Relaxed) as usize)
            % MAX_READER_SLOTS;

        for offset in 0..MAX_READER_SLOTS {
            let idx = (start + offset) % MAX_READER_SLOTS;
            if Some(idx) == cached {
                continue;
            }
            if self.try_claim_reader_slot(idx) {
                self.remember_reader_slot(idx);
                return Some(idx);
            }
        }

        None
    }

    /// The vacuum horizon: oldest snapshot `xmin` among in-flight txns (NOT
    /// min txn id — a snapshot can be older than its holder's id). Versions
    /// deleted below this bound are invisible to every live snapshot.
    pub fn global_xmin(&self) -> u64 {
        loop {
            let epoch = self.reader_epoch.load(Acquire);
            if epoch & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }

            let state = self.txn_state.load();
            let mut min_xmin = state.xmin;

            for i in 0..MAX_READER_SLOTS {
                let val = self.slots[i].load(Acquire);
                if val != EMPTY_SLOT {
                    min_xmin = min_xmin.min(val);
                }
            }

            if self.overflow_nonempty.load(Acquire) {
                let readers = self.overflow_readers.lock().unwrap();
                for val in readers.values() {
                    min_xmin = min_xmin.min(*val);
                }
            }

            if self.reader_epoch.load(Acquire) == epoch {
                return min_xmin;
            }
        }
    }

    /// Take a snapshot-only reader. Unlike `begin`, this allocates no
    /// `txn_id`, writes no CLOG entry, and never joins `active_txns`; it only
    /// publishes its snapshot `xmin` into `readers` so vacuum cannot advance
    /// past a version this reader can still see.
    ///
    /// Returns the `Snapshot` and a `token`; the caller MUST pass the token to
    /// [`end_read`] once the read is done, or the horizon stays pinned.
    ///
    /// Race safety: the snapshot is loaded from `txn_state` (lock-free via
    /// `ArcSwap`). The loaded `xmin` may be stale-low if a commit races, but
    /// a stale-low pin only delays vacuum — it can never allow vacuum to
    /// advance past versions this reader needs. This is safe.
    pub fn read_snapshot(&self) -> (Snapshot, u64) {
        self.reader_epoch.fetch_add(1, AcqRel);

        if let Some(slot_idx) = self.claim_reader_slot() {
            loop {
                let state = self.txn_state.load();
                self.slots[slot_idx].store(state.xmin, SeqCst);
                let check = self.txn_state.load();
                if check.xmin == state.xmin {
                    self.reader_epoch.fetch_add(1, Release);
                    return (Self::snapshot_from_state(&state), slot_idx as u64);
                }
                self.slots[slot_idx].store(SETTING_UP_SLOT, SeqCst);
            }
        } else {
            let token = self.next_reader_token.fetch_add(1, AcqRel);
            loop {
                {
                    let mut readers = self.overflow_readers.lock().unwrap();
                    readers.insert(token, SETTING_UP_SLOT);
                    self.overflow_nonempty.store(true, Release);
                }
                let state = self.txn_state.load();
                self.overflow_readers
                    .lock()
                    .unwrap()
                    .insert(token, state.xmin);
                let check = self.txn_state.load();
                if check.xmin == state.xmin {
                    self.reader_epoch.fetch_add(1, Release);
                    return (
                        Self::snapshot_from_state(&state),
                        token | OVERFLOW_READER_TOKEN_FLAG,
                    );
                }
            }
        }
    }

    /// Release a snapshot-only reader's horizon pin. Deregistration only ever
    /// raises `global_xmin`, so it needs no lock coordination with `begin`.
    pub fn end_read(&self, token: u64) {
        if token & OVERFLOW_READER_TOKEN_FLAG != 0 {
            let real_token = token & !OVERFLOW_READER_TOKEN_FLAG;
            let mut lock = self.overflow_readers.lock().unwrap();
            lock.remove(&real_token);
            if lock.is_empty() {
                self.overflow_nonempty.store(false, Release);
            }
        } else {
            let idx = token as usize;
            self.slots[idx].store(EMPTY_SLOT, SeqCst);
        }
    }

    /// Age of the oldest still-open transaction, `None` if nothing is in
    /// flight. A large value here means `global_xmin` is pinned and vacuum
    /// cannot reclaim anything — the classic forgotten-transaction stall.
    pub fn oldest_active_txn_age(&self) -> Option<Duration> {
        let active_state = self.active_state.read().unwrap();
        active_state
            .active
            .values()
            .map(|(_, began)| began.elapsed())
            .max()
    }

    /// Publishes the horizon of a COMPLETED full vacuum sweep. `fetch_max` so a
    /// slower concurrent sweep can never move the horizon backward. (DESIGN §8.4)
    pub fn publish_vacuum_horizon(&self, horizon: u64) {
        let previous = self.vacuum_horizon.fetch_max(horizon, AcqRel);
        let effective_horizon = previous.max(horizon);

        let mut active_state = self.active_state.write().unwrap();
        let before = active_state.recent_aborts.len();
        active_state
            .recent_aborts
            .retain(|txn_id| *txn_id >= effective_horizon);
        if active_state.recent_aborts.len() != before {
            self.publish_txn_state(&active_state);
        }
    }

    /// The oldest-active-txn-id captured at the start of the most recent
    /// completed full sweep (0 until the first post-restart sweep completes).
    pub fn vacuum_horizon(&self) -> u64 {
        self.vacuum_horizon.load(Acquire)
    }

    /// Counts dead row versions created since the last autovacuum sweep.
    /// One per tombstoned version (update/delete success paths).
    pub fn note_dead_version(&self) {
        self.dead_versions.fetch_add(1, AcqRel);
    }

    /// Current dead-version count (for stats/tests).
    pub fn dead_versions(&self) -> u64 {
        self.dead_versions.load(Acquire)
    }

    /// Atomically resets the counter and returns what it was. The autovacuum
    /// worker calls this once it has decided to sweep; the swap ensures
    /// concurrent increments are never lost or double-counted.
    pub fn take_dead_versions(&self) -> u64 {
        self.dead_versions.swap(0, AcqRel)
    }

    /// Returns a taken count after a failed sweep, so the next tick retries
    /// instead of waiting for a fresh threshold's worth of dead versions.
    pub fn restore_dead_versions(&self, n: u64) {
        self.dead_versions.fetch_add(n, AcqRel);
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
        let mut active = self.active_state.write().unwrap();

        let txn_id = self.next_txn_id.fetch_add(1, AcqRel);
        // Snapshot xmin = oldest in-flight txn; stored for global_xmin().
        let xmin = active.active.keys().min().copied().unwrap_or(txn_id);
        active.active.insert(txn_id, (xmin, Instant::now()));

        let state = self.publish_txn_state(&active);
        let snapshot = Self::snapshot_from_state(&state);

        drop(active);

        self.clog
            .write()
            .unwrap()
            .insert(txn_id, TransactionStatus::Active);

        Transaction {
            txn_id,
            snapshot,
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
        let mut active = self.active_state.write().unwrap();
        active.active.remove(&txn_id);
        self.publish_txn_state(&active);
        drop(active);
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
        let mut active = self.active_state.write().unwrap();
        active.active.remove(&txn_id);
        active.recent_aborts.insert(txn_id);
        self.publish_txn_state(&active);
        drop(active);
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
        self.active_state
            .read()
            .unwrap()
            .recent_aborts
            .contains(&txn_id)
    }

    /// Returns `true` if the transaction is currently in the active set.
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_state
            .read()
            .unwrap()
            .active
            .contains_key(&txn_id)
    }

    /// Generates a "latest" snapshot from the current manager state.
    ///
    /// This captures the current `xmin`, `xmax`, and active set. It is typically
    /// used for ad-hoc reads or by `begin()` to initialize a transaction's view.
    pub fn get_snapshot(&self) -> Snapshot {
        let state = self.txn_state.load();
        Self::snapshot_from_state(&state)
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

    /// Seed the CLOG from a checkpoint's pinned-aborted set.
    ///
    /// Called by recovery before the redo scan. Each txn in the list is
    /// marked Aborted in the CLOG, providing the durable shadow of the
    /// aborted entries that existed at checkpoint time.
    pub fn seed_clog_from_checkpoint(&self, pinned_aborted: &[u64]) {
        let mut active = self.active_state.write().unwrap();
        for &txn_id in pinned_aborted {
            active.recent_aborts.insert(txn_id);
        }
        self.publish_txn_state(&active);
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
    fn read_snapshot_pins_horizon_until_end_read() {
        let tm = std::sync::Arc::new(TransactionManager::new());

        // Open a writer so the horizon has a concrete pin to compare against,
        // then advance next_txn_id past it.
        let writer = tm.begin(); // txn_id = 1, xmin = 1
        let (snap, token) = tm.read_snapshot(); // snapshot xmin = 1 (writer active)

        // While the reader holds its token, its xmin pins global_xmin even after
        // the writer that produced it settles.
        tm.mark_committed(writer.txn_id);
        assert_eq!(
            tm.global_xmin(),
            snap.xmin,
            "reader token must keep global_xmin pinned at its snapshot xmin"
        );

        // Releasing the reader lets the horizon advance to next_txn_id (no one
        // left in flight).
        tm.end_read(token);
        assert_eq!(
            tm.global_xmin(),
            tm.next_txn_id.load(Acquire),
            "after end_read the horizon is no longer pinned by the reader"
        );
    }

    #[test]
    fn read_snapshot_takes_no_txn_id_or_clog_entry() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let before = tm.next_txn_id.load(Acquire);

        let (_snap, token) = tm.read_snapshot();
        assert_eq!(
            tm.next_txn_id.load(Acquire),
            before,
            "read_snapshot must not consume a txn_id"
        );
        assert!(
            tm.clog.read().unwrap().is_empty(),
            "read_snapshot must not write a CLOG entry"
        );
        tm.end_read(token);
    }

    #[test]
    fn read_snapshot_xmin_uses_snapshot_xmin_not_txn_id() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        // txn1: txn_id=1, stored xmin=1
        let txn1 = tm.begin();
        // txn2: txn_id=2, stored xmin=1 (txn1 was still active at begin)
        let txn2 = tm.begin();
        // Commit txn1 — txn2 is still active with snapshot xmin=1.
        tm.mark_committed(txn1.txn_id);

        let (snap, token) = tm.read_snapshot();
        // The reader's xmin must be 1 (txn2's stored snapshot xmin),
        // NOT 2 (txn2's txn_id / min active key). Pinning at 2 would
        // let vacuum advance past versions in [1, 2) that the reader
        // can still see.
        assert_eq!(
            snap.xmin, txn2.snapshot.xmin,
            "read_snapshot must use the snapshot xmin (from values), not the txn_id (from keys)"
        );
        tm.end_read(token);
    }

    #[test]
    fn test_seed_clog_from_checkpoint() {
        let tm = TransactionManager::new();

        let pinned = vec![10, 15, 20];
        tm.seed_clog_from_checkpoint(&pinned);

        assert!(tm.is_aborted(10));
        assert!(tm.is_aborted(15));
        assert!(tm.is_aborted(20));
        assert!(!tm.is_committed(10));
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

    #[test]
    fn published_txn_state_tracks_begin_commit_abort() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let initial = tm.txn_state.load();
        assert_eq!(initial.xmin, 1);
        assert_eq!(initial.xmax, 1);
        assert!(initial.active.is_empty());
        assert!(initial.aborted.is_empty());
        drop(initial);

        let txn1_id;
        let txn2;
        let txn3;
        {
            let txn1 = tm.begin();
            txn1_id = txn1.txn_id;
            txn2 = tm.begin();
            txn3 = tm.begin();

            let st = tm.txn_state.load();
            assert_eq!(&*st.active, &[txn1.txn_id, txn2.txn_id, txn3.txn_id]);
            assert_eq!(st.xmin, txn1.txn_id);
            assert_eq!(st.xmax, txn3.txn_id + 1);
            assert!(st.aborted.is_empty());
        }

        tm.mark_committed(txn2.txn_id);
        {
            let st = tm.txn_state.load();
            assert_eq!(&*st.active, &[txn1_id, txn3.txn_id]);
            assert_eq!(st.xmin, txn1_id);
            assert_eq!(st.xmax, txn3.txn_id + 1);
        }

        tm.mark_aborted(txn3.txn_id);
        {
            let st = tm.txn_state.load();
            assert_eq!(&*st.active, &[txn1_id]);
            assert_eq!(st.xmin, txn1_id);
            assert_eq!(st.xmax, txn3.txn_id + 1);
            assert_eq!(
                &*st.aborted,
                &[txn3.txn_id],
                "aborts move into TxnState in Phase 3"
            );
        }
    }

    #[test]
    fn stale_published_snapshot_hides_later_commit() {
        let tm = std::sync::Arc::new(TransactionManager::new());
        let writer = tm.begin();
        let stale = TransactionManager::snapshot_from_state(&tm.txn_state.load());

        tm.mark_committed(writer.txn_id);

        assert!(
            !stale.is_committed(writer.txn_id, &tm),
            "a transaction active in the loaded snapshot must stay invisible to it after commit"
        );

        let fresh = tm.get_snapshot();
        assert!(
            fresh.is_committed(writer.txn_id, &tm),
            "a fresh snapshot after commit should see the committed transaction"
        );
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

    #[test]
    fn test_truncate_clog_retains_aborted_above_vacuum_horizon() {
        let tm = std::sync::Arc::new(TransactionManager::new());

        //make a dummy entry into the database, followed by an aborted entry
        //and then a dummy entry again to mark global_xmin > aborted_txn.txn_id

        //txc2 was not committed - which means global_xmin will set to it's txn_id
        //when a sweep runs
        let txc1 = tm.begin();
        let tx_abort = tm.begin();

        tm.mark_committed(txc1.txn_id);
        tm.mark_aborted(tx_abort.txn_id);

        // Begin AFTER the first two settle: global_xmin is the min snapshot
        // xmin of live txns, so txc2's snapshot must postdate the aborted txn
        // for the horizon to rise above it.
        let _txc2 = tm.begin();

        //now, we run a full sweep
        tm.publish_vacuum_horizon(tx_abort.txn_id - 1); // so that the horizon is below the aborted txn's id 

        let committed_horizon = tm.global_xmin();
        assert!(tx_abort.txn_id < committed_horizon);
        assert!(tm.vacuum_horizon() < tx_abort.txn_id);

        tm.truncate_clog(committed_horizon);

        //Now the mandatory asserts - the transaction is still present in the map
        assert_eq!(
            tm.clog.read().unwrap().get(&tx_abort.txn_id),
            Some(&TransactionStatus::Aborted),
            "aborted entry below global_xmin but above vacuum_horizon must survive",
        );
        // And settled_status is unchanged for it, that is it did not flip to committed.
        assert_eq!(
            tm.settled_status(tx_abort.txn_id),
            TransactionStatus::Aborted
        );
    }

    #[test]
    fn test_truncate_clog_drops_aborted_below_vacuum_horizon() {
        let tm = std::sync::Arc::new(TransactionManager::new());

        let tx_abort = tm.begin();
        tm.mark_aborted(tx_abort.txn_id);

        let txc1 = tm.begin();
        tm.mark_committed(txc1.txn_id);
        let _txc2 = tm.begin();

        tm.publish_vacuum_horizon(txc1.txn_id);
        assert!(!tm.is_aborted(tx_abort.txn_id));
        let committed_horizon = tm.global_xmin();
        tm.truncate_clog(committed_horizon);

        assert_eq!(
            tm.clog.read().unwrap().get(&tx_abort.txn_id),
            None,
            "aborted entry below vacuum_horizon must not survive",
        );
    }
}
