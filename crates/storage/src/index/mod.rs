//! B+Tree index — Lehman-Yao optimistic locking with MVCC.
//!
//! ## Concurrency model
//! Optimistic descent with shared latches only. Exclusive latch on the leaf
//! for mutation. Snapshot-based MVCC visibility determines which record
//! versions a transaction can see. First-writer-wins conflict detection
//! prevents lost updates.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use common::{Key, MAX_KEY_SIZE, MAX_VALUE_SIZE, Value};
use db_core::transaction_manager::TransactionManager;

use crate::buffer_pool::{BufferPoolManager, PageReadGuard, PageWriteGuard};
use crate::page::{
    INTERNAL, InternalPageAccessor, InternalPageBuilder, InternalPageMutator, LEAF,
    LeafPageAccessor, LeafPageBuilder, LeafPageMutator, PAGE_SIZE, PageId,
};
use crate::wal::Wal;
use common::IndexError;
use db_core::transaction::Transaction;

pub type Result<T> = std::result::Result<T, IndexError>;

/// Sentinel `txn_id` for structural/maintenance records — owned by no txn.
const SYSTEM_TXN_ID: u64 = 0;

// ── SplitResult ───────────────────────────────────────────────────────────────

struct SplitResult {
    separator_key: Vec<u8>,
    new_page_id: PageId,
}

// ── BTStack ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct BTStackEntry {
    page_id: PageId,
}

type BTStack = Vec<BTStackEntry>;

// ── BTreeIndex ────────────────────────────────────────────────────────────────

pub struct BTreeIndex<K: Key, V: Value> {
    pool: Arc<BufferPoolManager>,
    wal: Arc<Wal>,
    root: Mutex<PageId>,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K: Key, V: Value> BTreeIndex<K, V> {
    // ── Constructors ──────────────────────────────────────────────────────────

    pub fn open(pool: Arc<BufferPoolManager>, wal: Arc<Wal>) -> Result<Self> {
        // Page 0 is the superblock. fetch_page verifies its checksum, so a torn
        // write surfaces here as BufferPoolError::PageCorruption.
        let meta = pool.fetch_page(0)?;
        let root = crate::page::meta::read_root(&meta[..]).ok_or_else(|| {
            IndexError::CorruptMetadata("page 0 is not a FluxDB superblock".into())
        })?;
        drop(meta);
        Ok(Self::from_root(pool, wal, root))
    }

    /// Reclaims storage space by physically removing dead record versions.
    ///
    /// This method performs a full sequential scan of all leaf pages in the
    /// B+Tree. For each page, it identifies "dead" tuples (those not visible
    /// to any active transaction snapshot) and removes them, compacting the
    /// page in-place to reclaim bytes for future inserts.
    ///
    /// Returns the total number of records removed across all pages.
    pub fn vacuum(&self, tm: &TransactionManager) -> Result<usize> {
        let global_xmin = tm.global_xmin();
        let root = self.root_page_id();
        let mut leaf_pid = self.find_leftmost_leaf(root)?;
        let mut total_dead = 0;

        loop {
            let mut guard = self.pool.fetch_page_mut(leaf_pid)?;

            total_dead +=
                LeafPageMutator::<K, V>::compact(leaf_pid, &mut guard[..], global_xmin, tm);

            let acc = LeafPageAccessor::<K, V>::new(&guard[..]);
            let next = acc.rightlink();
            drop(guard);

            match next {
                Some(pid) => leaf_pid = pid,
                None => break,
            }
        }

        Ok(total_dead)
    }

    pub fn create(pool: Arc<BufferPoolManager>, wal: Arc<Wal>) -> Result<(Self, PageId)> {
        let mut meta_guard = pool.new_page()?;
        let meta_pid = meta_guard.page_id;
        // create root page first
        let mut root_guard = pool.new_page()?;
        let root_pid = root_guard.page_id;
        LeafPageBuilder::<K, V>::new(root_pid, &mut root_guard[..]);
        drop(root_guard);
        // Make the root durable BEFORE the superblock that points to it, so a
        // crash can never leave page 0 referencing a not-yet-written root page.
        pool.flush_page(root_pid)?;
        crate::page::meta::init(&mut meta_guard[..], root_pid);
        drop(meta_guard);
        pool.flush_page(meta_pid)?;

        Ok((Self::from_root(pool, wal, root_pid), root_pid))
    }

    pub fn root_page_id(&self) -> PageId {
        *self.root.lock().unwrap()
    }

    fn from_root(pool: Arc<BufferPoolManager>, wal: Arc<Wal>, root: PageId) -> Self {
        Self {
            pool,
            wal,
            root: Mutex::new(root),
            _val: PhantomData,
            _key: PhantomData,
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Look up `key` and return the value visible to `txn`, or `None`.
    pub fn get(&self, key: &K::SelfType<'_>, txn: &Transaction) -> Result<Option<Vec<u8>>> {
        let root_pid = *self.root.lock().unwrap();
        let leaf_pid = self.find_leaf(root_pid, key)?;
        let key_bytes = K::as_bytes(key);

        // Shared latch on leaf + rightlink correction.
        let mut page = self.pool.fetch_page(leaf_pid)?;
        loop {
            let acc = LeafPageAccessor::<K, V>::new(&page[..]);
            if let Some(hk) = acc.high_key_bytes()
                && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
            {
                let right = acc.rightlink().unwrap();
                drop(page);
                page = self.pool.fetch_page(right)?;
                continue;
            }
            break;
        }

        let acc = LeafPageAccessor::<K, V>::new(&page[..]);
        match self.find_visible_slot(&acc, key, txn) {
            Some(slot) => {
                let val = acc.get_value(slot);
                Ok(Some(V::as_bytes(&val).as_ref().to_vec()))
            }
            None => Ok(None),
        }
    }

    /// Insert a new `(key, value)` pair. Returns `DuplicateKey` if a visible
    /// version already exists under `txn`'s snapshot.
    pub fn insert(
        &self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
        txn: &Transaction,
    ) -> Result<()> {
        let key_bytes = K::as_bytes(key);
        let key_len = key_bytes.as_ref().len();

        if key_len > MAX_KEY_SIZE {
            return Err(IndexError::KeyTooLarge {
                size: key_len,
                max: MAX_KEY_SIZE,
            });
        }

        let val_bytes_check = V::as_bytes(value);
        let val_len = val_bytes_check.as_ref().len();
        if val_len > MAX_VALUE_SIZE {
            return Err(IndexError::ValueTooLarge {
                size: val_len,
                max: MAX_VALUE_SIZE,
            });
        }

        let root_pid = *self.root.lock().unwrap();
        let mut stack = BTStack::new();
        let mut pid = root_pid;

        // ── Phase 1: Optimistic descent (shared latches only) ────────────
        let leaf_pid = loop {
            let page = self.pool.fetch_page(pid)?;
            // Lazily finish a split left incomplete (crash window / concurrent
            // split), then restart from the root.
            if crate::page::is_incomplete_split(&page[..]) {
                drop(page);
                self.finish_split(pid, &stack)?;
                stack.clear();
                pid = *self.root.lock().unwrap();
                continue;
            }
            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    if let Some(hk) = acc.high_key_bytes()
                        && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                    {
                        let right = acc.rightlink().unwrap();
                        drop(page);
                        pid = right;
                        continue;
                    }
                    let (_, child_pid) = acc.find_child(key);
                    stack.push(BTStackEntry { page_id: pid });
                    drop(page);
                    pid = child_pid;
                }
                LEAF => {
                    drop(page);
                    break pid;
                }
                found => {
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        };

        // ── Phase 2+3: Latch, conflict check, insert (retry on WaitFor) ──
        //
        // If check_insert_conflict returns WaitFor(blocking_txn), we must
        // drop the latch before waiting — holding it while sleeping would
        // block every other reader/writer on this page.
        let mut leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
        loop {
            // Rightlink correction: follow splits that happened during descent.
            loop {
                let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                {
                    let right = acc.rightlink().unwrap();
                    drop(leaf_guard);
                    leaf_guard = self.pool.fetch_page_mut(right)?;
                    continue;
                }
                break;
            }

            let (slot, exact) = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).position(key);

            if exact {
                let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
                match self.check_insert_conflict(&acc, slot, key, txn) {
                    Ok(()) => {}
                    Err(IndexError::WaitFor(blocking_txn)) => {
                        drop(leaf_guard);
                        Self::wait_for_txn(&txn.tm, blocking_txn, txn.txn_id)?;
                        leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            let result = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).insert(slot, key, value);
            return match result {
                Ok(()) => {
                    let page_id = leaf_guard.page_id;
                    LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_xmin(slot, txn.txn_id);
                    // WAL: physiological Insert (DESIGN §4.2). Append-only here — durability
                    // is enforced lazily by the buffer pool's WAL-before-page gate or at
                    // commit, never fsynced at insert time. Stamp the record's LSN as the
                    // page LSN so the gate flushes the WAL through it before the page lands.
                    let val_bytes = V::as_bytes(value);
                    let lsn = self.wal.log_insert(
                        txn.txn_id,
                        page_id,
                        slot as u16,
                        key_bytes.as_ref(),
                        val_bytes.as_ref(),
                        txn.txn_id,
                    )?;
                    LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
                    Ok(())
                }
                Err(_) => self.split_and_insert(leaf_guard, key, value, txn, &mut stack),
            };
        }
    }

    /// Delete `key` by setting xmax on the visible version.
    pub fn delete(&self, key: &K::SelfType<'_>, txn: &Transaction) -> Result<()> {
        let root_pid = *self.root.lock().unwrap();
        let key_bytes = K::as_bytes(key);
        let mut pid = root_pid;

        // Optimistic descent.
        let leaf_pid = loop {
            let page = self.pool.fetch_page(pid)?;
            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    if let Some(hk) = acc.high_key_bytes()
                        && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                    {
                        let right = acc.rightlink().unwrap();
                        drop(page);
                        pid = right;
                        continue;
                    }
                    let (_, child_pid) = acc.find_child(key);
                    drop(page);
                    pid = child_pid;
                }
                LEAF => {
                    drop(page);
                    break pid;
                }
                found => {
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        };

        // Exclusive latch + rightlink correction + conflict retry loop.
        let mut leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
        loop {
            // Rightlink correction: follow splits that happened during descent.
            loop {
                let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                {
                    let right = acc.rightlink().unwrap();
                    drop(leaf_guard);
                    leaf_guard = self.pool.fetch_page_mut(right)?;
                    continue;
                }
                break;
            }

            // Find visible version.
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let visible_slot = self
                .find_visible_slot(&acc, key, txn)
                .ok_or(IndexError::KeyNotFound)?;

            // Conflict check: if another in-progress txn holds xmax, wait for
            // it to settle then retry — same Wait-Die protocol as insert.
            let rec_xmax = acc.get_xmax(visible_slot);
            match self.check_write_conflict(rec_xmax, txn) {
                Ok(()) => {}
                Err(IndexError::WaitFor(blocking_txn)) => {
                    drop(leaf_guard);
                    Self::wait_for_txn(&txn.tm, blocking_txn, txn.txn_id)?;
                    leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
                    continue;
                }
                Err(e) => return Err(e),
            }

            // Set xmax to mark this version as deleted by our transaction.
            let page_id = leaf_guard.page_id;
            let lsn =
                self.wal
                    .log_set_xmax(txn.txn_id, page_id, visible_slot as u16, txn.txn_id)?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_xmax(visible_slot, txn.txn_id);
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
            return Ok(());
        }
    }

    /// Update `key` with `new_value`. Atomically sets xmax on the old version
    /// and inserts a new version — both under the same exclusive latch.
    pub fn update(
        &self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
        txn: &Transaction,
    ) -> Result<()> {
        let key_bytes = K::as_bytes(key);
        let key_len = key_bytes.as_ref().len();

        if key_len > MAX_KEY_SIZE {
            return Err(IndexError::KeyTooLarge {
                size: key_len,
                max: MAX_KEY_SIZE,
            });
        }

        let val_bytes_check = V::as_bytes(value);
        let val_len = val_bytes_check.as_ref().len();
        if val_len > MAX_VALUE_SIZE {
            return Err(IndexError::ValueTooLarge {
                size: val_len,
                max: MAX_VALUE_SIZE,
            });
        }

        let root_pid = *self.root.lock().unwrap();
        let mut stack = BTStack::new();
        let mut pid = root_pid;

        // Optimistic descent.
        let leaf_pid = loop {
            let page = self.pool.fetch_page(pid)?;
            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    if let Some(hk) = acc.high_key_bytes()
                        && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                    {
                        let right = acc.rightlink().unwrap();
                        drop(page);
                        pid = right;
                        continue;
                    }
                    let (_, child_pid) = acc.find_child(key);
                    stack.push(BTStackEntry { page_id: pid });
                    drop(page);
                    pid = child_pid;
                }
                LEAF => {
                    drop(page);
                    break pid;
                }
                found => {
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        };

        // Exclusive latch + rightlink correction + conflict retry loop.
        let mut leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
        loop {
            // Rightlink correction: follow splits that happened during descent.
            loop {
                let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(key_bytes.as_ref(), hk) != Ordering::Less
                {
                    let right = acc.rightlink().unwrap();
                    drop(leaf_guard);
                    leaf_guard = self.pool.fetch_page_mut(right)?;
                    continue;
                }
                break;
            }

            // Find visible version under same exclusive latch.
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let visible_slot = self
                .find_visible_slot(&acc, key, txn)
                .ok_or(IndexError::KeyNotFound)?;

            // Conflict check: if another in-progress txn holds xmax, wait for
            // it to settle then retry — same Wait-Die protocol as insert.
            let rec_xmax = acc.get_xmax(visible_slot);
            match self.check_write_conflict(rec_xmax, txn) {
                Ok(()) => {}
                Err(IndexError::WaitFor(blocking_txn)) => {
                    drop(leaf_guard);
                    Self::wait_for_txn(&txn.tm, blocking_txn, txn.txn_id)?;
                    leaf_guard = self.pool.fetch_page_mut(leaf_pid)?;
                    continue;
                }
                Err(e) => return Err(e),
            }

            // ── ATOMIC: set xmax on old + insert new (same latch) ────────────
            let page_id = leaf_guard.page_id;
            let _lsn_xmax =
                self.wal
                    .log_set_xmax(txn.txn_id, page_id, visible_slot as u16, txn.txn_id)?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_xmax(visible_slot, txn.txn_id);
            // Find insert position for the new version.
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let (slot, _) = acc.position(key);
            let val_bytes = V::as_bytes(value);

            // If the new version fits in place, insert() cannot fail — so we can
            // log to the WAL *before* dirtying the page (WAL-before-page).
            if acc.can_fit_direct(key_len, val_bytes.as_ref().len()) {
                let lsn = self.wal.log_insert(
                    txn.txn_id,
                    page_id,
                    slot as u16,
                    key_bytes.as_ref(),
                    val_bytes.as_ref(),
                    txn.txn_id,
                )?;
                let mut m = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
                m.insert(slot, key, value)
                    .expect("insert must succeed: can_fit_direct checked above");
                m.set_xmin(slot, txn.txn_id);
                m.set_lsn(lsn);
                return Ok(());
            }

            // Doesn't fit → split.
            return self.split_and_insert(leaf_guard, key, value, txn, &mut stack);
        }
    }

    /// Return a lazy iterator over entries visible to `txn`.
    pub fn range<R>(&self, range: R, txn: &Transaction) -> RangeScan<'_, K, V>
    where
        K: 'static,
        R: RangeBounds<K::SelfType<'static>>,
    {
        let root_pid = *self.root.lock().unwrap();

        let (current_leaf, start_slot) = match range.start_bound() {
            Bound::Included(k) => {
                let leaf_pid = self.find_leaf(root_pid, k).expect("find_leaf failed");
                let page = self.pool.fetch_page(leaf_pid).expect("fetch_page failed");
                let acc = LeafPageAccessor::<K, V>::new(&page[..]);
                let (slot, exact) = acc.position(k);
                let slot = if exact {
                    Self::duplicate_slot_bounds(&acc, k, slot).0
                } else {
                    slot
                };
                (Some(leaf_pid), slot)
            }
            Bound::Excluded(k) => {
                let leaf_pid = self.find_leaf(root_pid, k).expect("find_leaf failed");
                let page = self.pool.fetch_page(leaf_pid).expect("fetch_page failed");
                let acc = LeafPageAccessor::<K, V>::new(&page[..]);
                let (slot, exact) = acc.position(k);
                let slot = if exact {
                    Self::duplicate_slot_bounds(&acc, k, slot).1
                } else {
                    slot
                };
                (Some(leaf_pid), slot)
            }
            Bound::Unbounded => {
                let leaf_pid = self
                    .find_leftmost_leaf(root_pid)
                    .expect("find_leftmost failed");
                (Some(leaf_pid), 0)
            }
        };

        let (end_key, end_inclusive) = match range.end_bound() {
            Bound::Included(k) => (Some(K::as_bytes(k).as_ref().to_vec()), true),
            Bound::Excluded(k) => (Some(K::as_bytes(k).as_ref().to_vec()), false),
            Bound::Unbounded => (None, false),
        };

        RangeScan {
            pool: &self.pool,
            current_leaf,
            slot: start_slot,
            end_key,
            end_inclusive,
            txn: txn.clone(),
            _key: PhantomData,
            _val: PhantomData,
        }
    }

    pub fn range_backward<R>(&self, range: R, txn: &Transaction) -> BackwardRangeScan<'_, K, V>
    where
        K: 'static,
        R: RangeBounds<K::SelfType<'static>>,
    {
        let root_pid = *self.root.lock().unwrap();

        let (current_leaf, start_slot) = match range.end_bound() {
            Bound::Included(k) => {
                let leaf_pid = self.find_leaf(root_pid, k).expect("find_leaf failed");
                let page = self.pool.fetch_page(leaf_pid).expect("fetch_page failed");
                let acc = LeafPageAccessor::<K, V>::new(&page[..]);
                let (slot, exact) = acc.position(k);

                let s = if exact {
                    Self::duplicate_slot_bounds(&acc, k, slot).1 as i64 - 1
                } else {
                    slot as i64 - 1
                };
                if s >= 0 {
                    (Some(leaf_pid), s)
                } else {
                    // Upper bound is below every key on this leaf — start from
                    // the last slot of the previous leaf.
                    (acc.prev_page(), -1)
                }
            }
            Bound::Excluded(k) => {
                let leaf_pid = self.find_leaf(root_pid, k).expect("find_leaf failed");
                let page = self.pool.fetch_page(leaf_pid).expect("fetch_page failed");
                let acc = LeafPageAccessor::<K, V>::new(&page[..]);
                let (slot, exact) = acc.position(k);

                let s = if exact {
                    Self::duplicate_slot_bounds(&acc, k, slot).0 as i64 - 1
                } else {
                    slot as i64 - 1
                };
                if s >= 0 {
                    (Some(leaf_pid), s)
                } else {
                    (acc.prev_page(), -1)
                }
            }
            Bound::Unbounded => {
                let leaf_pid = self
                    .find_rightmost_leaf(root_pid)
                    .expect("find_rightmost_leaf failed");
                (Some(leaf_pid), -1)
            }
        };

        let (start_key, start_inclusive) = match range.start_bound() {
            Bound::Included(k) => (Some(K::as_bytes(k).as_ref().to_vec()), true),
            Bound::Excluded(k) => (Some(K::as_bytes(k).as_ref().to_vec()), false),
            Bound::Unbounded => (None, false),
        };

        BackwardRangeScan {
            pool: &self.pool,
            current_leaf,
            slot: start_slot,
            start_key,
            start_inclusive,
            txn: txn.clone(),
            _key: PhantomData,
            _val: PhantomData,
        }
    }

    // ── MVCC helpers ──────────────────────────────────────────────────────────

    /// Returns the `[first, past_end)` slot range for the duplicate run
    /// containing `known_duplicate_slot`.
    ///
    /// `position()` is a binary search and can return any physical duplicate.
    /// Callers that need key-level semantics should use this helper to normalize
    /// that arbitrary exact match into the full duplicate run.
    fn duplicate_slot_bounds(
        acc: &LeafPageAccessor<'_, K, V>,
        key: &K::SelfType<'_>,
        known_duplicate_slot: usize,
    ) -> (usize, usize) {
        let key_bytes = K::as_bytes(key);
        let key_ref = key_bytes.as_ref();

        let mut first = known_duplicate_slot;
        while first > 0 {
            let prev_key_val = acc.get_key(first - 1);
            let prev_key = K::as_bytes(&prev_key_val);
            if K::compare(prev_key.as_ref(), key_ref) != Ordering::Equal {
                break;
            }
            first -= 1;
        }

        let n = acc.num_pairs() as usize;
        let mut past_end = known_duplicate_slot + 1;
        while past_end < n {
            let next_key_val = acc.get_key(past_end);
            let next_key = K::as_bytes(&next_key_val);
            if K::compare(next_key.as_ref(), key_ref) != Ordering::Equal {
                break;
            }
            past_end += 1;
        }

        (first, past_end)
    }

    /// Scan among duplicate keys to find the version visible under `snap`.
    ///
    /// `position()` may return any slot among duplicates (binary search
    /// doesn't guarantee the first). We scan backward to the first duplicate,
    /// then forward through all of them looking for a visible version.
    fn find_visible_slot(
        &self,
        acc: &LeafPageAccessor<'_, K, V>,
        key: &K::SelfType<'_>,
        txn: &Transaction,
    ) -> Option<usize> {
        let (start, found) = acc.position(key);
        if !found {
            return None;
        }

        let (first, past_end) = Self::duplicate_slot_bounds(acc, key, start);
        let mut i = first;
        while i < past_end {
            if txn.is_visible(acc.get_xmin(i), acc.get_xmax(i)) {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// Check for insert conflicts among duplicate versions of `key`.
    ///
    /// Returns `DuplicateKey` if a visible version exists.
    /// Returns `WriteConflict` if an in-progress txn has an uncommitted
    /// insert (xmin in-progress) or uncommitted delete (xmax in-progress).
    fn check_insert_conflict(
        &self,
        acc: &LeafPageAccessor<'_, K, V>,
        start_slot: usize,
        key: &K::SelfType<'_>,
        txn: &Transaction,
    ) -> Result<()> {
        let (first, past_end) = Self::duplicate_slot_bounds(acc, key, start_slot);
        let mut i = first;
        while i < past_end {
            let xmin = acc.get_xmin(i);
            let xmax = acc.get_xmax(i);

            // If xmin is in-progress (uncommitted insert by another txn),
            // signal the caller to release the latch and wait. After the
            // blocking txn settles: if it committed this becomes DuplicateKey
            // on the retry; if it aborted the slot disappears and we proceed.
            if xmin != txn.txn_id && txn.is_in_progress(xmin) {
                return Err(IndexError::WaitFor(xmin));
            }

            // If the record is visible to us, it's a duplicate.
            if txn.is_visible(xmin, xmax) {
                return Err(IndexError::DuplicateKey);
            }

            // If xmax is in-progress (another txn is deleting this version),
            // signal the caller to wait. After settling: if it committed the
            // record is gone and our insert is valid; if it aborted the record
            // is still live and we'll find a visible duplicate on the retry.
            if xmax != 0 && xmax != txn.txn_id && txn.is_in_progress(xmax) {
                return Err(IndexError::WaitFor(xmax));
            }

            i += 1;
        }
        Ok(())
    }

    /// Wait for `blocking_txn` to settle using Wait-Die deadlock prevention.
    ///
    /// If the caller is younger than the blocker (higher txn_id), it dies
    /// immediately — this prevents circular waits where two transactions
    /// wait on each other across different keys.
    ///
    /// If the caller is older, it sleeps on a condvar until the blocker
    /// commits or aborts, then returns so the caller can retry.
    ///
    /// Must be called with no page latches held.
    fn wait_for_txn(tm: &TransactionManager, blocking_txn: u64, my_txn_id: u64) -> Result<()> {
        if my_txn_id > blocking_txn {
            return Err(IndexError::WriteConflict);
        }
        tm.wait_until_settled(blocking_txn);
        Ok(())
    }

    /// Conflict check before setting xmax on a record.
    ///
    /// Returns `WaitFor(txn_id)` if another in-progress transaction has already
    /// claimed this version — the caller must drop its latch, wait for the
    /// blocker to settle, then retry (same pattern as insert).
    fn check_write_conflict(&self, rec_xmax: u64, txn: &Transaction) -> Result<()> {
        if rec_xmax == 0 {
            return Ok(()); // nobody has touched this version
        }
        if rec_xmax == txn.txn_id {
            return Ok(()); // we already modified it (re-entrant)
        }
        if txn.is_in_progress(rec_xmax) {
            return Err(IndexError::WaitFor(rec_xmax));
        }
        // The modifier committed → version is already dead.
        // Caller will get KeyNotFound since find_visible_slot won't find it.
        Ok(())
    }

    // ── Tree navigation ───────────────────────────────────────────────────────

    fn find_leaf(&self, start_pid: PageId, key: &K::SelfType<'_>) -> Result<PageId> {
        let mut pid = start_pid;
        let mut parent_latch: Option<PageReadGuard<'_>> = None;

        loop {
            // acquired read guard on fetched page
            let page = self.pool.fetch_page(pid)?;
            // dropped parent latch
            drop(parent_latch.take());

            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    // rightlink correction in case split happened before we acquired lock on child
                    if let Some(hk) = acc.high_key_bytes() {
                        let key_b = K::as_bytes(key);
                        if K::compare(key_b.as_ref(), hk) != Ordering::Less {
                            let right = acc.rightlink().unwrap();
                            drop(page);
                            pid = right;
                            continue;
                        }
                    }
                    let (_, child_pid) = acc.find_child(key);
                    parent_latch = Some(page);
                    pid = child_pid;
                }
                LEAF => {
                    drop(page);
                    return Ok(pid);
                }
                found => {
                    drop(page);
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        }
    }

    fn find_leftmost_leaf(&self, start_pid: PageId) -> Result<PageId> {
        let mut pid = start_pid;
        loop {
            let page = self.pool.fetch_page(pid)?;
            match page[0] {
                INTERNAL => {
                    let child = InternalPageAccessor::<K>::new(&page[..]).child_page_at(0);
                    drop(page);
                    pid = child;
                }
                LEAF => {
                    drop(page);
                    return Ok(pid);
                }
                found => {
                    drop(page);
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        }
    }

    fn find_rightmost_leaf(&self, start_pid: PageId) -> Result<PageId> {
        let mut pid = start_pid;
        loop {
            let page = self.pool.fetch_page(pid)?;
            match page[0] {
                INTERNAL => {
                    let acc = InternalPageAccessor::<K>::new(&page[..]);
                    let n = acc.num_keys() as usize;
                    let child = acc.child_page_at(n);
                    drop(page);
                    pid = child;
                }
                LEAF => {
                    // Sweep rightlinks to correct for any in-flight splits.
                    drop(page);
                    loop {
                        let page = self.pool.fetch_page(pid)?;
                        let acc = LeafPageAccessor::<K, V>::new(&page[..]);
                        match acc.rightlink() {
                            Some(right) => {
                                drop(page);
                                pid = right;
                            }
                            None => {
                                drop(page);
                                return Ok(pid);
                            }
                        }
                    }
                }
                found => {
                    drop(page);
                    return Err(IndexError::UnexpectedPageType {
                        expected: LEAF,
                        found,
                    });
                }
            }
        }
    }
}

// ── RangeScan ─────────────────────────────────────────────────────────────────

pub struct RangeScan<'a, K: Key, V: Value> {
    pool: &'a BufferPoolManager,
    current_leaf: Option<PageId>,
    slot: usize,
    end_key: Option<Vec<u8>>,
    end_inclusive: bool,
    txn: Transaction,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<'a, K: Key, V: Value> Iterator for RangeScan<'a, K, V> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_pid = self.current_leaf?;
            let page = match self.pool.fetch_page(leaf_pid) {
                Ok(p) => p,
                Err(e) => return Some(Err(e.into())),
            };
            let acc = LeafPageAccessor::<K, V>::new(&page[..]);
            let n = acc.num_pairs() as usize;

            while self.slot < n {
                let xmin = acc.get_xmin(self.slot);
                let xmax = acc.get_xmax(self.slot);

                // Skip records not visible to our snapshot.
                if !self.txn.is_visible(xmin, xmax) {
                    self.slot += 1;
                    continue;
                }

                let k = K::as_bytes(&acc.get_key(self.slot)).as_ref().to_vec();
                let v = V::as_bytes(&acc.get_value(self.slot)).as_ref().to_vec();

                let in_range = match &self.end_key {
                    None => true,
                    Some(end) => {
                        let cmp = K::compare(&k, end);
                        if self.end_inclusive {
                            cmp != Ordering::Greater
                        } else {
                            cmp == Ordering::Less
                        }
                    }
                };

                if !in_range {
                    self.current_leaf = None;
                    return None;
                }

                self.slot += 1;
                return Some(Ok((k, v)));
            }

            self.current_leaf = acc.rightlink();
            self.slot = 0;
        }
    }
}

pub struct BackwardRangeScan<'a, K: Key, V: Value> {
    pool: &'a BufferPoolManager,
    current_leaf: Option<PageId>,
    slot: i64, // signed! counts DOWN, -1 is the "uninitialized" sentinel
    start_key: Option<Vec<u8>>,
    start_inclusive: bool,
    txn: Transaction,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<'a, K: Key, V: Value> Iterator for BackwardRangeScan<'a, K, V> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_pid = self.current_leaf?;
            let page = match self.pool.fetch_page(leaf_pid) {
                Ok(p) => p,
                Err(e) => return Some(Err(e.into())),
            };
            let acc = LeafPageAccessor::<K, V>::new(&page[..]);

            // If slot was not yet initialized for this page (== -1 sentinel),
            // set it to the last slot on the page.
            if self.slot < 0 {
                let n = acc.num_pairs() as usize;
                if n == 0 {
                    // Empty page — follow leftlink.
                    self.current_leaf = acc.prev_page();
                    self.slot = -1;
                    continue;
                }
                self.slot = (n - 1) as i64;
            }

            while self.slot >= 0 {
                let s = self.slot as usize;
                let xmin = acc.get_xmin(s);
                let xmax = acc.get_xmax(s);

                // Skip records not visible to our snapshot.
                if !self.txn.is_visible(xmin, xmax) {
                    self.slot -= 1;
                    continue;
                }

                let k = K::as_bytes(&acc.get_key(s)).as_ref().to_vec();
                let v = V::as_bytes(&acc.get_value(s)).as_ref().to_vec();

                let in_range = match &self.start_key {
                    None => true,
                    Some(start) => {
                        let cmp = K::compare(&k, start);
                        if self.start_inclusive {
                            cmp != Ordering::Less
                        } else {
                            cmp == Ordering::Greater
                        }
                    }
                };

                if !in_range {
                    self.current_leaf = None;
                    return None;
                }

                self.slot -= 1;
                if self.slot < 0 {
                    self.current_leaf = acc.prev_page();
                }
                return Some(Ok((k, v)));
            }

            self.current_leaf = acc.prev_page();
            self.slot = -1; // sentinel: will be set to last slot on next iteration
        }
    }
}

mod smo;

#[cfg(test)]
mod tests;
