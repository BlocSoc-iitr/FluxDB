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
    wal: Arc<Mutex<crate::wal::Wal>>,
    root: Mutex<PageId>,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K: Key, V: Value> BTreeIndex<K, V> {
    // ── Constructors ──────────────────────────────────────────────────────────

    pub fn open(pool: Arc<BufferPoolManager>, wal: Arc<Mutex<crate::wal::Wal>>) -> Result<Self> {
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

    pub fn create(
        pool: Arc<BufferPoolManager>,
        wal: Arc<Mutex<crate::wal::Wal>>,
    ) -> Result<(Self, PageId)> {
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

    fn from_root(
        pool: Arc<BufferPoolManager>,
        wal: Arc<Mutex<crate::wal::Wal>>,
        root: PageId,
    ) -> Self {
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
                    let lsn = self.wal.lock().unwrap().log_insert(
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
            let lsn = self.wal.lock().unwrap().log_set_xmax(
                txn.txn_id,
                page_id,
                visible_slot as u16,
                txn.txn_id,
            )?;
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
            let _lsn_xmax = self.wal.lock().unwrap().log_set_xmax(
                txn.txn_id,
                page_id,
                visible_slot as u16,
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_xmax(visible_slot, txn.txn_id);
            // Find insert position for the new version.
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let (slot, _) = acc.position(key);
            let val_bytes = V::as_bytes(value);

            // If the new version fits in place, insert() cannot fail — so we can
            // log to the WAL *before* dirtying the page (WAL-before-page).
            if acc.can_fit_direct(key_len, val_bytes.as_ref().len()) {
                let lsn = self.wal.lock().unwrap().log_insert(
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

    // ── Split ─────────────────────────────────────────────────────────────────

    /// Split a full leaf then insert `(key, value)` into the correct half.
    /// Takes ownership of `leaf_guard` so it can be dropped when inserting
    /// into the right page. Propagates the new separator up via `stack`.
    fn split_and_insert(
        &self,
        mut leaf_guard: PageWriteGuard<'_>,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
        txn: &Transaction,
        stack: &mut BTStack,
    ) -> Result<()> {
        let leaf_pid_actual = leaf_guard.page_id;

        // ── Try compaction first (design doc 25: bottom-up deletion) ──
        let global_xmin = txn.tm.global_xmin();
        let dead_count = LeafPageMutator::<K, V>::compact(
            leaf_pid_actual,
            &mut leaf_guard[..],
            global_xmin,
            &txn.tm,
        );

        // PageCompact FPI — only when compaction actually repacked the page.
        if dead_count > 0 {
            let fpi = <&[u8; PAGE_SIZE]>::try_from(&leaf_guard[..]).unwrap();
            let lsn =
                self.wal
                    .lock()
                    .unwrap()
                    .log_page_compact(SYSTEM_TXN_ID, leaf_pid_actual, fpi)?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);

            let key_bytes = K::as_bytes(key);
            let val_bytes = V::as_bytes(value);
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);

            if acc.can_fit_direct(key_bytes.as_ref().len(), val_bytes.as_ref().len()) {
                let (slot, _) = acc.position(key);

                let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
                mutator.insert(slot, key, value)?;
                mutator.set_xmin(slot, txn.txn_id);

                // Insert logged separately under the real txn; compact avoided the split.
                let lsn = self.wal.lock().unwrap().log_insert(
                    txn.txn_id,
                    leaf_pid_actual,
                    slot as u16,
                    key_bytes.as_ref(),
                    val_bytes.as_ref(),
                    txn.txn_id,
                )?;
                LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
                return Ok(());
            }
        }

        let key_bytes = K::as_bytes(key);
        let split = self.split_leaf_ly(&mut leaf_guard)?;

        let target_pid =
            if K::compare(key_bytes.as_ref(), split.separator_key.as_slice()) != Ordering::Less {
                split.new_page_id
            } else {
                leaf_pid_actual
            };

        // Insert the new tuple into its target half and log it under the real txn.
        let val_bytes = V::as_bytes(value);
        if target_pid == leaf_pid_actual {
            let (s, _) = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut leaf_guard[..]);
            mutator.insert(s, key, value)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.lock().unwrap().log_insert(
                txn.txn_id,
                leaf_pid_actual,
                s as u16,
                key_bytes.as_ref(),
                val_bytes.as_ref(),
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
            // Release before propagation so the downlink step can latch the leaf
            // to clear its flag (no descendant latch held across the ancestor walk).
            drop(leaf_guard);
        } else {
            drop(leaf_guard);
            let mut right = self.pool.fetch_page_mut(target_pid)?;
            let (s, _) = LeafPageAccessor::<K, V>::new(&right[..]).position(key);
            let mut mutator = LeafPageMutator::<K, V>::new(&mut right[..]);
            mutator.insert(s, key, value)?;
            mutator.set_xmin(s, txn.txn_id);
            let lsn = self.wal.lock().unwrap().log_insert(
                txn.txn_id,
                target_pid,
                s as u16,
                key_bytes.as_ref(),
                val_bytes.as_ref(),
                txn.txn_id,
            )?;
            LeafPageMutator::<K, V>::new(&mut right[..]).set_lsn(lsn);
        }

        self.insert_separator_via_stack(
            stack,
            split.separator_key,
            split.new_page_id,
            leaf_pid_actual,
        )
    }

    fn split_leaf_ly(
        &self,
        leaf_guard: &mut crate::buffer_pool::PageWriteGuard<'_>,
    ) -> Result<SplitResult> {
        let leaf_pid = leaf_guard.page_id;
        let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
        let n = acc.num_pairs() as usize;

        // Find a split point where the key CHANGES.
        // Start at n/2 and scan forward until we hit a different key.
        // This ensures all versions of the same key stay on the same page.
        let mid = {
            let target = n / 2;
            let target_key = K::as_bytes(&acc.get_key(target)).as_ref().to_vec();
            let mut split_at = target;
            // Scan forward past all slots with the same key as target.
            while split_at < n {
                let key_val = acc.get_key(split_at);
                let k = K::as_bytes(&key_val);
                if K::compare(k.as_ref(), &target_key) != Ordering::Equal {
                    break;
                }
                split_at += 1;
            }
            // If we reached the end (all remaining keys are duplicates),
            // try scanning backward from target instead.
            if split_at >= n {
                split_at = target;
                while split_at > 0 {
                    let key_val = acc.get_key(split_at - 1);
                    let k = K::as_bytes(&key_val);
                    if K::compare(k.as_ref(), &target_key) != Ordering::Equal {
                        break;
                    }
                    split_at -= 1;
                }
            }
            // If split_at is 0, the entire page has the same key
            // (pathological case — can only happen if MAX versions of one key
            // fill the page). Fall back to n/2 and accept the cross-page split.
            if split_at == 0 { target } else { split_at }
        };

        let separator_key = K::as_bytes(&acc.get_key(mid)).as_ref().to_vec();
        let old_rightlink = acc.rightlink();
        let old_high_key: Option<Vec<u8>> = acc.high_key_bytes().map(|b| b.to_vec());

        // Snapshot ALL entries (including dead versions) to preserve MVCC history.
        let left_entries: Vec<(Vec<u8>, Vec<u8>, u64, u64)> = (0..mid)
            .map(|i| {
                let k = K::as_bytes(&acc.get_key(i)).as_ref().to_vec();
                let v = V::as_bytes(&acc.get_value(i)).as_ref().to_vec();
                (k, v, acc.get_xmin(i), acc.get_xmax(i))
            })
            .collect();

        // Allocate right page.
        let mut right_guard = self.pool.new_page()?;
        let right_pid = right_guard.page_id;

        {
            let acc = LeafPageAccessor::<K, V>::new(&leaf_guard[..]);
            let mut builder = LeafPageBuilder::<K, V>::new(right_pid, &mut right_guard[..]);
            if let Some(ref hk) = old_high_key {
                builder.set_high_key(hk);
            }
            builder.set_rightlink(old_rightlink);
            builder.set_prev_page(Some(leaf_pid));
            for i in mid..n {
                builder.push_with_mvcc(
                    &acc.get_key(i),
                    &acc.get_value(i),
                    acc.get_xmin(i),
                    acc.get_xmax(i),
                );
            }
            builder.finish();
        }

        // Fix the old neighbour's back-link; held until logging so its post-fix
        // image lands in the LeafSplit FPI set.
        let mut old_right_guard = match old_rightlink {
            Some(old_right_pid) => {
                let mut g = self.pool.fetch_page_mut(old_right_pid)?;
                LeafPageMutator::<K, V>::new(&mut g[..]).set_prev_page(Some(right_pid));
                Some(g)
            }
            None => None,
        };

        // Rebuild left page from scratch (high_key changes slot base).
        {
            let prev = LeafPageAccessor::<K, V>::new(&leaf_guard[..]).prev_page();
            let mut builder = LeafPageBuilder::<K, V>::new(leaf_pid, &mut leaf_guard[..]);
            builder.set_high_key(&separator_key);
            builder.set_rightlink(Some(right_pid));
            builder.set_prev_page(prev);
            for (k, v, xmin, xmax) in &left_entries {
                builder.push_with_mvcc(&K::from_bytes(k), &V::from_bytes(v), *xmin, *xmax);
            }
            builder.finish();
        }

        // INCOMPLETE_SPLIT until the downlink lands; captured in the left FPI.
        crate::page::set_incomplete_split(&mut leaf_guard[..]);

        // Atomic LeafSplit FPI set; stamp the record LSN on every touched page.
        let left_fpi = <&[u8; PAGE_SIZE]>::try_from(&leaf_guard[..]).unwrap();
        let right_fpi = <&[u8; PAGE_SIZE]>::try_from(&right_guard[..]).unwrap();
        let neigh = old_right_guard
            .as_ref()
            .map(|g| (g.page_id, <&[u8; PAGE_SIZE]>::try_from(&g[..]).unwrap()));
        let lsn = self.wal.lock().unwrap().log_leaf_split(
            SYSTEM_TXN_ID,
            (leaf_pid, left_fpi),
            (right_pid, right_fpi),
            neigh,
        )?;
        LeafPageMutator::<K, V>::new(&mut leaf_guard[..]).set_lsn(lsn);
        LeafPageMutator::<K, V>::new(&mut right_guard[..]).set_lsn(lsn);
        if let Some(g) = old_right_guard.as_mut() {
            LeafPageMutator::<K, V>::new(&mut g[..]).set_lsn(lsn);
        }
        drop(right_guard);
        drop(old_right_guard);

        Ok(SplitResult {
            separator_key,
            new_page_id: right_pid,
        })
    }

    /// Insert the downlink `(sep_key, right_pid)` into the parent of `left_child`
    /// (the page that split) and clear `left_child`'s INCOMPLETE_SPLIT flag.
    /// Idempotent: a downlink already present (concurrent completion / redo) is a
    /// no-op that still clears the flag.
    fn insert_separator_via_stack(
        &self,
        stack: &mut BTStack,
        mut sep_key: Vec<u8>,
        mut right_pid: PageId,
        mut left_child: PageId,
    ) -> Result<()> {
        loop {
            if stack.is_empty() {
                // The splitting page is the root unless one was created concurrently.
                if *self.root.lock().unwrap() != left_child {
                    crate::page::clear_incomplete_split(
                        &mut self.pool.fetch_page_mut(left_child)?[..],
                    );
                    return Ok(());
                }
                let mut new_root_guard = self.pool.new_page()?;
                let new_root_pid = new_root_guard.page_id;

                let mut builder =
                    InternalPageBuilder::<K>::new(new_root_pid, &mut new_root_guard[..]);
                builder.push_first_child(left_child);
                builder.push_key_and_right_child(&K::from_bytes(&sep_key), right_pid);
                builder.finish();

                // Point page 0 at the new root in memory.
                let mut meta_guard = self.pool.fetch_page_mut(0)?;
                crate::page::meta::set_root(&mut meta_guard[..], new_root_pid);

                // NewRoot: new-root FPI + page-0 pointer in one atomic record.
                let new_root_fpi = <&[u8; PAGE_SIZE]>::try_from(&new_root_guard[..]).unwrap();
                let lsn = self
                    .wal
                    .lock()
                    .unwrap()
                    .log_new_root(SYSTEM_TXN_ID, (new_root_pid, new_root_fpi))?;
                InternalPageMutator::<K>::new(&mut new_root_guard[..]).set_lsn(lsn);
                crate::page::meta::set_lsn(&mut meta_guard[..], lsn);
                drop(new_root_guard);
                drop(meta_guard);

                // Old root's split is now linked via the new root — clear its flag.
                {
                    let mut old = self.pool.fetch_page_mut(left_child)?;
                    crate::page::clear_incomplete_split(&mut old[..]);
                    crate::page::set_lsn(&mut old[..], lsn);
                }

                *self.root.lock().unwrap() = new_root_pid;
                return Ok(());
            }

            let entry = stack.pop().unwrap();
            let mut parent_pid = entry.page_id;

            let mut parent_guard = self.pool.fetch_page_mut(parent_pid)?;
            loop {
                let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
                if let Some(hk) = acc.high_key_bytes()
                    && K::compare(&sep_key, hk) != Ordering::Less
                {
                    let right = acc.rightlink().unwrap();
                    drop(parent_guard);
                    parent_pid = right;
                    parent_guard = self.pool.fetch_page_mut(parent_pid)?;
                    continue;
                }
                break;
            }

            // insert-if-absent: skip if the downlink is already present.
            let already = {
                let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
                let n = acc.num_keys() as usize;
                (0..=n).any(|i| acc.child_page_at(i) == right_pid)
            };
            if already {
                drop(parent_guard);
                crate::page::clear_incomplete_split(&mut self.pool.fetch_page_mut(left_child)?[..]);
                return Ok(());
            }

            let acc = InternalPageAccessor::<K>::new(&parent_guard[..]);
            let (idx, _) = acc.find_child(&K::from_bytes(&sep_key));
            if acc.can_fit(sep_key.len()) {
                InternalPageMutator::<K>::new(&mut parent_guard[..]).insert_key_and_right_child(
                    idx,
                    &K::from_bytes(&sep_key),
                    right_pid,
                )?;
                let lsn = self.wal.lock().unwrap().log_insert_downlink(
                    SYSTEM_TXN_ID,
                    parent_pid,
                    idx as u16,
                    &sep_key,
                    right_pid,
                    left_child,
                )?;
                InternalPageMutator::<K>::new(&mut parent_guard[..]).set_lsn(lsn);
                // Clear the child's flag — logged via the InsertDownlink child block.
                let mut child = self.pool.fetch_page_mut(left_child)?;
                crate::page::clear_incomplete_split(&mut child[..]);
                crate::page::set_lsn(&mut child[..], lsn);
                return Ok(());
            }

            // Parent splits: split_internal_ly fuses in the downlink. Clear the
            // child's flag in memory (self-heals on redo until that path logs it).
            let parent_split = self.split_internal_ly(&mut parent_guard, &sep_key, right_pid)?;
            drop(parent_guard);
            crate::page::clear_incomplete_split(&mut self.pool.fetch_page_mut(left_child)?[..]);

            sep_key = parent_split.separator_key;
            right_pid = parent_split.new_page_id;
            left_child = parent_pid;
        }
    }

    /// Complete an incomplete split lazily: insert the missing downlink for
    /// `child_pid` (insert-if-absent) and clear its flag. Re-checks under a write
    /// latch so a racing completer / already-finished split is a no-op.
    fn finish_split(&self, child_pid: PageId, stack: &BTStack) -> Result<()> {
        let mut guard = self.pool.fetch_page_mut(child_pid)?;
        if !crate::page::is_incomplete_split(&guard[..]) {
            return Ok(());
        }
        // separator = the page's high key; right sibling = its rightlink.
        let (sep_key, right_pid) = match guard[0] {
            LEAF => {
                let acc = LeafPageAccessor::<K, V>::new(&guard[..]);
                (acc.high_key_bytes().map(|b| b.to_vec()), acc.rightlink())
            }
            INTERNAL => {
                let acc = InternalPageAccessor::<K>::new(&guard[..]);
                (acc.high_key_bytes().map(|b| b.to_vec()), acc.rightlink())
            }
            _ => return Ok(()),
        };
        if let (Some(sep_key), Some(right_pid)) = (sep_key, right_pid) {
            drop(guard);
            self.insert_separator_via_stack(&mut stack.clone(), sep_key, right_pid, child_pid)
        } else {
            // Spurious flag (no right sibling) — clear it so descent can proceed.
            crate::page::clear_incomplete_split(&mut guard[..]);
            Ok(())
        }
    }

    fn split_internal_ly(
        &self,
        guard: &mut crate::buffer_pool::PageWriteGuard<'_>,
        sep_key: &[u8],
        right_child: PageId,
    ) -> Result<SplitResult> {
        let internal_pid = guard.page_id;
        let acc = InternalPageAccessor::<K>::new(&guard[..]);
        let n = acc.num_keys() as usize;

        let old_rightlink = acc.rightlink();
        let old_high_key: Option<Vec<u8>> = acc.high_key_bytes().map(|b| b.to_vec());

        let mut children: Vec<PageId> = (0..=n).map(|i| acc.child_page_at(i)).collect();
        let mut keys: Vec<Vec<u8>> = (0..n)
            .map(|i| K::as_bytes(&acc.key_at(i)).as_ref().to_vec())
            .collect();

        let insert_idx = {
            let mut lo = 0usize;
            let mut hi = keys.len();
            while lo < hi {
                let mid_i = lo + (hi - lo) / 2;
                if K::compare(&keys[mid_i], sep_key) == Ordering::Greater {
                    hi = mid_i;
                } else {
                    lo = mid_i + 1;
                }
            }
            lo
        };
        keys.insert(insert_idx, sep_key.to_vec());
        children.insert(insert_idx + 1, right_child);

        let total_keys = keys.len();
        let mid = total_keys / 2;
        let push_up_key = keys[mid].clone();

        let mut right_guard = self.pool.new_page()?;
        let right_pid = right_guard.page_id;
        {
            let mut builder = InternalPageBuilder::<K>::new(right_pid, &mut right_guard[..]);
            builder.push_first_child(children[mid + 1]);
            for i in (mid + 1)..total_keys {
                builder.push_key_and_right_child(&K::from_bytes(&keys[i]), children[i + 1]);
            }
            builder.set_rightlink(old_rightlink);
            if let Some(ref hk) = old_high_key {
                builder.set_high_key(hk);
            }
            builder.finish();
        }
        {
            let mut builder = InternalPageBuilder::<K>::new(internal_pid, &mut guard[..]);
            builder.set_rightlink(Some(right_pid));
            builder.set_high_key(&push_up_key);
            builder.push_first_child(children[0]);
            for i in 0..mid {
                builder.push_key_and_right_child(&K::from_bytes(&keys[i]), children[i + 1]);
            }
            builder.finish();
        }

        // INCOMPLETE_SPLIT until the downlink lands; captured in the left FPI.
        crate::page::set_incomplete_split(&mut guard[..]);

        // Atomic InternalSplit FPI set — left + new right.
        let left_fpi = <&[u8; PAGE_SIZE]>::try_from(&guard[..]).unwrap();
        let right_fpi = <&[u8; PAGE_SIZE]>::try_from(&right_guard[..]).unwrap();
        let lsn = self.wal.lock().unwrap().log_internal_split(
            SYSTEM_TXN_ID,
            (internal_pid, left_fpi),
            (right_pid, right_fpi),
        )?;
        InternalPageMutator::<K>::new(&mut guard[..]).set_lsn(lsn);
        InternalPageMutator::<K>::new(&mut right_guard[..]).set_lsn(lsn);
        drop(right_guard);

        Ok(SplitResult {
            separator_key: push_up_key,
            new_page_id: right_pid,
        })
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::manager::BufferPoolManager;
    use crate::disk::DiskManager;
    use crate::wal::Wal;
    use crate::wal::{WalIterator, WalRecordType};
    use common::MAX_PAGE_SIZE;
    use std::mem::forget;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::{Arc, Mutex, OnceLock};
    use tempfile::tempdir;

    /// Open a throwaway WAL under `dir`. The index and pool must share one WAL,
    /// so callers build it once here and clone the `Arc` to both.
    fn make_wal(dir: &Path) -> Arc<Mutex<Wal>> {
        Arc::new(Mutex::new(Wal::new(dir.join("wal.log")).unwrap()))
    }

    /// Wrap `disk` in a pool backed by `wal` (required for WAL-before-page).
    fn make_pool(disk: Arc<DiskManager>, wal: Arc<Mutex<Wal>>) -> Arc<BufferPoolManager> {
        Arc::new(BufferPoolManager::new(disk, wal))
    }

    fn make_index() -> BTreeIndex<&'static [u8], &'static [u8]> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let wal = make_wal(dir.path());
        forget(dir);
        let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
        let pool = make_pool(disk, wal.clone());
        let (index, _) = BTreeIndex::create(pool, wal).unwrap();
        index
    }

    fn auto() -> Transaction {
        static TM_LOCK: OnceLock<Arc<db_core::transaction_manager::TransactionManager>> =
            OnceLock::new();
        let tm = TM_LOCK
            .get_or_init(|| Arc::new(db_core::transaction_manager::TransactionManager::new()))
            .clone();

        static TEST_TXN_ID: AtomicU64 = AtomicU64::new(1);
        Transaction::new(
            TEST_TXN_ID.fetch_add(1, Relaxed),
            db_core::transaction::Snapshot::latest(),
            tm,
        )
    }

    fn leak_bytes(b: &[u8]) -> &'static [u8] {
        Box::leak(b.to_vec().into_boxed_slice())
    }

    // ── Basic get / insert ────────────────────────────────────────────────

    #[test]
    fn get_missing_key_returns_none() {
        let idx = make_index();
        assert!(idx.get(&(&b"hello"[..]), &auto()).unwrap().is_none());
    }

    #[test]
    fn insert_and_get_single_entry() {
        let idx = make_index();
        idx.insert(&(&b"key"[..]), &(&b"value"[..]), &auto())
            .unwrap();
        let got = idx.get(&(&b"key"[..]), &auto()).unwrap().unwrap();
        assert_eq!(got, b"value");
    }

    #[test]
    fn insert_duplicate_returns_error() {
        let idx = make_index();
        idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
        match idx.insert(&(&b"k"[..]), &(&b"v2"[..]), &auto()) {
            Err(IndexError::DuplicateKey) => {}
            other => panic!("expected DuplicateKey, got {:?}", other),
        }
    }

    // ── Update ───────────────────────────────────────────────────────────

    #[test]
    fn update_returns_new_value() {
        let idx = make_index();
        idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
        idx.update(&(&b"k"[..]), &(&b"v2"[..]), &auto()).unwrap();
        let got = idx.get(&(&b"k"[..]), &auto()).unwrap().unwrap();
        assert_eq!(got, b"v2");
    }

    #[test]
    fn update_missing_key_returns_error() {
        let idx = make_index();
        match idx.update(&(&b"nope"[..]), &(&b"v"[..]), &auto()) {
            Err(IndexError::KeyNotFound) => {}
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    // ── Delete ───────────────────────────────────────────────────────────

    #[test]
    fn delete_missing_key_returns_error() {
        let idx = make_index();
        match idx.delete(&(&b"nope"[..]), &auto()) {
            Err(IndexError::KeyNotFound) => {}
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn insert_then_delete() {
        let idx = make_index();
        idx.insert(&(&b"k"[..]), &(&b"v"[..]), &auto()).unwrap();
        idx.delete(&(&b"k"[..]), &auto()).unwrap();
        assert!(idx.get(&(&b"k"[..]), &auto()).unwrap().is_none());
    }

    #[test]
    fn insert_after_delete() {
        let idx = make_index();
        idx.insert(&(&b"k"[..]), &(&b"v1"[..]), &auto()).unwrap();
        idx.delete(&(&b"k"[..]), &auto()).unwrap();
        // After delete, insert a new version — should succeed (old is dead).
        let r = idx.insert(&(&b"k"[..]), &(&b"v2"[..]), &auto());
        assert!(r.is_ok(), "insert after delete failed: {:?}", r.err());
        let got = idx.get(&(&b"k"[..]), &auto()).unwrap();
        assert!(got.is_some(), "get returned None after insert-after-delete");
        assert_eq!(got.unwrap(), b"v2");
    }

    // ── Reopen / metadata page ───────────────────────────────────────────

    #[test]
    fn reopen_recovers_root_and_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reopen.db");

        // Create, insert enough to force splits (incl. a root split), flush, close.
        {
            let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
            let wal = make_wal(dir.path());
            let pool = make_pool(disk, wal.clone());
            let (index, _root) =
                BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal).unwrap();
            for k in 0u32..300 {
                let key = leak_bytes(&k.to_be_bytes());
                let val = leak_bytes(&(k * 7).to_be_bytes());
                index.insert(&key, &val, &auto()).unwrap();
            }
            pool.flush_all_pages().unwrap();
        }

        // Reopen with NO root id — it must be recovered from the page-0 superblock.
        let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
        let wal = make_wal(dir.path());
        let pool = make_pool(disk, wal.clone());
        let index = BTreeIndex::<&'static [u8], &'static [u8]>::open(pool, wal).unwrap();
        for k in 0u32..300 {
            let key = leak_bytes(&k.to_be_bytes());
            let expected = (k * 7).to_be_bytes();
            let got = index.get(&key, &auto()).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(&expected[..]),
                "key {k} wrong after reopen"
            );
        }
    }

    #[test]
    fn reopen_after_create_makes_root_durable() {
        // `create` alone (no inserts, no flush_all) must make BOTH the superblock
        // and the root page durable — otherwise a cold reopen can't find the root.
        let dir = tempdir().unwrap();
        let path = dir.path().join("fresh.db");
        {
            let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
            let wal = make_wal(dir.path());
            let pool = make_pool(disk, wal.clone());
            let _ = BTreeIndex::<&'static [u8], &'static [u8]>::create(pool, wal).unwrap();
        }
        let disk = Arc::new(DiskManager::new(&path, MAX_PAGE_SIZE).unwrap());
        let wal = make_wal(dir.path());
        let pool = make_pool(disk, wal.clone());
        let index = BTreeIndex::<&'static [u8], &'static [u8]>::open(pool, wal).unwrap();
        assert!(index.get(&(&b"anything"[..]), &auto()).unwrap().is_none());
    }

    // ── Sequential inserts / splits ──────────────────────────────────────

    #[test]
    fn sequential_inserts_all_readable() {
        let idx = make_index();
        for i in 0u64..200 {
            let k = i.to_be_bytes();
            let v = (i * 10).to_be_bytes();
            idx.insert(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
        }
        for i in 0u64..200 {
            let k = i.to_be_bytes();
            let expected = (i * 10).to_be_bytes();
            let got = idx
                .get(&(k.as_ref()), &auto())
                .unwrap()
                .unwrap_or_else(|| panic!("key {} missing", i));
            assert_eq!(got, expected.as_ref());
        }
    }

    #[test]
    fn large_insert_all_keys_readable() {
        let idx = make_index();
        for i in 0u32..2000 {
            let k = i.to_be_bytes();
            let v = (i + 1).to_be_bytes();
            idx.insert(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
        }
        for i in 0u32..2000 {
            let k = i.to_be_bytes();
            let expected = (i + 1).to_be_bytes();
            let got = idx
                .get(&(k.as_ref()), &auto())
                .unwrap()
                .unwrap_or_else(|| panic!("key {} missing", i));
            assert_eq!(got, expected.as_ref());
        }
    }

    #[test]
    fn reverse_order_inserts_all_readable() {
        let idx = make_index();
        for i in (0u32..500).rev() {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        for i in 0u32..500 {
            let k = i.to_be_bytes();
            let got = idx
                .get(&(k.as_ref()), &auto())
                .unwrap()
                .unwrap_or_else(|| panic!("key {} missing", i));
            assert_eq!(got, k.as_ref());
        }
    }

    #[test]
    fn descent_finishes_incomplete_split() {
        let idx = make_index();
        // Build a multi-level tree with gaps (even keys) so an odd key routes
        // into an existing leaf.
        for i in (0u32..2000).step_by(2) {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }

        // Leftmost leaf — it has split, so it carries a rightlink + high key.
        let mut pid = idx.root_page_id();
        let leftmost = loop {
            let page = idx.pool.fetch_page(pid).unwrap();
            if page[0] == LEAF {
                break pid;
            }
            let child = InternalPageAccessor::<&[u8]>::new(&page[..]).child_page_at(0);
            drop(page);
            pid = child;
        };
        assert!(
            LeafPageAccessor::<&[u8], &[u8]>::new(&idx.pool.fetch_page(leftmost).unwrap()[..])
                .rightlink()
                .is_some(),
            "test needs a split tree",
        );

        // Simulate a recovered/raced incomplete split: flag set, downlink present.
        crate::page::set_incomplete_split(&mut idx.pool.fetch_page_mut(leftmost).unwrap()[..]);
        assert!(crate::page::is_incomplete_split(
            &idx.pool.fetch_page(leftmost).unwrap()[..]
        ));

        // An insert that descends through the leaf must finish the split.
        let k1 = 1u32.to_be_bytes();
        idx.insert(&(k1.as_ref()), &(k1.as_ref()), &auto()).unwrap();

        assert!(
            !crate::page::is_incomplete_split(&idx.pool.fetch_page(leftmost).unwrap()[..]),
            "descent should have cleared the flag",
        );

        // No duplicate downlink, no lost data.
        assert_eq!(
            idx.get(&(k1.as_ref()), &auto()).unwrap().unwrap(),
            k1.as_ref()
        );
        for i in (0u32..2000).step_by(2) {
            let k = i.to_be_bytes();
            assert!(
                idx.get(&(k.as_ref()), &auto()).unwrap().is_some(),
                "key {} missing",
                i
            );
        }
    }

    // ── Delete bulk ──────────────────────────────────────────────────────

    #[test]
    fn delete_half_keys_remaining_readable() {
        let idx = make_index();
        for i in 0u32..200 {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        for i in (0u32..200).filter(|x| x % 2 == 0) {
            let k = i.to_be_bytes();
            idx.delete(&(k.as_ref()), &auto()).unwrap();
        }
        for i in (0u32..200).filter(|x| x % 2 == 1) {
            let k = i.to_be_bytes();
            idx.get(&(k.as_ref()), &auto())
                .unwrap()
                .unwrap_or_else(|| panic!("odd key {} missing", i));
        }
        for i in (0u32..200).filter(|x| x % 2 == 0) {
            let k = i.to_be_bytes();
            assert!(idx.get(&(k.as_ref()), &auto()).unwrap().is_none());
        }
    }

    // ── Update bulk ──────────────────────────────────────────────────────

    #[test]
    fn stress_update_all_keys() {
        let idx = make_index();
        let n = 200u32;
        for i in 0..n {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        for i in 0..n {
            let k = i.to_be_bytes();
            let v = (i + 1000).to_be_bytes();
            idx.update(&(k.as_ref()), &(v.as_ref()), &auto()).unwrap();
        }
        for i in 0..n {
            let k = i.to_be_bytes();
            let expected = (i + 1000).to_be_bytes();
            let got = idx
                .get(&(k.as_ref()), &auto())
                .unwrap()
                .unwrap_or_else(|| panic!("key {} missing after update", i));
            assert_eq!(got, expected.as_ref());
        }
    }

    // ── Range scan ───────────────────────────────────────────────────────

    #[test]
    fn range_scan_full() {
        let idx = make_index();
        for i in 0u32..100 {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        let results: Vec<_> = idx
            .range::<std::ops::RangeFull>(.., &auto())
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(results.len(), 100);
    }

    #[test]
    fn range_scan_skips_deleted() {
        let idx = make_index();
        for i in 0u32..10 {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        for &i in &[3u32, 5, 7] {
            let k = i.to_be_bytes();
            idx.delete(&(k.as_ref()), &auto()).unwrap();
        }
        let results: Vec<_> = idx
            .range::<std::ops::RangeFull>(.., &auto())
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(results.len(), 7);
    }

    #[test]
    fn range_scan_bounded() {
        let idx = make_index();
        for i in 0u32..100 {
            let k = i.to_be_bytes();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &auto()).unwrap();
        }
        let start: &'static [u8] = leak_bytes(&10u32.to_be_bytes());
        let end: &'static [u8] = leak_bytes(&20u32.to_be_bytes());
        let results: Vec<_> = idx.range(start..end, &auto()).map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn range_scan_included_start_can_miss_only_visible_duplicate_before_position_result() {
        let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
        let idx = make_index();

        let duplicate_key: &'static [u8] = leak_bytes(&40u32.to_be_bytes());
        let after_duplicate_key: &'static [u8] = leak_bytes(&50u32.to_be_bytes());
        let after_duplicate_value: &'static [u8] = leak_bytes(&50u32.to_be_bytes());
        let value_1: &'static [u8] = leak_bytes(&1u32.to_be_bytes());
        let value_2: &'static [u8] = leak_bytes(&2u32.to_be_bytes());
        let value_3: &'static [u8] = leak_bytes(&3u32.to_be_bytes());

        let seed_txn = tm.begin();
        idx.insert(&after_duplicate_key, &after_duplicate_value, &seed_txn)
            .unwrap();
        tm.mark_committed(seed_txn.txn_id);

        let txn1 = tm.begin();
        idx.insert(&duplicate_key, &value_1, &txn1).unwrap();
        tm.mark_committed(txn1.txn_id);

        let txn2 = tm.begin();
        idx.update(&duplicate_key, &value_2, &txn2).unwrap();
        tm.mark_committed(txn2.txn_id);

        let reader = tm.begin();

        let txn3 = tm.begin();
        idx.update(&duplicate_key, &value_3, &txn3).unwrap();

        {
            let root = idx.root_page_id();
            let leaf = idx.pool.fetch_page(root).unwrap();
            let acc = LeafPageAccessor::<&'static [u8], &'static [u8]>::new(&leaf[..]);

            assert_eq!(acc.num_pairs(), 4);
            assert_eq!(acc.get_value(0), value_2);
            assert_eq!(acc.get_value(1), value_3);
            assert_eq!(acc.get_value(2), value_1);
            assert_eq!(acc.get_value(3), after_duplicate_value);

            assert!(reader.is_visible(acc.get_xmin(0), acc.get_xmax(0)));
            assert!(!reader.is_visible(acc.get_xmin(1), acc.get_xmax(1)));
            assert!(!reader.is_visible(acc.get_xmin(2), acc.get_xmax(2)));

            let acc = LeafPageAccessor::<&'static [u8], &'static [u8]>::new(&leaf[..]);
            assert_eq!(
                acc.position(&duplicate_key).0,
                2,
                "test setup expects position(40) to land after the visible duplicate"
            );
        }

        let results: Vec<_> = idx
            .range(duplicate_key..=duplicate_key, &reader)
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            results,
            vec![(duplicate_key.to_vec(), value_2.to_vec())],
            "range scan should return the only visible physical record for key 40"
        );
    }

    // ── Write conflict ───────────────────────────────────────────────────

    #[test]
    fn write_conflict_on_concurrent_delete() {
        let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
        let idx = make_index();

        // 1. Insert a key.
        let insert_txn = tm.begin();
        idx.insert(&(&b"k"[..]), &(&b"v"[..]), &insert_txn).unwrap();
        tm.mark_committed(insert_txn.txn_id);

        // 2. Start txn10 and delete the key.
        let txn10 = tm.begin();
        idx.delete(&(&b"k"[..]), &txn10).unwrap();

        // 3. Start txn20. It should see txn10 as active.
        let txn20 = tm.begin();

        match idx.delete(&(&b"k"[..]), &txn20) {
            Err(IndexError::WriteConflict) => {}
            other => panic!("expected WriteConflict, got {:?}", other),
        }
    }

    #[test]
    fn vacuum_reclaims_space() {
        let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
        let idx = make_index();

        // 1. Insert 100 keys and commit.
        for i in 0u32..100 {
            let k = i.to_be_bytes();
            let txn = tm.begin();
            idx.insert(&(k.as_ref()), &(k.as_ref()), &txn).unwrap();
            tm.mark_committed(txn.txn_id);
        }

        // 2. Delete 50 keys and commit.
        for i in 0u32..50 {
            let k = i.to_be_bytes();
            let txn = tm.begin();
            idx.delete(&(k.as_ref()), &txn).unwrap();
            tm.mark_committed(txn.txn_id);
        }

        // 3. Run vacuum. Since all transactions committed, it should reclaim 50 records.
        let removed = idx.vacuum(&tm).unwrap();
        assert_eq!(removed, 50);

        // 4. Verify data is still visible for the 50 live keys.
        let results: Vec<_> = idx
            .range::<std::ops::RangeFull>(.., &tm.begin())
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(results.len(), 50);
    }

    #[test]
    fn compact_prevents_split() {
        let tm = std::sync::Arc::new(db_core::transaction_manager::TransactionManager::new());
        let idx = make_index();

        for i in 0u32..80 {
            let k = i.to_be_bytes();
            let v = vec![0xAA; 40]; // padding to fill page
            let txn = tm.begin();
            idx.insert(&(k.as_ref()), &(v.as_ref()), &txn).unwrap();
            tm.mark_committed(txn.txn_id);
        }

        let root_before = idx.root_page_id();

        for i in 0u32..40 {
            let k = i.to_be_bytes();
            let txn = tm.begin();
            idx.delete(&(k.as_ref()), &txn).unwrap();
            tm.mark_committed(txn.txn_id);
        }

        let txn = tm.begin();
        let k = 999u32.to_be_bytes();
        let v = vec![0xBB; 40];
        idx.insert(&(k.as_ref()), &(v.as_ref()), &txn).unwrap();
        tm.mark_committed(txn.txn_id);

        let root_after = idx.root_page_id();
        assert_eq!(
            root_before, root_after,
            "Split should have been avoided via compaction"
        );

        let result = idx.get(&(k.as_ref()), &tm.begin()).unwrap();
        assert!(result.is_some(), "Inserted record should be readable");
    }
    // ── ValueTooLarge guard ──────────────────────────────────────────────

    #[test]
    fn insert_and_update_reject_oversized_value() {
        let idx = make_index();
        let k: &[u8] = b"key";
        let oversized = vec![0xFFu8; MAX_VALUE_SIZE + 1];

        // insert should reject
        let err = idx.insert(&k, &oversized.as_slice(), &auto()).unwrap_err();
        assert!(matches!(err, IndexError::ValueTooLarge { size, max }
            if size == MAX_VALUE_SIZE + 1 && max == MAX_VALUE_SIZE));

        // insert a small value so we have something to update
        let v: &[u8] = b"small";
        idx.insert(&k, &v, &auto()).unwrap();

        // update should also reject
        let err = idx.update(&k, &oversized.as_slice(), &auto()).unwrap_err();
        assert!(matches!(err, IndexError::ValueTooLarge { size, max }
            if size == MAX_VALUE_SIZE + 1 && max == MAX_VALUE_SIZE));
    }

    // ── WAL: physiological Insert logging brings the flush gate to life ───────

    #[test]
    fn insert_logs_record_stamps_page_lsn_and_gate_flushes() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("wal_insert.db");
        let wal = make_wal(dir.path());
        let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
        let pool = make_pool(disk, wal.clone());
        let (index, root) =
            BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

        // `create` logs nothing, so the LSN counter starts at 0.
        assert_eq!(wal.lock().unwrap().next_lsn, 0);

        // Two in-place inserts on the same leaf → two Insert records (LSN 0, 1).
        index.insert(&(&b"a"[..]), &(&b"1"[..]), &auto()).unwrap();
        index.insert(&(&b"b"[..]), &(&b"2"[..]), &auto()).unwrap();
        assert_eq!(
            wal.lock().unwrap().next_lsn,
            2,
            "each insert appends a record"
        );

        // The leaf page must carry the latest insert's LSN (set_lsn under the latch).
        let leaf = pool.fetch_page(root).unwrap();
        assert_eq!(crate::page::page_lsn(&leaf[..]), 1, "page LSN stamped");
        drop(leaf);

        // Flushing the dirty leaf must drive the WAL durable through that page LSN
        // (the WAL-before-page gate firing on a real, non-zero LSN).
        pool.flush_all_pages().unwrap();
        assert_eq!(
            wal.lock().unwrap().flushed_lsn,
            Some(1),
            "gate flushed WAL to page LSN"
        );
    }

    #[test]
    fn delete_emits_one_setxmax() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("wal_insert.db");
        let wal = make_wal(dir.path());
        let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
        let pool = make_pool(disk, wal.clone());
        let (index, root) =
            BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

        let txn = auto();
        let key: &[u8] = b"a";
        let val: &[u8] = b"1";
        index.insert(&key, &val, &auto()).unwrap();

        let n = wal.lock().unwrap().next_lsn;
        assert_eq!(n, 1);
        index.delete(&key, &txn).unwrap();
        assert!(
            wal.lock().unwrap().next_lsn == n + 1,
            "delete appends one record"
        );

        // The leaf page must carry the latest delete's LSN (set_lsn under the latch).
        let leaf = pool.fetch_page(root).unwrap();
        assert_eq!(crate::page::page_lsn(&leaf[..]), 1, "page LSN stamped");
        drop(leaf);

        // Flushing the dirty leaf must drive the WAL durable through that page LSN
        // (the WAL-before-page gate firing on a real, non-zero LSN).
        pool.flush_all_pages().unwrap();
        let mut it = WalIterator::new(dir.path().join("wal.log")).unwrap();
        it.next_record();
        let rec = it.next_record().unwrap().unwrap();
        assert!(rec.entry_type == WalRecordType::SetXMax);
        assert!(rec.txn_id == txn.txn_id);
        assert!(rec.blocks.len() == 1);
        assert!(rec.blocks[0].page_id == root);
        let data = rec.blocks[0].data.unwrap();
        assert!(u16::from_le_bytes(data[0..2].try_into().unwrap()) == 0);
        assert!(u64::from_le_bytes(data[2..10].try_into().unwrap()) == txn.txn_id);
    }

    #[test]
    fn update_emits_setxmax_then_insert_in_lsn_order() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("wal_insert.db");
        let wal = make_wal(dir.path());
        let disk = Arc::new(DiskManager::new(&db, MAX_PAGE_SIZE).unwrap());
        let pool = make_pool(disk, wal.clone());
        let (index, root) =
            BTreeIndex::<&'static [u8], &'static [u8]>::create(pool.clone(), wal.clone()).unwrap();

        let txn = auto();
        let key: &[u8] = b"a";
        let val: &[u8] = b"1";
        let new_val: &[u8] = b"2";
        index.insert(&key, &val, &auto()).unwrap();

        let n = wal.lock().unwrap().next_lsn;
        assert_eq!(n, 1);

        index.update(&key, &new_val, &txn).unwrap();
        assert!(
            wal.lock().unwrap().next_lsn == n + 2,
            "update appends two records"
        );

        // The leaf page must carry the latest delete's LSN (set_lsn under the latch).
        let leaf = pool.fetch_page(root).unwrap();
        assert_eq!(crate::page::page_lsn(&leaf[..]), n + 1, "page LSN stamped");
        drop(leaf);

        pool.flush_all_pages().unwrap();
        let mut it = WalIterator::new(dir.path().join("wal.log")).unwrap();
        it.next_record();

        let rec = it.next_record().unwrap().unwrap(); // SetXmax (LSN 1)
        assert_eq!(rec.entry_type, WalRecordType::SetXMax);
        assert_eq!(rec.txn_id, txn.txn_id);
        let xmax_lsn = rec.lsn;

        let rec = it.next_record().unwrap().unwrap(); // Insert (LSN 2)
        assert_eq!(rec.entry_type, WalRecordType::Insert);
        let ins_lsn = rec.lsn;

        assert!(
            xmax_lsn < ins_lsn,
            "SetXmax must be logged before the new Insert"
        );
    }
}
