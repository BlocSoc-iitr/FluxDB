//! Leaf (data) page types for the B+Tree storage engine.
//!
//! ## Layout (Lehman-Yao)
//!
//! ```text
//! Page size: 4096 bytes
//!
//! ┌─────────────────────────────────────────────────────────┐
//! │ FIXED HEADER — 48 bytes (fully 8-byte aligned)          │
//! ├────────┬───────────┬─────────────────────────────────── ┤
//! │ Off  0 │ u8        │ page_type                          │
//! │ Off  1 │ u8        │ _reserved (0)                      │
//! │ Off  2 │ u16       │ slot_count                         │
//! │ Off  4 │ u16       │ free_start  (end of slot directory)│
//! │ Off  6 │ u16       │ free_end    (start of record area) │
//! ├────────┼───────────┼─────────────────────────────────── ┤
//! │ Off  8 │ u64       │ page_id          [8-byte aligned]  │
//! │ Off 16 │ u64       │ lsn              [8-byte aligned]  │
//! │ Off 24 │ u64       │ prev_page        [8-byte aligned]  │
//! │ Off 32 │ u64       │ rightlink        [8-byte aligned]  │
//! │ Off 40 │ u16       │ high_key_len (0 = +∞ / rightmost)  │
//! │ Off 42 │ u16       │ _padding                           │
//! │ Off 44 │ u32       │ checksum (CRC32)                   │
//! └────────┴───────────┴─────────────────────────────────── ┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ HIGH KEY DATA  [high_key_len bytes, right after header]  │
//! │  page[48 .. 48 + high_key_len]                           │
//! └──────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ SLOT DIRECTORY  [slot_count × 4 bytes, grows →]          │
//! │  slot[i] = (offset: u16, rec_size: u16)                  │
//! │  slot[i] byte offset = slot_base + i*4                   │
//! │  where slot_base = 48 + ((high_key_len + 1) & !1)        │
//! └──────────────────────────────────────────────────────────┘
//!
//!          ↕  free space (free_end − free_start bytes)
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ RECORD DATA AREA  [grows ←]                              │
//! │  ┌──────┬──────┬──────┬──────┬──────┬──────────┬───────┐ │
//! │  │ u16  │ u16  │  u8  │  u8  │  u16 │  u64     │  u64  │ │
//! │  │k_len │v_len │r_type│ _pad │ _pad │  xmin    │  xmax │ │
//! │  └──────┴──────┴──────┴──────┴──────┴──────────┴───────┘ │
//! │    2B     2B     1B     1B     2B      8B         8B      │
//! │  + key bytes (k_len) + val bytes (v_len)                  │
//! │  Fixed record header = 24 bytes                           │
//! │  rec_type = 0 (REC_TYPE_INLINE) or 1 (REC_TYPE_OVERFLOW)  │
//! │  xmin = creating transaction ID                           │
//! │  xmax = deleting/replacing transaction ID (0 = live)      │
//! └──────────────────────────────────────────────────────────┘
//! ```

use common::{Key, Value};
use db_core::{transaction, transaction_manager::TransactionManager};
use std::cmp::Ordering;
use std::marker::PhantomData;

use super::overflow::OverflowDescriptor;
use super::{
    LEAF, Lsn, OFF_LSN, OFF_PAGE_ID, OFF_PAGE_TYPE, PAGE_SIZE, PageError, PageId, read_u8,
    read_u16, read_u64, write_u8, write_u16, write_u64,
};

// ── Leaf-page-specific header offsets ────────────────────────────────────────

const OFF_LEAF_SLOT_COUNT: usize = 2; // u16
const OFF_LEAF_FREE_START: usize = 4; // u16
const OFF_LEAF_FREE_END: usize = 6; // u16
// OFF_PAGE_ID at 8, OFF_LSN at 16 (shared)
const OFF_LEAF_PREV: usize = 24; // u64
const OFF_LEAF_RIGHTLINK: usize = 32; // u64 (was next_page)
const OFF_LEAF_HIGH_KEY_LEN: usize = 40; // u16
// bytes 42..48: padding
const LEAF_HEADER_SIZE: usize = 48;

const SLOT_SIZE: usize = 4; // u16 offset + u16 rec_size

// ── Record layout offsets (relative to record base) ──────────────────────────

const REC_OFF_KEY_LEN: usize = 0; // u16
const REC_OFF_VAL_LEN: usize = 2; // u16
const REC_OFF_REC_TYPE: usize = 4; // u8 — 0 = inline value, 1 = overflow pointer
// bytes 5..8: padding (u8 + u16, always 0)
const REC_OFF_XMIN: usize = 8; // u64 — creating transaction ID
const REC_OFF_XMAX: usize = 16; // u64 — deleting/replacing transaction ID (0 = live)
const REC_HEADER_SIZE: usize = 24; // 2+2+1+1+2+8+8 = 24 bytes

pub const REC_TYPE_INLINE: u8 = 0;
pub const REC_TYPE_OVERFLOW: u8 = 1;

// ── Layout helpers ───────────────────────────────────────────────────────────

/// Byte offset where the slot directory starts, accounting for the high key
/// region that sits between the header and the slot directory.
#[inline]
fn slot_base(high_key_len: usize) -> usize {
    LEAF_HEADER_SIZE + ((high_key_len + 1) & !1) // 2-byte aligned
}

#[inline]
fn slot_offset_at(high_key_len: usize, i: usize) -> usize {
    slot_base(high_key_len) + i * SLOT_SIZE
}

#[inline]
fn rec_key_offset(rec_base: usize) -> usize {
    rec_base + REC_HEADER_SIZE
}

/// Val start is offset by key_len rounded up to the next even byte.
#[inline]
fn rec_val_offset(rec_base: usize, key_len: usize) -> usize {
    rec_base + REC_HEADER_SIZE + ((key_len + 1) & !1)
}

/// Total record size rounded up to the next even byte so the next record stays
/// 2-byte aligned.
#[inline]
fn rec_total_size(key_len: usize, val_len: usize) -> usize {
    let raw = REC_HEADER_SIZE + ((key_len + 1) & !1) + val_len;
    (raw + 1) & !1
}

// ── LeafPageAccessor ──────────────────────────────────────────────────────────

/// Read-only typed view over a raw leaf page buffer.
///
/// The lifetime `'a` is tied to the underlying page data, not to `&self`,
/// so `get_key` / `get_value` can return zero-copy borrows that outlive the
/// accessor itself.
pub struct LeafPageAccessor<'a, K: Key, V: Value> {
    data: &'a [u8],
    slot_base: usize,
    num_pairs: u16,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<'a, K: Key, V: Value> LeafPageAccessor<'a, K, V> {
    /// Wraps a raw page buffer.
    ///
    /// # Panics
    /// Panics if the page-type byte does not equal [`LEAF`].
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            LEAF,
            "LeafPageAccessor: page type byte is not LEAF"
        );
        let high_key_len = read_u16(data, OFF_LEAF_HIGH_KEY_LEN) as usize;
        let num_pairs = read_u16(data, OFF_LEAF_SLOT_COUNT);
        Self {
            data,
            slot_base: slot_base(high_key_len),
            num_pairs,
            _key: PhantomData,
            _val: PhantomData,
        }
    }

    // ── Page-level metadata ───────────────────────────────────────────────────

    pub fn page_id(&self) -> PageId {
        read_u64(self.data, OFF_PAGE_ID)
    }

    pub fn lsn(&self) -> Lsn {
        read_u64(self.data, OFF_LSN)
    }

    pub fn num_pairs(&self) -> u16 {
        self.num_pairs
    }

    /// Previous leaf in the doubly-linked leaf chain, or `None` if this is the
    /// leftmost leaf.
    pub fn prev_page(&self) -> Option<PageId> {
        match read_u64(self.data, OFF_LEAF_PREV) {
            0 => None,
            v => Some(v),
        }
    }

    /// Right sibling (Lehman-Yao rightlink), or `None` if this is the
    /// rightmost leaf.
    pub fn rightlink(&self) -> Option<PageId> {
        match read_u64(self.data, OFF_LEAF_RIGHTLINK) {
            0 => None,
            v => Some(v),
        }
    }

    // ── High key ──────────────────────────────────────────────────────────────

    /// Length of the high key in bytes. 0 means this is the rightmost leaf
    /// (high key is +infinity).
    pub fn high_key_len(&self) -> u16 {
        read_u16(self.data, OFF_LEAF_HIGH_KEY_LEN)
    }

    /// Raw high key bytes, or `None` if this is the rightmost leaf (+infinity).
    pub fn high_key_bytes(&self) -> Option<&'a [u8]> {
        let len = self.high_key_len() as usize;
        if len == 0 {
            return None;
        }
        Some(&self.data[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + len])
    }

    /// Deserialized high key, or `None` if rightmost (+infinity).
    pub fn high_key(&self) -> Option<K::SelfType<'a>> {
        self.high_key_bytes().map(K::from_bytes)
    }

    // ── Free-space accounting ─────────────────────────────────────────────────

    pub fn free_start(&self) -> usize {
        read_u16(self.data, OFF_LEAF_FREE_START) as usize
    }

    pub fn free_end(&self) -> usize {
        read_u16(self.data, OFF_LEAF_FREE_END) as usize
    }

    /// Contiguous free bytes between the slot directory and the record area.
    pub fn free_space(&self) -> usize {
        self.free_end() - self.free_start()
    }

    /// `true` if `(key_len, val_len)` fits in the current contiguous free gap
    /// without needing a `compact()`.
    pub fn can_fit_direct(&self, key_len: usize, val_len: usize) -> bool {
        self.free_space() >= SLOT_SIZE + rec_total_size(key_len, val_len)
    }

    /// Returns `true` if this leaf has so few live (non-deleted) bytes that it
    /// should donate to or merge with a sibling.
    pub fn is_underfull(&self) -> bool {
        let n = self.num_pairs() as usize;
        if n == 0 {
            return true;
        }
        let mut live = 0usize;
        for i in 0..n {
            if !self.is_deleted(i) {
                live += self.slot_rec_size(i) + SLOT_SIZE;
            }
        }
        live * 2 < (PAGE_SIZE - self.slot_base)
    }

    // ── Binary search ─────────────────────────────────────────────────────────

    /// Binary-search the sorted slot directory for `query`.
    ///
    /// Returns `(index, true)` on exact match.
    /// Returns `(index, false)` when absent; `index` is the correct insertion
    /// point (first slot whose key is greater than `query`).
    pub fn position(&self, query: &K::SelfType<'_>) -> (usize, bool) {
        let mut lo: usize = 0;
        let mut hi: usize = self.num_pairs() as usize;
        let query_bytes = K::as_bytes(query);
        let query_bytes = query_bytes.as_ref();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match K::compare(self.key_bytes_at(mid), query_bytes) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return (mid, true),
            }
        }
        (lo, false)
    }

    /// Returns the slot index of `query`, or `None` if absent.
    pub fn find_key(&self, query: &K::SelfType<'_>) -> Option<usize> {
        let (idx, found) = self.position(query);
        if found { Some(idx) } else { None }
    }

    // ── Record data access ────────────────────────────────────────────────────

    /// Deserialised key at slot `i`. Lifetime is `'a` (zero-copy borrow).
    pub fn get_key(&self, i: usize) -> K::SelfType<'a> {
        K::from_bytes(self.key_bytes_at(i))
    }

    /// Deserialised value at slot `i`. Lifetime is `'a` (zero-copy borrow).
    pub fn get_value(&self, i: usize) -> V::SelfType<'a> {
        V::from_bytes(self.value_bytes_at(i))
    }

    /// Both key and value at slot `i` as a tuple.
    pub fn entry(&self, i: usize) -> (K::SelfType<'a>, V::SelfType<'a>) {
        (self.get_key(i), self.get_value(i))
    }

    /// Creating transaction ID of the record at slot `i`.
    pub fn get_xmin(&self, i: usize) -> u64 {
        let rec_base = self.slot_rec_base(i);
        read_u64(self.data, rec_base + REC_OFF_XMIN)
    }

    /// Deleting/replacing transaction ID of the record at slot `i`.
    /// 0 means the record is live (not deleted or replaced).
    pub fn get_xmax(&self, i: usize) -> u64 {
        let rec_base = self.slot_rec_base(i);
        read_u64(self.data, rec_base + REC_OFF_XMAX)
    }

    /// Returns `true` if the record at slot `i` has been deleted or replaced
    /// (xmax != 0). Use `is_visible()` from the transaction module for
    /// proper MVCC visibility checks.
    pub fn is_deleted(&self, i: usize) -> bool {
        self.get_xmax(i) != 0
    }

    /// Record type byte at slot `i`.
    pub fn get_rec_type(&self, i: usize) -> u8 {
        let rec_base = self.slot_rec_base(i);
        read_u8(self.data, rec_base + REC_OFF_REC_TYPE)
    }
    /// Returns `true` if the value at slot `i` is stored in an overflow page
    /// chain rather than inline.
    pub fn is_overflow(&self, i: usize) -> bool {
        self.get_rec_type(i) == REC_TYPE_OVERFLOW
    }

    /// Raw stored value bytes at slot `i`, without `V` deserialization.
    ///
    /// For an inline record these are the value bytes; for an overflow record
    /// they are the serialized [`OverflowDescriptor`]. Used by paths that move
    /// records verbatim (split / compact) and by the index when reconstructing
    /// overflow values.
    pub fn raw_value(&self, i: usize) -> &'a [u8] {
        self.value_bytes_at(i)
    }

    /// Decoded [`OverflowDescriptor`] for slot `i`, or `None` if the record is
    /// inline or its descriptor bytes are malformed.
    pub fn overflow_descriptor(&self, i: usize) -> Option<OverflowDescriptor> {
        if !self.is_overflow(i) {
            return None;
        }
        OverflowDescriptor::from_bytes(self.value_bytes_at(i))
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn slot_rec_base(&self, i: usize) -> usize {
        read_u16(self.data, self.slot_base + i * SLOT_SIZE) as usize
    }

    pub(crate) fn slot_rec_size(&self, i: usize) -> usize {
        read_u16(self.data, self.slot_base + i * SLOT_SIZE + 2) as usize
    }

    fn key_bytes_at(&self, i: usize) -> &'a [u8] {
        let rec_base = self.slot_rec_base(i);
        let key_len = read_u16(self.data, rec_base + REC_OFF_KEY_LEN) as usize;
        let key_off = rec_key_offset(rec_base);
        &self.data[key_off..key_off + key_len]
    }

    fn value_bytes_at(&self, i: usize) -> &'a [u8] {
        let rec_base = self.slot_rec_base(i);
        let key_len = read_u16(self.data, rec_base + REC_OFF_KEY_LEN) as usize;
        let val_len = read_u16(self.data, rec_base + REC_OFF_VAL_LEN) as usize;
        let val_off = rec_val_offset(rec_base, key_len);
        &self.data[val_off..val_off + val_len]
    }
}

// ── LeafPageMutator ───────────────────────────────────────────────────────────

/// Mutable typed view over a raw leaf page. All structural mutations go here.
pub struct LeafPageMutator<'a, K: Key, V: Value> {
    data: &'a mut [u8],
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<'a, K: Key, V: Value> LeafPageMutator<'a, K, V> {
    /// # Panics
    /// Panics if the page-type byte does not equal [`LEAF`].
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            LEAF,
            "LeafPageMutator: page type byte is not LEAF"
        );
        Self {
            data,
            _key: PhantomData,
            _val: PhantomData,
        }
    }

    // ── Header setters ────────────────────────────────────────────────────────

    pub fn set_lsn(&mut self, lsn: Lsn) {
        write_u64(self.data, OFF_LSN, lsn);
    }

    pub fn set_prev_page(&mut self, prev: Option<PageId>) {
        write_u64(self.data, OFF_LEAF_PREV, prev.unwrap_or(0));
    }

    pub fn set_rightlink(&mut self, right: Option<PageId>) {
        write_u64(self.data, OFF_LEAF_RIGHTLINK, right.unwrap_or(0));
    }

    /// Write the high key. `key_bytes` is stored right after the header.
    /// Pass `&[]` for the rightmost leaf (+infinity).
    ///
    /// **Must be called before any `insert` or slot directory mutation** because
    /// the slot directory base depends on `high_key_len`.
    pub fn set_high_key(&mut self, key_bytes: &[u8]) {
        let len = key_bytes.len();
        write_u16(self.data, OFF_LEAF_HIGH_KEY_LEN, len as u16);
        if len > 0 {
            self.data[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + len].copy_from_slice(key_bytes);
        }
        write_u16(self.data, OFF_LEAF_FREE_START, slot_base(len) as u16);
    }

    /// Borrow as a read-only accessor without releasing the mutable borrow.
    pub fn as_accessor(&self) -> LeafPageAccessor<'_, K, V> {
        LeafPageAccessor::new(self.data)
    }

    // ── Record-level mutations ────────────────────────────────────────────────

    /// Set the creating transaction ID on the record at slot `pos`.
    pub fn set_xmin(&mut self, pos: usize, xmin: u64) {
        let hkl = read_u16(self.data, OFF_LEAF_HIGH_KEY_LEN) as usize;
        let rec_base = read_u16(self.data, slot_offset_at(hkl, pos)) as usize;
        write_u64(self.data, rec_base + REC_OFF_XMIN, xmin);
    }

    /// Set the deleting/replacing transaction ID on the record at slot `pos`.
    /// Setting xmax to a non-zero value marks the record as dead to
    /// transactions whose snapshot sees that xmax as committed.
    pub fn set_xmax(&mut self, pos: usize, xmax: u64) {
        let hkl = read_u16(self.data, OFF_LEAF_HIGH_KEY_LEN) as usize;
        let rec_base = read_u16(self.data, slot_offset_at(hkl, pos)) as usize;
        write_u64(self.data, rec_base + REC_OFF_XMAX, xmax);
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    /// Insert a new `(key, value)` pair at slot position `pos`.
    ///
    /// Thin typed wrapper over [`insert_raw`](Self::insert_raw) that stores the
    /// value inline.
    pub fn insert(
        &mut self,
        pos: usize,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), PageError> {
        let key_bytes = K::as_bytes(key);
        let val_bytes = V::as_bytes(value);
        self.insert_raw(pos, key_bytes.as_ref(), val_bytes.as_ref(), REC_TYPE_INLINE)
    }

    /// Insert a record at slot `pos` from raw key/value byte slices with an
    /// explicit record-type byte.
    ///
    /// `val_bytes` is the *stored* payload: either the inline value bytes
    ///  or a serialized [`OverflowDescriptor`]
    /// The new record is created live (xmin/xmax = 0);
    /// callers stamp `xmin` via [`set_xmin`](Self::set_xmin) afterwards.
    pub fn insert_raw(
        &mut self,
        pos: usize,
        key_bytes: &[u8],
        val_bytes: &[u8],
        rec_type: u8,
    ) -> Result<(), PageError> {
        let key_len = key_bytes.len();
        let val_len = val_bytes.len();
        let rec_size = rec_total_size(key_len, val_len);

        let free_end = read_u16(self.data, OFF_LEAF_FREE_END) as usize;
        let free_start = read_u16(self.data, OFF_LEAF_FREE_START) as usize;
        let free = free_end - free_start;

        if free < SLOT_SIZE + rec_size {
            return Err(PageError::InsufficientSpace {
                needed: SLOT_SIZE + rec_size,
                available: free,
            });
        }

        // Write the record just below free_end (record area grows downward).
        let rec_base = free_end - rec_size;

        write_u16(self.data, rec_base + REC_OFF_KEY_LEN, key_len as u16);
        write_u16(self.data, rec_base + REC_OFF_VAL_LEN, val_len as u16);
        write_u8(self.data, rec_base + REC_OFF_REC_TYPE, rec_type); // bytes 5..8 zeroed by page fill
        write_u64(self.data, rec_base + REC_OFF_XMIN, 0);
        write_u64(self.data, rec_base + REC_OFF_XMAX, 0);

        let key_off = rec_key_offset(rec_base);
        self.data[key_off..key_off + key_len].copy_from_slice(key_bytes);

        let val_off = rec_val_offset(rec_base, key_len);
        self.data[val_off..val_off + val_len].copy_from_slice(val_bytes);

        // Shift slot directory: [pos..n] → [pos+1..n+1].
        let hkl = read_u16(self.data, OFF_LEAF_HIGH_KEY_LEN) as usize;
        let n = read_u16(self.data, OFF_LEAF_SLOT_COUNT) as usize;
        let src = slot_offset_at(hkl, pos);
        let len = (n - pos) * SLOT_SIZE;
        if len > 0 {
            self.data.copy_within(src..src + len, src + SLOT_SIZE);
        }

        // Write the new slot entry.
        let slot_off = slot_offset_at(hkl, pos);
        write_u16(self.data, slot_off, rec_base as u16);
        write_u16(self.data, slot_off + 2, rec_size as u16);

        // Update header.
        write_u16(self.data, OFF_LEAF_SLOT_COUNT, (n + 1) as u16);
        write_u16(
            self.data,
            OFF_LEAF_FREE_START,
            (free_start + SLOT_SIZE) as u16,
        );
        write_u16(self.data, OFF_LEAF_FREE_END, rec_base as u16);

        Ok(())
    }

    // ── Remove ────────────────────────────────────────────────────────────────

    /// Remove the entry at slot `pos`.
    ///
    /// The slot directory is immediately compacted: slots `[pos+1..n]` are
    /// shifted left. The record bytes in the data area are **not** erased —
    /// they become dead space. Call `compact()` to reclaim them.
    ///
    /// # Panics
    /// Panics if `pos >= num_pairs()`.
    pub fn remove(&mut self, pos: usize) {
        let n = read_u16(self.data, OFF_LEAF_SLOT_COUNT) as usize;
        let free_start = read_u16(self.data, OFF_LEAF_FREE_START) as usize;
        let hkl = read_u16(self.data, OFF_LEAF_HIGH_KEY_LEN) as usize;
        assert!(
            pos < n,
            "LeafPageMutator::remove: pos {} out of bounds (n={})",
            pos,
            n
        );

        let src = slot_offset_at(hkl, pos + 1);
        let len = (n - pos - 1) * SLOT_SIZE;
        if len > 0 {
            self.data.copy_within(src..src + len, src - SLOT_SIZE);
        }

        let vacated = slot_offset_at(hkl, n - 1);
        self.data[vacated..vacated + SLOT_SIZE].fill(0);

        write_u16(self.data, OFF_LEAF_SLOT_COUNT, (n - 1) as u16);
        write_u16(
            self.data,
            OFF_LEAF_FREE_START,
            (free_start - SLOT_SIZE) as u16,
        );
    }

    /// Reclaims space by physically removing dead records.
    ///
    /// This rebuilds the page in-place, keeping only records that are NOT
    /// considered vacuumable under the current `global_xmin` horizon.
    ///
    /// Returns `(removed, orphaned_chains)` where `removed` is the number of
    /// records dropped and `orphaned_chains` holds the `first_page_id` of every
    /// vacuumed *overflow* record. The page layer cannot reach the buffer pool,
    /// so the caller ([`crate::index::BTreeIndex::vacuum`]) frees those chains.
    pub fn compact(
        page_id: PageId,
        page_data: &mut [u8],
        horizon: u64,
        tm: &TransactionManager,
    ) -> (usize, Vec<PageId>) {
        let acc = LeafPageAccessor::<K, V>::new(page_data);
        let n = acc.num_pairs() as usize;

        // 1. Gather all non-vacuumable records. Value bytes and rec_type are
        //    captured verbatim so overflow pointers survive compaction
        type LiveRecord = (Vec<u8>, Vec<u8>, u8, u64, u64);
        let mut live: Vec<LiveRecord> = Vec::with_capacity(n);
        let mut dead_count = 0;
        let mut orphaned_chains: Vec<PageId> = Vec::new();

        for i in 0..n {
            let xmin = acc.get_xmin(i);
            let xmax = acc.get_xmax(i);

            if transaction::is_vacuumable(xmin, xmax, horizon, tm) {
                dead_count += 1;
                // Record the overflow chain so the caller can free it; the
                // descriptor disappears once the record bytes are dropped.
                if let Some(desc) = acc.overflow_descriptor(i) {
                    orphaned_chains.push(desc.first_page_id);
                }
                continue;
            }

            let k = K::as_bytes(&acc.get_key(i)).as_ref().to_vec();
            let v = acc.raw_value(i).to_vec();
            let rt = acc.get_rec_type(i);
            live.push((k, v, rt, xmin, xmax));
        }

        // 2. If no records were removed, don't touch the page.
        if dead_count == 0 {
            return (0, Vec::new());
        }

        // 3. Preserve page metadata before clearing. 
        let high_key: Option<Vec<u8>> = acc.high_key_bytes().map(|b| b.to_vec());
        let rightlink = acc.rightlink();
        let prev_page = acc.prev_page();
        let lsn = acc.lsn();
        let flags = read_u8(page_data, super::OFF_FLAGS);

        // 4. Rebuild the page using the Builder.
        let mut builder = LeafPageBuilder::<K, V>::new(page_id, page_data);
        if let Some(ref hk) = high_key {
            builder.set_high_key(hk);
        }
        builder.set_rightlink(rightlink);
        builder.set_prev_page(prev_page);
        for (k, v, rt, xmin, xmax) in &live {
            builder.push_with_mvcc_raw(k, v, *rt, *xmin, *xmax);
        }

        let mut m = builder.finish();
        m.set_lsn(lsn);
        write_u8(page_data, super::OFF_FLAGS, flags);

        (dead_count, orphaned_chains)
    }
}

// ── LeafPageBuilder ───────────────────────────────────────────────────────────

/// Write-once constructor for a fresh leaf page.
pub struct LeafPageBuilder<'a, K: Key, V: Value> {
    data: &'a mut [u8],
    write_end: usize,
    hkl: usize, // high_key_len, cached for slot offset calculation
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<'a, K: Key, V: Value> LeafPageBuilder<'a, K, V> {
    /// Zero the buffer, stamp the page type and page ID, initialise free
    /// pointers. High key defaults to 0 (rightmost, +infinity).
    pub fn new(page_id: PageId, data: &'a mut [u8]) -> Self {
        data.fill(0);
        write_u8(data, OFF_PAGE_TYPE, LEAF);
        write_u64(data, OFF_PAGE_ID, page_id);
        // high_key_len = 0 by default (rightmost page).
        // Slot directory starts at slot_base(0) = 48.
        write_u16(data, OFF_LEAF_FREE_START, slot_base(0) as u16);
        write_u16(data, OFF_LEAF_FREE_END, PAGE_SIZE as u16);
        Self {
            data,
            write_end: PAGE_SIZE,
            hkl: 0,
            _key: PhantomData,
            _val: PhantomData,
        }
    }

    pub fn set_prev_page(&mut self, prev: Option<PageId>) {
        write_u64(self.data, OFF_LEAF_PREV, prev.unwrap_or(0));
    }

    pub fn set_rightlink(&mut self, right: Option<PageId>) {
        write_u64(self.data, OFF_LEAF_RIGHTLINK, right.unwrap_or(0));
    }

    /// Set the high key. **Must be called before any `push()`** because the
    /// slot directory base depends on `high_key_len`.
    pub fn set_high_key(&mut self, key_bytes: &[u8]) {
        let len = key_bytes.len();
        write_u16(self.data, OFF_LEAF_HIGH_KEY_LEN, len as u16);
        if len > 0 {
            self.data[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + len].copy_from_slice(key_bytes);
        }
        self.hkl = len;
        // Update free_start to account for the high key region.
        write_u16(self.data, OFF_LEAF_FREE_START, slot_base(len) as u16);
    }

    /// Append a key-value pair. Keys must be pushed in strictly ascending
    /// order.
    ///
    /// # Panics
    /// Panics if the page has no remaining space for the record.
    /// Append a key-value pair with explicit xmin/xmax values.
    ///
    /// Used by split paths to preserve the version chain from the original
    /// page when rebuilding both halves.
    pub fn push_with_mvcc(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
        xmin: u64,
        xmax: u64,
    ) {
        let key_bytes = K::as_bytes(key);
        let val_bytes = V::as_bytes(value);
        self.push_with_mvcc_raw(
            key_bytes.as_ref(),
            val_bytes.as_ref(),
            REC_TYPE_INLINE,
            xmin,
            xmax,
        );
    }

    /// Append a record from raw key/value byte slices with an explicit
    /// record-type byte and caller-supplied xmin/xmax.
    ///
    /// `val_bytes` is the stored payload — inline value bytes or a serialized
    /// [`OverflowDescriptor`]. Used by split/compact paths to move records
    /// (including overflow pointers) while preserving their version
    /// chain. Keys must be pushed in strictly ascending order.
    ///
    /// # Panics
    /// Panics if the page has no remaining space for the record.
    pub fn push_with_mvcc_raw(
        &mut self,
        key_bytes: &[u8],
        val_bytes: &[u8],
        rec_type: u8,
        xmin: u64,
        xmax: u64,
    ) {
        let key_len = key_bytes.len();
        let val_len = val_bytes.len();
        let rec_size = rec_total_size(key_len, val_len);

        let n = read_u16(self.data, OFF_LEAF_SLOT_COUNT) as usize;
        let free_start = slot_base(self.hkl) + n * SLOT_SIZE;

        assert!(
            self.write_end >= free_start + SLOT_SIZE + rec_size,
            "LeafPageBuilder::push_with_mvcc_raw: page is full"
        );

        self.write_end -= rec_size;
        let rec_base = self.write_end;

        write_u16(self.data, rec_base + REC_OFF_KEY_LEN, key_len as u16);
        write_u16(self.data, rec_base + REC_OFF_VAL_LEN, val_len as u16);
        write_u8(self.data, rec_base + REC_OFF_REC_TYPE, rec_type); // bytes 5..8 zeroed by page fill
        write_u64(self.data, rec_base + REC_OFF_XMIN, xmin);
        write_u64(self.data, rec_base + REC_OFF_XMAX, xmax);

        let key_off = rec_key_offset(rec_base);
        self.data[key_off..key_off + key_len].copy_from_slice(key_bytes);

        let val_off = rec_val_offset(rec_base, key_len);
        self.data[val_off..val_off + val_len].copy_from_slice(val_bytes);

        let slot_off = slot_offset_at(self.hkl, n);
        write_u16(self.data, slot_off, rec_base as u16);
        write_u16(self.data, slot_off + 2, rec_size as u16);

        write_u16(self.data, OFF_LEAF_SLOT_COUNT, (n + 1) as u16);
        write_u16(
            self.data,
            OFF_LEAF_FREE_START,
            (slot_off + SLOT_SIZE) as u16,
        );
        write_u16(self.data, OFF_LEAF_FREE_END, rec_base as u16);
    }

    /// Seal the page. Returns a [`LeafPageMutator`] for any remaining header
    /// writes (lsn, prev/next pointers) before the page is handed to the
    /// buffer pool.
    pub fn finish(self) -> LeafPageMutator<'a, K, V> {
        LeafPageMutator::new(self.data)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageBuffer;

    type K = &'static [u8];
    type V = &'static [u8];

    fn build_page(page_id: u64, pairs: &[(&[u8], &[u8])]) -> PageBuffer {
        let mut buf = PageBuffer::new();
        let mut b = LeafPageBuilder::<K, V>::new(page_id, buf.memory_mut());
        for (k, v) in pairs {
            b.push_with_mvcc(k, v, 0, 0);
        }
        b.finish();
        buf
    }

    // ── Builder / accessor round-trip ─────────────────────────────────────────

    #[test]
    fn build_and_read_entries() {
        let pairs: &[(&[u8], &[u8])] =
            &[(b"apple", b"AAA"), (b"banana", b"BBB"), (b"cherry", b"CCC")];
        let buf = build_page(42, pairs);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());

        assert_eq!(acc.page_id(), 42);
        assert_eq!(acc.num_pairs(), 3);
        assert_eq!(acc.get_key(0), b"apple".as_ref());
        assert_eq!(acc.get_value(0), b"AAA".as_ref());
        assert_eq!(acc.get_key(2), b"cherry".as_ref());
        assert_eq!(acc.get_value(2), b"CCC".as_ref());
    }

    #[test]
    fn entry_returns_key_and_value_tuple() {
        let buf = build_page(1, &[(b"k1", b"v1"), (b"k2", b"v2")]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.entry(1), (b"k2".as_ref(), b"v2".as_ref()));
    }

    // ── Rightlink and high key ───────────────────────────────────────────────

    #[test]
    fn rightlink_none_when_zero() {
        let buf = build_page(1, &[]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.rightlink(), None);
    }

    #[test]
    fn rightlink_round_trip() {
        let mut buf = build_page(1, &[]);
        {
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            m.set_rightlink(Some(42));
        }
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.rightlink(), Some(42));
    }

    #[test]
    fn prev_page_round_trip() {
        let mut buf = build_page(1, &[]);
        {
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            m.set_prev_page(Some(10));
        }
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.prev_page(), Some(10));
    }

    #[test]
    fn high_key_round_trip() {
        let mut buf = PageBuffer::new();
        {
            let mut b = LeafPageBuilder::<K, V>::new(1, buf.memory_mut());
            b.set_high_key(&[42, 43, 44]);
            b.push_with_mvcc(&b"a".as_ref(), &b"A".as_ref(), 0, 0);
            b.finish();
        }
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.high_key_len(), 3);
        assert_eq!(acc.high_key_bytes(), Some(&[42u8, 43, 44][..]));
        // Entry is still readable with non-zero high_key_len.
        assert_eq!(acc.get_key(0), b"a".as_ref());
        assert_eq!(acc.get_value(0), b"A".as_ref());
    }

    #[test]
    fn high_key_none_when_rightmost() {
        let buf = build_page(1, &[]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.high_key_len(), 0);
        assert_eq!(acc.high_key_bytes(), None);
    }

    // ── LSN ───────────────────────────────────────────────────────────────────

    #[test]
    fn lsn_round_trip() {
        let mut buf = build_page(1, &[]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).set_lsn(12345);
        assert_eq!(LeafPageAccessor::<K, V>::new(buf.memory()).lsn(), 12345);
    }

    // ── xmin / xmax ────────────────────────────────────────────────────────────

    #[test]
    fn xmin_xmax_default_to_zero() {
        let buf = build_page(1, &[(b"k", b"v")]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.get_xmin(0), 0);
        assert_eq!(acc.get_xmax(0), 0);
        assert!(!acc.is_deleted(0));
    }

    #[test]
    fn xmin_round_trip() {
        let mut buf = build_page(1, &[(b"k", b"v")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).set_xmin(0, 42);
        assert_eq!(LeafPageAccessor::<K, V>::new(buf.memory()).get_xmin(0), 42);
    }

    #[test]
    fn xmax_round_trip() {
        let mut buf = build_page(1, &[(b"k", b"v")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).set_xmax(0, 99);
        assert_eq!(LeafPageAccessor::<K, V>::new(buf.memory()).get_xmax(0), 99);
    }

    #[test]
    fn is_deleted_when_xmax_nonzero() {
        let mut buf = build_page(1, &[(b"k", b"v")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).set_xmax(0, 5);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert!(acc.is_deleted(0));
        // Key and value are still readable (tombstone, not erased).
        assert_eq!(acc.get_key(0), b"k".as_ref());
    }

    #[test]
    fn is_deleted_false_when_xmax_zero() {
        let buf = build_page(1, &[(b"k", b"v")]);
        assert!(!LeafPageAccessor::<K, V>::new(buf.memory()).is_deleted(0));
    }

    #[test]
    fn push_with_mvcc_preserves_xmin_xmax() {
        let mut buf = PageBuffer::new();
        {
            let mut b = LeafPageBuilder::<K, V>::new(1, buf.memory_mut());
            b.push_with_mvcc(&b"k".as_ref(), &b"v".as_ref(), 10, 20);
            b.finish();
        }
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 1);
        assert_eq!(acc.get_xmin(0), 10);
        assert_eq!(acc.get_xmax(0), 20);
        assert_eq!(acc.get_key(0), b"k".as_ref());
        assert_eq!(acc.get_value(0), b"v".as_ref());
    }

    // ── Binary search ─────────────────────────────────────────────────────────

    #[test]
    fn position_finds_exact_match() {
        let buf = build_page(1, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.position(&b"b".as_ref()), (1, true));
    }

    #[test]
    fn position_returns_insertion_point_when_absent() {
        let buf = build_page(1, &[(b"a", b"1"), (b"c", b"3")]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.position(&b"b".as_ref()), (1, false));
    }

    #[test]
    fn find_key_some_and_none() {
        let buf = build_page(1, &[(b"x", b"X"), (b"y", b"Y")]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.find_key(&b"x".as_ref()), Some(0));
        assert_eq!(acc.find_key(&b"z".as_ref()), None);
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    #[test]
    fn insert_maintains_sorted_order() {
        let mut buf = build_page(1, &[(b"a", b"A"), (b"c", b"C")]);
        {
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            let pos = m.as_accessor().position(&b"b".as_ref()).0;
            m.insert(pos, &b"b".as_ref(), &b"B".as_ref()).unwrap();
        }
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 3);
        assert_eq!(acc.get_key(1), b"b".as_ref());
        assert_eq!(acc.get_value(1), b"B".as_ref());
        assert_eq!(acc.get_key(0), b"a".as_ref());
        assert_eq!(acc.get_key(2), b"c".as_ref());
    }

    #[test]
    fn insert_at_beginning() {
        let mut buf = build_page(1, &[(b"b", b"B"), (b"c", b"C")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut())
            .insert(0, &b"a".as_ref(), &b"A".as_ref())
            .unwrap();
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.get_key(0), b"a".as_ref());
        assert_eq!(acc.num_pairs(), 3);
    }

    #[test]
    fn insert_at_end() {
        let mut buf = build_page(1, &[(b"a", b"A"), (b"b", b"B")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut())
            .insert(2, &b"c".as_ref(), &b"C".as_ref())
            .unwrap();
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.get_key(2), b"c".as_ref());
        assert_eq!(acc.num_pairs(), 3);
    }

    #[test]
    fn insert_returns_err_when_no_space() {
        let mut buf = PageBuffer::new();
        LeafPageBuilder::<K, V>::new(1, buf.memory_mut()).finish();
        let big_key = vec![0u8; 100];
        let big_val = vec![0u8; 100];
        loop {
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            if m.as_accessor().can_fit_direct(big_key.len(), big_val.len()) {
                let n = m.as_accessor().num_pairs() as usize;
                m.insert(n, &big_key.as_slice(), &big_val.as_slice())
                    .unwrap();
            } else {
                let result = m.insert(0, &big_key.as_slice(), &big_val.as_slice());
                assert!(matches!(result, Err(PageError::InsufficientSpace { .. })));
                break;
            }
        }
    }

    // ── Remove ────────────────────────────────────────────────────────────────

    #[test]
    fn remove_middle_entry() {
        let mut buf = build_page(1, &[(b"a", b"A"), (b"b", b"B"), (b"c", b"C")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(1);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 2);
        assert_eq!(acc.get_key(0), b"a".as_ref());
        assert_eq!(acc.get_key(1), b"c".as_ref());
    }

    #[test]
    fn remove_first_entry() {
        let mut buf = build_page(1, &[(b"a", b"A"), (b"b", b"B")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(0);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 1);
        assert_eq!(acc.get_key(0), b"b".as_ref());
    }

    #[test]
    fn remove_last_entry() {
        let mut buf = build_page(1, &[(b"a", b"A"), (b"b", b"B")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(1);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 1);
        assert_eq!(acc.get_key(0), b"a".as_ref());
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn remove_out_of_bounds_panics() {
        let mut buf = build_page(1, &[(b"a", b"A")]);
        LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(1);
    }

    // ── Free-space accounting ─────────────────────────────────────────────────

    #[test]
    fn can_fit_direct_true_on_empty_page() {
        let buf = build_page(1, &[]);
        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert!(acc.can_fit_direct(10, 10));
    }

    #[test]
    fn compact_removes_dead_records() {
        let tm = TransactionManager::new();
        tm.mark_committed(10); // deleter of record 1
        tm.mark_aborted(20); // creator of record 2 (never valid)

        let mut buf = crate::page::PageBuffer::new();
        {
            let mut b = LeafPageBuilder::<K, V>::new(1, buf.memory_mut());
            // 0: Live (xmin=1, xmax=0)
            b.push_with_mvcc(&b"k0".as_ref(), &b"v0".as_ref(), 1, 0);
            // 1: Dead (xmin=1, xmax=10, horizon=15)
            b.push_with_mvcc(&b"k1".as_ref(), &b"v1".as_ref(), 1, 10);
            // 2: Dead (xmin=20, xmax=0) -> creator aborted
            b.push_with_mvcc(&b"k2".as_ref(), &b"v2".as_ref(), 20, 0);
            b.finish();
        }

        let (removed, _chains) = LeafPageMutator::<K, V>::compact(1, buf.memory_mut(), 15, &tm);
        assert_eq!(removed, 2);

        let acc = LeafPageAccessor::<K, V>::new(buf.memory());
        assert_eq!(acc.num_pairs(), 1);
        assert_eq!(acc.get_key(0), b"k0".as_ref());
    }

    #[test]
    fn compact_no_dead_records_is_noop() {
        let tm = TransactionManager::new();
        let mut buf = crate::page::PageBuffer::new();
        {
            let mut b = LeafPageBuilder::<K, V>::new(1, buf.memory_mut());
            b.push_with_mvcc(&b"k0".as_ref(), &b"v0".as_ref(), 1, 0);
            b.finish();
        }

        let (removed, _chains) = LeafPageMutator::<K, V>::compact(1, buf.memory_mut(), 100, &tm);
        assert_eq!(removed, 0);
        assert_eq!(LeafPageAccessor::<K, V>::new(buf.memory()).num_pairs(), 1);
    }
}

// ── Property-based tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::page::PageBuffer;
    use db_core::transaction_manager::TransactionManager;
    use proptest::prelude::*;
    use std::cmp::Ordering;

    // Helper functions to check if the page is valid
    // These read the raw bytes directly instead of using LeafPageAccessor
    // because we want to test if the accessor itself is working correctly

    /// Check all the structural invariants for a leaf page
    /// Returns Ok(()) if everything is good, Err with a message if something is wrong
    fn check_leaf_structural_invariants(page: &[u8]) -> Result<(), String> {
        // First check: make sure this is actually a leaf page
        let page_type = read_u8(page, OFF_PAGE_TYPE);
        if page_type != LEAF {
            return Err(format!(
                "page_type: expected {} (LEAF), got {}",
                LEAF, page_type
            ));
        }

        // Read the important fields from the page header
        // We need these to check if the page layout is correct
        let slot_count = read_u16(page, OFF_LEAF_SLOT_COUNT) as usize;
        let free_start = read_u16(page, OFF_LEAF_FREE_START) as usize;
        let free_end = read_u16(page, OFF_LEAF_FREE_END) as usize;
        let high_key_len = read_u16(page, OFF_LEAF_HIGH_KEY_LEN) as usize;

        // Check that free space pointers make sense
        // free_start is where the slot directory ends
        // free_end is where the record data starts
        // So free_start should be <= free_end
        if free_start > free_end {
            return Err(format!(
                "space_accounting: free_start ({}) > free_end ({})",
                free_start, free_end
            ));
        }
        // Also make sure free_end doesn't go past the end of the page
        if free_end > PAGE_SIZE {
            return Err(format!(
                "space_accounting: free_end ({}) > PAGE_SIZE ({})",
                free_end, PAGE_SIZE
            ));
        }

        // Check if free_start is at the right position
        // It should be at: header + high_key + (number of slots * 4 bytes per slot)
        let expected_free_start = slot_base(high_key_len) + slot_count * SLOT_SIZE;
        if free_start != expected_free_start {
            return Err(format!(
                "slot_directory_size: free_start ({}) != slot_base({}) + {} * {} = {}",
                free_start, high_key_len, slot_count, SLOT_SIZE, expected_free_start
            ));
        }

        // Now check each individual slot to make sure it's valid
        // We'll collect all the slot ranges so we can check for overlaps later
        let mut slot_ranges: Vec<(usize, usize, usize)> = Vec::with_capacity(slot_count);
        for i in 0..slot_count {
            // Calculate where this slot's data is stored
            let slot_off = slot_offset_at(high_key_len, i);
            let rec_base = read_u16(page, slot_off) as usize;
            let rec_size = read_u16(page, slot_off + 2) as usize;

            // Make sure the record starts at or after free_end
            // (records grow downward from the end of the page)
            if rec_base < free_end {
                return Err(format!(
                    "slot_{}_offset: rec_base ({}) < free_end ({})",
                    i, rec_base, free_end
                ));
            }

            // Make sure the record doesn't go past the end of the page
            if rec_base + rec_size > PAGE_SIZE {
                return Err(format!(
                    "slot_{}_bounds: rec_base ({}) + rec_size ({}) = {} > PAGE_SIZE ({})",
                    i,
                    rec_base,
                    rec_size,
                    rec_base + rec_size,
                    PAGE_SIZE
                ));
            }

            // Every record needs at least the header bytes
            if rec_size < REC_HEADER_SIZE {
                return Err(format!(
                    "slot_{}_size: rec_size ({}) < REC_HEADER_SIZE ({})",
                    i, rec_size, REC_HEADER_SIZE
                ));
            }

            // Check that the key and value lengths match what the slot says
            // This catches bugs where the record header has wrong sizes
            let key_len = read_u16(page, rec_base + REC_OFF_KEY_LEN) as usize;
            let val_len = read_u16(page, rec_base + REC_OFF_VAL_LEN) as usize;
            let expected_rec_size = rec_total_size(key_len, val_len);
            if rec_size != expected_rec_size {
                return Err(format!(
                    "slot_{}_consistency: rec_size ({}) != rec_total_size(key_len={}, val_len={}) = {}",
                    i, rec_size, key_len, val_len, expected_rec_size
                ));
            }

            // Save this slot's info so we can check for overlaps later
            slot_ranges.push((i, rec_base, rec_size));
        }

        // Check that no two slots point to overlapping memory regions
        // This would mean two records are overwriting each other's data
        for a in 0..slot_ranges.len() {
            let (ia, base_a, size_a) = slot_ranges[a];
            for b in (a + 1)..slot_ranges.len() {
                let (ib, base_b, size_b) = slot_ranges[b];
                let end_a = base_a + size_a;
                let end_b = base_b + size_b;
                // Check if ranges overlap
                // Two ranges overlap if: base_a < end_b AND base_b < end_a
                if base_a < end_b && base_b < end_a {
                    return Err(format!(
                        "slot_overlap: slot {} [{}, {}) overlaps slot {} [{}, {})",
                        ia, base_a, end_a, ib, base_b, end_b
                    ));
                }
            }
        }

        Ok(())
    }

    /// Check that keys are in the right order
    /// This uses LeafPageAccessor which is okay because we're just checking
    /// the logical ordering, not the low-level structure
    fn check_leaf_key_ordering(page: &[u8]) -> Result<(), String> {
        type K = &'static [u8];
        type V = &'static [u8];

        let acc = LeafPageAccessor::<K, V>::new(page);
        let n = acc.num_pairs() as usize;

        // 1. Keys sorted ascending (allow duplicates for MVCC)
        for i in 0..n.saturating_sub(1) {
            let k1 = acc.get_key(i);
            let k2 = acc.get_key(i + 1);
            if <K as Key>::compare(
                <K as Value>::as_bytes(&k1).as_ref(),
                <K as Value>::as_bytes(&k2).as_ref(),
            ) == Ordering::Greater
            {
                return Err(format!(
                    "keys_not_sorted: key[{}] ({:?}) > key[{}] ({:?})",
                    i,
                    k1,
                    i + 1,
                    k2
                ));
            }
        }

        // Also check that all keys are less than the high_key if one exists
        // (high_key is the upper bound for keys on this page)
        if let Some(hk) = acc.high_key_bytes() {
            for i in 0..n {
                let k = acc.get_key(i);
                if <K as Key>::compare(<K as Value>::as_bytes(&k).as_ref(), hk) == Ordering::Greater
                {
                    return Err(format!(
                        "key_exceeds_high_key: key[{}] ({:?}) >= high_key ({:?})",
                        i, k, hk
                    ));
                }
            }
        }

        Ok(())
    }

    /// Main checker function - calls both structural and ordering checks
    /// Use this after every operation in tests
    fn check_leaf_all_invariants(page: &[u8]) -> Result<(), String> {
        check_leaf_structural_invariants(page)?;
        check_leaf_key_ordering(page)?;
        Ok(())
    }

    /// After running a bunch of operations, read all the keys back
    /// and make sure they're still sorted and readable
    /// This catches corruption bugs that the other checkers might miss
    fn verify_leaf_readable_and_sorted(page: &[u8]) -> Result<(), String> {
        type K = &'static [u8];
        type V = &'static [u8];

        let acc = LeafPageAccessor::<K, V>::new(page);
        let n = acc.num_pairs() as usize;

        // Keep track of the previous key to compare with the next one
        let mut prev_key: Option<Vec<u8>> = None;
        for i in 0..n {
            let key = acc.get_key(i);
            let _val = acc.get_value(i);
            let key_bytes: Vec<u8> = <K as Value>::as_bytes(&key).as_ref().to_vec();

            // Make sure this key is >= the previous one (allowing duplicates)
            if let Some(ref pk) = prev_key {
                if <K as Key>::compare(pk.as_slice(), key_bytes.as_slice()) == Ordering::Greater {
                    return Err(format!(
                        "readback_not_sorted: key[{}] ({:?}) >= key[{}] ({:?})",
                        i - 1,
                        pk,
                        i,
                        key_bytes
                    ));
                }
            }
            prev_key = Some(key_bytes);
        }

        Ok(())
    }

    // ── Test data generators ───────────────────────────────────────────────────

    /// Generate random key bytes
    /// We use a mix of small key spaces (to encourage duplicates for MVCC testing)
    /// and larger random keys.
    fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            // 80% chance: small pool of keys (length 1-2, values 0-3) to ensure duplicates
            80 => prop::collection::vec(0u8..4u8, 1..=2usize),
            // 20% chance: completely random keys up to 16 bytes
            20 => prop::collection::vec(any::<u8>(), 1..=16usize),
        ]
    }

    /// Generate small random values (1-32 bytes)
    /// Used for sequence tests where we want lots of operations
    fn small_value_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 1..=32usize)
    }

    /// Generate larger random values (1-256 bytes)
    /// Used to test space accounting with bigger records
    fn large_value_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 1..=256usize)
    }

    /// Different operations we can do on a leaf page
    /// We'll generate random sequences of these to test
    #[derive(Debug, Clone)]
    enum LeafOp {
        Insert { key: Vec<u8>, value: Vec<u8> },
        Remove { slot_index: usize }, // we clamp this to valid range when applying
        SetXmax { slot_index: usize, xmax: u64 }, // mark record as deleted
        Compact { horizon: u64 },     // clean up dead records
    }

    /// Generate random operations with different probabilities
    /// More inserts (50%) because we need to build up state first
    /// Then removes, setxmax, and compact are less frequent
    fn leaf_op_strategy() -> impl Strategy<Value = LeafOp> {
        prop_oneof![
            50 => (key_strategy(), small_value_strategy())
                .prop_map(|(k, v)| LeafOp::Insert { key: k, value: v }),
            20 => (0usize..100)
                .prop_map(|idx| LeafOp::Remove { slot_index: idx }),
            15 => ((0usize..100), (1u64..200))
                .prop_map(|(idx, xmax)| LeafOp::SetXmax { slot_index: idx, xmax }),
            15 => (1u64..200)
                .prop_map(|h| LeafOp::Compact { horizon: h }),
        ]
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    type K = &'static [u8];
    type V = &'static [u8];

    /// Build a valid leaf page from sorted key-value pairs (allows duplicates for MVCC).
    /// Returns the number of pairs that actually fit.
    fn build_leaf_from_sorted(
        buf: &mut PageBuffer,
        page_id: u64,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> usize {
        LeafPageBuilder::<K, V>::new(page_id, buf.memory_mut()).finish();

        let mut count = 0;
        for (k, v) in pairs {
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            if !m.as_accessor().can_fit_direct(k.len(), v.len()) {
                break;
            }
            let (pos, _found) = m.as_accessor().position(&k.as_slice());
            m.insert(pos, &k.as_slice(), &v.as_slice()).unwrap();
            m.set_xmin(pos, 1); // dummy txn id
            count += 1;
        }
        count
    }

    /// Sort a list of (key, value) pairs by key, allowing duplicates.
    fn sort_pairs(mut pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }

    // ── Property tests for single operations ──────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Test that building a page from random entries always creates a valid page
        /// We sort the entries (allowing duplicates), then build the page
        #[test]
        fn prop_builder_produces_valid_page(
            entries in prop::collection::vec(
                (key_strategy(), small_value_strategy()),
                0..30
            )
        ) {
            let sorted = sort_pairs(entries);
            let mut buf = PageBuffer::new();
            build_leaf_from_sorted(&mut buf, 1, &sorted);
            // Check that all invariants are satisfied
            check_leaf_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after build: {}", e)))?;
        }

        /// Test that inserting one key into a page keeps it valid
        /// Start with a page that has some entries, add one more, check invariants
        #[test]
        fn prop_insert_maintains_invariants(
            initial in prop::collection::vec(
                (key_strategy(), small_value_strategy()),
                0..15
            ),
            new_key in key_strategy(),
            new_val in large_value_strategy(),
        ) {
            // Build the initial page
            let sorted = sort_pairs(initial);
            let mut buf = PageBuffer::new();
            build_leaf_from_sorted(&mut buf, 1, &sorted);

            // Try to insert the new key
            let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
            let acc = m.as_accessor();
            if acc.can_fit_direct(new_key.len(), new_val.len()) {
                let (pos, _found) = acc.position(&new_key.as_slice());
                drop(acc);
                m.insert(pos, &new_key.as_slice(), &new_val.as_slice()).unwrap();
                m.set_xmin(pos, 1);
            }

            check_leaf_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after insert: {}", e)))?;
        }

        /// A single remove from a valid page preserves all invariants.
        #[test]
        fn prop_remove_maintains_invariants(
            initial in prop::collection::vec(
                (key_strategy(), small_value_strategy()),
                1..20
            ),
            remove_idx in 0usize..100,
        ) {
            let sorted = sort_pairs(initial);
            let mut buf = PageBuffer::new();
            let count = build_leaf_from_sorted(&mut buf, 1, &sorted);

            if count > 0 {
                let pos = remove_idx.min(count - 1);
                LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(pos);
            }

            check_leaf_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after remove: {}", e)))?;
        }

        /// Building with a high key set produces a valid page where all keys < high_key.
        #[test]
        fn prop_builder_with_high_key_valid(
            entries in prop::collection::vec(
                (key_strategy(), small_value_strategy()),
                0..20
            ),
        ) {
            let sorted = sort_pairs(entries);
            let mut buf = PageBuffer::new();

            // Use a high key that's larger than all generated keys
            let high_key = vec![0xFF; 17]; // 17 bytes > max key of 16

            let mut builder = LeafPageBuilder::<K, V>::new(1, buf.memory_mut());
            builder.set_high_key(&high_key);
            builder.finish();

            for (k, v) in &sorted {
                let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
                if !m.as_accessor().can_fit_direct(k.len(), v.len()) {
                    break;
                }
                let (pos, _found) = m.as_accessor().position(&k.as_slice());
                m.insert(pos, &k.as_slice(), &v.as_slice()).unwrap();
            }

            check_leaf_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after build with high key: {}", e)))?;
        }
    }

    // ── Step 5: Operation sequence property tests ─────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Arbitrary operation sequences on a leaf page always preserve invariants.
        #[test]
        fn prop_leaf_op_sequence_maintains_invariants(
            ops in prop::collection::vec(leaf_op_strategy(), 1..30)
        ) {
            let mut buf = PageBuffer::new();
            LeafPageBuilder::<K, V>::new(1, buf.memory_mut()).finish();

            let tm = TransactionManager::new();
            let mut next_txn: u64 = 1;

            for (i, op) in ops.iter().enumerate() {
                match op {
                    LeafOp::Insert { key, value } => {
                        let mut m = LeafPageMutator::<K, V>::new(buf.memory_mut());
                        let acc = m.as_accessor();
                        if !acc.can_fit_direct(key.len(), value.len()) {
                            continue;
                        }
                        let (pos, _found) = acc.position(&key.as_slice());
                        drop(acc);
                        m.insert(pos, &key.as_slice(), &value.as_slice()).unwrap();
                        m.set_xmin(pos, next_txn);
                        tm.mark_committed(next_txn);
                        next_txn += 1;
                    }
                    LeafOp::Remove { slot_index } => {
                        let n =
                            LeafPageAccessor::<K, V>::new(buf.memory()).num_pairs() as usize;
                        if n == 0 {
                            continue;
                        }
                        let pos = (*slot_index).min(n - 1);
                        LeafPageMutator::<K, V>::new(buf.memory_mut()).remove(pos);
                    }
                    LeafOp::SetXmax { slot_index, xmax } => {
                        let n =
                            LeafPageAccessor::<K, V>::new(buf.memory()).num_pairs() as usize;
                        if n == 0 {
                            continue;
                        }
                        let pos = (*slot_index).min(n - 1);
                        LeafPageMutator::<K, V>::new(buf.memory_mut()).set_xmax(pos, *xmax);
                    }
                    LeafOp::Compact { horizon } => {
                        LeafPageMutator::<K, V>::compact(1, buf.memory_mut(), *horizon, &tm);
                    }
                }

                // Check structural invariants after EVERY operation
                check_leaf_all_invariants(buf.memory()).map_err(|e| {
                    TestCaseError::fail(format!(
                        "Invariant violated after op #{} ({:?}): {}",
                        i, op, e
                    ))
                })?;
            }

            // Final read-back: verify all keys are readable and sorted (allows duplicates)
            verify_leaf_readable_and_sorted(buf.memory()).map_err(|e| {
                TestCaseError::fail(format!("Post-sequence read-back failed: {}", e))
            })?;
        }
    }

    // ── Step 6: Split-at-page-level property tests ────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Splitting a page at an arbitrary point produces two valid halves
        /// with correct high keys and no data loss.
        #[test]
        fn prop_split_produces_valid_halves(
            entries in prop::collection::vec(
                (key_strategy(), small_value_strategy()),
                5..40
            ),
            split_frac_pct in 20u32..80,
        ) {
            // 1. Build a source page with sorted entries (allowing duplicates)
            let sorted = sort_pairs(entries);
            if sorted.len() < 3 {
                return Ok(()); // need at least 3 entries to split meaningfully
            }
            let mut src = PageBuffer::new();
            let count = build_leaf_from_sorted(&mut src, 1, &sorted);
            if count < 3 {
                return Ok(());
            }

            // 2. Pick split point
            let mid =
                ((count as u32 * split_frac_pct) / 100).max(1).min(count as u32 - 1) as usize;

            // 3. Snapshot all entries from the source page
            let acc = LeafPageAccessor::<K, V>::new(src.memory());
            let n = acc.num_pairs() as usize;
            let entries_snap: Vec<(Vec<u8>, Vec<u8>, u64, u64)> = (0..n)
                .map(|i| {
                    let k = <K as Value>::as_bytes(&acc.get_key(i)).as_ref().to_vec();
                    let v = <V as Value>::as_bytes(&acc.get_value(i)).as_ref().to_vec();
                    (k, v, acc.get_xmin(i), acc.get_xmax(i))
                })
                .collect();

            let separator = entries_snap[mid].0.clone();

            // 4. Build left page: entries[..mid], high_key = separator
            let mut left = PageBuffer::new();
            {
                let mut builder = LeafPageBuilder::<K, V>::new(1, left.memory_mut());
                builder.set_high_key(&separator);
                builder.set_rightlink(Some(2));
                for (k, v, xmin, xmax) in &entries_snap[..mid] {
                    builder.push_with_mvcc(
                        &<K as Value>::from_bytes(k),
                        &<V as Value>::from_bytes(v),
                        *xmin,
                        *xmax,
                    );
                }
                builder.finish();
            }

            // 5. Build right page: entries[mid..], high_key = +∞ (rightmost)
            let mut right = PageBuffer::new();
            {
                let mut builder = LeafPageBuilder::<K, V>::new(2, right.memory_mut());
                builder.set_prev_page(Some(1));
                for (k, v, xmin, xmax) in &entries_snap[mid..] {
                    builder.push_with_mvcc(
                        &<K as Value>::from_bytes(k),
                        &<V as Value>::from_bytes(v),
                        *xmin,
                        *xmax,
                    );
                }
                builder.finish();
            }

            // 6. Check invariants on both pages
            check_leaf_all_invariants(left.memory())
                .map_err(|e| TestCaseError::fail(format!("left half: {}", e)))?;
            check_leaf_all_invariants(right.memory())
                .map_err(|e| TestCaseError::fail(format!("right half: {}", e)))?;

            // 7. Verify all keys in left <= separator (allow duplicates for MVCC)
            let left_acc = LeafPageAccessor::<K, V>::new(left.memory());
            for i in 0..left_acc.num_pairs() as usize {
                let k = left_acc.get_key(i);
                let kb = <K as Value>::as_bytes(&k);
                if <K as Key>::compare(kb.as_ref(), &separator) == Ordering::Greater {
                    return Err(TestCaseError::fail(format!(
                        "left key[{}] ({:?}) > separator ({:?})",
                        i,
                        kb.as_ref(),
                        separator
                    )));
                }
            }

            // 8. Verify all keys in right >= separator
            let right_acc = LeafPageAccessor::<K, V>::new(right.memory());
            for i in 0..right_acc.num_pairs() as usize {
                let k = right_acc.get_key(i);
                let kb = <K as Value>::as_bytes(&k);
                if <K as Key>::compare(kb.as_ref(), &separator) == Ordering::Less {
                    return Err(TestCaseError::fail(format!(
                        "right key[{}] ({:?}) < separator ({:?})",
                        i,
                        kb.as_ref(),
                        separator
                    )));
                }
            }

            // 9. Total records = left + right
            let total = left_acc.num_pairs() as usize + right_acc.num_pairs() as usize;
            if total != n {
                return Err(TestCaseError::fail(format!(
                    "data_loss: left ({}) + right ({}) = {} != source ({})",
                    left_acc.num_pairs(),
                    right_acc.num_pairs(),
                    total,
                    n
                )));
            }
        }
    }
}
