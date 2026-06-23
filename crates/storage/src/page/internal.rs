//! Internal (branch) page types for the B+Tree storage engine.
//!
//! ## Layout (Lehman-Yao)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │ FIXED HEADER — 40 bytes (fully 8-byte aligned)          │
//! ├────────┬───────────┬──────────────────────────────────  ┤
//! │ Off  0 │ u8        │ page_type                           │
//! │ Off  1 │ u8        │ _reserved (0)                       │
//! │ Off  2 │ u16       │ num_keys                            │
//! │ Off  4 │ u16       │ high_key_len (0 = +∞ / rightmost)   │
//! │ Off  6 │ u16       │ _padding                            │
//! ├────────┼───────────┼───────────────────────────────────  ┤
//! │ Off  8 │ u64       │ page_id          [8-byte aligned ✓] │
//! │ Off 16 │ u64       │ lsn              [8-byte aligned ✓] │
//! │ Off 24 │ u64       │ rightlink         [8-byte aligned ✓] │
//! │ Off 32 │ u32       │ checksum (CRC32)                    │
//! │ Off 36 │ u32       │ _padding2                           │
//! └────────┴───────────┴───────────────────────────────────  ┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ SECTION A — Child page IDs  [(num_keys + 1) × 8 bytes]   │
//! │  child[i] offset = 40 + i*8                              │
//! └──────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ SECTION B — Key end offsets  [num_keys × 4 bytes]        │
//! │  key_end[i] = exclusive end of key[i] in Section C       │
//! └──────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ SECTION C — Key data  [variable, packed bytes]           │
//! │  key[i] spans [key_end[i-1], key_end[i])                 │
//! │  (key_end[-1] == 0 by convention)                        │
//! └──────────────────────────────────────────────────────────┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ HIGH KEY — stored at page[PAGE_SIZE - high_key_len ..]   │
//! │  The exclusive upper bound for keys on this page.        │
//! │  If high_key_len == 0, this is the rightmost page (+∞).  │
//! └──────────────────────────────────────────────────────────┘
//! ```

use common::Key;
use std::cmp::Ordering;
use std::marker::PhantomData;

use super::{
    ChildSide, INTERNAL, Lsn, OFF_LSN, OFF_PAGE_ID, OFF_PAGE_TYPE, PageError, PageId, read_u8,
    read_u16, read_u32, read_u64, write_u8, write_u16, write_u32, write_u64,
};

// ── Internal-page-specific header offsets ────────────────────────────────────

const OFF_INT_NUM_KEYS: usize = 2; // u16
const OFF_INT_HIGH_KEY_LEN: usize = 4; // u16
// bytes 6..8: u16 padding
const OFF_INT_RIGHTLINK: usize = 24; // u64
// bytes 32..36: u32 CRC32 checksum (see page::mod), 36..40: padding
const INT_HEADER_SIZE: usize = 40;

// ── Offset calculation helpers ────────────────────────────────────────────────

/// Byte offset of `child[i]` (Section A).
#[inline]
fn int_child_offset(i: usize) -> usize {
    INT_HEADER_SIZE + i * 8
}

/// Byte offset of the first byte of Section B (key-end offsets).
#[inline]
fn int_key_end_section(num_keys: usize) -> usize {
    INT_HEADER_SIZE + (num_keys + 1) * 8
}

/// Byte offset of `key_end[i]` (Section B entry).
#[inline]
fn int_key_end_offset(num_keys: usize, i: usize) -> usize {
    int_key_end_section(num_keys) + i * 4
}

/// Byte offset of the first byte of Section C (raw key data).
#[inline]
fn int_key_data_base(num_keys: usize) -> usize {
    int_key_end_section(num_keys) + num_keys * 4
}

// ── InternalPageAccessor ──────────────────────────────────────────────────────

/// Read-only typed view over a raw internal page buffer.
///
/// The lifetime `'a` is tied to the underlying page data, not to `&self`,
/// so `key_at` can return zero-copy `K::SelfType<'a>` values that outlive
/// the accessor itself.
pub struct InternalPageAccessor<'a, K: Key> {
    data: &'a [u8],
    num_keys: u16,
    key_data_base: usize,
    _key: PhantomData<K>,
}

impl<'a, K: Key> InternalPageAccessor<'a, K> {
    /// Wraps a raw page buffer.
    ///
    /// # Panics
    /// Panics if the page-type byte does not equal [`INTERNAL`].
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            INTERNAL,
            "InternalPageAccessor: page type byte is not INTERNAL"
        );
        let num_keys = read_u16(data, OFF_INT_NUM_KEYS);
        Self {
            data,
            num_keys,
            key_data_base: int_key_data_base(num_keys as usize),
            _key: PhantomData,
        }
    }

    pub fn lsn(&self) -> Lsn {
        read_u64(self.data, OFF_LSN)
    }

    pub fn page_id(&self) -> PageId {
        read_u64(self.data, OFF_PAGE_ID)
    }

    pub fn num_keys(&self) -> u16 {
        self.num_keys
    }

    /// Returns the right sibling page ID, or `None` if this is the rightmost
    /// page at this level.
    pub fn rightlink(&self) -> Option<PageId> {
        match read_u64(self.data, OFF_INT_RIGHTLINK) {
            0 => None,
            v => Some(v),
        }
    }

    /// Length of the high key in bytes. 0 means this is the rightmost page
    /// at this level (high key is +infinity).
    pub fn high_key_len(&self) -> u16 {
        read_u16(self.data, OFF_INT_HIGH_KEY_LEN)
    }

    /// Raw high key bytes, or `None` if this is the rightmost page (+infinity).
    pub fn high_key_bytes(&self) -> Option<&'a [u8]> {
        let len = self.high_key_len() as usize;
        if len == 0 {
            return None;
        }
        Some(&self.data[super::PAGE_SIZE - len..super::PAGE_SIZE])
    }

    /// Deserialized high key, or `None` if rightmost (+infinity).
    pub fn high_key(&self) -> Option<K::SelfType<'a>> {
        self.high_key_bytes().map(K::from_bytes)
    }

    pub fn child_page_at(&self, i: usize) -> PageId {
        read_u64(self.data, int_child_offset(i))
    }

    /// Deserialised key at position `i`. Lifetime is `'a` (tied to page data).
    pub fn key_at(&self, i: usize) -> K::SelfType<'a> {
        K::from_bytes(self.key_bytes_at(i))
    }

    /// Binary-search for the child to descend into for `search_key`.
    ///
    /// Returns `(child_index, child_page_id)` where `child_index` is the first
    /// separator key that is strictly greater than `search_key`, which is the
    /// correct subtree to follow.
    pub fn find_child(&self, search_key: &K::SelfType<'_>) -> (usize, PageId) {
        let n = self.num_keys as usize;
        let mut low = 0usize;
        let mut high = n;
        let search_bytes = K::as_bytes(search_key);
        let search_bytes = search_bytes.as_ref();

        while low < high {
            let mid = low + (high - low) / 2;
            match K::compare(self.key_bytes_at(mid), search_bytes) {
                Ordering::Greater => high = mid,
                Ordering::Less | Ordering::Equal => low = mid + 1,
            }
        }
        (low, self.child_page_at(low))
    }

    /// Contiguous free bytes remaining in this page (accounting for high key).
    pub fn free_bytes(&self) -> usize {
        super::PAGE_SIZE
            .saturating_sub(self.used_bytes())
            .saturating_sub(self.high_key_len() as usize)
    }

    /// Returns `true` if a new key of `key_len` bytes would fit on this page.
    pub fn can_fit(&self, key_len: usize) -> bool {
        self.free_bytes() >= 8 + 4 + key_len
    }

    /// Returns `true` if more than half the page is currently free.
    ///
    /// Used by the rebalance/vacuum paths to check whether a sibling can donate
    /// entries or whether a parent needs rebalancing after a merge.
    pub fn is_underfull(&self) -> bool {
        self.free_bytes() * 2 > super::PAGE_SIZE
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Raw bytes for `key[i]`. Lifetime `'a` lets `key_at` pass the slice
    /// directly to `K::from_bytes` without a copy.
    fn key_bytes_at(&self, i: usize) -> &'a [u8] {
        let n = self.num_keys as usize;
        let start = if i == 0 {
            0
        } else {
            read_u32(self.data, int_key_end_offset(n, i - 1)) as usize
        };
        let end = read_u32(self.data, int_key_end_offset(n, i)) as usize;
        &self.data[self.key_data_base + start..self.key_data_base + end]
    }

    fn used_bytes(&self) -> usize {
        let n = self.num_keys as usize;
        let key_data_size = if n == 0 {
            0
        } else {
            read_u32(self.data, int_key_end_offset(n, n - 1)) as usize
        };
        INT_HEADER_SIZE
            + (n + 1) * 8  // Section A: child pointers
            + n * 4         // Section B: key-end offsets
            + key_data_size // Section C: key data
    }
}

// ── InternalPageMutator ───────────────────────────────────────────────────────

/// Mutable typed view over a raw internal page buffer.
pub struct InternalPageMutator<'a, K: Key> {
    data: &'a mut [u8],
    _key: PhantomData<K>,
}

impl<'a, K: Key> InternalPageMutator<'a, K> {
    /// # Panics
    /// Panics if the page-type byte does not equal [`INTERNAL`].
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            INTERNAL,
            "InternalPageMutator: page type byte is not INTERNAL"
        );
        Self {
            data,
            _key: PhantomData,
        }
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        write_u64(self.data, OFF_LSN, lsn);
    }

    pub fn set_rightlink(&mut self, right: Option<PageId>) {
        write_u64(self.data, OFF_INT_RIGHTLINK, right.unwrap_or(0));
    }

    /// Write the high key. `key_bytes` is stored at the end of the page.
    /// Pass an empty slice (or call with `&[]`) for the rightmost page (+infinity).
    pub fn set_high_key(&mut self, key_bytes: &[u8]) {
        let len = key_bytes.len();
        write_u16(self.data, OFF_INT_HIGH_KEY_LEN, len as u16);
        if len > 0 {
            let start = super::PAGE_SIZE - len;
            self.data[start..super::PAGE_SIZE].copy_from_slice(key_bytes);
        }
    }

    /// Update a child pointer in-place (e.g. after a split assigns a new ID).
    pub fn set_child_at(&mut self, i: usize, page_id: PageId) {
        write_u64(self.data, int_child_offset(i), page_id);
    }

    /// Borrow as a read-only accessor without releasing the mutable borrow.
    pub fn as_accessor(&self) -> InternalPageAccessor<'_, K> {
        InternalPageAccessor::new(self.data)
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    /// Insert a promoted separator key at `index` after a child split.
    ///
    /// Before: `... | child[index] | key[index] | child[index+1] | ...`
    /// After:  `... | child[index] | key(new) | right_child | key[index] | ...`
    ///
    /// Returns `Err(InsufficientSpace)` when the page is too full.
    pub fn insert_key_and_right_child(
        &mut self,
        index: usize,
        key: &K::SelfType<'_>,
        right_child: PageId,
    ) -> Result<(), PageError> {
        let key_bytes = K::as_bytes(key);
        let key_bytes = key_bytes.as_ref();

        if !self.as_accessor().can_fit(key_bytes.len()) {
            return Err(PageError::InsufficientSpace {
                needed: 8 + 4 + key_bytes.len(),
                available: self.as_accessor().free_bytes(),
            });
        }

        let (mut children, mut keys) = self.snapshot();
        children.insert(index + 1, right_child);
        keys.insert(index, key_bytes.to_vec());
        self.rewrite(&children, &keys);
        Ok(())
    }

    // ── Remove ────────────────────────────────────────────────────────────────

    /// Remove `key[index]` and one adjacent child during a merge.
    ///
    /// `keep` controls which of the two children flanking the key is retained.
    pub fn remove_key_at(&mut self, index: usize, keep: ChildSide) {
        let (mut children, mut keys) = self.snapshot();
        keys.remove(index);
        match keep {
            ChildSide::Left => {
                children.remove(index + 1);
            }
            ChildSide::Right => {
                children.remove(index);
            }
        }
        self.rewrite(&children, &keys);
    }

    // ── Separator helpers ─────────────────────────────────────────────────────

    /// Replace `key[index]` with `new_key`, leaving children unchanged.
    pub fn update_separator_at(&mut self, index: usize, new_key: &K::SelfType<'_>) {
        let (children, mut keys) = self.snapshot();
        keys[index] = K::as_bytes(new_key).as_ref().to_vec();
        self.rewrite(&children, &keys);
    }

    /// Insert `new_key` before `key[0]` and `new_leftmost_child` before `child[0]`.
    pub fn prepend_separator(&mut self, new_key: &K::SelfType<'_>, new_leftmost_child: PageId) {
        let (mut children, mut keys) = self.snapshot();
        children.insert(0, new_leftmost_child);
        keys.insert(0, K::as_bytes(new_key).as_ref().to_vec());
        self.rewrite(&children, &keys);
    }

    // ── Truncate ──────────────────────────────────────────────────────────────

    /// Shrink this page to `num_keys` separator keys (and `num_keys + 1` children).
    ///
    /// All header fields (`page_id`, `lsn`, `rightlink`, `high_key`) are preserved
    /// because [`Self::rewrite`] only touches `num_keys` and Sections A/B/C.
    ///
    /// No-op if `num_keys >= current num_keys`.
    pub fn truncate_to(&mut self, num_keys: usize) {
        let n = read_u16(self.data, OFF_INT_NUM_KEYS) as usize;
        if num_keys >= n {
            return;
        }

        let children: Vec<PageId> = (0..=num_keys)
            .map(|i| read_u64(self.data, int_child_offset(i)))
            .collect();

        let base = int_key_data_base(n);
        let keys: Vec<Vec<u8>> = (0..num_keys)
            .map(|i| {
                let start = if i == 0 {
                    0
                } else {
                    read_u32(self.data, int_key_end_offset(n, i - 1)) as usize
                };
                let end = read_u32(self.data, int_key_end_offset(n, i)) as usize;
                self.data[base + start..base + end].to_vec()
            })
            .collect();

        self.rewrite(&children, &keys);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn snapshot(&self) -> (Vec<PageId>, Vec<Vec<u8>>) {
        let n = read_u16(self.data, OFF_INT_NUM_KEYS) as usize;

        let children = (0..=n)
            .map(|i| read_u64(self.data, int_child_offset(i)))
            .collect();

        let base = int_key_data_base(n);
        let keys = (0..n)
            .map(|i| {
                let start = if i == 0 {
                    0
                } else {
                    read_u32(self.data, int_key_end_offset(n, i - 1)) as usize
                };
                let end = read_u32(self.data, int_key_end_offset(n, i)) as usize;
                self.data[base + start..base + end].to_vec()
            })
            .collect();

        (children, keys)
    }

    fn rewrite(&mut self, children: &[PageId], keys: &[Vec<u8>]) {
        let new_n = keys.len();
        write_u16(self.data, OFF_INT_NUM_KEYS, new_n as u16);

        for (i, &child) in children.iter().enumerate() {
            write_u64(self.data, int_child_offset(i), child);
        }

        let mut cumulative = 0usize;
        for (i, key) in keys.iter().enumerate() {
            cumulative += key.len();
            write_u32(self.data, int_key_end_offset(new_n, i), cumulative as u32);
        }

        let base = int_key_data_base(new_n);
        let mut off = base;
        for key in keys {
            self.data[off..off + key.len()].copy_from_slice(key);
            off += key.len();
        }
    }
}

// ── InternalPageBuilder ───────────────────────────────────────────────────────

/// Write-once constructor for a fresh internal page.
pub struct InternalPageBuilder<'a, K: Key> {
    data: &'a mut [u8],
    keys: Vec<Vec<u8>>,
    children: Vec<PageId>,
    _key: PhantomData<K>,
}

impl<'a, K: Key> InternalPageBuilder<'a, K> {
    /// Zero the buffer, stamp the page type and page ID.
    /// Rightlink and high_key default to 0 (rightmost, +infinity).
    pub fn new(page_id: PageId, data: &'a mut [u8]) -> Self {
        data.fill(0);
        write_u8(data, OFF_PAGE_TYPE, INTERNAL);
        write_u64(data, OFF_PAGE_ID, page_id);
        // rightlink = 0 (rightmost), high_key_len = 0 (+infinity) — already zero
        Self {
            data,
            keys: Vec::new(),
            children: Vec::new(),
            _key: PhantomData,
        }
    }

    /// Register the leftmost child. Must be called exactly once, before any
    /// `push_key_and_right_child` calls.
    pub fn push_first_child(&mut self, child: PageId) {
        assert!(
            self.children.is_empty(),
            "InternalPageBuilder::push_first_child called more than once"
        );
        self.children.push(child);
    }

    /// Append a separator key and the right child that follows it.
    /// Keys must be pushed in strictly ascending order.
    pub fn push_key_and_right_child(&mut self, key: &K::SelfType<'_>, right_child: PageId) {
        assert!(
            !self.children.is_empty(),
            "InternalPageBuilder::push_key_and_right_child: call push_first_child first"
        );
        let key_bytes = K::as_bytes(key);
        self.keys.push(key_bytes.as_ref().to_vec());
        self.children.push(right_child);
    }

    /// Set the right sibling page ID. Call before `finish()`.
    pub fn set_rightlink(&mut self, right: Option<PageId>) {
        write_u64(self.data, OFF_INT_RIGHTLINK, right.unwrap_or(0));
    }

    /// Set the high key (exclusive upper bound). Call before `finish()`.
    /// Pass `&[]` for the rightmost page (+infinity).
    pub fn set_high_key(&mut self, key_bytes: &[u8]) {
        let len = key_bytes.len();
        write_u16(self.data, OFF_INT_HIGH_KEY_LEN, len as u16);
        if len > 0 {
            let start = super::PAGE_SIZE - len;
            self.data[start..super::PAGE_SIZE].copy_from_slice(key_bytes);
        }
    }

    /// Seal the page: flush all buffered data with the correct layout, then
    /// return a mutator for remaining header writes (lsn, etc.).
    pub fn finish(self) -> InternalPageMutator<'a, K> {
        let Self {
            data,
            keys,
            children,
            ..
        } = self;
        let num_keys = keys.len();

        write_u16(data, OFF_INT_NUM_KEYS, num_keys as u16);

        for (i, &child) in children.iter().enumerate() {
            write_u64(data, int_child_offset(i), child);
        }

        let mut cumulative = 0usize;
        for (i, key) in keys.iter().enumerate() {
            cumulative += key.len();
            write_u32(data, int_key_end_offset(num_keys, i), cumulative as u32);
        }

        let base = int_key_data_base(num_keys);
        let mut off = base;
        for key in &keys {
            data[off..off + key.len()].copy_from_slice(key);
            off += key.len();
        }

        InternalPageMutator::new(data)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageBuffer;

    /// Build an internal page from a slice of (key_bytes, right_child) pairs
    /// with an explicit leftmost child.
    fn build_page(page_id: u64, first_child: u64, entries: &[(&[u8], u64)]) -> PageBuffer {
        let mut buf = PageBuffer::new();
        let mut builder = InternalPageBuilder::<&[u8]>::new(page_id, buf.memory_mut());
        builder.push_first_child(first_child);
        for (key, child) in entries {
            builder.push_key_and_right_child(key, *child);
        }
        builder.finish();
        buf
    }

    #[test]
    fn build_and_read_keys_and_children() {
        let buf = build_page(1, 10, &[(&[5], 20), (&[10], 30), (&[15], 40)]);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());

        assert_eq!(acc.page_id(), 1);
        assert_eq!(acc.num_keys(), 3);
        assert_eq!(acc.child_page_at(0), 10);
        assert_eq!(acc.child_page_at(1), 20);
        assert_eq!(acc.child_page_at(2), 30);
        assert_eq!(acc.child_page_at(3), 40);
        assert_eq!(acc.key_at(0), &[5u8][..]);
        assert_eq!(acc.key_at(1), &[10u8][..]);
        assert_eq!(acc.key_at(2), &[15u8][..]);
    }

    #[test]
    fn rightlink_round_trip() {
        let mut buf = build_page(7, 1, &[(&[42], 2)]);
        {
            let mut m = InternalPageMutator::<&[u8]>::new(buf.memory_mut());
            m.set_rightlink(Some(99));
        }
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.rightlink(), Some(99));
    }

    #[test]
    fn rightlink_none_when_zero() {
        let buf = build_page(1, 10, &[]);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.rightlink(), None);
    }

    #[test]
    fn high_key_round_trip() {
        let mut buf = PageBuffer::new();
        {
            let mut builder = InternalPageBuilder::<&[u8]>::new(1, buf.memory_mut());
            builder.push_first_child(10);
            builder.push_key_and_right_child(&(&[5][..]), 20);
            builder.set_high_key(&[42, 43]);
            builder.set_rightlink(Some(99));
            builder.finish();
        }
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.high_key_len(), 2);
        assert_eq!(acc.high_key_bytes(), Some(&[42u8, 43][..]));
        assert_eq!(acc.high_key(), Some(&[42u8, 43][..]));
        assert_eq!(acc.rightlink(), Some(99));
    }

    #[test]
    fn high_key_none_when_rightmost() {
        let buf = build_page(1, 10, &[]);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.high_key_len(), 0);
        assert_eq!(acc.high_key_bytes(), None);
        assert_eq!(acc.high_key(), None);
    }

    #[test]
    fn lsn_round_trip() {
        let mut buf = build_page(7, 1, &[(&[42], 2)]);
        InternalPageMutator::<&[u8]>::new(buf.memory_mut()).set_lsn(99);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.lsn(), 99);
    }

    #[test]
    fn find_child_binary_search() {
        let buf = build_page(1, 1, &[(&[10], 2), (&[20], 3), (&[30], 4)]);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());

        let (idx, pid) = acc.find_child(&(&[5][..]));
        assert_eq!(idx, 0);
        assert_eq!(pid, 1);

        let (idx, pid) = acc.find_child(&(&[10][..]));
        assert_eq!(idx, 1);
        assert_eq!(pid, 2);

        let (idx, pid) = acc.find_child(&(&[15][..]));
        assert_eq!(idx, 1);
        assert_eq!(pid, 2);

        let (idx, pid) = acc.find_child(&(&[30][..]));
        assert_eq!(idx, 3);
        assert_eq!(pid, 4);

        let (idx, pid) = acc.find_child(&(&[99][..]));
        assert_eq!(idx, 3);
        assert_eq!(pid, 4);
    }

    #[test]
    fn insert_key_and_right_child() {
        let mut buf = build_page(1, 10, &[(&[10], 20), (&[30], 40)]);
        {
            let mut m = InternalPageMutator::<&[u8]>::new(buf.memory_mut());
            m.insert_key_and_right_child(1, &(&[20][..]), 30).unwrap();
        }
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.num_keys(), 3);
        assert_eq!(acc.key_at(0), &[10u8][..]);
        assert_eq!(acc.key_at(1), &[20u8][..]);
        assert_eq!(acc.key_at(2), &[30u8][..]);
        assert_eq!(acc.child_page_at(2), 30);
        assert_eq!(acc.child_page_at(3), 40);
    }

    #[test]
    fn insert_returns_err_when_full() {
        let mut buf = PageBuffer::new();
        {
            let mut b = InternalPageBuilder::<&[u8]>::new(1, buf.memory_mut());
            b.push_first_child(1);
            b.finish();
        }
        let big_key = vec![0u8; 200];
        let mut child_id = 2u64;
        loop {
            let mut m = InternalPageMutator::<&[u8]>::new(buf.memory_mut());
            if m.as_accessor().can_fit(big_key.len()) {
                let n = m.as_accessor().num_keys() as usize;
                m.insert_key_and_right_child(n, &big_key.as_slice(), child_id)
                    .unwrap();
                child_id += 1;
            } else {
                let result = m.insert_key_and_right_child(0, &big_key.as_slice(), 999);
                assert!(matches!(result, Err(PageError::InsufficientSpace { .. })));
                break;
            }
        }
    }

    #[test]
    fn remove_key_keep_left_child() {
        let mut buf = build_page(1, 1, &[(&[10], 2), (&[20], 3)]);
        InternalPageMutator::<&[u8]>::new(buf.memory_mut()).remove_key_at(0, ChildSide::Left);

        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.num_keys(), 1);
        assert_eq!(acc.key_at(0), &[20u8][..]);
        assert_eq!(acc.child_page_at(0), 1);
        assert_eq!(acc.child_page_at(1), 3);
    }

    #[test]
    fn remove_key_keep_right_child() {
        let mut buf = build_page(1, 1, &[(&[10], 2), (&[20], 3)]);
        InternalPageMutator::<&[u8]>::new(buf.memory_mut()).remove_key_at(0, ChildSide::Right);

        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.num_keys(), 1);
        assert_eq!(acc.key_at(0), &[20u8][..]);
        assert_eq!(acc.child_page_at(0), 2);
        assert_eq!(acc.child_page_at(1), 3);
    }

    #[test]
    fn set_child_at_updates_pointer() {
        let mut buf = build_page(1, 100, &[(&[5], 200)]);
        InternalPageMutator::<&[u8]>::new(buf.memory_mut()).set_child_at(1, 999);
        let acc = InternalPageAccessor::<&[u8]>::new(buf.memory());
        assert_eq!(acc.child_page_at(1), 999);
    }

    #[test]
    fn free_bytes_decreases_after_insert() {
        let mut buf = build_page(1, 1, &[]);
        let free_before = InternalPageAccessor::<&[u8]>::new(buf.memory()).free_bytes();
        InternalPageMutator::<&[u8]>::new(buf.memory_mut())
            .insert_key_and_right_child(0, &(&[42u8][..]), 2)
            .unwrap();
        let free_after = InternalPageAccessor::<&[u8]>::new(buf.memory()).free_bytes();
        assert_eq!(free_before - free_after, 8 + 4 + 1);
    }

    #[test]
    fn free_bytes_accounts_for_high_key() {
        let mut buf = PageBuffer::new();
        {
            let mut b = InternalPageBuilder::<&[u8]>::new(1, buf.memory_mut());
            b.push_first_child(10);
            b.set_high_key(&[1, 2, 3, 4, 5]); // 5 bytes
            b.finish();
        }
        let free_with_hk = InternalPageAccessor::<&[u8]>::new(buf.memory()).free_bytes();

        let mut buf2 = PageBuffer::new();
        {
            let mut b = InternalPageBuilder::<&[u8]>::new(1, buf2.memory_mut());
            b.push_first_child(10);
            b.finish();
        }
        let free_without_hk = InternalPageAccessor::<&[u8]>::new(buf2.memory()).free_bytes();

        assert_eq!(free_without_hk - free_with_hk, 5);
    }

    #[test]
    #[should_panic(expected = "InternalPageBuilder::push_first_child called more than once")]
    fn push_first_child_twice_panics() {
        let mut buf = PageBuffer::new();
        let mut b = InternalPageBuilder::<&[u8]>::new(1, buf.memory_mut());
        b.push_first_child(1);
        b.push_first_child(2);
    }

    #[test]
    #[should_panic(expected = "call push_first_child first")]
    fn push_key_without_first_child_panics() {
        let mut buf = PageBuffer::new();
        let mut b = InternalPageBuilder::<&[u8]>::new(1, buf.memory_mut());
        b.push_key_and_right_child(&(&[1][..]), 2);
    }
}

// ── Property-based tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::page::{PAGE_SIZE, PageBuffer};
    use proptest::prelude::*;
    use std::cmp::Ordering;

    // Helper functions to check if internal pages are valid
    // Internal pages are more complex than leaf pages - they have 3 sections!

    /// Check all the structural invariants for an internal page
    /// Internal pages store keys and child page pointers
    fn check_internal_structural_invariants(page: &[u8]) -> Result<(), String> {
        // First make sure this is actually an internal page
        let page_type = read_u8(page, OFF_PAGE_TYPE);
        if page_type != INTERNAL {
            return Err(format!(
                "page_type: expected {} (INTERNAL), got {}",
                INTERNAL, page_type
            ));
        }

        let num_keys = read_u16(page, OFF_INT_NUM_KEYS) as usize;
        let high_key_len = read_u16(page, OFF_INT_HIGH_KEY_LEN) as usize;

        // Check that all the child pointers fit in the page
        // We have num_keys + 1 children (Lehman-Yao B+tree property)
        let children_end = INT_HEADER_SIZE + (num_keys + 1) * 8;
        if children_end > PAGE_SIZE {
            return Err(format!(
                "children_overflow: {} children need {} bytes, PAGE_SIZE = {}",
                num_keys + 1,
                children_end,
                PAGE_SIZE
            ));
        }

        // Make sure all child pointers are valid (non-zero)
        // Zero is used as "no page" in other places, but here every child must be real
        for i in 0..=num_keys {
            let child = read_u64(page, int_child_offset(i));
            if child == 0 {
                return Err(format!(
                    "child_{}_zero: child page ID is 0 (should be a valid page ID)",
                    i
                ));
            }
        }

        // Check that the key-end offsets are in increasing order
        // These tell us where each key starts and ends in Section C
        if num_keys > 0 {
            let mut prev_end = 0u32;
            for i in 0..num_keys {
                let key_end = read_u32(page, int_key_end_offset(num_keys, i));
                if key_end <= prev_end {
                    return Err(format!(
                        "key_end_not_monotonic: key_end[{}] ({}) <= key_end[{}] ({})",
                        i,
                        key_end,
                        if i == 0 { 0 } else { i - 1 },
                        prev_end
                    ));
                }
                prev_end = key_end;
            }

            // Make sure the key data doesn't overlap with the high key
            // High key is stored at the END of the page
            let total_key_data = prev_end as usize;
            let key_data_start = int_key_data_base(num_keys);
            let key_data_end = key_data_start + total_key_data;
            let high_key_start = PAGE_SIZE - high_key_len;

            if key_data_end > high_key_start {
                return Err(format!(
                    "key_data_overlap: key data region [{}, {}) overlaps high key region [{}, {})",
                    key_data_start, key_data_end, high_key_start, PAGE_SIZE
                ));
            }
        }

        Ok(())
    }

    /// Check that keys are in sorted order on an internal page
    fn check_internal_key_ordering(page: &[u8]) -> Result<(), String> {
        type K = &'static [u8];

        let acc = InternalPageAccessor::<K>::new(page);
        let n = acc.num_keys() as usize;

        // Make sure keys are sorted (allow duplicates for MVCC)
        for i in 0..n.saturating_sub(1) {
            let k1 = acc.key_at(i);
            let k2 = acc.key_at(i + 1);
            if <K as Key>::compare(
                <K as common::Value>::as_bytes(&k1).as_ref(),
                <K as common::Value>::as_bytes(&k2).as_ref(),
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

        // If there's a high key, make sure all keys are less than it
        if let Some(hk) = acc.high_key_bytes() {
            for i in 0..n {
                let k = acc.key_at(i);
                if <K as Key>::compare(<K as common::Value>::as_bytes(&k).as_ref(), hk)
                    == Ordering::Greater
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

    /// Main checker - calls both structural and ordering checks
    fn check_internal_all_invariants(page: &[u8]) -> Result<(), String> {
        check_internal_structural_invariants(page)?;
        check_internal_key_ordering(page)?;
        Ok(())
    }

    // ── Test data generators for internal pages ────────────────────────────────

    type K = &'static [u8];

    /// Generate random keys (same as lea`f page tests)
    fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            // 80% chance: small pool of keys (length 1-2, values 0-3) to ensure duplicates
            80 => prop::collection::vec(0u8..4u8, 1..=2usize),
            // 20% chance: completely random keys up to 16 bytes
            20 => prop::collection::vec(any::<u8>(), 1..=16usize),
        ]
    }

    /// Operations we can do on internal pages
    #[derive(Debug, Clone)]
    enum InternalOp {
        Insert { key: Vec<u8>, child_id: u64 },
        RemoveLeft { index: usize },
        RemoveRight { index: usize },
    }

    fn internal_op_strategy() -> impl Strategy<Value = InternalOp> {
        prop_oneof![
            60 => (key_strategy(), 1u64..10000)
                .prop_map(|(k, c)| InternalOp::Insert { key: k, child_id: c }),
            20 => (0usize..100)
                .prop_map(|idx| InternalOp::RemoveLeft { index: idx }),
            20 => (0usize..100)
                .prop_map(|idx| InternalOp::RemoveRight { index: idx }),
        ]
    }

    /// Sort keys, returning sorted keys (allowing duplicates).
    fn sort_keys(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        keys.sort();
        keys
    }

    // ── Property tests ───────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Building an internal page from sorted unique keys always produces a valid page.
        #[test]
        fn prop_internal_build_valid(
            keys in prop::collection::vec(key_strategy(), 1..20),
            first_child in 1u64..1000,
        ) {
            let sorted = sort_keys(keys);
            let mut buf = PageBuffer::new();
            let mut builder = InternalPageBuilder::<K>::new(1, buf.memory_mut());
            builder.push_first_child(first_child);

            // Pre-calculate available space: PAGE_SIZE - header - first_child(8) - high_key(0)
            let mut used = INT_HEADER_SIZE + 8; // header + first child pointer
            let mut child_id = first_child + 1;
            for k in &sorted {
                // Each key+child costs: 8 (child) + 4 (key_end offset) + key.len() (key data)
                let cost = 8 + 4 + k.len();
                if used + cost > PAGE_SIZE {
                    break;
                }
                builder.push_key_and_right_child(&k.as_slice(), child_id);
                used += cost;
                child_id += 1;
            }
            builder.finish();

            check_internal_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after build: {}", e)))?;
        }

        /// Building with a high key produces a valid page where all keys < high_key.
        #[test]
        fn prop_internal_build_with_high_key(
            keys in prop::collection::vec(key_strategy(), 1..15),
            first_child in 1u64..1000,
        ) {
            let sorted = sort_keys(keys);
            let mut buf = PageBuffer::new();

            let high_key = vec![0xFF; 17]; // larger than any 16-byte key

            let mut builder = InternalPageBuilder::<K>::new(1, buf.memory_mut());
            builder.push_first_child(first_child);
            builder.set_high_key(&high_key);

            let mut child_id = first_child + 1;
            for k in &sorted {
                // Conservative: stop after a few to avoid overflow
                if child_id > first_child + 10 {
                    break;
                }
                builder.push_key_and_right_child(&k.as_slice(), child_id);
                child_id += 1;
            }
            builder.finish();

            check_internal_all_invariants(buf.memory())
                .map_err(|e| TestCaseError::fail(format!("after build with high key: {}", e)))?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Arbitrary insert/remove sequences on an internal page preserve invariants.
        #[test]
        fn prop_internal_op_sequence_maintains_invariants(
            ops in prop::collection::vec(internal_op_strategy(), 1..20)
        ) {
            // Build a minimal internal page: first_child only
            let mut buf = PageBuffer::new();
            {
                let mut builder = InternalPageBuilder::<K>::new(1, buf.memory_mut());
                builder.push_first_child(1);
                builder.finish();
            }

            let mut next_child: u64 = 2;

            for (i, op) in ops.iter().enumerate() {
                match op {
                    InternalOp::Insert { key, child_id } => {
                        let acc = InternalPageAccessor::<K>::new(buf.memory());
                        if !acc.can_fit(key.len()) {
                            continue;
                        }
                        // Find insert position using binary search
                        let n = acc.num_keys() as usize;
                        let mut lo = 0usize;
                        let mut hi = n;
                        while lo < hi {
                            let mid = lo + (hi - lo) / 2;
                            if <K as Key>::compare(
                                <K as common::Value>::as_bytes(&acc.key_at(mid)).as_ref(),
                                key.as_slice(),
                            ) == Ordering::Greater
                            {
                                hi = mid;
                            } else {
                                lo = mid + 1;
                            }
                        }

                        let child = if *child_id == 0 { next_child } else { *child_id };
                        next_child = next_child.max(child + 1);

                        drop(acc);
                        InternalPageMutator::<K>::new(buf.memory_mut())
                            .insert_key_and_right_child(lo, &key.as_slice(), child)
                            .unwrap();
                    }
                    InternalOp::RemoveLeft { index } => {
                        let n = InternalPageAccessor::<K>::new(buf.memory())
                            .num_keys() as usize;
                        if n == 0 {
                            continue;
                        }
                        let pos = (*index).min(n - 1);
                        InternalPageMutator::<K>::new(buf.memory_mut())
                            .remove_key_at(pos, ChildSide::Left);
                    }
                    InternalOp::RemoveRight { index } => {
                        let n = InternalPageAccessor::<K>::new(buf.memory())
                            .num_keys() as usize;
                        if n == 0 {
                            continue;
                        }
                        let pos = (*index).min(n - 1);
                        InternalPageMutator::<K>::new(buf.memory_mut())
                            .remove_key_at(pos, ChildSide::Right);
                    }
                }

                // Check invariants after EVERY operation
                check_internal_all_invariants(buf.memory()).map_err(|e| {
                    TestCaseError::fail(format!(
                        "Invariant violated after op #{} ({:?}): {}",
                        i, op, e
                    ))
                })?;
            }
        }
    }
}
