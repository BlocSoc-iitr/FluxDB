//! B+Tree page layer.
//!
//! This module is split into two sub-modules:
//! - [`internal`] — branch (internal) page types
//! - [`leaf`]     — data (leaf) page types
//!
//! Shared constants, helpers, and error types live here.

pub mod internal;
pub mod leaf;
pub mod meta;
pub mod overflow;
pub use internal::{InternalPageAccessor, InternalPageBuilder, InternalPageMutator};
pub use leaf::{LeafPageAccessor, LeafPageBuilder, LeafPageMutator};

pub use common::PageError;

// ── Type aliases ──────────────────────────────────────────────────────────────

pub type PageId = u64;
pub type Lsn = u64;
pub type SlotId = u16;

// ── Page-type marker bytes ────────────────────────────────────────────────────

/// Marker byte stored at offset 0 of every leaf page.
pub const LEAF: u8 = 1;
/// Marker byte stored at offset 0 of every internal page.
pub const INTERNAL: u8 = 2;
/// Marker byte stored at offset 0 of metadata page
pub const META: u8 = 3;
/// Marker byte stored at offset 0 of every overflow page.
pub const OVERFLOW: u8 = 4;
// ── Page size ─────────────────────────────────────────────────────────────────

/// Canonical page size used throughout the storage engine (4 KB).
pub const PAGE_SIZE: usize = 4096;

pub const OVERFLOW_THRESHHOLD: usize = 2048;

// ── Shared header offsets (present in all three page types) ───────────────────────

pub(super) const OFF_PAGE_TYPE: usize = 0; // u8
pub(super) const OFF_FLAGS: usize = 1; // u8 — page-header flag bits
pub(super) const OFF_PAGE_ID: usize = 8; // u64
pub(super) const OFF_LSN: usize = 16; // u64

/// Page split but its parent lacks the downlink yet (cleared on InsertDownlink).
pub const FLAG_INCOMPLETE_SPLIT: u8 = 0b0000_0001;

/// Stamp the LSN of the last WAL record that touched the page (any page type).
pub fn set_lsn(page: &mut [u8], lsn: u64) {
    write_u64(page, OFF_LSN, lsn);
}

pub fn is_incomplete_split(page: &[u8]) -> bool {
    read_u8(page, OFF_FLAGS) & FLAG_INCOMPLETE_SPLIT != 0
}

pub fn set_incomplete_split(page: &mut [u8]) {
    let f = read_u8(page, OFF_FLAGS) | FLAG_INCOMPLETE_SPLIT;
    write_u8(page, OFF_FLAGS, f);
}

pub fn clear_incomplete_split(page: &mut [u8]) {
    let f = read_u8(page, OFF_FLAGS) & !FLAG_INCOMPLETE_SPLIT;
    write_u8(page, OFF_FLAGS, f);
}

const CHECKSUM_LEN: usize = 4;
/// Checksum offset for a leaf page .
const OFF_LEAF_CHECKSUM: usize = 44;
/// Checksum offset for an internal page (first word of the fixed header).
const OFF_INT_CHECKSUM: usize = 32;
/// checksum offset for metadata page
const OFF_META_CHECKSUM: usize = 56;
/// checksum offset for overflow page
const OFF_OVERFLOW_CHECKSUM: usize = 44;
/// Byte offset of the CRC32 field for the given page-type marker, or `None` for
/// an unrecognised type (e.g. a never-initialised, all-zero page) which carries
/// no checksum to verify.
#[inline]
fn checksum_offset(page_type: u8) -> Option<usize> {
    match page_type {
        LEAF => Some(OFF_LEAF_CHECKSUM),
        INTERNAL => Some(OFF_INT_CHECKSUM),
        META => Some(OFF_META_CHECKSUM),
        OVERFLOW => Some(OFF_OVERFLOW_CHECKSUM),

        _ => None,
    }
}

/// CRC32 over the whole page with the 4-byte checksum field itself treated as
/// zero, so the result is independent of whatever is currently stored there.
fn compute_checksum(page: &[u8], off: usize) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&page[..off]);
    hasher.update(&[0u8; CHECKSUM_LEN]);
    hasher.update(&page[off + CHECKSUM_LEN..]);
    hasher.finalize()
}

/// Recompute and write the page's CRC32. Call immediately before flushing a page
/// to disk. No-op for unrecognised page types (nothing to protect yet).
pub fn stamp_checksum(page: &mut [u8]) {
    if let Some(off) = checksum_offset(read_u8(page, OFF_PAGE_TYPE)) {
        let crc = compute_checksum(page, off);
        write_u32(page, off, crc);
    }
}

/// Verify a page loaded from disk against its stored CRC32. Returns
/// `Err((expected, actual))` on mismatch, `Ok(())` when the checksum matches or
/// the page type carries no checksum.
pub fn verify_checksum(page: &[u8]) -> Result<(), (u32, u32)> {
    if let Some(off) = checksum_offset(read_u8(page, OFF_PAGE_TYPE)) {
        let stored = read_u32(page, off);
        let actual = compute_checksum(page, off);
        if stored != actual {
            return Err((stored, actual));
        }
    }
    Ok(())
}

// ── ChildSide — used when removing a separator key during a merge ─────────────

/// Which adjacent child to keep when a separator key is removed during a merge.
pub enum ChildSide {
    /// Keep `child[index]`, drop `child[index + 1]`.
    Left,
    /// Keep `child[index + 1]`, drop `child[index]`.
    Right,
}

// ── PageBuffer ────────────────────────────────────────────────────────────────

/// Heap-allocated, zero-initialised 4 KB page buffer.
///
/// Using a `Box<[u8; PAGE_SIZE]>` rather than a bare array prevents stack
/// overflows in debug builds while keeping the storage contiguous.
pub struct PageBuffer {
    data: Box<[u8; PAGE_SIZE]>,
}

impl PageBuffer {
    /// Allocates a new zeroed page buffer.
    pub fn new() -> Self {
        Self {
            // vec! avoids a stack-allocated [0; 4096] before moving to the Box.
            data: vec![0u8; PAGE_SIZE].into_boxed_slice().try_into().unwrap(),
        }
    }

    /// Read-only view of the raw bytes.
    pub fn memory(&self) -> &[u8] {
        self.data.as_ref()
    }

    /// Mutable view of the raw bytes.
    pub fn memory_mut(&mut self) -> &mut [u8] {
        self.data.as_mut()
    }
}

impl Default for PageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// ── Low-level read / write helpers ────────────────────────────────────────────
// These are plain functions (not methods) so both sub-modules can use them
// with a simple `use super::{read_u8, ...}` import.

/// Read a page's LSN (the `pageLSN`) straight from its raw bytes.
///
/// Used by the buffer pool's flush seam to enforce WAL-before-page without
/// having to know the page type. Returns 0 for a never-logged page.
#[inline]
pub fn page_lsn(data: &[u8]) -> Lsn {
    read_u64(data, OFF_LSN)
}

#[inline]
pub(super) fn read_u8(data: &[u8], off: usize) -> u8 {
    data[off]
}

#[inline]
pub(super) fn read_u16(data: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(data[off..off + 2].try_into().unwrap())
}

#[inline]
pub(super) fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

#[inline]
pub(super) fn read_u64(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
}

#[inline]
pub(super) fn write_u8(data: &mut [u8], off: usize, val: u8) {
    data[off] = val;
}

#[inline]
pub(super) fn write_u16(data: &mut [u8], off: usize, val: u16) {
    data[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub(super) fn write_u32(data: &mut [u8], off: usize, val: u32) {
    data[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
pub(super) fn write_u64(data: &mut [u8], off: usize, val: u64) {
    data[off..off + 8].copy_from_slice(&val.to_le_bytes());
}
