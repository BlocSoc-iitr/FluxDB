//! Free-space bitmap page — tracks page ids freed by vacuum for reuse
//!
//! One bit per page id (`set` = free). A single 4 KB page covers
//! ~32 k page ids; ids at or beyond `CAPACITY` are simply leaked
//! (bounded — see issue #89)
//!
//! ## Layout
//! Off  0  u8   page_type = FREE_SPACE
//! Off  8  u64  page_id
//! Off 16  u64  lsn
//! Off 24  u32  checksum (CRC32)
//! Off 32..     bitmap — bit n corresponds to page id n
use super::{FREE_SPACE, OFF_PAGE_ID, OFF_PAGE_TYPE, PAGE_SIZE, PageId, write_u8, write_u64};

/// First byte of the bitmap (everything before it is header + checksum)
const BITMAP_START: usize = 32;
/// Number of page ids the bitmap can track (exclusive upper bound)
pub const CAPACITY: u64 = ((PAGE_SIZE - BITMAP_START) * 8) as u64;

pub fn init(page: &mut [u8], page_id: PageId) {
    write_u8(page, OFF_PAGE_TYPE, FREE_SPACE);
    write_u64(page, OFF_PAGE_ID, page_id);
}

#[inline]
pub(super) fn locate(pid: PageId) -> (usize, u8) {
    (BITMAP_START + (pid / 8) as usize, (pid % 8) as u8)
}

/// Mark `pid` as free. Returns `false` when `pid` is beyond the bitmap's
/// capacity — the caller just leaks that id (the bounded leak path)
pub fn set_free(page: &mut [u8], pid: PageId) -> bool {
    if pid >= CAPACITY {
        return false;
    }
    let (byte, bit) = locate(pid);
    page[byte] |= 1 << bit;
    true
}

/// Mark `pid` as in use again.
pub fn clear_free(page: &mut [u8], pid: PageId) {
    if pid >= CAPACITY {
        return;
    }
    let (byte, bit) = locate(pid);
    page[byte] &= !(1 << bit);
}

/// All free page ids currently set in the bitmap.
pub fn scan_free(page: &[u8]) -> Vec<PageId> {
    let mut free = Vec::new();
    for (byte, b) in page.iter().enumerate().skip(BITMAP_START) {
        let mut bits = *b;
        while bits != 0 {
            let bit = bits.trailing_zeros();
            free.push(((byte - BITMAP_START) as u64) * 8 + bit as u64);
            bits &= bits - 1; // clear lowest set bit (local copy only)
        }
    }
    free
}
