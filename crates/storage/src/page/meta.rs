//! Metadata / superblock page — always page 0 of the database file.
//!
//! Records where the B+Tree root lives so the database can be reopened without
//! the caller already knowing the root page id. It flows through the buffer pool
//! like any other page, so it inherits the per-page CRC32 (stamped on flush,
//! verified on load) — a torn superblock write surfaces as `PageCorruption`.
//!
//! ## Layout
//! ```text
//! Off  0  u8   page_type = META
//! Off  4  u32  magic ("FLUX")
//! Off  8  u64  page_id (= 0; kept for header uniformity)
//! Off 16  u64  lsn     (= 0; reserved for WAL)
//! Off 24  u32  format_version
//! Off 32  u64  root_page_id
//! Off 40  u64  next_page_id    (reserved — free-space mgmt, #2)
//! Off 48  u64  free_list_head  (reserved — free-space mgmt, #2)
//! Off 56  u32  checksum (CRC32)
//! ```   
use super::{
    META, OFF_PAGE_TYPE, PageId, read_u8, read_u32, read_u64, write_u8, write_u32, write_u64,
};

/// marker bytes identifiying a fluxDB database file.
pub const MAGIC: u32 = u32::from_le_bytes(*b"FLUX");
/// on-disk format version understood by this build.
pub const FORMAT_VERSION: u32 = 1;

const OFF_MAGIC: usize = 4;
const OFF_VERSION: usize = 24;
const OFF_ROOT: usize = 32;
const OFF_FREE_SPACE: usize = 48;
// 40..56 reserved (next_page_id, free_list_head) — issue #2.
// The checksum at offset 56 is owned by `super::OFF_META_CHECKSUM` (the CRC is
// stamped/verified centrally by the buffer pool, like leaf/internal pages).

pub fn init(page: &mut [u8], root_page_id: PageId) {
    write_u8(page, OFF_PAGE_TYPE, META);
    write_u32(page, OFF_MAGIC, MAGIC);
    write_u32(page, OFF_VERSION, FORMAT_VERSION);
    write_u64(page, OFF_ROOT, root_page_id);
}

pub fn set_root(page: &mut [u8], root_page_id: PageId) {
    write_u64(page, OFF_ROOT, root_page_id);
}

/// Stamp the LSN of the last WAL record that modified page 0.
pub fn set_lsn(page: &mut [u8], lsn: u64) {
    write_u64(page, super::OFF_LSN, lsn);
}

pub fn read_root(page: &[u8]) -> Option<PageId> {
    if read_u8(page, OFF_PAGE_TYPE) != META
        || read_u32(page, OFF_MAGIC) != MAGIC
        || read_u32(page, OFF_VERSION) != FORMAT_VERSION
    {
        return None;
    }
    Some(read_u64(page, OFF_ROOT))
}

pub fn set_free_space(page: &mut [u8], pid: PageId) {
    write_u64(page, OFF_FREE_SPACE, pid);
}

pub fn read_free_space(page: &[u8]) -> PageId {
    read_u64(page, OFF_FREE_SPACE)
}
