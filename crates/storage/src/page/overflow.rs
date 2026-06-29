//! Overflow page type for the FluxDB storage engine.
//!
//! ## Layout
//!
//! ```text
//! Page size: 4096 bytes
//!
//! ┌─────────────────────────────────────────────────────────┐
//! │ FIXED HEADER — 48 bytes (fully 8-byte aligned)          │
//! ├────────┬───────────┬─────────────────────────────────── ┤
//! │ Off  0 │ u8        │ page_type  (= OVERFLOW = 4)        │
//! │ Off  1 │ u8        │ _reserved  (0, never set_incomplete_split) │
//! │ Off  2 │ [6 bytes] │ _padding                           │
//! ├────────┼───────────┼─────────────────────────────────── ┤
//! │ Off  8 │ u64       │ page_id          [8-byte aligned]  │
//! │ Off 16 │ u64       │ lsn              [8-byte aligned]  │
//! │ Off 24 │ u64       │ next_page_id     [8-byte aligned]  │
//! │        │           │  (0 = end of chain)                │
//! │ Off 32 │ u16       │ chunk_len  (bytes used in payload) │
//! │ Off 34 │ [10 bytes]│ _padding                           │
//! │ Off 44 │ u32       │ checksum (CRC32)                   │
//! └────────┴───────────┴─────────────────────────────────── ┘
//!
//! ┌──────────────────────────────────────────────────────────┐
//! │ DATA PAYLOAD  [offset 48 .. PAGE_SIZE]                   │
//! │  Up to 4048 bytes of raw value chunk per page.           │
//! │  Filled from offset 48 upward; only chunk_len bytes      │
//! │  are valid. The rest is zeroed.                          │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! Overflow pages form a singly-linked chain. The last page in the
//! chain has `next_page_id = 0`. `chunk_len` records how many bytes
//! of the payload region are actually populated on this page.
//!
//! These pages have no slot directory and never participate in B+Tree
//! splits — `_reserved` (offset 1) must never be touched by the
//! `set_incomplete_split` helper.

use super::{
    Lsn, OFF_LSN, OFF_PAGE_ID, OFF_PAGE_TYPE, OVERFLOW, PageError, PageId, read_u8, read_u16,
    read_u64, write_u8, write_u16, write_u64,
};

// ── Overflow-page-specific header offsets ────────────────────────────────────────
const OFF_OVERFLOW_NEXT_PAGE_ID: usize = 24;
const OFF_OVERFLOW_CHUNK_LEN: usize = 32;
const OVERFLOW_PAYLOAD_SIZE: usize = 4048;
const OVERFLOW_HEADER_SIZE: usize = 48;

pub const OVERFLOW_DESCRIPTOR_SIZE: usize = 12; // page_id (8 bytes) + total_size (4 bytes)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowDescriptor {
    pub first_page_id: PageId,
    pub total_size: u32,
}

impl OverflowDescriptor {
    pub fn to_bytes(self) -> [u8; OVERFLOW_DESCRIPTOR_SIZE] {
        let mut buf = [0u8; OVERFLOW_DESCRIPTOR_SIZE];
        buf[0..8].copy_from_slice(&self.first_page_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.total_size.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < OVERFLOW_DESCRIPTOR_SIZE {
            return None;
        }
        let first_page_id = PageId::from_le_bytes(bytes[0..8].try_into().unwrap());
        let total_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        Some(Self {
            first_page_id,
            total_size,
        })
    }
}
pub struct OverflowPageBuilder<'a> {
    data: &'a mut [u8],
}

impl<'a> OverflowPageBuilder<'a> {
    pub fn new(page_id: PageId, data: &'a mut [u8]) -> Self {
        data.fill(0);
        write_u8(data, OFF_PAGE_TYPE, OVERFLOW);
        write_u64(data, OFF_PAGE_ID, page_id);

        Self { data }
    }
    pub fn set_next_page_id(&mut self, next_page: Option<PageId>) {
        write_u64(self.data, OFF_OVERFLOW_NEXT_PAGE_ID, next_page.unwrap_or(0));
    }

    pub fn set_chunk(&mut self, chunk: &[u8]) -> Result<(), PageError> {
        if chunk.len() > OVERFLOW_PAYLOAD_SIZE {
            return Err(PageError::InsufficientSpace {
                needed: chunk.len(),
                available: OVERFLOW_PAYLOAD_SIZE,
            });
        }

        write_u16(self.data, OFF_OVERFLOW_CHUNK_LEN, chunk.len() as u16);
        self.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);
        Ok(())
    }

    pub fn finish(self) -> OverflowPageMutator<'a> {
        OverflowPageMutator::new(self.data)
    }
}
pub struct OverflowPageAccessor<'a> {
    data: &'a [u8],
}

impl<'a> OverflowPageAccessor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            OVERFLOW,
            "OverflowPageAccessor: page type byte is not OVERFLOW"
        );
        Self { data }
    }

    pub fn page_id(&self) -> PageId {
        read_u64(self.data, OFF_PAGE_ID)
    }

    pub fn lsn(&self) -> Lsn {
        read_u64(self.data, OFF_LSN)
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        match read_u64(self.data, OFF_OVERFLOW_NEXT_PAGE_ID) {
            0 => None,
            v => Some(v),
        }
    }

    pub fn chunk_len(&self) -> u16 {
        read_u16(self.data, OFF_OVERFLOW_CHUNK_LEN)
    }

    pub fn payload(&self) -> &'a [u8] {
        &self.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + self.chunk_len() as usize]
    }
}
pub struct OverflowPageMutator<'a> {
    data: &'a mut [u8],
}

impl<'a> OverflowPageMutator<'a> {
    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(
            read_u8(data, OFF_PAGE_TYPE),
            OVERFLOW,
            "OverflowPageAccessor: page type byte is not OVERFLOW"
        );
        Self { data }
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        write_u64(self.data, OFF_LSN, lsn);
    }

    pub fn as_accessor(&self) -> OverflowPageAccessor<'_> {
        OverflowPageAccessor::new(self.data)
    }

    pub fn set_next_page_id(&mut self, next_page: Option<PageId>) {
        write_u64(self.data, OFF_OVERFLOW_NEXT_PAGE_ID, next_page.unwrap_or(0));
    }

    pub fn set_chunk(&mut self, chunk: &[u8]) -> Result<(), PageError> {
        if chunk.len() > OVERFLOW_PAYLOAD_SIZE {
            return Err(PageError::InsufficientSpace {
                needed: chunk.len(),
                available: OVERFLOW_PAYLOAD_SIZE,
            });
        }

        write_u16(self.data, OFF_OVERFLOW_CHUNK_LEN, chunk.len() as u16);
        self.data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);
        Ok(())
    }
}

use crate::buffer_pool::BufferPoolManager;
use crate::page::PAGE_SIZE;
use crate::wal::Wal;
use common::IndexError;

pub fn write_overflow_chain(
    value: &[u8],
    pool: &BufferPoolManager,
    wal: &Wal,
    txn_id: u64,
) -> Result<OverflowDescriptor, IndexError> {
    let total_size = value.len() as u32;
    let chunks: Vec<&[u8]> = value.chunks(OVERFLOW_PAYLOAD_SIZE).collect();

    // Allocate all pages first so we know their IDs before linking
    let mut guards = Vec::with_capacity(chunks.len());
    for _ in &chunks {
        guards.push(pool.new_page()?);
    }

    let first_page_id = guards[0].page_id;

    // Build pages back to front so next_page_id is known when we write each page
    for i in (0..chunks.len()).rev() {
        let next_page_id = if i + 1 < chunks.len() {
            Some(guards[i + 1].page_id)
        } else {
            None
        };

        let page_id = guards[i].page_id;
        let mut builder = OverflowPageBuilder::new(page_id, &mut guards[i][..]);
        builder.set_next_page_id(next_page_id);
        builder.set_chunk(chunks[i])?;
        builder.finish();

        let image: &[u8; PAGE_SIZE] = (&guards[i][..]).try_into().unwrap();
        let lsn = wal.log_overflow_write(txn_id, page_id, image)?;
        OverflowPageMutator::new(&mut guards[i][..]).set_lsn(lsn);
    }

    Ok(OverflowDescriptor {
        first_page_id,
        total_size,
    })
}

pub fn read_overflow_chain(
    desc: OverflowDescriptor,
    pool: &BufferPoolManager,
) -> Result<Vec<u8>, IndexError> {
    let mut result = Vec::with_capacity(desc.total_size as usize);
    let mut current_page_id = Some(desc.first_page_id);

    while let Some(pid) = current_page_id {
        let guard = pool.fetch_page(pid)?;
        let acc = OverflowPageAccessor::new(&guard[..]);
        result.extend_from_slice(acc.payload());
        current_page_id = acc.next_page_id();
    }

    Ok(result)
}

/// Walk an overflow chain from `first_page_id` and return every page ID it
/// contains. Used before deallocation so the full free set can be WAL-logged
/// (the descriptor only stores `first_page_id`; the rest live in `next_page_id`
/// links).
pub fn collect_overflow_page_ids(
    first_page_id: PageId,
    pool: &BufferPoolManager,
) -> Result<Vec<PageId>, IndexError> {
    let mut ids = Vec::new();
    let mut current_page_id = Some(first_page_id);

    while let Some(pid) = current_page_id {
        let guard = pool.fetch_page(pid)?;
        let acc = OverflowPageAccessor::new(&guard[..]);
        ids.push(pid);
        current_page_id = acc.next_page_id();
    }

    Ok(ids)
}

pub fn free_overflow_chain(
    first_page_id: PageId,
    pool: &BufferPoolManager,
) -> Result<(), IndexError> {
    let mut current_page_id = Some(first_page_id);

    while let Some(pid) = current_page_id {
        // Fetch to read next_page_id before deleting
        let next = {
            let guard = pool.fetch_page(pid)?;
            let acc = OverflowPageAccessor::new(&guard[..]);
            acc.next_page_id()
        };

        pool.delete_page(pid)?;
        current_page_id = next;
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{OFF_PAGE_TYPE, PageBuffer, read_u8};

    // ── OverflowDescriptor ────────────────────────────────────────────────────

    #[test]
    fn descriptor_round_trip() {
        let desc = OverflowDescriptor {
            first_page_id: 0xDEAD_BEEF,
            total_size: 123_456,
        };
        let bytes = desc.to_bytes();
        assert_eq!(bytes.len(), OVERFLOW_DESCRIPTOR_SIZE);
        assert_eq!(OverflowDescriptor::from_bytes(&bytes), Some(desc));
    }

    #[test]
    fn descriptor_from_short_bytes_is_none() {
        // One byte short of a full descriptor → cannot decode.
        let short = [0u8; OVERFLOW_DESCRIPTOR_SIZE - 1];
        assert_eq!(OverflowDescriptor::from_bytes(&short), None);
    }

    #[test]
    fn descriptor_from_bytes_ignores_trailing_bytes() {
        // A buffer longer than the descriptor decodes from its 12-byte prefix.
        let desc = OverflowDescriptor {
            first_page_id: 7,
            total_size: 9,
        };
        let mut buf = desc.to_bytes().to_vec();
        buf.extend_from_slice(&[0xAB; 4]);
        assert_eq!(OverflowDescriptor::from_bytes(&buf), Some(desc));
    }

    // ── Builder / accessor ────────────────────────────────────────────────────

    #[test]
    fn builder_stamps_header() {
        let mut buf = PageBuffer::new();
        let chunk = [0x5Au8; 100];
        {
            let mut b = OverflowPageBuilder::new(42, buf.memory_mut());
            b.set_next_page_id(Some(43));
            b.set_chunk(&chunk).unwrap();
            b.finish();
        }

        assert_eq!(read_u8(buf.memory(), OFF_PAGE_TYPE), OVERFLOW);
        let acc = OverflowPageAccessor::new(buf.memory());
        assert_eq!(acc.page_id(), 42);
        assert_eq!(acc.next_page_id(), Some(43));
        assert_eq!(acc.chunk_len() as usize, chunk.len());
        assert_eq!(acc.payload(), &chunk[..]);
    }

    #[test]
    fn accessor_reads_payload_without_zero_tail() {
        // payload() must return exactly chunk_len bytes, not the zeroed remainder.
        let mut buf = PageBuffer::new();
        let chunk = [0xFFu8; 10];
        {
            let mut b = OverflowPageBuilder::new(1, buf.memory_mut());
            b.set_chunk(&chunk).unwrap();
            b.finish();
        }
        let acc = OverflowPageAccessor::new(buf.memory());
        assert_eq!(acc.payload().len(), 10);
        assert!(acc.payload().iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn next_page_id_none_when_zero() {
        // A terminal page (next = 0) reports None, not Some(0).
        let mut buf = PageBuffer::new();
        {
            let mut b = OverflowPageBuilder::new(1, buf.memory_mut());
            b.set_next_page_id(None);
            b.set_chunk(&[1, 2, 3]).unwrap();
            b.finish();
        }
        assert_eq!(OverflowPageAccessor::new(buf.memory()).next_page_id(), None);
    }

    // ── Chunk capacity bounds ─────────────────────────────────────────────────

    #[test]
    fn set_chunk_at_exact_capacity_ok() {
        let mut buf = PageBuffer::new();
        let chunk = vec![0xABu8; OVERFLOW_PAYLOAD_SIZE];
        let mut b = OverflowPageBuilder::new(1, buf.memory_mut());
        assert!(b.set_chunk(&chunk).is_ok());
        // Header + full payload exactly fills the page.
        assert_eq!(OVERFLOW_HEADER_SIZE + OVERFLOW_PAYLOAD_SIZE, PAGE_SIZE);
    }

    #[test]
    fn set_chunk_rejects_oversized() {
        let mut buf = PageBuffer::new();
        let chunk = vec![0u8; OVERFLOW_PAYLOAD_SIZE + 1];
        let mut b = OverflowPageBuilder::new(1, buf.memory_mut());
        match b.set_chunk(&chunk) {
            Err(PageError::InsufficientSpace { needed, available }) => {
                assert_eq!(needed, OVERFLOW_PAYLOAD_SIZE + 1);
                assert_eq!(available, OVERFLOW_PAYLOAD_SIZE);
            }
            other => panic!("expected InsufficientSpace, got {:?}", other),
        }
    }

    // ── Mutator ───────────────────────────────────────────────────────────────

    #[test]
    fn mutator_set_lsn_and_next_round_trip() {
        let mut buf = PageBuffer::new();
        {
            let mut b = OverflowPageBuilder::new(7, buf.memory_mut());
            b.set_next_page_id(None);
            b.set_chunk(&[9, 9, 9]).unwrap();
            b.finish();
        }
        {
            let mut m = OverflowPageMutator::new(buf.memory_mut());
            m.set_lsn(12345);
            m.set_next_page_id(Some(8));
            // as_accessor sees the in-progress mutations under the same borrow.
            let acc = m.as_accessor();
            assert_eq!(acc.lsn(), 12345);
            assert_eq!(acc.next_page_id(), Some(8));
        }
        let acc = OverflowPageAccessor::new(buf.memory());
        assert_eq!(acc.lsn(), 12345);
        assert_eq!(acc.next_page_id(), Some(8));
    }

    #[test]
    fn mutator_set_chunk_overwrites_payload() {
        let mut buf = PageBuffer::new();
        {
            let mut b = OverflowPageBuilder::new(1, buf.memory_mut());
            b.set_chunk(&[1, 1, 1, 1]).unwrap();
            b.finish();
        }
        OverflowPageMutator::new(buf.memory_mut())
            .set_chunk(&[2, 2])
            .unwrap();
        let acc = OverflowPageAccessor::new(buf.memory());
        assert_eq!(acc.chunk_len(), 2);
        assert_eq!(acc.payload(), &[2, 2]);
    }
}
