//! # Write-Ahead Log (WAL)
//!
//! The WAL is a crucial component for ensuring Database durability and atomicity.
//! It records all changes to the database before they are applied to the data files.
//! This implementation provides:
//! -   **Logical LSNs**: Each append receives a monotonically increasing
//!     logical counter, not a byte offset.
//! -   **Durability tracking**: `flushed_lsn` records the highest LSN known to
//!     be durable; `flush_up_to(lsn)` fsyncs only when needed.
//! -   **Checksumming**: Every record is protected by a CRC32 checksum to detect corruption.
//! -   **Sequential I/O**: Optimized for append-only writes.
//!
//! ## Record Layout (On-Disk Format)
//!
//! | Field         | Size (bytes) | Description                                  |
//! |---------------|--------------|----------------------------------------------|
//! | LSN           | 8            | Log Sequence Number (Little Endian)          |
//! | Record Len    | 4            | Total length of the record                   |
//! | Type          | 1            | WalRecordType (e.g. Insert, Commit, etc.)    |
//! | Num Blocks    | 1            | Number of block references                   |
//! | Txn ID        | 8            | Transaction ID                               |
//! | Main Data Len | 2            | Length of the main data payload              |
//! | Blocks        | variable     | Array of block references and their payloads |
//! | Main Data     | variable     | The main data payload bytes (optional)       |
//! | Checksum      | 4            | CRC32 of all preceding fields                |
//!
//! ### Block Reference Layout
//!
//! Each block in the `Blocks` array is structured on-disk as follows:
//!
//! | Field         | Size (bytes) | Description                                  |
//! |---------------|--------------|----------------------------------------------|
//! | Page ID       | 8            | ID of the modified page                      |
//! | Block Flags   | 1            | Bit flags (e.g., bit 0 indicates FPI presence)|
//! | Data Len      | 2            | Length of the block-specific redo payload    |
//! | FPI           | 4096 (opt)   | Full-Page Image, if indicated by Block Flags |
//! | Data          | variable     | Block-specific redo payload bytes            |
//!

use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::disk::DiskManager;
use crate::page::{Lsn, PAGE_SIZE, PageId};
use common::WalError;

pub type Result<T> = std::result::Result<T, WalError>;

/// Identifies the physiological operation that a WAL record represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    Insert = 0,
    SetXMax = 1,
    Commit = 2,
    Abort = 3,
    LeafSplit = 4,
    InternalSPlit = 5,
    InsertDownLink = 6,
    NewRoot = 7,
    PageCompact = 8,
    MarkHalfDead = 9,
    UnlinkPage = 10,
    Checkpoint = 11,
}

impl TryFrom<u8> for WalRecordType {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(WalRecordType::Insert),
            1 => Ok(WalRecordType::SetXMax),
            2 => Ok(WalRecordType::Commit),
            3 => Ok(WalRecordType::Abort),
            4 => Ok(WalRecordType::LeafSplit),
            5 => Ok(WalRecordType::InternalSPlit),
            6 => Ok(WalRecordType::InsertDownLink),
            7 => Ok(WalRecordType::NewRoot),
            8 => Ok(WalRecordType::PageCompact),
            9 => Ok(WalRecordType::MarkHalfDead),
            10 => Ok(WalRecordType::UnlinkPage),
            11 => Ok(WalRecordType::Checkpoint),
            _ => Err(WalError::InvalidEntryType(value)),
        }
    }
}

/// Block flag bits (`blk_flags`). Bit 0 marks a full-page image; bit 1 marks a
/// physiological redo payload. Reader keys FPI off bit 0 and data off `data_len`.
pub const BLK_HAS_FPI: u8 = 0b01;
pub const BLK_HAS_DATA: u8 = 0b10;

/// Represents a reference to a page modified by the transaction, potentially
/// including a Full-Page Image (FPI) and specific redo data for that page.
#[derive(Debug)]
pub struct Block<'a> {
    pub page_id: PageId,
    pub blk_flags: u8,
    pub fpi: Option<[u8; PAGE_SIZE]>,
    pub data: Option<&'a [u8]>,
}

/// A fully parsed Write-Ahead Log record representing a single logged operation.
#[derive(Debug)]
pub struct WalRecord<'a> {
    pub lsn: Lsn,
    pub rec_len: u32,
    pub entry_type: WalRecordType,
    pub txn_id: u64,
    pub blocks: Vec<Block<'a>>,
    pub main_data: Option<&'a [u8]>,
}

/// An iterator that sequentially reads and validates records from a WAL file.
pub struct WalIterator {
    reader: BufReader<File>,
    scratch: Vec<u8>,
}

/// The main Write-Ahead Log manager responsible for appending records sequentially
/// and maintaining the durability guarantees of the database.
pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
    scratch_pad: Vec<u8>,
    pub next_lsn: u64,
    pub flushed_lsn: Option<Lsn>,
}

impl WalIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<WalIterator> {
        let path = path.as_ref();
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
            scratch: Vec::with_capacity(4096),
        })
    }

    pub fn next_record(&mut self) -> Option<Result<WalRecord<'_>>> {
        match self.reader.fill_buf() {
            Ok([]) => return None,
            Ok(_) => {}
            Err(e) => return Some(Err(e.into())),
        }

        self.scratch.clear();

        let mut hasher = Hasher::new();

        let mut lsn_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut lsn_buf) {
            return Some(Err(e.into()));
        }
        let lsn = Lsn::from_le_bytes(lsn_buf);
        hasher.update(&lsn_buf);

        let mut rec_len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut rec_len_buf) {
            return Some(Err(e.into()));
        }
        let rec_len = u32::from_le_bytes(rec_len_buf);
        hasher.update(&rec_len_buf);

        let mut type_buf = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut type_buf) {
            return Some(Err(e.into()));
        }
        let entry_type = match WalRecordType::try_from(type_buf[0]) {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };
        hasher.update(&type_buf);

        let mut nblocks_buf = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut nblocks_buf) {
            return Some(Err(e.into()));
        }
        let nblocks = nblocks_buf[0];
        hasher.update(&nblocks_buf);

        let mut txn_id_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut txn_id_buf) {
            return Some(Err(e.into()));
        }
        let txn_id = u64::from_le_bytes(txn_id_buf);
        hasher.update(&txn_id_buf);

        let mut main_len_buf = [0u8; 2];
        if let Err(e) = self.reader.read_exact(&mut main_len_buf) {
            return Some(Err(e.into()));
        }
        let main_len = u16::from_le_bytes(main_len_buf);
        hasher.update(&main_len_buf);

        let mut temp_blocks = Vec::with_capacity(nblocks as usize);

        for _ in 0..nblocks {
            let mut page_id_buf = [0u8; 8];
            if let Err(e) = self.reader.read_exact(&mut page_id_buf) {
                return Some(Err(e.into()));
            }
            let page_id = PageId::from_le_bytes(page_id_buf);
            hasher.update(&page_id_buf);

            let mut blk_flags_buf = [0u8; 1];
            if let Err(e) = self.reader.read_exact(&mut blk_flags_buf) {
                return Some(Err(e.into()));
            }
            let blk_flags = blk_flags_buf[0];
            hasher.update(&blk_flags_buf);

            let mut data_len_buf = [0u8; 2];
            if let Err(e) = self.reader.read_exact(&mut data_len_buf) {
                return Some(Err(e.into()));
            }
            let data_len = u16::from_le_bytes(data_len_buf);
            hasher.update(&data_len_buf);

            let fpi = if blk_flags & 1 == 1 {
                let mut fpi_buf = [0u8; PAGE_SIZE];
                if let Err(e) = self.reader.read_exact(&mut fpi_buf) {
                    return Some(Err(e.into()));
                }
                hasher.update(&fpi_buf);
                Some(fpi_buf)
            } else {
                None
            };

            let data_range = if data_len > 0 {
                let start = self.scratch.len();
                let end = start + data_len as usize;
                self.scratch.resize(end, 0);
                if let Err(e) = self.reader.read_exact(&mut self.scratch[start..end]) {
                    return Some(Err(e.into()));
                }
                hasher.update(&self.scratch[start..end]);
                Some((start, end))
            } else {
                None
            };

            temp_blocks.push((page_id, blk_flags, fpi, data_range));
        }

        let main_data_range = if main_len > 0 {
            let start = self.scratch.len();
            let end = start + main_len as usize;
            self.scratch.resize(end, 0);
            if let Err(e) = self.reader.read_exact(&mut self.scratch[start..end]) {
                return Some(Err(e.into()));
            }
            hasher.update(&self.scratch[start..end]);
            Some((start, end))
        } else {
            None
        };

        let mut checksum_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut checksum_buf) {
            return Some(Err(e.into()));
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let actual_checksum = hasher.finalize();

        if actual_checksum != expected_checksum {
            return Some(Err(WalError::ChecksumMismatch {
                lsn,
                expected: expected_checksum,
                actual: actual_checksum,
            }));
        }

        let blocks = temp_blocks
            .into_iter()
            .map(|(page_id, blk_flags, fpi, data_range)| {
                let data = data_range.map(|(s, e)| &self.scratch[s..e]);
                Block {
                    page_id,
                    blk_flags,
                    fpi,
                    data,
                }
            })
            .collect();

        let main_data = main_data_range.map(|(s, e)| &self.scratch[s..e]);

        Some(Ok(WalRecord {
            lsn,
            rec_len,
            entry_type,
            txn_id,
            blocks,
            main_data,
        }))
    }
}

impl Wal {
    /// Opens an existing WAL file for appending, or creates a new one if it does not exist.
    ///
    /// Upon opening an existing file, this method scans the entire log to find the maximum
    /// LSN and handles any torn-tail corruption by truncating the file to the last valid
    /// record boundary. If mid-log corruption is detected, an error is returned.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let mut next_lsn = 0;
        let mut flushed_lsn = None;

        match OpenOptions::new().read(true).open(path) {
            Ok(file) => {
                let file_len = file.metadata().map_err(WalError::Io)?.len();
                let mut iter = WalIterator {
                    reader: BufReader::new(file),
                    scratch: Vec::with_capacity(4096),
                };

                loop {
                    use std::io::Seek;
                    let current_offset = iter.reader.stream_position().map_err(WalError::Io)?;

                    match iter.next_record() {
                        Some(Ok(record)) => {
                            if record.lsn >= next_lsn {
                                next_lsn = record.lsn + 1;
                            }
                            flushed_lsn = Some(record.lsn);
                        }
                        Some(Err(e)) => {
                            let err_pos = iter.reader.stream_position().map_err(WalError::Io)?;

                            let is_eof = match &e {
                                WalError::Io(io_err) => {
                                    io_err.kind() == io::ErrorKind::UnexpectedEof
                                }
                                _ => false,
                            };

                            // If the file is corrupted at the end, truncate it
                            if is_eof || err_pos == file_len {
                                let f = OpenOptions::new()
                                    .write(true)
                                    .open(path)
                                    .map_err(WalError::Io)?;
                                // chops off the corrupted part
                                f.set_len(current_offset).map_err(WalError::Io)?;
                                f.sync_all().map_err(WalError::Io)?;
                                break;
                            } else {
                                return Err(e);
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(WalError::Io)?;

        Ok(Wal {
            path: path.to_path_buf(),
            file: BufWriter::new(file),
            scratch_pad: Vec::with_capacity(4096),
            next_lsn,
            flushed_lsn,
        })
    }
    pub fn log_commit(&mut self, txn_id: u64) -> Result<Lsn> {
        self.append(WalRecordType::Commit, txn_id, &[], None)
    }

    pub fn log_abort(&mut self, txn_id: u64) -> Result<Lsn> {
        self.append(WalRecordType::Abort, txn_id, &[], None)
    }

    /// Appends only — durability is deferred to the buffer pool's flush seam
    /// (WAL-before-page) or to the transaction's commit, never an fsync here.
    pub fn log_insert(
        &mut self,
        txn_id: u64,
        page_id: PageId,
        slot: u16,
        key: &[u8],
        value: &[u8],
        xmin: u64,
    ) -> Result<Lsn> {
        let mut payload = Vec::with_capacity(2 + 2 + 2 + 8 + key.len() + value.len());
        payload.extend_from_slice(&slot.to_le_bytes());
        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
        payload.extend_from_slice(&(value.len() as u16).to_le_bytes());
        payload.extend_from_slice(&xmin.to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);

        let block = Block {
            page_id,
            blk_flags: BLK_HAS_DATA,
            fpi: None,
            data: Some(&payload),
        };
        self.append(WalRecordType::Insert, txn_id, &[block], None)
    }

    /// Appends a new physiological record to the WAL buffer.
    ///
    /// This method assigns the next available LSN, serializes the record according to the
    /// internal wire format, calculates its CRC32 checksum, and writes it to the internal
    /// `BufWriter`. Note that the record is not guaranteed to be durable on disk until
    /// `flush_up_to` is called.
    fn append(
        &mut self,
        entry_type: WalRecordType,
        txn_id: u64,
        blocks: &[Block<'_>],
        main_data: Option<&[u8]>,
    ) -> Result<Lsn> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        self.scratch_pad.clear();

        let blocks_size = blocks
            .iter()
            .map(|block| {
                let mut size = 8 + 1 + 2;
                if block.fpi.is_some() {
                    size += PAGE_SIZE;
                }
                if let Some(data) = block.data {
                    size += data.len();
                }
                size
            })
            .sum::<usize>();

        let main_data_size = main_data.as_ref().map_or(0, |data| data.len());

        let record_size = 8 + 4 + 1 + 1 + 8 + 2 + blocks_size + main_data_size + 4;
        self.scratch_pad.reserve(record_size);

        self.scratch_pad.extend_from_slice(&lsn.to_le_bytes());
        self.scratch_pad
            .extend_from_slice(&(record_size as u32).to_le_bytes());
        self.scratch_pad.push(entry_type as u8);
        self.scratch_pad.push(blocks.len() as u8);
        self.scratch_pad.extend_from_slice(&txn_id.to_le_bytes());

        let main_len = main_data.map_or(0, |data| data.len()) as u16;
        self.scratch_pad.extend_from_slice(&main_len.to_le_bytes());

        for block in blocks {
            self.scratch_pad
                .extend_from_slice(&block.page_id.to_le_bytes());
            self.scratch_pad.push(block.blk_flags);

            let data_len = block.data.map_or(0, |d| d.len()) as u16;
            self.scratch_pad.extend_from_slice(&data_len.to_le_bytes());

            if let Some(ref fpi) = block.fpi {
                self.scratch_pad.extend_from_slice(fpi);
            }
            if let Some(data) = block.data {
                self.scratch_pad.extend_from_slice(data);
            }
        }
        if let Some(data) = main_data {
            self.scratch_pad.extend_from_slice(data);
        }

        let mut hasher = Hasher::new();
        hasher.update(&self.scratch_pad);
        let checksum = hasher.finalize();

        self.file.write_all(&self.scratch_pad)?;
        self.file.write_all(&checksum.to_le_bytes())?;

        Ok(lsn)
    }

    /// Flushes pending WAL bytes so records up to and including `lsn` are durable.
    ///
    /// This is a no-op when `flushed_lsn >= lsn`. Otherwise it drains the `BufWriter`,
    /// fsyncs the WAL file and parent directory, then advances `flushed_lsn` to `lsn`.
    /// Commit code relies on this before publishing a transaction as committed.
    pub fn flush_up_to(&mut self, lsn: Lsn) -> Result<()> {
        let needs_flush = match self.flushed_lsn {
            Some(flushed) => lsn > flushed,
            None => true,
        };

        if needs_flush {
            self.file.flush().map_err(WalError::Io)?;
            DiskManager::sync_file_and_dir(self.file.get_ref(), &self.path)?;
            self.flushed_lsn = Some(lsn);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    #[test]
    fn test_wal_roundtrip() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("test.wal");

        let mut wal = Wal::new(&wal_path)?;

        let block1 = Block {
            page_id: 100,
            blk_flags: 2,
            fpi: None,
            data: Some(&[1, 2, 3, 4]),
        };

        wal.append(WalRecordType::Insert, 42, &[block1], None)?;

        let block2 = Block {
            page_id: 101,
            blk_flags: 1,
            fpi: Some([0u8; PAGE_SIZE]),
            data: None,
        };

        wal.append(
            WalRecordType::Commit,
            43,
            &[block2],
            Some(&[8, 7, 6, 5, 4, 3, 2, 1]),
        )?;

        wal.flush_up_to(2)?;

        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;

        {
            let entry1 = iter.next_record().unwrap()?;
            assert_eq!(entry1.lsn, 0);
            assert_eq!(entry1.entry_type, WalRecordType::Insert);
            assert_eq!(entry1.txn_id, 42);
            assert_eq!(entry1.blocks.len(), 1);
            assert_eq!(entry1.blocks[0].page_id, 100);
            assert_eq!(entry1.blocks[0].data.unwrap(), &[1, 2, 3, 4]);
        }

        {
            let entry2 = iter.next_record().unwrap()?;
            assert_eq!(entry2.lsn, 1);
            assert_eq!(entry2.entry_type, WalRecordType::Commit);
            assert_eq!(entry2.txn_id, 43);
            assert_eq!(entry2.blocks.len(), 1);
            assert_eq!(entry2.blocks[0].page_id, 101);
            assert!(entry2.blocks[0].fpi.is_some());
            assert_eq!(entry2.main_data.unwrap(), &[8, 7, 6, 5, 4, 3, 2, 1]);
        }

        assert!(iter.next_record().is_none());

        Ok(())
    }

    #[test]
    fn test_wal_recovery_clean() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("clean.wal");

        {
            let mut wal = Wal::new(&wal_path)?;
            let block1 = Block {
                page_id: 100,
                blk_flags: 2,
                fpi: None,
                data: Some(&[1, 2, 3, 4]),
            };
            wal.append(WalRecordType::Insert, 42, &[block1], None)?;
            let block2 = Block {
                page_id: 101,
                blk_flags: 0,
                fpi: None,
                data: None,
            };
            wal.append(WalRecordType::Commit, 43, &[block2], None)?;
            wal.flush_up_to(1)?;
        }

        let wal = Wal::new(&wal_path)?;
        assert_eq!(wal.next_lsn, 2);
        Ok(())
    }

    #[test]
    fn test_flush_tracks_requested_lsn_and_noops_when_durable() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_path = dir.path().join("flush_lsn.wal");

        let mut wal = Wal::new(&wal_path)?;
        let first = wal.append(WalRecordType::Insert, 42, &[], None)?;
        let second = wal.append(WalRecordType::Commit, 42, &[], None)?;

        assert_eq!(first, 0);
        assert_eq!(second, 1);
        assert_eq!(wal.flushed_lsn, None);

        wal.flush_up_to(first)?;
        assert_eq!(wal.flushed_lsn, Some(first));

        wal.flush_up_to(first)?;
        assert_eq!(wal.flushed_lsn, Some(first));

        wal.flush_up_to(second)?;
        assert_eq!(wal.flushed_lsn, Some(second));

        Ok(())
    }

    #[test]
    fn test_wal_recovery_torn_tail() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("torntail.wal");

        {
            let mut wal = Wal::new(&wal_path)?;
            let block1 = Block {
                page_id: 100,
                blk_flags: 2,
                fpi: None,
                data: Some(&[1, 2, 3, 4]),
            };
            wal.append(WalRecordType::Insert, 42, &[block1], None)?;
            let block2 = Block {
                page_id: 101,
                blk_flags: 0,
                fpi: None,
                data: None,
            };
            wal.append(WalRecordType::Commit, 43, &[block2], None)?;
            wal.flush_up_to(1)?;
        }

        let mut file = OpenOptions::new().write(true).open(&wal_path)?;
        let file_len = file.metadata()?.len();

        file.seek(SeekFrom::Start(file_len - 1))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let wal = Wal::new(&wal_path)?;
        assert_eq!(wal.next_lsn, 1);

        let new_file_len = file.metadata()?.len();
        assert!(new_file_len < file_len);

        Ok(())
    }

    #[test]
    fn test_wal_mid_log_corruption() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("midlog.wal");

        {
            let mut wal = Wal::new(&wal_path)?;
            let block1 = Block {
                page_id: 100,
                blk_flags: 2,
                fpi: None,
                data: Some(&[1, 2, 3, 4]),
            };
            wal.append(WalRecordType::Insert, 42, &[block1], None)?;
            let block2 = Block {
                page_id: 101,
                blk_flags: 0,
                fpi: None,
                data: None,
            };
            wal.append(WalRecordType::Commit, 43, &[block2], None)?;
            wal.flush_up_to(1)?;
        }

        let mut file = OpenOptions::new().write(true).open(&wal_path)?;

        file.seek(SeekFrom::Start(40))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let result = Wal::new(&wal_path);
        match result {
            Err(WalError::ChecksumMismatch { lsn, .. }) => assert_eq!(lsn, 0),
            Err(e) => panic!("Expected ChecksumMismatch error, got error: {:?}", e),
            Ok(_) => panic!("Expected ChecksumMismatch error, got Ok(_)"),
        }

        Ok(())
    }

    #[test]
    fn test_wal_torn_lsn() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_path = dir.path().join("torn_lsn.wal");

        {
            let mut wal = Wal::new(&wal_path)?;
            let block1 = Block {
                page_id: 100,
                blk_flags: 2,
                fpi: None,
                data: Some(&[1, 2, 3, 4]),
            };
            wal.append(WalRecordType::Insert, 42, &[block1], None)?;
            wal.flush_up_to(0)?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&wal_path)?;
        let clean_len = file.metadata()?.len();

        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
        file.sync_all()?;

        let _ = Wal::new(&wal_path)?;

        let new_file_len = std::fs::metadata(&wal_path).map_err(WalError::Io)?.len();
        assert_eq!(
            new_file_len, clean_len,
            "Garbage bytes were not truncated! Expected len {}, got {}",
            clean_len, new_file_len
        );

        Ok(())
    }
}
