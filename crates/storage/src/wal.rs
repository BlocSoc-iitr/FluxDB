//! # Write-Ahead Log (WAL)
//!
//! The WAL is a crucial component for ensuring Database durability and atomicity.
//! It records all changes to the database before they are applied to the data files.
//! This implementation provides:
//! -   **Durability**: Changes are flushed to disk before completion.
//! -   **Checksumming**: Every record is protected by a CRC32 checksum to detect corruption.
//! -   **Sequential I/O**: Optimized for append-only writes.
//!
//! ## Record Layout
//!
//! | Field      | Size (bytes) | Description                          |
//! |------------|--------------|--------------------------------------|
//! | LSN        | 8            | Log Sequence Number (Little Endian) |
//! | Type       | 1            | Entry type (0: Put, 1: Delete)        |
//! | Key Len    | 8            | Length of the key                    |
//! | Value Len  | 8            | Length of the value (0 if None)      |
//! | Timestamp  | 8            | Microseconds since Unix Epoch        |
//! | Key        | variable     | The actual key bytes                 |
//! | Value      | variable     | The actual value bytes (optional)    |
//! | Checksum   | 4            | CRC32 of all preceding fields        |

use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::vec;

use crate::disk::DiskManager;
use crate::page::{Lsn, PAGE_SIZE, PageId};
use common::WalError;

pub type Result<T> = std::result::Result<T, WalError>;

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
    PageAllocate = 8,
    PageCompact = 9,
    MarkHalfDead = 10,
    UnlinkPage = 11,
    Checkpoint = 12,
    Fpi = 13,
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
            8 => Ok(WalRecordType::PageAllocate),
            9 => Ok(WalRecordType::PageCompact),
            10 => Ok(WalRecordType::MarkHalfDead),
            11 => Ok(WalRecordType::UnlinkPage),
            12 => Ok(WalRecordType::Checkpoint),
            13 => Ok(WalRecordType::Fpi),
            _ => Err(WalError::InvalidEntryType(value)),
        }
    }
}

#[derive(Debug)]
pub struct Block {
    pub page_id: PageId,
    pub blk_flags: u8,
    pub data_len: u16,
    pub fpi: Option<[u8; PAGE_SIZE]>,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub rec_len: u32,
    pub entry_type: WalRecordType,
    pub nblocks: u8,
    pub txn_id: u64, 
    pub main_len: u16,
    pub blocks: Vec<Block>,
    pub main_data: Option<Vec<u8>>,
}

pub struct WalIterator {
    reader: BufReader<File>,
}

pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
    scratch_pad: Vec<u8>,
    pub next_lsn: u64,
}

impl Iterator for WalIterator {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut hasher = Hasher::new();

        let mut lsn_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut lsn_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(e.into()));
        }
        let lsn = Lsn::from_le_bytes(lsn_buf);

        hasher.update(&lsn_buf);

        let mut rec_len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut rec_len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(e.into()))
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

        let mut blocks: Vec<Block> = Vec::with_capacity(nblocks as usize);

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
                Some(fpi_buf)
            } else {
                None
            };

            if let Some(ref fpi_buf) = fpi {
                hasher.update(fpi_buf);
            }

            let data = if data_len > 0 {
                let mut data_buf = vec![0u8; data_len as usize];
                if let Err(e) = self.reader.read_exact(&mut data_buf) {
                    return Some(Err(e.into()));
                }
                Some(data_buf)
            } else {
                None
            };

            if let Some(ref data_buf) = data {
                hasher.update(data_buf);
            }

            blocks.push(Block {
                page_id,
                blk_flags,
                data_len,
                fpi,
                data,
            });
        }

        let main_data = if main_len > 0 {
            let mut main_data_buf = vec![0u8; main_len as usize];
            if let Err(e) = self.reader.read_exact(&mut main_data_buf) {
                return Some(Err(e.into()));
            }
            Some(main_data_buf)
        } else {
            None
        };

        if let Some(ref main_data_buf) = main_data {
            hasher.update(main_data_buf);
        }

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

        Some(Ok(WalRecord {
            lsn,
            rec_len,
            entry_type,
            nblocks,
            txn_id,
            main_len,
            blocks,
            main_data,
        }))
    }
}

impl WalIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<WalIterator> {
        let path = path.as_ref();
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
        })
    }
}

impl Wal {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        
        let mut next_lsn = 0;

        match OpenOptions::new().read(true).open(path) {
            Ok(file) => {
                let file_len = file.metadata().map_err(|e| WalError::Io(e))?.len();
                let mut iter = WalIterator { reader: BufReader::new(file) };

                loop {
                    use std::io::{Seek};
                    let current_offset = iter.reader.stream_position().map_err(|e| WalError::Io(e))?;
                    
                    match iter.next() {
                        Some(Ok(record)) => {
                            if record.lsn >= next_lsn {
                                next_lsn = record.lsn + 1;
                            }
                        }
                        Some(Err(e)) => {
                            let err_pos = iter.reader.stream_position().map_err(|e| WalError::Io(e))?;
                            
                            let is_eof = match &e {
                                WalError::Io(io_err) => io_err.kind() == io::ErrorKind::UnexpectedEof,
                                _ => false,
                            };

                            // If the file is corrupted at the end, truncate it
                            if is_eof || err_pos == file_len {
                                let f = OpenOptions::new().write(true).open(path).map_err(|e| WalError::Io(e))?;
                                // chops off the corrupted part
                                f.set_len(current_offset).map_err(|e| WalError::Io(e))?;
                                f.sync_all().map_err(|e| WalError::Io(e))?;
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

        let file = OpenOptions::new().create(true).append(true).open(path).map_err(|e| WalError::Io(e))?;
        
        Ok(Wal {
            path: path.to_path_buf(),
            file: BufWriter::new(file),
            scratch_pad: Vec::with_capacity(4096),
            next_lsn,
        })
    }

    pub fn append(
        &mut self,
        rec_len: u32,
        entry_type: WalRecordType,
        nblocks: u8,
        txn_id: u64,
        main_len: u16,
        blocks: Vec<Block>,
        main_data: Option<Vec<u8>>,
    ) -> Result<Lsn> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
    
        self.scratch_pad.clear();

        let blocks_size = blocks.iter().map(|block| {
            let mut size = 8 + 1 + 2;
            if block.fpi.is_some() {
                size += PAGE_SIZE;
            }
            if let Some(ref data) = block.data {
                size += data.len();
            }
            size
        }).sum::<usize>();
        
        let main_data_size = main_data.as_ref().map_or(0, |data| data.len());
        
        let record_size = 4 + 1 + 1 + 8 + 2 + blocks_size + main_data_size;
        self.scratch_pad.reserve(record_size);

        self.scratch_pad.extend_from_slice(&lsn.to_le_bytes());
        self.scratch_pad.extend_from_slice(&rec_len.to_le_bytes());
        self.scratch_pad.push(entry_type as u8);
        self.scratch_pad.push(nblocks);
        self.scratch_pad.extend_from_slice(&txn_id.to_le_bytes());
        self.scratch_pad.extend_from_slice(&main_len.to_le_bytes());

        for block in blocks {
            self.scratch_pad.extend_from_slice(&block.page_id.to_le_bytes());
            self.scratch_pad.push(block.blk_flags);
            self.scratch_pad.extend_from_slice(&block.data_len.to_le_bytes());
            if let Some(ref fpi) = block.fpi {
                self.scratch_pad.extend_from_slice(fpi);
            }
            if let Some(ref data) = block.data {
                self.scratch_pad.extend_from_slice(data);
            }
        }
        if let Some(ref data) = main_data {
            self.scratch_pad.extend_from_slice(data);
        }

        let mut hasher = Hasher::new();
        hasher.update(&self.scratch_pad);
        let checksum = hasher.finalize();

        self.file.write_all(&self.scratch_pad)?;
        self.file.write_all(&checksum.to_le_bytes())?;

        Ok(lsn)
    }

    pub fn flush_up_to(&mut self, lsn: Lsn) -> Result<()> {
        if self.next_lsn <= lsn {
            self.file.flush()?;
            DiskManager::sync_file_and_dir(self.file.get_ref(), &self.path)?;
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
            data_len: 4,
            fpi: None,
            data: Some(vec![1, 2, 3, 4]),
        };

        wal.append(
            32, 
            WalRecordType::Insert,
            1,
            42,
            0,
            vec![block1],
            None,
        )?;

        let block2 = Block {
            page_id: 101,
            blk_flags: 1,
            data_len: 0,
            fpi: Some([0u8; PAGE_SIZE]),
            data: None,
        };

        wal.append(
            32 + PAGE_SIZE as u32,
            WalRecordType::Commit,
            1,
            43,
            8,
            vec![block2],
            Some(vec![8, 7, 6, 5, 4, 3, 2, 1]),
        )?;

        wal.flush_up_to(2)?;

        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;

        let entry1 = iter.next().unwrap()?;
        assert_eq!(entry1.lsn, 0);
        assert_eq!(entry1.entry_type, WalRecordType::Insert);
        assert_eq!(entry1.txn_id, 42);
        assert_eq!(entry1.blocks.len(), 1);
        assert_eq!(entry1.blocks[0].page_id, 100);
        assert_eq!(entry1.blocks[0].data.as_ref().unwrap(), &vec![1, 2, 3, 4]);

        let entry2 = iter.next().unwrap()?;
        assert_eq!(entry2.lsn, 1);
        assert_eq!(entry2.entry_type, WalRecordType::Commit);
        assert_eq!(entry2.txn_id, 43);
        assert_eq!(entry2.blocks.len(), 1);
        assert_eq!(entry2.blocks[0].page_id, 101);
        assert!(entry2.blocks[0].fpi.is_some());
        assert_eq!(entry2.main_data.as_ref().unwrap(), &vec![8, 7, 6, 5, 4, 3, 2, 1]);

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn test_wal_corruption() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("corrupt.wal");

        let mut wal = Wal::new(&wal_path)?;
        let block1 = Block {
            page_id: 100,
            blk_flags: 2,
            data_len: 4,
            fpi: None,
            data: Some(vec![1, 2, 3, 4]),
        };
        wal.append(
            32,
            WalRecordType::Insert,
            1,
            42,
            0,
            vec![block1],
            None,
        )?;
        wal.flush_up_to(1)?;

        let mut file = OpenOptions::new().write(true).open(&wal_path)?;
        let file_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(file_len - 2))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;
        let result = iter.next().unwrap();

        match result {
            Err(WalError::ChecksumMismatch { lsn, .. }) => assert_eq!(lsn, 0),
            _ => panic!("Expected ChecksumMismatch error, got {:?}", result),
        }

        Ok(())
    }
}
