//! # Write-Ahead Log (WAL)
//!
//! The WAL is a crucial component for ensuring Database durability and atomicity.
//! It records all changes to the database before they are applied to the data files.
//! This implementation provides:
//! -   **Durability**: Changes are flushed to disk before completion.
//! -   **Checksumming**: Every record is protected by a CRC32 checksum to detect corruption.
//! -   **Sequential I/O**: Optimized for append-only writes.
//! -   **Transaction Support**: Commit and Abort markers for multi-operation transactions.
//!
//! ## Record Layout
//!
//! | Field      | Size (bytes) | Description                              |
//! |------------|--------------|------------------------------------------|
//! | LSN        | 8            | Log Sequence Number (Little Endian)      |
//! | Type       | 1            | Entry type (0: Put, 1: Delete, 2: Commit, 3: Abort) |
//! | Key Len    | 8            | Length of the key                        |
//! | Value Len  | 8            | Length of the value (0 if None)          |
//! | Txn ID     | 8            | Transaction ID                           |
//! | Page ID    | 8            | Page ID                                  |
//! | Key        | variable     | The actual key bytes                     |
//! | Value      | variable     | The actual value bytes (optional)        |
//! | Checksum   | 4            | CRC32 of all preceding fields            |

use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::vec;

use crate::disk::DiskManager;
use crate::page::{Lsn, PageId};
use common::WalError;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryType {
    Put = 0,
    Delete = 1,
    Commit = 2,
    Abort = 3,
}

impl TryFrom<u8> for WalEntryType {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(WalEntryType::Put),
            1 => Ok(WalEntryType::Delete),
            2 => Ok(WalEntryType::Commit),
            3 => Ok(WalEntryType::Abort),
            _ => Err(WalError::InvalidEntryType(value)),
        }
    }
}

#[derive(Debug)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub entry_type: WalEntryType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub txn_id: u64,
    pub page_id: PageId,
}

// Layout of a WAL record
// | lsn (8) | entry_type (1) | key_len (8) | value_len (8) | txn_id (8) | page_id (8) | key_bytes | value_bytes | checksum (4) |

pub struct WalIterator {
    reader: BufReader<File>,
}

pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
    lsn: AtomicU64,
    scratch_pad: Vec<u8>,
}

impl Iterator for WalIterator {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut lsn_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut lsn_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(e.into()));
        }
        let lsn = Lsn::from_le_bytes(lsn_buf);

        let mut type_buf = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut type_buf) {
            return Some(Err(e.into()));
        }
        let entry_type = match WalEntryType::try_from(type_buf[0]) {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };

        let mut key_len_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut key_len_buf) {
            return Some(Err(e.into()));
        }
        let key_len = u64::from_le_bytes(key_len_buf);

        let mut value_len_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut value_len_buf) {
            return Some(Err(e.into()));
        }
        let value_len = u64::from_le_bytes(value_len_buf);

        let mut txn_id_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut txn_id_buf) {
            return Some(Err(e.into()));
        }
        let txn_id = u64::from_le_bytes(txn_id_buf);

        let mut page_id_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut page_id_buf) {
            return Some(Err(e.into()));
        }
        let page_id = PageId::from_le_bytes(page_id_buf);

        let mut key = vec![0u8; key_len as usize];
        if let Err(e) = self.reader.read_exact(&mut key) {
            return Some(Err(e.into()));
        }

        let value = if value_len > 0 {
            let mut val_buf = vec![0u8; value_len as usize];
            if let Err(e) = self.reader.read_exact(&mut val_buf) {
                return Some(Err(e.into()));
            }
            Some(val_buf)
        } else {
            None
        };

        let mut checksum_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut checksum_buf) {
            return Some(Err(e.into()));
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let mut hasher = Hasher::new();
        hasher.update(&lsn_buf);
        hasher.update(&type_buf);
        hasher.update(&key_len_buf);
        hasher.update(&value_len_buf);
        hasher.update(&txn_id_buf);
        hasher.update(&page_id_buf);
        hasher.update(&key);
        if let Some(ref v) = value {
            hasher.update(v);
        }
        let actual_checksum = hasher.finalize();

        if actual_checksum != expected_checksum {
            return Some(Err(WalError::ChecksumMismatch {
                lsn,
                expected: expected_checksum,
                actual: actual_checksum,
            }));
        }

        Some(Ok(WalEntry {
            lsn,
            entry_type,
            key,
            value,
            txn_id,
            page_id,
        }))
    }
}

impl WalIterator {
    pub fn new(path: &Path) -> io::Result<WalIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
        })
    }
}

impl Wal {
    pub fn new(path: &Path) -> Result<Self> {
        // Scan the existing WAL, if any, to find the highest lsn so that the
        // atomic counter starts over correctly after a restart.
        let initial_lsn = if path.exists() {
            match WalIterator::new(path) {
                Ok(iter) => {
                    let max_lsn = iter.map_while(Result::ok).map(|e| e.lsn).max().unwrap_or(0);
                    max_lsn + 1
                }
                // File exists but cannot be opened for reading; abort to avoid duplicating LSNs.
                Err(e) => return Err(WalError::Io(e)),
            }
        } else {
            0
        };

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            file: BufWriter::new(file),
            lsn: AtomicU64::new(initial_lsn),
            scratch_pad: Vec::with_capacity(4096),
        })
    }

    pub fn append(
        &mut self,
        txn_id: u64,
        page_id: PageId,
        entry_type: WalEntryType,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<Lsn> {
        // Atomically claim the next LSN before doing any I/O.
        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst);

        let key_len = key.len() as u64;
        let value_bytes = match entry_type {
            WalEntryType::Put => {
                value.ok_or_else(|| WalError::CorruptedLog("Put entry missing value".into()))?
            }
            WalEntryType::Delete | WalEntryType::Commit | WalEntryType::Abort => &[],
        };
        let value_len = value_bytes.len() as u64;

        self.scratch_pad.clear();

        // Reserve space: 8 (LSN) + 1 (type) + 8 (key_len) + 8 (value_len)
        //              + 8 (txn_id) + 8 (page_id) + key + value
        let record_size = 8 + 1 + 8 + 8 + 8 + 8 + key.len() + value_bytes.len();
        self.scratch_pad.reserve(record_size);

        self.scratch_pad.extend_from_slice(&lsn.to_le_bytes());
        self.scratch_pad.push(entry_type as u8);
        self.scratch_pad.extend_from_slice(&key_len.to_le_bytes());
        self.scratch_pad.extend_from_slice(&value_len.to_le_bytes());
        self.scratch_pad.extend_from_slice(&txn_id.to_le_bytes());
        self.scratch_pad.extend_from_slice(&page_id.to_le_bytes());
        self.scratch_pad.extend_from_slice(key);
        self.scratch_pad.extend_from_slice(value_bytes);

        let mut hasher = Hasher::new();
        hasher.update(&self.scratch_pad);
        let checksum = hasher.finalize();

        self.file.write_all(&self.scratch_pad)?;
        self.file.write_all(&checksum.to_le_bytes())?;

        Ok(lsn)
    }


    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        DiskManager::sync_file_and_dir(self.file.get_ref(), &self.path);
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

        let lsn1 = wal.append(10, 1, WalEntryType::Put, b"key1", Some(b"value1"))?;
        let lsn2 = wal.append(10, 1, WalEntryType::Put, b"key2", Some(b"value2"))?;
        let lsn3 = wal.append(10, 1, WalEntryType::Delete, b"key1", None)?;
        let lsn4 = wal.append(10, PageId::MAX, WalEntryType::Commit, b"", None)?;
        wal.flush()?;

        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;

        let entry1 = iter.next().unwrap()?;
        assert_eq!(entry1.lsn, lsn1);
        assert_eq!(entry1.entry_type, WalEntryType::Put);
        assert_eq!(entry1.key, b"key1");
        assert_eq!(entry1.value, Some(b"value1".to_vec()));
        assert_eq!(entry1.txn_id, 10);
        assert_eq!(entry1.page_id, 1);

        let entry2 = iter.next().unwrap()?;
        assert_eq!(entry2.lsn, lsn2);
        assert_eq!(entry2.entry_type, WalEntryType::Put);
        assert_eq!(entry2.key, b"key2");
        assert_eq!(entry2.value, Some(b"value2".to_vec()));

        let entry3 = iter.next().unwrap()?;
        assert_eq!(entry3.lsn, lsn3);
        assert_eq!(entry3.entry_type, WalEntryType::Delete);
        assert_eq!(entry3.key, b"key1");
        assert_eq!(entry3.value, None);

        let entry4 = iter.next().unwrap()?;
        assert_eq!(entry4.lsn, lsn4);
        assert_eq!(entry4.entry_type, WalEntryType::Commit);

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn test_wal_lsn_recovery() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("recovery.wal");

        // Write some entries and close
        {
            let mut wal = Wal::new(&wal_path)?;
            wal.append(1, 0, WalEntryType::Put, b"k1", Some(b"v1"))?;
            wal.append(1, 0, WalEntryType::Put, b"k2", Some(b"v2"))?;
            wal.flush()?;
        }

        // Re-open: LSN counter must resume from max_lsn + 1
        let mut wal = Wal::new(&wal_path)?;
        let next_lsn = wal.append(2, PageId::MAX, WalEntryType::Commit, b"", None)?;
        assert_eq!(next_lsn, 2, "resumed lsn should be 2 (after 0 and 1)");

        Ok(())
    }

    #[test]
    fn test_wal_abort_entry() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("abort.wal");

        let mut wal = Wal::new(&wal_path)?;
        wal.append(99, 5, WalEntryType::Put, b"key", Some(b"val"))?;
        let abort_lsn = wal.append(99, PageId::MAX, WalEntryType::Abort, b"", None)?;
        wal.flush()?;

        let entries: Vec<_> = WalIterator::new(&wal_path)
            .map_err(|e| WalError::Io(e))?
            .collect::<Result<_>>()?;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].lsn, abort_lsn);
        assert_eq!(entries[1].entry_type, WalEntryType::Abort);
        assert_eq!(entries[1].txn_id, 99);

        Ok(())
    }

    #[test]
    fn test_wal_corruption() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("corrupt.wal");

        let mut wal = Wal::new(&wal_path)?;
        wal.append(1, 0, WalEntryType::Put, b"key1", Some(b"value1"))?;
        wal.flush()?;

        // Intentionally corrupt one byte inside the record body
        let mut file = OpenOptions::new().write(true).open(&wal_path)?;
        // Offset 25 falls inside the txn_id field — safe to corrupt without
        // altering any length fields that would cause an UnexpectedEof
        file.seek(SeekFrom::Start(25))?;
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
