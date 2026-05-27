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
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec;

use crate::disk::DiskManager;
use crate::page::Lsn;
use common::WalError;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryType {
    Put = 0,
    Delete = 1,
}

impl TryFrom<u8> for WalEntryType {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(WalEntryType::Put),
            1 => Ok(WalEntryType::Delete),
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
    pub timestamp: u64,
}

// Layout of a WAL record
// | lsn (8) | entry_type (1) | key_len (8) | value_len (8) | timestamp (8) | key_bytes | value_bytes | checksum (4) |

pub struct WalIterator {
    reader: BufReader<File>,
}

pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
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

        let mut timestamp_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut timestamp_buf) {
            return Some(Err(e.into()));
        }
        let timestamp = u64::from_le_bytes(timestamp_buf);

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
        hasher.update(&timestamp_buf);
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
            timestamp,
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
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            file: BufWriter::new(file),
            scratch_pad: Vec::with_capacity(4096),
        })
    }

    pub fn append(
        &mut self,
        lsn: Lsn,
        entry_type: WalEntryType,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<Lsn> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| WalError::CorruptedLog(e.to_string()))?
            .as_micros() as u64;

        let key_len = key.len() as u64;
        let value_bytes = match entry_type {
            WalEntryType::Put => {
                value.ok_or_else(|| WalError::CorruptedLog("Put entry missing value".into()))?
            }
            WalEntryType::Delete => &[],
        };
        let value_len = value_bytes.len() as u64;

        self.scratch_pad.clear();

        // Reserve space: 8 (LSN) + 1 (type) + 8 (key_len) + 8 (value_len) + 8 (timestamp) + key + value
        let record_size = 8 + 1 + 8 + 8 + 8 + key.len() + value_bytes.len();
        self.scratch_pad.reserve(record_size);

        self.scratch_pad.extend_from_slice(&lsn.to_le_bytes());
        self.scratch_pad.push(entry_type as u8);
        self.scratch_pad.extend_from_slice(&key_len.to_le_bytes());
        self.scratch_pad.extend_from_slice(&value_len.to_le_bytes());
        self.scratch_pad.extend_from_slice(&timestamp.to_le_bytes());
        self.scratch_pad.extend_from_slice(key);
        self.scratch_pad.extend_from_slice(value_bytes);

        let mut hasher = Hasher::new();
        hasher.update(&self.scratch_pad);
        let checksum = hasher.finalize();

        self.file.write_all(&self.scratch_pad)?;
        self.file.write_all(&checksum.to_le_bytes())?;

        Ok(lsn + 1)
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

        wal.append(1, WalEntryType::Put, b"key1", Some(b"value1"))?;
        wal.append(2, WalEntryType::Put, b"key2", Some(b"value2"))?;
        wal.append(3, WalEntryType::Delete, b"key1", None)?;
        wal.flush()?;
        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;

        let entry1 = iter.next().unwrap()?;
        assert_eq!(entry1.lsn, 1);
        assert_eq!(entry1.entry_type, WalEntryType::Put);
        assert_eq!(entry1.key, b"key1");
        assert_eq!(entry1.value, Some(b"value1".to_vec()));

        let entry2 = iter.next().unwrap()?;
        assert_eq!(entry2.lsn, 2);
        assert_eq!(entry2.entry_type, WalEntryType::Put);
        assert_eq!(entry2.key, b"key2");
        assert_eq!(entry2.value, Some(b"value2".to_vec()));

        let entry3 = iter.next().unwrap()?;
        assert_eq!(entry3.lsn, 3);
        assert_eq!(entry3.entry_type, WalEntryType::Delete);
        assert_eq!(entry3.key, b"key1");
        assert_eq!(entry3.value, None);

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn test_wal_corruption() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_path = dir.path().join("corrupt.wal");

        let mut wal = Wal::new(&wal_path)?;
        wal.append(1, WalEntryType::Put, b"key1", Some(b"value1"))?;
        wal.flush()?;

        // Intentionally corrupt the file
        let mut file = OpenOptions::new().write(true).open(&wal_path)?;
        // Corrupt the checksum (last 4 bytes of the first 47-byte record)
        file.seek(SeekFrom::Start(46))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let mut iter = WalIterator::new(&wal_path).map_err(|e| WalError::Io(e))?;
        let result = iter.next().unwrap();

        match result {
            Err(WalError::ChecksumMismatch { lsn, .. }) => assert_eq!(lsn, 1),
            _ => panic!("Expected ChecksumMismatch error, got {:?}", result),
        }

        Ok(())
    }
}
