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
//! -   **Segmented sequential I/O**: WAL records are appended to `wal_XXXXX.log`
//!     files under one WAL directory.
//!
//! ## Segment Layout
//!
//! `Wal::new(path)` treats `path` as a WAL segment directory. A database usually
//! passes `<db>/wal`, producing files such as `wal_00001.log`, `wal_00002.log`,
//! and so on. Records are never split across segments; rotation happens before
//! appending a record that would cross the configured segment size.
//!
//! ## LSN Convention
//!
//! LSN 0 is reserved as the null pageLSN for pages that have no WAL-backed
//! changes. Real WAL records start at LSN 1, which lets recovery distinguish an
//! untouched page from a page that has already applied the first record.
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
use std::collections::VecDeque;
use std::fs::{File, OpenOptions, create_dir_all, metadata, read_dir};
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::disk::DiskManager;
use crate::page::{Lsn, PAGE_SIZE, PageId};
use common::WalError;

pub type Result<T> = std::result::Result<T, WalError>;

/// Default bytes reserved for WAL records before they are flushed to disk.
const WAL_BUFFER_CAPACITY: usize = 16 * 1024 * 1024;
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const WAL_SEGMENT_PREFIX: &str = "wal_";
const WAL_SEGMENT_SUFFIX: &str = ".log";

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
    pub fpi: Option<&'a [u8; PAGE_SIZE]>,
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

#[derive(Debug, Clone)]
struct WalLayout {
    dir: PathBuf,
    segment_size: u64,
}

/// Iterator that reads and validates WAL records across segment files.
///
/// The iterator takes a WAL segment directory, opens segment files lazily, and
/// yields records in segment/offset order. Each record is checksum-validated
/// before it is returned.
pub struct WalIterator {
    reader: WalReader,
    scratch: Vec<u8>,
}

struct WalReader {
    segments: Vec<PathBuf>,
    current_segment: usize,
    current: Option<BufReader<File>>,
}

#[derive(Debug, Clone, Copy)]
struct BufferedRecord {
    lsn: Lsn,
    len: usize,
}

/// Bounded circular byte buffer for serialized WAL records.
///
/// Appenders copy complete records into this buffer. `flush_up_to` drains only
/// complete records from the head, preserving WAL record order on disk.
struct WalBuffer {
    bytes: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
    records: VecDeque<BufferedRecord>,
}

struct WalState {
    buffer: WalBuffer,
    next_lsn: Lsn,
    flushed_lsn: Option<Lsn>,
    flush_error: Option<String>,
    shutdown: bool,
}

struct WalShared {
    state: Mutex<WalState>,
    flush_requested: Condvar,
    durable: Condvar,
    segment_size: u64,
}

/// Write-ahead log manager for one WAL segment directory.
///
/// `Wal` assigns logical LSNs, serializes records into an in-memory ring buffer,
/// and uses a background flush thread to write complete records to segment
/// files. Callers use [`Wal::flush_up_to`] to wait until records are durable
/// before exposing commit or page-flush effects.
pub struct Wal {
    shared: Arc<WalShared>,
    flusher: Option<JoinHandle<()>>,
}

struct SegmentWriter {
    layout: WalLayout,
    segment_index: u64,
    offset: u64,
    file: File,
}

impl WalBuffer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: vec![0; capacity],
            read_pos: 0,
            write_pos: 0,
            records: VecDeque::new(),
        }
    }

    fn capacity(&self) -> usize {
        self.bytes.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn used_space(&self) -> usize {
        if self.is_empty() {
            0
        } else if self.write_pos == self.read_pos {
            self.capacity()
        } else if self.write_pos > self.read_pos {
            self.write_pos - self.read_pos
        } else {
            self.capacity() - self.read_pos + self.write_pos
        }
    }

    fn free_space(&self) -> usize {
        self.capacity() - self.used_space()
    }

    fn push_record(&mut self, lsn: Lsn, record: &[u8]) -> Result<()> {
        if record.len() > self.capacity() {
            return Err(WalError::RecordTooLarge {
                lsn,
                record_len: record.len(),
                capacity: self.capacity(),
            });
        }

        if record.len() > self.free_space() {
            return Err(WalError::BufferFull {
                needed: record.len(),
                available: self.free_space(),
            });
        }

        self.copy_into_ring(record);

        self.records.push_back(BufferedRecord {
            lsn,
            len: record.len(),
        });
        Ok(())
    }

    fn copy_into_ring(&mut self, record: &[u8]) {
        let first = record.len().min(self.capacity() - self.write_pos);
        self.bytes[self.write_pos..self.write_pos + first].copy_from_slice(&record[..first]);

        let second = record.len() - first;
        if second > 0 {
            self.bytes[..second].copy_from_slice(&record[first..]);
        }

        self.write_pos = (self.write_pos + record.len()) % self.capacity();
    }

    fn buffered_prefix_len(&self) -> Option<(usize, Lsn)> {
        let mut bytes = 0;
        let mut last_lsn = None;

        for record in &self.records {
            bytes += record.len;
            last_lsn = Some(record.lsn);
        }

        last_lsn.map(|lsn| (bytes, lsn))
    }

    fn copy_records(&self) -> Vec<(Lsn, Vec<u8>)> {
        let mut records = Vec::with_capacity(self.records.len());
        let mut cursor = self.read_pos;

        for record in &self.records {
            let mut bytes_left = record.len;
            let mut bytes = Vec::with_capacity(record.len);
            while bytes_left > 0 {
                let chunk = bytes_left.min(self.capacity() - cursor);
                bytes.extend_from_slice(&self.bytes[cursor..cursor + chunk]);
                bytes_left -= chunk;
                cursor = (cursor + chunk) % self.capacity();
            }
            records.push((record.lsn, bytes));
        }

        records
    }

    fn consume_prefix(&mut self, bytes_to_consume: usize) {
        let mut remaining = bytes_to_consume;
        while remaining > 0 {
            let record = self
                .records
                .pop_front()
                .expect("WAL buffer prefix should contain complete records");
            remaining -= record.len;
        }

        self.read_pos = (self.read_pos + bytes_to_consume) % self.capacity();

        if self.is_empty() {
            self.read_pos = 0;
            self.write_pos = 0;
        }
    }
}

fn segment_file_name(index: u64) -> String {
    format!("{WAL_SEGMENT_PREFIX}{index:05}{WAL_SEGMENT_SUFFIX}")
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(segment_file_name(index))
}

fn parse_segment_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let index = name
        .strip_prefix(WAL_SEGMENT_PREFIX)?
        .strip_suffix(WAL_SEGMENT_SUFFIX)?;
    index.parse().ok()
}

fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    match read_dir(dir) {
        Ok(entries) => {
            let mut segments = Vec::new();
            for entry in entries {
                let path = entry?.path();
                if let Some(index) = parse_segment_index(&path) {
                    segments.push((index, path));
                }
            }
            segments.sort_by_key(|(index, _)| *index);
            Ok(segments)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn wal_iterator_segments(path: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(list_segments(path)?
        .into_iter()
        .map(|(_, path)| path)
        .collect())
}

impl SegmentWriter {
    fn open(layout: WalLayout) -> Result<Self> {
        create_dir_all(&layout.dir).map_err(WalError::Io)?;
        let segments = list_segments(&layout.dir).map_err(WalError::Io)?;
        let (mut segment_index, mut offset) = match segments.last() {
            Some((index, path)) => (*index, metadata(path).map_err(WalError::Io)?.len()),
            None => (1, 0),
        };

        if offset >= layout.segment_size {
            segment_index += 1;
            offset = 0;
        }

        let path = segment_path(&layout.dir, segment_index);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(WalError::Io)?;

        Ok(Self {
            layout,
            segment_index,
            offset,
            file,
        })
    }

    fn current_path(&self) -> PathBuf {
        segment_path(&self.layout.dir, self.segment_index)
    }

    fn rotate(&mut self) -> Result<()> {
        self.file.flush().map_err(WalError::Io)?;
        DiskManager::sync_file_and_dir(&self.file, &self.current_path())?;

        self.segment_index += 1;
        self.offset = 0;
        let path = self.current_path();
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(WalError::Io)?;
        Ok(())
    }

    fn write_record(&mut self, lsn: Lsn, record: &[u8]) -> Result<()> {
        if record.len() as u64 > self.layout.segment_size {
            return Err(WalError::RecordTooLarge {
                lsn,
                record_len: record.len(),
                capacity: self.layout.segment_size as usize,
            });
        }

        if self.offset > 0 && self.offset + record.len() as u64 > self.layout.segment_size {
            self.rotate()?;
        }

        self.file.write_all(record)?;
        self.offset += record.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.flush().map_err(WalError::Io)?;
        DiskManager::sync_file_and_dir(&self.file, &self.current_path())?;
        Ok(())
    }
}

impl WalReader {
    fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            segments: wal_iterator_segments(path.as_ref())?,
            current_segment: 0,
            current: None,
        })
    }

    fn ensure_current(&mut self) -> io::Result<bool> {
        if self.current.is_some() {
            return Ok(true);
        }
        if self.current_segment >= self.segments.len() {
            return Ok(false);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.segments[self.current_segment])?;
        self.current = Some(BufReader::new(file));
        Ok(true)
    }

    fn advance_segment(&mut self) {
        self.current = None;
        self.current_segment += 1;
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        if !self.ensure_current()? {
            return Ok(0);
        }
        self.current.as_mut().unwrap().stream_position()
    }
}

impl Read for WalReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.ensure_current()? {
            return Ok(0);
        }
        self.current.as_mut().unwrap().read(buf)
    }
}

impl BufRead for WalReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        loop {
            if !self.ensure_current()? {
                return Ok(&[]);
            }

            if self.current.as_mut().unwrap().fill_buf()?.is_empty() {
                self.advance_segment();
                continue;
            }

            return self.current.as_mut().unwrap().fill_buf();
        }
    }

    fn consume(&mut self, amt: usize) {
        if let Some(reader) = self.current.as_mut() {
            reader.consume(amt);
        }
    }
}

impl WalIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<WalIterator> {
        Ok(WalIterator {
            reader: WalReader::new(path)?,
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

            let fpi_range = if blk_flags & 1 == 1 {
                let start = self.scratch.len();
                let end = start + PAGE_SIZE;
                self.scratch.resize(end, 0);
                if let Err(e) = self.reader.read_exact(&mut self.scratch[start..end]) {
                    return Some(Err(e.into()));
                }
                hasher.update(&self.scratch[start..end]);
                Some((start, end))
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

            temp_blocks.push((page_id, blk_flags, fpi_range, data_range));
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
            .map(|(page_id, blk_flags, fpi_range, data_range)| {
                let fpi = fpi_range
                    .map(|(s, e)| <&[u8; PAGE_SIZE]>::try_from(&self.scratch[s..e]).unwrap());
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
    /// Opens an existing WAL segment directory, or creates it if needed.
    ///
    /// Scans every segment to find the maximum LSN and handles torn-tail
    /// corruption by truncating the last segment to the last valid
    /// record boundary. If mid-log corruption is detected, an error is returned.
    /// New WAL directories start at LSN 1 because LSN 0 is reserved as the null
    /// pageLSN.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::new_with_buffer_capacity(path, WAL_BUFFER_CAPACITY)
    }

    fn new_with_buffer_capacity(path: impl AsRef<Path>, buffer_capacity: usize) -> Result<Self> {
        Self::new_with_options(path, buffer_capacity, WAL_SEGMENT_SIZE)
    }

    fn new_with_options(
        path: impl AsRef<Path>,
        buffer_capacity: usize,
        segment_size: u64,
    ) -> Result<Self> {
        let layout = WalLayout {
            dir: path.as_ref().to_path_buf(),
            segment_size,
        };
        create_dir_all(&layout.dir).map_err(WalError::Io)?;

        // LSN 0 is the null pageLSN; real WAL records start at 1.
        let mut next_lsn = 1;
        let mut flushed_lsn = None;
        let segments = list_segments(&layout.dir).map_err(WalError::Io)?;

        let last_segment_idx = segments.len().saturating_sub(1);
        for (idx, (_segment_index, segment_path)) in segments.iter().enumerate() {
            let file_len = metadata(segment_path).map_err(WalError::Io)?.len();
            let mut iter = WalIterator {
                reader: WalReader {
                    segments: vec![segment_path.clone()],
                    current_segment: 0,
                    current: None,
                },
                scratch: Vec::with_capacity(4096),
            };

            loop {
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
                            WalError::Io(io_err) => io_err.kind() == io::ErrorKind::UnexpectedEof,
                            _ => false,
                        };

                        if idx == last_segment_idx && (is_eof || err_pos == file_len) {
                            let f = OpenOptions::new()
                                .write(true)
                                .open(segment_path)
                                .map_err(WalError::Io)?;
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

        let segment_size = layout.segment_size;
        let writer = SegmentWriter::open(layout)?;

        let shared = Arc::new(WalShared {
            state: Mutex::new(WalState {
                buffer: WalBuffer::with_capacity(buffer_capacity),
                next_lsn,
                flushed_lsn,
                flush_error: None,
                shutdown: false,
            }),
            flush_requested: Condvar::new(),
            durable: Condvar::new(),
            segment_size,
        });

        let flusher = Some(Self::spawn_flush_thread(Arc::clone(&shared), writer)?);

        Ok(Wal { shared, flusher })
    }

    fn spawn_flush_thread(
        shared: Arc<WalShared>,
        mut writer: SegmentWriter,
    ) -> Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("fluxdb-wal-flusher".into())
            .spawn(move || run_flush_thread(shared, &mut writer))
            .map_err(WalError::Io)
    }

    pub fn next_lsn(&self) -> Lsn {
        self.shared.state.lock().unwrap().next_lsn
    }

    pub fn flushed_lsn(&self) -> Option<Lsn> {
        self.shared.state.lock().unwrap().flushed_lsn
    }

    pub fn log_commit(&self, txn_id: u64) -> Result<Lsn> {
        self.append(WalRecordType::Commit, txn_id, &[], None)
    }

    pub fn log_abort(&self, txn_id: u64) -> Result<Lsn> {
        self.append(WalRecordType::Abort, txn_id, &[], None)
    }

    /// Appends only — durability is deferred to the buffer pool's flush seam
    /// (WAL-before-page) or to the transaction's commit, never an fsync here.
    pub fn log_insert(
        &self,
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

    pub fn log_set_xmax(&self, txn_id: u64, page_id: PageId, slot: u16, xmax: u64) -> Result<Lsn> {
        let mut payload = Vec::with_capacity(2 + 8);
        payload.extend_from_slice(&slot.to_le_bytes());
        payload.extend_from_slice(&xmax.to_le_bytes());

        let block = Block {
            page_id,
            blk_flags: BLK_HAS_DATA,
            fpi: None,
            data: Some(&payload),
        };
        self.append(WalRecordType::SetXMax, txn_id, &[block], None)
    }
    pub fn log_leaf_split(
        &self,
        txn_id: u64,
        left: (PageId, &[u8; PAGE_SIZE]),
        right: (PageId, &[u8; PAGE_SIZE]),
        old_neighbour: Option<(PageId, &[u8; PAGE_SIZE])>,
    ) -> Result<Lsn> {
        let mut blocks = Vec::with_capacity(3);
        let left_block = Block {
            page_id: left.0,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(left.1),
            data: None,
        };
        blocks.push(left_block);
        let right_block = Block {
            page_id: right.0,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(right.1),
            data: None,
        };
        blocks.push(right_block);
        if let Some((pid, img)) = old_neighbour {
            let neighbour_block = Block {
                page_id: pid,
                blk_flags: BLK_HAS_FPI,
                fpi: Some(img),
                data: None,
            };
            blocks.push(neighbour_block);
        }
        self.append(WalRecordType::LeafSplit, txn_id, &blocks, None)
    }
    pub fn log_internal_split(
        &self,
        txn_id: u64,
        left: (PageId, &[u8; PAGE_SIZE]),
        right: (PageId, &[u8; PAGE_SIZE]),
    ) -> Result<Lsn> {
        let mut blocks = Vec::with_capacity(2);
        let left_block = Block {
            page_id: left.0,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(left.1),
            data: None,
        };
        let right_block = Block {
            page_id: right.0,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(right.1),
            data: None,
        };
        blocks.push(left_block);
        blocks.push(right_block);
        self.append(WalRecordType::InternalSPlit, txn_id, &blocks, None)
    }
    pub fn log_insert_downlink(
        &self,
        txn_id: u64,
        parent_page: PageId,
        at_index: u16,
        sep_key: &[u8],
        right_child: PageId,
        left_child: PageId,
    ) -> Result<Lsn> {
        let mut blocks = Vec::with_capacity(2);
        let mut parent_payload: Vec<u8> = Vec::with_capacity(2 + 2 + 8 + sep_key.len());
        parent_payload.extend_from_slice(&at_index.to_le_bytes());
        parent_payload.extend_from_slice(&(sep_key.len() as u16).to_le_bytes());
        parent_payload.extend_from_slice(&right_child.to_le_bytes());
        parent_payload.extend_from_slice(sep_key);
        let parent_block = Block {
            page_id: parent_page,
            blk_flags: BLK_HAS_DATA,
            fpi: None,
            data: Some(&parent_payload),
        };
        blocks.push(parent_block);
        let child_block = Block {
            page_id: left_child,
            blk_flags: 0,
            fpi: None,
            data: None,
        };
        blocks.push(child_block);
        self.append(WalRecordType::InsertDownLink, txn_id, &blocks, None)
    }
    pub fn log_new_root(&self, txn_id: u64, new_root: (PageId, &[u8; PAGE_SIZE])) -> Result<Lsn> {
        let mut blocks = Vec::with_capacity(2);
        let new_root_block = Block {
            page_id: new_root.0,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(new_root.1),
            data: None,
        };
        blocks.push(new_root_block);
        let root_bytes = new_root.0.to_le_bytes();
        let meta_block = Block {
            page_id: 0, // updating metadata page
            blk_flags: BLK_HAS_DATA,
            fpi: None,
            data: Some(&root_bytes),
        };
        blocks.push(meta_block);
        self.append(WalRecordType::NewRoot, txn_id, &blocks, None)
    }

    pub fn log_page_compact(
        &self,
        txn_id: u64,
        page_id: PageId,
        image: &[u8; PAGE_SIZE],
    ) -> Result<Lsn> {
        let block = Block {
            page_id,
            blk_flags: BLK_HAS_FPI,
            fpi: Some(image),
            data: None,
        };
        self.append(WalRecordType::PageCompact, txn_id, &[block], None)
    }

    /// Appends a new physiological record to the WAL buffer.
    ///
    /// This method assigns the next available LSN, serializes the record according to the
    /// internal wire format, calculates its CRC32 checksum, and writes it to the internal
    /// `BufWriter`. Note that the record is not guaranteed to be durable on disk until
    /// `flush_up_to` is called.
    fn append(
        &self,
        entry_type: WalRecordType,
        txn_id: u64,
        blocks: &[Block<'_>],
        main_data: Option<&[u8]>,
    ) -> Result<Lsn> {
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

        let segment_limit = self.shared.segment_size as usize;
        if record_size > segment_limit {
            return Err(WalError::RecordTooLarge {
                lsn: 0,
                record_len: record_size,
                capacity: segment_limit,
            });
        }

        let mut state = self.shared.state.lock().unwrap();
        if let Some(err) = state.flush_error.as_ref() {
            return Err(WalError::FlushFailed(err.clone()));
        }

        let lsn = state.next_lsn;

        let mut record = Vec::with_capacity(record_size);
        record.reserve(record_size);

        record.extend_from_slice(&lsn.to_le_bytes());
        record.extend_from_slice(&(record_size as u32).to_le_bytes());
        record.push(entry_type as u8);
        record.push(blocks.len() as u8);
        record.extend_from_slice(&txn_id.to_le_bytes());

        let main_len = main_data.map_or(0, |data| data.len()) as u16;
        record.extend_from_slice(&main_len.to_le_bytes());

        for block in blocks {
            record.extend_from_slice(&block.page_id.to_le_bytes());
            record.push(block.blk_flags);

            let data_len = block.data.map_or(0, |d| d.len()) as u16;
            record.extend_from_slice(&data_len.to_le_bytes());

            if let Some(fpi) = block.fpi {
                record.extend_from_slice(&fpi[..]);
            }
            if let Some(data) = block.data {
                record.extend_from_slice(data);
            }
        }
        if let Some(data) = main_data {
            record.extend_from_slice(data);
        }

        let mut hasher = Hasher::new();
        hasher.update(&record);
        let checksum = hasher.finalize();

        record.extend_from_slice(&checksum.to_le_bytes());
        state.buffer.push_record(lsn, &record)?;
        state.next_lsn += 1;

        Ok(lsn)
    }

    /// Flushes pending WAL bytes through `lsn`.
    ///
    /// LSN 0 is the null pageLSN, so it never needs flushing. If the WAL has no
    /// appended records yet (`next_lsn <= 1`), or the clamped target resolves to
    /// 0, the call is a no-op — preventing a permanent park on the `durable`
    /// condvar.
    pub fn flush_up_to(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.shared.state.lock().unwrap();

        // Guard the *clamped target*, not just the argument. When next_lsn == 1
        // (no records appended), checked_sub(1) yields Some(0) and the flusher
        // has nothing buffered, so parking on `durable` would hang forever.
        let target_lsn = state
            .next_lsn
            .checked_sub(1)
            .map(|last| lsn.min(last))
            .unwrap_or(0);
        if target_lsn == 0 {
            return Ok(());
        }

        loop {
            if state
                .flushed_lsn
                .is_some_and(|flushed| flushed >= target_lsn)
            {
                return Ok(());
            }
            if let Some(err) = state.flush_error.as_ref() {
                return Err(WalError::FlushFailed(err.clone()));
            }

            self.shared.flush_requested.notify_one();
            state = self.shared.durable.wait(state).unwrap();
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        {
            let mut state = self.shared.state.lock().unwrap();
            state.shutdown = true;
            self.shared.flush_requested.notify_one();
        }

        if let Some(flusher) = self.flusher.take() {
            let _ = flusher.join();
        }
    }
}

fn run_flush_thread(shared: Arc<WalShared>, writer: &mut SegmentWriter) {
    loop {
        let (records, bytes_to_consume, durable_lsn) = {
            let mut state = shared.state.lock().unwrap();

            loop {
                if state.shutdown {
                    return;
                }

                if let Some((bytes_to_flush, durable_lsn)) = state.buffer.buffered_prefix_len() {
                    let records = state.buffer.copy_records();
                    break (records, bytes_to_flush, durable_lsn);
                }

                state = shared.flush_requested.wait(state).unwrap();
            }
        };

        let flush_result = (|| -> Result<()> {
            for (lsn, record) in &records {
                writer.write_record(*lsn, record)?;
            }
            writer.sync()?;
            Ok(())
        })();

        let mut state = shared.state.lock().unwrap();
        match flush_result {
            Ok(()) => {
                state.buffer.consume_prefix(bytes_to_consume);
                state.flushed_lsn = Some(durable_lsn);
            }
            Err(err) => {
                state.flush_error = Some(err.to_string());
                shared.durable.notify_all();
                return;
            }
        }
        shared.durable.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_wal_roundtrip() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_dir = dir.path().join("test-wal");

        let wal = Wal::new(&wal_dir)?;

        let block1 = Block {
            page_id: 100,
            blk_flags: 2,
            fpi: None,
            data: Some(&[1, 2, 3, 4]),
        };

        wal.append(WalRecordType::Insert, 42, &[block1], None)?;

        let fpi2 = [0u8; PAGE_SIZE];
        let block2 = Block {
            page_id: 101,
            blk_flags: 1,
            fpi: Some(&fpi2),
            data: None,
        };

        wal.append(
            WalRecordType::Commit,
            43,
            &[block2],
            Some(&[8, 7, 6, 5, 4, 3, 2, 1]),
        )?;

        wal.flush_up_to(2)?;

        let mut iter = WalIterator::new(&wal_dir).map_err(|e| WalError::Io(e))?;

        {
            let entry1 = iter.next_record().unwrap()?;
            assert_eq!(entry1.lsn, 1);
            assert_eq!(entry1.entry_type, WalRecordType::Insert);
            assert_eq!(entry1.txn_id, 42);
            assert_eq!(entry1.blocks.len(), 1);
            assert_eq!(entry1.blocks[0].page_id, 100);
            assert_eq!(entry1.blocks[0].data.unwrap(), &[1, 2, 3, 4]);
        }

        {
            let entry2 = iter.next_record().unwrap()?;
            assert_eq!(entry2.lsn, 2);
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
        let wal_dir = dir.path().join("clean-wal");

        {
            let wal = Wal::new(&wal_dir)?;
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

        let wal = Wal::new(&wal_dir)?;
        assert_eq!(wal.next_lsn(), 3);
        Ok(())
    }

    #[test]
    fn test_flush_tracks_requested_lsn_and_noops_when_durable() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("flush-lsn-wal");

        let wal = Wal::new(&wal_dir)?;
        let first = wal.append(WalRecordType::Insert, 42, &[], None)?;
        let second = wal.append(WalRecordType::Commit, 42, &[], None)?;

        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(wal.flushed_lsn(), None);

        wal.flush_up_to(first)?;
        assert!(wal.flushed_lsn().is_some_and(|flushed| flushed >= first));

        wal.flush_up_to(first)?;
        assert!(wal.flushed_lsn().is_some_and(|flushed| flushed >= first));

        wal.flush_up_to(second)?;
        assert_eq!(wal.flushed_lsn(), Some(second));

        Ok(())
    }

    #[test]
    fn test_circular_buffer_wraparound_preserves_record_order() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("wraparound-wal");

        let wal = Wal::new_with_buffer_capacity(&wal_dir, 96)?;

        for txn_id in 0..3 {
            wal.append(WalRecordType::Commit, txn_id, &[], None)?;
        }
        wal.flush_up_to(1)?;

        for txn_id in 3..5 {
            wal.append(WalRecordType::Commit, txn_id, &[], None)?;
        }
        wal.flush_up_to(5)?;

        let mut iter = WalIterator::new(&wal_dir).map_err(WalError::Io)?;
        for expected in 0..5 {
            let record = iter.next_record().unwrap()?;
            assert_eq!(record.lsn, expected + 1);
            assert_eq!(record.txn_id, expected);
        }
        assert!(iter.next_record().is_none());

        Ok(())
    }

    #[test]
    fn test_circular_buffer_append_error_does_not_consume_lsn() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("too-small-wal");

        let wal = Wal::new_with_buffer_capacity(&wal_dir, 27)?;
        let result = wal.append(WalRecordType::Commit, 1, &[], None);

        assert!(matches!(
            result,
            Err(WalError::RecordTooLarge {
                lsn: 1,
                record_len: 28,
                capacity: 27
            })
        ));
        assert_eq!(wal.next_lsn(), 1);

        Ok(())
    }

    #[test]
    fn test_background_flush_wakes_multiple_waiters() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("group-commit-wal");
        let wal = Arc::new(Wal::new(&wal_dir)?);

        let mut handles = Vec::new();
        for txn_id in 0..8 {
            let lsn = wal.log_commit(txn_id)?;
            let wal = Arc::clone(&wal);
            handles.push(thread::spawn(move || wal.flush_up_to(lsn)));
        }

        for handle in handles {
            handle.join().expect("flush waiter panicked")?;
        }

        assert_eq!(wal.flushed_lsn(), Some(8));

        let mut iter = WalIterator::new(&wal_dir).map_err(WalError::Io)?;
        for expected in 0..8 {
            let record = iter.next_record().unwrap()?;
            assert_eq!(record.lsn, expected + 1);
            assert_eq!(record.txn_id, expected);
        }
        assert!(iter.next_record().is_none());

        Ok(())
    }

    #[test]
    fn test_wal_rotates_segments_without_splitting_records() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("rotating-wal");
        let wal = Wal::new_with_options(&wal_dir, 1024, 64)?;

        for txn_id in 0..5 {
            wal.log_commit(txn_id)?;
        }
        wal.flush_up_to(5)?;
        drop(wal);

        let segments = list_segments(&wal_dir).map_err(WalError::Io)?;
        assert_eq!(segments.len(), 3);
        assert_eq!(metadata(segment_path(&wal_dir, 1))?.len(), 56);
        assert_eq!(metadata(segment_path(&wal_dir, 2))?.len(), 56);
        assert_eq!(metadata(segment_path(&wal_dir, 3))?.len(), 28);

        let mut iter = WalIterator::new(&wal_dir).map_err(WalError::Io)?;
        for expected in 0..5 {
            let record = iter.next_record().unwrap()?;
            assert_eq!(record.lsn, expected + 1);
            assert_eq!(record.txn_id, expected);
        }
        assert!(iter.next_record().is_none());

        let reopened = Wal::new_with_options(&wal_dir, 1024, 64)?;
        assert_eq!(reopened.next_lsn(), 6);

        Ok(())
    }

    #[test]
    fn test_wal_recovery_torn_tail() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_dir = dir.path().join("torntail-wal");

        {
            let wal = Wal::new(&wal_dir)?;
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

        let first_segment = segment_path(&wal_dir, 1);
        let mut file = OpenOptions::new().write(true).open(&first_segment)?;
        let file_len = file.metadata()?.len();

        file.seek(SeekFrom::Start(file_len - 1))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let wal = Wal::new(&wal_dir)?;
        assert_eq!(wal.next_lsn(), 2);

        let new_file_len = file.metadata()?.len();
        assert!(new_file_len < file_len);

        Ok(())
    }

    #[test]
    fn test_wal_mid_log_corruption() -> Result<()> {
        let dir = tempdir().map_err(|e| WalError::Io(e))?;
        let wal_dir = dir.path().join("midlog-wal");

        {
            let wal = Wal::new(&wal_dir)?;
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

        let first_segment = segment_path(&wal_dir, 1);
        let mut file = OpenOptions::new().write(true).open(&first_segment)?;

        file.seek(SeekFrom::Start(40))?;
        file.write_all(&[0xFF])?;
        file.sync_all()?;

        let result = Wal::new(&wal_dir);
        match result {
            Err(WalError::ChecksumMismatch { lsn, .. }) => assert_eq!(lsn, 1),
            Err(e) => panic!("Expected ChecksumMismatch error, got error: {:?}", e),
            Ok(_) => panic!("Expected ChecksumMismatch error, got Ok(_)"),
        }

        Ok(())
    }

    #[test]
    fn test_wal_torn_lsn() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("torn-lsn-wal");

        {
            let wal = Wal::new(&wal_dir)?;
            let block1 = Block {
                page_id: 100,
                blk_flags: 2,
                fpi: None,
                data: Some(&[1, 2, 3, 4]),
            };
            wal.append(WalRecordType::Insert, 42, &[block1], None)?;
            wal.flush_up_to(1)?;
        }

        let first_segment = segment_path(&wal_dir, 1);
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&first_segment)?;
        let clean_len = file.metadata()?.len();

        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
        file.sync_all()?;

        let _ = Wal::new(&wal_dir)?;

        let new_file_len = metadata(&first_segment).map_err(WalError::Io)?.len();
        assert_eq!(
            new_file_len, clean_len,
            "Garbage bytes were not truncated! Expected len {}, got {}",
            clean_len, new_file_len
        );

        Ok(())
    }

    #[test]
    fn test_flush_up_to_on_empty_wal_does_not_deadlock() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("empty-flush-wal");

        let wal = Wal::new(&wal_dir)?;

        // No records appended: next_lsn == 1, flushed_lsn == None.
        // flush_up_to(5) must return Ok immediately — not park.
        wal.flush_up_to(5)?;
        // Also verify that flush_up_to(0) (null pageLSN) is a no-op.
        wal.flush_up_to(0)?;
        // And flush_up_to(1) on a WAL that hasn't had anything appended.
        wal.flush_up_to(1)?;

        Ok(())
    }

    /// Finding 2+8: after a persistent I/O error the flush thread must reach
    /// a terminal state — record the error, wake all waiters, and stop.
    /// Before the fix the thread hot-retried the same failing write, spinning
    /// a core and never draining the buffer.
    #[test]
    fn test_flush_thread_stops_on_io_error() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("io-error-wal");

        let wal = Wal::new(&wal_dir)?;
        wal.append(WalRecordType::Commit, 1, &[], None)?;

        // Remove the WAL directory to force an I/O error on the next flush.
        std::fs::remove_dir_all(&wal_dir).map_err(WalError::Io)?;

        let result = wal.flush_up_to(1);
        assert!(
            matches!(result, Err(WalError::FlushFailed(_))),
            "Expected FlushFailed, got {:?}",
            result
        );

        // Subsequent appends should also fail with FlushFailed.
        let result = wal.append(WalRecordType::Commit, 2, &[], None);
        assert!(
            matches!(result, Err(WalError::FlushFailed(_))),
            "Expected FlushFailed on subsequent append, got {:?}",
            result
        );

        Ok(())
    }

    /// Finding 7: records that exceed the segment size must be rejected at
    /// `append` time (before the LSN is consumed), not later on the flusher.
    /// This prevents the scenario where append succeeds but the flusher fails
    /// and sets `flush_error`, stranding an acknowledged record.
    #[test]
    fn test_segment_size_check_in_append() -> Result<()> {
        let dir = tempdir().map_err(WalError::Io)?;
        let wal_dir = dir.path().join("segment-limit-wal");

        // buffer_capacity=4096 > segment_size=30. A minimal commit record is
        // 28 bytes, which fits under segment_size=30. But a record with block
        // data will exceed it.
        let wal = Wal::new_with_options(&wal_dir, 4096, 30)?;

        // A bare commit (28 bytes) should succeed — under the 30-byte limit.
        let lsn = wal.append(WalRecordType::Commit, 1, &[], None)?;
        assert_eq!(lsn, 1);

        // A record with block data that pushes it over 30 bytes must fail
        // synchronously with RecordTooLarge (not BufferFull), and the LSN must
        // not be consumed.
        let block = Block {
            page_id: 42,
            blk_flags: BLK_HAS_DATA,
            fpi: None,
            data: Some(&[1, 2, 3, 4]),
        };
        let result = wal.append(WalRecordType::Insert, 2, &[block], None);
        assert!(
            matches!(result, Err(WalError::RecordTooLarge { .. })),
            "Expected RecordTooLarge, got {:?}",
            result
        );
        // LSN must not have been consumed.
        assert_eq!(wal.next_lsn(), 2);

        Ok(())
    }
}
