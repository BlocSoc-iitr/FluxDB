/// Centralized error types for the entire codebase.
// Every submodule's error enum is now written here.
// Using the `thiserror` crate we easily write clean reusable code without the boilerplate
use std::io;
use thiserror::Error;

// ==== DISK ERRORS ==========================================================

/// Errors that can occur during disk operations.
#[derive(Debug, Error)]
pub enum DiskError {
    /// An underlying I/O error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// The provided buffer or data size does not match the configured page size.
    #[error("Invalid page size")]
    InvalidPageSize,
}

// ===== WAL ERRORS ===========================================================

/// Errors that can occur during Write-Ahead Log operations.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Checksum mismatch for LSN {lsn}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        lsn: u64,
        expected: u32,
        actual: u32,
    },

    #[error("Corrupted log: {0}")]
    CorruptedLog(String),

    #[error("WAL record too large: LSN {lsn} is {record_len} bytes, buffer capacity is {capacity}")]
    RecordTooLarge {
        lsn: u64,
        record_len: usize,
        capacity: usize,
    },

    #[error("WAL buffer full: need {needed} bytes, only {available} available")]
    BufferFull { needed: usize, available: usize },

    #[error("WAL background flush failed: {0}")]
    FlushFailed(String),

    #[error("Invalid entry type: {0}")]
    InvalidEntryType(u8),

    #[error("Invalid LSN")]
    InvalidLsn,

    #[error("Disk error: {0}")]
    Disk(#[from] DiskError),
}

// ==== PAGE ERRORS =============================================================
/// Errors that can occur during page-level operations.
#[derive(Debug, Error)]
pub enum PageError {
    #[error("insufficient space: needed {needed} bytes but only {available} available")]
    InsufficientSpace { needed: usize, available: usize },

    #[error("unexpected page type: expected {expected}, found {found}")]
    UnexpectedPageType { expected: u8, found: u8 },
}

// === BUFFER POOL ERRORS =======================================================

/// Errors that can occur in the buffer pool subsystem.
#[derive(Debug, Error)]
pub enum BufferPoolError {
    #[error("Page with ID {0} not found")]
    PageNotFound(u64),

    #[error("Pin count error")]
    PinCountError,

    #[error("No evictable frames available")]
    NoEvictableFrames,

    #[error("Page {page_id} checksum mismatch: expected {expected}, got {actual}")]
    PageCorruption {
        page_id: u64,
        expected: u32,
        actual: u32,
    },

    /// A disk I/O error propagated from the [`DiskError`] layer.
    #[error("Disk error: {0}")]
    Disk(#[from] DiskError),

    /// A WAL error propagated while enforcing WAL-before-page during a flush.
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("Internal error: {0}")]
    InternalError(String),
}

// ==== INDEX ERRORS ==========================================================

/// Errors that can occur during B+Tree index operations.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Key too large: {size} bytes (max {max})")]
    KeyTooLarge { size: usize, max: usize },

    #[error("Value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Insert attempted on a key that already has a visible version.
    #[error("Duplicate key")]
    DuplicateKey,

    /// Another in-progress transaction has already modified this record.
    /// First-writer-wins: the current transaction should abort.
    #[error("Write conflict: another transaction modified this record")]
    WriteConflict,

    #[error("Unexpected page type: expected {expected}, found {found}")]
    UnexpectedPageType { expected: u8, found: u8 },

    #[error("Buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    #[error("Page error: {0}")]
    Page(#[from] PageError),

    /// A WAL error propagated while logging a mutation (e.g. `log_insert`).
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// An in-progress transaction is blocking this insert. The caller should
    /// drop its page latch, wait for `txn_id` to settle, then retry.
    #[error("Wait for transaction {0}")]
    WaitFor(u64),

    /// The metadata/superblock page (page 0) is missing, has the wrong magic,
    /// or an unrecognised format version — the file is not a readable database.
    #[error("Corrupt or invalid metadata page: {0}")]
    CorruptMetadata(String),
}

// ==== TYPE NAME ERRORS =============================================================

/// Errors that can occur when deserializing a [`TypeName`] from raw bytes.
#[derive(Debug, Error)]
pub enum TypeNameError {
    #[error("empty input: need at least 1 byte for the classification tag")]
    Empty,

    #[error("unknown classification byte: {0}")]
    UnknownClassification(u8),

    #[error("invalid UTF-8 in type name: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

// ==== ENGINE ERRORS =============================================================
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("another database exists at that location")]
    AlreadyExists,
    #[error("no database found at that location")]
    NotFound,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("disk error: {0}")]
    Disk(#[from] DiskError),
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("write conflict: the transaction was aborted and may be retried")]
    TransactionConflict,
    #[error("a background thread panicked")]
    BackgroundThreadPanicked,
}
