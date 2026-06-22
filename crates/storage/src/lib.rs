//! Storage subsystem.
//!
//! This crate owns the durable page file, buffer pool, WAL, crash recovery, and
//! B+Tree index implementation. Higher layers interact with it through typed
//! index operations while this crate enforces WAL-before-page flushing and
//! page-level corruption checks.
//!
//! ## Layout
//!
//! A database directory contains a fixed-size page file (`data.db`) and a WAL
//! segment directory (`wal/`). The WAL is replayed during engine open before
//! the index is used, so committed changes that were not written to `data.db`
//! can be reconstructed.

pub mod buffer_pool;
pub mod disk;
pub mod index;
pub mod page;
pub mod recovery;
pub mod wal;
