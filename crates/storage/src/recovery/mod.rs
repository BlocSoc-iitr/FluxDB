//! Crash recovery: replays the WAL at startup to rebuild the CLOG and redo
//! page changes, run before the engine serves queries. Redo-only (no undo).

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::{IndexError, Key, Value, WalError};
use db_core::transaction_manager::TransactionManager;

use crate::buffer_pool::BufferPoolManager;
use crate::page::{InternalPageMutator, LeafPageMutator};
use crate::wal::{WalIterator, WalRecord, WalRecordType};

pub type Result<T> = std::result::Result<T, IndexError>;

pub struct RecoveryManager {
    pool: Arc<BufferPoolManager>,
    wal_path: PathBuf,
    tm: Arc<TransactionManager>,
}

impl RecoveryManager {
    pub fn new(
        pool: Arc<BufferPoolManager>,
        wal_path: PathBuf,
        tm: Arc<TransactionManager>,
    ) -> Self {
        Self { pool, wal_path, tm }
    }

    /// Replay the WAL in LSN order: rebuild the CLOG, mark crash victims, restore
    /// the txn-id allocator, and redo page changes. Idempotent (LSN-gated).
    ///
    /// Generic over the index's `(K, V)` — physiological redo drives the typed
    /// page mutators. Assumes a single index per WAL.
    pub fn recover<K: Key, V: Value>(&self) -> Result<()> {
        let mut iter = WalIterator::new(&self.wal_path).map_err(WalError::Io)?;

        // Every txn that did work, and the highest id seen.
        let mut seen: HashSet<u64> = HashSet::new();
        let mut max_txn = 0u64;

        while let Some(r) = iter.next_record() {
            let record = r?;

            if record.txn_id != 0 {
                seen.insert(record.txn_id);
                max_txn = max_txn.max(record.txn_id);
            }

            match record.entry_type {
                WalRecordType::Commit => self.tm.mark_committed(record.txn_id),
                WalRecordType::Abort => self.tm.mark_aborted(record.txn_id),
                _ => self.redo_record::<K, V>(&record)?,
            }
        }

        // Crash victims: a txn that did work but never settled was in-flight at
        // the crash → abort (redo-only, no undo).
        for &txn_id in &seen {
            if !self.tm.is_committed(txn_id) && !self.tm.is_aborted(txn_id) {
                self.tm.mark_aborted(txn_id);
            }
        }

        // Restore the id allocator so new txns can't reuse a recovered id.
        self.tm.next_txn_id.fetch_max(max_txn + 1, Ordering::AcqRel);

        self.pool.flush_all_pages()?;
        Ok(())
    }

    /// Replay one page-touching record: per block, LSN-gate then apply (FPI
    /// stamp or physiological), and re-stamp the page LSN.
    fn redo_record<K: Key, V: Value>(&self, record: &WalRecord) -> Result<()> {
        for (i, block) in record.blocks.iter().enumerate() {
            let mut guard = self.pool.ensure_page(block.page_id)?;
            if record.lsn <= crate::page::page_lsn(&guard[..]) {
                continue; // already durable — idempotent skip
            }
            match block.fpi {
                Some(fpi) => {
                    let dst: &mut [u8] = &mut guard[..];
                    dst.copy_from_slice(fpi);
                }
                None => Self::apply_data::<K, V>(record.entry_type, i, block.data, &mut guard[..])?,
            }
            crate::page::set_lsn(&mut guard[..], record.lsn);
        }
        Ok(())
    }

    /// Physiological apply for a DATA / flag-only block, dispatched on
    /// `(record type, block index)`. Payload layouts mirror `wal.rs::log_*`.
    fn apply_data<K: Key, V: Value>(
        entry_type: WalRecordType,
        block_idx: usize,
        data: Option<&[u8]>,
        page: &mut [u8],
    ) -> Result<()> {
        match (entry_type, block_idx) {
            (WalRecordType::Insert, 0) => {
                // slot u16, key_len u16, val_len u16, xmin u64, key, val
                let d = data.expect("Insert record missing data block");
                let slot = u16::from_le_bytes(d[0..2].try_into().unwrap()) as usize;
                let key_len = u16::from_le_bytes(d[2..4].try_into().unwrap()) as usize;
                let val_len = u16::from_le_bytes(d[4..6].try_into().unwrap()) as usize;
                let xmin = u64::from_le_bytes(d[6..14].try_into().unwrap());
                let key = &d[14..14 + key_len];
                let val = &d[14 + key_len..14 + key_len + val_len];
                let mut m = LeafPageMutator::<K, V>::new(page);
                m.insert(slot, &K::from_bytes(key), &V::from_bytes(val))?;
                m.set_xmin(slot, xmin);
            }
            (WalRecordType::SetXMax, 0) => {
                // slot u16, xmax u64
                let d = data.expect("SetXmax record missing data block");
                let slot = u16::from_le_bytes(d[0..2].try_into().unwrap()) as usize;
                let xmax = u64::from_le_bytes(d[2..10].try_into().unwrap());
                LeafPageMutator::<K, V>::new(page).set_xmax(slot, xmax);
            }
            (WalRecordType::InsertDownLink, 0) => {
                // at_index u16, sep_len u16, right_child u64, sep_key
                let d = data.expect("InsertDownlink parent missing data block");
                let at_index = u16::from_le_bytes(d[0..2].try_into().unwrap()) as usize;
                let sep_len = u16::from_le_bytes(d[2..4].try_into().unwrap()) as usize;
                let right_child = u64::from_le_bytes(d[4..12].try_into().unwrap());
                let sep_key = &d[12..12 + sep_len];
                InternalPageMutator::<K>::new(page).insert_key_and_right_child(
                    at_index,
                    &K::from_bytes(sep_key),
                    right_child,
                )?;
            }
            (WalRecordType::InsertDownLink, 1) => {
                // child block: clear INCOMPLETE_SPLIT (option A).
                crate::page::clear_incomplete_split(page);
            }
            (WalRecordType::NewRoot, 1) => {
                // page-0 block: root_page_id u64
                let d = data.expect("NewRoot page-0 missing data block");
                let root_page_id = u64::from_le_bytes(d[0..8].try_into().unwrap());
                crate::page::meta::set_root(page, root_page_id);
            }
            _ => {
                // No physiological apply for this (type, block). FPI blocks never
                // reach here; v2 records aren't emitted yet.
            }
        }
        Ok(())
    }
}
