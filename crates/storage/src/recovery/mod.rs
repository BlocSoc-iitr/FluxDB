//! Crash recovery.
//!
//! Recovery replays WAL records at engine startup before the B+Tree index is
//! opened. It rebuilds the transaction manager's CLOG from commit/abort records
//! and redoes page changes that may not have reached `data.db`.
//!
//! ## Recovery Model
//!
//! This is redo-only recovery. Transactions that wrote records but never logged
//! a commit or abort are marked aborted after the WAL scan. Their page changes
//! may still be redone, but MVCC visibility hides them through the rebuilt CLOG.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::{IndexError, Key, Value, WalError};
use db_core::transaction_manager::TransactionManager;

use crate::buffer_pool::BufferPoolManager;
use crate::page::{ChildSide, InternalPageMutator, LeafPageMutator};
use crate::wal::{
    UNLINK_KEEP_RIGHT, UNLINK_ROLE_LEFT, UNLINK_ROLE_PARENT, UNLINK_ROLE_RIGHT, WalIterator,
    WalRecord, WalRecordType,
};

pub type Result<T> = std::result::Result<T, IndexError>;

/// Redo-only WAL recovery for one database directory.
///
/// The manager owns no background state. `Engine::open` constructs it with the
/// freshly opened buffer pool, the WAL segment directory, and a fresh
/// transaction manager, then calls [`RecoveryManager::recover`] before opening
/// the index.
pub struct RecoveryManager {
    pool: Arc<BufferPoolManager>,
    wal_dir: PathBuf,
    tm: Arc<TransactionManager>,
}

impl RecoveryManager {
    /// Creates a recovery manager over a WAL segment directory.
    ///
    /// `wal_dir` must be the same directory used by the live [`crate::wal::Wal`]
    /// manager, usually `<db>/wal`.
    pub fn new(
        pool: Arc<BufferPoolManager>,
        wal_dir: PathBuf,
        tm: Arc<TransactionManager>,
    ) -> Self {
        Self { pool, wal_dir, tm }
    }

    /// Replay the WAL in LSN order: rebuild the CLOG, mark crash victims, restore
    /// the txn-id allocator, and redo page changes. Idempotent (LSN-gated).
    pub fn recover<K: Key, V: Value>(&self) -> Result<()> {
        // PASS 1: Find the latest valid Checkpoint record
        let mut checkpoint_opt: Option<crate::wal::CheckpointData> = None;

        {
            let mut iter = WalIterator::new(&self.wal_dir).map_err(WalError::Io)?;
            while let Some(r) = iter.next_record() {
                let record = r?;
                if record.entry_type == WalRecordType::Checkpoint {
                    match record.parse_checkpoint() {
                        Ok(data) => {
                            checkpoint_opt = Some(data);
                        }
                        Err(e) => {
                            tracing::warn!("Warning: corrupt checkpoint at LSN {}: {:?}", record.lsn, e);
                        }
                    }
                }
            }
        }

        // Seed CLOG from checkpoint (if found)
        if let Some(ref ckpt) = checkpoint_opt {
            self.tm.seed_clog_from_checkpoint(&ckpt.pinned_aborted);
        }

        // Determine redo starting point and initial watermarks
        let redo_point = checkpoint_opt.as_ref().map(|c| c.redo_point).unwrap_or(0);

        let mut max_txn = checkpoint_opt
            .as_ref()
            .map(|c| c.next_txn_id.saturating_sub(1))
            .unwrap_or(0);

        let mut max_page_id = checkpoint_opt
            .as_ref()
            .map(|c| c.next_page_id.saturating_sub(1))
            .unwrap_or(0);

        // PASS 2: Redo scan from redo_point
        let mut seen: HashSet<u64> = HashSet::new();

        {
            let mut iter = WalIterator::new(&self.wal_dir).map_err(WalError::Io)?;
            while let Some(r) = iter.next_record() {
                let record = r?;

                // TODO: Skip records below the redo point once checkpoint flushes pages
                // Currently, checkpoints don't flush dirty pages (DESIGN.md step 3), so
                // skipping WAL records below the redo point would lose data. We must replay
                // all records until page flushing is implemented.
                // if record.lsn < redo_point {
                //     continue;
                // }
                let _ = redo_point; // silence unused warning

                // Track all txn IDs seen in record headers
                if record.txn_id != 0 {
                    seen.insert(record.txn_id);
                    max_txn = max_txn.max(record.txn_id);
                }

                // Track max page ID touched
                for block in &record.blocks {
                    max_page_id = max_page_id.max(block.page_id);
                }

                // Apply the record
                match record.entry_type {
                    WalRecordType::Commit => self.tm.mark_committed(record.txn_id),
                    WalRecordType::Abort => self.tm.mark_aborted(record.txn_id),
                    WalRecordType::Checkpoint => {} // skip checkpoint records in redo
                    WalRecordType::OverflowFree => self.redo_overflow_free(&record)?,
                    _ => self.redo_record::<K, V>(&record)?,
                }
            }
        }

        // Crash Victims: (checkpoint.active_txns ∪ seen) ∩ unsettled
        let crash_victim_candidates = if let Some(ref ckpt) = checkpoint_opt {
            let mut candidates = HashSet::from_iter(ckpt.active_txns.iter().copied());
            candidates.extend(&seen);
            candidates
        } else {
            seen
        };

        for txn_id in crash_victim_candidates {
            if !self.tm.is_committed(txn_id) && !self.tm.is_aborted(txn_id) {
                self.tm.mark_aborted(txn_id);
            }
        }

        // Restore Watermarks
        let final_next_txn_id = if let Some(ref ckpt) = checkpoint_opt {
            ckpt.next_txn_id.max(max_txn + 1)
        } else {
            max_txn + 1
        };

        if final_next_txn_id > 0 {
            self.tm
                .next_txn_id
                .fetch_max(final_next_txn_id, Ordering::AcqRel);
        }

        let final_next_page_id = if let Some(ref ckpt) = checkpoint_opt {
            ckpt.next_page_id.max(max_page_id + 1)
        } else {
            max_page_id + 1
        };
        self.pool.advance_next_page_id(final_next_page_id);

        self.pool.flush_all_pages()?;
        Ok(())
    }

    /// Replay an `OverflowFree` record: delete every overflow page listed in
    /// `main_data`. `delete_page` is idempotent on pages absent from the pool,
    /// so replaying an already-applied free is harmless.
    fn redo_overflow_free(&self, record: &WalRecord) -> Result<()> {
        let data = record.main_data.ok_or_else(|| {
            WalError::CorruptedLog("OverflowFree record missing main data".to_string())
        })?;
        if data.len() % 8 != 0 {
            return Err(WalError::CorruptedLog(format!(
                "OverflowFree main data length {} is not a multiple of 8",
                data.len()
            ))
            .into());
        }
        for chunk in data.chunks_exact(8) {
            let page_id = u64::from_le_bytes(chunk.try_into().unwrap());
            self.pool.delete_page(page_id)?;
        }
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
                // slot u16, key_len u16, val_len u16, rec_type u8, xmin u64, key, val
                let d = data.expect("Insert record missing data block");
                let slot = u16::from_le_bytes(d[0..2].try_into().unwrap()) as usize;
                let key_len = u16::from_le_bytes(d[2..4].try_into().unwrap()) as usize;
                let val_len = u16::from_le_bytes(d[4..6].try_into().unwrap()) as usize;
                let rec_type = d[6];
                let xmin = u64::from_le_bytes(d[7..15].try_into().unwrap());
                let key = &d[15..15 + key_len];
                // `val` is the stored payload verbatim — inline value bytes or a
                // serialized OverflowDescriptor. Replay via insert_raw so the
                // record's rec_type is preserved (a typed insert would force
                // REC_TYPE_INLINE and corrupt overflow records on redo).
                let val = &d[15 + key_len..15 + key_len + val_len];
                let mut m = LeafPageMutator::<K, V>::new(page);
                m.insert_raw(slot, key, val, rec_type)?;
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
            (WalRecordType::MarkHalfDead, 0) => {
                // Single page block: mark the target leaf half-dead.
                crate::page::set_half_dead(page);
            }
            (WalRecordType::UnlinkPage, _) => {
                // Each UnlinkPage block carries a role byte followed by that
                // page's redo payload, so block order can stay flexible.
                let d = data.expect("UnlinkPage record missing data block");
                match d[0] {
                    UNLINK_ROLE_LEFT => {
                        let new_rightlink = u64::from_le_bytes(d[1..9].try_into().unwrap());
                        LeafPageMutator::<K, V>::new(page)
                            .set_rightlink((new_rightlink != 0).then_some(new_rightlink));
                    }
                    UNLINK_ROLE_RIGHT => {
                        let new_prev = u64::from_le_bytes(d[1..9].try_into().unwrap());
                        LeafPageMutator::<K, V>::new(page)
                            .set_prev_page((new_prev != 0).then_some(new_prev));
                    }
                    UNLINK_ROLE_PARENT => {
                        let remove_index = u16::from_le_bytes(d[1..3].try_into().unwrap()) as usize;
                        let keep = if d[3] == UNLINK_KEEP_RIGHT {
                            ChildSide::Right
                        } else {
                            ChildSide::Left
                        };
                        InternalPageMutator::<K>::new(page).remove_key_at(remove_index, keep);
                    }
                    role => panic!("unknown UnlinkPage block role {role}"),
                }
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
            (WalRecordType::NewRoot, 2) => {
                // left_child block: clear INCOMPLETE_SPLIT on the old root.
                crate::page::clear_incomplete_split(page);
            }
            _ => {
                // No physiological apply for this (type, block). FPI blocks never reach here.
            }
        }
        Ok(())
    }
}
