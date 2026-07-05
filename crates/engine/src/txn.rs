//! Transaction lifecycle glue between the engine, WAL, and MVCC state.
//!
//! Write commits append a `Commit` WAL record, flush through that record's LSN,
//! and only then publish the commit to the transaction manager. Aborts append
//! an `Abort` record for recovery diagnostics, but deliberately do not fsync.

use crate::engine::Engine;
use common::{EngineError, Key, Value};
use db_core::transaction::Transaction;

/// A user-facing transaction handle tied to the lifetime of its engine.
///
/// The handle stores a borrowed engine reference rather than an `Arc`, which
/// prevents a transaction from outliving the engine that owns its WAL.
pub struct TxnHandle<'e, K: Key, V: Value> {
    pub(crate) engine: &'e Engine<K, V>,
    pub(crate) txn: Option<Transaction>,
    pub(crate) poisoned: bool,
}

impl<K, V> Engine<K, V>
where
    K: Key,
    V: Value,
{
    /// Commits a transaction.
    ///
    /// Read-only transactions take the fast path and update CLOG directly.
    /// Write transactions are not observable as committed until the commit
    /// record is durable through `flush_up_to(lsn)`.
    pub(crate) fn commit(&self, txn: Transaction) -> Result<(), EngineError> {
        if !txn.wrote_anything() {
            self.transaction_manager.mark_committed(txn.txn_id);
            return Ok(());
        }
        //Starting the RwLock which will be active until the its dropped
        // Ignore poison: lock holds no data, and doing this ensures checkpoint panics never crash user commit
        let _guard = self.status_guard.read().unwrap_or_else(|e| e.into_inner());

        let lsn_res = self.wal.log_commit(txn.txn_id);
        if let Ok(lsn) = lsn_res {
            if let Err(e) = self.wal.flush_up_to(lsn) {
                // If flush fails (e.g. disk full), the transaction record wasn't durably written.
                // We MUST abort it to prevent the txn_id from permanently pinning global_xmin.
                self.transaction_manager.mark_aborted(txn.txn_id);
                return Err(e.into());
            }
            self.transaction_manager.mark_committed(txn.txn_id);
            Ok(())
        } else {
            // Similarly, if appending the commit record fails, abort it.
            self.transaction_manager.mark_aborted(txn.txn_id);
            Err(lsn_res.unwrap_err().into())
        }
    }

    /// Aborts a transaction.
    ///
    /// Abort records are appended for WAL replay, but are never explicitly
    /// fsynced. Losing an abort record is safe because recovery treats an
    /// in-flight transaction without a commit record as aborted.
    pub(crate) fn abort(&self, txn: Transaction) -> Result<(), EngineError> {
        if !txn.wrote_anything() {
            self.transaction_manager.mark_aborted(txn.txn_id);
            return Ok(());
        }
        {
            let _guard = self.status_guard.read().unwrap_or_else(|e| e.into_inner());

            let _ = self.wal.log_abort(txn.txn_id)?;

            self.transaction_manager.mark_aborted(txn.txn_id);
        }
        Ok(())
    }
}

impl<K, V> TxnHandle<'_, K, V>
where
    K: Key,
    V: Value,
{
    pub fn commit(mut self) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::TransactionConflict); // no need to call abort here as self will get dropped and abort is called inside drop impl itself. 
        };
        if let Some(txn) = self.txn.take() {
            self.engine.commit(txn)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::TransactionConflict);
        }
        let txn = self
            .txn
            .as_mut()
            .expect("this shouldn't be none in any possible case"); // Txn is none only on either committed or dropped 
        let res = self.engine.insert_in(txn, key, value);
        if matches!(res, Err(EngineError::TransactionConflict)) {
            self.poisoned = true; //the whole txn is dead
        }
        res
    }

    pub fn delete(&mut self, key: &K::SelfType<'_>) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::TransactionConflict);
        }
        let txn = self
            .txn
            .as_mut()
            .expect("this shouldn't be none in any possible case"); // Txn is none only on either committed or dropped 
        let res = self.engine.delete_in(txn, key);
        if matches!(res, Err(EngineError::TransactionConflict)) {
            self.poisoned = true; //the whole txn is dead
        }
        res
    }

    pub fn update(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::TransactionConflict);
        }
        let txn = self
            .txn
            .as_mut()
            .expect("this shouldn't be none in any possible case"); // Txn is none only on either committed or dropped 
        let res = self.engine.update_in(txn, key, value);
        if matches!(res, Err(EngineError::TransactionConflict)) {
            self.poisoned = true; //the whole txn is dead
        }
        res
    }

    pub fn get(&mut self, key: &K::SelfType<'_>) -> Result<Option<Vec<u8>>, EngineError> {
        if self.poisoned {
            return Err(EngineError::TransactionConflict);
        }
        let txn = self
            .txn
            .as_ref()
            .expect("this shouldn't be none in any possible case"); // Txn is none only on either committed or dropped 
        self.engine.get_in(txn, key)
    }

    /// Explicitly aborts the transaction by consuming the handle.
    ///
    /// The `Drop` implementation performs the actual abort path.
    pub fn abort(self) {}
}

impl<K: Key, V: Value> Drop for TxnHandle<'_, K, V> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            // only way to reach this path is if no one commits so txn still has a value.
            let _ = self.engine.abort(txn); // abort the transaction is no one commits 
        }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    // A checkpoint (exclusive write lock) blocks commits until released.
    // Proves the status_guard correctly serializes checkpoint vs commit.
    // When status_guard is write, every other action is blocked unlike read
    #[test]
    fn test_status_guard_checkpoint_blocks_commits() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let engine = Arc::new(Engine::<u32, u32>::create(path).unwrap());

        let engine_clone = Arc::clone(&engine);

        // Simulate a checkpoint acquiring the exclusive lock
        let checkpoint_guard = engine.status_guard.write().unwrap();

        // Spawn a thread that tries to commit a write transaction
        let commit_thread = thread::spawn(move || {
            let mut txn = engine_clone.begin();
            txn.insert(&1u32, &30u32).unwrap();
            txn.commit().unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        assert!(!commit_thread.is_finished(), "Commit failed to block ");

        drop(checkpoint_guard);
        commit_thread.join().expect("commit thread panicked");
    }

    // Two commits can hold the shared lock simultaneously.
    // When status_guard is read only, other concurrent commit can still happen
    #[test]
    fn concurrent_commits_do_not_deadlock() {
        let dir = tempdir().unwrap();
        let engine = Engine::<u32, u32>::create(dir.path()).unwrap();

        // Simulate another concurrent commit already holding the shared lock
        let _simulated_guard = engine.status_guard.read().unwrap();

        let mut txn = engine.begin();
        txn.insert(&1u32, &30u32).unwrap();

        //read does not block commit
        let result = txn.commit();
        assert!(result.is_ok(), "");
    }
}
