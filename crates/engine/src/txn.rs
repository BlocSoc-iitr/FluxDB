use crate::engine::Engine;
use common::{EngineError, Key, Value};
use db_core::transaction::Transaction;
use storage::wal::WalRecordType;

// Used a reference instead of Arc as reference enforces that transaction does not outlive the engine.
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
    // this function is called once inserts/get/update/delete finishes and returns from index.rs
    pub(crate) fn commit(&self, txn: Transaction) -> Result<(), EngineError> {
        if !txn.wrote_anything() {
            self.transaction_manager.mark_committed(txn.txn_id);
            return Ok(());
        }
        {
            let mut guard = self.wal.lock().unwrap();
            let lsn = guard.append(WalRecordType::Commit, txn.txn_id, &[], None)?; // append commit record to wal
            guard.flush_up_to(lsn)?; // flush wal to disk
            self.transaction_manager.mark_committed(txn.txn_id);
        }
        Ok(())
    }

    // this function is called once inserts/get/update/delete finishes and returns from index.rs
    pub(crate) fn abort(&self, txn: Transaction) -> Result<(), EngineError> {
        if !txn.wrote_anything() {
            self.transaction_manager.mark_aborted(txn.txn_id);
            return Ok(());
        }
        {
            let _ = self
                .wal
                .lock()
                .unwrap()
                .append(WalRecordType::Abort, txn.txn_id, &[], None)?;

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

    pub fn abort(self) {} // drop does the work here as well.
}

impl<K: Key, V: Value> Drop for TxnHandle<'_, K, V> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            // only way to reach this path is if no one commits so txn still has a value.
            let _ = self.engine.abort(txn); // abort the transaction is no one commits 
        }
    }
}
