use crate::engine::Engine;
use common::{EngineError, IndexError, Key, Value};
use db_core::transaction::Transaction;
impl<K, V> Engine<K, V>
where
    K: Key,
    V: Value,
{
    pub(crate) fn insert_in(
        &self,
        txn: &mut Transaction,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        txn.note_write();
        self.index.insert(key, value, txn).map_err(map_conflict)
    }

    pub(crate) fn delete_in(
        &self,
        txn: &mut Transaction,
        key: &K::SelfType<'_>,
    ) -> Result<(), EngineError> {
        txn.note_write();
        self.index.delete(key, txn).map_err(map_conflict)
    }

    /// Reads take `&Transaction` — they never set the wrote-flag.
    pub(crate) fn get_in(
        &self,
        txn: &Transaction,
        key: &K::SelfType<'_>,
    ) -> Result<Option<Vec<u8>>, EngineError> {
        self.index.get(key, txn).map_err(map_conflict)
    }

    pub(crate) fn update_in(
        &self,
        txn: &mut Transaction,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        txn.note_write();
        self.index.update(key, value, txn).map_err(map_conflict)
    }
}

fn map_conflict(e: IndexError) -> EngineError {
    match e {
        IndexError::WriteConflict => EngineError::TransactionConflict,
        other => other.into(),
    }
}
