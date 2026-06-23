//! Top-level database engine facade.
//!
//! `Engine` owns the WAL and transaction manager, so it is responsible for the
//! commit-observability rule: a write transaction is marked committed only
//! after its commit WAL record has been appended and flushed. Public autocommit
//! methods propagate that durability error instead of acknowledging success.
//!
//! ## On-Disk Layout
//!
//! Each engine directory stores table pages in `data.db` and WAL segments in
//! `wal/`. Opening an engine constructs the WAL manager, runs crash recovery
//! from that segment directory, then opens the B+Tree index once page 0 and the
//! transaction CLOG have been rebuilt.

use common::{EngineError, Key, Value};
use db_core::transaction_manager::TransactionManager;
use std::path::Path;
use std::sync::Arc;
use storage::buffer_pool::BufferPoolManager;
use storage::disk::DiskManager;
use storage::index::BTreeIndex;
use storage::page::PAGE_SIZE;
use storage::recovery::RecoveryManager;
use storage::wal::Wal;

use crate::txn::TxnHandle;

/// Database facade for a single typed key/value index.
///
/// The engine owns the shared [`Wal`], [`BufferPoolManager`], [`DiskManager`],
/// and [`TransactionManager`]. User-facing operations either run as autocommit
/// transactions or through [`TxnHandle`], but all writes pass through the same
/// commit path so WAL durability is established before a transaction becomes
/// visible.
pub struct Engine<K, V>
where
    K: Key,
    V: Value,
{
    pub(crate) index: Arc<BTreeIndex<K, V>>,
    #[allow(dead_code)]
    pub(crate) wal: Arc<Wal>,
    #[allow(dead_code)]
    pub(crate) disk_manager: Arc<DiskManager>,
    #[allow(dead_code)]
    pub(crate) buffer_pool: Arc<BufferPoolManager>,
    pub(crate) transaction_manager: Arc<TransactionManager>,
}

impl<K, V> Engine<K, V>
where
    K: Key,
    V: Value,
{
    /// Creates a new database directory.
    ///
    /// This initializes `data.db`, creates the segmented `wal/` directory, and
    /// writes the initial metadata/root pages. Creating over an existing
    /// database returns [`EngineError::AlreadyExists`].
    pub fn create(dir_path: impl AsRef<Path>) -> Result<Engine<K, V>, EngineError> {
        let path = dir_path.as_ref();
        std::fs::create_dir_all(path)?;
        let exist = path.join("data.db").exists();
        if exist {
            return Err(EngineError::AlreadyExists);
        }
        // initialize disk manager
        let disk_manager = Arc::new(DiskManager::new(path.join("data.db"), PAGE_SIZE)?);
        // initialize WAL shared by the index and buffer pool.
        let wal = Arc::new(Wal::new(path.join("wal"))?);
        let buffer_pool = Arc::new(BufferPoolManager::new(
            Arc::clone(&disk_manager),
            Arc::clone(&wal),
        ));
        let transaction_manager = Arc::new(TransactionManager::new());
        let (index, _root) = BTreeIndex::create(Arc::clone(&buffer_pool), Arc::clone(&wal))?;
        // index needs Arc because vacuum will later clone it.
        let index = Arc::new(index);
        // TODO! spawn checkpoint thread once checkpoint is there
        // TODO! spawn vacuum thread once vacuum is implemented
        Ok(Engine {
            index,
            wal,
            buffer_pool,
            disk_manager,
            transaction_manager,
        })
    }
    /// Opens an existing database.
    ///
    /// The data file is the existence marker. Recovery replays `wal/` before
    /// the index opens so page changes and transaction statuses are restored
    /// before any user query can observe the database.
    pub fn open(dir_path: impl AsRef<Path>) -> Result<Engine<K, V>, EngineError> {
        let path = dir_path.as_ref();
        // the data file is the marker that a database lives here
        if !path.join("data.db").exists() {
            return Err(EngineError::NotFound);
        }
        let disk_manager = Arc::new(DiskManager::new(path.join("data.db"), PAGE_SIZE)?);
        // WAL segments live under `<db>/wal`; recovery replays the same directory.
        let wal = Arc::new(Wal::new(path.join("wal"))?);
        let buffer_pool = Arc::new(BufferPoolManager::new(
            Arc::clone(&disk_manager),
            Arc::clone(&wal),
        ));
        let transaction_manager = Arc::new(TransactionManager::new());
        // Recovery must rebuild page 0/root and CLOG before the index opens.
        let recovery = RecoveryManager::new(
            Arc::clone(&buffer_pool),
            path.join("wal"),
            Arc::clone(&transaction_manager),
        );
        recovery.recover::<K, V>()?;

        let index = Arc::new(BTreeIndex::open(
            Arc::clone(&buffer_pool),
            Arc::clone(&wal),
        )?);
        // TODO! spawn checkpoint + vacuum threads
        Ok(Engine {
            index,
            wal,
            buffer_pool,
            disk_manager,
            transaction_manager,
        })
    }

    // implement close() when checkpoint lands.

    // ── PUBLIC API ─────────────────────────────────────────────
    pub fn insert(
        &self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        let mut txn = self.transaction_manager.begin();
        match self.insert_in(&mut txn, key, value) {
            Ok(()) => {
                self.commit(txn)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.abort(txn);
                Err(e)
            }
        }
    }

    pub fn get(&self, key: &K::SelfType<'_>) -> Result<Option<Vec<u8>>, EngineError> {
        // reads still need a transaction: the snapshot from begin() is what
        // makes the read correct; its commit hits the read-only fast path
        let txn = self.transaction_manager.begin();
        match self.get_in(&txn, key) {
            Ok(v) => {
                self.commit(txn)?;
                Ok(v)
            }
            Err(e) => {
                let _ = self.abort(txn);
                Err(e)
            }
        }
    }

    pub fn update(
        &self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(), EngineError> {
        let mut txn = self.transaction_manager.begin();
        match self.update_in(&mut txn, key, value) {
            Ok(()) => {
                self.commit(txn)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.abort(txn);
                Err(e)
            }
        }
    }

    pub fn delete(&self, key: &K::SelfType<'_>) -> Result<(), EngineError> {
        let mut txn = self.transaction_manager.begin();
        match self.delete_in(&mut txn, key) {
            Ok(()) => {
                self.commit(txn)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.abort(txn);
                Err(e)
            }
        }
    }

    pub fn begin(&self) -> TxnHandle<'_, K, V> {
        TxnHandle {
            engine: self,
            txn: Some(self.transaction_manager.begin()),
            poisoned: false,
        }
    }
}
