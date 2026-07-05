mod checkpointer;
mod engine;
mod ops;
mod txn;

#[cfg(test)]
mod proptests;

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::checkpointer::Msg;

pub use common::{BufferPoolError, DiskError, EngineError, IndexError, WalError};
pub use common::{Key, Value};
pub use engine::Engine;
pub use txn::TxnHandle;

/// How often the background checkpointer runs. TODO: make configurable.
pub(crate) const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

/// Owning handle for an open database.
///
/// `Db` wraps the shared [`Engine`] and owns the background threads (the
/// checkpointer today; vacuum later). Callers clone `db.engine` into worker
/// threads.
/// # Shutdown
///
/// `Db` is an embedded library and installs **no** signal handler. The caller
/// must call [`Db::close`] for a clean final checkpoint. Skipping `close` is never a correctness bug: the next
/// [`Db::open`] replays the WAL and reconstructs everything; only replay time
/// is lost.
pub struct Db<K, V>
where
    K: Key + Send + Sync + 'static,
    V: Value + Send + Sync + 'static,
{
    /// The database. Clone this into worker threads (`db.engine.clone()`).
    pub engine: Arc<Engine<K, V>>,

    /// `Option` so `shutdown` can `take()` it and run exactly once.
    ckpt_handle: Option<JoinHandle<()>>,
    /// Owner-only; used solely to signal `Shutdown`. Never exposed.
    ckpt_tx: Sender<Msg>,
}

impl<K, V> Db<K, V>
where
    K: Key + Send + Sync + 'static,
    V: Value + Send + Sync + 'static,
{
    /// Creates a fresh database, then starts the background threads.
    ///
    /// Propagates any [`Engine::create`] error (e.g. [`EngineError::AlreadyExists`])
    /// before spawning anything.
    pub fn create(dir_path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let engine = Arc::new(Engine::create(dir_path)?);
        Ok(Self::start(engine))
    }

    /// Opens an existing database, then starts the background threads.
    ///
    /// Propagates a missing/corrupt-database error ([`EngineError::NotFound`],
    /// recovery errors) **before** any thread is spawned.
    pub fn open(dir_path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let engine = Arc::new(Engine::open(dir_path)?);
        Ok(Self::start(engine))
    }

    /// Shared constructor tail. Must run last: the engine is fully built and
    /// recovered here, so a checkpoint may safely fire.
    fn start(engine: Arc<Engine<K, V>>) -> Self {
        let (ckpt_tx, rx) = mpsc::channel();
        let engine_for_ckpt = Arc::clone(&engine);
        let ckpt_handle = thread::Builder::new()
            .name("fluxdb-checkpointer".into())
            .spawn(move || checkpointer::run(engine_for_ckpt, rx, CHECKPOINT_INTERVAL))
            .expect("spawn checkpointer thread");

        // TODO (later PR): spawn the vacuum thread here the same way, storing
        // its own handle + sender; `shutdown` will then join both.

        Db {
            engine,
            ckpt_handle: Some(ckpt_handle),
            ckpt_tx,
        }
    }

    /// Explicit graceful shutdown: signals the checkpointer to run one final
    /// checkpoint and joins it. Surfaces a thread panic as
    /// [`EngineError::BackgroundThreadPanicked`]. Consumes the `Db`.
    pub fn close(mut self) -> Result<(), EngineError> {
        self.shutdown()
        // `self` drops here; `Drop` calls `shutdown` again → no-op (handle taken).
    }

    /// Idempotent: `take()` ensures the signal + join happen at most once, so
    /// `close` followed by `Drop` (or a double `Drop`) is safe.
    fn shutdown(&mut self) -> Result<(), EngineError> {
        if let Some(handle) = self.ckpt_handle.take() {
            // Ignore the send error: if the thread already exited, the channel
            // is closed — we still want to join.
            let _ = self.ckpt_tx.send(Msg::Shutdown);
            handle
                .join()
                .map_err(|_| EngineError::BackgroundThreadPanicked)?;
        }
        Ok(())
    }
}

/// Backstop for callers who forget `close` (or on unwind). Runs the same
/// idempotent shutdown so the final checkpoint still fires on normal exit.
impl<K, V> Drop for Db<K, V>
where
    K: Key + Send + Sync + 'static,
    V: Value + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
