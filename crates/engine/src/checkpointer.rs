//! Background checkpointer thread.
//!
//! One `std::thread` owned by [`crate::Db`]. It runs a checkpoint every
//! [`CHECKPOINT_INTERVAL`](crate::CHECKPOINT_INTERVAL) and exits after a final
//! checkpoint on `Shutdown`.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use common::{Key, Value};
use storage::page::Lsn;

use crate::engine::Engine;

pub(crate) enum Msg {
    /// Run one final checkpoint, then exit.
    Shutdown,
}


enum Wake {
    /// Interval elapsed with no message → periodic checkpoint. The only trigger.
    Tick,
    /// `Shutdown` received, or all senders dropped → final checkpoint + exit.
    Shutdown,
}

fn next_wake(rx: &Receiver<Msg>, interval: Duration) -> Wake {
    match rx.recv_timeout(interval) {
        Err(RecvTimeoutError::Timeout) => Wake::Tick,
        // A dropped sender shouldn't happen with the Db design, but fold it into
        // Shutdown so we never busy-spin on repeated `Disconnected`.
        Ok(Msg::Shutdown) | Err(RecvTimeoutError::Disconnected) => Wake::Shutdown,
    }
}

/// The checkpointer loop. Owns a strong `Arc<Engine>` on purpose: `Db`'s
/// deterministic `join()` guarantees this returns and releases the Arc.
pub(crate) fn run<K, V>(engine: Arc<Engine<K, V>>, rx: Receiver<Msg>, interval: Duration)
where
    K: Key,
    V: Value,
{
    // For the stall warning: remember the last redo point.
    let mut last_redo: Option<Lsn> = None;

    loop {
        match next_wake(&rx, interval) {
            Wake::Tick => do_checkpoint(&engine, &mut last_redo),
            Wake::Shutdown => {
                do_checkpoint(&engine, &mut last_redo); // one final checkpoint
                break;
            }
        }
    }
    // Returning here drops this thread's `Arc<Engine>` clone.
}

/// Runs a checkpoint and warns if the redo point didn't advance. Errors are
/// logged, not propagated: a failed periodic checkpoint must not kill the
/// thread (the next tick retries).
fn do_checkpoint<K, V>(engine: &Engine<K, V>, last_redo: &mut Option<Lsn>)
where
    K: Key,
    V: Value,
{
    let _span = tracing::info_span!("checkpoint").entered();
    match engine.checkpoint() {
        Ok(redo) => {
            if *last_redo == Some(redo) {
                // A frozen redo point across checkpoints means a dirty frame is
                // pinned. 
                tracing::warn!(
                    redo_point = redo,
                    "redo point stalled; WAL will keep growing"
                );
            } else {
                tracing::debug!(redo_point = redo, "checkpoint complete");
            }
            *last_redo = Some(redo);
        }
        Err(e) => tracing::error!(error = %e, "checkpoint failed"),
    }
}
