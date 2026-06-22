//! End-to-end lifecycle tests
use common::IndexError;
use engine::{Engine, EngineError};
use tempfile::TempDir;

type TestEngine = Engine<&'static [u8], &'static [u8]>;

fn fresh_engine() -> (TempDir, TestEngine) {
    let dir = TempDir::new().unwrap();
    let engine = TestEngine::create(dir.path()).unwrap();
    (dir, engine)
}

// ── create / open ─────────────────────────────────────────────────────────

#[test]
fn create_twice_fails_with_already_exists() {
    let (dir, engine) = fresh_engine();
    drop(engine);
    assert!(matches!(
        TestEngine::create(dir.path()),
        Err(EngineError::AlreadyExists)
    ));
}

#[test]
fn open_without_database_fails_with_not_found() {
    let dir = TempDir::new().unwrap();
    assert!(matches!(
        TestEngine::open(dir.path()),
        Err(EngineError::NotFound)
    ));
}

#[test]
fn reopen_sees_committed_data() {
    let dir = TempDir::new().unwrap();
    {
        let engine = TestEngine::create(dir.path()).unwrap();
        engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    } // engine dropped — data must survive via the data file
    let engine = TestEngine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
}

// ── autocommit lifecycle ──────────────────────────────────────────────────

#[test]
fn insert_then_get_roundtrip() {
    let (_dir, engine) = fresh_engine();
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(engine.get(&b"missing".as_slice()).unwrap(), None);
}

#[test]
fn update_replaces_value() {
    let (_dir, engine) = fresh_engine();
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    engine.update(&b"k1".as_slice(), &b"v2".as_slice()).unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn delete_makes_key_invisible() {
    let (_dir, engine) = fresh_engine();
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    engine.delete(&b"k1".as_slice()).unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), None);
}

#[test]
fn duplicate_insert_fails_and_aborts_cleanly() {
    let (_dir, engine) = fresh_engine();
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    let err = engine
        .insert(&b"k1".as_slice(), &b"v2".as_slice())
        .unwrap_err();
    assert!(matches!(err, EngineError::Index(IndexError::DuplicateKey)));
    // the failed txn aborted via the envelope — original value untouched
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
}

// ── explicit transactions (TxnHandle) ─────────────────────────────────────

#[test]
fn handle_groups_writes_atomically() {
    let (_dir, engine) = fresh_engine();
    let mut txn = engine.begin();
    txn.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    txn.insert(&b"k2".as_slice(), &b"v2".as_slice()).unwrap();
    txn.commit().unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(engine.get(&b"k2".as_slice()).unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn dropped_handle_aborts_everything() {
    let (_dir, engine) = fresh_engine();
    let mut txn = engine.begin();
    txn.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();
    txn.insert(&b"k2".as_slice(), &b"v2".as_slice()).unwrap();
    drop(txn); // no commit — RAII abort must hide BOTH writes
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), None);
    assert_eq!(engine.get(&b"k2".as_slice()).unwrap(), None);
}

#[test]
fn reads_own_writes_but_invisible_to_others_until_commit() {
    let (_dir, engine) = fresh_engine();
    let mut txn = engine.begin();
    txn.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();

    // the writer sees its own uncommitted write...
    assert_eq!(txn.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
    // ...but a concurrent transaction (autocommit get) does not
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), None);

    txn.commit().unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v1".to_vec()));
}

// ── conflicts & poisoning (wait-die surfaced through the API) ─────────────

#[test]
fn younger_conflicting_txn_dies_and_handle_is_poisoned() {
    let (_dir, engine) = fresh_engine();

    let mut older = engine.begin(); // smaller txn id
    let mut younger = engine.begin(); // larger txn id

    // older claims k1 and holds it (uncommitted)
    older
        .insert(&b"k1".as_slice(), &b"v_old".as_slice())
        .unwrap();

    // younger hits the in-progress version: wait-die says the younger dies
    let err = younger
        .insert(&b"k1".as_slice(), &b"v_young".as_slice())
        .unwrap_err();
    assert!(matches!(err, EngineError::TransactionConflict));

    // the handle is now poisoned: every further op refuses...
    let err = younger
        .insert(&b"k2".as_slice(), &b"v2".as_slice())
        .unwrap_err();
    assert!(matches!(err, EngineError::TransactionConflict));

    // ...and commit must fail too — a dead txn can never appear to succeed
    let err = younger.commit().unwrap_err();
    assert!(matches!(err, EngineError::TransactionConflict));

    // the older transaction is unaffected and commits normally
    older.commit().unwrap();
    assert_eq!(
        engine.get(&b"k1".as_slice()).unwrap(),
        Some(b"v_old".to_vec())
    );
    // nothing from the dead transaction survives
    assert_eq!(engine.get(&b"k2".as_slice()).unwrap(), None);
}

#[test]
fn retry_after_conflict_succeeds_with_fresh_txn() {
    let (_dir, engine) = fresh_engine();

    let mut older = engine.begin();
    let mut younger = engine.begin();
    older.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();

    let err = younger
        .insert(&b"k1".as_slice(), &b"v2".as_slice())
        .unwrap_err();
    assert!(matches!(err, EngineError::TransactionConflict));
    drop(younger); // dead txn aborted

    older.commit().unwrap();

    // the wait-die contract: retry with a FRESH transaction, which now
    // sees the committed k1 and conflicts no more (update, not insert)
    let mut retry = engine.begin();
    retry.update(&b"k1".as_slice(), &b"v2".as_slice()).unwrap();
    retry.commit().unwrap();
    assert_eq!(engine.get(&b"k1".as_slice()).unwrap(), Some(b"v2".to_vec()));
}
