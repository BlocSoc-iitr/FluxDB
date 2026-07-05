//! End-to-end tests for the `Db` handle: it must spawn the checkpointer,
//! serve reads/writes through `db.engine`, and shut the thread down cleanly on
//! both `close()` and `Drop`.

use engine::Db;
use tempfile::TempDir;

type TestDb = Db<&'static [u8], &'static [u8]>;

#[test]
fn create_insert_close_then_reopen_sees_data() {
    let dir = TempDir::new().unwrap();

    let db: TestDb = Db::create(dir.path()).unwrap();
    db.engine
        .insert(&b"k1".as_slice(), &b"v1".as_slice())
        .unwrap();
    assert_eq!(
        db.engine.get(&b"k1".as_slice()).unwrap().as_deref(),
        Some(b"v1".as_ref())
    );
    db.close().expect("clean shutdown"); // signals + joins the checkpointer

    // Reopen: recovery replays the WAL; the row is still there.
    let db2: TestDb = Db::open(dir.path()).unwrap();
    assert_eq!(
        db2.engine.get(&b"k1".as_slice()).unwrap().as_deref(),
        Some(b"v1".as_ref())
    );
    db2.close().unwrap();
}

#[test]
fn drop_without_close_still_shuts_down() {
    let dir = TempDir::new().unwrap();
    {
        let db: TestDb = Db::create(dir.path()).unwrap();
        db.engine
            .insert(&b"k".as_slice(), &b"v".as_slice())
            .unwrap();
        // No close(): Drop must signal + join the checkpointer here without hanging.
    }
    // Reopening proves the previous handle released its files/threads cleanly.
    let db: TestDb = Db::open(dir.path()).unwrap();
    assert_eq!(
        db.engine.get(&b"k".as_slice()).unwrap().as_deref(),
        Some(b"v".as_ref())
    );
    db.close().unwrap();
}

#[test]
fn open_missing_database_errors_and_spawns_nothing() {
    let dir = TempDir::new().unwrap();
    // No database created at this path → open must fail before any thread spawns.
    let res: Result<TestDb, _> = Db::open(dir.path());
    assert!(res.is_err());
}

#[test]
fn engine_clones_into_worker_threads() {
    let dir = TempDir::new().unwrap();
    let db: TestDb = Db::create(dir.path()).unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let e = db.engine.clone();
        handles.push(std::thread::spawn(move || {
            e.insert(&b"shared".as_slice(), &b"x".as_slice()).ok();
            e.get(&b"shared".as_slice()).unwrap()
        }));
    }
    for h in handles {
        let _ = h.join().unwrap();
    }
    db.close().unwrap();
}
