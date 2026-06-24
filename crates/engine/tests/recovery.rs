use engine::Engine;
use tempfile::TempDir;
type TestEngine = Engine<&'static [u8], &'static [u8]>;
fn leak(b: &[u8]) -> &'static [u8] {
    Box::leak(b.to_vec().into_boxed_slice())
}

#[test]
fn split_sized_survival() {
    let dir = TempDir::new().unwrap();
    {
        let e = TestEngine::create(dir.path()).unwrap();
        for i in 0u32..500 {
            let k = leak(&i.to_be_bytes());
            let v = leak(&(i * 7).to_be_bytes());
            e.insert(&k, &v).unwrap();
        }
    } // crash: drop the engine
    let e = TestEngine::open(dir.path()).unwrap();
    for i in 0u32..500 {
        let k = leak(&i.to_be_bytes());
        assert_eq!(
            e.get(&k).unwrap().as_deref(),
            Some(&(i * 7).to_be_bytes()[..])
        );
    }
}

#[test]
fn delete_survives_crash() {
    let dir = TempDir::new().unwrap();
    {
        let e = TestEngine::create(dir.path()).unwrap();
        e.insert(&leak(b"k"), &leak(b"v")).unwrap();
        e.delete(&leak(b"k")).unwrap();
    } // crash
    let e = TestEngine::open(dir.path()).unwrap();
    assert_eq!(e.get(&leak(b"k")).unwrap(), None);
}

#[test]
fn update_survives_crash() {
    let dir = TempDir::new().unwrap();
    {
        let e = TestEngine::create(dir.path()).unwrap();
        e.insert(&leak(b"k"), &leak(b"v1")).unwrap();
        e.update(&leak(b"k"), &leak(b"v2")).unwrap();
    } // crash
    let e = TestEngine::open(dir.path()).unwrap();
    assert_eq!(e.get(&leak(b"k")).unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn double_recovery_is_idempotent() {
    let dir = TempDir::new().unwrap();

    let e = TestEngine::create(dir.path()).unwrap();
    for i in 0u32..300 {
        let k = leak(&i.to_be_bytes());
        e.insert(&k, &k).unwrap();
    }
    // crash
    let e = TestEngine::open(dir.path()).unwrap();
    for i in 0u32..300 {
        let k = leak(&i.to_be_bytes());
        assert_eq!(e.get(&k).unwrap().as_deref(), Some(&i.to_be_bytes()[..]));
    }
    // drop — no new writes
    let e = TestEngine::open(dir.path()).unwrap();
    for i in 0u32..300 {
        let k = leak(&i.to_be_bytes());
        assert_eq!(e.get(&k).unwrap().as_deref(), Some(&i.to_be_bytes()[..]));
    }
}

#[test]
fn torn_tail_truncates_last_record() {
    let dir = TempDir::new().unwrap();

    let e = TestEngine::create(dir.path()).unwrap();
    for i in 0u32..20 {
        let k = leak(&i.to_be_bytes());
        e.insert(&k, &k).unwrap();
    } // crash

    let seg = std::fs::read_dir(dir.path().join("wal"))
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            p.is_file().then_some(p)
        })
        .max()
        .unwrap();
    let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
    let len = f.metadata().unwrap().len();
    f.set_len(len - 1).unwrap();
    f.sync_all().unwrap();

    // reopen: Wal::new truncates the torn tail; recovery sees Insert(19) but no Commit ⇒ aborts it.
    let e = TestEngine::open(dir.path()).unwrap();
    for i in 0u32..19 {
        let k = leak(&i.to_be_bytes());
        assert!(e.get(&k).unwrap().is_some());
    }
    assert!(e.get(&leak(&19u32.to_be_bytes())).unwrap().is_none()); // last record truncated
}
