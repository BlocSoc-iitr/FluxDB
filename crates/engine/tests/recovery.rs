use engine::Engine;
use tempfile::TempDir;
type TestEngine = Engine<&'static [u8], &'static [u8]>;
fn leak(b: &[u8]) -> &'static [u8] { Box::leak(b.to_vec().into_boxed_slice()) }

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
        assert_eq!(e.get(&k).unwrap().as_deref(), Some(&(i * 7).to_be_bytes()[..]));
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