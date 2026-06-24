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

