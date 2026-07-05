use engine::Engine;
use storage::wal::{WalIterator, WalRecordType};
use tempfile::TempDir;

type TestEngine = Engine<&'static [u8], &'static [u8]>;

fn fresh_engine() -> (TempDir, TestEngine) {
    let dir = TempDir::new().unwrap();
    let engine = TestEngine::create(dir.path()).unwrap();
    (dir, engine)
}

#[test]
fn test_checkpoint_record_written() {
    let (dir, engine) = fresh_engine();

    // Transaction IDs are allocated sequentially starting at 1.
    // 1. A committed transaction (txn_id = 1)
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();

    // 2. An aborted transaction (txn_id = 2)
    {
        let mut txn = engine.begin();
        txn.insert(&b"k2".as_slice(), &b"v2".as_slice()).unwrap();
        // drop aborts
    }

    // 3. An active transaction (txn_id = 3) — kept alive across checkpoint
    let _active_handle = engine.begin();
    let expected_active_txn_id: u64 = 3;
    let expected_aborted_txn_id: u64 = 2;

    engine.checkpoint().expect("Checkpoint failed");

    // Read back the WAL and find the Checkpoint record
    let wal_dir = dir.path().join("wal");
    let mut iter = WalIterator::new(wal_dir).expect("Failed to open WalIterator");

    let mut found_checkpoint = false;
    while let Some(record) = iter.next_record() {
        let record = record.expect("Failed to read record");
        if record.entry_type == WalRecordType::Checkpoint {
            found_checkpoint = true;
            let data = record.main_data.expect("Checkpoint should have main data");

            // Fixed-size header = 5 × 8 = 40 bytes
            // + active_len (4) + aborted_len (4) = 48 minimum
            assert!(data.len() >= 48, "Checkpoint payload too small");

            // Parse the fixed fields
            let _redo_point = u64::from_le_bytes(data[0..8].try_into().unwrap());
            let next_txn_id = u64::from_le_bytes(data[8..16].try_into().unwrap());

            // 1 committed + 1 aborted + 1 active = 3 txns consumed,
            // so next_txn_id should be at least 4.
            assert!(
                next_txn_id >= 4,
                "next_txn_id should be >= 4, got {next_txn_id}"
            );

            let _vacuum_horizon = u64::from_le_bytes(data[16..24].try_into().unwrap());
            let _root_pid = u64::from_le_bytes(data[24..32].try_into().unwrap());
            let _next_page_id = u64::from_le_bytes(data[32..40].try_into().unwrap());

            // Parse active set
            let active_len = u32::from_le_bytes(data[40..44].try_into().unwrap()) as usize;
            let mut offset = 44;
            let mut active_txns = Vec::new();
            for _ in 0..active_len {
                active_txns.push(u64::from_le_bytes(
                    data[offset..offset + 8].try_into().unwrap(),
                ));
                offset += 8;
            }

            assert_eq!(active_len, 1, "expected exactly 1 active transaction");
            assert!(
                active_txns.contains(&expected_active_txn_id),
                "active set should contain txn {expected_active_txn_id}, \
                 got {active_txns:?}"
            );

            // Parse pinned-aborted set
            let aborted_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let mut aborted_txns = Vec::new();
            for _ in 0..aborted_len {
                aborted_txns.push(u64::from_le_bytes(
                    data[offset..offset + 8].try_into().unwrap(),
                ));
                offset += 8;
            }

            assert!(aborted_len >= 1, "expected at least 1 aborted transaction");
            assert!(
                aborted_txns.contains(&expected_aborted_txn_id),
                "pinned-aborted set should contain txn \
                 {expected_aborted_txn_id}, got {aborted_txns:?}"
            );

            // Verify we consumed all bytes
            assert_eq!(
                offset,
                data.len(),
                "unparsed trailing bytes in checkpoint payload"
            );
        }
    }

    assert!(found_checkpoint, "Checkpoint record not found in WAL");
}

#[test]
fn test_checkpoint_recovery() {
    let (dir, engine) = fresh_engine();

    // 1. Insert and commit a record
    engine.insert(&b"k1".as_slice(), &b"v1".as_slice()).unwrap();

    // 2. Start an active transaction and insert, but do NOT commit
    let mut active_txn = engine.begin();
    active_txn
        .insert(&b"k2".as_slice(), &b"v2".as_slice())
        .unwrap();

    // 3. Start a transaction, insert, and abort (to test pinned_aborted seeding)
    let mut aborted_txn = engine.begin();
    aborted_txn
        .insert(&b"k3".as_slice(), &b"v3".as_slice())
        .unwrap();
    drop(aborted_txn);

    // 4. Force checkpoint
    engine.checkpoint().expect("Checkpoint failed");

    // 5. Simulate a ghost transaction: does work, but page flushes happen,
    //    so its WAL records are below the redo point. Then it crashes.
    let mut ghost_txn = engine.begin();
    ghost_txn
        .insert(&b"k4".as_slice(), &b"v4".as_slice())
        .unwrap();
    engine.flush_all_pages().unwrap();
    engine.checkpoint().expect("Checkpoint 2 failed");

    // "Crash": Drop the engine completely without a clean shutdown
    drop(active_txn);
    drop(ghost_txn);
    drop(engine);

    // 6. Recover the engine
    let engine = TestEngine::open(dir.path()).expect("Recovery failed");

    // 7. Assertions

    // k1 should be visible (committed)
    let v1 = engine.get(&b"k1".as_slice()).unwrap();
    assert_eq!(v1, Some(b"v1".to_vec()));

    // k2 should NOT be visible (active_txn crash victim)
    let v2 = engine.get(&b"k2".as_slice()).unwrap();
    assert_eq!(v2, None, "k2 should be aborted as a crash victim");

    // k3 should NOT be visible (aborted_txn seeded from checkpoint)
    let v3 = engine.get(&b"k3".as_slice()).unwrap();
    assert_eq!(v3, None, "k3 should be aborted from pinned_aborted seed");

    // k4 should NOT be visible (ghost_txn caught by active_txns union)
    let v4 = engine.get(&b"k4".as_slice()).unwrap();
    assert_eq!(
        v4, None,
        "k4 should be aborted as a ghost transaction crash victim"
    );
}
