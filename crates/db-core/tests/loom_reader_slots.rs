#[cfg(loom)]
mod reader_slots {
    use loom::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use loom::thread;

    const OLD_XMIN: usize = 1;
    const NEW_XMIN: usize = 3;
    const SETTING_UP: usize = 0;
    const EMPTY: usize = usize::MAX;

    fn run_model(reader: fn(&AtomicUsize, &AtomicUsize, &AtomicBool, &AtomicUsize)) {
        loom::model(move || {
            let published_xmin = Arc::new(AtomicUsize::new(OLD_XMIN));
            let slot = Arc::new(AtomicUsize::new(EMPTY));
            let old_version_freed = Arc::new(AtomicBool::new(false));
            let reader_epoch = Arc::new(AtomicUsize::new(0));

            let reader = {
                let published_xmin = Arc::clone(&published_xmin);
                let slot = Arc::clone(&slot);
                let old_version_freed = Arc::clone(&old_version_freed);
                let reader_epoch = Arc::clone(&reader_epoch);
                thread::spawn(move || {
                    reader(&published_xmin, &slot, &old_version_freed, &reader_epoch)
                })
            };

            let writer = {
                let published_xmin = Arc::clone(&published_xmin);
                thread::spawn(move || published_xmin.store(NEW_XMIN, Ordering::SeqCst))
            };

            let vacuum = {
                let published_xmin = Arc::clone(&published_xmin);
                let slot = Arc::clone(&slot);
                let old_version_freed = Arc::clone(&old_version_freed);
                let reader_epoch = Arc::clone(&reader_epoch);
                thread::spawn(move || {
                    loop {
                        let epoch = reader_epoch.load(Ordering::SeqCst);
                        if epoch & 1 != 0 {
                            thread::yield_now();
                            continue;
                        }
                        if published_xmin.load(Ordering::SeqCst) != NEW_XMIN {
                            return;
                        }
                        let pinned = slot.load(Ordering::SeqCst);
                        if reader_epoch.load(Ordering::SeqCst) != epoch {
                            continue;
                        }
                        if pinned == EMPTY || pinned > OLD_XMIN {
                            old_version_freed.store(true, Ordering::SeqCst);
                        }
                        return;
                    }
                })
            };

            reader.join().unwrap();
            writer.join().unwrap();
            vacuum.join().unwrap();
        });
    }

    fn run_pinned_model() {
        loom::model(|| {
            let published_xmin = Arc::new(AtomicUsize::new(OLD_XMIN));
            let slot = Arc::new(AtomicUsize::new(EMPTY));
            let old_version_freed = Arc::new(AtomicBool::new(false));
            assert_eq!(
                slot.compare_exchange(EMPTY, SETTING_UP, Ordering::SeqCst, Ordering::SeqCst),
                Ok(EMPTY)
            );

            let reader_published_xmin = Arc::clone(&published_xmin);
            let reader_slot = Arc::clone(&slot);
            let reader_old_version_freed = Arc::clone(&old_version_freed);
            let reader = thread::spawn(move || {
                let snapshot_xmin = reader_published_xmin.load(Ordering::SeqCst);
                reader_slot.store(snapshot_xmin, Ordering::SeqCst);
                let check_xmin = reader_published_xmin.load(Ordering::SeqCst);
                if check_xmin == snapshot_xmin && snapshot_xmin == OLD_XMIN {
                    assert!(!reader_old_version_freed.load(Ordering::SeqCst));
                }
            });

            let writer_published_xmin = Arc::clone(&published_xmin);
            let writer = thread::spawn(move || {
                writer_published_xmin.store(NEW_XMIN, Ordering::SeqCst);
            });

            let vacuum_published_xmin = Arc::clone(&published_xmin);
            let vacuum_slot = Arc::clone(&slot);
            let vacuum_old_version_freed = Arc::clone(&old_version_freed);
            let vacuum = thread::spawn(move || {
                if vacuum_published_xmin.load(Ordering::SeqCst) == NEW_XMIN {
                    let pinned = vacuum_slot.load(Ordering::SeqCst);
                    if pinned == EMPTY || pinned > OLD_XMIN {
                        vacuum_old_version_freed.store(true, Ordering::SeqCst);
                    }
                }
            });

            reader.join().unwrap();
            writer.join().unwrap();
            vacuum.join().unwrap();
        });
    }

    fn pin_high_first(
        published_xmin: &AtomicUsize,
        slot: &AtomicUsize,
        old_version_freed: &AtomicBool,
        _epoch: &AtomicUsize,
    ) {
        slot.store(NEW_XMIN, Ordering::SeqCst);
        let snapshot_xmin = published_xmin.load(Ordering::SeqCst);
        if snapshot_xmin == OLD_XMIN {
            assert!(!old_version_freed.load(Ordering::SeqCst));
        }
        slot.store(EMPTY, Ordering::SeqCst);
    }

    fn no_initial_pin(
        published_xmin: &AtomicUsize,
        slot: &AtomicUsize,
        old_version_freed: &AtomicBool,
        _epoch: &AtomicUsize,
    ) {
        let snapshot_xmin = published_xmin.load(Ordering::SeqCst);
        if snapshot_xmin == OLD_XMIN {
            assert!(!old_version_freed.load(Ordering::SeqCst));
        }
        slot.store(EMPTY, Ordering::SeqCst);
    }

    #[test]
    fn setup_floor_then_load_protects_reader_snapshot() {
        run_pinned_model();
    }

    #[test]
    fn high_first_pin_is_rejected_by_model() {
        let result = std::panic::catch_unwind(|| run_model(pin_high_first));
        assert!(result.is_err());
    }

    #[test]
    fn missing_initial_pin_is_rejected_by_model() {
        let result = std::panic::catch_unwind(|| run_model(no_initial_pin));
        assert!(result.is_err());
    }
}

#[cfg(not(loom))]
#[test]
fn loom_reader_slot_tests_require_cfg_loom() {
    eprintln!("run with `RUSTFLAGS='--cfg loom' cargo test -p db-core --test loom_reader_slots`");
}
