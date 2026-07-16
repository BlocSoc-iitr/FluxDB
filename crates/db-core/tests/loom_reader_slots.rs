#[cfg(loom)]
mod reader_slots {
    use loom::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use loom::thread;

    const OLD_XMIN: usize = 1;
    const NEW_XMIN: usize = 3;
    const EMPTY: usize = usize::MAX;

    fn run_model(reader: fn(&AtomicUsize, &AtomicUsize, &AtomicBool)) {
        loom::model(move || {
            let published_xmin = Arc::new(AtomicUsize::new(OLD_XMIN));
            let slot = Arc::new(AtomicUsize::new(EMPTY));
            let old_version_freed = Arc::new(AtomicBool::new(false));

            let reader_published_xmin = Arc::clone(&published_xmin);
            let reader_slot = Arc::clone(&slot);
            let reader_old_version_freed = Arc::clone(&old_version_freed);
            let reader = thread::spawn(move || {
                reader(
                    &reader_published_xmin,
                    &reader_slot,
                    &reader_old_version_freed,
                );
            });

            let writer_published_xmin = Arc::clone(&published_xmin);
            let writer = thread::spawn(move || {
                writer_published_xmin.store(NEW_XMIN, Ordering::SeqCst);
            });

            let vacuum_published_xmin = Arc::clone(&published_xmin);
            let vacuum_slot = Arc::clone(&slot);
            let vacuum_old_version_freed = Arc::clone(&old_version_freed);
            let vacuum = thread::spawn(move || {
                if vacuum_published_xmin.load(Ordering::SeqCst) != NEW_XMIN {
                    return;
                }

                let pinned = vacuum_slot.load(Ordering::SeqCst);

                if pinned == EMPTY || pinned > OLD_XMIN {
                    vacuum_old_version_freed.store(true, Ordering::SeqCst);
                }
            });

            reader.join().unwrap();
            writer.join().unwrap();
            vacuum.join().unwrap();
        });
    }

    fn pin_low_then_validate(
        published_xmin: &AtomicUsize,
        slot: &AtomicUsize,
        old_version_freed: &AtomicBool,
    ) {
        let pin = published_xmin.load(Ordering::SeqCst);
        slot.store(pin, Ordering::SeqCst);

        let snapshot_xmin = published_xmin.load(Ordering::SeqCst);
        if snapshot_xmin != pin {
            slot.store(snapshot_xmin, Ordering::SeqCst);
            return;
        }

        if snapshot_xmin == OLD_XMIN {
            assert!(
                !old_version_freed.load(Ordering::SeqCst),
                "old version was freed while a snapshot that can need it is live"
            );
        }

        slot.store(EMPTY, Ordering::Release);
    }

    fn pin_high_first(
        published_xmin: &AtomicUsize,
        slot: &AtomicUsize,
        old_version_freed: &AtomicBool,
    ) {
        slot.store(NEW_XMIN, Ordering::SeqCst);

        let snapshot_xmin = published_xmin.load(Ordering::SeqCst);
        if snapshot_xmin == OLD_XMIN {
            assert!(
                !old_version_freed.load(Ordering::SeqCst),
                "old version was freed while a snapshot that can need it is live"
            );
        }

        slot.store(EMPTY, Ordering::Release);
    }

    fn no_initial_pin(
        published_xmin: &AtomicUsize,
        slot: &AtomicUsize,
        old_version_freed: &AtomicBool,
    ) {
        let snapshot_xmin = published_xmin.load(Ordering::SeqCst);
        if snapshot_xmin == OLD_XMIN {
            assert!(
                !old_version_freed.load(Ordering::SeqCst),
                "old version was freed while a snapshot that can need it is live"
            );
        }

        slot.store(EMPTY, Ordering::Release);
    }

    #[test]
    #[ignore = "Phase 2 gate: current model exposes a publish/scan race that must be resolved before replacing readers with slots"]
    fn pin_low_then_validate_protects_reader_snapshot() {
        run_model(pin_low_then_validate);
    }

    #[test]
    fn high_first_pin_is_rejected_by_model() {
        let result = std::panic::catch_unwind(|| run_model(pin_high_first));
        assert!(
            result.is_err(),
            "the model should find an interleaving where high-first pinning frees a needed version"
        );
    }

    #[test]
    fn missing_initial_pin_is_rejected_by_model() {
        let result = std::panic::catch_unwind(|| run_model(no_initial_pin));
        assert!(
            result.is_err(),
            "the model should find an interleaving where omitting the initial pin frees a needed version"
        );
    }
}

#[cfg(not(loom))]
#[test]
fn loom_reader_slot_tests_require_cfg_loom() {
    eprintln!("run with `RUSTFLAGS='--cfg loom' cargo test -p db-core --test loom_reader_slots`");
}
