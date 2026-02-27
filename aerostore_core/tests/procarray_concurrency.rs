#![cfg(unix)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use aerostore_core::{OccError, OccTable, SharedWalRing, ShmArena, WalRingCommit, PROCARRAY_SLOTS};

#[test]
fn procarray_thread_stress_begin_end_and_snapshot_consistency() {
    const WORKERS: usize = 64;
    const ITERATIONS: usize = 20_000;

    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let stop_snapshotter = Arc::new(AtomicBool::new(false));
    let claimed = Arc::new(AtomicU64::new(0));

    let snapshotter = {
        let shm = Arc::clone(&shm);
        let stop = Arc::clone(&stop_snapshotter);
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let snapshot = shm.create_snapshot();
                assert!(snapshot.xmin <= snapshot.xmax);
                assert!(snapshot.len() <= PROCARRAY_SLOTS);
                for txid in snapshot.in_flight_txids() {
                    assert_ne!(*txid, 0, "in-flight txid must never be zero");
                    assert!(
                        *txid < snapshot.xmax,
                        "in-flight txid must be < xmax (txid={}, xmax={})",
                        txid,
                        snapshot.xmax
                    );
                }
                thread::yield_now();
            }
        })
    };

    let mut workers = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let shm = Arc::clone(&shm);
        let claimed = Arc::clone(&claimed);
        workers.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let reg = shm
                    .begin_transaction()
                    .expect("expected free ProcArray slot");
                claimed.fetch_add(1, Ordering::AcqRel);
                shm.end_transaction(reg)
                    .expect("end_transaction must release owned slot");
            }
        }));
    }

    for worker in workers {
        worker.join().expect("worker thread panicked");
    }

    stop_snapshotter.store(true, Ordering::Release);
    snapshotter.join().expect("snapshotter thread panicked");

    let final_snapshot = shm.create_snapshot();
    assert_eq!(
        final_snapshot.len(),
        0,
        "all procarray slots must be released after stress run"
    );
    assert_eq!(
        claimed.load(Ordering::Acquire),
        (WORKERS * ITERATIONS) as u64,
        "expected every transaction begin to be claimed"
    );
}

#[test]
fn procarray_forked_processes_release_all_slots() {
    const CHILDREN: usize = 4;
    const ITERATIONS: usize = 10_000;

    let shm = ShmArena::new(8 << 20).expect("failed to create shared arena");
    let mut pids = Vec::with_capacity(CHILDREN);

    for _ in 0..CHILDREN {
        // SAFETY:
        // Called from test process to create child worker processes.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

        if pid == 0 {
            let mut code = 0_i32;
            for _ in 0..ITERATIONS {
                match shm.begin_transaction() {
                    Ok(reg) => {
                        if shm.end_transaction(reg).is_err() {
                            code = 11;
                            break;
                        }
                    }
                    Err(_) => {
                        code = 10;
                        break;
                    }
                }
            }
            // SAFETY:
            // Child exits without unwinding.
            unsafe { libc::_exit(code) };
        }

        pids.push(pid);
    }

    for pid in pids {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // Waiting for child PID returned by fork.
        let waited = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
        assert_eq!(
            waited,
            pid,
            "waitpid failed: {}",
            std::io::Error::last_os_error()
        );
        assert!(libc::WIFEXITED(status), "child did not exit cleanly");
        assert_eq!(
            libc::WEXITSTATUS(status),
            0,
            "child exited with non-zero code"
        );
    }

    let snapshot = shm.create_snapshot();
    assert_eq!(
        snapshot.len(),
        0,
        "forked workers left dangling active txids in ProcArray"
    );
}

#[test]
fn procarray_slots_release_under_occ_conflicts_and_ring_backpressure() {
    const WORKERS: usize = 16;
    const ITERATIONS: usize = 400;
    const RING_SLOTS: usize = 8;
    const RING_SLOT_BYTES: usize = 128;

    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to create shared arena"));
    let table =
        Arc::new(OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create occ table"));
    table
        .seed_row(0, 0_u64)
        .expect("failed to seed contention row");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create pressure ring");
    let stop_consumer = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));

    let consumer = {
        let ring = ring.clone();
        let stop = Arc::clone(&stop_consumer);
        let consumed = Arc::clone(&consumed);
        thread::spawn(move || loop {
            match ring.pop_bytes() {
                Ok(Some(payload)) => {
                    let _decoded = aerostore_core::deserialize_commit_record(payload.as_slice())
                        .expect("consumer failed to deserialize ring payload");
                    consumed.fetch_add(1, Ordering::AcqRel);
                    thread::sleep(std::time::Duration::from_micros(250));
                }
                Ok(None) => {
                    if stop.load(Ordering::Acquire) {
                        let empty = ring.is_empty().expect("failed to query ring empty state");
                        if empty {
                            break;
                        }
                    }
                    thread::yield_now();
                }
                Err(err) => panic!("ring consumer failed: {}", err),
            }
        })
    };

    let successful_commits = Arc::new(AtomicU64::new(0));
    let serialization_failures = Arc::new(AtomicU64::new(0));
    let start_barrier = Arc::new(std::sync::Barrier::new(WORKERS));

    let mut workers = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let table = Arc::clone(&table);
        let ring = ring.clone();
        let successful_commits = Arc::clone(&successful_commits);
        let serialization_failures = Arc::clone(&serialization_failures);
        let start_barrier = Arc::clone(&start_barrier);

        workers.push(thread::spawn(move || {
            start_barrier.wait();
            for _ in 0..ITERATIONS {
                let mut tx = table
                    .begin_transaction()
                    .expect("begin_transaction failed under contention");
                let current = table
                    .read(&mut tx, 0)
                    .expect("contention read failed")
                    .expect("contention row unexpectedly missing");
                std::thread::yield_now();
                std::thread::sleep(std::time::Duration::from_micros(20));

                table
                    .write(&mut tx, 0, current + 1)
                    .expect("contention write failed");

                match table.commit_with_record(&mut tx) {
                    Ok(record) => {
                        let wal_record = WalRingCommit::from(&record);
                        ring.push_commit_record(&wal_record)
                            .expect("ring push failed under pressure");
                        successful_commits.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(OccError::SerializationFailure) => {
                        serialization_failures.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(other) => panic!("unexpected OCC error: {}", other),
                }
            }
        }));
    }

    for worker in workers {
        worker.join().expect("contention worker panicked");
    }

    stop_consumer.store(true, Ordering::Release);
    consumer.join().expect("ring consumer panicked");

    let committed = successful_commits.load(Ordering::Acquire);
    let conflicts = serialization_failures.load(Ordering::Acquire);
    let consumed_total = consumed.load(Ordering::Acquire);

    assert!(committed > 0, "expected at least one successful OCC commit");
    assert!(
        conflicts > 0,
        "expected at least one serialization failure under heavy contention"
    );
    assert_eq!(
        consumed_total, committed,
        "consumer must observe every successful commit without ring loss"
    );

    let snapshot = shm.create_snapshot();
    assert_eq!(
        snapshot.len(),
        0,
        "all ProcArray slots must be released even when OCC commits fail under pressure"
    );
}
