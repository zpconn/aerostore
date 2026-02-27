#![cfg(unix)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use aerostore_core::{ShmArena, PROCARRAY_SLOTS};

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
