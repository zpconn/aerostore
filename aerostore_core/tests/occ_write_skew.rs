use std::sync::{Arc, Barrier};
use std::thread;

use aerostore_core::{
    spawn_wal_writer_daemon, OccCommitter, OccError, OccTable, SharedWalRing, ShmArena,
};

const RING_SLOTS: usize = 256;
const RING_SLOT_BYTES: usize = 128;

#[test]
fn ssi_rejects_write_skew_and_returns_serialization_failure() {
    run_write_skew_scenario(Mode::Synchronous);
    run_write_skew_scenario(Mode::Asynchronous);
}

#[derive(Clone, Copy)]
enum Mode {
    Synchronous,
    Asynchronous,
}

fn run_write_skew_scenario(mode: Mode) {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared arena"));
    let table =
        Arc::new(OccTable::<bool>::new(Arc::clone(&shm), 2).expect("failed to create OCC table"));

    table
        .seed_row(0, true)
        .expect("failed to seed row 0 as on-call");
    table
        .seed_row(1, true)
        .expect("failed to seed row 1 as on-call");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_{:?}_{}.wal",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        },
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for async skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("tx A begin_transaction failed");
            let row_0 = table.read(&mut tx, 0).expect("tx A read row 0 failed");
            let row_1 = table.read(&mut tx, 1).expect("tx A read row 1 failed");
            assert_eq!(row_0, Some(true));
            assert_eq!(row_1, Some(true));

            barrier.wait();

            table
                .write(&mut tx, 0, false)
                .expect("tx A write row 0 failed");
            committer
                .lock()
                .expect("tx A failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in tx A: {}", other),
                })
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("tx B begin_transaction failed");
            let row_0 = table.read(&mut tx, 0).expect("tx B read row 0 failed");
            let row_1 = table.read(&mut tx, 1).expect("tx B read row 1 failed");
            assert_eq!(row_0, Some(true));
            assert_eq!(row_1, Some(true));

            barrier.wait();

            table
                .write(&mut tx, 1, false)
                .expect("tx B write row 1 failed");
            committer
                .lock()
                .expect("tx B failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in tx B: {}", other),
                })
        })
    };

    let result_a = tx_a.join().expect("tx A thread panicked");
    let result_b = tx_b.join().expect("tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(
        committed,
        1,
        "exactly one writer should commit in write-skew scenario ({:?})",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        }
    );
    assert_eq!(
        serialization_failures,
        1,
        "exactly one writer should fail with SerializationFailure ({:?})",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        }
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let final_row_0 = table
        .read(&mut verify_tx, 0)
        .expect("verify read row 0 failed")
        .expect("row 0 missing after skew test");
    let final_row_1 = table
        .read(&mut verify_tx, 1)
        .expect("verify read row 1 failed")
        .expect("row 1 missing after skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        final_row_0 || final_row_1,
        "serializable validation failed to preserve invariant in mode {:?}",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        }
    );

    if let Some(daemon) = daemon {
        ring.close().expect("failed to close skew test wal ring");
        daemon
            .join()
            .expect("async skew writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}
