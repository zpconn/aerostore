use std::sync::{Arc, Barrier};
use std::thread;

use aerostore_core::{OccError, OccTable, ShmArena};

#[test]
fn ssi_rejects_write_skew_and_returns_serialization_failure() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table =
        Arc::new(OccTable::<bool>::new(Arc::clone(&shm), 2).expect("failed to create OCC table"));

    table
        .seed_row(0, true)
        .expect("failed to seed row 0 as on-call");
    table
        .seed_row(1, true)
        .expect("failed to seed row 1 as on-call");

    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
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
            table.commit(&mut tx)
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
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
            table.commit(&mut tx)
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
        committed, 1,
        "exactly one writer should commit in write-skew scenario"
    );
    assert_eq!(
        serialization_failures, 1,
        "exactly one writer should fail with SerializationFailure"
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
        "serializable validation failed to preserve invariant: at least one row must remain true"
    );
}
