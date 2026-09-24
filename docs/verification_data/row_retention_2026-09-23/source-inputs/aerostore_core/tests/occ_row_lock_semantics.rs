use std::sync::Arc;

use aerostore_core::{OccError, OccTable, ShmArena};

#[test]
fn lock_for_update_forces_competing_tx_fast_abort() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table = OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create OCC table");
    table.seed_row(0, 42_u64).expect("failed to seed row");

    let mut tx1 = table
        .begin_transaction()
        .expect("tx1 begin_transaction should succeed");
    let row_lock = table
        .lock_for_update(&tx1, 0)
        .expect("tx1 lock_for_update should succeed");

    let mut tx2 = table
        .begin_transaction()
        .expect("tx2 begin_transaction should succeed");
    let read_err = table
        .read(&mut tx2, 0)
        .expect_err("tx2 read should fail while tx1 holds row lock");
    assert!(
        matches!(read_err, OccError::SerializationFailure),
        "expected SerializationFailure from competing read, got {read_err}"
    );
    table.abort(&mut tx2).expect("tx2 abort should succeed");

    let mut tx3 = table
        .begin_transaction()
        .expect("tx3 begin_transaction should succeed");
    let write_err = table
        .write(&mut tx3, 0, 43_u64)
        .expect_err("tx3 write should fail while tx1 holds row lock");
    assert!(
        matches!(write_err, OccError::SerializationFailure),
        "expected SerializationFailure from competing write, got {write_err}"
    );
    table.abort(&mut tx3).expect("tx3 abort should succeed");

    drop(row_lock);
    table.abort(&mut tx1).expect("tx1 abort should succeed");
}

#[test]
fn row_lock_releases_after_holder_abort_and_guard_drop() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table = OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create OCC table");
    table.seed_row(0, 7_u64).expect("failed to seed row");

    let mut tx1 = table
        .begin_transaction()
        .expect("tx1 begin_transaction should succeed");
    let row_lock = table
        .lock_for_update(&tx1, 0)
        .expect("tx1 lock_for_update should succeed");

    let mut tx2 = table
        .begin_transaction()
        .expect("tx2 begin_transaction should succeed");
    let lock_err = match table.lock_for_update(&tx2, 0) {
        Ok(_) => panic!("tx2 lock_for_update unexpectedly succeeded while tx1 holds row lock"),
        Err(err) => err,
    };
    assert!(
        matches!(lock_err, OccError::SerializationFailure),
        "expected SerializationFailure from competing lock_for_update, got {lock_err}"
    );
    table.abort(&mut tx2).expect("tx2 abort should succeed");

    table.abort(&mut tx1).expect("tx1 abort should succeed");
    drop(row_lock);

    let mut tx3 = table
        .begin_transaction()
        .expect("tx3 begin_transaction should succeed");
    let tx3_lock = table
        .lock_for_update(&tx3, 0)
        .expect("tx3 should acquire row lock after tx1 abort + guard drop");

    let current = table
        .read(&mut tx3, 0)
        .expect("tx3 read should succeed")
        .expect("seed row should be visible");
    assert_eq!(current, 7_u64, "unexpected row value before tx3 update");

    table
        .write(&mut tx3, 0, current.wrapping_add(1))
        .expect("tx3 write should succeed");

    drop(tx3_lock);
    table.commit(&mut tx3).expect("tx3 commit should succeed");

    let mut tx4 = table
        .begin_transaction()
        .expect("tx4 begin_transaction should succeed");
    let updated = table
        .read(&mut tx4, 0)
        .expect("tx4 read should succeed")
        .expect("row should still exist");
    assert_eq!(updated, 8_u64, "tx3 committed value should be visible");
    table.abort(&mut tx4).expect("tx4 abort should succeed");
}
