use std::sync::Arc;

use aerostore_core::{OccTable, ShmArena};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TinyRow {
    value: u64,
}

#[test]
fn rollback_to_savepoint_reuses_abandoned_allocations() {
    let shm = Arc::new(ShmArena::new(256 << 10).expect("failed to create shared arena"));
    let table = OccTable::<TinyRow>::new(Arc::clone(&shm), 1).expect("failed to create occ table");
    table
        .seed_row(0, TinyRow { value: 0 })
        .expect("failed to seed row");

    let base_head = shm.chunked_arena().head_offset();
    const LOOPS: usize = 8_000;

    for i in 0..LOOPS {
        let mut tx = table.begin_transaction().expect("begin_transaction failed");
        table.savepoint(&mut tx, "sp").expect("savepoint failed");
        table
            .write(
                &mut tx,
                0,
                TinyRow {
                    value: i as u64 + 1,
                },
            )
            .expect("write failed");
        table
            .rollback_to(&mut tx, "sp")
            .expect("rollback_to failed");

        let observed = table
            .read(&mut tx, 0)
            .expect("read after rollback failed")
            .expect("seeded row unexpectedly missing");
        assert_eq!(
            observed.value, 0,
            "rollback_to must restore the pre-savepoint row view"
        );

        table.abort(&mut tx).expect("abort failed");
    }

    let end_head = shm.chunked_arena().head_offset();
    let consumed = end_head.saturating_sub(base_head);
    assert!(
        consumed <= 1024,
        "abandoned savepoint writes were not reclaimed (head grew by {} bytes)",
        consumed
    );
}

#[test]
fn nested_savepoint_rollbacks_restore_pending_state() {
    let shm = Arc::new(ShmArena::new(256 << 10).expect("failed to create shared arena"));
    let table = OccTable::<TinyRow>::new(Arc::clone(&shm), 1).expect("failed to create occ table");
    table
        .seed_row(0, TinyRow { value: 7 })
        .expect("failed to seed row");

    let mut tx = table.begin_transaction().expect("begin_transaction failed");

    table
        .savepoint(&mut tx, "outer")
        .expect("outer savepoint failed");
    table
        .write(&mut tx, 0, TinyRow { value: 10 })
        .expect("outer write failed");
    table
        .savepoint(&mut tx, "inner")
        .expect("inner savepoint failed");
    table
        .write(&mut tx, 0, TinyRow { value: 20 })
        .expect("inner write failed");
    table
        .write(&mut tx, 0, TinyRow { value: 30 })
        .expect("inner write(2) failed");

    let before_rollback = table
        .read(&mut tx, 0)
        .expect("read before rollback failed")
        .expect("row missing before rollback");
    assert_eq!(before_rollback.value, 30);

    table
        .rollback_to(&mut tx, "inner")
        .expect("rollback_to inner failed");
    let after_inner = table
        .read(&mut tx, 0)
        .expect("read after inner rollback failed")
        .expect("row missing after inner rollback");
    assert_eq!(after_inner.value, 10);

    table
        .rollback_to(&mut tx, "outer")
        .expect("rollback_to outer failed");
    let after_outer = table
        .read(&mut tx, 0)
        .expect("read after outer rollback failed")
        .expect("row missing after outer rollback");
    assert_eq!(after_outer.value, 7);

    table.abort(&mut tx).expect("abort failed");
}
