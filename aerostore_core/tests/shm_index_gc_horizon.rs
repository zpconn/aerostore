use std::sync::Arc;

use aerostore_core::{IndexValue, SecondaryIndex, ShmArena};

#[test]
fn retired_nodes_are_not_reclaimed_before_oldest_active_snapshot_finishes() {
    const NODES: u32 = 4_000;

    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));

    let blocker = shm
        .begin_transaction()
        .expect("failed to create long-lived snapshot blocker");

    for row_id in 0..NODES {
        index.insert(IndexValue::I64(row_id as i64), row_id);
    }
    for row_id in 0..NODES {
        index.remove(&IndexValue::I64(row_id as i64), &row_id);
    }

    let retired_before = index.retired_nodes();
    assert!(
        retired_before > 0,
        "expected retired nodes after deleting all inserted keys"
    );

    let reclaimed_while_blocked = index.collect_garbage_once(usize::MAX);
    assert_eq!(
        reclaimed_while_blocked, 0,
        "GC reclaimed nodes despite an older active snapshot"
    );
    assert_eq!(
        index.reclaimed_nodes(),
        0,
        "reclaimed counter must remain zero while blocker is active"
    );
    assert_eq!(
        index.retired_nodes(),
        retired_before,
        "retired backlog should remain unchanged while blocker is active"
    );

    shm.end_transaction(blocker)
        .expect("failed to release snapshot blocker");

    let mut total_reclaimed = 0_usize;
    for _ in 0..16 {
        let reclaimed = index.collect_garbage_once(usize::MAX);
        total_reclaimed += reclaimed;
        if index.retired_nodes() == 0 {
            break;
        }
    }

    assert!(
        total_reclaimed > 0,
        "expected GC to reclaim retired nodes after blocker completion"
    );
    assert_eq!(
        index.retired_nodes(),
        0,
        "retired backlog should fully drain after horizon advances"
    );
    assert!(
        index.reclaimed_nodes() >= retired_before,
        "reclaimed node counter should cover original retired backlog"
    );
}
