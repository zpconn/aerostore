use std::sync::Arc;

use aerostore_core::{OccError, OccTable, ShmArena};

const ROW_ID: usize = 0;
const UPDATES: usize = 2_000;

#[test]
fn single_thread_recycle_invariants_hold_under_update_reclaim_cycle() {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("create shm"));
    let table = OccTable::<u64>::new(Arc::clone(&shm), 1).expect("create table");
    table.seed_row(ROW_ID, 0).expect("seed row");

    for _ in 0..UPDATES {
        loop {
            let mut tx = table.begin_transaction().expect("begin tx");
            let current = match table.read(&mut tx, ROW_ID) {
                Ok(Some(v)) => v,
                Ok(None) => panic!("row unexpectedly missing"),
                Err(OccError::SerializationFailure) => {
                    let _ = table.abort(&mut tx);
                    continue;
                }
                Err(err) => panic!("read failed: {err}"),
            };

            match table.write(&mut tx, ROW_ID, current + 1) {
                Ok(()) => {}
                Err(OccError::SerializationFailure) => {
                    let _ = table.abort(&mut tx);
                    continue;
                }
                Err(err) => panic!("write failed: {err}"),
            }

            match table.commit(&mut tx) {
                Ok(1) => break,
                Ok(other) => panic!("expected one committed write, got {}", other),
                Err(OccError::SerializationFailure) => continue,
                Err(err) => panic!("commit failed: {err}"),
            }
        }

        let global_xmin = table.current_global_txid();
        let _ = table
            .vacuum_reclaim_once(global_xmin)
            .expect("vacuum reclaim pass");
    }

    let latest = table
        .latest_value(ROW_ID)
        .expect("latest value")
        .expect("row missing");
    assert_eq!(latest, UPDATES as u64);

    let telemetry = table.recycle_telemetry().expect("recycle telemetry");
    let pops = telemetry
        .alloc_from_primary
        .saturating_add(telemetry.alloc_from_probe)
        .saturating_add(telemetry.alloc_from_starved);
    assert!(telemetry.push_success > 0, "expected recycle pushes");
    assert!(pops > 0, "expected recycled row allocations");
    assert!(
        pops <= telemetry.push_success,
        "recycle invariant violated: pops={} pushes={}",
        pops,
        telemetry.push_success
    );
}
