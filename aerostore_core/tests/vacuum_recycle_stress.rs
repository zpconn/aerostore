use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{spawn_vacuum_daemon, OccError, OccRow, OccTable, ShmArena};

const ARENA_BYTES: usize = 10 << 20;
const TOTAL_UPDATES: usize = 500_000;
const WARMUP_UPDATES: usize = 100_000;
const ROW_ID: usize = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CounterRow {
    value: u64,
}

#[test]
fn vacuum_recycles_dead_versions_and_prevents_oom_in_10mb_arena() {
    let shm = Arc::new(ShmArena::new(ARENA_BYTES).expect("failed to create shared arena"));
    let table = Arc::new(OccTable::<CounterRow>::new(Arc::clone(&shm), 1).expect("create table"));
    table
        .seed_row(ROW_ID, CounterRow { value: 0 })
        .expect("seed row");

    let base_head = shm.chunked_arena().head_offset();
    let _vacuum = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn vacuum daemon");

    apply_increments(table.as_ref(), WARMUP_UPDATES);
    wait_until(
        Duration::from_secs(5),
        || shm.free_list_pushes() > 0,
        "vacuum did not reclaim any row versions during warm-up window",
    );
    apply_increments(table.as_ref(), TOTAL_UPDATES - WARMUP_UPDATES);

    let latest = table
        .latest_value(ROW_ID)
        .expect("latest_value failed")
        .expect("row missing after stress");
    assert_eq!(
        latest.value, TOTAL_UPDATES as u64,
        "final value mismatch after stress updates"
    );

    wait_until(
        Duration::from_secs(5),
        || shm.free_list_pushes() > 0 && shm.free_list_pops() > 0,
        "shared free list counters never advanced; recycle path appears inactive",
    );

    let pushes = shm.free_list_pushes();
    let pops = shm.free_list_pops();
    assert!(
        pushes > 0,
        "vacuum never pushed reclaimed rows into free list"
    );
    assert!(
        pops > 0,
        "allocator never popped recycled rows from shared free list"
    );
    assert!(
        pops <= pushes,
        "free-list pops ({pops}) cannot exceed pushes ({pushes})"
    );

    let end_head = shm.chunked_arena().head_offset();
    let actual_growth = end_head.saturating_sub(base_head) as u64;
    let naive_growth =
        (std::mem::size_of::<OccRow<CounterRow>>() as u64).saturating_mul(TOTAL_UPDATES as u64);
    assert!(
        actual_growth.saturating_mul(2) < naive_growth,
        "allocator high-water mark grew too close to no-vacuum baseline (actual={} naive={})",
        actual_growth,
        naive_growth
    );
}

fn apply_increments(table: &OccTable<CounterRow>, count: usize) {
    for _ in 0..count {
        loop {
            let mut tx = table.begin_transaction().expect("begin_transaction failed");
            let current = match table.read(&mut tx, ROW_ID) {
                Ok(Some(value)) => value,
                Ok(None) => panic!("row disappeared during stress run"),
                Err(OccError::SerializationFailure) => {
                    let _ = table.abort(&mut tx);
                    continue;
                }
                Err(err) => panic!("read failed: {err}"),
            };

            if let Err(err) = table.write(
                &mut tx,
                ROW_ID,
                CounterRow {
                    value: current.value + 1,
                },
            ) {
                match err {
                    OccError::SerializationFailure => {
                        let _ = table.abort(&mut tx);
                        continue;
                    }
                    other => panic!("write failed: {other}"),
                }
            }

            match table.commit(&mut tx) {
                Ok(1) => break,
                Ok(other) => panic!("expected exactly one committed write, got {}", other),
                Err(OccError::SerializationFailure) => {
                    continue;
                }
                Err(err) => panic!("commit failed: {err}"),
            }
        }
    }
}

fn wait_until(mut timeout: Duration, mut condition: impl FnMut() -> bool, failure_msg: &str) {
    let poll = Duration::from_millis(50);
    let start = Instant::now();
    while timeout > Duration::ZERO {
        if condition() {
            return;
        }
        std::thread::sleep(poll);
        timeout = timeout.saturating_sub(poll);
    }
    panic!("{} (elapsed={:?})", failure_msg, start.elapsed());
}
