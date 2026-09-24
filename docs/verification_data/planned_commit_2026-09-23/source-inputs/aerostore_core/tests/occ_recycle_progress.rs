use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{
    spawn_vacuum_daemon_with_config, OccError, OccTable, ShmArena, VacuumDaemonConfig,
};

#[test]
fn allocation_pressure_wakes_vacuum_and_preserves_reclaim_callbacks() {
    let shm = Arc::new(ShmArena::new(2 << 20).unwrap());
    let table = Arc::new(OccTable::new(shm, 1).unwrap());
    table.seed_row(0, 0_u64).unwrap();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&callbacks);
    let config = VacuumDaemonConfig::default()
        .with_interval(Duration::from_secs(60))
        .with_reclaim_callback(Arc::new(move |reclaimed| {
            observed.fetch_add(reclaimed.len(), Ordering::Relaxed);
        }));
    let vacuum = spawn_vacuum_daemon_with_config(Arc::clone(&table), config).unwrap();
    let start = Instant::now();
    // Several times the arena's fresh-row capacity; periodic vacuum cannot
    // provide progress within this test without an allocation-pressure wakeup.
    for value in 1..=120_000_u64 {
        loop {
            let mut tx = table.begin_transaction().unwrap();
            table.write(&mut tx, 0, value).unwrap();
            match table.commit(&mut tx) {
                Ok(1) => break,
                Err(OccError::SerializationFailure) => continue,
                other => panic!("unexpected commit result: {other:?}"),
            }
        }
    }
    vacuum.stop().unwrap();
    assert!(start.elapsed() < Duration::from_secs(10));
    assert_eq!(table.latest_value(0).unwrap(), Some(120_000));
    assert!(callbacks.load(Ordering::Relaxed) > 40_000);
    assert!(table.recycle_telemetry().unwrap().alloc_from_primary > 40_000);
}
