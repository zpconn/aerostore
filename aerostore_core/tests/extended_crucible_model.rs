//! The simulator is executable specification: test both independent business
//! assertions and its adapter over real row versions/indexes.
#![allow(dead_code)]

#[path = "../benches/extended_crucible/aerostore.rs"]
mod aerostore;
#[path = "../benches/extended_crucible/metrics.rs"]
mod metrics;
#[path = "../benches/extended_crucible/model.rs"]
mod model;

#[test]
fn native_adapter_replays_transactions_and_retains_exact_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let shared = aerostore::Shared::create(
        &dir.path().join("arena"),
        64 << 20,
        &model::initial_records(4),
    )
    .unwrap();
    let mut actual = aerostore::Adapter::new(&shared);
    let mut reference = model::ReferenceStore::new(4);
    for phase in model::generate_trace(4, 2, 19) {
        for message in &phase.messages {
            let expected = model::execute_message(&mut reference, message).unwrap();
            let observed = model::execute_message(&mut actual, message).unwrap();
            assert_eq!(
                observed, expected,
                "{} message {}",
                phase.name, message.sequence
            );
            // This test validates row/index semantics; the benchmark supplies
            // the real WAL writer process. Drain bounded async records here.
            while shared.ring.pop_bytes().unwrap().is_some() {}
        }
        let rows = shared.snapshot().unwrap();
        assert_eq!(rows, reference.snapshot(), "{}", phase.name);
        model::validate_snapshot(&rows).unwrap();
        aerostore_core::run_vacuum_pass(&shared.table).unwrap();
    }
    drop(actual);
    shared.audit().unwrap();
}

#[test]
fn adapter_rejects_out_of_transaction_index_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let shared = aerostore::Shared::create(
        &dir.path().join("arena"),
        32 << 20,
        &model::initial_records(1),
    )
    .unwrap();
    let phases = model::generate_trace(1, 1, 3);
    let mut store = aerostore::Adapter::new(&shared);
    model::execute_message(&mut store, &phases[0].messages[0]).unwrap();
    while shared.ring.pop_bytes().unwrap().is_some() {}
    for row in shared
        .snapshot()
        .unwrap()
        .iter()
        .filter(|r| r.active && r.kind == model::FLIGHT)
    {
        // The old postcommit protocol permitted these removals. Bound native
        // indexes now reject the attempted corruption before it can hide rows.
        for (index, key) in [(0, row.callsign), (1, row.tail)] {
            assert!(matches!(
                shared.indexes[index].try_remove(&aerostore_core::IndexValue::I64(key), &row.id),
                Err(aerostore_core::ShmIndexError::ManagedMutation { .. })
            ));
        }
    }
    model::execute_message(&mut store, &phases[1].messages[0]).unwrap();
    while shared.ring.pop_bytes().unwrap().is_some() {}
    shared.audit().unwrap();
}
