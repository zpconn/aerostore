#![cfg(test)]

use super::{FlightIndexes, FlightState};
use aerostore_core::{IndexCompare, IndexValue, OccTable, ShmArena};
use std::sync::Arc;

fn fixture() -> (
    Arc<ShmArena>,
    OccTable<FlightState>,
    FlightIndexes,
    FlightState,
) {
    let shm = Arc::new(ShmArena::new(16 << 20).unwrap());
    let mut table = OccTable::new(Arc::clone(&shm), 1).unwrap();
    let indexes = FlightIndexes::new(Arc::clone(&shm));
    let row = FlightState::from_decoded("UAL123", 37.6189, -122.375, 32000, 450, 100).unwrap();
    table.seed_row(0, row).unwrap();
    indexes.insert_row(0, &row).unwrap();
    indexes.bind(&mut table).unwrap();
    (shm, table, indexes, row)
}

#[test]
fn vacuum_keeps_current_postings_when_key_returns_to_reclaimed_value() {
    let (shm, table, indexes, initial) = fixture();
    for speed in [455, 460, 450] {
        let mut row = initial;
        row.gs = speed;
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, row).unwrap();
        table.commit(&mut tx).unwrap();
    }
    let reclaimed = table
        .vacuum_reclaim_once(aerostore_core::compute_global_xmin(&shm))
        .unwrap();
    assert_eq!(reclaimed.len(), 3);
    assert_eq!(
        indexes.gs.try_entries().unwrap(),
        vec![(IndexValue::I64(450), 0)]
    );
    assert_eq!(
        indexes.altitude.try_entries().unwrap(),
        vec![(IndexValue::I64(32000), 0)]
    );
    let mut tx = table.begin_transaction().unwrap();
    assert_eq!(
        table
            .index_lookup(
                &mut tx,
                &indexes.gs,
                &IndexCompare::Eq(IndexValue::I64(450))
            )
            .unwrap(),
        vec![0]
    );
    table.commit(&mut tx).unwrap();
}

#[test]
fn vacuum_after_delete_never_recreates_or_removes_another_live_posting() {
    let (shm, table, indexes, initial) = fixture();
    let mut deleted = initial;
    deleted.exists = 0;
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, deleted).unwrap();
    table.commit(&mut tx).unwrap();
    table
        .vacuum_reclaim_once(aerostore_core::compute_global_xmin(&shm))
        .unwrap();
    for index in [
        &indexes.flight_id,
        &indexes.altitude,
        &indexes.gs,
        &indexes.lat,
        &indexes.lon,
        &indexes.updated_at,
    ] {
        assert!(index.try_entries().unwrap().is_empty());
        index.collect_garbage_once(usize::MAX);
        index.audit_allocations().unwrap();
    }
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, initial).unwrap();
    table.commit(&mut tx).unwrap();
    table
        .vacuum_reclaim_once(aerostore_core::compute_global_xmin(&shm))
        .unwrap();
    assert_eq!(
        indexes.gs.try_entries().unwrap(),
        vec![(IndexValue::I64(450), 0)]
    );
}

#[test]
fn savepoint_and_abort_do_not_publish_rolled_back_index_keys() {
    let (_, table, indexes, initial) = fixture();
    let mut committed = initial;
    committed.gs = 455;
    let mut rolled_back = committed;
    rolled_back.gs = 460;
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, committed).unwrap();
    table.savepoint(&mut tx, "branch").unwrap();
    table.write(&mut tx, 0, rolled_back).unwrap();
    table.rollback_to(&mut tx, "branch").unwrap();
    table.commit(&mut tx).unwrap();
    assert_eq!(
        indexes.gs.try_entries().unwrap(),
        vec![(IndexValue::I64(455), 0)]
    );
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, rolled_back).unwrap();
    table.abort(&mut tx).unwrap();
    assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, committed)]);
    assert_eq!(
        indexes.gs.try_entries().unwrap(),
        vec![(IndexValue::I64(455), 0)]
    );
}

#[test]
fn warm_attachment_rebinds_all_six_indexes_before_writing() {
    let (shm, table, indexes, initial) = fixture();
    let mut layout = aerostore_core::BootLayout::new(1).unwrap();
    indexes.write_layout_offsets(&mut layout).unwrap();
    let attached_indexes = FlightIndexes::from_layout(Arc::clone(&shm), &layout).unwrap();
    let mut attached_table = OccTable::from_existing(
        Arc::clone(&shm),
        table.shared_header_offset(),
        table.index_slot_offsets(),
    )
    .unwrap();
    assert!(attached_table.begin_transaction().is_err());
    attached_indexes.bind(&mut attached_table).unwrap();
    let mut changed = initial;
    changed.gs = 455;
    let mut tx = attached_table.begin_transaction().unwrap();
    attached_table.write(&mut tx, 0, changed).unwrap();
    attached_table.commit(&mut tx).unwrap();
    assert_eq!(
        indexes.gs.try_entries().unwrap(),
        vec![(IndexValue::I64(455), 0)]
    );
    assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, changed)]);
}
