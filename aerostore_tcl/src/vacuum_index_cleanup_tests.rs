#![cfg(test)]

use super::{
    cleanup_reclaimed_index_entries, FlightIndexes, FlightState, VacuumReclaimedRow,
    FLIGHT_ID_BYTES,
};
use aerostore_core::{IndexValue, ShmArena};
use std::sync::Arc;

#[test]
fn cleanup_preserves_live_postings_for_unchanged_columns() {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared memory"));
    let indexes = FlightIndexes::new(Arc::clone(&shm));
    let row_id = 7_usize;

    let old = make_row("UAL123", 37.6189, -122.3750, 32_000, 450, 1_700_000_000);
    let mut live = old;
    live.gs = 455;
    live.updated_at = 1_700_000_100;

    // Current live postings.
    indexes.insert_row(row_id, &live);
    // Simulate stale postings only for changed fields.
    indexes.gs.insert(IndexValue::I64(old.gs as i64), row_id);
    indexes
        .updated_at
        .insert(IndexValue::I64(old.updated_at as i64), row_id);

    cleanup_reclaimed_index_entries(
        &indexes,
        &[VacuumReclaimedRow {
            row_id,
            reclaimed_value: old,
            live_head_value: Some(live),
        }],
    );

    // Unchanged columns must still contain the live row id.
    assert_eq!(
        indexes
            .flight_id
            .lookup_posting_count(&IndexValue::String(live.flight_id_string())),
        1
    );
    assert_eq!(
        indexes
            .altitude
            .lookup_posting_count(&IndexValue::I64(live.altitude as i64)),
        1
    );
    assert_eq!(
        indexes
            .lat
            .lookup_posting_count(&IndexValue::I64(live.lat_scaled)),
        1
    );
    assert_eq!(
        indexes
            .lon
            .lookup_posting_count(&IndexValue::I64(live.lon_scaled)),
        1
    );

    // Changed stale postings must be removed.
    assert_eq!(
        indexes
            .gs
            .lookup_posting_count(&IndexValue::I64(old.gs as i64)),
        0
    );
    assert_eq!(
        indexes
            .updated_at
            .lookup_posting_count(&IndexValue::I64(old.updated_at as i64)),
        0
    );

    // Changed live postings must remain.
    assert_eq!(
        indexes
            .gs
            .lookup_posting_count(&IndexValue::I64(live.gs as i64)),
        1
    );
    assert_eq!(
        indexes
            .updated_at
            .lookup_posting_count(&IndexValue::I64(live.updated_at as i64)),
        1
    );
}

#[test]
fn cleanup_drops_all_postings_when_no_live_head_exists() {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared memory"));
    let indexes = FlightIndexes::new(Arc::clone(&shm));
    let row_id = 19_usize;
    let old = make_row("DAL789", 35.0000, -120.1000, 28_000, 402, 1_700_100_000);

    indexes.insert_row(row_id, &old);
    cleanup_reclaimed_index_entries(
        &indexes,
        &[VacuumReclaimedRow {
            row_id,
            reclaimed_value: old,
            live_head_value: None,
        }],
    );

    assert_eq!(
        indexes
            .flight_id
            .lookup_posting_count(&IndexValue::String(old.flight_id_string())),
        0
    );
    assert_eq!(
        indexes
            .altitude
            .lookup_posting_count(&IndexValue::I64(old.altitude as i64)),
        0
    );
    assert_eq!(
        indexes
            .gs
            .lookup_posting_count(&IndexValue::I64(old.gs as i64)),
        0
    );
    assert_eq!(
        indexes
            .lat
            .lookup_posting_count(&IndexValue::I64(old.lat_scaled)),
        0
    );
    assert_eq!(
        indexes
            .lon
            .lookup_posting_count(&IndexValue::I64(old.lon_scaled)),
        0
    );
    assert_eq!(
        indexes
            .updated_at
            .lookup_posting_count(&IndexValue::I64(old.updated_at as i64)),
        0
    );
}

fn make_row(
    flight: &str,
    lat: f64,
    lon: f64,
    altitude: i32,
    gs: u16,
    updated_at: u64,
) -> FlightState {
    let mut encoded = [0_u8; FLIGHT_ID_BYTES];
    let bytes = flight.as_bytes();
    let len = bytes.len().min(FLIGHT_ID_BYTES);
    encoded[..len].copy_from_slice(&bytes[..len]);
    FlightState {
        exists: 1,
        flight_id: encoded,
        lat_scaled: (lat * 1_000_000.0).round() as i64,
        lon_scaled: (lon * 1_000_000.0).round() as i64,
        altitude,
        gs,
        updated_at,
    }
}
