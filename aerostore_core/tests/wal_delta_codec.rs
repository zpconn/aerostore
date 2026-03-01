use std::sync::Arc;

use aerostore_core::{
    deserialize_delta_wal_record, wal_commit_from_occ_record, DeltaWalRecord, OccTable, ShmArena,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct FlightMini {
    flight_id: u64,
    altitude: i64,
    groundspeed: u16,
}

impl aerostore_core::WalDeltaCodec for FlightMini {}

#[test]
fn occ_commit_tracks_dirty_mask_and_emits_update_delta_record() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared arena"));
    let table = OccTable::<FlightMini>::new(Arc::clone(&shm), 1).expect("failed to create table");

    table
        .seed_row(
            0,
            FlightMini {
                flight_id: 101,
                altitude: 30_000,
                groundspeed: 450,
            },
        )
        .expect("seed_row failed");

    let mut tx = table
        .begin_transaction()
        .expect("begin_transaction should succeed");
    table
        .write(
            &mut tx,
            0,
            FlightMini {
                flight_id: 101,
                altitude: 31_250,
                groundspeed: 450,
            },
        )
        .expect("write should succeed");

    let commit = table
        .commit_with_record(&mut tx)
        .expect("commit_with_record should succeed");
    assert_eq!(commit.writes.len(), 1);
    assert_ne!(
        commit.writes[0].dirty_columns_bitmask, 0,
        "single-column updates must set at least one dirty bit"
    );

    let wal_commit = wal_commit_from_occ_record(&commit).expect("wal conversion should succeed");
    assert_eq!(wal_commit.writes.len(), 1);

    let wal_write = &wal_commit.writes[0];
    assert!(
        !wal_write.wal_record_payload.is_empty(),
        "delta wal payload should be populated"
    );

    let decoded = deserialize_delta_wal_record(wal_write.wal_record_payload.as_slice())
        .expect("delta wal decode should succeed");
    match decoded {
        DeltaWalRecord::UpdateDelta {
            pk,
            dirty_mask,
            delta_bytes,
        } => {
            assert_eq!(pk, "0");
            assert_ne!(dirty_mask, 0);
            assert!(!delta_bytes.is_empty());
        }
        DeltaWalRecord::UpdateFull { .. } => {
            panic!("fixed-size row should emit UpdateDelta in WAL")
        }
    }
}
