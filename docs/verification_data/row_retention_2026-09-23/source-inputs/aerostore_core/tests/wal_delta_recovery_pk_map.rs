use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aerostore_core::{
    build_update_record, recover_occ_table_from_checkpoint_and_wal_with_pk_map,
    replay_update_record_with_pk_map, write_occ_checkpoint_and_truncate_wal, DeltaWalRecord,
    OccCommitter, OccError, OccTable, ShmArena, ShmPrimaryKeyMap, WalDeltaCodec,
};
use serde::{Deserialize, Serialize};

const RING_SLOTS: usize = 256;
const RING_SLOT_BYTES: usize = 256;
const ROW_CAPACITY: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct KeyedRow {
    key: [u8; 8],
    altitude: i32,
    gs: u16,
}

impl KeyedRow {
    fn new(key: &str, altitude: i32, gs: u16) -> Self {
        let mut encoded = [0_u8; 8];
        let bytes = key.as_bytes();
        let len = bytes.len().min(encoded.len());
        encoded[..len].copy_from_slice(&bytes[..len]);
        Self {
            key: encoded,
            altitude,
            gs,
        }
    }

    fn key_string(&self) -> String {
        let end = self
            .key
            .iter()
            .position(|ch| *ch == 0)
            .unwrap_or(self.key.len());
        String::from_utf8_lossy(&self.key[..end]).to_string()
    }
}

impl WalDeltaCodec for KeyedRow {
    fn wal_primary_key(_row_id_hint: usize, value: &Self) -> String {
        value.key_string()
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}_{nonce}"));
    std::fs::create_dir_all(&path).expect("failed to create unique temp directory");
    path
}

fn commit_row(
    table: &OccTable<KeyedRow>,
    committer: &mut OccCommitter<RING_SLOTS, RING_SLOT_BYTES>,
    row_id: usize,
    value: KeyedRow,
) {
    let mut tx = table
        .begin_transaction()
        .expect("begin_transaction should succeed");
    table
        .write(&mut tx, row_id, value)
        .expect("write should succeed");
    committer
        .commit(table, &mut tx)
        .expect("commit should succeed");
}

#[test]
fn replay_update_record_with_pk_map_updates_existing_key_with_delta() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table = OccTable::<KeyedRow>::new(Arc::clone(&shm), 8).expect("failed to create table");
    let pk_map =
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 32, 8).expect("failed to create pk map");

    let base = KeyedRow::new("UAL123", 12_000, 430);
    table.seed_row(3, base).expect("seed_row should succeed");
    pk_map
        .insert_existing("UAL123", 3)
        .expect("insert_existing should succeed");

    let updated = KeyedRow::new("UAL123", 12_550, 430);
    let record =
        build_update_record("UAL123".to_string(), &base, &updated, 0).expect("build failed");
    match record {
        DeltaWalRecord::UpdateDelta { .. } => {}
        DeltaWalRecord::UpdateFull { .. } => panic!("fixed-size keyed row should emit delta"),
    }

    let replayed_row_id = replay_update_record_with_pk_map(&table, &pk_map, 11, &record)
        .expect("replay_update_record_with_pk_map should succeed");
    assert_eq!(replayed_row_id, 3);
    assert_eq!(
        table.latest_value(3).expect("latest_value failed"),
        Some(updated)
    );
    assert_eq!(pk_map.get("UAL123").expect("pk lookup failed"), Some(3));
}

#[test]
fn replay_update_record_with_pk_map_inserts_absent_key_then_applies_followup_delta() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table = OccTable::<KeyedRow>::new(Arc::clone(&shm), 8).expect("failed to create table");
    let pk_map =
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 32, 8).expect("failed to create pk map");

    let initial = KeyedRow::new("DAL456", 9_000, 390);
    let full_record = DeltaWalRecord::UpdateFull {
        pk: "DAL456".to_string(),
        payload: bincode::serialize(&initial).expect("serialize full payload failed"),
    };

    let inserted_row_id = replay_update_record_with_pk_map(&table, &pk_map, 21, &full_record)
        .expect("replay for absent key should succeed");
    assert_eq!(inserted_row_id, 0);
    assert_eq!(
        table
            .latest_value(inserted_row_id)
            .expect("latest_value failed"),
        Some(initial)
    );

    let updated = KeyedRow::new("DAL456", 9_450, 392);
    let delta_record = build_update_record("DAL456".to_string(), &initial, &updated, 0)
        .expect("build delta failed");
    let replayed_row_id = replay_update_record_with_pk_map(&table, &pk_map, 22, &delta_record)
        .expect("follow-up delta replay should succeed");
    assert_eq!(replayed_row_id, inserted_row_id);
    assert_eq!(
        table
            .latest_value(inserted_row_id)
            .expect("latest_value failed"),
        Some(updated)
    );
    assert_eq!(pk_map.get("DAL456").expect("pk lookup failed"), Some(0));
}

#[test]
fn checkpoint_recovery_with_pk_map_preserves_key_to_row_mapping_and_applies_delta_tail() {
    let data_dir = unique_temp_dir("aerostore_wal_delta_pk_checkpoint");
    let wal_path = data_dir.join("wal_delta_pk_checkpoint.wal");
    let checkpoint_path = data_dir.join("wal_delta_pk_checkpoint.dat");

    let source_shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create source shm"));
    let table = OccTable::<KeyedRow>::new(Arc::clone(&source_shm), ROW_CAPACITY)
        .expect("failed to create source table");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
        .expect("failed to create synchronous committer");

    let seed_rows = [
        (2_usize, "UAL123", KeyedRow::new("UAL123", 12_000, 450)),
        (7_usize, "DAL456", KeyedRow::new("DAL456", 9_000, 390)),
        (11_usize, "SWA321", KeyedRow::new("SWA321", 10_500, 380)),
    ];
    for (row_id, _, row) in seed_rows {
        table
            .seed_row(row_id, row)
            .expect("seed_row should succeed");
    }

    for (row_id, key, row) in seed_rows {
        let updated = KeyedRow::new(key, row.altitude + 100, row.gs + 1);
        commit_row(&table, &mut committer, row_id, updated);
    }

    let checkpoint_rows =
        write_occ_checkpoint_and_truncate_wal(&table, &checkpoint_path, &wal_path)
            .expect("checkpoint should succeed");
    assert!(
        checkpoint_rows >= seed_rows.len(),
        "checkpoint should include all seeded rows"
    );

    let mut expected_final = HashMap::new();
    for (row_id, key, row) in seed_rows {
        let final_row = KeyedRow::new(key, row.altitude + 900, row.gs + 5);
        commit_row(&table, &mut committer, row_id, final_row);
        expected_final.insert(key.to_string(), (row_id, final_row));
    }

    let recover_shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create recover shm"));
    let recover_table = OccTable::<KeyedRow>::new(Arc::clone(&recover_shm), ROW_CAPACITY)
        .expect("failed to create recover table");
    let recover_pk_map =
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&recover_shm), 64, ROW_CAPACITY)
            .expect("failed to create recover pk map");

    let recovery_state = recover_occ_table_from_checkpoint_and_wal_with_pk_map(
        &recover_table,
        Some(&recover_pk_map),
        &checkpoint_path,
        &wal_path,
    )
    .expect("checkpoint + wal recovery with pk map should succeed");
    assert!(
        recovery_state.applied_writes >= expected_final.len(),
        "recovery should apply checkpoint rows and WAL tail writes"
    );

    for (key, (expected_row_id, expected_row)) in expected_final {
        assert_eq!(
            recover_pk_map.get(key.as_str()).expect("pk lookup failed"),
            Some(expected_row_id),
            "primary key map should preserve stable row mapping for {key}"
        );
        assert_eq!(
            recover_table
                .latest_value(expected_row_id)
                .expect("latest_value failed"),
            Some(expected_row),
            "row {expected_row_id} should contain final tail-applied value"
        );
    }

    std::fs::remove_dir_all(&data_dir).expect("failed to clean temp dir");
}

#[test]
fn apply_recovered_write_cas_rejects_stale_expected_base_offset() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let table = OccTable::<KeyedRow>::new(Arc::clone(&shm), 1).expect("failed to create table");

    table
        .seed_row(0, KeyedRow::new("UAL123", 12_000, 430))
        .expect("seed_row should succeed");
    let stale_base_offset = table
        .row_head_offset(0)
        .expect("row_head_offset should succeed");

    table
        .apply_recovered_write(0, 31, KeyedRow::new("UAL123", 12_300, 432))
        .expect("apply_recovered_write should succeed");
    let err = table
        .apply_recovered_write_cas(
            0,
            32,
            stale_base_offset,
            KeyedRow::new("UAL123", 13_000, 440),
        )
        .expect_err("stale expected base offset should fail CAS");
    assert!(matches!(err, OccError::SerializationFailure));

    assert_eq!(
        table.latest_value(0).expect("latest_value failed"),
        Some(KeyedRow::new("UAL123", 12_300, 432))
    );
}
