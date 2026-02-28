use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use aerostore_core::{
    deserialize_commit_record, read_wal_file, recover_occ_table_from_checkpoint_and_wal,
    recover_occ_table_from_wal, spawn_wal_writer_daemon, write_occ_checkpoint_and_truncate_wal,
    IndexCompare, IndexValue, OccCommitter, OccTable, SecondaryIndex, SharedWalRing, ShmArena,
    WalWriterError,
};

fn tmp_data_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("aerostore_wal_recovery_{nonce}"))
}

const RING_SLOTS: usize = 1024;
const RING_SLOT_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TclLikeRow {
    exists: u8,
    flight: [u8; 12],
    altitude: i32,
    gs: u16,
}

impl TclLikeRow {
    #[inline]
    fn empty() -> Self {
        Self {
            exists: 0,
            flight: [0_u8; 12],
            altitude: 0,
            gs: 0,
        }
    }

    fn from_parts(flight: &str, altitude: i32, gs: u16) -> Self {
        let mut encoded = [0_u8; 12];
        let bytes = flight.as_bytes();
        let len = bytes.len().min(encoded.len());
        encoded[..len].copy_from_slice(&bytes[..len]);
        Self {
            exists: 1,
            flight: encoded,
            altitude,
            gs,
        }
    }
}

fn decode_flight(row: &TclLikeRow) -> String {
    let end = row
        .flight
        .iter()
        .position(|ch| *ch == 0)
        .unwrap_or(row.flight.len());
    String::from_utf8_lossy(&row.flight[..end]).to_string()
}

#[test]
fn crash_recovery_restores_occ_rows_and_indexes_from_wal() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("aerostore_occ_recovery.wal");

    let shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create shared memory for recovery test"),
    );
    let table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&shm), 16)
        .expect("failed to create occ table");
    for row_id in 0..16 {
        table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed tcl-like row");
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(std::sync::Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", std::sync::Arc::clone(&shm));
    let mut key_to_row = HashMap::<String, usize>::new();
    let mut next_row_id = 0_usize;
    let mut expected_final = HashMap::<String, TclLikeRow>::new();

    let first_wave = [
        ("UAL123", 12_000_i32, 451_u16),
        ("AAL456", 9_000, 390),
        ("DAL789", 15_000, 430),
        ("UAL123", 12_150, 452),
    ];
    run_tcl_like_upsert_wave(
        &table,
        &mut committer,
        &altitude_index,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &first_wave,
    );

    let second_wave = [
        ("AAL456", 13_050_i32, 400_u16),
        ("DAL789", 16_000, 435),
        ("UAL123", 18_000, 468),
        ("SWA321", 11_000, 380),
    ];
    run_tcl_like_upsert_wave(
        &table,
        &mut committer,
        &altitude_index,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &second_wave,
    );

    ring.close().expect("failed to close ring");
    daemon.join().expect("wal daemon should exit cleanly");
    drop(table);
    drop(shm);

    let recovered_shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create recovery shared memory"),
    );
    let recovered_table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_shm), 16)
        .expect("failed to create recovery occ table");
    for row_id in 0..16 {
        recovered_table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed recovery row");
    }

    let recovery_state = recover_occ_table_from_wal(&recovered_table, &wal_path)
        .expect("failed to recover OCC table from wal");
    assert!(recovery_state.wal_records >= 1);
    assert!(recovery_state.applied_writes >= expected_final.len());
    assert!(recovery_state.max_txid > 0);

    let recovered_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", std::sync::Arc::clone(&recovered_shm));
    let mut recovered_by_flight = HashMap::<String, (usize, TclLikeRow)>::new();
    for row_id in 0..16 {
        let row = recovered_table
            .latest_value(row_id)
            .expect("latest_value failed during recovery verification")
            .expect("seeded row unexpectedly missing during recovery verification");
        if row.exists == 0 {
            continue;
        }
        let flight = decode_flight(&row);
        recovered_index.insert(IndexValue::I64(row.altitude as i64), row_id);
        recovered_by_flight.insert(flight, (row_id, row));
    }

    assert_eq!(recovered_by_flight.len(), expected_final.len());

    for (flight, expected_row) in &expected_final {
        let (row_id, recovered_row) = recovered_by_flight
            .get(flight)
            .unwrap_or_else(|| panic!("missing recovered row for key {}", flight));
        assert_eq!(
            recovered_row, expected_row,
            "recovered row mismatch for key {}",
            flight
        );

        let candidates = recovered_index.lookup(&IndexCompare::Eq(IndexValue::I64(
            expected_row.altitude as i64,
        )));
        assert!(
            candidates.contains(row_id),
            "recovered index missing key={} altitude={} row_id={}",
            flight,
            expected_row.altitude,
            row_id
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn async_wal_daemon_crash_then_restart_preserves_replayable_wal_stream() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("aerostore.wal");

    let shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create shared memory for async wal test"),
    );
    let table =
        OccTable::<u64>::new(std::sync::Arc::clone(&shm), 1).expect("failed to create occ table");
    table
        .seed_row(0, 0_u64)
        .expect("failed to seed async wal test row");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(std::sync::Arc::clone(&shm))
        .expect("failed to create shared wal ring");

    let daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn first wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    const FIRST_WAVE: usize = 800;
    const SECOND_WAVE: usize = 500;

    run_occ_increment_commits(&table, &mut committer, FIRST_WAVE);

    daemon
        .terminate(libc::SIGKILL)
        .expect("failed to kill first wal writer daemon");
    let killed_status = daemon
        .join_any_status()
        .expect("failed waiting for killed daemon");
    assert!(
        libc::WIFSIGNALED(killed_status),
        "expected first daemon to be signaled, status={}",
        killed_status
    );

    let daemon2 = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn second wal writer daemon");
    run_occ_increment_commits(&table, &mut committer, SECOND_WAVE);

    ring.close()
        .expect("failed to close ring after second wave");
    daemon2
        .join()
        .expect("second daemon should exit cleanly after ring close");

    let commits = read_ring_wal_records(&wal_path);
    assert!(
        !commits.is_empty(),
        "expected at least some commits to persist to WAL file"
    );
    assert!(
        commits.len() <= FIRST_WAVE + SECOND_WAVE,
        "persisted more commits than produced"
    );

    let mut prev_txid = 0_u64;
    for commit in &commits {
        assert!(
            commit.txid > prev_txid,
            "wal txids must be strictly increasing"
        );
        prev_txid = commit.txid;
        assert_eq!(
            commit.writes.len(),
            1,
            "each benchmark txn should produce exactly one write"
        );
        assert_eq!(commit.writes[0].row_id, 0);
    }

    // Simulated replay check: stream is fully parseable and ordered even after daemon crash.
    let replayed_commits = commits.len() as u64;
    assert!(
        replayed_commits >= SECOND_WAVE as u64,
        "expected post-restart wave to be durable (replayed={}, second_wave={})",
        replayed_commits,
        SECOND_WAVE
    );

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn checkpoint_compaction_truncates_wal_and_recovery_replays_checkpoint_plus_tail() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("checkpoint_compaction.wal");
    let checkpoint_path = data_dir.join("occ_checkpoint.dat");

    let shm = std::sync::Arc::new(
        ShmArena::new(32 << 20).expect("failed to create checkpoint compaction shm"),
    );
    let table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&shm), 4)
        .expect("failed to create checkpoint compaction table");
    for row_id in 0..4 {
        table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed checkpoint compaction row");
    }

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
        .expect("failed to create checkpoint compaction committer");

    let initial_rows = [
        TclLikeRow::from_parts("UAL123", 12_000, 451),
        TclLikeRow::from_parts("AAL456", 9_800, 380),
    ];
    for (row_id, row) in initial_rows.into_iter().enumerate() {
        let mut tx = table
            .begin_transaction()
            .expect("begin_transaction failed for pre-checkpoint write");
        table
            .write(&mut tx, row_id, row)
            .expect("write failed for pre-checkpoint write");
        committer
            .commit(&table, &mut tx)
            .expect("commit failed for pre-checkpoint write");
    }

    let checkpoint_rows =
        write_occ_checkpoint_and_truncate_wal(&table, &checkpoint_path, &wal_path)
            .expect("checkpoint compaction should succeed");
    assert!(
        checkpoint_rows >= 2,
        "checkpoint should capture at least live seeded rows, rows={}",
        checkpoint_rows
    );
    let wal_size = std::fs::metadata(&wal_path)
        .expect("wal metadata query failed after truncation")
        .len();
    assert_eq!(wal_size, 0, "checkpoint compaction must truncate WAL");

    let post_checkpoint_rows = [
        TclLikeRow::from_parts("UAL123", 18_000, 468),
        TclLikeRow::from_parts("AAL456", 13_200, 401),
    ];
    for (row_id, row) in post_checkpoint_rows.into_iter().enumerate() {
        let mut tx = table
            .begin_transaction()
            .expect("begin_transaction failed for post-checkpoint write");
        table
            .write(&mut tx, row_id, row)
            .expect("write failed for post-checkpoint write");
        committer
            .commit(&table, &mut tx)
            .expect("commit failed for post-checkpoint write");
    }

    drop(table);
    drop(shm);

    let recovered_shm = std::sync::Arc::new(
        ShmArena::new(32 << 20).expect("failed to create checkpoint recovery shm"),
    );
    let recovered_table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_shm), 4)
        .expect("failed to create checkpoint recovery table");
    for row_id in 0..4 {
        recovered_table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed checkpoint recovery row");
    }

    let state =
        recover_occ_table_from_checkpoint_and_wal(&recovered_table, &checkpoint_path, &wal_path)
            .expect("checkpoint+wal recovery failed");
    assert!(
        state.applied_writes >= 2,
        "checkpoint+wal recovery should apply writes"
    );

    let row0 = recovered_table
        .latest_value(0)
        .expect("latest_value failed for recovered row 0")
        .expect("recovered row 0 unexpectedly missing");
    let row1 = recovered_table
        .latest_value(1)
        .expect("latest_value failed for recovered row 1")
        .expect("recovered row 1 unexpectedly missing");
    assert_eq!(
        row0,
        TclLikeRow::from_parts("UAL123", 18_000, 468),
        "row 0 should reflect post-checkpoint WAL tail"
    );
    assert_eq!(
        row1,
        TclLikeRow::from_parts("AAL456", 13_200, 401),
        "row 1 should reflect post-checkpoint WAL tail"
    );

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn async_wal_daemon_crash_then_restart_replays_tcl_like_upserts_and_indexes() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("aerostore_tcl_like.wal");

    let shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create shared memory for tcl-like wal test"),
    );
    let table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&shm), 16)
        .expect("failed to create occ table");
    for row_id in 0..16 {
        table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed tcl-like row");
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(std::sync::Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn first wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", std::sync::Arc::clone(&shm));
    let mut key_to_row = HashMap::<String, usize>::new();
    let mut next_row_id = 0_usize;
    let mut expected_final = HashMap::<String, TclLikeRow>::new();

    let first_wave = [
        ("UAL123", 12_000_i32, 451_u16),
        ("AAL456", 9_000, 390),
        ("DAL789", 15_000, 430),
        ("UAL123", 12_150, 452),
    ];
    run_tcl_like_upsert_wave(
        &table,
        &mut committer,
        &altitude_index,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &first_wave,
    );

    daemon
        .terminate(libc::SIGKILL)
        .expect("failed to kill first wal writer daemon");
    let killed_status = daemon
        .join_any_status()
        .expect("failed waiting for killed daemon");
    assert!(
        libc::WIFSIGNALED(killed_status),
        "expected first daemon to be signaled, status={}",
        killed_status
    );

    let daemon2 = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn second wal writer daemon");
    let second_wave = [
        ("AAL456", 13_050_i32, 400_u16),
        ("DAL789", 16_000, 435),
        ("UAL123", 18_000, 468),
        ("SWA321", 11_000, 380),
    ];
    run_tcl_like_upsert_wave(
        &table,
        &mut committer,
        &altitude_index,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &second_wave,
    );

    ring.close()
        .expect("failed to close ring after second wave");
    daemon2
        .join()
        .expect("second daemon should exit cleanly after ring close");

    let commits = read_ring_wal_records(&wal_path);
    assert!(
        !commits.is_empty(),
        "expected at least some commits to persist to WAL file"
    );
    assert!(
        commits.len() <= first_wave.len() + second_wave.len(),
        "persisted more commits than produced"
    );

    let mut prev_txid = 0_u64;
    for commit in &commits {
        assert!(
            commit.txid > prev_txid,
            "wal txids must be strictly increasing"
        );
        prev_txid = commit.txid;
        assert_eq!(
            commit.writes.len(),
            1,
            "each tcl-like upsert should produce exactly one write"
        );
    }

    let recovered_shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create shared memory for replay validation"),
    );
    let recovered_table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_shm), 16)
        .expect("failed to create replay-validation occ table");
    for row_id in 0..16 {
        recovered_table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed replay-validation row");
    }

    let recovery_state = recover_occ_table_from_wal(&recovered_table, &wal_path)
        .expect("failed to recover tcl-like upsert wal");
    assert!(recovery_state.wal_records >= 1);
    assert!(recovery_state.applied_writes >= expected_final.len());

    let recovered_altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", std::sync::Arc::clone(&recovered_shm));
    let mut recovered_rows = HashMap::<String, TclLikeRow>::new();
    for row_id in 0..16 {
        let row = recovered_table
            .latest_value(row_id)
            .expect("latest_value failed during replay validation")
            .expect("seeded row unexpectedly missing during replay validation");
        if row.exists == 0 {
            continue;
        }
        recovered_altitude_index.insert(IndexValue::I64(row.altitude as i64), row_id);
        recovered_rows.insert(decode_flight(&row), row);
    }

    for (flight, expected_row) in &expected_final {
        let replayed_row = recovered_rows
            .get(flight)
            .unwrap_or_else(|| panic!("missing recovered row for key {}", flight));
        assert_eq!(
            replayed_row, expected_row,
            "recovered row mismatch for key {}",
            flight
        );

        let row_id = *key_to_row
            .get(flight)
            .unwrap_or_else(|| panic!("missing row id for key {}", flight));
        let candidates = recovered_altitude_index.lookup(&IndexCompare::Eq(IndexValue::I64(
            expected_row.altitude as i64,
        )));
        assert!(
            candidates.contains(&row_id),
            "recovered altitude index missing key={} altitude={} row_id={}",
            flight,
            expected_row.altitude,
            row_id
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn async_wal_daemon_restart_does_not_persist_rolled_back_savepoint_intents() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("aerostore_tcl_like_savepoint.wal");

    let shm = std::sync::Arc::new(
        ShmArena::new(64 << 20)
            .expect("failed to create shared memory for savepoint wal replay test"),
    );
    let table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&shm), 16)
        .expect("failed to create occ table");
    for row_id in 0..16 {
        table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed tcl-like row");
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(std::sync::Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn first wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let mut key_to_row = HashMap::<String, usize>::new();
    let mut next_row_id = 0_usize;
    let mut expected_final = HashMap::<String, TclLikeRow>::new();

    let first_wave = [
        ("UAL123", 11_800_i32, 446_u16),
        ("AAL456", 12_400, 401),
        ("DAL789", 14_000, 432),
    ];
    run_tcl_like_upsert_wave_with_savepoint_churn(
        &table,
        &mut committer,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &first_wave,
    );

    daemon
        .terminate(libc::SIGKILL)
        .expect("failed to kill first wal writer daemon");
    let killed_status = daemon
        .join_any_status()
        .expect("failed waiting for killed daemon");
    assert!(
        libc::WIFSIGNALED(killed_status),
        "expected first daemon to be signaled, status={}",
        killed_status
    );

    let daemon2 = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .expect("failed to spawn second wal writer daemon");
    let second_wave = [
        ("UAL123", 12_200_i32, 450_u16),
        ("AAL456", 13_100, 410),
        ("SWA321", 10_900, 382),
    ];
    run_tcl_like_upsert_wave_with_savepoint_churn(
        &table,
        &mut committer,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &second_wave,
    );

    ring.close()
        .expect("failed to close ring after second wave");
    daemon2
        .join()
        .expect("second daemon should exit cleanly after ring close");

    let commits = read_ring_wal_records(&wal_path);
    assert!(
        !commits.is_empty(),
        "expected at least some commits to persist to WAL file"
    );
    assert!(
        commits.len() <= first_wave.len() + second_wave.len(),
        "persisted more commits than produced"
    );

    for commit in &commits {
        assert_eq!(
            commit.writes.len(),
            1,
            "rolled-back savepoint intents must not appear in durable WAL records"
        );
    }

    let recovered_shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create shared memory for savepoint recovery"),
    );
    let recovered_table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_shm), 16)
        .expect("failed to create savepoint recovery occ table");
    for row_id in 0..16 {
        recovered_table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed savepoint recovery row");
    }

    let recovery_state = recover_occ_table_from_wal(&recovered_table, &wal_path)
        .expect("failed to recover savepoint wal");
    assert!(recovery_state.wal_records >= 1);
    assert!(recovery_state.applied_writes >= expected_final.len());

    for (flight, expected_row) in &expected_final {
        let row_id = *key_to_row
            .get(flight)
            .unwrap_or_else(|| panic!("missing row mapping for key {}", flight));
        let mut verify_tx = recovered_table
            .begin_transaction()
            .expect("verify begin_transaction failed");
        let live = recovered_table
            .read(&mut verify_tx, row_id)
            .expect("verify read failed")
            .expect("recovered row missing after savepoint replay test");
        recovered_table
            .abort(&mut verify_tx)
            .expect("verify abort failed");
        assert_eq!(
            live, *expected_row,
            "recovered table state diverged from expected final value for key {}",
            flight
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn recovery_rejects_truncated_or_corrupt_wal_frames_with_explicit_error() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");

    let truncated_path = data_dir.join("truncated.wal");
    let mut truncated = Vec::new();
    truncated.extend_from_slice(&(16_u32).to_le_bytes());
    truncated.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // declared 16, only 3 bytes present
    std::fs::write(&truncated_path, truncated).expect("failed to write truncated wal");

    match read_wal_file(&truncated_path) {
        Err(WalWriterError::Io(err)) => {
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::InvalidData,
                "truncated wal must report InvalidData"
            );
        }
        Err(other) => panic!("unexpected truncated WAL error variant: {}", other),
        Ok(_) => panic!("truncated WAL unexpectedly parsed as valid"),
    }

    let corrupt_path = data_dir.join("corrupt.wal");
    let mut corrupt = Vec::new();
    corrupt.extend_from_slice(&(8_u32).to_le_bytes());
    corrupt.extend_from_slice(&[0_u8; 8]); // valid frame size, invalid rkyv payload
    std::fs::write(&corrupt_path, corrupt).expect("failed to write corrupt wal");

    match read_wal_file(&corrupt_path) {
        Err(WalWriterError::Ring(_)) => {}
        Err(other) => panic!("unexpected corrupt WAL error variant: {}", other),
        Ok(_) => panic!("corrupt WAL unexpectedly parsed as valid"),
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn replaying_same_wal_into_fresh_tables_is_idempotent() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("idempotent_replay.wal");

    let source_shm = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create source shared memory"),
    );
    let source_table = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&source_shm), 16)
        .expect("failed to create source table");
    for row_id in 0..16 {
        source_table
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed source row");
    }

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
        .expect("failed to create synchronous committer for idempotency test");
    let altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", std::sync::Arc::clone(&source_shm));
    let mut key_to_row = HashMap::<String, usize>::new();
    let mut next_row_id = 0_usize;
    let mut expected_final = HashMap::<String, TclLikeRow>::new();

    let ops = [
        ("UAL123", 12_000_i32, 451_u16),
        ("AAL456", 13_500, 402),
        ("UAL123", 12_320, 452),
        ("DAL789", 9_750, 330),
        ("DAL789", 10_020, 338),
    ];
    run_tcl_like_upsert_wave(
        &source_table,
        &mut committer,
        &altitude_index,
        &mut key_to_row,
        &mut next_row_id,
        &mut expected_final,
        &ops,
    );
    drop(source_table);
    drop(source_shm);

    let recovered_a = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create replay A shared memory"),
    );
    let table_a = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_a), 16)
        .expect("failed to create replay A table");
    for row_id in 0..16 {
        table_a
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed replay A row");
    }

    let recovered_b = std::sync::Arc::new(
        ShmArena::new(64 << 20).expect("failed to create replay B shared memory"),
    );
    let table_b = OccTable::<TclLikeRow>::new(std::sync::Arc::clone(&recovered_b), 16)
        .expect("failed to create replay B table");
    for row_id in 0..16 {
        table_b
            .seed_row(row_id, TclLikeRow::empty())
            .expect("failed to seed replay B row");
    }

    let state_a =
        recover_occ_table_from_wal(&table_a, &wal_path).expect("failed replay A recovery from wal");
    let state_b =
        recover_occ_table_from_wal(&table_b, &wal_path).expect("failed replay B recovery from wal");
    assert_eq!(
        state_a, state_b,
        "recovery metadata diverged across replays"
    );

    let rows_a = collect_live_rows(&table_a, 16);
    let rows_b = collect_live_rows(&table_b, 16);
    assert_eq!(rows_a, rows_b, "replayed live rows diverged across replays");

    let recovered_by_flight: HashMap<_, _> = rows_a
        .iter()
        .map(|(_, row)| (decode_flight(row), *row))
        .collect();
    assert_eq!(recovered_by_flight.len(), expected_final.len());
    for (flight, expected) in &expected_final {
        let recovered = recovered_by_flight
            .get(flight)
            .unwrap_or_else(|| panic!("missing recovered row for key {}", flight));
        assert_eq!(
            recovered, expected,
            "idempotent replay mismatch for key {}",
            flight
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn recovery_large_cardinality_index_parity_matches_table_scan() {
    const ROWS: usize = 100_000;

    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("large_cardinality_parity.wal");

    let source_shm = Arc::new(
        ShmArena::new(256 << 20).expect("failed to create source shared memory for parity test"),
    );
    let source_table = OccTable::<i32>::new(Arc::clone(&source_shm), ROWS)
        .expect("failed to create source occ table");
    for row_id in 0..ROWS {
        source_table
            .seed_row(row_id, 0_i32)
            .expect("failed to seed source parity row");
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&source_shm))
        .expect("failed to create parity ring");
    let daemon =
        spawn_wal_writer_daemon(ring.clone(), &wal_path).expect("failed to spawn parity daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    for row_id in 0..ROWS {
        let altitude = (((row_id as i32) * 17) % 45_000) + 500;
        let mut tx = source_table
            .begin_transaction()
            .expect("begin_transaction failed for parity source write");
        source_table
            .write(&mut tx, row_id, altitude)
            .expect("write failed for parity source row");
        committer
            .commit(&source_table, &mut tx)
            .expect("commit failed for parity source row");
    }

    ring.close().expect("failed to close parity ring");
    daemon.join().expect("parity daemon did not exit cleanly");
    drop(source_table);
    drop(source_shm);

    let recovered_shm = Arc::new(
        ShmArena::new(256 << 20).expect("failed to create recovery shared memory for parity test"),
    );
    let recovered_table = OccTable::<i32>::new(Arc::clone(&recovered_shm), ROWS)
        .expect("failed to create recovered occ table");
    for row_id in 0..ROWS {
        recovered_table
            .seed_row(row_id, 0_i32)
            .expect("failed to seed recovered parity row");
    }

    let recovery = recover_occ_table_from_wal(&recovered_table, &wal_path)
        .expect("failed to recover parity wal");
    assert_eq!(
        recovery.applied_writes, ROWS,
        "expected one recovered write per parity row"
    );

    let altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", Arc::clone(&recovered_shm));
    let mut altitude_by_row = Vec::with_capacity(ROWS);
    for row_id in 0..ROWS {
        let altitude = recovered_table
            .latest_value(row_id)
            .expect("latest_value failed for recovered parity row")
            .expect("recovered parity row unexpectedly missing");
        altitude_by_row.push(altitude);
        altitude_index.insert(IndexValue::I64(altitude as i64), row_id);
    }

    let eq_predicates = [500_i64, 2_345, 11_111, 23_456, 44_999];
    for altitude in eq_predicates {
        let indexed: BTreeSet<usize> = altitude_index
            .lookup(&IndexCompare::Eq(IndexValue::I64(altitude)))
            .into_iter()
            .collect();
        let scanned: BTreeSet<usize> = altitude_by_row
            .iter()
            .enumerate()
            .filter_map(|(row_id, value)| (*value as i64 == altitude).then_some(row_id))
            .collect();
        assert_eq!(
            indexed, scanned,
            "EQ predicate parity mismatch for altitude={}",
            altitude
        );
    }

    let gt_predicates = [1_000_i64, 10_000, 20_000, 30_000, 40_000];
    for altitude in gt_predicates {
        let indexed: BTreeSet<usize> = altitude_index
            .lookup(&IndexCompare::Gt(IndexValue::I64(altitude)))
            .into_iter()
            .collect();
        let scanned: BTreeSet<usize> = altitude_by_row
            .iter()
            .enumerate()
            .filter_map(|(row_id, value)| (*value as i64 > altitude).then_some(row_id))
            .collect();
        assert_eq!(
            indexed, scanned,
            "GT predicate parity mismatch for altitude={}",
            altitude
        );
    }

    let lt_predicates = [1_000_i64, 10_000, 20_000, 30_000, 40_000];
    for altitude in lt_predicates {
        let indexed: BTreeSet<usize> = altitude_index
            .lookup(&IndexCompare::Lt(IndexValue::I64(altitude)))
            .into_iter()
            .collect();
        let scanned: BTreeSet<usize> = altitude_by_row
            .iter()
            .enumerate()
            .filter_map(|(row_id, value)| ((*value as i64) < altitude).then_some(row_id))
            .collect();
        assert_eq!(
            indexed, scanned,
            "LT predicate parity mismatch for altitude={}",
            altitude
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn recovery_high_cardinality_multi_round_updates_restore_latest_values() {
    const ROWS: usize = 8_192;
    const ROUNDS: usize = 4;

    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("high_cardinality_multi_round.wal");

    let source_shm = Arc::new(
        ShmArena::new(256 << 20).expect("failed to create source shared memory for stress test"),
    );
    let source_table = OccTable::<i32>::new(Arc::clone(&source_shm), ROWS)
        .expect("failed to create source table for stress test");
    for row_id in 0..ROWS {
        source_table
            .seed_row(row_id, 0_i32)
            .expect("failed to seed source stress row");
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&source_shm))
        .expect("failed to create stress ring");
    let daemon =
        spawn_wal_writer_daemon(ring.clone(), &wal_path).expect("failed to spawn stress daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let mut expected_latest = vec![0_i32; ROWS];
    for round in 0..ROUNDS {
        for row_id in 0..ROWS {
            let value = (((row_id as i32 * 97) + (round as i32 * 1_231)) % 45_000) + 500;
            let mut tx = source_table
                .begin_transaction()
                .expect("begin_transaction failed for stress source write");
            source_table
                .write(&mut tx, row_id, value)
                .expect("write failed for stress source row");
            committer
                .commit(&source_table, &mut tx)
                .expect("commit failed for stress source row");
            expected_latest[row_id] = value;
        }
    }

    ring.close().expect("failed to close stress ring");
    daemon.join().expect("stress daemon did not exit cleanly");
    drop(source_table);
    drop(source_shm);

    let recovered_shm = Arc::new(
        ShmArena::new(256 << 20).expect("failed to create recovered shared memory for stress test"),
    );
    let recovered_table = OccTable::<i32>::new(Arc::clone(&recovered_shm), ROWS)
        .expect("failed to create recovered table for stress test");
    for row_id in 0..ROWS {
        recovered_table
            .seed_row(row_id, 0_i32)
            .expect("failed to seed recovered stress row");
    }

    let recovery = recover_occ_table_from_wal(&recovered_table, &wal_path)
        .expect("failed to recover stress WAL");
    assert_eq!(
        recovery.applied_writes,
        ROWS * ROUNDS,
        "stress recovery should apply every generated write"
    );

    let altitude_index =
        SecondaryIndex::<usize>::new_in_shared("altitude", Arc::clone(&recovered_shm));
    let mut recovered_values = Vec::with_capacity(ROWS);
    for row_id in 0..ROWS {
        let value = recovered_table
            .latest_value(row_id)
            .expect("latest_value failed for recovered stress row")
            .expect("recovered stress row unexpectedly missing");
        assert_eq!(
            value, expected_latest[row_id],
            "recovered latest value mismatch for row {}",
            row_id
        );
        altitude_index.insert(IndexValue::I64(value as i64), row_id);
        recovered_values.push(value);
    }

    for sample_row in [0_usize, ROWS / 3, ROWS / 2, ROWS - 1] {
        let target = expected_latest[sample_row] as i64;
        let indexed: BTreeSet<usize> = altitude_index
            .lookup(&IndexCompare::Eq(IndexValue::I64(target)))
            .into_iter()
            .collect();
        let scanned: BTreeSet<usize> = recovered_values
            .iter()
            .enumerate()
            .filter_map(|(row_id, value)| ((*value as i64) == target).then_some(row_id))
            .collect();
        assert_eq!(
            indexed, scanned,
            "stress EQ predicate parity mismatch for value={}",
            target
        );
    }

    for threshold in [5_000_i64, 12_000, 24_000, 36_000] {
        let indexed: BTreeSet<usize> = altitude_index
            .lookup(&IndexCompare::Gt(IndexValue::I64(threshold)))
            .into_iter()
            .collect();
        let scanned: BTreeSet<usize> = recovered_values
            .iter()
            .enumerate()
            .filter_map(|(row_id, value)| ((*value as i64) > threshold).then_some(row_id))
            .collect();
        assert_eq!(
            indexed, scanned,
            "stress GT predicate parity mismatch for threshold={}",
            threshold
        );
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn benchmark_occ_wal_replay_startup_throughput() {
    const TXNS: usize = 12_000;
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("replay_benchmark.wal");

    let source_shm = std::sync::Arc::new(
        ShmArena::new(32 << 20).expect("failed to create benchmark source shm"),
    );
    let source_table = OccTable::<u64>::new(std::sync::Arc::clone(&source_shm), 1)
        .expect("failed to create benchmark source table");
    source_table
        .seed_row(0, 0_u64)
        .expect("failed to seed benchmark source row");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
        .expect("failed to create benchmark synchronous committer");

    run_occ_increment_commits(&source_table, &mut committer, TXNS);
    drop(source_table);
    drop(source_shm);

    let recover_shm = std::sync::Arc::new(
        ShmArena::new(32 << 20).expect("failed to create benchmark recovery shm"),
    );
    let recover_table = OccTable::<u64>::new(std::sync::Arc::clone(&recover_shm), 1)
        .expect("failed to create benchmark recovery table");
    recover_table
        .seed_row(0, 0_u64)
        .expect("failed to seed benchmark recovery row");

    let start = Instant::now();
    let recovery = recover_occ_table_from_wal(&recover_table, &wal_path)
        .expect("benchmark recovery should succeed");
    let elapsed = start.elapsed();
    let throughput = recovery.applied_writes as f64 / elapsed.as_secs_f64().max(1e-9);

    let final_value = recover_table
        .latest_value(0)
        .expect("latest value query failed")
        .expect("benchmark recovery row unexpectedly missing");
    assert_eq!(
        final_value, TXNS as u64,
        "replayed benchmark row does not match committed txn count"
    );
    assert_eq!(
        recovery.applied_writes, TXNS,
        "recovery write count should match committed txn count"
    );
    assert!(
        throughput >= 1_000.0,
        "recovery throughput unexpectedly low: {:.2} writes/sec",
        throughput
    );
    eprintln!(
        "occ_replay_benchmark: txns={} applied_writes={} elapsed={:?} throughput_writes_per_sec={:.2}",
        TXNS, recovery.applied_writes, elapsed, throughput
    );

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn checkpoint_under_write_load_remains_recoverable() {
    const TOTAL_WRITES: usize = 2_000;

    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");
    let wal_path = data_dir.join("checkpoint_under_load.wal");
    let checkpoint_path = data_dir.join("checkpoint_under_load.dat");

    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create shared memory"));
    let table =
        Arc::new(OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create occ table"));
    table.seed_row(0, 0_u64).expect("failed to seed row");

    let committer = Arc::new(Mutex::new(
        OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
            .expect("failed to create synchronous committer"),
    ));
    let writer_done = Arc::new(AtomicBool::new(false));
    let writer_error = Arc::new(Mutex::new(None::<String>));

    let writer = {
        let table = Arc::clone(&table);
        let committer = Arc::clone(&committer);
        let writer_done = Arc::clone(&writer_done);
        let writer_error = Arc::clone(&writer_error);
        thread::spawn(move || {
            for _ in 0..TOTAL_WRITES {
                let mut tx = match table.begin_transaction() {
                    Ok(tx) => tx,
                    Err(err) => {
                        *writer_error.lock().expect("writer error lock poisoned") =
                            Some(format!("begin_transaction failed: {}", err));
                        break;
                    }
                };
                let current = match table.read(&mut tx, 0) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        *writer_error.lock().expect("writer error lock poisoned") =
                            Some("writer observed missing row".to_string());
                        break;
                    }
                    Err(err) => {
                        *writer_error.lock().expect("writer error lock poisoned") =
                            Some(format!("writer read failed: {}", err));
                        break;
                    }
                };
                if let Err(err) = table.write(&mut tx, 0, current + 1) {
                    *writer_error.lock().expect("writer error lock poisoned") =
                        Some(format!("writer write failed: {}", err));
                    break;
                }
                let commit_result = {
                    let mut guard = committer.lock().expect("committer lock poisoned");
                    guard.commit(&table, &mut tx)
                };
                if let Err(err) = commit_result {
                    *writer_error.lock().expect("writer error lock poisoned") =
                        Some(format!("writer commit failed: {}", err));
                    break;
                }
            }
            writer_done.store(true, Ordering::Release);
        })
    };

    let mut checkpoint_passes = 0_usize;
    while !writer_done.load(Ordering::Acquire) {
        {
            let _guard = committer.lock().expect("committer lock poisoned");
            let _rows = write_occ_checkpoint_and_truncate_wal(&table, &checkpoint_path, &wal_path)
                .expect("checkpoint under write load failed");
        }
        checkpoint_passes += 1;
        thread::sleep(Duration::from_millis(2));
    }

    writer
        .join()
        .expect("writer thread panicked in checkpoint-under-load test");
    if let Some(err) = writer_error
        .lock()
        .expect("writer error lock poisoned")
        .clone()
    {
        panic!("{}", err);
    }

    {
        let _guard = committer.lock().expect("committer lock poisoned");
        let _ = write_occ_checkpoint_and_truncate_wal(&table, &checkpoint_path, &wal_path)
            .expect("final checkpoint should succeed");
    }

    drop(table);
    drop(shm);
    drop(committer);

    let recovered_shm =
        Arc::new(ShmArena::new(64 << 20).expect("failed to create recovery shared memory"));
    let recovered_table = OccTable::<u64>::new(Arc::clone(&recovered_shm), 1)
        .expect("failed to create recovery occ table");
    recovered_table
        .seed_row(0, 0_u64)
        .expect("failed to seed recovery row");

    let recovery_state =
        recover_occ_table_from_checkpoint_and_wal(&recovered_table, &checkpoint_path, &wal_path)
            .expect("recovery should succeed after checkpoint under load");
    let recovered = recovered_table
        .latest_value(0)
        .expect("latest_value failed after recovery")
        .expect("recovered row unexpectedly missing");

    assert_eq!(
        recovered, TOTAL_WRITES as u64,
        "recovered row value diverged after concurrent checkpoints"
    );
    assert!(
        checkpoint_passes > 0,
        "expected at least one checkpoint while writer was active"
    );
    assert!(
        recovery_state.applied_writes >= 1,
        "recovery metadata reported no applied writes"
    );

    let _ = std::fs::remove_dir_all(data_dir);
}

fn run_occ_increment_commits(
    table: &OccTable<u64>,
    committer: &mut OccCommitter<RING_SLOTS, RING_SLOT_BYTES>,
    count: usize,
) {
    for _ in 0..count {
        let mut tx = table
            .begin_transaction()
            .expect("begin_transaction failed for wal replay test");
        let current = table
            .read(&mut tx, 0)
            .expect("read failed for wal replay test")
            .expect("seed row missing during wal replay test");
        table
            .write(&mut tx, 0, current + 1)
            .expect("write failed for wal replay test");
        committer
            .commit(table, &mut tx)
            .expect("commit failed for wal replay test");
    }
}

fn run_tcl_like_upsert_wave(
    table: &OccTable<TclLikeRow>,
    committer: &mut OccCommitter<RING_SLOTS, RING_SLOT_BYTES>,
    altitude_index: &SecondaryIndex<usize>,
    key_to_row: &mut HashMap<String, usize>,
    next_row_id: &mut usize,
    expected_final: &mut HashMap<String, TclLikeRow>,
    ops: &[(&str, i32, u16)],
) {
    for (flight, altitude, gs) in ops {
        let row_id = if let Some(row_id) = key_to_row.get(*flight).copied() {
            row_id
        } else {
            let assigned = *next_row_id;
            *next_row_id += 1;
            key_to_row.insert((*flight).to_string(), assigned);
            assigned
        };

        let mut tx = table
            .begin_transaction()
            .expect("begin_transaction failed for tcl-like wal replay test");
        let current = table
            .read(&mut tx, row_id)
            .expect("read failed for tcl-like wal replay test")
            .expect("seed row missing during tcl-like wal replay test");

        let next = TclLikeRow::from_parts(flight, *altitude, *gs);
        table
            .write(&mut tx, row_id, next)
            .expect("write failed for tcl-like wal replay test");
        committer
            .commit(table, &mut tx)
            .expect("commit failed for tcl-like wal replay test");

        if current.exists != 0 {
            altitude_index.remove(&IndexValue::I64(current.altitude as i64), &row_id);
        }
        altitude_index.insert(IndexValue::I64(next.altitude as i64), row_id);
        expected_final.insert((*flight).to_string(), next);
    }
}

fn run_tcl_like_upsert_wave_with_savepoint_churn(
    table: &OccTable<TclLikeRow>,
    committer: &mut OccCommitter<RING_SLOTS, RING_SLOT_BYTES>,
    key_to_row: &mut HashMap<String, usize>,
    next_row_id: &mut usize,
    expected_final: &mut HashMap<String, TclLikeRow>,
    ops: &[(&str, i32, u16)],
) {
    for (flight, altitude, gs) in ops {
        let row_id = if let Some(row_id) = key_to_row.get(*flight).copied() {
            row_id
        } else {
            let assigned = *next_row_id;
            *next_row_id += 1;
            key_to_row.insert((*flight).to_string(), assigned);
            assigned
        };

        let mut tx = table
            .begin_transaction()
            .expect("begin_transaction failed for savepoint replay test");
        let current = table
            .read(&mut tx, row_id)
            .expect("read failed for savepoint replay test")
            .expect("seed row missing during savepoint replay test");

        table
            .savepoint(&mut tx, "sp")
            .expect("savepoint failed for savepoint replay test");

        let churn_a = TclLikeRow::from_parts(flight, *altitude + 4_000, gs.wrapping_add(10));
        table
            .write(&mut tx, row_id, churn_a)
            .expect("write(churn_a) failed for savepoint replay test");
        table
            .rollback_to(&mut tx, "sp")
            .expect("rollback_to(sp) failed for savepoint replay test");

        let churn_b = TclLikeRow::from_parts(flight, *altitude + 8_000, gs.wrapping_add(20));
        table
            .write(&mut tx, row_id, churn_b)
            .expect("write(churn_b) failed for savepoint replay test");
        table
            .rollback_to(&mut tx, "sp")
            .expect("rollback_to(sp #2) failed for savepoint replay test");

        let restored = table
            .read(&mut tx, row_id)
            .expect("restored read failed for savepoint replay test")
            .expect("row missing after rollback in savepoint replay test");
        assert_eq!(
            restored, current,
            "row must match pre-savepoint value after rollback"
        );

        let final_row = TclLikeRow::from_parts(flight, *altitude, *gs);
        table
            .write(&mut tx, row_id, final_row)
            .expect("final write failed for savepoint replay test");
        committer
            .commit(table, &mut tx)
            .expect("commit failed for savepoint replay test");

        expected_final.insert((*flight).to_string(), final_row);
    }
}

fn read_ring_wal_records(path: &Path) -> Vec<aerostore_core::WalRingCommit> {
    if !path.exists() {
        return Vec::new();
    }

    let bytes = std::fs::read(path).expect("failed to read wal ring file");
    let mut cursor = 0_usize;
    let mut out = Vec::new();

    while cursor + 4 <= bytes.len() {
        let mut len_buf = [0_u8; 4];
        len_buf.copy_from_slice(&bytes[cursor..cursor + 4]);
        cursor += 4;
        let frame_len = u32::from_le_bytes(len_buf) as usize;

        if cursor + frame_len > bytes.len() {
            break;
        }
        let frame = &bytes[cursor..cursor + frame_len];
        cursor += frame_len;

        let decoded = deserialize_commit_record(frame).expect("failed to decode wal ring frame");
        out.push(decoded);
    }

    out
}

fn collect_live_rows(table: &OccTable<TclLikeRow>, capacity: usize) -> Vec<(usize, TclLikeRow)> {
    let mut rows = Vec::new();
    for row_id in 0..capacity {
        let row = table
            .latest_value(row_id)
            .expect("latest_value failed while collecting live rows")
            .expect("seeded row unexpectedly missing while collecting live rows");
        if row.exists == 0 {
            continue;
        }
        rows.push((row_id, row));
    }
    rows.sort_unstable_by_key(|entry| entry.0);
    rows
}
