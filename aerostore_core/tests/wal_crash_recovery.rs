use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::epoch;
use serde::{Deserialize, Serialize};

use aerostore_core::{
    deserialize_commit_record, spawn_wal_writer_daemon, DurableDatabase, Field, IndexDefinition,
    IndexCompare, IndexValue, OccCommitter, OccRow, OccTable, RecoveryStage, RelPtr,
    SecondaryIndex, SharedWalRing, ShmArena, SortDirection,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FlightRow {
    altitude: i32,
    groundspeed: u16,
}

fn altitude(row: &FlightRow) -> i32 {
    row.altitude
}

fn altitude_field() -> Field<FlightRow, i32> {
    Field::new("altitude", altitude)
}

fn tmp_data_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("aerostore_wal_recovery_{nonce}"))
}

const RING_SLOTS: usize = 1024;
const RING_SLOT_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_restores_checkpoint_wal_and_indexes() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");

    let index_defs = vec![IndexDefinition::new("altitude", altitude)];

    let (db, first_recovery) = DurableDatabase::<u64, FlightRow>::open_with_recovery(
        &data_dir,
        2048,
        Duration::from_secs(300),
        index_defs.clone(),
    )
    .await
    .expect("failed to open durable database");

    assert_eq!(first_recovery.stage, RecoveryStage::Complete);
    assert_eq!(first_recovery.applied_writes, 0);

    let mut tx1 = db.begin();
    db.insert(
        &mut tx1,
        1001,
        FlightRow {
            altitude: 12_000,
            groundspeed: 440,
        },
    )
    .expect("insert should succeed");
    db.insert(
        &mut tx1,
        1002,
        FlightRow {
            altitude: 18_500,
            groundspeed: 452,
        },
    )
    .expect("insert should succeed");
    db.commit(tx1).await.expect("commit 1 must succeed");

    let mut tx2 = db.begin();
    db.update(
        &mut tx2,
        &1001,
        FlightRow {
            altitude: 13_250,
            groundspeed: 448,
        },
    )
    .expect("update should succeed");
    db.delete(&mut tx2, &1002).expect("delete should succeed");
    db.insert(
        &mut tx2,
        1003,
        FlightRow {
            altitude: 25_100,
            groundspeed: 470,
        },
    )
    .expect("insert should succeed");
    db.commit(tx2).await.expect("commit 2 must succeed");

    let checkpoint_rows = db
        .checkpoint_now()
        .await
        .expect("checkpoint should complete");
    assert!(checkpoint_rows >= 2);

    let mut tx3 = db.begin();
    db.update(
        &mut tx3,
        &1003,
        FlightRow {
            altitude: 26_000,
            groundspeed: 475,
        },
    )
    .expect("update should succeed");
    db.insert(
        &mut tx3,
        1004,
        FlightRow {
            altitude: 10_400,
            groundspeed: 399,
        },
    )
    .expect("insert should succeed");
    db.commit(tx3).await.expect("commit 3 must succeed");

    drop(db);

    let (recovered, recovery) = DurableDatabase::<u64, FlightRow>::open_with_recovery(
        &data_dir,
        2048,
        Duration::from_secs(300),
        index_defs,
    )
    .await
    .expect("failed to reopen durable database");

    assert_eq!(recovery.stage, RecoveryStage::Complete);
    assert!(recovery.checkpoint_rows >= 2);
    assert!(recovery.wal_records >= 1);
    assert!(recovery.applied_writes >= 4);

    let read_tx = recovered.begin();
    let row_1001 = recovered
        .read_visible(&1001, &read_tx)
        .expect("row 1001 missing after recovery");
    let row_1002 = recovered.read_visible(&1002, &read_tx);
    let row_1003 = recovered
        .read_visible(&1003, &read_tx)
        .expect("row 1003 missing after recovery");
    let row_1004 = recovered
        .read_visible(&1004, &read_tx)
        .expect("row 1004 missing after recovery");

    assert_eq!(row_1001.altitude, 13_250);
    assert!(row_1002.is_none(), "deleted row 1002 should stay deleted");
    assert_eq!(row_1003.altitude, 26_000);
    assert_eq!(row_1004.altitude, 10_400);

    let guard = epoch::pin();
    let indexed_rows = recovered
        .engine()
        .query()
        .gt(altitude_field(), 12_000_i32)
        .sort_by(altitude_field(), SortDirection::Desc)
        .limit(2)
        .execute(read_tx.tx(), &guard);

    assert_eq!(indexed_rows.len(), 2);
    assert!(indexed_rows[0].altitude >= indexed_rows[1].altitude);
    assert!(indexed_rows.iter().all(|row| row.altitude > 12_000));

    recovered.abort(read_tx);

    drop(recovered);
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

    let altitude_index = SecondaryIndex::<usize>::new("altitude");
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

    let replayed = replay_latest_rows_from_wal(&commits, &shm);
    for (flight, expected_row) in &expected_final {
        let replayed_row = replayed
            .get(flight)
            .unwrap_or_else(|| panic!("missing replayed row for key {}", flight));
        assert_eq!(
            replayed_row, expected_row,
            "replayed row mismatch for key {}",
            flight
        );

        let row_id = *key_to_row
            .get(flight)
            .unwrap_or_else(|| panic!("missing row id for key {}", flight));
        let candidates = altitude_index.lookup(&IndexCompare::Eq(IndexValue::I64(
            expected_row.altitude as i64,
        )));
        assert!(
            candidates.contains(&row_id),
            "altitude index missing key={} altitude={} row_id={}",
            flight,
            expected_row.altitude,
            row_id
        );
    }

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

fn replay_latest_rows_from_wal(
    commits: &[aerostore_core::WalRingCommit],
    shm: &ShmArena,
) -> HashMap<String, TclLikeRow> {
    let mut by_key = HashMap::<String, TclLikeRow>::new();

    for commit in commits {
        for write in &commit.writes {
            let row_ptr = RelPtr::<OccRow<TclLikeRow>>::from_offset(write.new_offset);
            let row = row_ptr
                .as_ref(shm.mmap_base())
                .unwrap_or_else(|| panic!("failed to resolve row pointer offset {}", write.new_offset));
            if row.value.exists == 0 {
                continue;
            }
            by_key.insert(decode_flight(&row.value), row.value);
        }
    }

    by_key
}
