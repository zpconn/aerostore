use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::epoch;
use serde::{Deserialize, Serialize};

use aerostore_core::{
    deserialize_commit_record, spawn_wal_writer_daemon, DurableDatabase, Field, IndexDefinition,
    OccCommitter, OccTable, RecoveryStage, SharedWalRing, ShmArena, SortDirection,
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
