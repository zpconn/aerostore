#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::{
    append_logical_wal_record, write_occ_checkpoint_and_truncate_wal, LogicalDatabase,
    LogicalDatabaseConfig, LogicalWalRecord, OccTable, ShmArena,
};
use serde::{Deserialize, Serialize};

fn tmp_data_dir(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nonce}"))
}

fn shm_bytes_for_rows(rows: usize) -> usize {
    let estimated = rows.saturating_mul(96).saturating_add(16 << 20);
    estimated.max(64 << 20).next_power_of_two()
}

fn logical_shm_bytes_for_rows(rows: usize) -> usize {
    let estimated = rows.saturating_mul(1_600).saturating_add(64 << 20);
    estimated.max(256 << 20).next_power_of_two()
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotRowCompat {
    pk: String,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFileCompat {
    version: u32,
    table_name: String,
    checkpoint_txid: u64,
    rows: Vec<SnapshotRowCompat>,
}

#[test]
fn benchmark_occ_checkpoint_latency_by_cardinality() {
    let cardinalities = [10_000_usize, 100_000_usize, 1_000_000_usize];
    let mut results = Vec::with_capacity(cardinalities.len());

    for rows in cardinalities {
        let data_dir = tmp_data_dir("aerostore_occ_checkpoint_bench");
        std::fs::create_dir_all(&data_dir).expect("failed to create benchmark temp dir");
        let wal_path = data_dir.join("bench_checkpoint.wal");
        let checkpoint_path = data_dir.join("bench_checkpoint.dat");

        let shm = Arc::new(
            ShmArena::new(shm_bytes_for_rows(rows))
                .unwrap_or_else(|err| panic!("failed to allocate shm for rows={rows}: {err}")),
        );
        let table = OccTable::<u64>::new(Arc::clone(&shm), rows)
            .unwrap_or_else(|err| panic!("failed to create occ table for rows={rows}: {err}"));

        for row_id in 0..rows {
            table
                .seed_row(row_id, row_id as u64)
                .unwrap_or_else(|err| panic!("failed to seed row_id={row_id} rows={rows}: {err}"));
        }

        let start = Instant::now();
        let checkpoint_rows =
            write_occ_checkpoint_and_truncate_wal(&table, &checkpoint_path, &wal_path)
                .unwrap_or_else(|err| panic!("checkpoint failed for rows={rows}: {err}"));
        let elapsed = start.elapsed();
        let per_row_ns = elapsed.as_nanos() as f64 / rows as f64;

        assert_eq!(
            checkpoint_rows, rows,
            "checkpoint row count mismatch for cardinality {}",
            rows
        );
        let checkpoint_size = std::fs::metadata(&checkpoint_path)
            .expect("checkpoint metadata query failed")
            .len();
        assert!(
            checkpoint_size > 0,
            "checkpoint file should be non-empty for cardinality {}",
            rows
        );
        let wal_size = std::fs::metadata(&wal_path)
            .expect("wal metadata query failed")
            .len();
        assert_eq!(
            wal_size, 0,
            "wal should be truncated to zero for cardinality {}",
            rows
        );

        results.push((rows, elapsed, per_row_ns, checkpoint_size));
        let _ = std::fs::remove_dir_all(data_dir);
    }

    for (rows, elapsed, per_row_ns, checkpoint_size) in &results {
        eprintln!(
            "occ_checkpoint_latency_benchmark: rows={} elapsed={:?} per_row_ns={:.2} checkpoint_bytes={}",
            rows, elapsed, per_row_ns, checkpoint_size
        );
    }
}

#[test]
fn benchmark_logical_snapshot_and_replay_recovery_by_cardinality() {
    let cardinalities = [10_000_usize, 100_000_usize, 1_000_000_usize];
    let mut results = Vec::with_capacity(cardinalities.len());

    for rows in cardinalities {
        let data_dir = tmp_data_dir("aerostore_logical_recovery_bench");
        std::fs::create_dir_all(&data_dir).expect("failed to create logical benchmark temp dir");

        let snapshot_path = data_dir.join("snapshot.dat");
        let wal_path = data_dir.join("aerostore_logical.wal");
        let checkpoint_txid = rows as u64;

        let mut snapshot_rows = Vec::with_capacity(rows);
        for i in 0..rows {
            snapshot_rows.push(SnapshotRowCompat {
                pk: format!("K{:07}", i),
                payload: vec![(i % 251) as u8, (i % 97) as u8],
            });
        }
        let snapshot = SnapshotFileCompat {
            version: 1,
            table_name: "flight_state".to_string(),
            checkpoint_txid,
            rows: snapshot_rows,
        };
        let snapshot_bytes =
            bincode::serialize(&snapshot).expect("failed to serialize logical snapshot fixture");
        std::fs::write(&snapshot_path, snapshot_bytes).expect("failed to write snapshot fixture");

        let updates = 1_000_usize.min(rows);
        let deletes = 250_usize.min(rows.saturating_sub(updates));
        let inserts = 1_000_usize;
        let mut txid = checkpoint_txid;
        for i in 0..updates {
            txid += 1;
            append_logical_wal_record(
                &wal_path,
                &LogicalWalRecord::Upsert {
                    txid,
                    table: "flight_state".to_string(),
                    pk: format!("K{:07}", i),
                    payload: vec![0xAA, (i % 255) as u8],
                },
            )
            .expect("failed to append logical update wal fixture");
            append_logical_wal_record(&wal_path, &LogicalWalRecord::Commit { txid })
                .expect("failed to append logical update commit");
        }
        for i in updates..(updates + deletes) {
            txid += 1;
            append_logical_wal_record(
                &wal_path,
                &LogicalWalRecord::Delete {
                    txid,
                    table: "flight_state".to_string(),
                    pk: format!("K{:07}", i),
                },
            )
            .expect("failed to append logical delete wal fixture");
            append_logical_wal_record(&wal_path, &LogicalWalRecord::Commit { txid })
                .expect("failed to append logical delete commit");
        }
        for i in 0..inserts {
            txid += 1;
            append_logical_wal_record(
                &wal_path,
                &LogicalWalRecord::Upsert {
                    txid,
                    table: "flight_state".to_string(),
                    pk: format!("KNEW{:07}", i),
                    payload: vec![0xCC, (i % 255) as u8],
                },
            )
            .expect("failed to append logical insert wal fixture");
            append_logical_wal_record(&wal_path, &LogicalWalRecord::Commit { txid })
                .expect("failed to append logical insert commit");
        }

        let cfg = LogicalDatabaseConfig {
            data_dir: data_dir.clone(),
            table_name: "flight_state".to_string(),
            row_capacity: rows + inserts + 1_024,
            shm_bytes: logical_shm_bytes_for_rows(rows + inserts + 1_024),
            synchronous_commit: true,
        };

        let start = Instant::now();
        let recovered = LogicalDatabase::<64, 256>::boot_from_disk(cfg).unwrap_or_else(|err| {
            panic!("logical recovery benchmark boot failed rows={rows}: {err}")
        });
        let elapsed = start.elapsed();

        let expected_rows = rows - deletes + inserts;
        let observed_rows = recovered
            .row_count()
            .unwrap_or_else(|err| panic!("row_count failed rows={rows}: {err}"));
        assert_eq!(
            observed_rows, expected_rows,
            "logical replay row count mismatch for rows={}",
            rows
        );
        recovered
            .shutdown()
            .unwrap_or_else(|err| panic!("shutdown failed rows={rows}: {err}"));

        let snapshot_bytes = std::fs::metadata(&snapshot_path)
            .expect("snapshot metadata failed")
            .len();
        let wal_bytes = std::fs::metadata(&wal_path)
            .expect("wal metadata failed")
            .len();
        let rows_per_sec = expected_rows as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
        results.push((rows, elapsed, rows_per_sec, snapshot_bytes, wal_bytes));

        let _ = std::fs::remove_dir_all(data_dir);
    }

    for (rows, elapsed, rows_per_sec, snapshot_bytes, wal_bytes) in &results {
        eprintln!(
            "logical_snapshot_replay_benchmark: rows={} elapsed={:?} rows_per_sec={:.2} snapshot_bytes={} wal_bytes={}",
            rows, elapsed, rows_per_sec, snapshot_bytes, wal_bytes
        );
    }
}
