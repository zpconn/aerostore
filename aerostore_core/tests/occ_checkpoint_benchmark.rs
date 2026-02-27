#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::{write_occ_checkpoint_and_truncate_wal, OccTable, ShmArena};

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
