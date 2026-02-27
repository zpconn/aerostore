#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use aerostore_core::{
    deserialize_commit_record, spawn_wal_writer_daemon, IndexValue, LogicalDatabase,
    LogicalDatabaseConfig, OccCommitter, OccTable, SecondaryIndex, SharedWalRing, ShmArena,
    WalRingCommit, WalRingWrite, WalWriterDaemon,
};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};

const TXNS: usize = 10_000;
const ROW_ID: usize = 0;
const RING_SLOTS: usize = 1024;
const RING_SLOT_BYTES: usize = 128;
const TCL_BENCH_KEYS: usize = 64;
const SAVEPOINT_CHURN_TXNS: usize = 5_000;
const SAVEPOINT_CHURN_WRITES: usize = 6;
const LOGICAL_RING_SLOTS: usize = 1024;
const LOGICAL_RING_SLOT_BYTES: usize = 256;

#[test]
fn benchmark_async_synchronous_commit_modes() {
    let root = std::env::temp_dir().join("aerostore_wal_ring_benchmark");
    fs::create_dir_all(&root).expect("failed to create wal_ring benchmark directory");

    let sync_wal = root.join("sync_mode.wal");
    let async_wal = root.join("async_mode.wal");
    remove_if_exists(&sync_wal);
    remove_if_exists(&async_wal);

    let sync_tps = run_synchronous_benchmark(&sync_wal);
    let async_tps = run_asynchronous_benchmark(&async_wal);
    let ratio = async_tps / sync_tps.max(1.0);

    eprintln!(
        "wal_ring_benchmark: txns={} sync_tps={:.2} async_tps={:.2} ratio={:.2}x",
        TXNS, sync_tps, async_tps, ratio
    );

    assert!(
        ratio >= 10.0,
        "expected async synchronous_commit=off throughput to be >=10x sync mode; observed {:.2}x",
        ratio
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TclBenchRow {
    exists: u8,
    flight: [u8; 12],
    altitude: i32,
    gs: u16,
}

impl TclBenchRow {
    fn new(flight: &str, altitude: i32, gs: u16) -> Self {
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

#[test]
fn benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts() {
    let root = std::env::temp_dir().join("aerostore_wal_ring_benchmark_tcl_like");
    fs::create_dir_all(&root).expect("failed to create tcl-like wal_ring benchmark directory");

    let sync_wal = root.join("sync_mode_tcl_like.wal");
    let async_wal = root.join("async_mode_tcl_like.wal");
    remove_if_exists(&sync_wal);
    remove_if_exists(&async_wal);

    let sync_tps = run_synchronous_tcl_like_benchmark(&sync_wal);
    let async_tps = run_asynchronous_tcl_like_benchmark(&async_wal);
    let ratio = async_tps / sync_tps.max(1.0);

    eprintln!(
        "wal_ring_tcl_like_benchmark: txns={} keys={} sync_tps={:.2} async_tps={:.2} ratio={:.2}x",
        TXNS, TCL_BENCH_KEYS, sync_tps, async_tps, ratio
    );

    assert!(
        ratio >= 10.0,
        "expected async tcl-like upsert throughput to be >=10x sync mode; observed {:.2}x",
        ratio
    );
}

#[test]
fn benchmark_async_synchronous_commit_modes_savepoint_churn() {
    let root = std::env::temp_dir().join("aerostore_wal_ring_benchmark_savepoint_churn");
    fs::create_dir_all(&root).expect("failed to create savepoint wal_ring benchmark directory");

    let sync_wal = root.join("sync_mode_savepoint_churn.wal");
    let async_wal = root.join("async_mode_savepoint_churn.wal");
    remove_if_exists(&sync_wal);
    remove_if_exists(&async_wal);

    let sync_tps = run_synchronous_savepoint_churn_benchmark(&sync_wal);
    let async_tps = run_asynchronous_savepoint_churn_benchmark(&async_wal);
    let ratio = async_tps / sync_tps.max(1.0);

    eprintln!(
        "wal_ring_savepoint_churn_benchmark: txns={} rolled_back_writes_per_tx={} sync_tps={:.2} async_tps={:.2} ratio={:.2}x",
        SAVEPOINT_CHURN_TXNS,
        SAVEPOINT_CHURN_WRITES,
        sync_tps,
        async_tps,
        ratio
    );

    assert!(
        ratio >= 10.0,
        "expected async savepoint churn throughput to be >=10x sync mode; observed {:.2}x",
        ratio
    );
}

#[test]
fn benchmark_logical_async_vs_sync_commit_modes() {
    let root = std::env::temp_dir().join("aerostore_logical_wal_benchmark");
    fs::create_dir_all(&root).expect("failed to create logical wal benchmark directory");

    let sync_tps = run_logical_benchmark(&root.join("sync"), true);
    let async_tps = run_logical_benchmark(&root.join("async"), false);
    let ratio = async_tps / sync_tps.max(1.0);

    eprintln!(
        "logical_wal_benchmark: txns={} sync_tps={:.2} async_tps={:.2} ratio={:.2}x",
        TXNS, sync_tps, async_tps, ratio
    );

    assert!(
        ratio >= 10.0,
        "expected logical async synchronous_commit=off throughput to be >=10x sync mode; observed {:.2}x",
        ratio
    );
}

#[test]
fn wal_ring_backpressure_blocks_producers_without_corruption() {
    const TOTAL_MESSAGES: usize = 240;
    const BACKPRESSURE_SLOTS: usize = 4;
    const BACKPRESSURE_SLOT_BYTES: usize = 256;

    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create backpressure shm arena"));
    let ring =
        SharedWalRing::<BACKPRESSURE_SLOTS, BACKPRESSURE_SLOT_BYTES>::create(Arc::clone(&shm))
            .expect("failed to create tiny backpressure ring");

    let done = Arc::new(AtomicBool::new(false));

    let consumer = {
        let ring = ring.clone();
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut received = Vec::<(u64, u64)>::with_capacity(TOTAL_MESSAGES);
            loop {
                match ring.pop_bytes() {
                    Ok(Some(payload)) => {
                        let msg =
                            deserialize_commit_record(payload.as_slice()).expect("decode failed");
                        assert_eq!(
                            msg.writes.len(),
                            1,
                            "backpressure payload should contain exactly one write"
                        );
                        let write = &msg.writes[0];
                        assert_eq!(
                            write.value_payload.len(),
                            std::mem::size_of::<u64>(),
                            "backpressure payload value length mismatch"
                        );
                        let mut payload_buf = [0_u8; 8];
                        payload_buf.copy_from_slice(write.value_payload.as_slice());
                        let payload_value = u64::from_le_bytes(payload_buf);
                        received.push((msg.txid, payload_value));
                        std::thread::sleep(std::time::Duration::from_micros(400));
                    }
                    Ok(None) => {
                        if done.load(Ordering::Acquire)
                            && ring.is_empty().expect("ring empty query failed")
                        {
                            break received;
                        }
                        std::thread::yield_now();
                    }
                    Err(err) => panic!("backpressure consumer ring error: {}", err),
                }
            }
        })
    };

    let start = Instant::now();
    for txid in 1..=TOTAL_MESSAGES as u64 {
        let payload_value = txid.rotate_left(17) ^ 0xA5A5_5A5A_5A5A_A5A5_u64;
        let msg = WalRingCommit {
            txid,
            writes: vec![WalRingWrite {
                row_id: 0,
                base_offset: txid as u32 - 1,
                new_offset: txid as u32,
                value_payload: payload_value.to_le_bytes().to_vec(),
            }],
        };
        ring.push_commit_record(&msg)
            .expect("producer push failed during backpressure");
    }
    let producer_elapsed = start.elapsed();

    done.store(true, Ordering::Release);
    let mut consumed = consumer.join().expect("backpressure consumer panicked");

    assert_eq!(
        consumed.len(),
        TOTAL_MESSAGES,
        "ring consumer lost messages under producer backpressure"
    );
    consumed.sort_unstable_by_key(|entry| entry.0);
    for (idx, (txid, payload_value)) in consumed.iter().enumerate() {
        let expected_txid = (idx + 1) as u64;
        assert_eq!(
            *txid, expected_txid,
            "ring payload corruption or duplicate/missing txid detected"
        );
        let expected_payload = expected_txid.rotate_left(17) ^ 0xA5A5_5A5A_5A5A_A5A5_u64;
        assert_eq!(
            *payload_value, expected_payload,
            "ring payload value corruption detected for txid {}",
            expected_txid
        );
    }

    assert!(
        producer_elapsed >= std::time::Duration::from_millis(40),
        "producer did not block/yield under full ring pressure (elapsed={:?})",
        producer_elapsed
    );
}

fn run_synchronous_benchmark(wal_path: &Path) -> f64 {
    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to create sync benchmark shm arena"));
    let table = OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create sync occ table");
    table
        .seed_row(ROW_ID, 0_u64)
        .expect("failed to seed sync benchmark row");

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(wal_path)
        .expect("failed to create synchronous committer");

    let start = Instant::now();
    for _ in 0..TXNS {
        let mut tx = table
            .begin_transaction()
            .expect("sync benchmark begin_transaction failed");
        let value = table
            .read(&mut tx, ROW_ID)
            .expect("sync benchmark read failed")
            .expect("sync benchmark row missing");
        table
            .write(&mut tx, ROW_ID, value + 1)
            .expect("sync benchmark write failed");
        committer
            .commit(&table, &mut tx)
            .expect("sync benchmark commit failed");
    }
    let elapsed = start.elapsed();

    let mut verify = table
        .begin_transaction()
        .expect("sync benchmark verify begin_transaction failed");
    let final_value = table
        .read(&mut verify, ROW_ID)
        .expect("sync benchmark verify read failed")
        .expect("sync benchmark verify row missing");
    table
        .abort(&mut verify)
        .expect("sync benchmark verify abort failed");
    assert_eq!(final_value, TXNS as u64);

    TXNS as f64 / elapsed.as_secs_f64()
}

fn run_asynchronous_benchmark(wal_path: &Path) -> f64 {
    let shm =
        Arc::new(ShmArena::new(64 << 20).expect("failed to create async benchmark shm arena"));
    let table =
        OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create async occ table");
    table
        .seed_row(ROW_ID, 0_u64)
        .expect("failed to seed async benchmark row");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon: WalWriterDaemon =
        spawn_wal_writer_daemon(ring.clone(), wal_path).expect("failed to spawn wal writer daemon");

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let start = Instant::now();
    for _ in 0..TXNS {
        let mut tx = table
            .begin_transaction()
            .expect("async benchmark begin_transaction failed");
        let value = table
            .read(&mut tx, ROW_ID)
            .expect("async benchmark read failed")
            .expect("async benchmark row missing");
        table
            .write(&mut tx, ROW_ID, value + 1)
            .expect("async benchmark write failed");
        committer
            .commit(&table, &mut tx)
            .expect("async benchmark commit failed");
    }
    let producer_elapsed = start.elapsed();

    ring.close().expect("failed to close wal ring");
    daemon.join().expect("wal writer daemon join failed");

    let mut verify = table
        .begin_transaction()
        .expect("async benchmark verify begin_transaction failed");
    let final_value = table
        .read(&mut verify, ROW_ID)
        .expect("async benchmark verify read failed")
        .expect("async benchmark verify row missing");
    table
        .abort(&mut verify)
        .expect("async benchmark verify abort failed");
    assert_eq!(final_value, TXNS as u64);

    TXNS as f64 / producer_elapsed.as_secs_f64()
}

fn run_synchronous_tcl_like_benchmark(wal_path: &Path) -> f64 {
    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create sync tcl-like shm arena"));
    let table = OccTable::<TclBenchRow>::new(Arc::clone(&shm), TCL_BENCH_KEYS)
        .expect("failed to create sync tcl-like occ table");
    let key_index = SkipMap::<String, usize>::new();
    let altitude_index = SecondaryIndex::<usize>::new("altitude");

    for row_id in 0..TCL_BENCH_KEYS {
        let flight = format!("FLT{:03}", row_id);
        let row = TclBenchRow::new(flight.as_str(), 10_000 + row_id as i32, 350);
        table
            .seed_row(row_id, row)
            .expect("failed to seed sync tcl-like row");
        key_index.insert(flight, row_id);
        altitude_index.insert(IndexValue::I64(row.altitude as i64), row_id);
    }

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(wal_path)
        .expect("failed to create synchronous tcl-like committer");

    let start = Instant::now();
    for i in 0..TXNS {
        let key_slot = i % TCL_BENCH_KEYS;
        let key = format!("FLT{:03}", key_slot);
        let row_id = key_index
            .get(key.as_str())
            .map(|entry| *entry.value())
            .expect("missing keyed row in sync tcl-like benchmark");

        let mut tx = table
            .begin_transaction()
            .expect("sync tcl-like begin_transaction failed");
        let mut current = table
            .read(&mut tx, row_id)
            .expect("sync tcl-like read failed")
            .expect("sync tcl-like row missing");

        let old_altitude = current.altitude;
        current.altitude += 1;
        current.gs = current.gs.wrapping_add(1);
        table
            .write(&mut tx, row_id, current)
            .expect("sync tcl-like write failed");
        committer
            .commit(&table, &mut tx)
            .expect("sync tcl-like commit failed");

        altitude_index.remove(&IndexValue::I64(old_altitude as i64), &row_id);
        altitude_index.insert(IndexValue::I64(current.altitude as i64), row_id);
    }
    let elapsed = start.elapsed();

    TXNS as f64 / elapsed.as_secs_f64()
}

fn run_asynchronous_tcl_like_benchmark(wal_path: &Path) -> f64 {
    let shm = Arc::new(ShmArena::new(96 << 20).expect("failed to create async tcl-like shm arena"));
    let table = OccTable::<TclBenchRow>::new(Arc::clone(&shm), TCL_BENCH_KEYS)
        .expect("failed to create async tcl-like occ table");
    let key_index = SkipMap::<String, usize>::new();
    let altitude_index = SecondaryIndex::<usize>::new("altitude");

    for row_id in 0..TCL_BENCH_KEYS {
        let flight = format!("FLT{:03}", row_id);
        let row = TclBenchRow::new(flight.as_str(), 10_000 + row_id as i32, 350);
        table
            .seed_row(row_id, row)
            .expect("failed to seed async tcl-like row");
        key_index.insert(flight, row_id);
        altitude_index.insert(IndexValue::I64(row.altitude as i64), row_id);
    }

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon: WalWriterDaemon =
        spawn_wal_writer_daemon(ring.clone(), wal_path).expect("failed to spawn wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let start = Instant::now();
    for i in 0..TXNS {
        let key_slot = i % TCL_BENCH_KEYS;
        let key = format!("FLT{:03}", key_slot);
        let row_id = key_index
            .get(key.as_str())
            .map(|entry| *entry.value())
            .expect("missing keyed row in async tcl-like benchmark");

        let mut tx = table
            .begin_transaction()
            .expect("async tcl-like begin_transaction failed");
        let mut current = table
            .read(&mut tx, row_id)
            .expect("async tcl-like read failed")
            .expect("async tcl-like row missing");

        let old_altitude = current.altitude;
        current.altitude += 1;
        current.gs = current.gs.wrapping_add(1);
        table
            .write(&mut tx, row_id, current)
            .expect("async tcl-like write failed");
        committer
            .commit(&table, &mut tx)
            .expect("async tcl-like commit failed");

        altitude_index.remove(&IndexValue::I64(old_altitude as i64), &row_id);
        altitude_index.insert(IndexValue::I64(current.altitude as i64), row_id);
    }
    let producer_elapsed = start.elapsed();

    ring.close().expect("failed to close wal ring");
    daemon.join().expect("wal writer daemon join failed");

    TXNS as f64 / producer_elapsed.as_secs_f64()
}

fn run_synchronous_savepoint_churn_benchmark(wal_path: &Path) -> f64 {
    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create sync churn shm arena"));
    let table =
        OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create churn occ table");
    table
        .seed_row(ROW_ID, 0_u64)
        .expect("failed to seed churn benchmark row");

    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(wal_path)
        .expect("failed to create synchronous churn committer");

    let start = Instant::now();
    for _ in 0..SAVEPOINT_CHURN_TXNS {
        let mut tx = table
            .begin_transaction()
            .expect("sync churn begin_transaction failed");
        let value = table
            .read(&mut tx, ROW_ID)
            .expect("sync churn read failed")
            .expect("sync churn row missing");

        table
            .savepoint(&mut tx, "sp")
            .expect("sync churn savepoint failed");
        for step in 0..SAVEPOINT_CHURN_WRITES {
            table
                .write(&mut tx, ROW_ID, value + step as u64 + 10)
                .expect("sync churn write failed");
            table
                .rollback_to(&mut tx, "sp")
                .expect("sync churn rollback_to failed");
        }

        table
            .write(&mut tx, ROW_ID, value + 1)
            .expect("sync churn final write failed");
        committer
            .commit(&table, &mut tx)
            .expect("sync churn commit failed");
    }
    let elapsed = start.elapsed();

    let mut verify = table
        .begin_transaction()
        .expect("sync churn verify begin_transaction failed");
    let final_value = table
        .read(&mut verify, ROW_ID)
        .expect("sync churn verify read failed")
        .expect("sync churn verify row missing");
    table
        .abort(&mut verify)
        .expect("sync churn verify abort failed");
    assert_eq!(final_value, SAVEPOINT_CHURN_TXNS as u64);

    SAVEPOINT_CHURN_TXNS as f64 / elapsed.as_secs_f64()
}

fn run_asynchronous_savepoint_churn_benchmark(wal_path: &Path) -> f64 {
    let shm = Arc::new(ShmArena::new(96 << 20).expect("failed to create async churn shm arena"));
    let table =
        OccTable::<u64>::new(Arc::clone(&shm), 1).expect("failed to create churn occ table");
    table
        .seed_row(ROW_ID, 0_u64)
        .expect("failed to seed churn benchmark row");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let daemon: WalWriterDaemon =
        spawn_wal_writer_daemon(ring.clone(), wal_path).expect("failed to spawn wal writer daemon");
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone());

    let start = Instant::now();
    for _ in 0..SAVEPOINT_CHURN_TXNS {
        let mut tx = table
            .begin_transaction()
            .expect("async churn begin_transaction failed");
        let value = table
            .read(&mut tx, ROW_ID)
            .expect("async churn read failed")
            .expect("async churn row missing");

        table
            .savepoint(&mut tx, "sp")
            .expect("async churn savepoint failed");
        for step in 0..SAVEPOINT_CHURN_WRITES {
            table
                .write(&mut tx, ROW_ID, value + step as u64 + 10)
                .expect("async churn write failed");
            table
                .rollback_to(&mut tx, "sp")
                .expect("async churn rollback_to failed");
        }

        table
            .write(&mut tx, ROW_ID, value + 1)
            .expect("async churn final write failed");
        committer
            .commit(&table, &mut tx)
            .expect("async churn commit failed");
    }
    let producer_elapsed = start.elapsed();

    ring.close().expect("failed to close wal ring");
    daemon.join().expect("wal writer daemon join failed");

    let mut verify = table
        .begin_transaction()
        .expect("async churn verify begin_transaction failed");
    let final_value = table
        .read(&mut verify, ROW_ID)
        .expect("async churn verify read failed")
        .expect("async churn verify row missing");
    table
        .abort(&mut verify)
        .expect("async churn verify abort failed");
    assert_eq!(final_value, SAVEPOINT_CHURN_TXNS as u64);

    SAVEPOINT_CHURN_TXNS as f64 / producer_elapsed.as_secs_f64()
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

fn run_logical_benchmark(data_dir: &Path, synchronous_commit: bool) -> f64 {
    fs::create_dir_all(data_dir).expect("failed to create logical benchmark data dir");
    remove_if_exists(&data_dir.join("aerostore_logical.wal"));
    remove_if_exists(&data_dir.join("snapshot.dat"));

    let db = LogicalDatabase::<LOGICAL_RING_SLOTS, LOGICAL_RING_SLOT_BYTES>::boot_from_disk(
        LogicalDatabaseConfig {
            data_dir: data_dir.to_path_buf(),
            table_name: "flight_state".to_string(),
            row_capacity: 1,
            shm_bytes: 64 << 20,
            synchronous_commit,
        },
    )
    .expect("failed to boot logical benchmark database");

    let start = Instant::now();
    for i in 0..TXNS {
        let payload = (i as u64).to_le_bytes();
        db.upsert("UAL123", payload.as_slice())
            .expect("logical benchmark upsert failed");
    }
    let elapsed = start.elapsed();

    let final_payload = db
        .payload_for_key("UAL123")
        .expect("logical benchmark payload lookup failed")
        .expect("logical benchmark key missing");
    assert_eq!(
        final_payload,
        ((TXNS - 1) as u64).to_le_bytes().to_vec(),
        "logical benchmark final payload mismatch"
    );
    db.shutdown()
        .expect("failed to shutdown logical benchmark database");

    TXNS as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}
