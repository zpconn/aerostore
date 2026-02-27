#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use aerostore_core::{
    deserialize_commit_record, spawn_wal_writer_daemon, OccCommitter, OccTable, SharedWalRing,
    ShmArena, WalRingCommit, WalRingWrite, WalWriterDaemon,
};

const TXNS: usize = 10_000;
const ROW_ID: usize = 0;
const RING_SLOTS: usize = 1024;
const RING_SLOT_BYTES: usize = 128;

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
            let mut received = Vec::<u64>::with_capacity(TOTAL_MESSAGES);
            loop {
                match ring.pop_bytes() {
                    Ok(Some(payload)) => {
                        let msg =
                            deserialize_commit_record(payload.as_slice()).expect("decode failed");
                        received.push(msg.txid);
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
        let msg = WalRingCommit {
            txid,
            writes: vec![WalRingWrite {
                row_id: 0,
                base_offset: txid as u32 - 1,
                new_offset: txid as u32,
            }],
        };
        ring.push_commit_record(&msg)
            .expect("producer push failed during backpressure");
    }
    let producer_elapsed = start.elapsed();

    done.store(true, Ordering::Release);
    let mut consumed_txids = consumer.join().expect("backpressure consumer panicked");

    assert_eq!(
        consumed_txids.len(),
        TOTAL_MESSAGES,
        "ring consumer lost messages under producer backpressure"
    );
    consumed_txids.sort_unstable();
    for (idx, txid) in consumed_txids.iter().enumerate() {
        assert_eq!(
            *txid,
            (idx + 1) as u64,
            "ring payload corruption or duplicate/missing txid detected"
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

fn remove_if_exists(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}
