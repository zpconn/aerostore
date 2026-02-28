use std::sync::{Arc, Barrier};
use std::thread;

use aerostore_core::{
    spawn_wal_writer_daemon, IndexValue, OccCommitter, OccError, OccTable, RuleBasedOptimizer,
    SchemaCatalog, SecondaryIndex, SharedWalRing, ShmArena, ShmPrimaryKeyMap, StapiRow, StapiValue,
};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};

const RING_SLOTS: usize = 256;
const RING_SLOT_BYTES: usize = 128;
const OCC_PARTITION_LOCK_BUCKETS: usize = 1024;

#[test]
fn ssi_rejects_write_skew_and_returns_serialization_failure() {
    run_write_skew_scenario(Mode::Synchronous);
    run_write_skew_scenario(Mode::Asynchronous);
}

#[test]
fn ssi_rejects_write_skew_across_distinct_partition_lock_buckets() {
    let (row_a, row_b) = find_distinct_bucket_rows();
    assert_ne!(
        lock_bucket_for_row_id(row_a),
        lock_bucket_for_row_id(row_b),
        "test setup must use rows in different OCC partition buckets"
    );

    run_write_skew_for_rows(Mode::Synchronous, row_a, row_b, "bucketed_sync");
    run_write_skew_for_rows(Mode::Asynchronous, row_a, row_b, "bucketed_async");
}

#[test]
fn ssi_rejects_write_skew_when_reads_flow_through_stapi_planner() {
    run_write_skew_with_planner_reads(Mode::Synchronous);
    run_write_skew_with_planner_reads(Mode::Asynchronous);
}

#[test]
fn ssi_rejects_write_skew_when_reads_flow_through_pk_rbo_route() {
    run_write_skew_with_pk_route_reads(Mode::Synchronous);
    run_write_skew_with_pk_route_reads(Mode::Asynchronous);
}

#[test]
fn ssi_rejects_write_skew_for_tcl_like_keyed_upserts() {
    run_write_skew_with_tcl_like_keyed_upserts(Mode::Synchronous);
    run_write_skew_with_tcl_like_keyed_upserts(Mode::Asynchronous);
}

#[test]
fn ssi_rejects_write_skew_with_savepoint_heavy_write_paths() {
    run_write_skew_with_savepoint_churn(Mode::Synchronous);
    run_write_skew_with_savepoint_churn(Mode::Asynchronous);
}

#[derive(Clone, Copy)]
enum Mode {
    Synchronous,
    Asynchronous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OnCallRow {
    doctor: [u8; 8],
    on_call: i64,
}

impl OnCallRow {
    fn new(doctor: &str, on_call: i64) -> Self {
        Self {
            doctor: fixed_ascii::<8>(doctor),
            on_call,
        }
    }
}

impl StapiRow for OnCallRow {
    fn has_field(field: &str) -> bool {
        matches!(field, "doctor" | "on_call")
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        match field {
            "doctor" => Some(StapiValue::Text(decode_ascii(&self.doctor))),
            "on_call" => Some(StapiValue::Int(self.on_call)),
            _ => None,
        }
    }
}

fn fixed_ascii<const N: usize>(value: &str) -> [u8; N] {
    let mut out = [0_u8; N];
    let bytes = value.as_bytes();
    let len = bytes.len().min(N);
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

fn decode_ascii(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|v| *v == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).to_string()
}

fn run_write_skew_scenario(mode: Mode) {
    run_write_skew_for_rows(mode, 0, 1, "baseline");
}

fn run_write_skew_for_rows(mode: Mode, row_a: usize, row_b: usize, label: &str) {
    let capacity = row_a.max(row_b) + 1;
    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to create shared arena"));
    let table = Arc::new(
        OccTable::<bool>::new(Arc::clone(&shm), capacity).expect("failed to create OCC table"),
    );

    table
        .seed_row(row_a, true)
        .expect("failed to seed first skew row as on-call");
    table
        .seed_row(row_b, true)
        .expect("failed to seed second skew row as on-call");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_{}_{}_{}_{}.wal",
        label,
        row_a,
        row_b,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("tx A begin_transaction failed");
            let row_a_value = table
                .read(&mut tx, row_a)
                .expect("tx A read first row failed");
            let row_b_value = table
                .read(&mut tx, row_b)
                .expect("tx A read second row failed");
            assert_eq!(row_a_value, Some(true));
            assert_eq!(row_b_value, Some(true));

            barrier.wait();

            table
                .write(&mut tx, row_a, false)
                .expect("tx A write first row failed");
            committer
                .lock()
                .expect("tx A failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in tx A: {}", other),
                })
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("tx B begin_transaction failed");
            let row_a_value = table
                .read(&mut tx, row_a)
                .expect("tx B read first row failed");
            let row_b_value = table
                .read(&mut tx, row_b)
                .expect("tx B read second row failed");
            assert_eq!(row_a_value, Some(true));
            assert_eq!(row_b_value, Some(true));

            barrier.wait();

            table
                .write(&mut tx, row_b, false)
                .expect("tx B write second row failed");
            committer
                .lock()
                .expect("tx B failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in tx B: {}", other),
                })
        })
    };

    let result_a = tx_a.join().expect("tx A thread panicked");
    let result_b = tx_b.join().expect("tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(
        committed, 1,
        "exactly one writer should commit in write-skew scenario ({})",
        label
    );
    assert_eq!(
        serialization_failures, 1,
        "exactly one writer should fail with SerializationFailure ({})",
        label
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let final_row_a = table
        .read(&mut verify_tx, row_a)
        .expect("verify read first row failed")
        .expect("first row missing after skew test");
    let final_row_b = table
        .read(&mut verify_tx, row_b)
        .expect("verify read second row failed")
        .expect("second row missing after skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        final_row_a || final_row_b,
        "serializable validation failed to preserve invariant for {}",
        label
    );

    if let Some(daemon) = daemon {
        ring.close().expect("failed to close skew test wal ring");
        daemon
            .join()
            .expect("async skew writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}

fn find_distinct_bucket_rows() -> (usize, usize) {
    for left in 0..4_096 {
        for right in (left + 1)..4_096 {
            if lock_bucket_for_row_id(left) != lock_bucket_for_row_id(right) {
                return (left, right);
            }
        }
    }
    panic!("failed to find distinct OCC lock buckets for write-skew test rows");
}

fn lock_bucket_for_row_id(row_id: usize) -> usize {
    let mut mixed = row_id as u64;
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xff51_afd7_ed55_8ccd);
    mixed ^= mixed >> 33;
    (mixed as usize) % OCC_PARTITION_LOCK_BUCKETS
}

fn run_write_skew_with_planner_reads(mode: Mode) {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared arena"));
    let table = Arc::new(
        OccTable::<OnCallRow>::new(Arc::clone(&shm), 2).expect("failed to create OCC table"),
    );

    table
        .seed_row(0, OnCallRow::new("DOC_A", 1))
        .expect("failed to seed row 0");
    table
        .seed_row(1, OnCallRow::new("DOC_B", 1))
        .expect("failed to seed row 1");

    let on_call_index = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "on_call",
        Arc::clone(&shm),
    ));
    on_call_index.insert(IndexValue::I64(1), 0);
    on_call_index.insert(IndexValue::I64(1), 1);
    let catalog = SchemaCatalog::new("doctor").with_index("on_call", on_call_index);
    let planner = Arc::new(RuleBasedOptimizer::<OnCallRow>::new(catalog));

    let read_plan = Arc::new(
        planner
            .compile_from_stapi("-compare {{> on_call 0}} -limit 2")
            .expect("failed to compile STAPI read plan"),
    );

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for planner skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_planner_{:?}_{}.wal",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        },
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for planner skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let read_plan = Arc::clone(&read_plan);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("planner tx A begin_transaction failed");

            let rows = read_plan
                .execute(&table, &mut tx)
                .expect("planner tx A read plan execution failed");
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| row.on_call == 1));
            assert_eq!(decode_ascii(&rows[0].doctor).len(), 5);

            barrier.wait();

            let mut row = table
                .read(&mut tx, 0)
                .expect("planner tx A read row 0 failed")
                .expect("planner tx A row 0 missing");
            row.on_call = 0;
            table
                .write(&mut tx, 0, row)
                .expect("planner tx A write row 0 failed");

            committer
                .lock()
                .expect("planner tx A failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in planner tx A: {}", other),
                })
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let read_plan = Arc::clone(&read_plan);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("planner tx B begin_transaction failed");

            let rows = read_plan
                .execute(&table, &mut tx)
                .expect("planner tx B read plan execution failed");
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| row.on_call == 1));

            barrier.wait();

            let mut row = table
                .read(&mut tx, 1)
                .expect("planner tx B read row 1 failed")
                .expect("planner tx B row 1 missing");
            row.on_call = 0;
            table
                .write(&mut tx, 1, row)
                .expect("planner tx B write row 1 failed");

            committer
                .lock()
                .expect("planner tx B failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in planner tx B: {}", other),
                })
        })
    };

    let result_a = tx_a.join().expect("planner tx A thread panicked");
    let result_b = tx_b.join().expect("planner tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(
        committed, 1,
        "exactly one planner-driven writer should commit"
    );
    assert_eq!(
        serialization_failures, 1,
        "exactly one planner-driven writer should fail with SerializationFailure"
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let final_row_0 = table
        .read(&mut verify_tx, 0)
        .expect("verify read row 0 failed")
        .expect("row 0 missing after planner skew test");
    let final_row_1 = table
        .read(&mut verify_tx, 1)
        .expect("verify read row 1 failed")
        .expect("row 1 missing after planner skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        final_row_0.on_call == 1 || final_row_1.on_call == 1,
        "planner-driven serializable validation failed to preserve invariant"
    );

    if let Some(daemon) = daemon {
        ring.close()
            .expect("failed to close planner skew test wal ring");
        daemon
            .join()
            .expect("planner async wal writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}

fn run_write_skew_with_tcl_like_keyed_upserts(mode: Mode) {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared arena"));
    let table = Arc::new(
        OccTable::<OnCallRow>::new(Arc::clone(&shm), 8).expect("failed to create OCC table"),
    );

    table
        .seed_row(0, OnCallRow::new("DOC_A", 1))
        .expect("failed to seed row 0");
    table
        .seed_row(1, OnCallRow::new("DOC_B", 1))
        .expect("failed to seed row 1");
    for row_id in 2..8 {
        table
            .seed_row(row_id, OnCallRow::new("EMPTY", 0))
            .expect("failed to seed placeholder row");
    }

    let key_index = Arc::new(SkipMap::<String, usize>::new());
    key_index.insert("DOC_A".to_string(), 0);
    key_index.insert("DOC_B".to_string(), 1);

    let on_call_index = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "on_call",
        Arc::clone(&shm),
    ));
    on_call_index.insert(IndexValue::I64(1), 0);
    on_call_index.insert(IndexValue::I64(1), 1);
    let catalog = SchemaCatalog::new("doctor").with_index("on_call", Arc::clone(&on_call_index));
    let planner = Arc::new(RuleBasedOptimizer::<OnCallRow>::new(catalog));

    let read_plan = Arc::new(
        planner
            .compile_from_stapi("-compare {{> on_call 0}} -limit 2")
            .expect("failed to compile keyed STAPI read plan"),
    );

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for keyed skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_keyed_{:?}_{}.wal",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        },
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for keyed skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let read_plan = Arc::clone(&read_plan);
        let key_index = Arc::clone(&key_index);
        let on_call_index = Arc::clone(&on_call_index);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("keyed tx A begin_transaction failed");
            let rows = read_plan
                .execute(&table, &mut tx)
                .expect("keyed tx A read plan execution failed");
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| row.on_call == 1));

            barrier.wait();

            let row_id = key_index
                .get("DOC_A")
                .map(|entry| *entry.value())
                .expect("missing DOC_A key mapping");
            let mut row = table
                .read(&mut tx, row_id)
                .expect("keyed tx A read row failed")
                .expect("keyed tx A row missing");
            let previous = row.on_call;
            row.on_call = 0;
            table
                .write(&mut tx, row_id, row)
                .expect("keyed tx A write row failed");

            let commit_result = committer
                .lock()
                .expect("keyed tx A failed to lock committer")
                .commit(&table, &mut tx);

            match commit_result {
                Ok(_) => {
                    on_call_index.remove(&IndexValue::I64(previous), &row_id);
                    on_call_index.insert(IndexValue::I64(row.on_call), row_id);
                    Ok(())
                }
                Err(aerostore_core::WalWriterError::Occ(err)) => Err(err),
                Err(other) => panic!("unexpected wal writer error in keyed tx A: {}", other),
            }
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let read_plan = Arc::clone(&read_plan);
        let key_index = Arc::clone(&key_index);
        let on_call_index = Arc::clone(&on_call_index);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("keyed tx B begin_transaction failed");
            let rows = read_plan
                .execute(&table, &mut tx)
                .expect("keyed tx B read plan execution failed");
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| row.on_call == 1));

            barrier.wait();

            let row_id = key_index
                .get("DOC_B")
                .map(|entry| *entry.value())
                .expect("missing DOC_B key mapping");
            let mut row = table
                .read(&mut tx, row_id)
                .expect("keyed tx B read row failed")
                .expect("keyed tx B row missing");
            let previous = row.on_call;
            row.on_call = 0;
            table
                .write(&mut tx, row_id, row)
                .expect("keyed tx B write row failed");

            let commit_result = committer
                .lock()
                .expect("keyed tx B failed to lock committer")
                .commit(&table, &mut tx);

            match commit_result {
                Ok(_) => {
                    on_call_index.remove(&IndexValue::I64(previous), &row_id);
                    on_call_index.insert(IndexValue::I64(row.on_call), row_id);
                    Ok(())
                }
                Err(aerostore_core::WalWriterError::Occ(err)) => Err(err),
                Err(other) => panic!("unexpected wal writer error in keyed tx B: {}", other),
            }
        })
    };

    let result_a = tx_a.join().expect("keyed tx A thread panicked");
    let result_b = tx_b.join().expect("keyed tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(committed, 1, "exactly one keyed writer should commit");
    assert_eq!(
        serialization_failures, 1,
        "exactly one keyed writer should fail with SerializationFailure"
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let row_a = table
        .read(&mut verify_tx, 0)
        .expect("verify read row 0 failed")
        .expect("row 0 missing after keyed skew test");
    let row_b = table
        .read(&mut verify_tx, 1)
        .expect("verify read row 1 failed")
        .expect("row 1 missing after keyed skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        row_a.on_call == 1 || row_b.on_call == 1,
        "keyed serializable validation failed to preserve invariant"
    );

    if let Some(daemon) = daemon {
        ring.close()
            .expect("failed to close keyed skew test wal ring");
        daemon
            .join()
            .expect("keyed async wal writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}

fn run_write_skew_with_pk_route_reads(mode: Mode) {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared arena"));
    let table = Arc::new(
        OccTable::<OnCallRow>::new(Arc::clone(&shm), 2).expect("failed to create OCC table"),
    );

    table
        .seed_row(0, OnCallRow::new("DOC_A", 1))
        .expect("failed to seed row 0");
    table
        .seed_row(1, OnCallRow::new("DOC_B", 1))
        .expect("failed to seed row 1");

    let pk_map = Arc::new(
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 64, 2)
            .expect("failed to create shared primary key map"),
    );
    pk_map
        .insert_existing("DOC_A", 0)
        .expect("failed to insert DOC_A primary key");
    pk_map
        .insert_existing("DOC_B", 1)
        .expect("failed to insert DOC_B primary key");

    let catalog = SchemaCatalog::new("doctor").with_primary_key_map(Arc::clone(&pk_map));
    let planner = Arc::new(RuleBasedOptimizer::<OnCallRow>::new(catalog));

    let read_doc_a = Arc::new(
        planner
            .compile_from_stapi("-compare {{= doctor DOC_A}} -limit 1")
            .expect("failed to compile DOC_A PK read plan"),
    );
    let read_doc_b = Arc::new(
        planner
            .compile_from_stapi("-compare {{= doctor DOC_B}} -limit 1")
            .expect("failed to compile DOC_B PK read plan"),
    );

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for PK planner skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_pk_rbo_{:?}_{}.wal",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        },
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for PK planner skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let read_doc_a = Arc::clone(&read_doc_a);
        let read_doc_b = Arc::clone(&read_doc_b);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("PK tx A begin_transaction failed");

            let rows_a = read_doc_a
                .execute(&table, &mut tx)
                .expect("PK tx A read DOC_A plan failed");
            let rows_b = read_doc_b
                .execute(&table, &mut tx)
                .expect("PK tx A read DOC_B plan failed");
            assert_eq!(rows_a.len(), 1);
            assert_eq!(rows_b.len(), 1);
            assert_eq!(rows_a[0].on_call, 1);
            assert_eq!(rows_b[0].on_call, 1);

            barrier.wait();

            let mut row = table
                .read(&mut tx, 0)
                .expect("PK tx A read row 0 failed")
                .expect("PK tx A row 0 missing");
            row.on_call = 0;
            table
                .write(&mut tx, 0, row)
                .expect("PK tx A write row 0 failed");

            committer
                .lock()
                .expect("PK tx A failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in PK tx A: {}", other),
                })
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let read_doc_a = Arc::clone(&read_doc_a);
        let read_doc_b = Arc::clone(&read_doc_b);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("PK tx B begin_transaction failed");

            let rows_a = read_doc_a
                .execute(&table, &mut tx)
                .expect("PK tx B read DOC_A plan failed");
            let rows_b = read_doc_b
                .execute(&table, &mut tx)
                .expect("PK tx B read DOC_B plan failed");
            assert_eq!(rows_a.len(), 1);
            assert_eq!(rows_b.len(), 1);
            assert_eq!(rows_a[0].on_call, 1);
            assert_eq!(rows_b[0].on_call, 1);

            barrier.wait();

            let mut row = table
                .read(&mut tx, 1)
                .expect("PK tx B read row 1 failed")
                .expect("PK tx B row 1 missing");
            row.on_call = 0;
            table
                .write(&mut tx, 1, row)
                .expect("PK tx B write row 1 failed");

            committer
                .lock()
                .expect("PK tx B failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in PK tx B: {}", other),
                })
        })
    };

    let result_a = tx_a.join().expect("PK tx A thread panicked");
    let result_b = tx_b.join().expect("PK tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(committed, 1, "exactly one PK-route writer should commit");
    assert_eq!(
        serialization_failures, 1,
        "exactly one PK-route writer should fail with SerializationFailure"
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let row_a = table
        .read(&mut verify_tx, 0)
        .expect("verify read row 0 failed")
        .expect("row 0 missing after PK skew test");
    let row_b = table
        .read(&mut verify_tx, 1)
        .expect("verify read row 1 failed")
        .expect("row 1 missing after PK skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        row_a.on_call == 1 || row_b.on_call == 1,
        "PK-route serializable validation failed to preserve invariant"
    );

    if let Some(daemon) = daemon {
        ring.close()
            .expect("failed to close PK planner skew test wal ring");
        daemon
            .join()
            .expect("PK planner async wal writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}

fn run_write_skew_with_savepoint_churn(mode: Mode) {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to create shared arena"));
    let table =
        Arc::new(OccTable::<bool>::new(Arc::clone(&shm), 2).expect("failed to create OCC table"));

    table
        .seed_row(0, true)
        .expect("failed to seed row 0 as on-call");
    table
        .seed_row(1, true)
        .expect("failed to seed row 1 as on-call");

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create wal ring for savepoint skew test");
    let wal_path = std::env::temp_dir().join(format!(
        "aerostore_occ_write_skew_savepoint_{:?}_{}.wal",
        match mode {
            Mode::Synchronous => "sync",
            Mode::Asynchronous => "async",
        },
        std::process::id()
    ));
    let _ = std::fs::remove_file(&wal_path);

    let daemon = match mode {
        Mode::Asynchronous => Some(
            spawn_wal_writer_daemon(ring.clone(), &wal_path)
                .expect("failed to spawn wal writer daemon for savepoint skew test"),
        ),
        Mode::Synchronous => None,
    };

    let committer = match mode {
        Mode::Synchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_synchronous(&wal_path)
                .expect("failed to create synchronous committer")
        }
        Mode::Asynchronous => {
            OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring.clone())
        }
    };
    let committer = Arc::new(std::sync::Mutex::new(committer));
    let barrier = Arc::new(Barrier::new(2));

    let tx_a = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("savepoint tx A begin_transaction failed");
            let row_0 = table
                .read(&mut tx, 0)
                .expect("savepoint tx A read row 0 failed");
            let row_1 = table
                .read(&mut tx, 1)
                .expect("savepoint tx A read row 1 failed");
            assert_eq!(row_0, Some(true));
            assert_eq!(row_1, Some(true));

            barrier.wait();

            table
                .savepoint(&mut tx, "root")
                .expect("savepoint tx A root savepoint failed");
            table
                .write(&mut tx, 0, false)
                .expect("savepoint tx A write(1) failed");
            table
                .savepoint(&mut tx, "inner")
                .expect("savepoint tx A inner savepoint failed");
            table
                .write(&mut tx, 0, true)
                .expect("savepoint tx A write(2) failed");
            table
                .rollback_to(&mut tx, "inner")
                .expect("savepoint tx A rollback_to(inner) failed");
            table
                .rollback_to(&mut tx, "root")
                .expect("savepoint tx A rollback_to(root) failed");
            table
                .write(&mut tx, 0, false)
                .expect("savepoint tx A final write failed");

            committer
                .lock()
                .expect("savepoint tx A failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in savepoint tx A: {}", other),
                })
        })
    };

    let tx_b = {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let committer = Arc::clone(&committer);
        thread::spawn(move || {
            let mut tx = table
                .begin_transaction()
                .expect("savepoint tx B begin_transaction failed");
            let row_0 = table
                .read(&mut tx, 0)
                .expect("savepoint tx B read row 0 failed");
            let row_1 = table
                .read(&mut tx, 1)
                .expect("savepoint tx B read row 1 failed");
            assert_eq!(row_0, Some(true));
            assert_eq!(row_1, Some(true));

            barrier.wait();

            table
                .savepoint(&mut tx, "root")
                .expect("savepoint tx B root savepoint failed");
            table
                .write(&mut tx, 1, false)
                .expect("savepoint tx B write(1) failed");
            table
                .savepoint(&mut tx, "inner")
                .expect("savepoint tx B inner savepoint failed");
            table
                .write(&mut tx, 1, true)
                .expect("savepoint tx B write(2) failed");
            table
                .rollback_to(&mut tx, "inner")
                .expect("savepoint tx B rollback_to(inner) failed");
            table
                .rollback_to(&mut tx, "root")
                .expect("savepoint tx B rollback_to(root) failed");
            table
                .write(&mut tx, 1, false)
                .expect("savepoint tx B final write failed");

            committer
                .lock()
                .expect("savepoint tx B failed to lock committer")
                .commit(&table, &mut tx)
                .map_err(|e| match e {
                    aerostore_core::WalWriterError::Occ(err) => err,
                    other => panic!("unexpected wal writer error in savepoint tx B: {}", other),
                })
        })
    };

    let result_a = tx_a.join().expect("savepoint tx A thread panicked");
    let result_b = tx_b.join().expect("savepoint tx B thread panicked");

    let outcomes = [result_a, result_b];
    let committed = outcomes.iter().filter(|r| r.is_ok()).count();
    let serialization_failures = outcomes
        .iter()
        .filter(|r| matches!(r, Err(OccError::SerializationFailure)))
        .count();

    assert_eq!(
        committed, 1,
        "exactly one savepoint-heavy writer should commit"
    );
    assert_eq!(
        serialization_failures, 1,
        "exactly one savepoint-heavy writer should fail with SerializationFailure"
    );

    let mut verify_tx = table
        .begin_transaction()
        .expect("verify begin_transaction failed");
    let final_row_0 = table
        .read(&mut verify_tx, 0)
        .expect("verify read row 0 failed")
        .expect("row 0 missing after savepoint skew test");
    let final_row_1 = table
        .read(&mut verify_tx, 1)
        .expect("verify read row 1 failed")
        .expect("row 1 missing after savepoint skew test");
    table
        .abort(&mut verify_tx)
        .expect("verify transaction abort failed");

    assert!(
        final_row_0 || final_row_1,
        "savepoint-heavy serializable validation failed to preserve invariant"
    );

    if let Some(daemon) = daemon {
        ring.close()
            .expect("failed to close savepoint skew test wal ring");
        daemon
            .join()
            .expect("savepoint async wal writer daemon did not exit cleanly");
    }

    let _ = std::fs::remove_file(wal_path);
}
