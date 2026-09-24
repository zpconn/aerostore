use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use aerostore_core::{
    read_logical_wal_records, LogicalDatabase, LogicalDatabaseConfig, LogicalWalError,
    RecoveryError, LOGICAL_WAL_FILE_NAME,
};
use tempfile::tempdir;

const RING_SLOTS: usize = 64;
const RING_SLOT_BYTES: usize = 256;

const CHILD_MODE_ENV: &str = "AEROSTORE_LOGICAL_RECOVERY_CHILD_MODE";
const CHILD_DIR_ENV: &str = "AEROSTORE_LOGICAL_RECOVERY_DIR";

fn payload_for(i: usize) -> Vec<u8> {
    format!("flight_state_payload_{:05}", i).into_bytes()
}

fn updated_payload_for(i: usize) -> Vec<u8> {
    format!("flight_state_updated_{:05}", i).into_bytes()
}

fn inserted_payload_for(i: usize) -> Vec<u8> {
    format!("flight_state_new_{:05}", i).into_bytes()
}

fn key_for(i: usize) -> String {
    format!("FLIGHT_{:05}", i)
}

fn logical_cfg(
    data_dir: &Path,
    row_capacity: usize,
    shm_bytes: usize,
    synchronous_commit: bool,
) -> LogicalDatabaseConfig {
    LogicalDatabaseConfig {
        data_dir: data_dir.to_path_buf(),
        table_name: "flight_state".to_string(),
        row_capacity,
        shm_bytes,
        synchronous_commit,
    }
}

fn offsets_file_path(base: &Path) -> PathBuf {
    base.join("pre_crash_offsets.tsv")
}

fn write_offsets(path: &Path, offsets: &HashMap<String, u32>) {
    let mut rows = offsets
        .iter()
        .map(|(k, v)| format!("{}\t{}\n", k, v))
        .collect::<Vec<_>>();
    rows.sort();
    fs::write(path, rows.concat()).expect("failed to write pre-crash offsets file");
}

fn read_offsets(path: &Path) -> HashMap<String, u32> {
    let mut out = HashMap::new();
    let bytes = fs::read_to_string(path).expect("failed to read pre-crash offsets file");
    for line in bytes.lines() {
        let (key, value) = line
            .split_once('\t')
            .unwrap_or_else(|| panic!("invalid offsets line '{}'", line));
        let offset = value
            .parse::<u32>()
            .unwrap_or_else(|err| panic!("invalid offset '{}' for key {}: {}", value, key, err));
        out.insert(key.to_string(), offset);
    }
    out
}

fn maybe_run_child(mode: &str, run: fn(&Path)) {
    let child_mode = std::env::var(CHILD_MODE_ENV).unwrap_or_default();
    if child_mode != mode {
        return;
    }

    let data_dir = std::env::var(CHILD_DIR_ENV)
        .unwrap_or_else(|_| panic!("missing {} for child mode {}", CHILD_DIR_ENV, mode));
    run(Path::new(&data_dir));

    // SAFETY:
    // child paths intentionally simulate hard process crash by skipping all Rust unwinding.
    unsafe {
        libc::_exit(0);
    }
}

fn spawn_child_for_test(test_name: &str, mode: &str, data_dir: &Path) {
    let exe = std::env::current_exe().expect("failed to resolve integration-test executable");
    let status = Command::new(exe)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_DIR_ENV, data_dir)
        .status()
        .expect("failed to spawn crash child");
    assert!(
        status.success(),
        "child mode '{}' failed with status {}",
        mode,
        status
    );
}

fn run_crash_child_base(data_dir: &Path) {
    let db = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir,
        16_384,
        256 << 20,
        true,
    ))
    .expect("failed to boot initial logical database");

    for i in 0..10_000 {
        let key = key_for(i);
        let payload = payload_for(i);
        db.upsert(key.as_str(), payload.as_slice())
            .unwrap_or_else(|err| panic!("initial upsert failed for key {}: {}", key, err));
    }

    let snapshot_meta = db.checkpoint_now().expect("checkpoint should succeed");
    assert_eq!(
        snapshot_meta.row_count, 10_000,
        "checkpoint row count should match pre-checkpoint inserts"
    );

    for i in 10_000..15_000 {
        let key = key_for(i);
        let payload = payload_for(i);
        db.upsert(key.as_str(), payload.as_slice())
            .unwrap_or_else(|err| panic!("post-checkpoint upsert failed for key {}: {}", key, err));
    }

    let offsets_before = db.snapshot_offsets();
    assert_eq!(
        offsets_before.len(),
        15_000,
        "expected 15k live keys before simulated crash"
    );
    write_offsets(offsets_file_path(data_dir).as_path(), &offsets_before);
}

fn run_crash_child_mixed_ops(data_dir: &Path) {
    let db = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir,
        20_480,
        320 << 20,
        true,
    ))
    .expect("failed to boot mixed-ops logical database");

    for i in 0..10_000 {
        let key = key_for(i);
        db.upsert(key.as_str(), payload_for(i).as_slice())
            .unwrap_or_else(|err| panic!("baseline upsert failed for key {}: {}", key, err));
    }

    db.checkpoint_now()
        .expect("mixed-ops checkpoint should succeed");

    for i in 0..3_000 {
        let key = key_for(i);
        db.upsert(key.as_str(), updated_payload_for(i).as_slice())
            .unwrap_or_else(|err| panic!("update failed for key {}: {}", key, err));
    }
    for i in 3_000..5_000 {
        let key = key_for(i);
        db.delete(key.as_str())
            .unwrap_or_else(|err| panic!("delete failed for key {}: {}", key, err));
    }
    for i in 10_000..12_000 {
        let key = key_for(i);
        db.upsert(key.as_str(), inserted_payload_for(i).as_slice())
            .unwrap_or_else(|err| panic!("insert failed for key {}: {}", key, err));
    }
}

fn assert_corrupt_wal(error: RecoveryError, expected_fragment: &str) {
    match error {
        RecoveryError::Wal(LogicalWalError::CorruptFrame { reason, .. }) => {
            assert!(
                reason.contains(expected_fragment),
                "expected corrupt-wal reason containing '{}', got '{}'",
                expected_fragment,
                reason
            );
        }
        other => panic!("expected corrupt logical wal error, got {}", other),
    }
}

#[test]
fn logical_wal_snapshot_recovery_rebuilds_15000_rows_with_fresh_offsets() {
    maybe_run_child("base_recovery", run_crash_child_base as fn(&Path));

    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path().to_path_buf();
    spawn_child_for_test(
        "logical_wal_snapshot_recovery_rebuilds_15000_rows_with_fresh_offsets",
        "base_recovery",
        &data_dir,
    );

    let offsets_before = read_offsets(offsets_file_path(&data_dir).as_path());
    let recovered = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        &data_dir,
        20_480,
        320 << 20,
        true,
    ))
    .expect("failed to boot recovered logical database");

    let recovered_rows = recovered
        .row_count()
        .expect("failed to count recovered rows");
    assert_eq!(
        recovered_rows, 15_000,
        "recovered row count mismatch after snapshot + logical wal replay"
    );

    let mut offset_changes = 0_usize;
    for i in 0..15_000 {
        let key = key_for(i);
        let expected_payload = payload_for(i);
        let observed_payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("payload lookup failed for key {}: {}", key, err))
            .unwrap_or_else(|| panic!("missing recovered key {}", key));
        assert_eq!(
            observed_payload, expected_payload,
            "payload mismatch for key {}",
            key
        );

        let recovered_offset = recovered
            .head_offset_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("offset lookup failed for key {}: {}", key, err))
            .unwrap_or_else(|| panic!("missing recovered offset for key {}", key));
        assert!(
            recovered_offset > 0,
            "recovered row must have non-zero RelPtr offset for key {}",
            key
        );

        let Some(previous_offset) = offsets_before.get(key.as_str()) else {
            panic!("missing pre-crash offset for key {}", key);
        };
        if *previous_offset != recovered_offset {
            offset_changes += 1;
        }
    }

    assert!(
        offset_changes > 0,
        "expected fresh shared-memory replay to allocate new RelPtr offsets"
    );

    recovered
        .shutdown()
        .expect("failed to shutdown recovered logical database");
}

#[test]
fn logical_wal_hard_crash_replays_mixed_upserts_updates_and_deletes() {
    maybe_run_child("mixed_ops_recovery", run_crash_child_mixed_ops as fn(&Path));

    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path().to_path_buf();
    spawn_child_for_test(
        "logical_wal_hard_crash_replays_mixed_upserts_updates_and_deletes",
        "mixed_ops_recovery",
        &data_dir,
    );

    let recovered = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        &data_dir,
        24_576,
        320 << 20,
        true,
    ))
    .expect("failed to boot mixed-ops recovered database");

    let recovered_rows = recovered
        .row_count()
        .expect("failed to count mixed-ops recovered rows");
    assert_eq!(
        recovered_rows, 10_000,
        "mixed-ops recovered row count mismatch"
    );

    for i in 0..3_000 {
        let key = key_for(i);
        let payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("payload lookup failed for key {}: {}", key, err))
            .unwrap_or_else(|| panic!("missing updated key {}", key));
        assert_eq!(payload, updated_payload_for(i), "updated payload mismatch");
    }
    for i in 3_000..5_000 {
        let key = key_for(i);
        let payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("delete lookup failed for key {}: {}", key, err));
        assert!(
            payload.is_none(),
            "deleted key {} unexpectedly visible",
            key
        );
    }
    for i in 5_000..10_000 {
        let key = key_for(i);
        let payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("payload lookup failed for key {}: {}", key, err))
            .unwrap_or_else(|| panic!("missing baseline key {}", key));
        assert_eq!(payload, payload_for(i), "baseline payload mismatch");
    }
    for i in 10_000..12_000 {
        let key = key_for(i);
        let payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| panic!("payload lookup failed for key {}: {}", key, err))
            .unwrap_or_else(|| panic!("missing inserted key {}", key));
        assert_eq!(
            payload,
            inserted_payload_for(i),
            "inserted payload mismatch"
        );
    }

    recovered
        .shutdown()
        .expect("failed to shutdown mixed-ops recovered database");
}

#[test]
fn logical_wal_recovery_rejects_truncated_length_prefix() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path();
    fs::write(data_dir.join(LOGICAL_WAL_FILE_NAME), [0xAB, 0xCD])
        .expect("failed to write corrupt wal");

    let err = match LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir,
        16,
        8 << 20,
        true,
    )) {
        Ok(_) => panic!("expected boot to fail for truncated prefix"),
        Err(err) => err,
    };
    assert_corrupt_wal(err, "truncated length prefix");
}

#[test]
fn logical_wal_recovery_rejects_truncated_frame_payload() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(32_u32).to_le_bytes());
    bytes.extend_from_slice(&[1_u8, 2_u8, 3_u8, 4_u8]);
    fs::write(data_dir.join(LOGICAL_WAL_FILE_NAME), bytes).expect("failed to write corrupt wal");

    let err = match LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir,
        16,
        8 << 20,
        true,
    )) {
        Ok(_) => panic!("expected boot to fail for truncated frame"),
        Err(err) => err,
    };
    assert_corrupt_wal(err, "truncated frame");
}

#[test]
fn logical_wal_recovery_rejects_invalid_record_payload() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(3_u32).to_le_bytes());
    bytes.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
    fs::write(data_dir.join(LOGICAL_WAL_FILE_NAME), bytes).expect("failed to write corrupt wal");

    let err = match LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir,
        16,
        8 << 20,
        true,
    )) {
        Ok(_) => panic!("expected boot to fail for invalid payload"),
        Err(err) => err,
    };
    assert_corrupt_wal(err, "decode failed");
}

#[test]
fn logical_wal_commit_ack_stress_never_reports_undurable_success() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path().to_path_buf();
    let db = Arc::new(
        LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
            data_dir.as_path(),
            4_096,
            128 << 20,
            true,
        ))
        .expect("failed to boot logical db for ack stress"),
    );

    const WRITERS: usize = 8;
    const WRITES_PER_WRITER: usize = 64;
    let (tx, rx) = mpsc::channel::<u64>();
    let mut workers = Vec::with_capacity(WRITERS);

    for worker_id in 0..WRITERS {
        let db = Arc::clone(&db);
        let tx = tx.clone();
        workers.push(thread::spawn(move || {
            for i in 0..WRITES_PER_WRITER {
                let key = format!("ACK_{}_{}", worker_id, i);
                let payload = ((worker_id as u64) << 32 | i as u64).to_le_bytes().to_vec();
                let txid = db
                    .upsert(key.as_str(), payload.as_slice())
                    .unwrap_or_else(|err| {
                        panic!("ack-stress upsert failed for key {}: {}", key, err)
                    });
                tx.send(txid)
                    .unwrap_or_else(|err| panic!("failed to send txid over channel: {}", err));
            }
        }));
    }
    drop(tx);

    for worker in workers {
        worker.join().expect("ack-stress worker panicked");
    }

    let returned_txids = rx.iter().collect::<Vec<_>>();
    let expected = WRITERS * WRITES_PER_WRITER;
    assert_eq!(
        returned_txids.len(),
        expected,
        "unexpected returned txid count"
    );

    let returned_set = returned_txids.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        returned_set.len(),
        expected,
        "returned txids must be unique per committed transaction"
    );

    db.shutdown()
        .expect("failed to shutdown logical db after ack stress");

    let wal_records = read_logical_wal_records(data_dir.join(LOGICAL_WAL_FILE_NAME))
        .expect("failed to parse logical wal after ack stress");
    let commit_txids = wal_records
        .iter()
        .filter_map(|record| match record {
            aerostore_core::LogicalWalRecord::Commit { txid } => Some(*txid),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(commit_txids.len(), expected, "unexpected WAL commit count");

    for pair in commit_txids.windows(2) {
        assert!(
            pair[1] > pair[0],
            "WAL commit txids must be strictly increasing"
        );
    }

    let commit_set = commit_txids.into_iter().collect::<HashSet<_>>();
    for txid in returned_set {
        assert!(
            commit_set.contains(&txid),
            "commit txid {} returned to caller but missing from durable WAL",
            txid
        );
    }
}

#[test]
fn logical_wal_daemon_kill_restart_recovers_cleanly() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path().to_path_buf();
    let db = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir.as_path(),
        64,
        64 << 20,
        true,
    ))
    .expect("failed to boot logical db for daemon lifecycle test");

    db.upsert("UAL123", b"baseline")
        .expect("baseline upsert should succeed");
    db.terminate_wal_daemon(libc::SIGKILL)
        .expect("failed to terminate logical wal daemon");
    thread::sleep(Duration::from_millis(50));

    let err = db
        .upsert("UAL123", b"after_kill")
        .expect_err("commit should fail while daemon is killed");
    match err {
        RecoveryError::Io(_) | RecoveryError::Wal(_) | RecoveryError::RuntimeUnavailable => {}
        other => panic!("unexpected error after daemon kill: {}", other),
    }

    db.restart_wal_daemon()
        .expect("failed to restart logical wal daemon");
    db.upsert("UAL123", b"after_restart")
        .expect("commit should succeed after daemon restart");
    db.shutdown()
        .expect("failed to shutdown logical db after daemon lifecycle test");

    let records = read_logical_wal_records(data_dir.join(LOGICAL_WAL_FILE_NAME))
        .expect("logical WAL should remain parseable after daemon restart");
    assert!(
        !records.is_empty(),
        "expected at least one logical WAL record after daemon restart test"
    );

    let recovered = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir.as_path(),
        64,
        64 << 20,
        true,
    ))
    .expect("failed to boot recovered db after daemon restart test");
    let payload = recovered
        .payload_for_key("UAL123")
        .expect("payload lookup failed after daemon restart test")
        .expect("UAL123 missing after daemon restart recovery");
    assert_eq!(payload, b"after_restart".to_vec());
    recovered
        .shutdown()
        .expect("failed to shutdown recovered daemon lifecycle db");
}

#[test]
fn forked_writer_crash_recovery_restores_state() {
    let temp = tempdir().expect("failed to create tempdir");
    let data_dir = temp.path().to_path_buf();

    // SAFETY:
    // fork is used intentionally to model a separate writer process crashing without unwind.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

    if pid == 0 {
        let db = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
            data_dir.as_path(),
            12_288,
            192 << 20,
            true,
        ))
        .expect("child failed to boot logical db");

        for i in 0..5_000 {
            let key = key_for(i);
            db.upsert(key.as_str(), payload_for(i).as_slice())
                .unwrap_or_else(|err| panic!("child upsert failed for key {}: {}", key, err));
        }
        db.checkpoint_now().expect("child checkpoint failed");
        for i in 5_000..7_500 {
            let key = key_for(i);
            db.upsert(key.as_str(), payload_for(i).as_slice())
                .unwrap_or_else(|err| {
                    panic!(
                        "child post-checkpoint upsert failed for key {}: {}",
                        key, err
                    )
                });
        }

        // SAFETY:
        // intentionally hard-exit child process.
        unsafe {
            libc::_exit(0);
        }
    }

    let mut status = 0_i32;
    // SAFETY:
    // waiting on child pid returned by fork.
    let waited = unsafe { libc::waitpid(pid, &mut status as *mut i32, 0) };
    assert_eq!(waited, pid, "waitpid failed for forked writer child");
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "forked writer child exited unexpectedly (status={})",
        status
    );

    let recovered = LogicalDatabase::<RING_SLOTS, RING_SLOT_BYTES>::boot_from_disk(logical_cfg(
        data_dir.as_path(),
        16_384,
        256 << 20,
        true,
    ))
    .expect("failed to boot recovery db after forked crash");
    let row_count = recovered
        .row_count()
        .expect("failed to count recovered rows after forked crash");
    assert_eq!(row_count, 7_500, "forked crash recovery row count mismatch");

    for i in [0_usize, 1_337, 4_999, 5_000, 6_123, 7_499] {
        let key = key_for(i);
        let payload = recovered
            .payload_for_key(key.as_str())
            .unwrap_or_else(|err| {
                panic!("recovered payload lookup failed for key {}: {}", key, err)
            })
            .unwrap_or_else(|| panic!("recovered row missing for key {}", key));
        assert_eq!(payload, payload_for(i), "forked recovery payload mismatch");
    }

    recovered
        .shutdown()
        .expect("failed to shutdown recovered forked db");
}
