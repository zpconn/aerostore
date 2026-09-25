#![cfg(target_os = "linux")]
//! Bounded process-death observations on disposable mappings. These tests lock
//! in the present recovery contract, including limitations; they do not claim
//! arbitrary kill points are safe for warm attach.

use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{
    alloc_u32_array, load_boot_layout, map_tmpfs_shared, open_boot_context, persist_boot_layout,
    read_u32_array, recover_occ_table_from_wal, BootLayout, BootMode, IndexCompare, IndexValue,
    OccCommitter, OccError, OccTable, SecondaryIndex, ShmArena,
};
use serde_json::{json, Value};

const BYTES: usize = 16 << 20;
const DEADLINE: Duration = Duration::from_secs(10);
const MODE: &str = "AEROSTORE_WORKER_FAILURE_MODE";
const DIRECTORY: &str = "AEROSTORE_WORKER_FAILURE_DIRECTORY";

struct Store {
    arena: Arc<ShmArena>,
    table: OccTable<u64>,
    index: SecondaryIndex<usize>,
}

fn key(value: &u64) -> Option<IndexValue> {
    Some(IndexValue::U64(*value))
}

fn attach(arena: Arc<ShmArena>) -> Store {
    let layout = load_boot_layout(&arena).unwrap().unwrap();
    let offsets = read_u32_array(
        &arena,
        layout.occ_slot_offsets_offset,
        layout.occ_slot_offsets_len,
    )
    .unwrap();
    let index = SecondaryIndex::from_existing("value", Arc::clone(&arena), layout.index_offsets[0])
        .unwrap();
    let mut table =
        OccTable::from_existing(Arc::clone(&arena), layout.occ_shared_header_offset, offsets)
            .unwrap();
    table.bind_index(index.clone(), key).unwrap();
    Store {
        arena,
        table,
        index,
    }
}

fn create(path: &Path, rows: usize) -> Store {
    let arena = Arc::new(map_tmpfs_shared(path, BYTES).unwrap().arena);
    let mut table = OccTable::new(Arc::clone(&arena), rows).unwrap();
    let index = SecondaryIndex::new_in_shared("value", Arc::clone(&arena));
    for id in 0..rows {
        table.seed_row(id, id as u64 + 10).unwrap();
        index
            .try_insert(IndexValue::U64(id as u64 + 10), id)
            .unwrap();
    }
    table.bind_index(index.clone(), key).unwrap();
    let mut layout = BootLayout::new(rows).unwrap();
    layout.occ_shared_header_offset = table.shared_header_offset();
    let (offset, len) = alloc_u32_array(&arena, &table.index_slot_offsets()).unwrap();
    layout.occ_slot_offsets_offset = offset;
    layout.occ_slot_offsets_len = len;
    layout.index_offsets[0] = index.header_offset();
    layout.index_count = 1;
    persist_boot_layout(&arena, &layout).unwrap();
    Store {
        arena,
        table,
        index,
    }
}

fn active(store: &Store) -> usize {
    store.arena.create_snapshot().len()
}

fn update(store: &Store, row: usize, value: u64) -> Result<usize, OccError> {
    let mut tx = store.table.begin_transaction()?;
    let result = store
        .table
        .write(&mut tx, row, value)
        .and_then(|()| store.table.commit(&mut tx));
    if result.is_err() {
        let _ = store.table.abort(&mut tx);
    }
    result
}

fn equality(store: &Store, value: u64) -> Result<Vec<usize>, OccError> {
    let mut tx = store.table.begin_transaction()?;
    let result = store.table.index_lookup(
        &mut tx,
        &store.index,
        &IndexCompare::Eq(IndexValue::U64(value)),
    );
    store.table.abort(&mut tx)?;
    result
}

fn ready(directory: &Path, value: Value) {
    let pending = directory.join("ready.pending");
    std::fs::write(&pending, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(pending, directory.join("ready.json")).unwrap();
}

fn park() -> ! {
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

// A separate executable process avoids forking Rust's multithreaded test host.
// The supervisor always kills and reaps this child, including on assertion panic.
struct Worker(Child);
impl Worker {
    fn spawn(directory: &Path, mode: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "worker_failure_child", "--nocapture"])
                .env(MODE, mode)
                .env(DIRECTORY, directory)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    fn ready(&mut self, directory: &Path) -> Value {
        let start = Instant::now();
        loop {
            if let Ok(bytes) = std::fs::read(directory.join("ready.json")) {
                return serde_json::from_slice(&bytes).unwrap();
            }
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "worker exited before checkpoint"
            );
            assert!(
                start.elapsed() < DEADLINE,
                "worker did not reach checkpoint"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn kill_reap(&mut self) -> f64 {
        let started = Instant::now();
        self.0.kill().unwrap();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert_eq!(status.signal(), Some(libc::SIGKILL));
                return started.elapsed().as_secs_f64() * 1000.0;
            }
            assert!(started.elapsed() < DEADLINE, "SIGKILL worker did not exit");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn worker_failure_child() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let directory = PathBuf::from(std::env::var(DIRECTORY).unwrap());
    // Ordinary worker attach MUST NOT call open_boot_context: that is exclusive
    // startup recovery and clears registrations belonging to other workers.
    let store = attach(Arc::new(
        map_tmpfs_shared(&directory.join("arena"), BYTES)
            .unwrap()
            .arena,
    ));
    if let Some(victim) = mode.strip_prefix("survivor_") {
        std::fs::write(directory.join("survivor-ready"), b"ready").unwrap();
        let deadline = Instant::now();
        while !directory.join("survivor-go").exists() {
            assert!(
                deadline.elapsed() < DEADLINE,
                "supervisor did not release survivor"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let started = Instant::now();
        let mut attempts = Vec::new();
        for (kind, row, value) in [("same_row", 0, 500), ("disjoint_row", 2, 502)] {
            let start = Instant::now();
            let mut message = json!(null);
            for attempt in 1..=16 {
                let optional_guard = if victim == "indexed_guard" {
                    store.table.try_lock_indexed_rows(&[row]).unwrap()
                } else {
                    None
                };
                let result = if victim == "indexed_guard" && optional_guard.is_none() {
                    Err("indexed_update_guard_unavailable".to_string())
                } else if victim == "commit_waiting_wal" {
                    let mut committer =
                        OccCommitter::<8, 1024>::new_synchronous(directory.join("wal")).unwrap();
                    let mut tx = store.table.begin_transaction().unwrap();
                    let result = store
                        .table
                        .write(&mut tx, row, value)
                        .map_err(|e| e.to_string())
                        .and_then(|()| {
                            committer
                                .commit(&store.table, &mut tx)
                                .map_err(|e| e.to_string())
                        });
                    if result.is_err() {
                        let _ = store.table.abort(&mut tx);
                    }
                    result
                } else {
                    update(&store, row, value).map_err(|e| e.to_string())
                };
                let committed = result.is_ok();
                let query = if committed {
                    equality(&store, value).map_err(|e| e.to_string())
                } else {
                    Err("write did not commit".to_string())
                };
                message = json!({"kind":kind,"row":row,"committed":committed,
                    "attempt_count":attempt,
                    "verified_query": query.as_ref().is_ok_and(|ids| ids == &vec![row]),
                    "error": result.err(),"query_error":query.err(),
                    "elapsed_ms":start.elapsed().as_secs_f64()*1000.0});
                if committed {
                    break;
                }
                drop(optional_guard);
                std::thread::sleep(Duration::from_micros(100));
            }
            attempts.push(message);
        }
        // This predicate's logical result is the successfully updated disjoint
        // row. Range tracking covers all buckets, so an abandoned unrelated
        // bucket can still block it. This is measured separately from row IDs.
        let mut tx = store.table.begin_transaction().unwrap();
        let range = store.table.index_lookup(
            &mut tx,
            &store.index,
            &IndexCompare::Gte(IndexValue::U64(501)),
        );
        store.table.abort(&mut tx).unwrap();
        let range_result = json!({"completed":range.is_ok(),
            "rows":range.as_ref().ok(),"error":range.err().map(|e|e.to_string())});
        std::fs::write(
            directory.join("survivor-result.json"),
            serde_json::to_vec(&json!({
                "pid":std::process::id(),"attempts":attempts,"disjoint_range_query":range_result,
                "elapsed_ms":started.elapsed().as_secs_f64()*1000.0
            }))
            .unwrap(),
        )
        .unwrap();
        return;
    }
    match mode.as_str() {
        "idle" => {
            ready(&directory, json!({"active": active(&store)}));
            park();
        }
        "private" => {
            let mut tx = store.table.begin_transaction().unwrap();
            let before = store.arena.chunked_arena().head_offset();
            store.table.write(&mut tx, 0, 999).unwrap();
            ready(
                &directory,
                json!({"txid": tx.txid(), "head_before": before,
                "head_after": store.arena.chunked_arena().head_offset()}),
            );
            park();
        }
        "row_lock" => {
            let tx = store.table.begin_transaction().unwrap();
            let _guard = store.table.lock_for_update(&tx, 0).unwrap();
            ready(&directory, json!({"txid": tx.txid()}));
            park();
        }
        "indexed_guard" => {
            let _guard = store.table.lock_indexed_rows(&[0]).unwrap();
            ready(&directory, json!({"active": active(&store)}));
            park();
        }
        "commit_waiting_wal" => {
            let mut committer =
                OccCommitter::<8, 1024>::new_synchronous(directory.join("wal")).unwrap();
            let mut tx = store.table.begin_transaction().unwrap();
            store.table.write(&mut tx, 0, 999).unwrap();
            store.table.write(&mut tx, 1, 1000).unwrap();
            ready(&directory, json!({"txid": tx.txid()}));
            committer.commit(&store.table, &mut tx).unwrap();
            panic!("supervisor's WAL flock must prevent commit completion");
        }
        other => panic!("unknown worker role: {other}"),
    }
}

fn warm(directory: &Path) -> (Store, usize, f64) {
    // Caller has reaped the only worker, ended every supervisor transaction and
    // dropped its old handle before entering exclusive recovery.
    let started = Instant::now();
    let boot = open_boot_context(Some(&directory.join("arena")), BYTES).unwrap();
    assert_eq!(boot.mode, BootMode::WarmAttach);
    let cleared = boot.orphaned_proc_slots_cleared;
    let store = attach(boot.shm);
    (store, cleared, started.elapsed().as_secs_f64() * 1000.0)
}

fn observation(value: Value) {
    if let Ok(directory) = std::env::var("AEROSTORE_WORKER_FAILURE_REPORT_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        let name = value["phase"].as_str().unwrap();
        std::fs::write(
            Path::new(&directory).join(format!("{name}.json")),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
    println!(
        "WORKER_FAILURE_OBSERVATION {}",
        serde_json::to_string(&value).unwrap()
    );
}

#[test]
fn killed_idle_worker_does_not_interrupt_transactions() {
    let directory = tempfile::tempdir().unwrap();
    let store = create(&directory.path().join("arena"), 2);
    let mut worker = Worker::spawn(directory.path(), "idle");
    worker.ready(directory.path());
    let kill_ms = worker.kill_reap();
    assert_eq!(active(&store), 0);
    let started = Instant::now();
    update(&store, 0, 100).unwrap();
    assert_eq!(equality(&store, 100).unwrap(), vec![0]);
    observation(json!({"phase":"idle", "kill_reap_ms":kill_ms,
        "first_verified_message_ms":started.elapsed().as_secs_f64()*1000.0,
        "recovery":"none", "active_after_kill":active(&store)}));
}

#[test]
fn killed_private_writer_pins_retention_until_exclusive_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let store = create(&directory.path().join("arena"), 2);
    let mut worker = Worker::spawn(directory.path(), "private");
    let checkpoint = worker.ready(directory.path());
    let kill_ms = worker.kill_reap();
    assert_eq!(active(&store), 1);
    let dead_txid = checkpoint["txid"].as_u64().unwrap();
    assert_eq!(
        store
            .arena
            .proc_array()
            .oldest_snapshot_xmin(store.arena.global_txid()),
        dead_txid
    );
    assert_eq!(store.table.latest_value(0).unwrap(), Some(10));
    let mut completed = 0;
    for value in 100..108 {
        update(&store, 1, value).unwrap();
        completed += 1;
    }
    let before_recovery = store.table.vacuum_reclaim_once(u64::MAX).unwrap().len();
    assert_eq!(
        before_recovery, 0,
        "orphaned snapshot must prevent reclaiming these versions"
    );
    let head_before_warm = store.arena.chunked_arena().head_offset();
    drop(store);
    let (store, cleared, warm_ms) = warm(directory.path());
    assert_eq!(cleared, 1);
    assert_eq!(active(&store), 0);
    assert_eq!(store.arena.chunked_arena().head_offset(), head_before_warm);
    assert_eq!(store.table.latest_value(0).unwrap(), Some(10));
    let reclaimed = store.table.vacuum_reclaim_once(u64::MAX).unwrap().len();
    assert_eq!(reclaimed, completed);
    observation(json!({"phase":"private_write", "kill_reap_ms":kill_ms,
        "completed_while_orphaned":completed,"active_after_kill":1,
        "reclaimed_before_recovery":before_recovery,"reclaimed_after_recovery":reclaimed,
        "warm_attach_ms":warm_ms,"cleared_slots":cleared,
        "private_allocation_bytes":checkpoint["head_after"].as_u64().unwrap()-checkpoint["head_before"].as_u64().unwrap(),
        "private_write_published":false,"warm_attach_rewinds_allocator":false}));
}

#[test]
fn killed_row_lock_owner_still_blocks_updates_after_warm_attach() {
    let directory = tempfile::tempdir().unwrap();
    let store = create(&directory.path().join("arena"), 2);
    let mut worker = Worker::spawn(directory.path(), "row_lock");
    worker.ready(directory.path());
    let kill_ms = worker.kill_reap();
    assert!(matches!(
        update(&store, 0, 100),
        Err(OccError::SerializationFailure)
    ));
    update(&store, 1, 200).unwrap();
    drop(store);
    let (store, cleared, warm_ms) = warm(directory.path());
    assert_eq!(cleared, 1);
    assert_eq!(active(&store), 0);
    assert!(matches!(
        update(&store, 0, 100),
        Err(OccError::SerializationFailure)
    ));
    assert_eq!(store.table.latest_value(0).unwrap(), Some(10));
    observation(json!({"phase":"row_lock", "kill_reap_ms":kill_ms,
        "unrelated_update_succeeded":true,"warm_attach_ms":warm_ms,
        "cleared_slots":cleared,"locked_row_usable_after_warm_attach":false}));
}

#[test]
fn killed_indexed_guard_owner_still_blocks_participating_writers_after_warm_attach() {
    let directory = tempfile::tempdir().unwrap();
    let store = create(&directory.path().join("arena"), 2);
    let mut worker = Worker::spawn(directory.path(), "indexed_guard");
    worker.ready(directory.path());
    let kill_ms = worker.kill_reap();
    assert!(store.table.try_lock_indexed_rows(&[0]).unwrap().is_none());
    assert!(store.table.try_lock_indexed_rows(&[1]).unwrap().is_some());
    assert_eq!(active(&store), 0);
    drop(store);
    let (store, cleared, warm_ms) = warm(directory.path());
    assert_eq!(cleared, 0);
    assert!(store.table.try_lock_indexed_rows(&[0]).unwrap().is_none());
    observation(
        json!({"phase":"indexed_update_guard", "kill_reap_ms":kill_ms,
        "warm_attach_ms":warm_ms,"cleared_slots":cleared,
        "unrelated_guard_available":true,"guard_available_after_warm_attach":false,
        "scope":"optional application guard, not every native transaction"}),
    );
}

struct FileLock(File);
impl FileLock {
    fn new(path: &Path) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        Self(file)
    }
}
impl Drop for FileLock {
    fn drop(&mut self) {
        assert_eq!(unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) }, 0);
    }
}

fn wait_for_wal_lock(worker: &mut Worker) -> f64 {
    let started = Instant::now();
    let pid = worker.0.id().to_string();
    loop {
        let locks = std::fs::read_to_string("/proc/locks").unwrap();
        // Linux marks blocked flock requests with `->`; the requesting child's
        // PID is the token following WRITE. There is only one flock in this role.
        let blocked = locks.lines().any(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            fields
                .windows(2)
                .any(|pair| pair == ["WRITE", pid.as_str()])
                && fields.contains(&"->")
                && fields.contains(&"FLOCK")
        });
        if blocked {
            return started.elapsed().as_secs_f64() * 1000.0;
        }
        assert!(
            worker.0.try_wait().unwrap().is_none(),
            "worker exited before WAL lock wait"
        );
        assert!(
            started.elapsed() < DEADLINE,
            "native committer did not block on WAL flock"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn killed_committer_holding_native_guards_requires_fresh_mapping_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let store = create(&directory.path().join("arena"), 2);
    let wal = directory.path().join("wal");
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut baseline = store.table.begin_transaction().unwrap();
    store.table.write(&mut baseline, 0, 10).unwrap();
    store.table.write(&mut baseline, 1, 11).unwrap();
    committer.commit(&store.table, &mut baseline).unwrap();
    drop(committer);
    let durable_bytes = std::fs::read(&wal).unwrap();
    let held_wal = FileLock::new(&wal);
    let mut worker = Worker::spawn(directory.path(), "commit_waiting_wal");
    worker.ready(directory.path());
    let acquire_ms = wait_for_wal_lock(&mut worker);
    let kill_ms = worker.kill_reap();
    drop(held_wal);
    assert_eq!(std::fs::read(&wal).unwrap(), durable_bytes);
    assert_eq!(store.table.latest_value(0).unwrap(), Some(10));
    assert_eq!(store.table.latest_value(1).unwrap(), Some(11));
    assert!(matches!(
        equality(&store, 10),
        Err(OccError::SerializationFailure)
    ));
    assert_eq!(active(&store), 1);
    drop(store);
    let (store, cleared, warm_ms) = warm(directory.path());
    assert_eq!(cleared, 1);
    assert_eq!(active(&store), 0);
    assert!(matches!(
        equality(&store, 10),
        Err(OccError::SerializationFailure)
    ));
    drop(store);

    // Recovery never tries to unlock or reuse the damaged mapping. The durable
    // baseline is replayed into new allocator, row, partition and index state.
    let started = Instant::now();
    let fresh_arena = Arc::new(ShmArena::new(BYTES).unwrap());
    let mut recovered = OccTable::<u64>::new(Arc::clone(&fresh_arena), 2).unwrap();
    // This WAL may contain deltas against deterministic bootstrap values.
    // A deployment must likewise restore its checkpoint/base before replay.
    recovered.seed_row(0, 10).unwrap();
    recovered.seed_row(1, 11).unwrap();
    let replay = recover_occ_table_from_wal(&recovered, &wal).unwrap();
    let recovered_index = SecondaryIndex::new_in_shared("value", Arc::clone(&fresh_arena));
    for (id, value) in recovered.snapshot_latest_rows().unwrap() {
        recovered_index
            .try_insert(IndexValue::U64(value), id)
            .unwrap();
    }
    recovered.bind_index(recovered_index.clone(), key).unwrap();
    let fresh = Store {
        arena: fresh_arena,
        table: recovered,
        index: recovered_index,
    };
    assert_eq!(equality(&fresh, 10).unwrap(), vec![0]);
    assert_eq!(equality(&fresh, 11).unwrap(), vec![1]);
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut resumed = fresh.table.begin_transaction().unwrap();
    fresh.table.write(&mut resumed, 0, 20).unwrap();
    fresh.table.write(&mut resumed, 1, 21).unwrap();
    committer.commit(&fresh.table, &mut resumed).unwrap();
    assert_eq!(equality(&fresh, 20).unwrap(), vec![0]);
    assert_eq!(equality(&fresh, 21).unwrap(), vec![1]);
    let recovery_ms = started.elapsed().as_secs_f64() * 1000.0;
    observation(json!({"phase":"native_commit_before_wal_acceptance",
        "wait_for_native_wal_cutpoint_ms":acquire_ms,"kill_reap_ms":kill_ms,
        "warm_attach_ms":warm_ms,"cleared_slots":cleared,
        "predicate_usable_after_warm_attach":false,"rows_partially_published":false,
        "wal_bytes_changed":false,"fresh_replay_index_rebuild_and_first_message_ms":recovery_ms,
        "replayed_transactions":replay.wal_records,"replayed_writes":replay.applied_writes,
        "fixture_rows":2,"arena_bytes":BYTES,
        "timing_scope":"microfixture, excludes failure detection and stopping other workers",
        "arbitrary_partial_publication_kill_tested":false}));
}

#[test]
fn survivor_availability_characterization_reports_current_contract_failure() {
    let mut phases = Vec::new();
    for phase in [
        "idle",
        "private",
        "row_lock",
        "indexed_guard",
        "commit_waiting_wal",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = create(&directory.path().join("arena"), 3);
        if phase == "commit_waiting_wal" {
            let mut committer =
                OccCommitter::<8, 1024>::new_synchronous(directory.path().join("wal")).unwrap();
            let mut tx = store.table.begin_transaction().unwrap();
            for row in 0..3 {
                store.table.write(&mut tx, row, row as u64 + 10).unwrap();
            }
            committer.commit(&store.table, &mut tx).unwrap();
        }
        // The survivor has attached BEFORE the victim dies and retains exactly
        // that mapping/table/index handle throughout. No reset/rebuild occurs.
        let mut survivor = Worker::spawn(directory.path(), &format!("survivor_{phase}"));
        let started = Instant::now();
        while !directory.path().join("survivor-ready").exists() {
            assert!(survivor.0.try_wait().unwrap().is_none());
            assert!(started.elapsed() < DEADLINE);
            std::thread::sleep(Duration::from_millis(1));
        }
        let held_wal =
            (phase == "commit_waiting_wal").then(|| FileLock::new(&directory.path().join("wal")));
        let mut victim = Worker::spawn(directory.path(), phase);
        victim.ready(directory.path());
        if phase == "commit_waiting_wal" {
            wait_for_wal_lock(&mut victim);
        }
        let kill_ms = victim.kill_reap();
        drop(held_wal);
        std::fs::write(directory.path().join("survivor-go"), b"go").unwrap();
        let resumed = Instant::now();
        let mut timed_out = false;
        loop {
            if let Some(status) = survivor.0.try_wait().unwrap() {
                assert!(status.success(), "survivor failed unexpectedly in {phase}");
                break;
            }
            if resumed.elapsed() >= DEADLINE {
                survivor.kill_reap();
                timed_out = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let result = if timed_out {
            json!({"timed_out":true})
        } else {
            serde_json::from_slice::<Value>(
                &std::fs::read(directory.path().join("survivor-result.json")).unwrap(),
            )
            .unwrap()
        };
        let progress = !timed_out
            && result["disjoint_range_query"]["completed"] == true
            && result["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|attempt| attempt["committed"] == true && attempt["verified_query"] == true);
        if ["idle", "private"].contains(&phase) {
            assert!(
                progress,
                "unexpected immediate failure in {phase}: {result}"
            );
        }
        if ["row_lock", "indexed_guard", "commit_waiting_wal"].contains(&phase) {
            assert!(!progress, "known failure characterization changed: review contract and promote availability result");
        }
        let slots = active(&store);
        phases.push(
            json!({"phase":phase,"kill_reap_ms":kill_ms,"survivor":result,
            "same_and_disjoint_message_progress":progress,"retained_orphan_slots":slots,
            "other_workers_kept_same_mapping":true,"reset_or_rebuild_performed":false}),
        );
        // Even the idle/private cases are observations at controlled safe cuts;
        // they do not establish arbitrary-worker failure availability.
    }
    let availability = phases.iter().all(|phase| {
        phase["same_and_disjoint_message_progress"] == true && phase["retained_orphan_slots"] == 0
    });
    assert!(!availability);
    observation(
        json!({"phase":"survivor_availability","characterization_passed":true,
        "required_worker_failure_availability_satisfied":availability,
        "contract":"survivors keep existing mappings and complete same/disjoint work without whole-engine restart or unbounded orphan retention",
        "probe_timeout_ms":DEADLINE.as_millis(),"max_attempts_per_message":16,"phases":phases,
        "arbitrary_partial_publication_kill_tested":false}),
    );
}
