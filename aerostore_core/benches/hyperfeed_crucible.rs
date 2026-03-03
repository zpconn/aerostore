#![cfg(unix)]

use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::{
    spawn_vacuum_daemon_with_config, spawn_wal_writer_daemon, IndexCompare, IndexValue,
    OccCommitter, OccError, OccTable, RelPtr, RetryBackoff, RetryPolicy, SecondaryIndex,
    SharedWalRing, ShmArena, ShmIndexGcDaemon, VacuumDaemon, VacuumDaemonConfig,
    VacuumReclaimedRow, WalDeltaCodec, WalWriterError,
};
use criterion::{criterion_group, criterion_main, Criterion};
use postgres::{Client, NoTls, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use testcontainers::clients;
use testcontainers::core::WaitFor;
use testcontainers::GenericImage;

const WORKERS: usize = 16;
const TOTAL_KEYS: usize = 50_000;
const HOT_KEY_COUNT: usize = 256;
const MIX_PERIOD: u64 = 100;
const UPSERTS_PER_PERIOD: u64 = 80;
const HOT_UPSERT_EVERY: u64 = 20; // 5% of upserts
const SCAN_LIMIT: i64 = 64;
const SCAN_TAIL_WINDOW: i64 = 4_096;
const HIST_BUCKETS: usize = 64;
const RING_SLOTS: usize = 2048;
const RING_SLOT_BYTES: usize = 256;
const SHM_BYTES_METRICS: usize = 16 << 20;
const PG_EXPLAIN_SAMPLE_EVERY: u64 = 128;
const SHM_BYTES_AEROSTORE_512M: usize = 512 << 20;
const SHM_BYTES_AEROSTORE_1G: usize = 1 << 30;
const DEFAULT_VACUUM_INTERVAL_MS: u64 = 50;
const DEFAULT_INDEX_GC_INTERVAL_MS: u64 = 50;

const MIX_RATIO_TOLERANCE: f64 = 0.02;
const HOT_RATIO_TOLERANCE: f64 = 0.01;
const REQUIRED_TPS_RATIO: f64 = 2.0;
const REQUIRED_P99_RATIO: f64 = 0.6;

const PROFILES: [CrucibleProfile; 2] = [
    CrucibleProfile {
        label: "profile_512m",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_512M,
    },
    CrucibleProfile {
        label: "profile_1g",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_1G,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CrucibleRow {
    exists: u8,
    altitude: i32,
    event_ts: i64,
    payload: [u8; 32],
}

impl CrucibleRow {
    #[inline]
    fn seeded(row_id: usize) -> Self {
        let mut payload = [0_u8; 32];
        payload[..8].copy_from_slice(&(row_id as u64).to_le_bytes());
        Self {
            exists: 1,
            altitude: 30_000 + (row_id % 2_000) as i32,
            event_ts: row_id as i64,
            payload,
        }
    }
}

impl WalDeltaCodec for CrucibleRow {}

#[repr(C, align(64))]
struct Histogram {
    samples: AtomicU64,
    buckets: [AtomicU64; HIST_BUCKETS],
}

impl Histogram {
    fn new() -> Self {
        Self {
            samples: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn record_ns(&self, latency_ns: u64) {
        let idx = latency_bucket(latency_ns);
        self.samples.fetch_add(1, Ordering::AcqRel);
        self.buckets[idx].fetch_add(1, Ordering::AcqRel);
    }

    fn snapshot(&self) -> ([u64; HIST_BUCKETS], u64) {
        let mut out = [0_u64; HIST_BUCKETS];
        for (idx, slot) in out.iter_mut().enumerate() {
            *slot = self.buckets[idx].load(Ordering::Acquire);
        }
        (out, self.samples.load(Ordering::Acquire))
    }
}

#[repr(C, align(64))]
struct WorkerStats {
    total_ops: AtomicU64,
    upsert_ops: AtomicU64,
    scan_ops: AtomicU64,
    hot_upserts: AtomicU64,
    conflicts: AtomicU64,
    index_remove_failures: AtomicU64,
    index_insert_failures: AtomicU64,
    total_latency: Histogram,
    upsert_latency: Histogram,
    scan_latency: Histogram,
    scan_server_exec_latency: Histogram,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            upsert_ops: AtomicU64::new(0),
            scan_ops: AtomicU64::new(0),
            hot_upserts: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            index_remove_failures: AtomicU64::new(0),
            index_insert_failures: AtomicU64::new(0),
            total_latency: Histogram::new(),
            upsert_latency: Histogram::new(),
            scan_latency: Histogram::new(),
            scan_server_exec_latency: Histogram::new(),
        }
    }
}

#[repr(C, align(64))]
struct RunState {
    ready: AtomicU32,
    go: AtomicU32,
    stop: AtomicU32,
    _pad: [u32; 13],
    global_event_ts: AtomicI64,
    workers: [WorkerStats; WORKERS],
}

impl RunState {
    fn new(initial_event_ts: i64) -> Self {
        Self {
            ready: AtomicU32::new(0),
            go: AtomicU32::new(0),
            stop: AtomicU32::new(0),
            _pad: [0_u32; 13],
            global_event_ts: AtomicI64::new(initial_event_ts),
            workers: std::array::from_fn(|_| WorkerStats::new()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LatencySummary {
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
}

#[derive(Debug)]
struct EngineRunResult {
    label: &'static str,
    elapsed: Duration,
    total_ops: u64,
    upsert_ops: u64,
    scan_ops: u64,
    hot_upserts: u64,
    conflicts: u64,
    index_remove_failures: u64,
    index_insert_failures: u64,
    tps: f64,
    total_latency: LatencySummary,
    upsert_latency: LatencySummary,
    scan_latency: LatencySummary,
    scan_server_exec_latency: Option<LatencySummary>,
    reclaim_telemetry: Option<ReclaimTelemetry>,
    index_retry_telemetry: Option<IndexRetryTelemetry>,
}

#[derive(Debug)]
struct ProfileRunResult {
    profile: CrucibleProfile,
    aerostore: EngineRunResult,
    postgres: EngineRunResult,
}

#[derive(Clone, Copy, Debug)]
struct CrucibleProfile {
    label: &'static str,
    aerostore_shm_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct ReclaimTelemetry {
    vacuum_reclaimed_rows: u64,
    index_retired_nodes_delta: u64,
    index_reclaimed_nodes_delta: u64,
    free_list_pushes_delta: u64,
    free_list_pops_delta: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct IndexRetryTelemetry {
    insert_ops: u64,
    remove_ops: u64,
    retry_loops: u64,
    retry_alloc: u64,
    retry_structural: u64,
    retry_epoch: u64,
    max_insert_attempts: u64,
    max_remove_attempts: u64,
}

#[derive(Clone, Copy)]
enum QueryKind {
    Upsert,
    Scan,
}

fn bench_hyperfeed_crucible(c: &mut Criterion) {
    let duration = crucible_duration();
    let results = run_crucible(duration);

    for result in &results {
        print_results(result, duration);
        assert_workload_mix(&result.aerostore, result.profile.label, "aerostore");
        assert_workload_mix(&result.postgres, result.profile.label, "postgres");
        assert_postgres_config_and_overhead(&result.postgres, result.profile.label);
        assert_performance_gates(result);
    }

    let mut group = c.benchmark_group("hyperfeed_crucible");
    group.sample_size(10);
    for result in &results {
        let tps_ratio = result.aerostore.tps / result.postgres.tps.max(f64::EPSILON);
        let p99_ratio = result.aerostore.total_latency.p99_ns as f64
            / (result.postgres.total_latency.p99_ns.max(1) as f64);
        let tps_label = format!("{}_aerostore_vs_postgres_tps_ratio", result.profile.label);
        let p99_label = format!("{}_aerostore_vs_postgres_p99_ratio", result.profile.label);

        group.bench_function(tps_label, |b| b.iter(|| black_box(tps_ratio)));
        group.bench_function(p99_label, |b| b.iter(|| black_box(p99_ratio)));
    }
    group.finish();
}

fn run_crucible(duration: Duration) -> Vec<ProfileRunResult> {
    let mut out = Vec::with_capacity(PROFILES.len());
    for profile in PROFILES {
        let aerostore = run_aerostore_crucible(duration, profile).unwrap_or_else(|err| {
            panic!("aerostore crucible run failed ({}): {err}", profile.label)
        });
        let postgres = run_postgres_crucible(duration, profile).unwrap_or_else(|err| {
            panic!("postgres crucible run failed ({}): {err}", profile.label)
        });
        out.push(ProfileRunResult {
            profile,
            aerostore,
            postgres,
        });
    }
    out
}

fn run_aerostore_crucible(
    duration: Duration,
    profile: CrucibleProfile,
) -> Result<EngineRunResult, String> {
    let shm = Arc::new(ShmArena::new(profile.aerostore_shm_bytes).map_err(|err| {
        format!(
            "failed to create Aerostore arena for {}: {}",
            profile.label, err
        )
    })?);
    let table = Arc::new(
        OccTable::<CrucibleRow>::new(Arc::clone(&shm), TOTAL_KEYS)
            .map_err(|err| err.to_string())?,
    );
    let time_index = SecondaryIndex::<usize>::new_in_shared("event_ts", Arc::clone(&shm));

    for row_id in 0..TOTAL_KEYS {
        let row = CrucibleRow::seeded(row_id);
        table.seed_row(row_id, row).map_err(|err| err.to_string())?;
        time_index.insert(IndexValue::I64(row.event_ts), row_id);
    }

    let state_ptr = shm
        .chunked_arena()
        .alloc(RunState::new(TOTAL_KEYS as i64))
        .map_err(|err| err.to_string())?;
    let state_offset = state_ptr.load(Ordering::Acquire);

    let reclaim_start = ReclaimSnapshot {
        index_retired_nodes: time_index.retired_nodes(),
        index_reclaimed_nodes: time_index.reclaimed_nodes(),
        free_list_pushes: shm.free_list_pushes(),
        free_list_pops: shm.free_list_pops(),
    };
    let vacuum_reclaimed_rows = Arc::new(AtomicU64::new(0));
    let reclaim_counter = Arc::clone(&vacuum_reclaimed_rows);
    let reclaim_callback: Arc<dyn Fn(&[VacuumReclaimedRow<CrucibleRow>]) + Send + Sync + 'static> =
        Arc::new(move |rows: &[VacuumReclaimedRow<CrucibleRow>]| {
            reclaim_counter.fetch_add(rows.len() as u64, Ordering::AcqRel);
        });
    let vacuum_config = VacuumDaemonConfig::default()
        .with_interval(vacuum_interval())
        .with_reclaim_callback(reclaim_callback);
    let vacuum_daemon = spawn_vacuum_daemon_with_config(Arc::clone(&table), vacuum_config)
        .map_err(|err| {
            format!(
                "failed to spawn vacuum daemon for {}: {}",
                profile.label, err
            )
        })?;
    let index_gc_daemon = match time_index.spawn_gc_daemon(index_gc_interval()) {
        Ok(daemon) => daemon,
        Err(err) => {
            let _ = vacuum_daemon.stop();
            return Err(format!(
                "failed to spawn index GC daemon for {}: {}",
                profile.label, err
            ));
        }
    };

    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .map_err(|err| format!("failed to create shared WAL ring for aerostore crucible: {err}"))?;

    let wal_path = unique_temp_path("aerostore_hyperfeed_crucible", "wal");
    if let Some(parent) = wal_path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    remove_if_exists(&wal_path);

    let wal_daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
        .map_err(|err| format!("failed to spawn Aerostore WAL daemon: {err}"))?;

    let mut pids = Vec::with_capacity(WORKERS);
    for worker_idx in 0..WORKERS {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            terminate_children(&pids);
            let _ = ring.close();
            let _ = wal_daemon.join();
            stop_background_daemons(&vacuum_daemon, &index_gc_daemon);
            remove_if_exists(&wal_path);
            return Err(format!(
                "fork failed for aerostore worker {}: {}",
                worker_idx,
                std::io::Error::last_os_error()
            ));
        }

        if pid == 0 {
            run_aerostore_worker(
                worker_idx,
                state_offset,
                shm.as_ref(),
                table.as_ref(),
                &time_index,
                ring.clone(),
            );
        }

        pids.push(pid);
    }

    let state = state_ptr
        .as_ref(shm.mmap_base())
        .ok_or_else(|| "failed to resolve Aerostore run-state".to_string())?;

    let startup_result = wait_for_ready_or_child_failure(
        || state.ready.load(Ordering::Acquire) == WORKERS as u32,
        &mut pids,
        Duration::from_secs(20),
        "aerostore workers did not reach startup barrier",
    );
    if let Err(err) = startup_result {
        terminate_children(&pids);
        let _ = ring.close();
        let _ = wal_daemon.join();
        stop_background_daemons(&vacuum_daemon, &index_gc_daemon);
        remove_if_exists(&wal_path);
        return Err(err);
    }

    let started = Instant::now();
    state.go.store(1, Ordering::Release);
    std::thread::sleep(duration);
    state.stop.store(1, Ordering::Release);

    let timed_out_workers =
        wait_for_children_or_terminate(&pids, duration + Duration::from_secs(60));
    let elapsed = started.elapsed();

    ring.close()
        .map_err(|err| format!("failed to close Aerostore ring: {err}"))?;
    wal_daemon
        .join()
        .map_err(|err| format!("Aerostore WAL daemon failed to exit cleanly: {err}"))?;
    stop_background_daemons(&vacuum_daemon, &index_gc_daemon);

    let reclaim_end = ReclaimSnapshot {
        index_retired_nodes: time_index.retired_nodes(),
        index_reclaimed_nodes: time_index.reclaimed_nodes(),
        free_list_pushes: shm.free_list_pushes(),
        free_list_pops: shm.free_list_pops(),
    };
    let reclaim_telemetry = ReclaimTelemetry {
        vacuum_reclaimed_rows: vacuum_reclaimed_rows.load(Ordering::Acquire),
        index_retired_nodes_delta: reclaim_end
            .index_retired_nodes
            .saturating_sub(reclaim_start.index_retired_nodes),
        index_reclaimed_nodes_delta: reclaim_end
            .index_reclaimed_nodes
            .saturating_sub(reclaim_start.index_reclaimed_nodes),
        free_list_pushes_delta: reclaim_end
            .free_list_pushes
            .saturating_sub(reclaim_start.free_list_pushes),
        free_list_pops_delta: reclaim_end
            .free_list_pops
            .saturating_sub(reclaim_start.free_list_pops),
    };
    let index_retry = time_index.mutation_telemetry();
    let index_retry_telemetry = IndexRetryTelemetry {
        insert_ops: index_retry.insert_ops,
        remove_ops: index_retry.remove_ops,
        retry_loops: index_retry.retry_loops,
        retry_alloc: index_retry.retry_alloc,
        retry_structural: index_retry.retry_structural,
        retry_epoch: index_retry.retry_epoch,
        max_insert_attempts: index_retry.max_insert_attempts,
        max_remove_attempts: index_retry.max_remove_attempts,
    };

    if timed_out_workers > 0 {
        remove_if_exists(&wal_path);
        return Err(format!(
            "aerostore crucible timed out waiting for {} workers",
            timed_out_workers
        ));
    }

    let result = aggregate_result(
        "aerostore",
        state,
        elapsed,
        false,
        Some(reclaim_telemetry),
        Some(index_retry_telemetry),
    );
    remove_if_exists(&wal_path);
    Ok(result)
}

#[derive(Clone, Copy, Debug, Default)]
struct ReclaimSnapshot {
    index_retired_nodes: u64,
    index_reclaimed_nodes: u64,
    free_list_pushes: u64,
    free_list_pops: u64,
}

fn run_aerostore_worker(
    worker_idx: usize,
    state_offset: u32,
    shm: &ShmArena,
    table: &OccTable<CrucibleRow>,
    time_index: &SecondaryIndex<usize>,
    ring: SharedWalRing<RING_SLOTS, RING_SLOT_BYTES>,
) -> ! {
    let Some(state) = RelPtr::<RunState>::from_offset(state_offset).as_ref(shm.mmap_base()) else {
        unsafe { libc::_exit(71) };
    };

    state.ready.fetch_add(1, Ordering::AcqRel);
    while state.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let mut rng = seed_rng(worker_idx as u64, 0xA3E0_52D1_9911_AA11);
    let mut retry = RetryBackoff::with_seed(next_u64(&mut rng), RetryPolicy::hot_key_default());
    let mut committer = OccCommitter::<RING_SLOTS, RING_SLOT_BYTES>::new_asynchronous(ring);

    let stats = &state.workers[worker_idx];
    let mut op_idx = 0_u64;
    let mut upsert_idx = 0_u64;

    'worker: while state.stop.load(Ordering::Acquire) == 0 {
        let kind = if (op_idx % MIX_PERIOD) < UPSERTS_PER_PERIOD {
            QueryKind::Upsert
        } else {
            QueryKind::Scan
        };

        let started = Instant::now();
        match kind {
            QueryKind::Upsert => {
                let is_hot = (upsert_idx % HOT_UPSERT_EVERY) == 0;
                let row_id = pick_row_id(worker_idx, is_hot, &mut rng);

                let mut attempts = 0_u32;
                loop {
                    if state.stop.load(Ordering::Acquire) != 0 {
                        break 'worker;
                    }
                    let mut tx = match table.begin_transaction() {
                        Ok(tx) => tx,
                        Err(_) => {
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            continue;
                        }
                    };

                    let current = match table.read(&mut tx, row_id) {
                        Ok(Some(row)) => row,
                        Ok(None) => {
                            let _ = table.abort(&mut tx);
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            continue;
                        }
                        Err(OccError::SerializationFailure) => {
                            let _ = table.abort(&mut tx);
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            continue;
                        }
                        Err(_) => {
                            let _ = table.abort(&mut tx);
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            continue;
                        }
                    };

                    let old_ts = current.event_ts;
                    let new_ts = state.global_event_ts.fetch_add(1, Ordering::AcqRel) + 1;
                    let mut next = current;
                    next.altitude = next.altitude.wrapping_add(1);
                    next.event_ts = new_ts;
                    next.payload[0] = next.payload[0].wrapping_add(1);

                    if table.write(&mut tx, row_id, next).is_err() {
                        let _ = table.abort(&mut tx);
                        stats.conflicts.fetch_add(1, Ordering::AcqRel);
                        attempts = attempts.saturating_add(1);
                        retry.sleep_for_attempt(attempts.saturating_sub(1));
                        continue;
                    }

                    match committer.commit(table, &mut tx) {
                        Ok(_) => {
                            if !move_index_payload_with_retry(
                                time_index,
                                old_ts,
                                new_ts,
                                row_id,
                                &state.stop,
                            ) && state.stop.load(Ordering::Acquire) == 0
                            {
                                stats.index_remove_failures.fetch_add(1, Ordering::AcqRel);
                                stats.index_insert_failures.fetch_add(1, Ordering::AcqRel);
                            }
                            break;
                        }
                        Err(WalWriterError::Occ(OccError::SerializationFailure)) => {
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            if state.stop.load(Ordering::Acquire) != 0 {
                                break 'worker;
                            }
                        }
                        Err(_) => {
                            let _ = table.abort(&mut tx);
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            if state.stop.load(Ordering::Acquire) != 0 {
                                break 'worker;
                            }
                        }
                    }
                }

                let elapsed_ns = nanos_u64(started.elapsed());
                stats.total_ops.fetch_add(1, Ordering::AcqRel);
                stats.upsert_ops.fetch_add(1, Ordering::AcqRel);
                if is_hot {
                    stats.hot_upserts.fetch_add(1, Ordering::AcqRel);
                }
                stats.total_latency.record_ns(elapsed_ns);
                stats.upsert_latency.record_ns(elapsed_ns);
                upsert_idx = upsert_idx.wrapping_add(1);
            }
            QueryKind::Scan => {
                let head_ts = state.global_event_ts.load(Ordering::Acquire);
                let bound = head_ts.saturating_sub(SCAN_TAIL_WINDOW);
                let hits = time_index.lookup_count_with_limit(
                    &IndexCompare::Gt(IndexValue::I64(bound)),
                    SCAN_LIMIT as usize,
                );
                black_box(hits);

                let elapsed_ns = nanos_u64(started.elapsed());
                stats.total_ops.fetch_add(1, Ordering::AcqRel);
                stats.scan_ops.fetch_add(1, Ordering::AcqRel);
                stats.total_latency.record_ns(elapsed_ns);
                stats.scan_latency.record_ns(elapsed_ns);
            }
        }

        op_idx = op_idx.wrapping_add(1);
    }

    unsafe { libc::_exit(0) }
}

fn run_postgres_crucible(
    duration: Duration,
    _profile: CrucibleProfile,
) -> Result<EngineRunResult, String> {
    ensure_docker_ready()?;
    let docker = clients::Cli::default();
    let image = GenericImage::new("postgres", "16")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "hyperfeed")
        .with_exposed_port(5432)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ));
    let image_args = vec![
        "-c".to_string(),
        "synchronous_commit=off".to_string(),
        "-c".to_string(),
        "fsync=on".to_string(),
        "-c".to_string(),
        "wal_writer_delay=10s".to_string(),
    ];

    let container = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        docker.run((image, image_args))
    }))
    .map_err(|panic_payload| {
        let reason = if let Some(msg) = panic_payload.downcast_ref::<&str>() {
            (*msg).to_string()
        } else if let Some(msg) = panic_payload.downcast_ref::<String>() {
            msg.clone()
        } else {
            "unknown panic".to_string()
        };
        format!(
            "failed to start PostgreSQL testcontainer (docker unavailable or daemon not ready): {}",
            reason
        )
    })?;
    let pg_port = container.get_host_port_ipv4(5432);
    let conn_str = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=hyperfeed connect_timeout=5",
        pg_port
    );

    let mut admin = connect_postgres_with_retry(conn_str.as_str(), Duration::from_secs(30))?;
    setup_postgres(&mut admin)?;
    verify_postgres_tuning(&mut admin)?;
    verify_postgres_index_plan(&mut admin)?;

    let metrics_shm = Arc::new(ShmArena::new(SHM_BYTES_METRICS).map_err(|err| err.to_string())?);
    let state_ptr = metrics_shm
        .chunked_arena()
        .alloc(RunState::new(TOTAL_KEYS as i64))
        .map_err(|err| err.to_string())?;
    let state_offset = state_ptr.load(Ordering::Acquire);

    let mut pids = Vec::with_capacity(WORKERS);
    for worker_idx in 0..WORKERS {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(format!(
                "fork failed for postgres worker {}: {}",
                worker_idx,
                std::io::Error::last_os_error()
            ));
        }

        if pid == 0 {
            run_postgres_worker(
                worker_idx,
                state_offset,
                metrics_shm.as_ref(),
                conn_str.as_str(),
            );
        }

        pids.push(pid);
    }

    let state = state_ptr
        .as_ref(metrics_shm.mmap_base())
        .ok_or_else(|| "failed to resolve postgres run-state".to_string())?;

    wait_for_ready_or_child_failure(
        || state.ready.load(Ordering::Acquire) == WORKERS as u32,
        &mut pids,
        Duration::from_secs(30),
        "postgres workers did not reach startup barrier",
    )
    .map_err(|err| {
        terminate_children(&pids);
        err
    })?;

    let started = Instant::now();
    state.go.store(1, Ordering::Release);
    std::thread::sleep(duration);
    state.stop.store(1, Ordering::Release);

    let timed_out_workers =
        wait_for_children_or_terminate(&pids, duration + Duration::from_secs(60));
    let elapsed = started.elapsed();

    if timed_out_workers > 0 {
        return Err(format!(
            "postgres crucible timed out waiting for {} workers",
            timed_out_workers
        ));
    }

    Ok(aggregate_result(
        "postgres", state, elapsed, true, None, None,
    ))
}

fn ensure_docker_ready() -> Result<(), String> {
    let output = Command::new("docker").arg("info").output().map_err(|err| {
        format!(
            "docker is required for hyperfeed_crucible but could not be executed: {}",
            err
        )
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "docker is required for hyperfeed_crucible but is not ready (docker info failed): {}",
            stderr.trim()
        ));
    }

    Ok(())
}

fn run_postgres_worker(worker_idx: usize, state_offset: u32, shm: &ShmArena, conn_str: &str) -> ! {
    let Some(state) = RelPtr::<RunState>::from_offset(state_offset).as_ref(shm.mmap_base()) else {
        unsafe { libc::_exit(81) };
    };

    let mut client = match connect_postgres_with_retry(conn_str, Duration::from_secs(20)) {
        Ok(client) => client,
        Err(_) => unsafe { libc::_exit(82) },
    };

    if client
        .batch_execute("SET synchronous_commit TO off;")
        .is_err()
    {
        unsafe { libc::_exit(83) }
    }

    let upsert_stmt = match client.prepare(concat!(
        "INSERT INTO flight_state (id, altitude, event_ts, payload) ",
        "VALUES ($1, $2, $3, $4) ",
        "ON CONFLICT (id) DO UPDATE ",
        "SET altitude = EXCLUDED.altitude, event_ts = EXCLUDED.event_ts, payload = EXCLUDED.payload"
    )) {
        Ok(stmt) => stmt,
        Err(_) => unsafe { libc::_exit(84) },
    };

    let scan_stmt = match client.prepare(
        "SELECT id, event_ts FROM flight_state WHERE event_ts > $1 ORDER BY event_ts ASC LIMIT $2",
    ) {
        Ok(stmt) => stmt,
        Err(_) => unsafe { libc::_exit(85) },
    };

    let explain_stmt = match client.prepare(concat!(
        "EXPLAIN (ANALYZE, FORMAT JSON) ",
        "SELECT id, event_ts FROM flight_state WHERE event_ts > $1 ORDER BY event_ts ASC LIMIT $2"
    )) {
        Ok(stmt) => stmt,
        Err(_) => unsafe { libc::_exit(86) },
    };

    state.ready.fetch_add(1, Ordering::AcqRel);
    while state.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let mut rng = seed_rng(worker_idx as u64, 0xCC77_AA22_1958_3321);
    let mut retry = RetryBackoff::with_seed(next_u64(&mut rng), RetryPolicy::hot_key_default());
    let stats = &state.workers[worker_idx];

    let mut op_idx = 0_u64;
    let mut upsert_idx = 0_u64;
    let mut scan_ops = 0_u64;

    'worker: while state.stop.load(Ordering::Acquire) == 0 {
        let kind = if (op_idx % MIX_PERIOD) < UPSERTS_PER_PERIOD {
            QueryKind::Upsert
        } else {
            QueryKind::Scan
        };

        let started = Instant::now();
        match kind {
            QueryKind::Upsert => {
                let is_hot = (upsert_idx % HOT_UPSERT_EVERY) == 0;
                let row_id = pick_row_id(worker_idx, is_hot, &mut rng) as i64;
                let new_ts = state.global_event_ts.fetch_add(1, Ordering::AcqRel) + 1;
                let altitude = 30_000 + ((new_ts as i32) & 0x7ff);
                let payload = payload_for_key(row_id as usize, new_ts);

                let mut attempts = 0_u32;
                loop {
                    if state.stop.load(Ordering::Acquire) != 0 {
                        break 'worker;
                    }
                    let exec =
                        client.execute(&upsert_stmt, &[&row_id, &altitude, &new_ts, &&payload[..]]);
                    match exec {
                        Ok(_) => break,
                        Err(_) => {
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            if state.stop.load(Ordering::Acquire) != 0 {
                                break 'worker;
                            }
                        }
                    }
                }

                let elapsed_ns = nanos_u64(started.elapsed());
                stats.total_ops.fetch_add(1, Ordering::AcqRel);
                stats.upsert_ops.fetch_add(1, Ordering::AcqRel);
                if is_hot {
                    stats.hot_upserts.fetch_add(1, Ordering::AcqRel);
                }
                stats.total_latency.record_ns(elapsed_ns);
                stats.upsert_latency.record_ns(elapsed_ns);
                upsert_idx = upsert_idx.wrapping_add(1);
            }
            QueryKind::Scan => {
                let head_ts = state.global_event_ts.load(Ordering::Acquire);
                let bound = head_ts.saturating_sub(SCAN_TAIL_WINDOW);

                let query_res = client.query(&scan_stmt, &[&bound, &SCAN_LIMIT]);
                if query_res.is_err() {
                    stats.conflicts.fetch_add(1, Ordering::AcqRel);
                    std::thread::yield_now();
                    op_idx = op_idx.wrapping_add(1);
                    continue;
                }
                let rows = query_res.unwrap_or_default();
                black_box(rows.len());

                let elapsed_ns = nanos_u64(started.elapsed());
                stats.total_ops.fetch_add(1, Ordering::AcqRel);
                stats.scan_ops.fetch_add(1, Ordering::AcqRel);
                stats.total_latency.record_ns(elapsed_ns);
                stats.scan_latency.record_ns(elapsed_ns);

                scan_ops = scan_ops.wrapping_add(1);
                if scan_ops % PG_EXPLAIN_SAMPLE_EVERY == 0 {
                    if let Ok(explain_rows) = client.query(&explain_stmt, &[&bound, &SCAN_LIMIT]) {
                        if let Some(exec_ns) = explain_execution_time_ns(explain_rows.as_slice()) {
                            stats.scan_server_exec_latency.record_ns(exec_ns);
                        }
                    }
                }
            }
        }

        op_idx = op_idx.wrapping_add(1);
    }

    unsafe { libc::_exit(0) }
}

fn setup_postgres(client: &mut Client) -> Result<(), String> {
    client
        .batch_execute(
            "DROP TABLE IF EXISTS flight_state;\
             CREATE TABLE flight_state (\
                 id BIGINT PRIMARY KEY,\
                 altitude INTEGER NOT NULL,\
                 event_ts BIGINT NOT NULL,\
                 payload BYTEA NOT NULL\
             );\
             CREATE INDEX flight_state_event_ts_idx ON flight_state (event_ts);",
        )
        .map_err(|err| format!("failed to create postgres schema: {err}"))?;

    let mut tx = client
        .transaction()
        .map_err(|err| format!("failed to open postgres seed transaction: {err}"))?;

    let stmt = tx
        .prepare(
            "INSERT INTO flight_state (id, altitude, event_ts, payload) VALUES ($1, $2, $3, $4)",
        )
        .map_err(|err| format!("failed to prepare postgres seed insert: {err}"))?;

    for row_id in 0..TOTAL_KEYS {
        let id = row_id as i64;
        let altitude = 30_000 + (row_id % 2_000) as i32;
        let event_ts = row_id as i64;
        let payload = payload_for_key(row_id, event_ts);
        tx.execute(&stmt, &[&id, &altitude, &event_ts, &&payload[..]])
            .map_err(|err| format!("failed to seed postgres row {row_id}: {err}"))?;
    }

    tx.commit()
        .map_err(|err| format!("failed to commit postgres seed transaction: {err}"))?;

    client
        .batch_execute("ANALYZE flight_state;")
        .map_err(|err| format!("failed to analyze postgres table: {err}"))?;
    Ok(())
}

fn verify_postgres_tuning(client: &mut Client) -> Result<(), String> {
    let sync_commit = client
        .query_one("SHOW synchronous_commit", &[])
        .map_err(|err| format!("failed to read synchronous_commit: {err}"))?
        .get::<usize, String>(0);
    let fsync = client
        .query_one("SHOW fsync", &[])
        .map_err(|err| format!("failed to read fsync: {err}"))?
        .get::<usize, String>(0);
    let wal_writer_delay = client
        .query_one("SHOW wal_writer_delay", &[])
        .map_err(|err| format!("failed to read wal_writer_delay: {err}"))?
        .get::<usize, String>(0);

    if sync_commit.trim() != "off" {
        return Err(format!(
            "postgres tuning mismatch: expected synchronous_commit=off, observed {}",
            sync_commit.trim()
        ));
    }
    if fsync.trim() != "on" {
        return Err(format!(
            "postgres tuning mismatch: expected fsync=on, observed {}",
            fsync.trim()
        ));
    }
    if wal_writer_delay.trim() != "10s" {
        return Err(format!(
            "postgres tuning mismatch: expected wal_writer_delay=10s, observed {}",
            wal_writer_delay.trim()
        ));
    }

    Ok(())
}

fn verify_postgres_index_plan(client: &mut Client) -> Result<(), String> {
    let rows = client
        .query(
            "EXPLAIN (FORMAT JSON) SELECT id, event_ts FROM flight_state \
             WHERE event_ts > $1 ORDER BY event_ts ASC LIMIT $2",
            &[&((TOTAL_KEYS as i64) - SCAN_TAIL_WINDOW), &SCAN_LIMIT],
        )
        .map_err(|err| format!("failed to collect explain plan: {err}"))?;

    let Some(row) = rows.first() else {
        return Err("postgres explain returned no rows".to_string());
    };

    let plan_json = decode_json_column(row)
        .map_err(|err| format!("failed to decode postgres explain json: {err}"))?;

    if !postgres_plan_uses_index(&plan_json) {
        return Err(format!(
            "postgres explain plan for event_ts range scan did not use index: {}",
            plan_json
        ));
    }

    Ok(())
}

fn postgres_plan_uses_index(value: &Value) -> bool {
    if let Some(array) = value.as_array() {
        for entry in array {
            if let Some(plan) = entry.get("Plan") {
                if plan_uses_index_node(plan) {
                    return true;
                }
            }
            if plan_uses_index_node(entry) {
                return true;
            }
        }
    }
    plan_uses_index_node(value)
}

fn plan_uses_index_node(value: &Value) -> bool {
    if let Some(obj) = value.as_object() {
        if let Some(node_type) = obj.get("Node Type").and_then(Value::as_str) {
            if node_type.contains("Index") {
                return true;
            }
        }

        if let Some(plans) = obj.get("Plans").and_then(Value::as_array) {
            for plan in plans {
                if plan_uses_index_node(plan) {
                    return true;
                }
            }
        }
    }

    if let Some(arr) = value.as_array() {
        for item in arr {
            if plan_uses_index_node(item) {
                return true;
            }
        }
    }

    false
}

fn connect_postgres_with_retry(conn_str: &str, timeout: Duration) -> Result<Client, String> {
    let started = Instant::now();
    loop {
        match Client::connect(conn_str, NoTls) {
            Ok(client) => return Ok(client),
            Err(err) => {
                if started.elapsed() >= timeout {
                    return Err(format!(
                        "unable to connect to postgres within {:?}: {}",
                        timeout, err
                    ));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn explain_execution_time_ns(rows: &[Row]) -> Option<u64> {
    let row = rows.first()?;

    let value = decode_json_column(row).ok()?;

    let arr = value.as_array()?;
    let root = arr.first()?.as_object()?;
    let exec_ms = root.get("Execution Time")?.as_f64()?;
    let exec_ns = (exec_ms * 1_000_000.0).max(0.0) as u64;
    Some(exec_ns)
}

fn decode_json_column(row: &Row) -> Result<Value, String> {
    if let Ok(value) = row.try_get::<usize, Value>(0) {
        return Ok(value);
    }

    let text = row
        .try_get::<usize, String>(0)
        .map_err(|err| format!("failed to read json column as text: {err}"))?;
    serde_json::from_str::<Value>(text.as_str())
        .map_err(|err| format!("failed to parse json text: {err}"))
}

fn aggregate_result(
    label: &'static str,
    state: &RunState,
    elapsed: Duration,
    include_server_exec: bool,
    reclaim_telemetry: Option<ReclaimTelemetry>,
    index_retry_telemetry: Option<IndexRetryTelemetry>,
) -> EngineRunResult {
    let mut total_ops = 0_u64;
    let mut upsert_ops = 0_u64;
    let mut scan_ops = 0_u64;
    let mut hot_upserts = 0_u64;
    let mut conflicts = 0_u64;
    let mut index_remove_failures = 0_u64;
    let mut index_insert_failures = 0_u64;

    let mut total_hist = [0_u64; HIST_BUCKETS];
    let mut upsert_hist = [0_u64; HIST_BUCKETS];
    let mut scan_hist = [0_u64; HIST_BUCKETS];
    let mut server_exec_hist = [0_u64; HIST_BUCKETS];
    let mut total_samples = 0_u64;
    let mut upsert_samples = 0_u64;
    let mut scan_samples = 0_u64;
    let mut server_exec_samples = 0_u64;

    for worker in &state.workers {
        total_ops = total_ops.saturating_add(worker.total_ops.load(Ordering::Acquire));
        upsert_ops = upsert_ops.saturating_add(worker.upsert_ops.load(Ordering::Acquire));
        scan_ops = scan_ops.saturating_add(worker.scan_ops.load(Ordering::Acquire));
        hot_upserts = hot_upserts.saturating_add(worker.hot_upserts.load(Ordering::Acquire));
        conflicts = conflicts.saturating_add(worker.conflicts.load(Ordering::Acquire));
        index_remove_failures = index_remove_failures
            .saturating_add(worker.index_remove_failures.load(Ordering::Acquire));
        index_insert_failures = index_insert_failures
            .saturating_add(worker.index_insert_failures.load(Ordering::Acquire));

        let (worker_total_hist, worker_total_samples) = worker.total_latency.snapshot();
        let (worker_upsert_hist, worker_upsert_samples) = worker.upsert_latency.snapshot();
        let (worker_scan_hist, worker_scan_samples) = worker.scan_latency.snapshot();
        let (worker_server_hist, worker_server_samples) =
            worker.scan_server_exec_latency.snapshot();

        merge_histograms(&mut total_hist, &worker_total_hist);
        merge_histograms(&mut upsert_hist, &worker_upsert_hist);
        merge_histograms(&mut scan_hist, &worker_scan_hist);
        merge_histograms(&mut server_exec_hist, &worker_server_hist);

        total_samples = total_samples.saturating_add(worker_total_samples);
        upsert_samples = upsert_samples.saturating_add(worker_upsert_samples);
        scan_samples = scan_samples.saturating_add(worker_scan_samples);
        server_exec_samples = server_exec_samples.saturating_add(worker_server_samples);
    }

    let total_latency = latency_summary(&total_hist, total_samples);
    let upsert_latency = latency_summary(&upsert_hist, upsert_samples);
    let scan_latency = latency_summary(&scan_hist, scan_samples);
    let scan_server_exec_latency = if include_server_exec {
        Some(latency_summary(&server_exec_hist, server_exec_samples))
    } else {
        None
    };

    EngineRunResult {
        label,
        elapsed,
        total_ops,
        upsert_ops,
        scan_ops,
        hot_upserts,
        conflicts,
        index_remove_failures,
        index_insert_failures,
        tps: total_ops as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        total_latency,
        upsert_latency,
        scan_latency,
        scan_server_exec_latency,
        reclaim_telemetry,
        index_retry_telemetry,
    }
}

fn print_results(result: &ProfileRunResult, duration: Duration) {
    let aerostore = &result.aerostore;
    let postgres = &result.postgres;

    let tps_ratio = aerostore.tps / postgres.tps.max(f64::EPSILON);
    let total_p99_ratio =
        aerostore.total_latency.p99_ns as f64 / (postgres.total_latency.p99_ns.max(1) as f64);

    let pg_scan_server = postgres.scan_server_exec_latency.unwrap_or(LatencySummary {
        p50_ns: 0,
        p90_ns: 0,
        p99_ns: 0,
    });

    let pg_overhead_p50 = postgres
        .scan_latency
        .p50_ns
        .saturating_sub(pg_scan_server.p50_ns);
    let pg_overhead_p90 = postgres
        .scan_latency
        .p90_ns
        .saturating_sub(pg_scan_server.p90_ns);
    let pg_overhead_p99 = postgres
        .scan_latency
        .p99_ns
        .saturating_sub(pg_scan_server.p99_ns);

    println!(
        "hyperfeed_crucible_config: profile={} aerostore_shm_bytes={} vacuum_interval_ms={} index_gc_interval_ms={} workers={} duration_secs={} mix_upsert={} mix_scan={} hot_upsert_share=5% postgres={{synchronous_commit=off,fsync=on,wal_writer_delay=10s}}",
        result.profile.label,
        result.profile.aerostore_shm_bytes,
        vacuum_interval().as_millis(),
        index_gc_interval().as_millis(),
        WORKERS,
        duration.as_secs(),
        UPSERTS_PER_PERIOD,
        MIX_PERIOD - UPSERTS_PER_PERIOD,
    );

    println!(
        "| Engine | TPS | Total Ops | p50 (us) | p90 (us) | p99 (us) | Upserts | Scans | Hot Upserts | Conflicts | Index Remove Fail | Index Insert Fail |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    println!(
        "| {} | {:.2} | {} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} |",
        aerostore.label,
        aerostore.tps,
        aerostore.total_ops,
        ns_to_us(aerostore.total_latency.p50_ns),
        ns_to_us(aerostore.total_latency.p90_ns),
        ns_to_us(aerostore.total_latency.p99_ns),
        aerostore.upsert_ops,
        aerostore.scan_ops,
        aerostore.hot_upserts,
        aerostore.conflicts,
        aerostore.index_remove_failures,
        aerostore.index_insert_failures,
    );
    println!(
        "| {} | {:.2} | {} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} |",
        postgres.label,
        postgres.tps,
        postgres.total_ops,
        ns_to_us(postgres.total_latency.p50_ns),
        ns_to_us(postgres.total_latency.p90_ns),
        ns_to_us(postgres.total_latency.p99_ns),
        postgres.upsert_ops,
        postgres.scan_ops,
        postgres.hot_upserts,
        postgres.conflicts,
        postgres.index_remove_failures,
        postgres.index_insert_failures,
    );

    if let Some(reclaim) = aerostore.reclaim_telemetry {
        println!(
            "| Aerostore Reclaim Telemetry | vacuum_reclaimed_rows | index_retired_nodes_delta | index_reclaimed_nodes_delta | free_list_pushes_delta | free_list_pops_delta |"
        );
        println!("|---|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {} | {} | {} | {} |",
            result.profile.label,
            reclaim.vacuum_reclaimed_rows,
            reclaim.index_retired_nodes_delta,
            reclaim.index_reclaimed_nodes_delta,
            reclaim.free_list_pushes_delta,
            reclaim.free_list_pops_delta,
        );
    }

    if let Some(retry) = aerostore.index_retry_telemetry {
        println!(
            "| Aerostore Index Retry Telemetry | insert_ops | remove_ops | retry_loops | retry_alloc | retry_structural | retry_epoch | max_insert_attempts | max_remove_attempts |"
        );
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            result.profile.label,
            retry.insert_ops,
            retry.remove_ops,
            retry.retry_loops,
            retry.retry_alloc,
            retry.retry_structural,
            retry.retry_epoch,
            retry.max_insert_attempts,
            retry.max_remove_attempts,
        );
    }

    println!(
        "| Postgres Scan Breakdown | Client RTT p50 (us) | Server Exec p50 (us) | IPC/Protocol p50 (us) | Client RTT p90 (us) | Server Exec p90 (us) | IPC/Protocol p90 (us) | Client RTT p99 (us) | Server Exec p99 (us) | IPC/Protocol p99 (us) |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    println!(
        "| postgres_scan | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
        ns_to_us(postgres.scan_latency.p50_ns),
        ns_to_us(pg_scan_server.p50_ns),
        ns_to_us(pg_overhead_p50),
        ns_to_us(postgres.scan_latency.p90_ns),
        ns_to_us(pg_scan_server.p90_ns),
        ns_to_us(pg_overhead_p90),
        ns_to_us(postgres.scan_latency.p99_ns),
        ns_to_us(pg_scan_server.p99_ns),
        ns_to_us(pg_overhead_p99),
    );

    println!("| Aerostore Raw Scan | p50 (us) | p90 (us) | p99 (us) |",);
    println!("|---|---:|---:|---:|");
    println!(
        "| aerostore_scan | {:.2} | {:.2} | {:.2} |",
        ns_to_us(aerostore.scan_latency.p50_ns),
        ns_to_us(aerostore.scan_latency.p90_ns),
        ns_to_us(aerostore.scan_latency.p99_ns),
    );

    println!(
        "hyperfeed_crucible_summary: profile={} tps_ratio_aerostore_vs_postgres={:.2}x total_p99_ratio={:.3}",
        result.profile.label,
        tps_ratio,
        total_p99_ratio,
    );
    println!(
        "hyperfeed_crucible_engine_timing: profile={} aerostore_elapsed_secs={:.3} postgres_elapsed_secs={:.3} aerostore_upsert_p99_us={:.2} postgres_upsert_p99_us={:.2}",
        result.profile.label,
        aerostore.elapsed.as_secs_f64(),
        postgres.elapsed.as_secs_f64(),
        ns_to_us(aerostore.upsert_latency.p99_ns),
        ns_to_us(postgres.upsert_latency.p99_ns),
    );
}

fn assert_workload_mix(result: &EngineRunResult, profile_label: &str, engine_label: &str) {
    let total = result.total_ops.max(1) as f64;
    let upsert_ratio = result.upsert_ops as f64 / total;
    let scan_ratio = result.scan_ops as f64 / total;
    let hot_ratio = result.hot_upserts as f64 / result.upsert_ops.max(1) as f64;

    let expected_upsert = UPSERTS_PER_PERIOD as f64 / MIX_PERIOD as f64;
    let expected_scan = 1.0 - expected_upsert;
    let expected_hot = 1.0 / HOT_UPSERT_EVERY as f64;

    assert!(
        (upsert_ratio - expected_upsert).abs() <= MIX_RATIO_TOLERANCE,
        "{} {} workload mix drift: upsert ratio {:.4} outside {:.4} ± {:.4}",
        profile_label,
        engine_label,
        upsert_ratio,
        expected_upsert,
        MIX_RATIO_TOLERANCE
    );
    assert!(
        (scan_ratio - expected_scan).abs() <= MIX_RATIO_TOLERANCE,
        "{} {} workload mix drift: scan ratio {:.4} outside {:.4} ± {:.4}",
        profile_label,
        engine_label,
        scan_ratio,
        expected_scan,
        MIX_RATIO_TOLERANCE
    );
    assert!(
        (hot_ratio - expected_hot).abs() <= HOT_RATIO_TOLERANCE,
        "{} {} workload mix drift: hot-upsert ratio {:.4} outside {:.4} ± {:.4}",
        profile_label,
        engine_label,
        hot_ratio,
        expected_hot,
        HOT_RATIO_TOLERANCE
    );
}

fn assert_postgres_config_and_overhead(postgres: &EngineRunResult, profile_label: &str) {
    let server = postgres.scan_server_exec_latency.unwrap_or(LatencySummary {
        p50_ns: 0,
        p90_ns: 0,
        p99_ns: 0,
    });

    assert!(
        server.p50_ns > 0,
        "{} postgres scan execution-time samples were not collected",
        profile_label
    );
    assert!(
        postgres.scan_latency.p50_ns > server.p50_ns,
        "{} expected postgres client RTT p50 ({}) to exceed server execution p50 ({})",
        profile_label,
        postgres.scan_latency.p50_ns,
        server.p50_ns
    );
}

fn assert_performance_gates(result: &ProfileRunResult) {
    let tps_ratio = result.aerostore.tps / result.postgres.tps.max(f64::EPSILON);
    let p99_ratio = result.aerostore.total_latency.p99_ns as f64
        / (result.postgres.total_latency.p99_ns.max(1) as f64);

    assert!(
        tps_ratio >= REQUIRED_TPS_RATIO,
        "{} Aerostore TPS gate failed: observed {:.2}x, required >= {:.2}x",
        result.profile.label,
        tps_ratio,
        REQUIRED_TPS_RATIO
    );
    assert!(
        p99_ratio <= REQUIRED_P99_RATIO,
        "{} Aerostore p99 gate failed: observed {:.3}, required <= {:.3}",
        result.profile.label,
        p99_ratio,
        REQUIRED_P99_RATIO
    );
}

fn merge_histograms(dst: &mut [u64; HIST_BUCKETS], src: &[u64; HIST_BUCKETS]) {
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d = d.saturating_add(*s);
    }
}

fn latency_summary(hist: &[u64; HIST_BUCKETS], total_samples: u64) -> LatencySummary {
    LatencySummary {
        p50_ns: percentile_ns(hist, total_samples, 0.50),
        p90_ns: percentile_ns(hist, total_samples, 0.90),
        p99_ns: percentile_ns(hist, total_samples, 0.99),
    }
}

fn percentile_ns(hist: &[u64; HIST_BUCKETS], total_samples: u64, quantile: f64) -> u64 {
    if total_samples == 0 {
        return 0;
    }

    let target = ((total_samples as f64) * quantile).ceil() as u64;
    let mut cumulative = 0_u64;

    for (idx, count) in hist.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= target {
            return bucket_upper_bound_ns(idx);
        }
    }

    bucket_upper_bound_ns(HIST_BUCKETS - 1)
}

#[inline]
fn bucket_upper_bound_ns(bucket_idx: usize) -> u64 {
    if bucket_idx >= 63 {
        u64::MAX
    } else {
        1_u64 << bucket_idx
    }
}

#[inline]
fn latency_bucket(latency_ns: u64) -> usize {
    let val = latency_ns.max(1);
    let idx = (63_u32.saturating_sub(val.leading_zeros())) as usize;
    idx.min(HIST_BUCKETS - 1)
}

#[inline]
fn nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[inline]
fn ns_to_us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

fn pick_row_id(worker_idx: usize, is_hot: bool, rng_state: &mut u64) -> usize {
    if is_hot {
        return (next_u64(rng_state) as usize) % HOT_KEY_COUNT.max(1);
    }

    let cold_total = TOTAL_KEYS.saturating_sub(HOT_KEY_COUNT).max(1);
    let stripe = (cold_total / WORKERS.max(1)).max(1);
    let base = HOT_KEY_COUNT + (worker_idx.min(WORKERS - 1) * stripe);
    let span = if worker_idx == WORKERS - 1 {
        cold_total.saturating_sub(stripe * (WORKERS - 1)).max(1)
    } else {
        stripe
    };

    base + ((next_u64(rng_state) as usize) % span)
}

fn move_index_payload_with_retry(
    time_index: &SecondaryIndex<usize>,
    old_ts: i64,
    new_ts: i64,
    row_id: usize,
    stop: &AtomicU32,
) -> bool {
    const MOVE_RETRY_LIMIT: usize = 16;
    for attempt in 0..=MOVE_RETRY_LIMIT {
        if stop.load(Ordering::Acquire) != 0 {
            return false;
        }
        if time_index
            .try_move_payload(&IndexValue::I64(old_ts), IndexValue::I64(new_ts), &row_id)
            .is_ok()
        {
            return true;
        }

        if attempt == MOVE_RETRY_LIMIT {
            return false;
        }

        if attempt < 8 {
            std::hint::spin_loop();
        } else if attempt < 12 {
            std::thread::yield_now();
        } else {
            let shift = ((attempt - 12) / 4).min(4);
            let sleep_us = 50_u64 << shift;
            std::thread::sleep(Duration::from_micros(sleep_us));
        }
    }
    false
}

#[inline]
fn payload_for_key(row_id: usize, event_ts: i64) -> [u8; 32] {
    let mut payload = [0_u8; 32];
    payload[..8].copy_from_slice(&(row_id as u64).to_le_bytes());
    payload[8..16].copy_from_slice(&(event_ts as u64).to_le_bytes());
    payload[16..24]
        .copy_from_slice(&(row_id as u64 ^ (event_ts as u64).rotate_left(7)).to_le_bytes());
    payload[24..32].copy_from_slice(&((row_id as u64).wrapping_mul(17)).to_le_bytes());
    payload
}

fn wait_for_ready_or_child_failure<F>(
    mut condition: F,
    pids: &mut Vec<libc::pid_t>,
    timeout: Duration,
    message: &str,
) -> Result<(), String>
where
    F: FnMut() -> bool,
{
    let started = Instant::now();
    while !condition() {
        let mut still_running = Vec::with_capacity(pids.len());
        for pid in pids.iter().copied() {
            let mut status: libc::c_int = 0;
            let waited =
                unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, libc::WNOHANG) };
            if waited == 0 {
                still_running.push(pid);
                continue;
            }
            if waited == pid {
                if libc::WIFEXITED(status) {
                    let code = libc::WEXITSTATUS(status);
                    return Err(format!(
                        "{} (worker pid {} exited early with status {})",
                        message, pid, code
                    ));
                }
                if libc::WIFSIGNALED(status) {
                    let sig = libc::WTERMSIG(status);
                    return Err(format!(
                        "{} (worker pid {} terminated by signal {})",
                        message, pid, sig
                    ));
                }
                return Err(format!(
                    "{} (worker pid {} exited unexpectedly; raw_status={})",
                    message, pid, status
                ));
            }
            if waited < 0 {
                return Err(format!(
                    "{} (waitpid failed for pid {}: {})",
                    message,
                    pid,
                    std::io::Error::last_os_error()
                ));
            }
            still_running.push(pid);
        }
        *pids = still_running;

        if started.elapsed() >= timeout {
            return Err(message.to_string());
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    Ok(())
}

fn wait_for_children_or_terminate(pids: &[libc::pid_t], timeout: Duration) -> usize {
    let mut pending = pids.to_vec();
    let started = Instant::now();

    while !pending.is_empty() {
        pending.retain(|pid| {
            let mut status: libc::c_int = 0;
            let waited =
                unsafe { libc::waitpid(*pid, &mut status as *mut libc::c_int, libc::WNOHANG) };
            waited == 0
        });

        if pending.is_empty() {
            return 0;
        }

        if started.elapsed() >= timeout {
            let timed_out = pending.len();
            for pid in &pending {
                unsafe {
                    libc::kill(*pid, libc::SIGKILL);
                    libc::waitpid(*pid, std::ptr::null_mut(), 0);
                }
            }
            return timed_out;
        }

        std::thread::sleep(Duration::from_millis(2));
    }

    0
}

fn terminate_children(pids: &[libc::pid_t]) {
    for pid in pids {
        unsafe {
            libc::kill(*pid, libc::SIGKILL);
            libc::waitpid(*pid, std::ptr::null_mut(), 0);
        }
    }
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

fn unique_temp_path(prefix: &str, ext: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nonce}.{ext}"))
}

fn crucible_duration() -> Duration {
    let seconds = std::env::var("AEROSTORE_CRUCIBLE_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(60);
    Duration::from_secs(seconds)
}

fn vacuum_interval() -> Duration {
    let millis = std::env::var("AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_VACUUM_INTERVAL_MS);
    Duration::from_millis(millis)
}

fn index_gc_interval() -> Duration {
    let millis = std::env::var("AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_INDEX_GC_INTERVAL_MS);
    Duration::from_millis(millis)
}

fn stop_background_daemons(
    vacuum_daemon: &VacuumDaemon<CrucibleRow>,
    index_gc_daemon: &ShmIndexGcDaemon,
) {
    if let Err(err) = vacuum_daemon.stop() {
        eprintln!("warning: failed to stop vacuum daemon cleanly: {}", err);
    }
    if let Err(err) = index_gc_daemon.terminate(libc::SIGTERM) {
        eprintln!("warning: failed to signal index GC daemon: {}", err);
    }
    if let Err(err) = index_gc_daemon.join() {
        eprintln!("warning: failed to join index GC daemon: {}", err);
    }
}

#[inline]
fn seed_rng(worker: u64, salt: u64) -> u64 {
    let pid = unsafe { libc::getpid() as u64 };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut state = worker
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(pid.rotate_left(17))
        .wrapping_add(now)
        .wrapping_add(salt);
    if state == 0 {
        state = 1;
    }
    state
}

#[inline]
fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    if x == 0 {
        x = 1;
    }
    *state = x;
    x
}

criterion_group!(benches, bench_hyperfeed_crucible);
criterion_main!(benches);
