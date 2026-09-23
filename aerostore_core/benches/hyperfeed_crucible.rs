#![cfg(unix)]

use std::fs;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::shm::ARENA_CLASS_COUNT;
use aerostore_core::{
    spawn_vacuum_daemon_with_config, spawn_wal_writer_daemon, IndexCompare, IndexValue,
    OccCommitter, OccError, OccRecycleTelemetry, OccTable, RelPtr, RetryBackoff, RetryPolicy,
    SecondaryIndex, SharedWalRing, ShmArena, ShmIndexGcDaemon, VacuumDaemon, VacuumDaemonConfig,
    VacuumReclaimedRow, WalDeltaCodec, WalWriterError,
};
use criterion::{criterion_group, criterion_main, Criterion};
use postgres::{Client, NoTls, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use testcontainers::clients;
use testcontainers::core::WaitFor;
use testcontainers::GenericImage;

#[path = "support/latency_histogram.rs"]
mod latency_histogram;
use latency_histogram::{
    latency_bucket, merge_histograms, percentile_bounds, HIST_BUCKETS, SUBDIVISIONS,
};

#[path = "support/crucible_seed.rs"]
mod crucible_seed;
use crucible_seed::{fixed_worker_seed, next_u64, parse_seed, FIXED_SEED_ALGORITHM, SEED_ENV};

const WORKERS: usize = 16;
const TOTAL_KEYS: usize = 50_000;
const HOT_KEY_COUNT: usize = 256;
const MIX_PERIOD: u64 = 100;
const UPSERTS_PER_PERIOD: u64 = 80;
const HOT_UPSERT_EVERY: u64 = 20; // 5% of upserts
const SCAN_LIMIT: i64 = 64;
const SCAN_TAIL_WINDOW: i64 = 4_096;
const RING_SLOTS: usize = 2048;
const RING_SLOT_BYTES: usize = 256;
const SHM_BYTES_METRICS: usize = 16 << 20;
const PG_EXPLAIN_SAMPLE_EVERY: u64 = 128;
const SHM_BYTES_AEROSTORE_512M: usize = 512 << 20;
const SHM_BYTES_AEROSTORE_1G: usize = 1 << 30;
const SHM_BYTES_AEROSTORE_2G: usize = 2 << 30;
const SHM_BYTES_AEROSTORE_3584M: usize = 3584 << 20;
const DEFAULT_VACUUM_INTERVAL_MS: u64 = 25;
const DEFAULT_INDEX_GC_INTERVAL_MS: u64 = 25;
const MEMORY_SAMPLE_INTERVAL_MS: u64 = 5_000;
const DEFAULT_ALLOC_TELEMETRY_INTERVAL_MS: u64 = 5_000;
const DEFAULT_ALLOC_TELEMETRY_DEPTH_SCAN_LIMIT: usize = 65_536;

const MIX_RATIO_TOLERANCE: f64 = 0.02;
const HOT_RATIO_TOLERANCE: f64 = 0.01;
const REQUIRED_TPS_RATIO: f64 = 2.0;
const REQUIRED_P99_RATIO: f64 = 0.6;

const PROFILES: [CrucibleProfile; 4] = [
    CrucibleProfile {
        label: "profile_512m",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_512M,
    },
    CrucibleProfile {
        label: "profile_1g",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_1G,
    },
    CrucibleProfile {
        label: "profile_2g",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_2G,
    },
    CrucibleProfile {
        label: "profile_3584m",
        aerostore_shm_bytes: SHM_BYTES_AEROSTORE_3584M,
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
    // SAFETY: `out` is aligned, uniquely owned storage for one Histogram. The
    // caller must not publish it until this function has initialized every field.
    unsafe fn initialize_at(out: *mut Self) {
        unsafe {
            std::ptr::addr_of_mut!((*out).samples).write(AtomicU64::new(0));
            let buckets = std::ptr::addr_of_mut!((*out).buckets).cast::<AtomicU64>();
            for idx in 0..HIST_BUCKETS {
                buckets.add(idx).write(AtomicU64::new(0));
            }
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
    operation_failures: AtomicU64,
    index_remove_failures: AtomicU64,
    index_insert_failures: AtomicU64,
    total_latency: Histogram,
    upsert_latency: Histogram,
    scan_latency: Histogram,
    scan_server_exec_latency: Histogram,
}

impl WorkerStats {
    // SAFETY: same initialization contract as Histogram::initialize_at.
    unsafe fn initialize_at(out: *mut Self) {
        unsafe {
            std::ptr::addr_of_mut!((*out).total_ops).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).upsert_ops).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).scan_ops).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).hot_upserts).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).conflicts).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).operation_failures).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).index_remove_failures).write(AtomicU64::new(0));
            std::ptr::addr_of_mut!((*out).index_insert_failures).write(AtomicU64::new(0));
            Histogram::initialize_at(std::ptr::addr_of_mut!((*out).total_latency));
            Histogram::initialize_at(std::ptr::addr_of_mut!((*out).upsert_latency));
            Histogram::initialize_at(std::ptr::addr_of_mut!((*out).scan_latency));
            Histogram::initialize_at(std::ptr::addr_of_mut!((*out).scan_server_exec_latency));
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
    fn allocate(shm: &ShmArena, initial_event_ts: i64) -> Result<RelPtr<Self>, String> {
        // Use the same tracked General-class allocation as ChunkedArena::alloc,
        // but initialize in place: this state is now about 1.85 MiB and must not
        // be constructed or copied as a large stack temporary.
        let offset = shm
            .chunked_arena()
            .alloc_raw(std::mem::size_of::<Self>(), std::mem::align_of::<Self>())
            .map_err(|err| err.to_string())?;
        // SAFETY: alloc_raw reserves a unique, aligned region inside this mapped
        // arena. Initialize fields with raw pointers, without forming a reference
        // to the partially initialized state. No workers exist yet; returning
        // the offset is the first publication of this fully initialized value.
        unsafe {
            let out = shm.mmap_base().as_ptr().add(offset as usize).cast::<Self>();
            std::ptr::addr_of_mut!((*out).ready).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*out).go).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*out).stop).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*out)._pad).write([0; 13]);
            std::ptr::addr_of_mut!((*out).global_event_ts).write(AtomicI64::new(initial_event_ts));
            let workers = std::ptr::addr_of_mut!((*out).workers).cast::<WorkerStats>();
            for idx in 0..WORKERS {
                WorkerStats::initialize_at(workers.add(idx));
            }
        }
        Ok(RelPtr::from_offset(offset))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LatencySummary {
    samples: u64,
    p99_lower_ns: u64,
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
    operation_failures: u64,
    index_remove_failures: u64,
    index_insert_failures: u64,
    tps: f64,
    total_latency: LatencySummary,
    upsert_latency: LatencySummary,
    scan_latency: LatencySummary,
    scan_server_exec_latency: Option<LatencySummary>,
    reclaim_telemetry: Option<ReclaimTelemetry>,
    occ_recycle_telemetry: Option<OccRecycleRunTelemetry>,
    index_retry_telemetry: Option<IndexRetryTelemetry>,
    memory_telemetry: Option<MemoryTelemetry>,
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
    epoch_lag_start: u64,
    epoch_lag_end: u64,
    epoch_lag_peak: u64,
    active_slots_start: u32,
    active_slots_end: u32,
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
    gc_nodes_examined: u64,
    gc_nodes_requeued: u64,
    gc_recycle_errors: u64,
    gc_assist_calls: u64,
    gc_assist_reclaimed: u64,
    gc_daemon_cycles: u64,
    gc_daemon_reclaimed: u64,
    pressure_window_failures: u64,
    pressure_window_reclaimed: u64,
    pressure_consecutive_healthy_windows: u32,
    retired_backlog: u64,
    pressure_state: u32,
    pressure_to_normal: u64,
    pressure_to_warm: u64,
    pressure_to_hot: u64,
    alloc_failure_events: u64,
    reserve_node_pushes: u64,
    reserve_node_hits: u64,
    reserve_node_misses: u64,
    reserve_posting_pushes: u64,
    reserve_posting_hits: u64,
    reserve_posting_misses: u64,
    reserve_tower_pushes: u64,
    reserve_tower_hits: u64,
    reserve_tower_misses: u64,
    retry_phase_b_hits: u64,
    retry_phase_c_hits: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct OccRecycleRunTelemetry {
    alloc_from_starved_delta: u64,
    alloc_from_primary_delta: u64,
    alloc_from_probe_delta: u64,
    alloc_fresh_delta: u64,
    pop_empty_delta: u64,
    pop_cas_fail_delta: u64,
    push_success_delta: u64,
    push_cas_fail_delta: u64,
    stash_starved_delta: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct MemoryTelemetry {
    source: &'static str,
    peak_kb: u64,
    end_kb: u64,
    samples: u64,
}

#[derive(Debug)]
struct AllocTelemetryConfig {
    path: PathBuf,
    sample_interval: Duration,
    depth_scan_limit: usize,
}

struct AllocTelemetryRecorder {
    writer: BufWriter<fs::File>,
    sample_interval: Duration,
    depth_scan_limit: usize,
    next_sample_at: Instant,
    head_peak: u32,
}

#[derive(Clone, Copy)]
enum QueryKind {
    Upsert,
    Scan,
}

#[derive(Clone, Copy, Debug)]
struct IntervalSample {
    elapsed: Duration,
    total_ops: u64,
    head_offset: u32,
    reclaimed_nodes: u64,
    retired_backlog: u64,
    alloc_failures: u64,
    fresh_by_class: [u64; ARENA_CLASS_COUNT],
    retired_postings: u64,
    reclaimed_postings: u64,
}

impl IntervalSample {
    fn capture(
        now: Instant,
        started: Instant,
        state: &RunState,
        shm: &ShmArena,
        index: &SecondaryIndex<usize>,
    ) -> Self {
        let telemetry = index.mutation_telemetry();
        Self {
            elapsed: now.saturating_duration_since(started),
            total_ops: state
                .workers
                .iter()
                .map(|w| w.total_ops.load(Ordering::Acquire))
                .sum(),
            head_offset: shm.chunked_arena().head_offset(),
            reclaimed_nodes: index.reclaimed_nodes(),
            retired_backlog: telemetry.retired_backlog,
            alloc_failures: telemetry.alloc_failure_events,
            fresh_by_class: shm.fresh_allocation_bytes(),
            retired_postings: telemetry.retired_postings,
            reclaimed_postings: telemetry.reclaimed_postings,
        }
    }

    fn tps_since(&self, previous: &Self) -> f64 {
        (self.total_ops - previous.total_ops) as f64
            / self
                .elapsed
                .saturating_sub(previous.elapsed)
                .as_secs_f64()
                .max(f64::EPSILON)
    }

    fn print(&self, profile: &str, previous: &Self) {
        println!("hyperfeed_crucible_interval: profile={} elapsed_secs={:.3} interval_secs={:.3} interval_tps={:.2} total_ops={} arena_head_bytes={} fresh_bytes={} reclaimed_nodes={} retired_backlog={} alloc_failure_events={} retired_postings={} reclaimed_postings={}",
            profile, self.elapsed.as_secs_f64(), self.elapsed.saturating_sub(previous.elapsed).as_secs_f64(),
            self.tps_since(previous), self.total_ops, self.head_offset,
            self.head_offset.saturating_sub(previous.head_offset),
            self.reclaimed_nodes.saturating_sub(previous.reclaimed_nodes),
            self.retired_backlog, self.alloc_failures, self.retired_postings,
            self.reclaimed_postings.saturating_sub(previous.reclaimed_postings));
        let fresh: [u64; ARENA_CLASS_COUNT] = std::array::from_fn(|i| {
            self.fresh_by_class[i].saturating_sub(previous.fresh_by_class[i])
        });
        println!("hyperfeed_crucible_fresh: profile={} elapsed_secs={:.3} general_bytes={} row_version_bytes={} skip_node_bytes={} skip_posting_bytes={} skip_tower_bytes={} spill_bytes={}",
            profile, self.elapsed.as_secs_f64(), fresh[0], fresh[1], fresh[2], fresh[3], fresh[4], fresh[5..].iter().sum::<u64>());
    }
}

fn validate_table_index(
    table: &OccTable<CrucibleRow>,
    index: &SecondaryIndex<usize>,
    profile: &str,
) -> Result<(), String> {
    let rows = table
        .snapshot_latest_rows()
        .map_err(|err| format!("final table snapshot: {err}"))?;
    if rows.len() != TOTAL_KEYS {
        return Err(format!(
            "final table row count {}, expected {TOTAL_KEYS}",
            rows.len()
        ));
    }
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|(row_id, row)| (IndexValue::I64(row.event_ts), row_id))
        .collect();
    expected.sort_unstable();
    let actual = index
        .try_entries()
        .map_err(|err| format!("final index traversal: {err}"))?;
    // Compare the raw traversal: sorting/deduplicating it would conceal corruption.
    if actual != expected {
        let mismatch = actual
            .iter()
            .zip(&expected)
            .position(|(a, e)| a != e)
            .unwrap_or(actual.len().min(expected.len()));
        return Err(format!("table/index mismatch: expected={} actual={} first_mismatch={} expected_entry={:?} actual_entry={:?}",
            expected.len(), actual.len(), mismatch, expected.get(mismatch), actual.get(mismatch)));
    }
    if index.distinct_key_count() != TOTAL_KEYS {
        return Err(format!(
            "index key accounting mismatch: reported={} reachable={TOTAL_KEYS}",
            index.distinct_key_count()
        ));
    }
    println!("hyperfeed_crucible_correctness: profile={profile} table_rows={} index_postings={} exact_match=true", expected.len(), actual.len());
    Ok(())
}

fn validate_intervals(
    samples: &[IntervalSample],
    duration: Duration,
    profile: CrucibleProfile,
) -> Result<(), String> {
    let first = samples.first().expect("initial sample");
    let last = samples.last().expect("final sample");
    // Short smoke runs verify correctness, but cannot establish a steady state.
    if duration < Duration::from_secs(30) {
        println!(
            "hyperfeed_crucible_stability: profile={} status=short_run arena_head_bytes={}",
            profile.label, last.head_offset
        );
        return Ok(());
    }
    if samples.len() < 7 {
        return Err(format!("insufficient interval samples ({}) for a sustained run; reduce AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS", samples.len()));
    }
    let midpoint = samples.iter().find(|s| s.elapsed >= duration / 2).unwrap();
    let tail_growth = last.head_offset.saturating_sub(midpoint.head_offset);
    // Fixed-cardinality churn should reuse storage. Permit one additional seeded
    // working set in the second half; this bound does not grow with arena size or
    // host throughput, so simply raising the memory cap cannot hide a leak.
    let growth_budget = first.head_offset;
    let tps: Vec<f64> = samples
        .windows(2)
        .filter(|w| w[1].elapsed - w[0].elapsed >= interval_sample_period() * 4 / 5)
        .map(|w| w[1].tps_since(&w[0]))
        .collect();
    let split = tps.len() / 2;
    let mean = |values: &[f64]| values.iter().sum::<f64>() / values.len().max(1) as f64;
    let early_tps = mean(&tps[..split]);
    let late_tps = mean(&tps[split..]);
    let retained_tps = late_tps / early_tps.max(f64::EPSILON);
    println!("hyperfeed_crucible_stability: profile={} tail_fresh_bytes={} fresh_growth_budget_bytes={} early_interval_tps={:.2} late_interval_tps={:.2} retained_tps={:.4} arena_head_bytes={}",
        profile.label, tail_growth, growth_budget, early_tps, late_tps, retained_tps, last.head_offset);
    if tps.is_empty() || tps.iter().any(|value| *value == 0.0) || retained_tps < 0.5 {
        return Err(format!(
            "sustained throughput cliff: retained_tps={retained_tps:.4}, intervals={tps:?}"
        ));
    }
    if tail_growth > growth_budget {
        return Err(format!("unbounded arena growth: second half allocated {tail_growth} bytes, seeded footprint budget {growth_budget}"));
    }
    Ok(())
}

fn print_aerostore_diagnostic(result: &EngineRunResult, profile: CrucibleProfile) {
    print_total_latency_bounds(result, profile);
    println!("hyperfeed_crucible_config: profile={} mode=aerostore_only aerostore_shm_bytes={} workers={}",
        profile.label, profile.aerostore_shm_bytes, WORKERS);
    println!("| Engine | TPS | Total Ops | p50 upper (us) | p90 upper (us) | p99 upper (us) | Upserts | Scans | Hot Upserts | Conflicts | Index Remove Fail | Index Insert Fail |");
    println!(
        "| aerostore | {:.2} | {} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} |",
        result.tps,
        result.total_ops,
        ns_to_us(result.total_latency.p50_ns),
        ns_to_us(result.total_latency.p90_ns),
        ns_to_us(result.total_latency.p99_ns),
        result.upsert_ops,
        result.scan_ops,
        result.hot_upserts,
        result.conflicts,
        result.index_remove_failures,
        result.index_insert_failures
    );
    println!("hyperfeed_crucible_engine_timing: profile={} aerostore_elapsed_secs={:.3} operation_failures={}",
        profile.label, result.elapsed.as_secs_f64(), result.operation_failures);
    if let Some(reclaim) = result.reclaim_telemetry {
        println!("hyperfeed_crucible_reclaim: profile={} vacuum_reclaimed_rows={} index_retired_nodes={} index_reclaimed_nodes={} free_list_pushes={} free_list_pops={}",
            profile.label, reclaim.vacuum_reclaimed_rows, reclaim.index_retired_nodes_delta,
            reclaim.index_reclaimed_nodes_delta, reclaim.free_list_pushes_delta, reclaim.free_list_pops_delta);
    }
    if let Some(recycle) = result.occ_recycle_telemetry {
        println!(
            "hyperfeed_crucible_occ: profile={} alloc_fresh={} reused={}",
            profile.label,
            recycle.alloc_fresh_delta,
            recycle.alloc_from_starved_delta
                + recycle.alloc_from_primary_delta
                + recycle.alloc_from_probe_delta
        );
    }
}

fn bench_hyperfeed_crucible(c: &mut Criterion) {
    // Parse once in the parent. An explicitly invalid seed must fail before
    // allocation, Docker setup, or any worker/daemon fork.
    let fixed_seed = parse_seed(std::env::var_os(SEED_ENV).as_deref())
        .unwrap_or_else(|error| panic!("{SEED_ENV}: {error}"));
    match fixed_seed {
        Some(seed) => println!(
            "hyperfeed_crucible_seed: mode=fixed seed={seed} algorithm={FIXED_SEED_ALGORITHM}"
        ),
        None => println!(
            "hyperfeed_crucible_seed: mode=entropy seed=none algorithm=pid_time_xorshift64_v1"
        ),
    }
    let duration = crucible_duration();
    // This mode runs the identical Aerostore workload and correctness gates without
    // requiring Docker. Comparison runs below retain all PostgreSQL performance gates.
    if aerostore_only() {
        for profile in selected_profiles() {
            let result = run_aerostore_crucible(duration, profile, fixed_seed).unwrap_or_else(|err| {
                panic!("aerostore diagnostic failed ({}): {err}", profile.label)
            });
            assert_workload_mix(&result, profile.label, "aerostore");
            print_aerostore_diagnostic(&result, profile);
        }
        return;
    }
    let results = run_crucible(duration, fixed_seed);

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
            / (result.postgres.total_latency.p99_lower_ns.max(1) as f64);
        let tps_label = format!("{}_aerostore_vs_postgres_tps_ratio", result.profile.label);
        let p99_label = format!("{}_aerostore_vs_postgres_p99_ratio_upper", result.profile.label);

        group.bench_function(tps_label, |b| b.iter(|| black_box(tps_ratio)));
        group.bench_function(p99_label, |b| b.iter(|| black_box(p99_ratio)));
    }
    group.finish();
}

fn run_crucible(duration: Duration, fixed_seed: Option<u64>) -> Vec<ProfileRunResult> {
    let profiles = selected_profiles();
    let mut out = Vec::with_capacity(profiles.len());
    for profile in profiles {
        let aerostore = run_aerostore_crucible(duration, profile, fixed_seed).unwrap_or_else(|err| {
            panic!("aerostore crucible run failed ({}): {err}", profile.label)
        });
        let postgres = run_postgres_crucible(duration, profile, fixed_seed).unwrap_or_else(|err| {
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

fn aerostore_only() -> bool {
    std::env::var("AEROSTORE_CRUCIBLE_AEROSTORE_ONLY").as_deref() == Ok("1")
}

fn selected_profiles() -> Vec<CrucibleProfile> {
    if let Ok(raw) = std::env::var("AEROSTORE_CRUCIBLE_SHM_MIB") {
        assert!(
            aerostore_only(),
            "arena overrides require AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1"
        );
        let mib = raw
            .parse::<usize>()
            .expect("AEROSTORE_CRUCIBLE_SHM_MIB must be an integer");
        assert!(
            (32..=3584).contains(&mib),
            "diagnostic arena must be 32..=3584 MiB"
        );
        return vec![CrucibleProfile {
            label: "diagnostic",
            aerostore_shm_bytes: mib << 20,
        }];
    }
    let Some(raw) = std::env::var("AEROSTORE_CRUCIBLE_PROFILE_FILTER").ok() else {
        return PROFILES.to_vec();
    };
    let wanted: Vec<String> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if wanted.is_empty() {
        return PROFILES.to_vec();
    }

    let mut selected = Vec::new();
    for profile in PROFILES {
        if wanted.iter().any(|w| w == profile.label) {
            selected.push(profile);
        }
    }

    assert!(
        !selected.is_empty(),
        "AEROSTORE_CRUCIBLE_PROFILE_FILTER={raw} selected no known profiles"
    );
    selected
}

impl AllocTelemetryRecorder {
    fn start(
        config: AllocTelemetryConfig,
        started: Instant,
        shm: &ShmArena,
    ) -> Result<Self, String> {
        if let Some(parent) = config.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| {
                    format!(
                        "failed to create alloc telemetry directory {}: {}",
                        parent.display(),
                        err
                    )
                })?;
            }
        }
        let file = fs::File::create(&config.path).map_err(|err| {
            format!(
                "failed to create alloc telemetry file {}: {}",
                config.path.display(),
                err
            )
        })?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "elapsed_ms,head_offset,head_peak,free_list_head_offset,free_list_depth_est,free_list_depth_truncated,free_list_pushes,free_list_pops,free_list_net,free_list_pop_misses,retry_alloc,retry_loops,retry_structural,max_insert_attempts,max_remove_attempts,gc_nodes_examined,gc_nodes_requeued,gc_recycle_errors,gc_assist_calls,gc_assist_reclaimed,retired_backlog,pressure_state,pressure_to_normal,pressure_to_warm,pressure_to_hot,alloc_failure_events,reserve_node_pushes,reserve_node_hits,reserve_node_misses,reserve_posting_pushes,reserve_posting_hits,reserve_posting_misses,reserve_tower_pushes,reserve_tower_hits,reserve_tower_misses,fresh_general_bytes,fresh_row_version_bytes,fresh_skip_node_bytes,fresh_skip_posting_bytes,fresh_skip_tower_bytes,fresh_spill_bytes,retired_postings,reclaimed_postings"
        )
        .map_err(|err| {
            format!(
                "failed to write alloc telemetry header {}: {}",
                config.path.display(),
                err
            )
        })?;

        Ok(Self {
            writer,
            sample_interval: config.sample_interval,
            depth_scan_limit: config.depth_scan_limit,
            next_sample_at: started,
            head_peak: shm.chunked_arena().head_offset(),
        })
    }

    fn maybe_sample(
        &mut self,
        now: Instant,
        started: Instant,
        shm: &ShmArena,
        time_index: &SecondaryIndex<usize>,
    ) -> Result<(), String> {
        if now < self.next_sample_at {
            return Ok(());
        }
        self.sample(now, started, shm, time_index)?;
        self.next_sample_at = now + self.sample_interval;
        Ok(())
    }

    fn finalize(
        mut self,
        now: Instant,
        started: Instant,
        shm: &ShmArena,
        time_index: &SecondaryIndex<usize>,
    ) -> Result<(), String> {
        self.sample(now, started, shm, time_index)?;
        self.writer
            .flush()
            .map_err(|err| format!("failed to flush alloc telemetry writer: {}", err))
    }

    fn sample(
        &mut self,
        now: Instant,
        started: Instant,
        shm: &ShmArena,
        time_index: &SecondaryIndex<usize>,
    ) -> Result<(), String> {
        let head_offset = shm.chunked_arena().head_offset();
        self.head_peak = self.head_peak.max(head_offset);
        let free_list_head_offset = shm.free_list_head_offset();
        let (free_list_depth_est, depth_truncated) =
            shm.free_list_depth_estimate(self.depth_scan_limit);
        let free_list_pushes = shm.free_list_pushes();
        let free_list_pops = shm.free_list_pops();
        let free_list_net = free_list_pushes.saturating_sub(free_list_pops);
        let free_list_pop_misses = shm.free_list_pop_misses();
        let retry = time_index.mutation_telemetry();
        let fresh = shm.fresh_allocation_bytes();
        let elapsed_ms = now.saturating_duration_since(started).as_millis();

        writeln!(
            self.writer,
            "{elapsed_ms},{head_offset},{head_peak},{free_list_head_offset},{free_list_depth_est},{depth_truncated},{free_list_pushes},{free_list_pops},{free_list_net},{free_list_pop_misses},{retry_alloc},{retry_loops},{retry_structural},{max_insert_attempts},{max_remove_attempts},{gc_nodes_examined},{gc_nodes_requeued},{gc_recycle_errors},{gc_assist_calls},{gc_assist_reclaimed},{retired_backlog},{pressure_state},{pressure_to_normal},{pressure_to_warm},{pressure_to_hot},{alloc_failure_events},{reserve_node_pushes},{reserve_node_hits},{reserve_node_misses},{reserve_posting_pushes},{reserve_posting_hits},{reserve_posting_misses},{reserve_tower_pushes},{reserve_tower_hits},{reserve_tower_misses},{fresh_general_bytes},{fresh_row_version_bytes},{fresh_skip_node_bytes},{fresh_skip_posting_bytes},{fresh_skip_tower_bytes},{fresh_spill_bytes},{retired_postings},{reclaimed_postings}",
            head_peak = self.head_peak,
            retry_alloc = retry.retry_alloc,
            retry_loops = retry.retry_loops,
            retry_structural = retry.retry_structural,
            max_insert_attempts = retry.max_insert_attempts,
            max_remove_attempts = retry.max_remove_attempts,
            gc_nodes_examined = retry.gc_nodes_examined,
            gc_nodes_requeued = retry.gc_nodes_requeued,
            gc_recycle_errors = retry.gc_recycle_errors,
            gc_assist_calls = retry.gc_assist_calls,
            gc_assist_reclaimed = retry.gc_assist_reclaimed,
            retired_backlog = retry.retired_backlog,
            pressure_state = retry.pressure_state,
            pressure_to_normal = retry.pressure_to_normal,
            pressure_to_warm = retry.pressure_to_warm,
            pressure_to_hot = retry.pressure_to_hot,
            alloc_failure_events = retry.alloc_failure_events,
            reserve_node_pushes = retry.reserve_node_pushes,
            reserve_node_hits = retry.reserve_node_hits,
            reserve_node_misses = retry.reserve_node_misses,
            reserve_posting_pushes = retry.reserve_posting_pushes,
            reserve_posting_hits = retry.reserve_posting_hits,
            reserve_posting_misses = retry.reserve_posting_misses,
            reserve_tower_pushes = retry.reserve_tower_pushes,
            reserve_tower_hits = retry.reserve_tower_hits,
            reserve_tower_misses = retry.reserve_tower_misses,
            fresh_general_bytes = fresh[0],
            fresh_row_version_bytes = fresh[1],
            fresh_skip_node_bytes = fresh[2],
            fresh_skip_posting_bytes = fresh[3],
            fresh_skip_tower_bytes = fresh[4],
            fresh_spill_bytes = fresh[5..].iter().sum::<u64>(),
            retired_postings = retry.retired_postings,
            reclaimed_postings = retry.reclaimed_postings,
        )
        .map_err(|err| format!("failed to write alloc telemetry sample: {}", err))
    }
}

fn alloc_telemetry_config(profile_label: &str) -> Option<AllocTelemetryConfig> {
    let raw_path = std::env::var("AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH").ok()?;
    let path_text = if raw_path.contains("{profile}") {
        raw_path.replace("{profile}", profile_label)
    } else {
        raw_path
    };

    let sample_interval = Duration::from_millis(
        std::env::var("AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_ALLOC_TELEMETRY_INTERVAL_MS),
    );
    let depth_scan_limit = std::env::var("AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_DEPTH_SCAN_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ALLOC_TELEMETRY_DEPTH_SCAN_LIMIT);

    Some(AllocTelemetryConfig {
        path: PathBuf::from(path_text),
        sample_interval,
        depth_scan_limit,
    })
}

fn run_aerostore_crucible(
    duration: Duration,
    profile: CrucibleProfile,
    fixed_seed: Option<u64>,
) -> Result<EngineRunResult, String> {
    let alloc_telemetry_cfg = alloc_telemetry_config(profile.label);
    let shm = Arc::new(ShmArena::new(profile.aerostore_shm_bytes).map_err(|err| {
        format!(
            "failed to create Aerostore arena for {}: {}",
            profile.label, err
        )
    })?);
    let mut table = OccTable::<CrucibleRow>::new(Arc::clone(&shm), TOTAL_KEYS)
        .map_err(|err| err.to_string())?;
    let time_index = SecondaryIndex::<usize>::new_in_shared("event_ts", Arc::clone(&shm));

    for row_id in 0..TOTAL_KEYS {
        let row = CrucibleRow::seeded(row_id);
        table.seed_row(row_id, row).map_err(|err| err.to_string())?;
        time_index
            .try_insert(IndexValue::I64(row.event_ts), row_id)
            .map_err(|err| format!("failed to seed index row {row_id}: {err}"))?;
    }

    table
        .bind_index(time_index.clone(), |row| {
            Some(IndexValue::I64(row.event_ts))
        })
        .map_err(|err| err.to_string())?;
    let table = Arc::new(table);

    let state_ptr = RunState::allocate(shm.as_ref(), TOTAL_KEYS as i64)?;
    let state_offset = state_ptr.load(Ordering::Acquire);

    // Open optional output before launching children, so a path error cannot
    // leave workers running after the parent returns.
    let mut alloc_telemetry = if let Some(cfg) = alloc_telemetry_cfg {
        Some(
            AllocTelemetryRecorder::start(cfg, Instant::now(), shm.as_ref())
                .map_err(|err| format!("failed to start alloc telemetry: {}", err))?,
        )
    } else {
        None
    };

    let reclaim_start = ReclaimSnapshot {
        index_retired_nodes: time_index.retired_nodes(),
        index_reclaimed_nodes: time_index.reclaimed_nodes(),
        free_list_pushes: shm.free_list_pushes(),
        free_list_pops: shm.free_list_pops(),
        epoch: read_epoch_lag_snapshot(shm.as_ref()),
    };
    let recycle_start = table
        .recycle_telemetry()
        .map_err(|err| format!("failed to read recycle telemetry start: {}", err))?;
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
            let _ = stop_background_daemons(&vacuum_daemon, &index_gc_daemon);
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
                fixed_seed,
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
        let _ = stop_background_daemons(&vacuum_daemon, &index_gc_daemon);
        remove_if_exists(&wal_path);
        return Err(err);
    }

    let tracked_pid = std::process::id() as libc::pid_t;
    let mut mem_peak_kb = 0_u64;
    let mut mem_end_kb = 0_u64;
    let mut mem_samples = 0_u64;
    if let Some(sample_kb) = read_process_pss_kb(tracked_pid) {
        mem_peak_kb = sample_kb;
        mem_end_kb = sample_kb;
        mem_samples = 1;
    }

    let started = Instant::now();
    state.go.store(1, Ordering::Release);
    let mut alloc_telemetry_error: Option<String> = None;
    if let Some(recorder) = alloc_telemetry.as_mut() {
        if let Err(err) = recorder.maybe_sample(started, started, shm.as_ref(), &time_index) {
            alloc_telemetry_error = Some(err);
            alloc_telemetry = None;
        }
    }

    let mut intervals = vec![IntervalSample::capture(
        started,
        started,
        state,
        shm.as_ref(),
        &time_index,
    )];
    let mut next_interval = started + interval_sample_period();
    let deadline = started + duration;
    let mut next_mem_sample = started;
    let mut epoch_lag_peak = reclaim_start.epoch.lag();
    while Instant::now() < deadline && state.stop.load(Ordering::Acquire) == 0 {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        let sleep_for = remaining.min(Duration::from_millis(25));
        std::thread::sleep(sleep_for);
        let snapshot = read_epoch_lag_snapshot(shm.as_ref());
        epoch_lag_peak = epoch_lag_peak.max(snapshot.lag());
        if now >= next_mem_sample {
            if let Some(sample_kb) = read_process_pss_kb(tracked_pid) {
                mem_peak_kb = mem_peak_kb.max(sample_kb);
                mem_end_kb = sample_kb;
                mem_samples = mem_samples.saturating_add(1);
            }
            next_mem_sample = now + Duration::from_millis(MEMORY_SAMPLE_INTERVAL_MS);
        }
        if now >= next_interval {
            let sample = IntervalSample::capture(now, started, state, shm.as_ref(), &time_index);
            sample.print(profile.label, intervals.last().unwrap());
            intervals.push(sample);
            next_interval = now + interval_sample_period();
        }
        if let Some(recorder) = alloc_telemetry.as_mut() {
            if let Err(err) = recorder.maybe_sample(now, started, shm.as_ref(), &time_index) {
                alloc_telemetry_error = Some(err);
                alloc_telemetry = None;
            }
        }
    }
    state.stop.store(1, Ordering::Release);

    let workers_result = wait_for_children_or_terminate(&pids, Duration::from_secs(10));
    let elapsed = started.elapsed();
    if let Err(err) = &workers_result {
        // A killed/crashed worker may own a shared lock. Do not enter that arena
        // again or join the vacuum thread: either can block forever. Reap the
        // remaining child daemons and exit this disposable benchmark process.
        eprintln!("fatal Aerostore worker failure: {err}");
        let _ = index_gc_daemon.terminate(libc::SIGKILL);
        let _ = wal_daemon.terminate(libc::SIGKILL);
        let _ = index_gc_daemon.join();
        let _ = wal_daemon.join_any_status();
        remove_if_exists(&wal_path);
        std::process::exit(1);
    }

    let ring_result = ring
        .close()
        .map_err(|err| format!("failed to close Aerostore ring: {err}"));
    let wal_result = wal_daemon
        .join()
        .map_err(|err| format!("Aerostore WAL daemon failed to exit cleanly: {err}"));
    let daemon_result = stop_background_daemons(&vacuum_daemon, &index_gc_daemon);
    let final_sample =
        IntervalSample::capture(Instant::now(), started, state, shm.as_ref(), &time_index);
    final_sample.print(profile.label, intervals.last().unwrap());
    intervals.push(final_sample);
    if let Some(recorder) = alloc_telemetry {
        if let Err(err) = recorder.finalize(Instant::now(), started, shm.as_ref(), &time_index) {
            alloc_telemetry_error = Some(err);
        }
    }
    if let Some(err) = alloc_telemetry_error {
        remove_if_exists(&wal_path);
        return Err(format!(
            "alloc telemetry failed ({}): {}",
            profile.label, err
        ));
    }

    let reclaim_end = ReclaimSnapshot {
        index_retired_nodes: time_index.retired_nodes(),
        index_reclaimed_nodes: time_index.reclaimed_nodes(),
        free_list_pushes: shm.free_list_pushes(),
        free_list_pops: shm.free_list_pops(),
        epoch: read_epoch_lag_snapshot(shm.as_ref()),
    };
    let reclaim_telemetry = ReclaimTelemetry {
        vacuum_reclaimed_rows: vacuum_reclaimed_rows.load(Ordering::Acquire),
        // retired_nodes() is a queue depth, not a cumulative retirement count.
        index_retired_nodes_delta: reclaim_end
            .index_retired_nodes
            .saturating_add(reclaim_end.index_reclaimed_nodes)
            .saturating_sub(
                reclaim_start
                    .index_retired_nodes
                    .saturating_add(reclaim_start.index_reclaimed_nodes),
            ),
        index_reclaimed_nodes_delta: reclaim_end
            .index_reclaimed_nodes
            .saturating_sub(reclaim_start.index_reclaimed_nodes),
        free_list_pushes_delta: reclaim_end
            .free_list_pushes
            .saturating_sub(reclaim_start.free_list_pushes),
        free_list_pops_delta: reclaim_end
            .free_list_pops
            .saturating_sub(reclaim_start.free_list_pops),
        epoch_lag_start: reclaim_start.epoch.lag(),
        epoch_lag_end: reclaim_end.epoch.lag(),
        epoch_lag_peak,
        active_slots_start: reclaim_start.epoch.active_slots,
        active_slots_end: reclaim_end.epoch.active_slots,
    };
    let recycle_end = table
        .recycle_telemetry()
        .map_err(|err| format!("failed to read recycle telemetry end: {}", err))?;
    let occ_recycle_telemetry = fold_recycle_telemetry(recycle_start, recycle_end);
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
        gc_nodes_examined: index_retry.gc_nodes_examined,
        gc_nodes_requeued: index_retry.gc_nodes_requeued,
        gc_recycle_errors: index_retry.gc_recycle_errors,
        gc_assist_calls: index_retry.gc_assist_calls,
        gc_assist_reclaimed: index_retry.gc_assist_reclaimed,
        gc_daemon_cycles: index_retry.gc_daemon_cycles,
        gc_daemon_reclaimed: index_retry.gc_daemon_reclaimed,
        pressure_window_failures: index_retry.pressure_window_failures,
        pressure_window_reclaimed: index_retry.pressure_window_reclaimed,
        pressure_consecutive_healthy_windows: index_retry.pressure_consecutive_healthy_windows,
        retired_backlog: index_retry.retired_backlog,
        pressure_state: index_retry.pressure_state,
        pressure_to_normal: index_retry.pressure_to_normal,
        pressure_to_warm: index_retry.pressure_to_warm,
        pressure_to_hot: index_retry.pressure_to_hot,
        alloc_failure_events: index_retry.alloc_failure_events,
        reserve_node_pushes: index_retry.reserve_node_pushes,
        reserve_node_hits: index_retry.reserve_node_hits,
        reserve_node_misses: index_retry.reserve_node_misses,
        reserve_posting_pushes: index_retry.reserve_posting_pushes,
        reserve_posting_hits: index_retry.reserve_posting_hits,
        reserve_posting_misses: index_retry.reserve_posting_misses,
        reserve_tower_pushes: index_retry.reserve_tower_pushes,
        reserve_tower_hits: index_retry.reserve_tower_hits,
        reserve_tower_misses: index_retry.reserve_tower_misses,
        retry_phase_b_hits: index_retry.retry_phase_b_hits,
        retry_phase_c_hits: index_retry.retry_phase_c_hits,
    };
    let memory_telemetry = if mem_samples > 0 {
        Some(MemoryTelemetry {
            source: "proc_pss_kb(parent)",
            peak_kb: mem_peak_kb,
            end_kb: mem_end_kb,
            samples: mem_samples,
        })
    } else {
        None
    };

    remove_if_exists(&wal_path);
    workers_result?;
    ring_result?;
    wal_result?;
    daemon_result?;
    if reclaim_end.epoch.active_slots != 0 {
        return Err(format!(
            "workers left {} epoch registrations active",
            reclaim_end.epoch.active_slots
        ));
    }
    validate_table_index(table.as_ref(), &time_index, profile.label)?;
    validate_intervals(&intervals, duration, profile)?;
    // With all workers quiescent, reclamation must drain the entire retired queue.
    for _ in 0..8 {
        time_index.collect_garbage_once(usize::MAX);
        let pending = time_index.mutation_telemetry();
        if pending.retired_backlog == 0 && pending.retired_postings == 0 {
            break;
        }
    }
    let drained = time_index.mutation_telemetry();
    if drained.retired_backlog != 0
        || drained.retired_postings != 0
        || drained.gc_recycle_errors != 0
    {
        return Err(format!(
            "index GC failed to drain: nodes={} postings={} recycle_errors={}",
            drained.retired_backlog, drained.retired_postings, drained.gc_recycle_errors
        ));
    }
    println!(
        "hyperfeed_crucible_gc_drain: profile={} retired_backlog={} gc_recycle_errors={} retired_postings={} reclaimed_postings={}",
        profile.label, drained.retired_backlog, drained.gc_recycle_errors, drained.retired_postings, drained.reclaimed_postings
    );

    let audit = time_index
        .audit_allocations()
        .map_err(|err| format!("final index allocation audit: {err}"))?;
    if audit.retired.nodes != 0
        || audit.retired.postings != 0
        || audit.retired.towers != 0
        || audit.retired.tower_lanes != 0
    {
        return Err(format!(
            "allocation audit found retired storage after GC drain: {:?}",
            audit.retired
        ));
    }
    if audit.reachable.nodes != (TOTAL_KEYS + 1) as u64
        || audit.reachable.postings != TOTAL_KEYS as u64
    {
        return Err(format!(
            "allocation audit disagrees with fixed table cardinality (including sentinel): {:?}",
            audit.reachable
        ));
    }
    for (class, allocated, reachable, retired, reusable) in [
        (
            "nodes",
            audit.allocated.nodes,
            audit.reachable.nodes,
            audit.retired.nodes,
            audit.reusable.nodes,
        ),
        (
            "postings",
            audit.allocated.postings,
            audit.reachable.postings,
            audit.retired.postings,
            audit.reusable.postings,
        ),
        (
            "towers",
            audit.allocated.towers,
            audit.reachable.towers,
            audit.retired.towers,
            audit.reusable.towers,
        ),
        (
            "tower_lanes",
            audit.allocated.tower_lanes,
            audit.reachable.tower_lanes,
            audit.retired.tower_lanes,
            audit.reusable.tower_lanes,
        ),
    ] {
        println!("hyperfeed_crucible_allocation_class: profile={} class={} allocated={} reachable={} retired={} reusable={}",
            profile.label, class, allocated, reachable, retired, reusable);
    }
    println!(
        "hyperfeed_crucible_allocation_audit: profile={} status=pass",
        profile.label
    );

    let result = aggregate_result(
        "aerostore",
        state,
        elapsed,
        false,
        Some(reclaim_telemetry),
        Some(occ_recycle_telemetry),
        Some(index_retry_telemetry),
        memory_telemetry,
    );
    println!("hyperfeed_crucible_retry: profile={} max_insert_attempts={} pressure_state={} retry_alloc={} gc_recycle_errors={}",
        profile.label, index_retry_telemetry.max_insert_attempts, index_retry_telemetry.pressure_state,
        index_retry_telemetry.retry_alloc, index_retry_telemetry.gc_recycle_errors);
    if result.index_insert_failures != 0
        || result.index_remove_failures != 0
        || result.operation_failures != 0
    {
        return Err(format!(
            "workload failures: operations={} insert={} remove={}",
            result.operation_failures, result.index_insert_failures, result.index_remove_failures
        ));
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, Default)]
struct ReclaimSnapshot {
    index_retired_nodes: u64,
    index_reclaimed_nodes: u64,
    free_list_pushes: u64,
    free_list_pops: u64,
    epoch: EpochLagSnapshot,
}

#[derive(Clone, Copy, Debug, Default)]
struct EpochLagSnapshot {
    global_txid: u64,
    global_xmin: u64,
    active_slots: u32,
}

impl EpochLagSnapshot {
    #[inline]
    fn lag(self) -> u64 {
        self.global_txid.saturating_sub(self.global_xmin)
    }
}

fn fold_recycle_telemetry(
    start: OccRecycleTelemetry,
    end: OccRecycleTelemetry,
) -> OccRecycleRunTelemetry {
    OccRecycleRunTelemetry {
        alloc_from_starved_delta: end
            .alloc_from_starved
            .saturating_sub(start.alloc_from_starved),
        alloc_from_primary_delta: end
            .alloc_from_primary
            .saturating_sub(start.alloc_from_primary),
        alloc_from_probe_delta: end.alloc_from_probe.saturating_sub(start.alloc_from_probe),
        alloc_fresh_delta: end.alloc_fresh.saturating_sub(start.alloc_fresh),
        pop_empty_delta: end.pop_empty.saturating_sub(start.pop_empty),
        pop_cas_fail_delta: end.pop_cas_fail.saturating_sub(start.pop_cas_fail),
        push_success_delta: end.push_success.saturating_sub(start.push_success),
        push_cas_fail_delta: end.push_cas_fail.saturating_sub(start.push_cas_fail),
        stash_starved_delta: end.stash_starved.saturating_sub(start.stash_starved),
    }
}

fn run_aerostore_worker(
    worker_idx: usize,
    state_offset: u32,
    shm: &ShmArena,
    table: &OccTable<CrucibleRow>,
    time_index: &SecondaryIndex<usize>,
    ring: SharedWalRing<RING_SLOTS, RING_SLOT_BYTES>,
    fixed_seed: Option<u64>,
) -> ! {
    let Some(state) = RelPtr::<RunState>::from_offset(state_offset).as_ref(shm.mmap_base()) else {
        unsafe { libc::_exit(71) };
    };

    state.ready.fetch_add(1, Ordering::AcqRel);
    while state.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let mut rng = seed_rng(worker_idx as u64, 0xA3E0_52D1_9911_AA11, fixed_seed);
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

                // Bound hot-row contention before taking the transaction snapshot.
                // Native commit itself now publishes the row and index together.
                let _indexed_update = match table.lock_indexed_rows(&[row_id]) {
                    Ok(guard) => guard,
                    Err(err) => {
                        eprintln!("worker {worker_idx}: indexed row lock failed: {err}");
                        stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                        state.stop.store(1, Ordering::Release);
                        break 'worker;
                    }
                };
                let mut attempts = 0_u32;
                loop {
                    if state.stop.load(Ordering::Acquire) != 0 {
                        break 'worker;
                    }
                    let mut tx = match table.begin_transaction() {
                        Ok(tx) => tx,
                        Err(err) => {
                            eprintln!("worker {worker_idx}: begin transaction failed: {err}");
                            stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                            state.stop.store(1, Ordering::Release);
                            break 'worker;
                        }
                    };

                    let current = match table.read(&mut tx, row_id) {
                        Ok(Some(row)) => row,
                        Ok(None) => {
                            let _ = table.abort(&mut tx);
                            eprintln!("worker {worker_idx}: seeded row {row_id} disappeared");
                            stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                            state.stop.store(1, Ordering::Release);
                            break 'worker;
                        }
                        Err(OccError::SerializationFailure) => {
                            let _ = table.abort(&mut tx);
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            continue;
                        }
                        Err(err) => {
                            let _ = table.abort(&mut tx);
                            eprintln!("worker {worker_idx}: read row {row_id} failed: {err}");
                            stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                            state.stop.store(1, Ordering::Release);
                            break 'worker;
                        }
                    };

                    let new_ts = state.global_event_ts.fetch_add(1, Ordering::AcqRel) + 1;
                    let mut next = current;
                    next.altitude = next.altitude.wrapping_add(1);
                    next.event_ts = new_ts;
                    next.payload[0] = next.payload[0].wrapping_add(1);

                    if let Err(err) = table.write(&mut tx, row_id, next) {
                        let _ = table.abort(&mut tx);
                        if err != OccError::SerializationFailure {
                            eprintln!("worker {worker_idx}: write row {row_id} failed: {err}");
                            stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                            state.stop.store(1, Ordering::Release);
                            break 'worker;
                        }
                        stats.conflicts.fetch_add(1, Ordering::AcqRel);
                        attempts = attempts.saturating_add(1);
                        retry.sleep_for_attempt(attempts.saturating_sub(1));
                        continue;
                    }

                    match committer.commit(table, &mut tx) {
                        Ok(_) => break,
                        Err(WalWriterError::Occ(OccError::SerializationFailure)) => {
                            stats.conflicts.fetch_add(1, Ordering::AcqRel);
                            attempts = attempts.saturating_add(1);
                            retry.sleep_for_attempt(attempts.saturating_sub(1));
                            if state.stop.load(Ordering::Acquire) != 0 {
                                break 'worker;
                            }
                        }
                        Err(err) => {
                            // Native commit already maintains row/index agreement.
                            // A later WAL failure remains fatal and cannot retry.
                            if matches!(&err, WalWriterError::Occ(_)) {
                                let _ = table.abort(&mut tx);
                            }
                            eprintln!("worker {worker_idx}: commit row {row_id} failed: {err}");
                            stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                            state.stop.store(1, Ordering::Release);
                            break 'worker;
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
                // This original storage-churn probe intentionally counts raw
                // postings. Transactional range semantics are checked by the
                // Extended Crucible and native index regression suite.
                let hits = match time_index.try_lookup_count_with_limit(
                    &IndexCompare::Gt(IndexValue::I64(bound)),
                    SCAN_LIMIT as usize,
                ) {
                    Ok(hits) => hits,
                    Err(err) => {
                        eprintln!("worker {worker_idx}: index scan failed: {err}");
                        stats.operation_failures.fetch_add(1, Ordering::AcqRel);
                        state.stop.store(1, Ordering::Release);
                        break 'worker;
                    }
                };
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
    fixed_seed: Option<u64>,
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
    let container_id = container.id().to_string();
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
    let state_ptr = RunState::allocate(metrics_shm.as_ref(), TOTAL_KEYS as i64)?;
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
                fixed_seed,
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

    let mut mem_peak_kb = 0_u64;
    let mut mem_end_kb = 0_u64;
    let mut mem_samples = 0_u64;
    if let Some(sample_kb) = sample_docker_container_mem_kb(container_id.as_str()) {
        mem_peak_kb = sample_kb;
        mem_end_kb = sample_kb;
        mem_samples = 1;
    }

    let started = Instant::now();
    state.go.store(1, Ordering::Release);
    let deadline = started + duration;
    let mut next_mem_sample = started;
    while Instant::now() < deadline {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        if now >= next_mem_sample {
            if let Some(sample_kb) = sample_docker_container_mem_kb(container_id.as_str()) {
                mem_peak_kb = mem_peak_kb.max(sample_kb);
                mem_end_kb = sample_kb;
                mem_samples = mem_samples.saturating_add(1);
            }
            next_mem_sample = now + Duration::from_millis(MEMORY_SAMPLE_INTERVAL_MS);
        }
        let remaining = deadline.saturating_duration_since(now);
        let sleep_for = remaining.min(Duration::from_millis(25));
        std::thread::sleep(sleep_for);
    }
    state.stop.store(1, Ordering::Release);

    let workers_result = wait_for_children_or_terminate(&pids, Duration::from_secs(10));
    let elapsed = started.elapsed();

    workers_result?;

    let memory_telemetry = if mem_samples > 0 {
        Some(MemoryTelemetry {
            source: "docker_stats_mem_usage_kb",
            peak_kb: mem_peak_kb,
            end_kb: mem_end_kb,
            samples: mem_samples,
        })
    } else {
        None
    };

    Ok(aggregate_result(
        "postgres",
        state,
        elapsed,
        true,
        None,
        None,
        None,
        memory_telemetry,
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

fn run_postgres_worker(
    worker_idx: usize,
    state_offset: u32,
    shm: &ShmArena,
    conn_str: &str,
    fixed_seed: Option<u64>,
) -> ! {
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

    let mut rng = seed_rng(worker_idx as u64, 0xCC77_AA22_1958_3321, fixed_seed);
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
    occ_recycle_telemetry: Option<OccRecycleRunTelemetry>,
    index_retry_telemetry: Option<IndexRetryTelemetry>,
    memory_telemetry: Option<MemoryTelemetry>,
) -> EngineRunResult {
    let mut total_ops = 0_u64;
    let mut upsert_ops = 0_u64;
    let mut scan_ops = 0_u64;
    let mut hot_upserts = 0_u64;
    let mut conflicts = 0_u64;
    let mut operation_failures = 0_u64;
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
        operation_failures =
            operation_failures.saturating_add(worker.operation_failures.load(Ordering::Acquire));
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
        operation_failures,
        index_remove_failures,
        index_insert_failures,
        tps: total_ops as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        total_latency,
        upsert_latency,
        scan_latency,
        scan_server_exec_latency,
        reclaim_telemetry,
        occ_recycle_telemetry,
        index_retry_telemetry,
        memory_telemetry,
    }
}

fn print_results(result: &ProfileRunResult, duration: Duration) {
    let aerostore = &result.aerostore;
    let postgres = &result.postgres;
    print_total_latency_bounds(aerostore, result.profile);
    print_total_latency_bounds(postgres, result.profile);

    let tps_ratio = aerostore.tps / postgres.tps.max(f64::EPSILON);
    let total_p99_ratio =
        aerostore.total_latency.p99_ns as f64 / (postgres.total_latency.p99_lower_ns.max(1) as f64);

    let pg_scan_server = postgres.scan_server_exec_latency.unwrap_or_default();

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
        "| Engine | TPS | Total Ops | p50 upper (us) | p90 upper (us) | p99 upper (us) | Upserts | Scans | Hot Upserts | Conflicts | Index Remove Fail | Index Insert Fail |"
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

    if aerostore.memory_telemetry.is_some() || postgres.memory_telemetry.is_some() {
        println!("| Engine Memory Telemetry | Source | Peak (MiB) | End (MiB) | Samples | Notes |");
        println!("|---|---|---:|---:|---:|---|");

        if let Some(mem) = aerostore.memory_telemetry {
            println!(
                "| aerostore | {} | {:.2} | {:.2} | {} | profile_shm_bytes={} ({:.2} MiB) |",
                mem.source,
                kb_to_mib(mem.peak_kb),
                kb_to_mib(mem.end_kb),
                mem.samples,
                result.profile.aerostore_shm_bytes,
                result.profile.aerostore_shm_bytes as f64 / (1024.0 * 1024.0),
            );
        }

        if let Some(mem) = postgres.memory_telemetry {
            println!(
                "| postgres | {} | {:.2} | {:.2} | {} | container_runtime_memory |",
                mem.source,
                kb_to_mib(mem.peak_kb),
                kb_to_mib(mem.end_kb),
                mem.samples,
            );
        }
    }

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
        println!(
            "| Aerostore Epoch Lag Telemetry | epoch_lag_start | epoch_lag_end | epoch_lag_peak | active_slots_start | active_slots_end |"
        );
        println!("|---|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {} | {} | {} | {} |",
            result.profile.label,
            reclaim.epoch_lag_start,
            reclaim.epoch_lag_end,
            reclaim.epoch_lag_peak,
            reclaim.active_slots_start,
            reclaim.active_slots_end,
        );
    }

    if let Some(recycle) = aerostore.occ_recycle_telemetry {
        println!(
            "| Aerostore OCC Recycle Telemetry | alloc_from_starved | alloc_from_primary | alloc_from_probe | alloc_fresh | pop_empty | pop_cas_fail | push_success | push_cas_fail | stash_starved |"
        );
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            result.profile.label,
            recycle.alloc_from_starved_delta,
            recycle.alloc_from_primary_delta,
            recycle.alloc_from_probe_delta,
            recycle.alloc_fresh_delta,
            recycle.pop_empty_delta,
            recycle.pop_cas_fail_delta,
            recycle.push_success_delta,
            recycle.push_cas_fail_delta,
            recycle.stash_starved_delta,
        );
    }

    if let Some(retry) = aerostore.index_retry_telemetry {
        println!(
            "| Aerostore Index Retry Telemetry | insert_ops | remove_ops | retry_loops | retry_alloc | retry_structural | retry_epoch | max_insert_attempts | max_remove_attempts | gc_nodes_examined | gc_nodes_requeued | gc_recycle_errors | gc_assist_calls | gc_assist_reclaimed | gc_daemon_cycles | gc_daemon_reclaimed | pressure_window_failures | pressure_window_reclaimed | pressure_consecutive_healthy_windows | retired_backlog | pressure_state | pressure_to_normal | pressure_to_warm | pressure_to_hot | alloc_failure_events | reserve_node_pushes | reserve_node_hits | reserve_node_misses | reserve_posting_pushes | reserve_posting_hits | reserve_posting_misses | reserve_tower_pushes | reserve_tower_hits | reserve_tower_misses | retry_phase_b_hits | retry_phase_c_hits |"
        );
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            result.profile.label,
            retry.insert_ops,
            retry.remove_ops,
            retry.retry_loops,
            retry.retry_alloc,
            retry.retry_structural,
            retry.retry_epoch,
            retry.max_insert_attempts,
            retry.max_remove_attempts,
            retry.gc_nodes_examined,
            retry.gc_nodes_requeued,
            retry.gc_recycle_errors,
            retry.gc_assist_calls,
            retry.gc_assist_reclaimed,
            retry.gc_daemon_cycles,
            retry.gc_daemon_reclaimed,
            retry.pressure_window_failures,
            retry.pressure_window_reclaimed,
            retry.pressure_consecutive_healthy_windows,
            retry.retired_backlog,
            retry.pressure_state,
            retry.pressure_to_normal,
            retry.pressure_to_warm,
            retry.pressure_to_hot,
            retry.alloc_failure_events,
            retry.reserve_node_pushes,
            retry.reserve_node_hits,
            retry.reserve_node_misses,
            retry.reserve_posting_pushes,
            retry.reserve_posting_hits,
            retry.reserve_posting_misses,
            retry.reserve_tower_pushes,
            retry.reserve_tower_hits,
            retry.reserve_tower_misses,
            retry.retry_phase_b_hits,
            retry.retry_phase_c_hits,
        );
    }

    println!(
        "| Postgres Scan Breakdown | Client RTT p50 upper (us) | Server Exec p50 upper (us) | p50 endpoint difference (us) | Client RTT p90 upper (us) | Server Exec p90 upper (us) | p90 endpoint difference (us) | Client RTT p99 upper (us) | Server Exec p99 upper (us) | p99 endpoint difference (us) |"
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

    println!("| Aerostore Raw Scan | p50 upper (us) | p90 upper (us) | p99 upper (us) |",);
    println!("|---|---:|---:|---:|");
    println!(
        "| aerostore_scan | {:.2} | {:.2} | {:.2} |",
        ns_to_us(aerostore.scan_latency.p50_ns),
        ns_to_us(aerostore.scan_latency.p90_ns),
        ns_to_us(aerostore.scan_latency.p99_ns),
    );

    println!(
        "hyperfeed_crucible_summary: profile={} tps_ratio_aerostore_vs_postgres={:.2}x total_p99_ratio_upper={:.3}",
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
    let server = postgres.scan_server_exec_latency.unwrap_or_default();

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
    assert!(
        result.postgres.total_latency.p99_lower_ns > 0,
        "PostgreSQL p99 lower bound must be positive for a ratio gate"
    );
    let tps_ratio = result.aerostore.tps / result.postgres.tps.max(f64::EPSILON);
    let p99_ratio = result.aerostore.total_latency.p99_ns as f64
        / (result.postgres.total_latency.p99_lower_ns.max(1) as f64);

    assert!(
        tps_ratio >= REQUIRED_TPS_RATIO,
        "{} Aerostore TPS gate failed: observed {:.2}x, required >= {:.2}x",
        result.profile.label,
        tps_ratio,
        REQUIRED_TPS_RATIO
    );
    assert!(
        p99_ratio <= REQUIRED_P99_RATIO,
        "{} Aerostore p99 gate failed: conservative upper ratio {:.3}, required <= {:.3}",
        result.profile.label,
        p99_ratio,
        REQUIRED_P99_RATIO
    );
}

fn latency_summary(hist: &[u64; HIST_BUCKETS], total_samples: u64) -> LatencySummary {
    let p99 = percentile_bounds(hist, total_samples, 99);
    LatencySummary {
        samples: total_samples,
        p50_ns: percentile_bounds(hist, total_samples, 50).upper_ns,
        p90_ns: percentile_bounds(hist, total_samples, 90).upper_ns,
        p99_ns: p99.upper_ns,
        p99_lower_ns: p99.lower_ns,
    }
}

// Human-facing microseconds are rounded; this line preserves exact inclusive
// nanosecond bounds for conservative comparisons against another engine/run.
fn print_total_latency_bounds(result: &EngineRunResult, profile: CrucibleProfile) {
    println!(
        "hyperfeed_crucible_latency_bounds: profile={} engine={} histogram_subdivisions={} samples={} p99_lower_ns={} p99_upper_ns={}",
        profile.label,
        result.label,
        SUBDIVISIONS,
        result.total_latency.samples,
        result.total_latency.p99_lower_ns,
        result.total_latency.p99_ns,
    );
}

#[inline]
fn nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[inline]
fn ns_to_us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

#[inline]
fn kb_to_mib(kb: u64) -> f64 {
    kb as f64 / 1024.0
}

fn read_process_pss_kb(pid: libc::pid_t) -> Option<u64> {
    let path = format!("/proc/{pid}/smaps_rollup");
    let contents = fs::read_to_string(path).ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("Pss:") {
            let value = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())?;
            return Some(value);
        }
    }
    None
}

fn sample_docker_container_mem_kb(container_id: &str) -> Option<u64> {
    let output = Command::new("docker")
        .arg("stats")
        .arg("--no-stream")
        .arg("--format")
        .arg("{{.MemUsage}}")
        .arg(container_id)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    parse_docker_mem_usage_kb(text.trim())
}

fn parse_docker_mem_usage_kb(text: &str) -> Option<u64> {
    let used = text.split('/').next()?.trim();
    parse_size_to_kb(used)
}

fn parse_size_to_kb(text: &str) -> Option<u64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let split_idx = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    if split_idx == 0 {
        return None;
    }

    let value = trimmed[..split_idx].trim().parse::<f64>().ok()?;
    let unit = trimmed[split_idx..].trim().to_ascii_lowercase();
    let kb = match unit.as_str() {
        "b" => value / 1024.0,
        "kb" => value,
        "kib" => value,
        "mb" => value * 1_000.0,
        "mib" => value * 1_024.0,
        "gb" => value * 1_000_000.0,
        "gib" => value * 1_048_576.0,
        "tb" => value * 1_000_000_000.0,
        "tib" => value * 1_073_741_824.0,
        _ => return None,
    };
    if !kb.is_finite() || kb < 0.0 {
        return None;
    }
    Some(kb.round() as u64)
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

fn wait_for_children_or_terminate(pids: &[libc::pid_t], timeout: Duration) -> Result<(), String> {
    let mut pending = pids.to_vec();
    let started = Instant::now();
    let mut failures = Vec::new();
    while !pending.is_empty() {
        pending.retain(|pid| {
            let mut status: libc::c_int = 0;
            let waited = unsafe { libc::waitpid(*pid, &mut status, libc::WNOHANG) };
            if waited == 0 {
                return true;
            }
            if waited < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    return true;
                }
                failures.push(format!("waitpid({pid}): {error}"));
            } else if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
                failures.push(format!("worker {pid} failed with raw status {status}"));
            }
            false
        });
        if started.elapsed() >= timeout && !pending.is_empty() {
            failures.push(format!(
                "{} workers exceeded {timeout:?} drain budget",
                pending.len()
            ));
            terminate_children(&pending);
            break;
        }
        if !pending.is_empty() {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
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

fn interval_sample_period() -> Duration {
    Duration::from_millis(
        std::env::var("AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MEMORY_SAMPLE_INTERVAL_MS),
    )
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

fn read_epoch_lag_snapshot(shm: &ShmArena) -> EpochLagSnapshot {
    let global_txid = shm.global_txid().load(Ordering::Acquire);
    let proc_array = shm.proc_array();
    let mut global_xmin = global_txid;
    let mut active_slots = 0_u32;
    for slot_idx in 0..proc_array.slots_len() {
        let txid = proc_array.slot_txid(slot_idx).unwrap_or(0);
        if txid == 0 {
            continue;
        }
        active_slots = active_slots.saturating_add(1);
        if txid < global_xmin {
            global_xmin = txid;
        }
    }
    EpochLagSnapshot {
        global_txid,
        global_xmin,
        active_slots,
    }
}

fn stop_background_daemons(
    vacuum_daemon: &VacuumDaemon<CrucibleRow>,
    index_gc_daemon: &ShmIndexGcDaemon,
) -> Result<(), String> {
    let mut failures = Vec::new();
    if let Err(err) = vacuum_daemon.stop() {
        failures.push(format!("failed to stop vacuum daemon: {err}"));
    }
    if let Err(err) = index_gc_daemon.stop() {
        failures.push(format!("failed to stop index GC daemon: {err}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[inline]
fn seed_rng(worker: u64, salt: u64, fixed_seed: Option<u64>) -> u64 {
    if let Some(seed) = fixed_seed {
        return fixed_worker_seed(seed, worker, salt);
    }
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

criterion_group!(benches, bench_hyperfeed_crucible);
criterion_main!(benches);
