#![cfg(unix)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{OccError, OccTable, RelPtr, ShmArena};

const PROCESSES: usize = 16;
const ATTEMPTS_PER_PROCESS: usize = 1_000_000;
const LOCK_BUCKETS: usize = 1024;
const REQUIRED_SCALING_RATIO: f64 = 3.0;
const SCENARIO_RUNTIME_MS: u64 = 4_000;
const WORKER_EXIT_TIMEOUT_SECS: u64 = 5;

#[repr(C, align(64))]
struct WorkerStats {
    commits: AtomicU64,
    conflicts: AtomicU64,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            commits: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
        }
    }
}

#[repr(C, align(64))]
struct BenchState {
    ready: AtomicU32,
    go: AtomicU32,
    stop: AtomicU32,
    _pad: [u32; 14],
    workers: [WorkerStats; PROCESSES],
}

impl BenchState {
    fn new() -> Self {
        Self {
            ready: AtomicU32::new(0),
            go: AtomicU32::new(0),
            stop: AtomicU32::new(0),
            _pad: [0; 14],
            workers: std::array::from_fn(|_| WorkerStats::new()),
        }
    }
}

#[derive(Clone, Copy)]
struct ScenarioResult {
    commits: u64,
    conflicts: u64,
    throughput_tps: f64,
    elapsed: Duration,
    timed_out_workers: usize,
}

#[derive(Clone, Copy)]
enum Scenario {
    HeavyContention,
    DisjointWrites,
}

#[test]
#[ignore = "contention benchmark; run explicitly with --ignored for deterministic resource isolation"]
fn benchmark_partitioned_occ_lock_striping_multi_process_scaling() {
    let rows = distinct_bucket_rows(PROCESSES);

    let heavily_contended = run_scenario(&rows, Scenario::HeavyContention);
    let disjoint = run_scenario(&rows, Scenario::DisjointWrites);

    let ratio = disjoint.throughput_tps / heavily_contended.throughput_tps.max(1e-9);

    eprintln!(
        "occ_partitioned_lock_striping_benchmark: processes={} attempts_per_process={} contended_elapsed={:?} contended_tps={:.2} contended_commits={} contended_conflicts={} contended_timed_out_workers={} disjoint_elapsed={:?} disjoint_tps={:.2} disjoint_commits={} disjoint_conflicts={} disjoint_timed_out_workers={} ratio={:.2}x",
        PROCESSES,
        ATTEMPTS_PER_PROCESS,
        heavily_contended.elapsed,
        heavily_contended.throughput_tps,
        heavily_contended.commits,
        heavily_contended.conflicts,
        heavily_contended.timed_out_workers,
        disjoint.elapsed,
        disjoint.throughput_tps,
        disjoint.commits,
        disjoint.conflicts,
        disjoint.timed_out_workers,
        ratio
    );

    assert!(
        heavily_contended.conflicts > 0,
        "heavy contention scenario should produce serialization conflicts"
    );
    assert!(
        disjoint.commits > heavily_contended.commits,
        "disjoint writes should commit more transactions than heavy contention"
    );
    assert!(
        ratio >= REQUIRED_SCALING_RATIO,
        "expected disjoint lock-striping throughput to be >= {:.1}x heavy contention; observed {:.2}x",
        REQUIRED_SCALING_RATIO,
        ratio
    );
}

fn run_scenario(rows: &[usize], scenario: Scenario) -> ScenarioResult {
    let capacity = rows.iter().copied().max().unwrap_or(0) + 1;
    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create benchmark shared arena"));
    let table = Arc::new(
        OccTable::<u64>::new(Arc::clone(&shm), capacity)
            .expect("failed to create partitioned OCC benchmark table"),
    );

    for row_id in rows {
        table
            .seed_row(*row_id, 0_u64)
            .expect("failed to seed benchmark row");
    }

    let state_ptr = shm
        .chunked_arena()
        .alloc(BenchState::new())
        .expect("failed to allocate benchmark coordination state");
    let state_offset = state_ptr.load(Ordering::Acquire);

    let mut pids = Vec::with_capacity(PROCESSES);
    for worker_idx in 0..PROCESSES {
        // SAFETY:
        // fork is intentionally used to measure cross-process lock-striping throughput.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

        if pid == 0 {
            let worker_row = match scenario {
                Scenario::HeavyContention => rows[0],
                Scenario::DisjointWrites => rows[worker_idx],
            };
            run_worker_process(
                worker_idx,
                worker_row,
                scenario,
                state_offset,
                shm.as_ref(),
                table.as_ref(),
            );
        }

        pids.push(pid);
    }

    let state = state_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve benchmark state");

    wait_until(
        || state.ready.load(Ordering::Acquire) == PROCESSES as u32,
        Duration::from_secs(10),
        "workers did not reach benchmark barrier",
    );

    let start = Instant::now();
    state.go.store(1, Ordering::Release);
    std::thread::sleep(Duration::from_millis(SCENARIO_RUNTIME_MS));
    state.stop.store(1, Ordering::Release);
    let timed_out_workers =
        wait_for_children_or_terminate(&pids, Duration::from_secs(WORKER_EXIT_TIMEOUT_SECS));
    let elapsed = start.elapsed();

    let mut commits = 0_u64;
    let mut conflicts = 0_u64;
    for worker in &state.workers {
        commits = commits.wrapping_add(worker.commits.load(Ordering::Acquire));
        conflicts = conflicts.wrapping_add(worker.conflicts.load(Ordering::Acquire));
    }

    ScenarioResult {
        commits,
        conflicts,
        throughput_tps: commits as f64 / elapsed.as_secs_f64().max(1e-9),
        elapsed,
        timed_out_workers,
    }
}

fn run_worker_process(
    worker_idx: usize,
    worker_row: usize,
    scenario: Scenario,
    state_offset: u32,
    shm: &ShmArena,
    table: &OccTable<u64>,
) -> ! {
    let Some(state) = RelPtr::<BenchState>::from_offset(state_offset).as_ref(shm.mmap_base())
    else {
        // SAFETY:
        // child exits immediately without unwinding.
        unsafe { libc::_exit(21) };
    };

    state.ready.fetch_add(1, Ordering::AcqRel);
    while state.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    for _ in 0..ATTEMPTS_PER_PROCESS {
        if state.stop.load(Ordering::Acquire) != 0 {
            break;
        }
        let mut tx = match table.begin_transaction() {
            Ok(tx) => tx,
            Err(_) => {
                state.workers[worker_idx]
                    .conflicts
                    .fetch_add(1, Ordering::AcqRel);
                std::thread::yield_now();
                continue;
            }
        };

        let current = match table.read(&mut tx, worker_row) {
            Ok(Some(value)) => value,
            _ => {
                let _ = table.abort(&mut tx);
                state.workers[worker_idx]
                    .conflicts
                    .fetch_add(1, Ordering::AcqRel);
                continue;
            }
        };

        if matches!(scenario, Scenario::HeavyContention) {
            std::thread::yield_now();
        }

        if table
            .write(&mut tx, worker_row, current.wrapping_add(1))
            .is_err()
        {
            let _ = table.abort(&mut tx);
            state.workers[worker_idx]
                .conflicts
                .fetch_add(1, Ordering::AcqRel);
            continue;
        }

        match table.commit(&mut tx) {
            Ok(_) => {
                state.workers[worker_idx]
                    .commits
                    .fetch_add(1, Ordering::AcqRel);
            }
            Err(OccError::SerializationFailure) => {
                state.workers[worker_idx]
                    .conflicts
                    .fetch_add(1, Ordering::AcqRel);
            }
            Err(_) => {
                let _ = table.abort(&mut tx);
                state.workers[worker_idx]
                    .conflicts
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    // SAFETY:
    // child exits immediately without unwinding.
    unsafe { libc::_exit(0) }
}

fn distinct_bucket_rows(target: usize) -> Vec<usize> {
    let mut seen_buckets = BTreeSet::new();
    let mut rows = Vec::with_capacity(target);
    let mut candidate = 0_usize;

    while rows.len() < target {
        let bucket = lock_bucket_for_row_id(candidate);
        if seen_buckets.insert(bucket) {
            rows.push(candidate);
        }
        candidate = candidate.saturating_add(1);
    }

    rows
}

fn lock_bucket_for_row_id(row_id: usize) -> usize {
    let mut mixed = row_id as u64;
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xff51_afd7_ed55_8ccd);
    mixed ^= mixed >> 33;
    (mixed as usize) % LOCK_BUCKETS
}

fn wait_until<F>(mut condition: F, timeout: Duration, message: &str)
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < timeout, "{message}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_children_or_terminate(pids: &[libc::pid_t], timeout: Duration) -> usize {
    let mut pending = pids.to_vec();
    let start = Instant::now();

    while !pending.is_empty() {
        pending.retain(|pid| {
            let mut status: libc::c_int = 0;
            // SAFETY:
            // pid came from successful fork in this process.
            let waited =
                unsafe { libc::waitpid(*pid, &mut status as *mut libc::c_int, libc::WNOHANG) };
            if waited == 0 {
                return true;
            }
            if waited < 0 {
                panic!("waitpid failed: {}", std::io::Error::last_os_error());
            }

            assert!(
                libc::WIFEXITED(status),
                "worker exited abnormally: pid={pid} status={status}"
            );
            assert_eq!(
                libc::WEXITSTATUS(status),
                0,
                "worker exited with failure code {}",
                libc::WEXITSTATUS(status)
            );
            false
        });

        if pending.is_empty() {
            break;
        }

        if start.elapsed() > timeout {
            for pid in &pending {
                // SAFETY:
                // best-effort cleanup for stuck child processes.
                let _ = unsafe { libc::kill(*pid, libc::SIGKILL) };
            }

            let killed = pending.len();
            for pid in pending {
                let mut status: libc::c_int = 0;
                // SAFETY:
                // reap the process we just terminated.
                let _ = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
            }
            return killed;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    0
}
