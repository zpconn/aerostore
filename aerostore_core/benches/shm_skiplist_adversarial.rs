#![cfg(unix)]

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexValue, RelPtr, SecondaryIndex, ShmArena};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const MAX_WRITERS: u32 = 8;
const MAX_READERS: u32 = 8;
const INSERTS_PER_WRITER_SMALL: u32 = 50_000;
const INSERTS_PER_WRITER_MEDIUM: u32 = 90_000;
const INSERTS_PER_WRITER_LARGE: u32 = 160_000;
const HIST_BUCKET_NS: u64 = 100;
const HIST_BUCKETS: usize = 4_096;
const LOOKUP_P99_BUDGET_NS: u64 = 5_000;

#[repr(C, align(64))]
struct BenchControl {
    ready_writers: AtomicU32,
    ready_readers: AtomicU32,
    go: AtomicU32,
    done_writers: AtomicU32,
    sample_phase: AtomicU32,
    stop_readers: AtomicU32,
    done_readers: AtomicU32,
}

impl BenchControl {
    fn new() -> Self {
        Self {
            ready_writers: AtomicU32::new(0),
            ready_readers: AtomicU32::new(0),
            go: AtomicU32::new(0),
            done_writers: AtomicU32::new(0),
            sample_phase: AtomicU32::new(0),
            stop_readers: AtomicU32::new(0),
            done_readers: AtomicU32::new(0),
        }
    }
}

#[repr(C, align(64))]
struct ReaderHistogram {
    samples: AtomicU64,
    overflow: AtomicU64,
    buckets: [AtomicU64; HIST_BUCKETS],
}

impl ReaderHistogram {
    fn new() -> Self {
        Self {
            samples: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[repr(C, align(64))]
struct BenchMetrics {
    readers: [ReaderHistogram; MAX_READERS as usize],
}

impl BenchMetrics {
    fn new() -> Self {
        Self {
            readers: std::array::from_fn(|_| ReaderHistogram::new()),
        }
    }
}

#[derive(Clone, Copy)]
struct ScenarioResult {
    p99_ns: u64,
    total_samples: u64,
    inserted_rows: u64,
}

fn bench_shm_skiplist_adversarial(c: &mut Criterion) {
    let (writers, readers) = active_worker_counts();
    let small = run_adversarial_scenario(INSERTS_PER_WRITER_SMALL, writers, readers);
    let medium = run_adversarial_scenario(INSERTS_PER_WRITER_MEDIUM, writers, readers);
    let large = run_adversarial_scenario(INSERTS_PER_WRITER_LARGE, writers, readers);

    let small_p99 = small.p99_ns.max(1);
    let medium_p99 = medium.p99_ns.max(1);
    let slope_small_to_medium = local_log_slope(
        small.inserted_rows,
        small_p99,
        medium.inserted_rows,
        medium_p99,
    );
    let slope_medium_to_large = local_log_slope(
        medium.inserted_rows,
        medium_p99,
        large.inserted_rows,
        large.p99_ns.max(1),
    );

    println!(
        "shm_skiplist_adversarial: writers={} readers={} small_p99_ns={} medium_p99_ns={} large_p99_ns={} slope_small_to_medium={:.3} slope_medium_to_large={:.3} small_samples={} medium_samples={} large_samples={}",
        writers,
        readers,
        small.p99_ns,
        medium.p99_ns,
        large.p99_ns,
        slope_small_to_medium,
        slope_medium_to_large,
        small.total_samples,
        medium.total_samples,
        large.total_samples,
    );

    assert!(
        large.p99_ns < LOOKUP_P99_BUDGET_NS,
        "skiplist p99 lookup latency exceeded {}ns budget (observed {}ns)",
        LOOKUP_P99_BUDGET_NS,
        large.p99_ns,
    );
    assert!(
        slope_small_to_medium < 0.6,
        "lookup latency scaling drifted from logarithmic/sublinear behavior for small->medium (slope={:.3})",
        slope_small_to_medium,
    );
    assert!(
        slope_medium_to_large < 0.6,
        "lookup latency scaling drifted from logarithmic/sublinear behavior for medium->large (slope={:.3})",
        slope_medium_to_large,
    );

    let mut group = c.benchmark_group("shm_skiplist_adversarial");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(2));
    group.bench_with_input(BenchmarkId::from_parameter("p99_ns"), &large, |b, r| {
        b.iter(|| black_box(r.p99_ns))
    });
    group.finish();
}

fn run_adversarial_scenario(inserts_per_writer: u32, writers: u32, readers: u32) -> ScenarioResult {
    let keyspace = writers.saturating_mul(inserts_per_writer).saturating_mul(4);
    let arena_bytes = scenario_arena_bytes(writers, inserts_per_writer);
    let shm = Arc::new(ShmArena::new(arena_bytes).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u64>::new_in_shared("aircraft_id", Arc::clone(&shm));

    let control_ptr = shm
        .chunked_arena()
        .alloc(BenchControl::new())
        .expect("failed to allocate control block");
    let metrics_ptr = shm
        .chunked_arena()
        .alloc(BenchMetrics::new())
        .expect("failed to allocate metrics block");

    let control_offset = control_ptr.load(Ordering::Acquire);
    let metrics_offset = metrics_ptr.load(Ordering::Acquire);

    let mut pids = Vec::with_capacity((writers + readers) as usize);

    for writer_id in 0..writers {
        let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed for writer");
        match fork_result {
            rustix::runtime::Fork::Child(_) => {
                writer_main(
                    index.clone(),
                    control_offset,
                    writer_id,
                    keyspace,
                    inserts_per_writer,
                );
            }
            rustix::runtime::Fork::Parent(pid) => pids.push(pid),
        }
    }

    for reader_id in 0..readers {
        let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed for reader");
        match fork_result {
            rustix::runtime::Fork::Child(_) => {
                reader_main(
                    index.clone(),
                    control_offset,
                    metrics_offset,
                    reader_id as usize,
                    keyspace,
                );
            }
            rustix::runtime::Fork::Parent(pid) => pids.push(pid),
        }
    }

    let control = control_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve control block");

    wait_until(
        || {
            control.ready_writers.load(Ordering::Acquire) == writers
                && control.ready_readers.load(Ordering::Acquire) == readers
        },
        Duration::from_secs(20),
        "workers did not reach startup barrier",
    );

    control.go.store(1, Ordering::Release);

    wait_until(
        || control.done_writers.load(Ordering::Acquire) == writers,
        Duration::from_secs(120),
        "writers did not finish in time",
    );

    // Measure only the steady-state full-cardinality phase.
    control.sample_phase.store(1, Ordering::Release);
    std::thread::sleep(Duration::from_millis(500));
    control.stop_readers.store(1, Ordering::Release);

    wait_until(
        || control.done_readers.load(Ordering::Acquire) == readers,
        Duration::from_secs(30),
        "readers did not finish in time",
    );

    for pid in pids {
        wait_for_child(pid);
    }

    let metrics = metrics_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve metrics block");
    let (p99_ns, total_samples) = aggregate_p99(metrics, readers as usize);

    ScenarioResult {
        p99_ns,
        total_samples,
        inserted_rows: (writers as u64) * (inserts_per_writer as u64),
    }
}

fn writer_main(
    index: SecondaryIndex<u64>,
    control_offset: u32,
    writer_id: u32,
    keyspace: u32,
    inserts_per_writer: u32,
) -> ! {
    pin_current_process(writer_id as usize);

    let Some(control) = RelPtr::<BenchControl>::from_offset(control_offset)
        .as_ref(index.shared_arena().mmap_base())
    else {
        unsafe { libc::_exit(201) };
    };

    control.ready_writers.fetch_add(1, Ordering::AcqRel);
    while control.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let mut rng = seed_rng(0xA3E0_52_01_u64 ^ writer_id as u64);
    for i in 0..inserts_per_writer {
        let aircraft_id = (next_u64(&mut rng) % keyspace as u64) as u32;
        let row_id = ((writer_id as u64) << 32) | i as u64;
        index.insert(IndexValue::U64(aircraft_id as u64), row_id);
    }

    control.done_writers.fetch_add(1, Ordering::AcqRel);
    unsafe { libc::_exit(0) };
}

fn reader_main(
    index: SecondaryIndex<u64>,
    control_offset: u32,
    metrics_offset: u32,
    reader_slot: usize,
    keyspace: u32,
) -> ! {
    pin_current_process((MAX_WRITERS as usize).saturating_add(reader_slot));

    let Some(control) = RelPtr::<BenchControl>::from_offset(control_offset)
        .as_ref(index.shared_arena().mmap_base())
    else {
        unsafe { libc::_exit(211) };
    };
    let Some(metrics) = RelPtr::<BenchMetrics>::from_offset(metrics_offset)
        .as_ref(index.shared_arena().mmap_base())
    else {
        unsafe { libc::_exit(212) };
    };
    let hist = &metrics.readers[reader_slot];

    control.ready_readers.fetch_add(1, Ordering::AcqRel);
    while control.go.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let mut rng = seed_rng(0xB4F1_63_02_u64 ^ reader_slot as u64);
    while control.stop_readers.load(Ordering::Acquire) == 0 {
        let aircraft_id = (next_u64(&mut rng) % keyspace as u64) as u32;
        let t0 = Instant::now();
        let hits = index.lookup_u64_posting_count(aircraft_id as u64);
        black_box(hits);
        if control.sample_phase.load(Ordering::Acquire) != 0 {
            let elapsed_ns = t0.elapsed().as_nanos() as u64;
            hist.samples.fetch_add(1, Ordering::AcqRel);
            let bucket = (elapsed_ns / HIST_BUCKET_NS) as usize;
            if bucket < HIST_BUCKETS {
                hist.buckets[bucket].fetch_add(1, Ordering::AcqRel);
            } else {
                hist.overflow.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    control.done_readers.fetch_add(1, Ordering::AcqRel);
    unsafe { libc::_exit(0) };
}

fn aggregate_p99(metrics: &BenchMetrics, active_readers: usize) -> (u64, u64) {
    let reader_count = active_readers.min(MAX_READERS as usize).max(1);
    let mut combined = vec![0_u64; HIST_BUCKETS];
    let mut total = 0_u64;
    let mut overflow = 0_u64;

    for reader in metrics.readers.iter().take(reader_count) {
        total = total.saturating_add(reader.samples.load(Ordering::Acquire));
        overflow = overflow.saturating_add(reader.overflow.load(Ordering::Acquire));
        for (idx, slot) in combined.iter_mut().enumerate() {
            *slot = slot.saturating_add(reader.buckets[idx].load(Ordering::Acquire));
        }
    }

    if total == 0 {
        return (u64::MAX, 0);
    }

    let target = ((total as f64) * 0.99).ceil() as u64;
    let mut cumulative = 0_u64;
    for (idx, count) in combined.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= target {
            return ((idx as u64) * HIST_BUCKET_NS, total);
        }
    }

    if overflow > 0 {
        ((HIST_BUCKETS as u64) * HIST_BUCKET_NS + 1, total)
    } else {
        ((HIST_BUCKETS as u64) * HIST_BUCKET_NS, total)
    }
}

fn wait_for_child(pid: rustix::process::Pid) {
    let status = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::empty())
        .expect("waitpid failed")
        .expect("waitpid returned no status");
    assert!(status.exited(), "child did not exit cleanly: {:?}", status);
    assert_eq!(
        status.exit_status(),
        Some(0),
        "child process reported failure: {:?}",
        status
    );
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

fn scenario_arena_bytes(writers: u32, inserts_per_writer: u32) -> usize {
    let total_rows = (writers as usize).saturating_mul(inserts_per_writer as usize);
    let estimated = total_rows.saturating_mul(640);
    estimated.clamp(256 << 20, 1_536 << 20)
}

fn active_worker_counts() -> (u32, u32) {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // Avoid oversubscribing heavily on small runners; benchmark behavior should
    // stay compute-bound rather than scheduler-bound.
    let cap = cpus.max(2).min(MAX_WRITERS as usize) as u32;
    (cap.min(MAX_WRITERS), cap.min(MAX_READERS))
}

#[cfg(target_os = "linux")]
fn pin_current_process(slot: usize) {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if cpus == 0 {
        return;
    }
    let cpu = slot % cpus;
    // SAFETY:
    // we provide a valid cpu_set_t pointer and size for the current process (pid=0).
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_process(_slot: usize) {}

#[inline]
fn seed_rng(seed: u64) -> u64 {
    let mut x = seed ^ 0x9E37_79B9_7F4A_7C15_u64;
    if x == 0 {
        x = 1;
    }
    x
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

#[inline]
fn local_log_slope(rows_a: u64, p99_a: u64, rows_b: u64, p99_b: u64) -> f64 {
    ((p99_b as f64).ln() - (p99_a as f64).ln()) / ((rows_b as f64).ln() - (rows_a as f64).ln())
}

criterion_group!(benches, bench_shm_skiplist_adversarial);
criterion_main!(benches);
