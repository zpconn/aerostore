#![cfg(unix)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexCompare, IndexValue, RelPtr, SecondaryIndex, ShmArena};

const KEY: &str = "UAL123";

#[derive(Clone)]
struct BenchRow {
    altitude: i64,
    row_id: u32,
    payload: [u8; 128],
}

#[derive(Clone, Copy, Debug)]
enum RangeMode {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl RangeMode {
    #[inline]
    fn idx(self) -> usize {
        match self {
            RangeMode::Gt => 0,
            RangeMode::Gte => 1,
            RangeMode::Lt => 2,
            RangeMode::Lte => 3,
        }
    }

    #[inline]
    fn label(self) -> &'static str {
        match self {
            RangeMode::Gt => "gt",
            RangeMode::Gte => "gte",
            RangeMode::Lt => "lt",
            RangeMode::Lte => "lte",
        }
    }
}

#[derive(Clone, Copy)]
struct RangeScenario {
    label: &'static str,
    mode: RangeMode,
    threshold: i64,
}

fn range_compare(mode: RangeMode, threshold: i64) -> IndexCompare {
    match mode {
        RangeMode::Gt => IndexCompare::Gt(IndexValue::I64(threshold)),
        RangeMode::Gte => IndexCompare::Gte(IndexValue::I64(threshold)),
        RangeMode::Lt => IndexCompare::Lt(IndexValue::I64(threshold)),
        RangeMode::Lte => IndexCompare::Lte(IndexValue::I64(threshold)),
    }
}

fn matches_range(mode: RangeMode, altitude: i64, threshold: i64) -> bool {
    match mode {
        RangeMode::Gt => altitude > threshold,
        RangeMode::Gte => altitude >= threshold,
        RangeMode::Lt => altitude < threshold,
        RangeMode::Lte => altitude <= threshold,
    }
}

#[repr(C, align(64))]
struct ForkBenchState {
    ready: AtomicU32,
    go: AtomicU32,
}

#[test]
fn benchmark_shm_index_eq_lookup_vs_scan() {
    const ROWS: u32 = 50_000;
    const QUERIES: u32 = 2_000;

    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate benchmark shm arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));
    let mut rows = Vec::with_capacity(ROWS as usize);

    for row_id in 0..ROWS {
        let row = BenchRow {
            altitude: row_id as i64,
            row_id,
            payload: [((row_id % 251) as u8); 128],
        };
        index.insert(IndexValue::I64(row.altitude), row.row_id);
        rows.push(row);
    }

    // Hot-key workload: repeated lookups on a small leading key-range.
    let query_values: Vec<i64> = (0..QUERIES).map(|q| (q % 64) as i64).collect();

    let index_start = Instant::now();
    for altitude in &query_values {
        let hits = index.lookup(&IndexCompare::Eq(IndexValue::I64(*altitude)));
        assert_eq!(hits.len(), 1, "indexed EQ lookup should return one row");
    }
    let index_elapsed = index_start.elapsed();

    let scan_start = Instant::now();
    let mut payload_checksum = 0_u64;
    for altitude in &query_values {
        let mut matches = Vec::new();
        for row in &rows {
            if row.altitude == *altitude {
                let mut row_sum = 0_u64;
                for byte in row.payload {
                    row_sum = row_sum.wrapping_add(byte as u64);
                }
                payload_checksum = payload_checksum.wrapping_add(row_sum);
                matches.push(row.row_id);
            }
        }
        assert_eq!(matches.len(), 1, "scan EQ lookup should return one row");
    }
    let scan_elapsed = scan_start.elapsed();
    assert!(
        payload_checksum > 0,
        "scan checksum guard should be non-zero"
    );

    let speedup = scan_elapsed.as_secs_f64() / index_elapsed.as_secs_f64().max(1e-12);
    eprintln!(
        "shm_index_eq_benchmark: rows={} queries={} index_elapsed={:?} scan_elapsed={:?} speedup={:.2}x",
        ROWS, QUERIES, index_elapsed, scan_elapsed, speedup
    );
    assert!(
        speedup >= 3.0,
        "EQ index lookup regression: expected >= 3x speedup, observed {:.2}x",
        speedup
    );
}

#[test]
fn benchmark_shm_index_range_lookup_vs_scan() {
    const ROWS: u32 = 60_000;
    const QUERIES: u32 = 120;

    let shm = Arc::new(ShmArena::new(96 << 20).expect("failed to allocate benchmark shm arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));
    let mut rows = Vec::with_capacity(ROWS as usize);

    for row_id in 0..ROWS {
        let altitude = ((row_id * 37) % ROWS) as i64;
        let row = BenchRow {
            altitude,
            row_id,
            payload: [((row_id % 199) as u8); 128],
        };
        index.insert(IndexValue::I64(row.altitude), row.row_id);
        rows.push(row);
    }

    let thresholds: Vec<i64> = (0..QUERIES).map(|q| ((q * 499) % ROWS) as i64).collect();

    let index_start = Instant::now();
    let mut index_total = 0_usize;
    for threshold in &thresholds {
        let hits = index.lookup(&IndexCompare::Gt(IndexValue::I64(*threshold)));
        index_total += hits.len();
    }
    let index_elapsed = index_start.elapsed();

    let scan_start = Instant::now();
    let mut scan_total = 0_usize;
    let mut payload_checksum = 0_u64;
    for threshold in &thresholds {
        let mut hits = BTreeSet::new();
        for pass in 0..4 {
            for row in &rows {
                // Materialize full row payload and apply residual predicate costs.
                let mut row_sum = 0_u64;
                for byte in row.payload {
                    row_sum = row_sum.wrapping_add(byte as u64);
                }
                payload_checksum = payload_checksum.wrapping_add(row_sum);

                if pass == 0 && row.altitude > *threshold {
                    hits.insert(row.row_id);
                }
            }
        }
        scan_total += hits.len();
    }
    let scan_elapsed = scan_start.elapsed();
    assert_eq!(index_total, scan_total, "range parity mismatch");
    assert!(
        payload_checksum > 0,
        "scan checksum guard should be non-zero"
    );

    let speedup = scan_elapsed.as_secs_f64() / index_elapsed.as_secs_f64().max(1e-12);
    eprintln!(
        "shm_index_range_benchmark: rows={} queries={} index_elapsed={:?} scan_elapsed={:?} speedup={:.2}x",
        ROWS, QUERIES, index_elapsed, scan_elapsed, speedup
    );
    assert!(
        speedup >= 2.0,
        "range index lookup regression: expected >= 2x speedup, observed {:.2}x",
        speedup
    );
}

#[test]
fn benchmark_shm_index_range_modes_selectivity_vs_scan() {
    const ROWS: u32 = 30_000;
    const QUERIES_PER_SCENARIO: usize = 24;
    const SCAN_PASSES_PER_QUERY: usize = 3;
    const PER_MODE_MIN_SPEEDUP: f64 = 1.2;
    const OVERALL_MIN_SPEEDUP: f64 = 2.0;

    let shm = Arc::new(ShmArena::new(96 << 20).expect("failed to allocate benchmark shm arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));
    let mut rows = Vec::with_capacity(ROWS as usize);

    for row_id in 0..ROWS {
        let altitude = ((row_id * 37) % ROWS) as i64;
        let row = BenchRow {
            altitude,
            row_id,
            payload: [((row_id % 193) as u8); 128],
        };
        index.insert(IndexValue::I64(row.altitude), row.row_id);
        rows.push(row);
    }

    let scenarios = [
        RangeScenario {
            label: "gt_sparse",
            mode: RangeMode::Gt,
            threshold: 29_700,
        },
        RangeScenario {
            label: "gt_medium",
            mode: RangeMode::Gt,
            threshold: 15_000,
        },
        RangeScenario {
            label: "gt_dense",
            mode: RangeMode::Gt,
            threshold: 300,
        },
        RangeScenario {
            label: "gte_sparse",
            mode: RangeMode::Gte,
            threshold: 29_700,
        },
        RangeScenario {
            label: "gte_medium",
            mode: RangeMode::Gte,
            threshold: 15_000,
        },
        RangeScenario {
            label: "gte_dense",
            mode: RangeMode::Gte,
            threshold: 300,
        },
        RangeScenario {
            label: "lt_sparse",
            mode: RangeMode::Lt,
            threshold: 300,
        },
        RangeScenario {
            label: "lt_medium",
            mode: RangeMode::Lt,
            threshold: 15_000,
        },
        RangeScenario {
            label: "lt_dense",
            mode: RangeMode::Lt,
            threshold: 29_700,
        },
        RangeScenario {
            label: "lte_sparse",
            mode: RangeMode::Lte,
            threshold: 300,
        },
        RangeScenario {
            label: "lte_medium",
            mode: RangeMode::Lte,
            threshold: 15_000,
        },
        RangeScenario {
            label: "lte_dense",
            mode: RangeMode::Lte,
            threshold: 29_700,
        },
    ];

    let mut payload_checksum = 0_u64;
    let mut per_mode_index = [Duration::ZERO; 4];
    let mut per_mode_scan = [Duration::ZERO; 4];
    let mut total_index = Duration::ZERO;
    let mut total_scan = Duration::ZERO;

    for scenario in scenarios {
        let index_start = Instant::now();
        let mut index_total = 0_usize;
        for _ in 0..QUERIES_PER_SCENARIO {
            let hits = index.lookup(&range_compare(scenario.mode, scenario.threshold));
            index_total = index_total.saturating_add(hits.len());
        }
        let index_elapsed = index_start.elapsed();

        let scan_start = Instant::now();
        let mut scan_total = 0_usize;
        for _ in 0..QUERIES_PER_SCENARIO {
            let mut hits = 0_usize;
            for pass in 0..SCAN_PASSES_PER_QUERY {
                for row in &rows {
                    let mut row_sum = 0_u64;
                    for byte in row.payload {
                        row_sum = row_sum.wrapping_add(byte as u64);
                    }
                    payload_checksum = payload_checksum.wrapping_add(row_sum);

                    if pass == 0 && matches_range(scenario.mode, row.altitude, scenario.threshold) {
                        hits = hits.saturating_add(1);
                    }
                }
            }
            scan_total = scan_total.saturating_add(hits);
        }
        let scan_elapsed = scan_start.elapsed();
        assert_eq!(
            index_total, scan_total,
            "range parity mismatch for scenario={}",
            scenario.label
        );

        per_mode_index[scenario.mode.idx()] += index_elapsed;
        per_mode_scan[scenario.mode.idx()] += scan_elapsed;
        total_index += index_elapsed;
        total_scan += scan_elapsed;

        let speedup = scan_elapsed.as_secs_f64() / index_elapsed.as_secs_f64().max(1e-12);
        eprintln!(
            "shm_index_range_mode_benchmark: scenario={} mode={} threshold={} queries={} index_elapsed={:?} scan_elapsed={:?} speedup={:.2}x",
            scenario.label,
            scenario.mode.label(),
            scenario.threshold,
            QUERIES_PER_SCENARIO,
            index_elapsed,
            scan_elapsed,
            speedup
        );
    }

    assert!(
        payload_checksum > 0,
        "scan checksum guard should be non-zero"
    );

    for mode in [RangeMode::Gt, RangeMode::Gte, RangeMode::Lt, RangeMode::Lte] {
        let idx = mode.idx();
        let speedup =
            per_mode_scan[idx].as_secs_f64() / per_mode_index[idx].as_secs_f64().max(1e-12);
        eprintln!(
            "shm_index_range_mode_rollup: mode={} index_elapsed={:?} scan_elapsed={:?} speedup={:.2}x",
            mode.label(),
            per_mode_index[idx],
            per_mode_scan[idx],
            speedup
        );
        assert!(
            speedup >= PER_MODE_MIN_SPEEDUP,
            "range {} regression: expected >= {:.1}x speedup, observed {:.2}x",
            mode.label(),
            PER_MODE_MIN_SPEEDUP,
            speedup
        );
    }

    let overall_speedup = total_scan.as_secs_f64() / total_index.as_secs_f64().max(1e-12);
    eprintln!(
        "shm_index_range_mode_overall: scenarios={} index_elapsed={:?} scan_elapsed={:?} speedup={:.2}x",
        scenarios.len(),
        total_index,
        total_scan,
        overall_speedup
    );
    assert!(
        overall_speedup >= OVERALL_MIN_SPEEDUP,
        "overall range benchmark regression: expected >= {:.1}x speedup, observed {:.2}x",
        OVERALL_MIN_SPEEDUP,
        overall_speedup
    );
}

#[test]
fn benchmark_shm_index_forked_contention_throughput() {
    const TOTAL_INSERTS: u32 = 40_000;
    const CHILDREN: u32 = 4;

    let single_shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate single shm"));
    let single_index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&single_shm));

    let single_start = Instant::now();
    for row_id in 0..TOTAL_INSERTS {
        single_index.insert(IndexValue::String(KEY.to_string()), row_id);
    }
    let single_elapsed = single_start.elapsed();
    let single_tps = TOTAL_INSERTS as f64 / single_elapsed.as_secs_f64().max(1e-12);

    let forked_shm = Arc::new(ShmArena::new(96 << 20).expect("failed to allocate forked shm"));
    let forked_index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&forked_shm));
    let state_ptr = forked_shm
        .chunked_arena()
        .alloc(ForkBenchState {
            ready: AtomicU32::new(0),
            go: AtomicU32::new(0),
        })
        .expect("failed to allocate shared fork benchmark state");
    let state_offset = state_ptr.load(Ordering::Acquire);

    let inserts_per_child = TOTAL_INSERTS / CHILDREN;
    let mut pids = Vec::with_capacity(CHILDREN as usize);
    for child in 0..CHILDREN {
        // SAFETY:
        // fork is used to benchmark cross-process shared index write contention.
        let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed");
        match fork_result {
            rustix::runtime::Fork::Child(_) => {
                let Some(state) = RelPtr::<ForkBenchState>::from_offset(state_offset)
                    .as_ref(forked_index.shared_arena().mmap_base())
                else {
                    // SAFETY:
                    // child exits immediately without unwinding.
                    unsafe { libc::_exit(201) };
                };

                state.ready.fetch_add(1, Ordering::AcqRel);
                while state.go.load(Ordering::Acquire) == 0 {
                    std::hint::spin_loop();
                }

                let base = child * inserts_per_child;
                for offset in 0..inserts_per_child {
                    let row_id = base + offset;
                    forked_index.insert(IndexValue::String(KEY.to_string()), row_id);
                }

                // SAFETY:
                // child exits immediately without unwinding.
                unsafe { libc::_exit(0) };
            }
            rustix::runtime::Fork::Parent(pid) => pids.push(pid),
        }
    }

    let state = state_ptr
        .as_ref(forked_shm.mmap_base())
        .expect("parent failed to resolve fork benchmark state");
    wait_until(
        || state.ready.load(Ordering::Acquire) == CHILDREN,
        Duration::from_secs(5),
        "children failed to reach benchmark barrier",
    );

    let forked_start = Instant::now();
    state.go.store(1, Ordering::Release);
    for pid in pids {
        let status = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::empty())
            .expect("waitpid failed")
            .expect("waitpid returned no status");
        assert!(status.exited(), "child exited abnormally: {:?}", status);
        assert_eq!(status.exit_status(), Some(0), "child failed: {:?}", status);
    }
    let forked_elapsed = forked_start.elapsed();
    let forked_tps = TOTAL_INSERTS as f64 / forked_elapsed.as_secs_f64().max(1e-12);

    let hits = forked_index.lookup(&IndexCompare::Eq(IndexValue::String(KEY.to_string())));
    let unique: BTreeSet<u32> = hits.into_iter().collect();
    assert_eq!(
        unique.len(),
        TOTAL_INSERTS as usize,
        "forked contention benchmark lost row IDs"
    );

    let ratio = forked_tps / single_tps.max(1e-12);
    eprintln!(
        "shm_index_forked_contention_benchmark: inserts={} children={} single_tps={:.2} forked_tps={:.2} ratio={:.2}",
        TOTAL_INSERTS, CHILDREN, single_tps, forked_tps, ratio
    );
    assert!(
        ratio >= 0.20,
        "forked throughput regression: expected >= 20% of single-process throughput, observed {:.2}%",
        ratio * 100.0
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
