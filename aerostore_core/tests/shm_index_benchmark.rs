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
