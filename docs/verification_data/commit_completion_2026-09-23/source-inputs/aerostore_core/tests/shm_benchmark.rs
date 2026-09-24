#![cfg(unix)]

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use aerostore_core::ShmArena;

#[repr(C)]
struct BenchRow {
    altitude: u32,
    groundspeed: u16,
    _pad: u16,
}

#[repr(C)]
struct ForkScanResult {
    matched_rows: AtomicU32,
    elapsed_ns: AtomicU64,
}

#[test]
fn benchmark_shm_allocation_throughput() {
    const ROWS: usize = 250_000;
    const ARENA_BYTES: usize = 32 * 1024 * 1024;

    let shm = ShmArena::new(ARENA_BYTES).expect("failed to create shared arena");
    let arena = shm.chunked_arena();

    let start = Instant::now();
    for i in 0..ROWS {
        let altitude = ((i % 45_000) as u32) + 500;
        let groundspeed = 250 + ((i % 220) as u16);
        let ptr = arena
            .alloc(BenchRow {
                altitude,
                groundspeed,
                _pad: 0,
            })
            .expect("shared allocation benchmark ran out of memory");
        assert!(ptr.load(Ordering::Acquire) > 0);
    }
    let elapsed = start.elapsed();
    let rows_per_sec = ROWS as f64 / elapsed.as_secs_f64();

    eprintln!(
        "shm_alloc_benchmark: rows={} elapsed={:?} throughput={:.0} rows/s",
        ROWS, elapsed, rows_per_sec
    );

    assert!(
        rows_per_sec > 100_000.0,
        "unexpectedly low shm alloc throughput"
    );
}

#[test]
fn benchmark_forked_range_scan_latency() {
    const ROWS: usize = 120_000;
    const ARENA_BYTES: usize = 64 * 1024 * 1024;
    const THRESHOLD: u32 = 10_000;

    let shm = ShmArena::new(ARENA_BYTES).expect("failed to create shared arena");
    let arena = shm.chunked_arena();

    let mut row_ptrs = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let altitude = ((i % 45_000) as u32) + 500;
        let groundspeed = 230 + ((i % 280) as u16);
        let ptr = arena
            .alloc(BenchRow {
                altitude,
                groundspeed,
                _pad: 0,
            })
            .expect("failed to allocate benchmark row");
        row_ptrs.push(ptr);
    }

    let result_ptr = arena
        .alloc(ForkScanResult {
            matched_rows: AtomicU32::new(0),
            elapsed_ns: AtomicU64::new(0),
        })
        .expect("failed to allocate shared benchmark result");

    // SAFETY:
    // `fork` duplicates process state. Child exits via `_exit` and does not unwind.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

    if pid == 0 {
        let mut exit_code = 0_i32;
        let base = shm.mmap_base();

        if let Some(result) = result_ptr.as_ref(base) {
            let start = Instant::now();
            let mut matched = 0_u32;

            for ptr in &row_ptrs {
                match ptr.as_ref(base) {
                    Some(row) if row.altitude > THRESHOLD => matched = matched.wrapping_add(1),
                    Some(_) => {}
                    None => {
                        exit_code = 41;
                        break;
                    }
                }
            }

            if exit_code == 0 {
                result.matched_rows.store(matched, Ordering::Release);
                result
                    .elapsed_ns
                    .store(start.elapsed().as_nanos() as u64, Ordering::Release);
            }
        } else {
            exit_code = 40;
        }

        // SAFETY:
        // Child exits immediately without invoking Rust destructors.
        unsafe { libc::_exit(exit_code) };
    }

    let mut status: libc::c_int = 0;
    // SAFETY:
    // `pid` is a valid child process identifier from `fork`.
    let waited = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
    assert_eq!(
        waited,
        pid,
        "waitpid failed: {}",
        std::io::Error::last_os_error()
    );
    assert!(libc::WIFEXITED(status), "child did not exit cleanly");
    assert_eq!(
        libc::WEXITSTATUS(status),
        0,
        "child range scan benchmark failed with non-zero exit"
    );

    let result = result_ptr
        .as_ref(shm.mmap_base())
        .expect("failed to resolve shared scan result");
    let matched = result.matched_rows.load(Ordering::Acquire);
    let elapsed_ns = result.elapsed_ns.load(Ordering::Acquire);

    let expected = row_ptrs
        .iter()
        .filter(|ptr| {
            ptr.as_ref(shm.mmap_base())
                .map(|row| row.altitude > THRESHOLD)
                .unwrap_or(false)
        })
        .count() as u32;
    assert_eq!(matched, expected, "forked range scan result count mismatch");
    assert!(
        elapsed_ns > 0,
        "forked range scan elapsed_ns was not recorded"
    );

    let rows_per_sec = ROWS as f64 / (elapsed_ns as f64 / 1_000_000_000.0);
    eprintln!(
        "shm_fork_scan_benchmark: rows={} threshold={} matched={} elapsed_ns={} throughput={:.0} rows/s",
        ROWS, THRESHOLD, matched, elapsed_ns, rows_per_sec
    );
}
