#![cfg(unix)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{run_vacuum_pass, OccTable, ShmArena};

const CHILDREN: usize = 6;
const ROWS: usize = 4;
const ITERATIONS_PER_CHILD: usize = 45_000;
const ARENA_BYTES: usize = 64 << 20;

#[test]
fn multiprocess_churn_preserves_shared_free_list_invariants() {
    let shm = Arc::new(ShmArena::new(ARENA_BYTES).expect("failed to create shared arena"));
    let table = Arc::new(OccTable::<u64>::new(Arc::clone(&shm), ROWS).expect("create occ table"));
    for row_id in 0..ROWS {
        table.seed_row(row_id, row_id as u64).expect("seed row");
    }
    let baseline_head = shm.chunked_arena().head_offset();

    let mut alive = HashSet::<libc::pid_t>::with_capacity(CHILDREN);
    for child_idx in 0..CHILDREN {
        // SAFETY:
        // `fork` duplicates process state for isolated churn workers.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
        if pid == 0 {
            let code = run_worker(table.as_ref(), child_idx);
            // SAFETY:
            // child exits immediately without unwinding.
            unsafe { libc::_exit(code) };
        }
        alive.insert(pid);
    }

    let start = Instant::now();
    while !alive.is_empty() {
        // Parent drives vacuum while child processes churn row versions.
        let _ = run_vacuum_pass(table.as_ref());
        assert!(
            shm.free_list_pops() <= shm.free_list_pushes(),
            "free-list invariant violated during churn: pops={} pushes={}",
            shm.free_list_pops(),
            shm.free_list_pushes()
        );

        let completed = reap_children_nonblocking(&mut alive);
        if completed == 0 {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    // Final catch-up vacuum passes to reclaim tail versions after all workers exit.
    for _ in 0..200 {
        let reclaimed = run_vacuum_pass(table.as_ref()).expect("final vacuum pass failed");
        if reclaimed.is_empty() {
            break;
        }
    }

    let recycle = table
        .recycle_telemetry()
        .expect("failed to read OCC recycle telemetry");
    let pops = recycle
        .alloc_from_primary
        .saturating_add(recycle.alloc_from_probe)
        .saturating_add(recycle.alloc_from_starved);
    assert!(
        recycle.push_success > 0,
        "expected reclaimed rows to be pushed into OCC recycler"
    );
    assert!(pops > 0, "expected allocator to reuse recycled OCC rows");
    assert!(
        pops <= recycle.push_success,
        "OCC recycler invariant violated after churn: pops={} pushes={}",
        pops,
        recycle.push_success
    );

    let head_growth = shm
        .chunked_arena()
        .head_offset()
        .saturating_sub(baseline_head) as u64;
    let naive_growth = (std::mem::size_of::<aerostore_core::OccRow<u64>>() as u64)
        .saturating_mul((CHILDREN * ITERATIONS_PER_CHILD) as u64);
    assert!(
        head_growth.saturating_mul(3) < naive_growth.saturating_mul(2),
        "allocator growth too high under vacuum churn: actual={} naive={}",
        head_growth,
        naive_growth
    );

    let elapsed = start.elapsed();
    let updates = (CHILDREN * ITERATIONS_PER_CHILD) as f64;
    let tps = updates / elapsed.as_secs_f64().max(f64::EPSILON);
    assert!(
        tps >= 50_000.0,
        "multi-process churn throughput unexpectedly low: {:.2} updates/sec",
        tps
    );
}

fn run_worker(table: &OccTable<u64>, child_idx: usize) -> i32 {
    for iter in 0..ITERATIONS_PER_CHILD {
        let row_id = (child_idx + iter) % ROWS;
        let mut committed = false;
        for _ in 0..64 {
            let mut tx = match table.begin_transaction() {
                Ok(tx) => tx,
                Err(_) => {
                    std::thread::yield_now();
                    continue;
                }
            };

            let current = match table.read(&mut tx, row_id) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    let _ = table.abort(&mut tx);
                    std::thread::yield_now();
                    continue;
                }
                Err(_) => {
                    let _ = table.abort(&mut tx);
                    std::thread::yield_now();
                    continue;
                }
            };

            match table.write(&mut tx, row_id, current.wrapping_add(1)) {
                Ok(()) => {}
                Err(_) => {
                    let _ = table.abort(&mut tx);
                    std::thread::yield_now();
                    continue;
                }
            }

            match table.commit(&mut tx) {
                Ok(1) => {
                    committed = true;
                    break;
                }
                Ok(_) => return 14,
                Err(_) => {
                    std::thread::yield_now();
                    continue;
                }
            }
        }

        if !committed {
            return 16;
        }
    }
    0
}

fn reap_children_nonblocking(alive: &mut HashSet<libc::pid_t>) -> usize {
    let pids: Vec<libc::pid_t> = alive.iter().copied().collect();
    let mut completed = 0_usize;
    for pid in pids {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // pid belongs to a child process spawned in this test.
        let waited = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, libc::WNOHANG) };
        if waited == 0 {
            continue;
        }
        assert_eq!(
            waited,
            pid,
            "waitpid failed: {}",
            std::io::Error::last_os_error()
        );
        assert!(libc::WIFEXITED(status), "child did not exit cleanly");
        let code = libc::WEXITSTATUS(status);
        assert_eq!(code, 0, "child exited non-zero during churn: {}", code);
        alive.remove(&pid);
        completed += 1;
    }
    completed
}
