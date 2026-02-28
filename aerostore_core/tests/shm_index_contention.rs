#![cfg(unix)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexCompare, IndexValue, RelPtr, SecondaryIndex, ShmArena};

const SHARED_KEY: &str = "UAL123";
const ROW_IDS: u32 = 2_000;

#[repr(C, align(64))]
struct SharedPhase {
    ready: AtomicU32,
    phase: AtomicU32,
    insert_done: AtomicU32,
    remove_done: AtomicU32,
}

#[derive(Clone, Copy)]
enum WorkerRole {
    Insert,
    Remove,
}

#[test]
fn cross_process_same_key_insert_remove_contention_with_duplicates_is_exact() {
    let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let phase_ptr = shm
        .chunked_arena()
        .alloc(SharedPhase {
            ready: AtomicU32::new(0),
            phase: AtomicU32::new(0),
            insert_done: AtomicU32::new(0),
            remove_done: AtomicU32::new(0),
        })
        .expect("failed to allocate shared phase state");
    let phase_offset = phase_ptr.load(Ordering::Acquire);

    let pids = [
        spawn_worker(index.clone(), phase_offset, WorkerRole::Insert),
        spawn_worker(index.clone(), phase_offset, WorkerRole::Insert),
        spawn_worker(index.clone(), phase_offset, WorkerRole::Remove),
        spawn_worker(index.clone(), phase_offset, WorkerRole::Remove),
    ];

    let phase = phase_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve shared phase state");
    wait_until(
        || phase.ready.load(Ordering::Acquire) == 4,
        Duration::from_secs(3),
        "workers did not reach ready barrier in time",
    );

    // Phase 1: two inserters concurrently publish duplicate postings for same key.
    phase.phase.store(1, Ordering::Release);
    wait_until(
        || phase.insert_done.load(Ordering::Acquire) == 2,
        Duration::from_secs(10),
        "insert workers did not finish in time",
    );

    // Phase 2: two removers concurrently remove odd row IDs twice total.
    phase.phase.store(2, Ordering::Release);
    wait_until(
        || phase.remove_done.load(Ordering::Acquire) == 2,
        Duration::from_secs(10),
        "remove workers did not finish in time",
    );

    for pid in pids {
        wait_for_child(pid);
    }

    let hits = index.lookup(&IndexCompare::Eq(IndexValue::String(
        SHARED_KEY.to_string(),
    )));
    let observed: BTreeSet<u32> = hits.into_iter().collect();
    let expected: BTreeSet<u32> = (0..ROW_IDS).filter(|row_id| row_id % 2 == 0).collect();

    assert_eq!(
        observed, expected,
        "concurrent insert/remove contention produced incorrect final posting set"
    );
}

fn spawn_worker(
    index: SecondaryIndex<u32>,
    phase_offset: u32,
    role: WorkerRole,
) -> rustix::process::Pid {
    // SAFETY:
    // fork is used to validate cross-process shared-memory index behavior.
    let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed");
    match fork_result {
        rustix::runtime::Fork::Child(_) => {
            let mut exit_code = 0_i32;

            let Some(phase) = RelPtr::<SharedPhase>::from_offset(phase_offset)
                .as_ref(index.shared_arena().mmap_base())
            else {
                // SAFETY:
                // child exits immediately without unwinding.
                unsafe { libc::_exit(101) };
            };

            phase.ready.fetch_add(1, Ordering::AcqRel);

            match role {
                WorkerRole::Insert => {
                    while phase.phase.load(Ordering::Acquire) < 1 {
                        std::hint::spin_loop();
                    }
                    for row_id in 0..ROW_IDS {
                        index.insert(IndexValue::String(SHARED_KEY.to_string()), row_id);
                    }
                    phase.insert_done.fetch_add(1, Ordering::AcqRel);
                }
                WorkerRole::Remove => {
                    while phase.phase.load(Ordering::Acquire) < 2 {
                        std::hint::spin_loop();
                    }
                    for row_id in 0..ROW_IDS {
                        if row_id % 2 == 1 {
                            index.remove(&IndexValue::String(SHARED_KEY.to_string()), &row_id);
                        }
                    }
                    phase.remove_done.fetch_add(1, Ordering::AcqRel);
                }
            }

            if phase.phase.load(Ordering::Acquire) == 0 {
                exit_code = 102;
            }

            // SAFETY:
            // child exits immediately without unwinding.
            unsafe { libc::_exit(exit_code) };
        }
        rustix::runtime::Fork::Parent(pid) => pid,
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
        "child worker failed: {:?}",
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
