#![cfg(unix)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexCompare, IndexValue, RelPtr, SecondaryIndex, ShmArena};

const SHARED_KEY: &str = "UAL123";
const ROW_IDS: u32 = 2_000;
const INSERT_WORKERS: u32 = 2;
const REMOVE_WORKERS: u32 = 2;
const READER_WORKERS: u32 = 3;
const STRESS_WORKERS: u32 = 8;
const STRESS_KEYS: u32 = 96;

#[repr(C, align(64))]
struct SharedPhase {
    ready: AtomicU32,
    phase: AtomicU32,
    insert_done: AtomicU32,
    remove_done: AtomicU32,
    stop_readers: AtomicU32,
    reader_done: AtomicU32,
    reader_samples: AtomicU64,
    reader_bad_hits: AtomicU64,
}

#[repr(C, align(64))]
struct StartGate {
    ready: AtomicU32,
    start: AtomicU32,
}

#[derive(Clone, Copy)]
enum WorkerRole {
    Insert,
    Remove,
    Reader,
}

#[test]
fn cross_process_same_key_insert_remove_contention_with_duplicates_is_exact() {
    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let phase_ptr = shm
        .chunked_arena()
        .alloc(SharedPhase {
            ready: AtomicU32::new(0),
            phase: AtomicU32::new(0),
            insert_done: AtomicU32::new(0),
            remove_done: AtomicU32::new(0),
            stop_readers: AtomicU32::new(0),
            reader_done: AtomicU32::new(0),
            reader_samples: AtomicU64::new(0),
            reader_bad_hits: AtomicU64::new(0),
        })
        .expect("failed to allocate shared phase state");
    let phase_offset = phase_ptr.load(Ordering::Acquire);

    let mut pids = Vec::new();
    for _ in 0..INSERT_WORKERS {
        pids.push(spawn_worker(
            index.clone(),
            phase_offset,
            WorkerRole::Insert,
        ));
    }
    for _ in 0..REMOVE_WORKERS {
        pids.push(spawn_worker(
            index.clone(),
            phase_offset,
            WorkerRole::Remove,
        ));
    }
    for _ in 0..READER_WORKERS {
        pids.push(spawn_worker(
            index.clone(),
            phase_offset,
            WorkerRole::Reader,
        ));
    }

    let phase = phase_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve shared phase state");
    let total_workers = INSERT_WORKERS + REMOVE_WORKERS + READER_WORKERS;
    wait_until(
        || phase.ready.load(Ordering::Acquire) == total_workers,
        Duration::from_secs(5),
        "workers did not reach ready barrier in time",
    );

    // Phase 1: two inserters concurrently publish duplicate postings for same key.
    phase.phase.store(1, Ordering::Release);
    wait_until(
        || phase.insert_done.load(Ordering::Acquire) == INSERT_WORKERS,
        Duration::from_secs(15),
        "insert workers did not finish in time",
    );

    // Phase 2: removers concurrently remove odd row IDs while readers keep scanning.
    phase.phase.store(2, Ordering::Release);
    wait_until(
        || phase.remove_done.load(Ordering::Acquire) == REMOVE_WORKERS,
        Duration::from_secs(15),
        "remove workers did not finish in time",
    );

    std::thread::sleep(Duration::from_millis(100));
    phase.stop_readers.store(1, Ordering::Release);
    wait_until(
        || phase.reader_done.load(Ordering::Acquire) == READER_WORKERS,
        Duration::from_secs(5),
        "reader workers did not finish in time",
    );

    for pid in pids {
        wait_for_child(pid);
    }

    let reader_samples = phase.reader_samples.load(Ordering::Acquire);
    assert!(
        reader_samples >= 100,
        "reader workers collected too few lookup samples under contention: {}",
        reader_samples
    );
    assert_eq!(
        phase.reader_bad_hits.load(Ordering::Acquire),
        0,
        "readers observed out-of-domain row IDs during contention"
    );

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

#[test]
fn cross_process_distinct_key_count_matches_live_keyspace_after_stress() {
    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let gate_ptr = shm
        .chunked_arena()
        .alloc(StartGate {
            ready: AtomicU32::new(0),
            start: AtomicU32::new(0),
        })
        .expect("failed to allocate stress start gate");
    let gate_offset = gate_ptr.load(Ordering::Acquire);

    let mut pids = Vec::new();
    for worker_id in 0..STRESS_WORKERS {
        pids.push(spawn_stress_worker(index.clone(), gate_offset, worker_id));
    }

    let gate = gate_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve stress start gate");
    wait_until(
        || gate.ready.load(Ordering::Acquire) == STRESS_WORKERS,
        Duration::from_secs(5),
        "stress workers did not reach ready barrier in time",
    );
    gate.start.store(1, Ordering::Release);

    for pid in pids {
        wait_for_child(pid);
    }

    let expected_keys: BTreeSet<String> = (0..STRESS_KEYS).map(stress_key).collect();
    assert_eq!(
        index.distinct_key_count(),
        expected_keys.len(),
        "distinct key count should match deterministic live key set after stress"
    );

    let entries = index.traverse();
    assert_eq!(
        entries.len(),
        expected_keys.len(),
        "traversal cardinality mismatch after stress"
    );

    for (value, row_ids) in entries {
        let key = match value {
            IndexValue::String(key) => key,
            other => panic!("expected string key in stress traversal, got {:?}", other),
        };
        assert!(
            expected_keys.contains(&key),
            "unexpected key {} remained live after stress",
            key
        );

        let key_id = parse_stress_key(&key);
        let observed_rows: BTreeSet<u32> = row_ids.into_iter().collect();
        assert_eq!(
            observed_rows.is_empty(),
            false,
            "each retained key should have at least one live posting"
        );

        let mut has_even_worker = false;
        for row_id in observed_rows {
            assert!(
                row_id < STRESS_WORKERS * STRESS_KEYS,
                "row_id {} exceeded stress-domain bounds",
                row_id
            );
            assert_eq!(
                row_id % STRESS_KEYS,
                key_id,
                "row_id {} does not map to key {}",
                row_id,
                key
            );
            let worker_id = row_id / STRESS_KEYS;
            if worker_id % 2 == 0 {
                has_even_worker = true;
            }
        }
        assert!(
            has_even_worker,
            "key {} should retain at least one non-remover worker posting",
            key
        );
    }
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
                WorkerRole::Reader => {
                    while phase.phase.load(Ordering::Acquire) < 1 {
                        std::hint::spin_loop();
                    }
                    while phase.stop_readers.load(Ordering::Acquire) == 0 {
                        let hits = index.lookup(&IndexCompare::Eq(IndexValue::String(
                            SHARED_KEY.to_string(),
                        )));
                        phase.reader_samples.fetch_add(1, Ordering::AcqRel);
                        for row_id in hits {
                            if row_id >= ROW_IDS {
                                phase.reader_bad_hits.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                    }
                    phase.reader_done.fetch_add(1, Ordering::AcqRel);
                }
            }

            // SAFETY:
            // child exits immediately without unwinding.
            unsafe { libc::_exit(0) };
        }
        rustix::runtime::Fork::Parent(pid) => pid,
    }
}

fn spawn_stress_worker(
    index: SecondaryIndex<u32>,
    gate_offset: u32,
    worker_id: u32,
) -> rustix::process::Pid {
    // SAFETY:
    // fork is used to validate cross-process shared-memory index behavior.
    let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed");
    match fork_result {
        rustix::runtime::Fork::Child(_) => {
            let Some(gate) = RelPtr::<StartGate>::from_offset(gate_offset)
                .as_ref(index.shared_arena().mmap_base())
            else {
                // SAFETY:
                // child exits immediately without unwinding.
                unsafe { libc::_exit(111) };
            };

            gate.ready.fetch_add(1, Ordering::AcqRel);
            while gate.start.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }

            for key_id in 0..STRESS_KEYS {
                let key = stress_key(key_id);
                let row_id = worker_id * STRESS_KEYS + key_id;
                index.insert(IndexValue::String(key.clone()), row_id);
                if worker_id % 2 == 1 {
                    index.remove(&IndexValue::String(key), &row_id);
                }
            }

            // SAFETY:
            // child exits immediately without unwinding.
            unsafe { libc::_exit(0) };
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

fn stress_key(key_id: u32) -> String {
    format!("FLT{:03}", key_id)
}

fn parse_stress_key(key: &str) -> u32 {
    key.strip_prefix("FLT")
        .expect("stress key missing FLT prefix")
        .parse::<u32>()
        .expect("stress key suffix was not numeric")
}
