#![cfg(unix)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexCompare, IndexValue, RelPtr, SecondaryIndex, ShmArena};

const CHURN_KEY: &str = "CHURN_KEY";

#[repr(C, align(64))]
struct SharedChurnState {
    ready: AtomicU32,
    epoch: AtomicU32,
    done: AtomicU32,
    stop: AtomicU32,
}

impl SharedChurnState {
    fn new() -> Self {
        Self {
            ready: AtomicU32::new(0),
            epoch: AtomicU32::new(0),
            done: AtomicU32::new(0),
            stop: AtomicU32::new(0),
        }
    }
}

#[test]
fn randomized_phased_churn_final_set_is_exact() {
    const WORKERS: u32 = 4;
    const ROWS_PER_ROUND: u32 = 256;
    const ROUNDS: u32 = 6;

    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let state_ptr = shm
        .chunked_arena()
        .alloc(SharedChurnState::new())
        .expect("failed to allocate shared churn state");
    let state_offset = state_ptr.load(Ordering::Acquire);

    let mut pids = Vec::with_capacity(WORKERS as usize);
    for worker_id in 0..WORKERS {
        pids.push(spawn_randomized_worker(
            index.clone(),
            state_offset,
            worker_id,
            ROWS_PER_ROUND,
        ));
    }

    let state = state_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve churn state");
    wait_until(
        || state.ready.load(Ordering::Acquire) == WORKERS,
        Duration::from_secs(5),
        "workers did not reach randomized churn barrier",
    );

    let mut expected = BTreeSet::new();
    for round in 0..ROUNDS {
        for worker_id in 0..WORKERS {
            for local_row in 0..ROWS_PER_ROUND {
                let row_id = round * ROWS_PER_ROUND + local_row;
                if insert_pred(worker_id, round, local_row) {
                    expected.insert(row_id);
                }
            }
        }
        for local_row in 0..ROWS_PER_ROUND {
            let row_id = round * ROWS_PER_ROUND + local_row;
            if remove_pred(round, local_row) {
                expected.remove(&row_id);
            }
        }
        for local_row in 0..ROWS_PER_ROUND {
            let row_id = round * ROWS_PER_ROUND + local_row;
            if reinsert_pred(round, local_row) {
                expected.insert(row_id);
            }
        }
    }

    let total_epochs = ROUNDS * 3;
    for epoch in 1..=total_epochs {
        state.done.store(0, Ordering::Release);
        state.epoch.store(epoch, Ordering::Release);
        wait_until(
            || state.done.load(Ordering::Acquire) == WORKERS,
            Duration::from_secs(20),
            "randomized churn workers did not finish epoch",
        );
    }

    state.stop.store(1, Ordering::Release);
    state.epoch.store(total_epochs + 1, Ordering::Release);

    for pid in pids {
        wait_for_child(pid);
    }

    let hits = index.lookup(&IndexCompare::Eq(IndexValue::String(CHURN_KEY.to_string())));
    let observed: BTreeSet<u32> = hits.into_iter().collect();
    assert_eq!(
        observed, expected,
        "randomized phased churn produced incorrect final posting set"
    );
}

#[test]
fn hot_duplicate_churn_insert_remove_reinsert_is_exact() {
    const WORKERS: u32 = 4;
    const ROW_IDS: u32 = 512;

    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let state_ptr = shm
        .chunked_arena()
        .alloc(SharedChurnState::new())
        .expect("failed to allocate shared churn state");
    let state_offset = state_ptr.load(Ordering::Acquire);

    let mut pids = Vec::with_capacity(WORKERS as usize);
    for _ in 0..WORKERS {
        pids.push(spawn_hot_duplicate_worker(
            index.clone(),
            state_offset,
            ROW_IDS,
        ));
    }

    let state = state_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve churn state");
    wait_until(
        || state.ready.load(Ordering::Acquire) == WORKERS,
        Duration::from_secs(5),
        "workers did not reach hot-duplicate barrier",
    );

    // Epoch 1: all workers insert all row IDs (heavy duplicate posting contention).
    state.done.store(0, Ordering::Release);
    state.epoch.store(1, Ordering::Release);
    wait_until(
        || state.done.load(Ordering::Acquire) == WORKERS,
        Duration::from_secs(20),
        "hot-duplicate workers did not finish insert epoch",
    );

    // Epoch 2: all workers remove odd row IDs.
    state.done.store(0, Ordering::Release);
    state.epoch.store(2, Ordering::Release);
    wait_until(
        || state.done.load(Ordering::Acquire) == WORKERS,
        Duration::from_secs(20),
        "hot-duplicate workers did not finish remove epoch",
    );

    // Epoch 3: all workers reinsert ids divisible by 5.
    state.done.store(0, Ordering::Release);
    state.epoch.store(3, Ordering::Release);
    wait_until(
        || state.done.load(Ordering::Acquire) == WORKERS,
        Duration::from_secs(20),
        "hot-duplicate workers did not finish reinsert epoch",
    );

    state.stop.store(1, Ordering::Release);
    state.epoch.store(4, Ordering::Release);

    for pid in pids {
        wait_for_child(pid);
    }

    let expected: BTreeSet<u32> = (0..ROW_IDS)
        .filter(|row_id| row_id % 2 == 0 || row_id % 5 == 0)
        .collect();
    let hits = index.lookup(&IndexCompare::Eq(IndexValue::String(CHURN_KEY.to_string())));
    let observed: BTreeSet<u32> = hits.into_iter().collect();
    assert_eq!(
        observed, expected,
        "hot duplicate churn produced incorrect final posting set"
    );
}

fn spawn_randomized_worker(
    index: SecondaryIndex<u32>,
    state_offset: u32,
    worker_id: u32,
    rows_per_round: u32,
) -> rustix::process::Pid {
    // SAFETY:
    // fork is used to validate cross-process shared-memory index behavior.
    let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed");
    match fork_result {
        rustix::runtime::Fork::Child(_) => {
            let Some(state) = RelPtr::<SharedChurnState>::from_offset(state_offset)
                .as_ref(index.shared_arena().mmap_base())
            else {
                unsafe { libc::_exit(101) };
            };

            state.ready.fetch_add(1, Ordering::AcqRel);
            let mut observed_epoch = 0_u32;
            loop {
                let current_epoch = state.epoch.load(Ordering::Acquire);
                if state.stop.load(Ordering::Acquire) != 0 {
                    break;
                }
                if current_epoch == 0 || current_epoch == observed_epoch {
                    std::hint::spin_loop();
                    continue;
                }

                let phase = (current_epoch - 1) % 3;
                let round = (current_epoch - 1) / 3;
                match phase {
                    0 => {
                        for local_row in 0..rows_per_round {
                            if insert_pred(worker_id, round, local_row) {
                                let row_id = round * rows_per_round + local_row;
                                index.insert(IndexValue::String(CHURN_KEY.to_string()), row_id);
                            }
                        }
                    }
                    1 => {
                        for local_row in 0..rows_per_round {
                            if remove_pred(round, local_row) {
                                let row_id = round * rows_per_round + local_row;
                                index.remove(&IndexValue::String(CHURN_KEY.to_string()), &row_id);
                            }
                        }
                    }
                    _ => {
                        for local_row in 0..rows_per_round {
                            if reinsert_pred(round, local_row) {
                                let row_id = round * rows_per_round + local_row;
                                index.insert(IndexValue::String(CHURN_KEY.to_string()), row_id);
                            }
                        }
                    }
                }

                state.done.fetch_add(1, Ordering::AcqRel);
                observed_epoch = current_epoch;
            }

            unsafe { libc::_exit(0) };
        }
        rustix::runtime::Fork::Parent(pid) => pid,
    }
}

fn spawn_hot_duplicate_worker(
    index: SecondaryIndex<u32>,
    state_offset: u32,
    row_ids: u32,
) -> rustix::process::Pid {
    // SAFETY:
    // fork is used to validate cross-process shared-memory index behavior.
    let fork_result = unsafe { rustix::runtime::fork() }.expect("fork failed");
    match fork_result {
        rustix::runtime::Fork::Child(_) => {
            let Some(state) = RelPtr::<SharedChurnState>::from_offset(state_offset)
                .as_ref(index.shared_arena().mmap_base())
            else {
                unsafe { libc::_exit(111) };
            };

            state.ready.fetch_add(1, Ordering::AcqRel);
            let mut observed_epoch = 0_u32;
            loop {
                let current_epoch = state.epoch.load(Ordering::Acquire);
                if state.stop.load(Ordering::Acquire) != 0 {
                    break;
                }
                if current_epoch == 0 || current_epoch == observed_epoch {
                    std::hint::spin_loop();
                    continue;
                }

                match current_epoch {
                    1 => {
                        for row_id in 0..row_ids {
                            index.insert(IndexValue::String(CHURN_KEY.to_string()), row_id);
                        }
                    }
                    2 => {
                        for row_id in 0..row_ids {
                            if row_id % 2 == 1 {
                                index.remove(&IndexValue::String(CHURN_KEY.to_string()), &row_id);
                            }
                        }
                    }
                    _ => {
                        for row_id in 0..row_ids {
                            if row_id % 5 == 0 {
                                index.insert(IndexValue::String(CHURN_KEY.to_string()), row_id);
                            }
                        }
                    }
                }

                state.done.fetch_add(1, Ordering::AcqRel);
                observed_epoch = current_epoch;
            }

            unsafe { libc::_exit(0) };
        }
        rustix::runtime::Fork::Parent(pid) => pid,
    }
}

#[inline]
fn insert_pred(worker_id: u32, round: u32, row_id: u32) -> bool {
    (row_id + round) % 4 == worker_id
}

#[inline]
fn remove_pred(round: u32, row_id: u32) -> bool {
    (row_id + round.wrapping_mul(7)) % 5 == 0
}

#[inline]
fn reinsert_pred(round: u32, row_id: u32) -> bool {
    (row_id.wrapping_mul(3) + round.wrapping_mul(5)) % 13 == 0
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
