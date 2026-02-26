use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use aerostore_core::{MvccTable, TransactionManager};
use crossbeam::epoch;

#[test]
fn loom_validates_cas_update_and_index_pointer_swaps() {
    use loom::sync::atomic::AtomicI64;
    use loom::sync::atomic::AtomicUsize as LoomAtomicUsize;
    use loom::sync::atomic::Ordering::{AcqRel, Acquire, Release};
    use loom::sync::Arc as LoomArc;
    use loom::thread as loom_thread;

    const NONE: usize = usize::MAX;

    struct Version {
        altitude: AtomicI64,
        xmax: LoomAtomicUsize,
        next: LoomAtomicUsize,
    }

    impl Version {
        fn new(altitude: i64, next: usize) -> Self {
            Self {
                altitude: AtomicI64::new(altitude),
                xmax: LoomAtomicUsize::new(0),
                next: LoomAtomicUsize::new(next),
            }
        }
    }

    let mut builder = loom::model::Builder::new();
    builder.max_branches = 512;
    builder.preemption_bound = Some(2);
    builder.max_permutations = Some(2_000);
    builder.max_duration = Some(Duration::from_secs(20));

    builder.check(|| {
        let versions = LoomArc::new(vec![
            Version::new(30_000, NONE),
            Version::new(0, NONE),
            Version::new(0, NONE),
        ]);
        let head = LoomArc::new(LoomAtomicUsize::new(0));
        let index_head = LoomArc::new(LoomAtomicUsize::new(0));

        let writer1 = {
            let versions = LoomArc::clone(&versions);
            let head = LoomArc::clone(&head);
            let index_head = LoomArc::clone(&index_head);

            loom_thread::spawn(move || {
                let slot = 1_usize;
                let txid = 11_usize;
                let mut attempts = 0_usize;

                loop {
                    attempts += 1;
                    assert!(attempts <= 8, "writer 1 made no progress");

                    let current = head.load(Acquire);
                    if current == slot {
                        return;
                    }

                    if versions[current]
                        .xmax
                        .compare_exchange(0, txid, AcqRel, Acquire)
                        .is_err()
                    {
                        loom_thread::yield_now();
                        continue;
                    }

                    versions[slot].altitude.store(31_000, Release);
                    versions[slot].next.store(current, Release);
                    versions[slot].xmax.store(0, Release);

                    if head
                        .compare_exchange(current, slot, AcqRel, Acquire)
                        .is_ok()
                    {
                        index_head.store(slot, Release);
                        return;
                    }

                    let _ = versions[current]
                        .xmax
                        .compare_exchange(txid, 0, AcqRel, Acquire);
                    loom_thread::yield_now();
                }
            })
        };

        let writer2 = {
            let versions = LoomArc::clone(&versions);
            let head = LoomArc::clone(&head);
            let index_head = LoomArc::clone(&index_head);

            loom_thread::spawn(move || {
                let slot = 2_usize;
                let txid = 22_usize;
                let mut attempts = 0_usize;

                loop {
                    attempts += 1;
                    assert!(attempts <= 8, "writer 2 made no progress");

                    let current = head.load(Acquire);
                    if current == slot {
                        return;
                    }

                    if versions[current]
                        .xmax
                        .compare_exchange(0, txid, AcqRel, Acquire)
                        .is_err()
                    {
                        loom_thread::yield_now();
                        continue;
                    }

                    versions[slot].altitude.store(32_000, Release);
                    versions[slot].next.store(current, Release);
                    versions[slot].xmax.store(0, Release);

                    if head
                        .compare_exchange(current, slot, AcqRel, Acquire)
                        .is_ok()
                    {
                        index_head.store(slot, Release);
                        return;
                    }

                    let _ = versions[current]
                        .xmax
                        .compare_exchange(txid, 0, AcqRel, Acquire);
                    loom_thread::yield_now();
                }
            })
        };

        let observer = {
            let versions = LoomArc::clone(&versions);
            let index_head = LoomArc::clone(&index_head);

            loom_thread::spawn(move || {
                for _ in 0..3 {
                    let idx = index_head.load(Acquire);
                    if idx != 0 {
                        let next = versions[idx].next.load(Acquire);
                        assert_ne!(
                            next, NONE,
                            "index head pointed to an incompletely linked MVCC node"
                        );
                        let alt = versions[idx].altitude.load(Acquire);
                        assert!(
                            alt >= 31_000,
                            "index published before the writer published row payload"
                        );
                    }
                    loom_thread::yield_now();
                }
            })
        };

        writer1.join().expect("writer 1 panicked");
        writer2.join().expect("writer 2 panicked");
        observer.join().expect("observer panicked");

        let final_head = head.load(Acquire);
        assert!(
            final_head == 1 || final_head == 2,
            "expected one of two writer nodes to be head, got {}",
            final_head
        );

        let final_index_head = index_head.load(Acquire);
        assert!(
            final_index_head == 0 || final_index_head == 1 || final_index_head == 2,
            "index head escaped modelled slots"
        );

        let mut seen = [false; 3];
        let mut cursor = final_head;
        for _ in 0..3 {
            assert!(cursor < 3, "cursor escaped modelled nodes");
            assert!(!seen[cursor], "ABA/cycle detected in version chain");
            seen[cursor] = true;

            if cursor == 0 {
                break;
            }
            cursor = versions[cursor].next.load(Acquire);
        }

        assert!(
            seen.iter().all(|v| *v),
            "all versions must remain reachable in linearized order"
        );
        assert!(
            seen[final_index_head],
            "index head must always point to a node reachable from the committed MVCC chain"
        );

        for (slot, seen_flag) in seen.into_iter().enumerate() {
            assert!(seen_flag, "slot {} was not reachable", slot);
            let xmax = versions[slot].xmax.load(Acquire);
            if slot == final_head {
                assert_eq!(xmax, 0, "head must remain open for readers");
            } else {
                assert!(
                    xmax == 11 || xmax == 22,
                    "non-head slot {} must be closed by one writer, got xmax={}",
                    slot,
                    xmax
                );
            }
        }
    });
}

#[derive(Debug, Clone)]
struct TrackerCounters {
    allocated: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl TrackerCounters {
    fn new() -> Self {
        Self {
            allocated: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn note_allocation(&self) {
        self.allocated.fetch_add(1, Ordering::AcqRel);
    }

    fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Acquire)
    }

    fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct DropTracker {
    counters: TrackerCounters,
}

impl Drop for DropTracker {
    fn drop(&mut self) {
        let allocated = self.counters.allocated();
        let observed = self.counters.dropped.fetch_add(1, Ordering::AcqRel) + 1;
        assert!(
            observed <= allocated,
            "drop counter exceeded allocations (drops={}, allocations={})",
            observed,
            allocated
        );
    }
}

#[derive(Clone, Debug)]
struct FlightState {
    altitude: i32,
    tracker: Arc<DropTracker>,
}

fn tracked_flight_state(altitude: i32, counters: &TrackerCounters) -> FlightState {
    counters.note_allocation();
    FlightState {
        altitude,
        tracker: Arc::new(DropTracker {
            counters: counters.clone(),
        }),
    }
}

fn flush_epoch() {
    let guard = epoch::pin();
    guard.flush();
}

fn pump_epoch(rounds: usize) {
    for _ in 0..rounds {
        let guard = epoch::pin();
        unsafe {
            let marker = epoch::Owned::new(0_u8).into_shared(&guard);
            guard.defer_destroy(marker);
        }
        guard.flush();
        drop(guard);
        thread::yield_now();
    }
}

fn wait_for_drops(counters: &TrackerCounters, expected: usize, timeout: Duration, message: &str) {
    let start = Instant::now();
    while counters.dropped() < expected && start.elapsed() < timeout {
        flush_epoch();
        pump_epoch(32);
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        counters.dropped(),
        expected,
        "{} (expected {}, observed {})",
        message,
        expected,
        counters.dropped()
    );
}

#[test]
#[ignore = "long-running stress test; run with --ignored --release"]
fn hyperfeed_gc_stress_reclaims_dead_versions_without_memory_leaks() {
    const FLIGHT_KEY: u64 = 42;
    const UPDATE_COUNT: usize = 100_000;
    const READER_THREADS: usize = 4;
    const BASE_ALTITUDE: i32 = 30_000;

    let bootstrap_counters = TrackerCounters::new();
    let update_counters = TrackerCounters::new();
    let worker_bootstrap = bootstrap_counters.clone();
    let worker_updates = update_counters.clone();
    let worker = thread::spawn(move || {
        let tx_manager = Arc::new(TransactionManager::new());
        let table = Arc::new(MvccTable::<u64, FlightState>::new(4_096));

        let bootstrap_tx = tx_manager.begin();
        table
            .insert(
                FLIGHT_KEY,
                tracked_flight_state(BASE_ALTITUDE, &worker_bootstrap),
                &bootstrap_tx,
            )
            .expect("bootstrap insert must succeed");
        let reclaimed = table.commit(&tx_manager, &bootstrap_tx);
        assert_eq!(reclaimed, 0);

        let barrier = Arc::new(Barrier::new(READER_THREADS + 1));
        let stop_readers = Arc::new(AtomicBool::new(false));

        let mut readers = Vec::with_capacity(READER_THREADS);
        for _ in 0..READER_THREADS {
            let tx_manager = Arc::clone(&tx_manager);
            let table = Arc::clone(&table);
            let barrier = Arc::clone(&barrier);
            let stop_readers = Arc::clone(&stop_readers);

            readers.push(thread::spawn(move || {
                let snapshot_tx = tx_manager.begin();
                barrier.wait();

                while !stop_readers.load(Ordering::Acquire) {
                    let row = table
                        .read_visible(&FLIGHT_KEY, &snapshot_tx)
                        .expect("reader snapshot should always see the original row");
                    assert_eq!(row.altitude, BASE_ALTITUDE);
                    let _strong_count = Arc::strong_count(&row.tracker);
                    thread::sleep(Duration::from_micros(25));
                }

                tx_manager.abort(&snapshot_tx);
            }));
        }

        let writer = {
            let tx_manager = Arc::clone(&tx_manager);
            let table = Arc::clone(&table);
            let barrier = Arc::clone(&barrier);
            let worker_updates = worker_updates.clone();

            thread::spawn(move || {
                barrier.wait();
                let mut reclaimed_total = 0_usize;

                for step in 1..=UPDATE_COUNT {
                    let tx = tx_manager.begin();
                    table
                        .update(
                            &FLIGHT_KEY,
                            tracked_flight_state(BASE_ALTITUDE + step as i32, &worker_updates),
                            &tx,
                        )
                        .expect("writer update must succeed");
                    reclaimed_total += table.commit(&tx_manager, &tx);

                    if step % 8_192 == 0 {
                        thread::yield_now();
                    }
                }

                reclaimed_total
            })
        };

        let reclaimed_during_writes = writer.join().expect("writer thread panicked");
        assert_eq!(
            reclaimed_during_writes, 0,
            "no dead versions should be reclaimable while slow snapshots are still active"
        );

        stop_readers.store(true, Ordering::Release);
        for reader in readers {
            reader.join().expect("reader thread panicked");
        }

        let read_tx = tx_manager.begin();
        let read_guard = epoch::pin();
        let latest = table
            .read_visible_ref_with_guard(&FLIGHT_KEY, &read_tx, &read_guard)
            .expect("latest row should be visible");
        assert_eq!(latest.altitude, BASE_ALTITUDE + UPDATE_COUNT as i32);
        tx_manager.abort(&read_tx);
        drop(read_guard);

        let mut reclaimed_after_readers = 0_usize;
        loop {
            let oldest = tx_manager.oldest_active_snapshot_xmin();
            let reclaimed = table.garbage_collect(oldest);
            reclaimed_after_readers += reclaimed;
            flush_epoch();
            pump_epoch(16);
            if reclaimed == 0 {
                break;
            }
        }

        assert_eq!(
            Arc::strong_count(&table),
            1,
            "table should have a single strong reference before teardown"
        );
        drop(table);
        drop(tx_manager);
        reclaimed_after_readers
    });

    let reclaimed_after_readers = worker.join().expect("gc worker thread panicked");
    let expected_dead_versions = UPDATE_COUNT - 1;
    assert_eq!(
        reclaimed_after_readers, expected_dead_versions,
        "expected exactly 99,999 dead row versions to be reclaimed once readers released snapshots"
    );
    assert_eq!(
        update_counters.allocated(),
        UPDATE_COUNT,
        "writer loop must allocate exactly one row version per update"
    );
    assert_eq!(
        bootstrap_counters.allocated(),
        1,
        "bootstrap phase must allocate exactly one initial row version"
    );
    wait_for_drops(
        &bootstrap_counters,
        1,
        Duration::from_secs(60),
        "bootstrap allocation was leaked",
    );
    wait_for_drops(
        &update_counters,
        expected_dead_versions,
        Duration::from_secs(60),
        "dead update versions were not reclaimed after readers released snapshots",
    );
    assert_eq!(
        bootstrap_counters.dropped(),
        bootstrap_counters.allocated(),
        "bootstrap allocation/drop accounting mismatch"
    );
    assert_eq!(
        update_counters.dropped(),
        expected_dead_versions,
        "expected all 99,999 dead update versions to be dropped"
    );
}
