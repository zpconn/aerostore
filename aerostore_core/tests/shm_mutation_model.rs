//! Bounded models of the serialized index protocol, using the production lock.
//!
//! Run with:
//! RUSTFLAGS="--cfg aerostore_loom" cargo test -p aerostore_core --test shm_mutation_model --release
//!
//! The graph/allocator below is an abstraction, not a proof of mmap or of every
//! skiplist level. Real skiplist regression and multiprocess tests accompany it.
#![cfg(aerostore_loom)]

#[path = "../src/shm_lock.rs"]
mod shm_lock;

use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use shm_lock::ShmMutex;

fn check_model(f: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    // Explicit finite scheduling scope, without a time/permutation cutoff that
    // could silently stop before a counterexample. Covers the known two-switch
    // predecessor and commit/index reorder schedules.
    model.preemption_bound = Some(2);
    model.max_branches = 10_000;
    model.check(f);
}

const NONE: usize = usize::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ownership {
    Free,
    Owned,
    Live,
    Retired,
}

struct Graph {
    head: usize,
    next: [usize; 3],
    ownership: [Ownership; 3],
}

impl Graph {
    fn seeded() -> Self {
        Self {
            head: 0,
            next: [NONE; 3],
            ownership: [Ownership::Live, Ownership::Free, Ownership::Free],
        }
    }

    fn check(&self) {
        let mut seen = [false; 3];
        let mut cursor = self.head;
        while cursor != NONE {
            assert!(cursor < seen.len());
            assert!(!seen[cursor], "cycle in live index");
            assert_eq!(self.ownership[cursor], Ownership::Live);
            seen[cursor] = true;
            cursor = self.next[cursor];
        }
        for (i, state) in self.ownership.iter().enumerate() {
            assert_eq!(
                seen[i],
                *state == Ownership::Live,
                "unreachable live allocation"
            );
            assert_ne!(
                *state,
                Ownership::Owned,
                "operation leaked a partial allocation"
            );
        }
    }
}

struct Index {
    lock: ShmMutex,
    graph: UnsafeCell<Graph>,
}

#[test]
fn insertion_deletion_reader_and_gc_preserve_reachability_and_ownership() {
    // Two workers interleave insertion, deletion, observation and collection.
    // Collection reacquires the production priority lock after deletion.
    check_model(|| {
        let index = Arc::new(Index {
            lock: ShmMutex::new(),
            graph: UnsafeCell::new(Graph::seeded()),
        });
        let inserter = {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                let _guard = index.lock.lock();
                index.graph.with_mut(|ptr| unsafe {
                    let graph = &mut *ptr;
                    graph.ownership[1] = Ownership::Owned;
                    // The predecessor search and link publication share the
                    // production mutation lock, as in the repaired skiplist.
                    let predecessor = graph.head;
                    thread::yield_now();
                    if predecessor == NONE {
                        graph.head = 1;
                    } else {
                        graph.next[predecessor] = 1;
                    }
                    graph.ownership[1] = Ownership::Live;
                    graph.check();
                });
            })
        };
        let deleter = {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                let _guard = index.lock.lock();
                index.graph.with_mut(|ptr| unsafe {
                    let graph = &mut *ptr;
                    assert_eq!(graph.head, 0);
                    graph.head = graph.next[0];
                    graph.ownership[0] = Ownership::Retired;
                    graph.check();
                });
                drop(_guard);
                thread::yield_now();
                let _collector = index.lock.lock_priority();
                index.graph.with_mut(|ptr| unsafe {
                    let graph = &mut *ptr;
                    graph.check();
                    for state in &mut graph.ownership {
                        if *state == Ownership::Retired {
                            *state = Ownership::Free;
                        }
                    }
                    graph.check();
                });
            })
        };
        inserter.join().unwrap();
        deleter.join().unwrap();
        index.graph.with(|ptr| unsafe {
            let graph = &*ptr;
            graph.check();
            assert_eq!(graph.head, 1, "successful insert was lost");
        });
    });
}

#[test]
fn row_guard_preserves_commit_and_index_order() {
    check_model(|| {
        let row_guard = Arc::new(ShmMutex::new());
        let index_guard = Arc::new(ShmMutex::new());
        let row = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(UnsafeCell::new(vec![0]));
        let mut writers = Vec::new();
        for _ in 0..2 {
            let row_guard = Arc::clone(&row_guard);
            let index_guard = Arc::clone(&index_guard);
            let row = Arc::clone(&row);
            let index = Arc::clone(&index);
            writers.push(thread::spawn(move || {
                // Stable row ownership starts BEFORE the transaction snapshot
                // and ends AFTER publication of its complete index delta.
                let _row_guard = row_guard.lock();
                let old = row.load(Ordering::Acquire);
                row.store(old + 1, Ordering::Release);
                thread::yield_now();
                let _index_guard = index_guard.lock();
                index.with_mut(|ptr| unsafe {
                    let entries = &mut *ptr;
                    entries.push(old + 1);
                    entries.retain(|value| *value != old);
                });
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }
        index.with(|ptr| unsafe {
            assert_eq!(&*ptr, &[row.load(Ordering::Acquire)]);
        });
    });
}

#[test]
fn failed_prepublication_allocations_are_returned_before_unlock() {
    for successful_allocations_before_failure in 0..=2 {
        check_model(move || {
            let index = Arc::new(Index {
                lock: ShmMutex::new(),
                graph: UnsafeCell::new(Graph::seeded()),
            });
            let failed_move = {
                let index = Arc::clone(&index);
                thread::spawn(move || {
                    let _guard = index.lock.lock();
                    index.graph.with_mut(|ptr| unsafe {
                        let graph = &mut *ptr;
                        for slot in 1..=successful_allocations_before_failure {
                            graph.ownership[slot] = Ownership::Owned;
                            thread::yield_now();
                        }
                        // Publication/removal of the source has not occurred.
                        for slot in 1..=successful_allocations_before_failure {
                            graph.ownership[slot] = Ownership::Free;
                        }
                        graph.check();
                    });
                })
            };
            let reader = {
                let index = Arc::clone(&index);
                thread::spawn(move || {
                    let _guard = index.lock.lock();
                    index.graph.with(|ptr| unsafe {
                        (&*ptr).check();
                        assert_eq!((*ptr).head, 0);
                    });
                })
            };
            failed_move.join().unwrap();
            reader.join().unwrap();
        });
    }
}

#[test]
#[should_panic(expected = "detached predecessor loses insertion")]
fn model_detects_the_original_unprotected_predecessor_race() {
    check_model(|| {
        let head = Arc::new(AtomicUsize::new(0));
        let predecessor_next = Arc::new(AtomicUsize::new(NONE));
        let inserter = {
            let head = Arc::clone(&head);
            let next = Arc::clone(&predecessor_next);
            thread::spawn(move || {
                let predecessor = head.load(Ordering::Acquire);
                if predecessor == NONE {
                    head.store(1, Ordering::Release);
                } else {
                    // This can succeed after predecessor 0 has been unlinked.
                    next.compare_exchange(NONE, 1, Ordering::AcqRel, Ordering::Acquire)
                        .unwrap();
                }
            })
        };
        let deleter = {
            let head = Arc::clone(&head);
            let next = Arc::clone(&predecessor_next);
            thread::spawn(move || {
                let successor = next.load(Ordering::Acquire);
                let _ = head.compare_exchange(0, successor, Ordering::AcqRel, Ordering::Acquire);
            })
        };
        inserter.join().unwrap();
        deleter.join().unwrap();
        assert_eq!(
            head.load(Ordering::Acquire),
            1,
            "detached predecessor loses insertion"
        );
    });
}

#[test]
#[should_panic(expected = "row and index publication reordered")]
fn model_detects_missing_row_guard_even_with_serialized_index_moves() {
    check_model(|| {
        let row = Arc::new(AtomicUsize::new(0));
        let entries = Arc::new(AtomicUsize::new(1)); // bit 0: initial timestamp
        let index_lock = Arc::new(ShmMutex::new());
        let mut writers = Vec::new();
        for _ in 0..2 {
            let row = Arc::clone(&row);
            let entries = Arc::clone(&entries);
            let index_lock = Arc::clone(&index_lock);
            writers.push(thread::spawn(move || {
                let old = row.fetch_add(1, Ordering::AcqRel);
                // Missing stable row guard permits the other commit's index
                // move to complete before this one.
                let _index_guard = index_lock.lock();
                let bits = entries.load(Ordering::Acquire);
                entries.store((bits & !(1 << old)) | (1 << (old + 1)), Ordering::Release);
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(
            entries.load(Ordering::Acquire),
            1 << row.load(Ordering::Acquire),
            "row and index publication reordered"
        );
    });
}
