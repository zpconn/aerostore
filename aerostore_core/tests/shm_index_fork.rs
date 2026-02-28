#![cfg(unix)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{IndexCompare, IndexValue, RelPtr, SecondaryIndex, ShmArena};

#[repr(C)]
struct SharedFlightRow {
    altitude: AtomicU32,
}

#[repr(C, align(64))]
struct ForkLatch {
    ready: AtomicU32,
    go: AtomicU32,
}

#[test]
fn forked_children_insert_into_shared_secondary_index_and_parent_sees_both() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared mmap arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("flight_id", Arc::clone(&shm));
    let arena = shm.chunked_arena();

    let ual_ptr = arena
        .alloc(SharedFlightRow {
            altitude: AtomicU32::new(31_000),
        })
        .expect("failed to allocate row for UAL123");
    let dal_ptr = arena
        .alloc(SharedFlightRow {
            altitude: AtomicU32::new(34_000),
        })
        .expect("failed to allocate row for DAL456");
    let latch_ptr = arena
        .alloc(ForkLatch {
            ready: AtomicU32::new(0),
            go: AtomicU32::new(0),
        })
        .expect("failed to allocate shared fork latch");

    let ual_row_id = ual_ptr.load(Ordering::Acquire);
    let dal_row_id = dal_ptr.load(Ordering::Acquire);

    let child_a = fork_insert_child(
        index.clone(),
        latch_ptr.load(Ordering::Acquire),
        "UAL123",
        ual_row_id,
    );
    let child_b = fork_insert_child(
        index.clone(),
        latch_ptr.load(Ordering::Acquire),
        "DAL456",
        dal_row_id,
    );

    let latch = latch_ptr
        .as_ref(shm.mmap_base())
        .expect("parent could not resolve latch");

    let start = Instant::now();
    while latch.ready.load(Ordering::Acquire) < 2 {
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "children failed to reach ready barrier"
        );
        std::hint::spin_loop();
    }
    latch.go.store(1, Ordering::Release);

    wait_child(child_a);
    wait_child(child_b);

    let all_entries = index.traverse();
    assert!(
        all_entries.iter().any(
            |(key, rows)| *key == IndexValue::String("UAL123".to_string())
                && rows.contains(&ual_row_id)
        ),
        "parent traversal did not include child A insert"
    );
    assert!(
        all_entries.iter().any(
            |(key, rows)| *key == IndexValue::String("DAL456".to_string())
                && rows.contains(&dal_row_id)
        ),
        "parent traversal did not include child B insert"
    );

    let ual_hits = index.lookup(&IndexCompare::Eq(IndexValue::String("UAL123".to_string())));
    let dal_hits = index.lookup(&IndexCompare::Eq(IndexValue::String("DAL456".to_string())));
    assert!(ual_hits.contains(&ual_row_id), "missing UAL123 in lookup");
    assert!(dal_hits.contains(&dal_row_id), "missing DAL456 in lookup");

    let ual_row = RelPtr::<SharedFlightRow>::from_offset(ual_row_id)
        .as_ref(shm.mmap_base())
        .expect("UAL row id should resolve to a valid shared row");
    let dal_row = RelPtr::<SharedFlightRow>::from_offset(dal_row_id)
        .as_ref(shm.mmap_base())
        .expect("DAL row id should resolve to a valid shared row");
    assert_eq!(ual_row.altitude.load(Ordering::Acquire), 31_000);
    assert_eq!(dal_row.altitude.load(Ordering::Acquire), 34_000);
}

fn fork_insert_child(
    index: SecondaryIndex<u32>,
    latch_offset: u32,
    key: &'static str,
    row_id: u32,
) -> rustix::process::Pid {
    // SAFETY:
    // `fork` is used for process-level shared-memory visibility validation.
    let result = unsafe { rustix::runtime::fork() }.expect("fork failed");

    match result {
        rustix::runtime::Fork::Child(_) => {
            let mut exit_code = 0_i32;

            if let Some(latch) = RelPtr::<ForkLatch>::from_offset(latch_offset)
                .as_ref(index.shared_arena().mmap_base())
            {
                latch.ready.fetch_add(1, Ordering::AcqRel);
                while latch.go.load(Ordering::Acquire) == 0 {
                    std::hint::spin_loop();
                }
                index.insert(IndexValue::String(key.to_string()), row_id);
            } else {
                exit_code = 101;
            }

            // SAFETY:
            // Child exits immediately without unwinding.
            unsafe { libc::_exit(exit_code) };
        }
        rustix::runtime::Fork::Parent(pid) => pid,
    }
}

fn wait_child(pid: rustix::process::Pid) {
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
