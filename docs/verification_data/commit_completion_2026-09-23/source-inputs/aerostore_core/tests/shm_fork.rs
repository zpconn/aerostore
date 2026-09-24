#![cfg(unix)]

use std::sync::atomic::{AtomicU32, Ordering};

use aerostore_core::ShmArena;

#[repr(C)]
struct SharedFlightState {
    altitude: AtomicU32,
}

#[test]
fn forked_child_can_read_and_cas_mutate_shared_relptr() {
    const INITIAL_ALTITUDE: u32 = 30_000;
    const CHILD_ALTITUDE: u32 = 31_500;
    const PARENT_ALTITUDE: u32 = 32_000;

    let shm = ShmArena::new(1 << 20).expect("failed to create shared mmap arena");
    let arena = shm.chunked_arena();

    let rel_ptr = arena
        .alloc(SharedFlightState {
            altitude: AtomicU32::new(INITIAL_ALTITUDE),
        })
        .expect("failed to allocate shared row");

    let before_fork = rel_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve RelPtr before fork");
    assert_eq!(
        before_fork.altitude.load(Ordering::Acquire),
        INITIAL_ALTITUDE
    );

    // SAFETY:
    // `fork` duplicates the current process. Child exits via `_exit` without running Rust
    // destructors and avoids panic paths.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

    if pid == 0 {
        let mut exit_code = 0_i32;
        let base = shm.mmap_base();

        if let Some(shared_row) = rel_ptr.as_ref(base) {
            let current = shared_row.altitude.load(Ordering::Acquire);
            if current != INITIAL_ALTITUDE {
                exit_code = 11;
            } else if shared_row
                .altitude
                .compare_exchange(
                    INITIAL_ALTITUDE,
                    CHILD_ALTITUDE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                exit_code = 12;
            }
        } else {
            exit_code = 10;
        }

        // SAFETY:
        // `_exit` immediately terminates child without touching process-global runtime state.
        unsafe { libc::_exit(exit_code) };
    }

    let mut status: libc::c_int = 0;
    // SAFETY:
    // `pid` is a child process ID returned by `fork`.
    let waited = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
    assert_eq!(
        waited,
        pid,
        "waitpid failed: {}",
        std::io::Error::last_os_error()
    );
    assert!(libc::WIFEXITED(status), "child did not exit cleanly");
    let child_exit = libc::WEXITSTATUS(status);
    assert_eq!(
        child_exit, 0,
        "child reported failure while using RelPtr (exit={})",
        child_exit
    );

    let after_fork = rel_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve RelPtr after fork");
    assert_eq!(
        after_fork.altitude.load(Ordering::Acquire),
        CHILD_ALTITUDE,
        "child CAS mutation was not visible in parent"
    );

    after_fork
        .altitude
        .compare_exchange(
            CHILD_ALTITUDE,
            PARENT_ALTITUDE,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .expect("parent CAS mutation should succeed after child exits");
    assert_eq!(after_fork.altitude.load(Ordering::Acquire), PARENT_ALTITUDE);
}
