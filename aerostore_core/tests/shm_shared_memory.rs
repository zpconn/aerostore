#![cfg(unix)]

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use aerostore_core::{
    deserialize_commit_record, RelPtr, SharedWalRing, ShmAllocError, ShmArena, WalRingCommit,
    WalRingWrite,
};

#[repr(C, align(64))]
struct AlignedBlock {
    bytes: [u8; 32],
}

#[repr(C)]
struct SharedCounter {
    value: AtomicU32,
}

#[repr(C)]
struct RingIntegrity {
    count: AtomicU32,
    sum: AtomicU64,
    xor: AtomicU64,
}

#[test]
fn relptr_validates_null_bounds_and_alignment() {
    let shm = ShmArena::new(64 << 10).expect("failed to create shared arena");
    let base = shm.mmap_base();

    let null_ptr = RelPtr::<u64>::null();
    assert!(
        null_ptr.as_ref(base).is_none(),
        "null RelPtr must resolve to None"
    );

    let out_of_bounds_ptr = RelPtr::<u64>::from_offset(4090);
    assert!(
        out_of_bounds_ptr.as_ref(base).is_none(),
        "out-of-bounds RelPtr must resolve to None"
    );

    let misaligned_ptr = RelPtr::<u64>::from_offset(65);
    assert!(
        misaligned_ptr.as_ref(base).is_none(),
        "misaligned RelPtr must resolve to None"
    );

    let arena = shm.chunked_arena();
    let valid_ptr = arena.alloc(1234_u64).expect("allocation must succeed");
    let value_ref = valid_ptr
        .as_ref(base)
        .expect("valid RelPtr must resolve inside mapping");
    assert_eq!(*value_ref, 1234_u64);
}

#[test]
fn shared_chunked_arena_enforces_alignment_and_reports_oom() {
    let shm = ShmArena::new(1 << 20).expect("failed to create shared arena");
    let arena = shm.chunked_arena();

    let mut previous_offset = 0_u32;
    for i in 0..2048_u32 {
        let ptr = arena
            .alloc(AlignedBlock {
                bytes: [i as u8; 32],
            })
            .expect("aligned allocation must succeed");
        let offset = ptr.load(Ordering::Acquire);
        assert_eq!(
            offset % 64,
            0,
            "offset {} must satisfy 64-byte alignment",
            offset
        );
        assert!(
            offset >= previous_offset,
            "allocator offsets must be monotonic"
        );
        previous_offset = offset;
    }

    let tiny_shm = ShmArena::new(20 << 10).expect("failed to create tiny shared arena");
    let tiny_arena = tiny_shm.chunked_arena();
    let mut successful_allocations = 0_usize;

    loop {
        match tiny_arena.alloc([0_u8; 128]) {
            Ok(_) => successful_allocations += 1,
            Err(ShmAllocError::OutOfMemory { .. }) => break,
            Err(other) => panic!("unexpected allocation error: {}", other),
        }
    }

    assert!(
        successful_allocations > 0,
        "expected at least one successful allocation before OOM"
    );
}

#[test]
fn forked_children_cas_contention_reaches_exact_total() {
    const CHILDREN: usize = 8;
    const INCREMENTS_PER_CHILD: u32 = 20_000;

    let shm = ShmArena::new(1 << 20).expect("failed to create shared arena");
    let arena = shm.chunked_arena();
    let counter_ptr = arena
        .alloc(SharedCounter {
            value: AtomicU32::new(0),
        })
        .expect("failed to allocate shared counter");

    let mut pids = Vec::with_capacity(CHILDREN);
    for _ in 0..CHILDREN {
        // SAFETY:
        // `fork` duplicates the process. Child exits via `_exit` and does not unwind.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

        if pid == 0 {
            let mut exit_code = 0_i32;
            if let Some(counter) = counter_ptr.as_ref(shm.mmap_base()) {
                for _ in 0..INCREMENTS_PER_CHILD {
                    loop {
                        let current = counter.value.load(Ordering::Acquire);
                        if counter
                            .value
                            .compare_exchange(
                                current,
                                current.wrapping_add(1),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            } else {
                exit_code = 21;
            }

            // SAFETY:
            // Child exits immediately without touching parent runtime state.
            unsafe { libc::_exit(exit_code) };
        }

        pids.push(pid);
    }

    for pid in pids {
        wait_for_child(pid);
    }

    let counter = counter_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve shared counter");
    let expected = CHILDREN as u32 * INCREMENTS_PER_CHILD;
    assert_eq!(
        counter.value.load(Ordering::Acquire),
        expected,
        "final CAS total diverged under multi-process contention"
    );
}

#[test]
fn forked_mpsc_ring_producers_preserve_message_integrity() {
    const PRODUCERS: usize = 6;
    const MESSAGES_PER_PRODUCER: usize = 300;
    const TOTAL_MESSAGES: usize = PRODUCERS * MESSAGES_PER_PRODUCER;
    const RING_SLOTS: usize = 128;
    const RING_SLOT_BYTES: usize = 128;

    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
        .expect("failed to create shared wal ring");
    let integrity_ptr = shm
        .chunked_arena()
        .alloc(RingIntegrity {
            count: AtomicU32::new(0),
            sum: AtomicU64::new(0),
            xor: AtomicU64::new(0),
        })
        .expect("failed to allocate shared integrity block");

    // Consumer process.
    // SAFETY:
    // `fork` duplicates process. Child exits via `_exit`.
    let consumer_pid = unsafe { libc::fork() };
    assert!(
        consumer_pid >= 0,
        "consumer fork failed: {}",
        std::io::Error::last_os_error()
    );
    if consumer_pid == 0 {
        let mut count = 0_u32;
        let mut sum = 0_u64;
        let mut xor = 0_u64;

        loop {
            match ring.pop_bytes() {
                Ok(Some(payload)) => {
                    let commit = match deserialize_commit_record(payload.as_slice()) {
                        Ok(msg) => msg,
                        Err(_) => unsafe { libc::_exit(61) },
                    };
                    count = count.wrapping_add(1);
                    sum = sum.wrapping_add(commit.txid);
                    xor ^= commit.txid;
                    if count == TOTAL_MESSAGES as u32 {
                        break;
                    }
                }
                Ok(None) => {
                    std::thread::yield_now();
                    std::thread::sleep(std::time::Duration::from_micros(25));
                }
                Err(_) => unsafe { libc::_exit(62) },
            }
        }

        let Some(integrity) = integrity_ptr.as_ref(shm.mmap_base()) else {
            // SAFETY:
            // child exits without unwinding.
            unsafe { libc::_exit(63) };
        };
        integrity.count.store(count, Ordering::Release);
        integrity.sum.store(sum, Ordering::Release);
        integrity.xor.store(xor, Ordering::Release);

        // SAFETY:
        // child exits without unwinding.
        unsafe { libc::_exit(0) };
    }

    let mut producer_pids = Vec::with_capacity(PRODUCERS);
    for producer_idx in 0..PRODUCERS {
        // SAFETY:
        // `fork` duplicates process. Child exits via `_exit`.
        let pid = unsafe { libc::fork() };
        assert!(
            pid >= 0,
            "producer fork failed: {}",
            std::io::Error::last_os_error()
        );

        if pid == 0 {
            for seq in 0..MESSAGES_PER_PRODUCER {
                let txid = ((producer_idx as u64) << 32) | (seq as u64 + 1);
                let msg = WalRingCommit {
                    txid,
                    writes: vec![WalRingWrite {
                        row_id: producer_idx as u64,
                        base_offset: seq as u32,
                        new_offset: seq as u32 + 1,
                    }],
                };

                if ring.push_commit_record(&msg).is_err() {
                    // SAFETY:
                    // child exits without unwinding.
                    unsafe { libc::_exit(71) };
                }
            }
            // SAFETY:
            // child exits without unwinding.
            unsafe { libc::_exit(0) };
        }

        producer_pids.push(pid);
    }

    for pid in producer_pids {
        wait_for_child(pid);
    }
    wait_for_child(consumer_pid);

    let integrity = integrity_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve integrity block");

    let mut expected_sum = 0_u64;
    let mut expected_xor = 0_u64;
    for producer_idx in 0..PRODUCERS {
        for seq in 0..MESSAGES_PER_PRODUCER {
            let txid = ((producer_idx as u64) << 32) | (seq as u64 + 1);
            expected_sum = expected_sum.wrapping_add(txid);
            expected_xor ^= txid;
        }
    }

    assert_eq!(
        integrity.count.load(Ordering::Acquire),
        TOTAL_MESSAGES as u32,
        "consumer did not receive all producer messages"
    );
    assert_eq!(
        integrity.sum.load(Ordering::Acquire),
        expected_sum,
        "ring consumer checksum mismatch indicates corruption/loss"
    );
    assert_eq!(
        integrity.xor.load(Ordering::Acquire),
        expected_xor,
        "ring consumer xor mismatch indicates corruption/loss"
    );
}

fn wait_for_child(pid: libc::pid_t) {
    let mut status: libc::c_int = 0;
    // SAFETY:
    // `pid` is a child process identifier obtained from `fork`.
    let waited = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
    assert_eq!(
        waited,
        pid,
        "waitpid failed: {}",
        std::io::Error::last_os_error()
    );
    assert!(libc::WIFEXITED(status), "child did not exit cleanly");
    let code = libc::WEXITSTATUS(status);
    assert_eq!(code, 0, "child exited with non-zero status {}", code);
}
