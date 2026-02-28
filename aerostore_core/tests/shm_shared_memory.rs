#![cfg(unix)]

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use aerostore_core::{
    deserialize_commit_record, OccTable, RelPtr, SharedWalRing, ShmAllocError, ShmArena,
    ShmPrimaryKeyMap, WalRingCommit, WalRingWrite,
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
    payload_sum: AtomicU64,
    payload_xor: AtomicU64,
}

#[test]
fn relptr_validates_null_bounds_and_alignment() {
    let shm = ShmArena::new(1 << 20).expect("failed to create shared arena");
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

    let tiny_shm = ShmArena::new(256 << 10).expect("failed to create tiny shared arena");
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
fn forked_primary_key_map_updates_are_cross_process_visible() {
    const CHILDREN: usize = 4;

    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create shared arena"));
    let pk_map = ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 256, 1024)
        .expect("failed to create shared primary key map");
    pk_map
        .insert_existing("PARENT", 7)
        .expect("failed to seed parent key");

    let result_ptr = shm
        .chunked_arena()
        .alloc(std::array::from_fn::<AtomicU32, CHILDREN, _>(|_| {
            AtomicU32::new(u32::MAX)
        }))
        .expect("failed to allocate shared result slots");

    let mut pids = Vec::with_capacity(CHILDREN);
    for child_idx in 0..CHILDREN {
        // SAFETY:
        // `fork` duplicates process. Child exits via `_exit`.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

        if pid == 0 {
            let Some(result_slots) = result_ptr.as_ref(shm.mmap_base()) else {
                // SAFETY:
                // child exits without unwinding.
                unsafe { libc::_exit(91) };
            };

            let outcome = match child_idx {
                0 => pk_map.insert_existing("UAL123", 101),
                1 => pk_map.insert_existing("DAL456", 202),
                2 => pk_map.get_or_insert("RACE001"),
                3 => pk_map.get_or_insert("RACE001"),
                _ => unreachable!(),
            };

            let row_id = match outcome {
                Ok(row_id) => row_id as u32,
                Err(_) => {
                    // SAFETY:
                    // child exits without unwinding.
                    unsafe { libc::_exit(92) };
                }
            };
            result_slots[child_idx].store(row_id, Ordering::Release);

            // SAFETY:
            // child exits without unwinding.
            unsafe { libc::_exit(0) };
        }

        pids.push(pid);
    }

    for pid in pids {
        wait_for_child(pid);
    }

    let slots = result_ptr
        .as_ref(shm.mmap_base())
        .expect("parent failed to resolve shared result slots");

    assert_eq!(
        pk_map.get("PARENT").expect("parent key lookup failed"),
        Some(7)
    );
    assert_eq!(
        pk_map.get("UAL123").expect("UAL123 lookup failed"),
        Some(101)
    );
    assert_eq!(
        pk_map.get("DAL456").expect("DAL456 lookup failed"),
        Some(202)
    );

    let race_id = pk_map
        .get("RACE001")
        .expect("RACE001 lookup failed")
        .expect("RACE001 must exist after child inserts");
    assert_eq!(
        slots[2].load(Ordering::Acquire),
        race_id as u32,
        "child 2 observed unexpected row id for contended key"
    );
    assert_eq!(
        slots[3].load(Ordering::Acquire),
        race_id as u32,
        "child 3 observed unexpected row id for contended key"
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
            payload_sum: AtomicU64::new(0),
            payload_xor: AtomicU64::new(0),
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
        let mut payload_sum = 0_u64;
        let mut payload_xor = 0_u64;

        loop {
            match ring.pop_bytes() {
                Ok(Some(payload)) => {
                    let commit = match deserialize_commit_record(payload.as_slice()) {
                        Ok(msg) => msg,
                        Err(_) => unsafe { libc::_exit(61) },
                    };
                    if commit.writes.len() != 1 {
                        // SAFETY:
                        // child exits without unwinding.
                        unsafe { libc::_exit(64) };
                    }
                    let write = &commit.writes[0];
                    if write.value_payload.len() != std::mem::size_of::<u64>() {
                        // SAFETY:
                        // child exits without unwinding.
                        unsafe { libc::_exit(65) };
                    }
                    let mut payload_buf = [0_u8; 8];
                    payload_buf.copy_from_slice(write.value_payload.as_slice());
                    let payload_value = u64::from_le_bytes(payload_buf);

                    count = count.wrapping_add(1);
                    sum = sum.wrapping_add(commit.txid);
                    xor ^= commit.txid;
                    payload_sum = payload_sum.wrapping_add(payload_value);
                    payload_xor ^= payload_value;
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
        integrity.payload_sum.store(payload_sum, Ordering::Release);
        integrity.payload_xor.store(payload_xor, Ordering::Release);

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
                let payload_value = txid.rotate_left(11) ^ 0x9E37_79B1_85EB_CA87_u64;
                let msg = WalRingCommit {
                    txid,
                    writes: vec![WalRingWrite {
                        row_id: producer_idx as u64,
                        base_offset: seq as u32,
                        new_offset: seq as u32 + 1,
                        value_payload: payload_value.to_le_bytes().to_vec(),
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
    let mut expected_payload_sum = 0_u64;
    let mut expected_payload_xor = 0_u64;
    for producer_idx in 0..PRODUCERS {
        for seq in 0..MESSAGES_PER_PRODUCER {
            let txid = ((producer_idx as u64) << 32) | (seq as u64 + 1);
            let payload_value = txid.rotate_left(11) ^ 0x9E37_79B1_85EB_CA87_u64;
            expected_sum = expected_sum.wrapping_add(txid);
            expected_xor ^= txid;
            expected_payload_sum = expected_payload_sum.wrapping_add(payload_value);
            expected_payload_xor ^= payload_value;
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
    assert_eq!(
        integrity.payload_sum.load(Ordering::Acquire),
        expected_payload_sum,
        "ring consumer payload checksum mismatch indicates corruption/loss"
    );
    assert_eq!(
        integrity.payload_xor.load(Ordering::Acquire),
        expected_payload_xor,
        "ring consumer payload xor mismatch indicates corruption/loss"
    );
}

#[test]
fn forked_occ_recycle_free_list_stress_is_safe_and_bounded() {
    const CHILDREN: usize = 6;
    const ITERATIONS: usize = 8_000;
    const ROWS: usize = 4;
    const MAX_HEAD_GROWTH_BYTES: u32 = 512 * 1024;

    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to create shared arena"));
    let table = OccTable::<u64>::new(Arc::clone(&shm), ROWS).expect("failed to create occ table");

    for row_id in 0..ROWS {
        table
            .seed_row(row_id, row_id as u64)
            .expect("failed to seed row");
    }

    let base_head = shm.chunked_arena().head_offset();
    let mut pids = Vec::with_capacity(CHILDREN);

    for child_idx in 0..CHILDREN {
        // SAFETY:
        // `fork` duplicates process. Child exits via `_exit`.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());

        if pid == 0 {
            let mut code = 0_i32;
            for iter in 0..ITERATIONS {
                let row_id = (child_idx + iter) % ROWS;

                let mut tx = match table.begin_transaction() {
                    Ok(tx) => tx,
                    Err(_) => {
                        code = 81;
                        break;
                    }
                };

                let current = match table.read(&mut tx, row_id) {
                    Ok(Some(value)) => value,
                    _ => {
                        code = 82;
                        break;
                    }
                };

                if table.savepoint(&mut tx, "sp").is_err() {
                    code = 83;
                    break;
                }
                if table.write(&mut tx, row_id, current + 1).is_err() {
                    code = 84;
                    break;
                }
                if table.rollback_to(&mut tx, "sp").is_err() {
                    code = 85;
                    break;
                }

                let restored = match table.read(&mut tx, row_id) {
                    Ok(Some(value)) => value,
                    _ => {
                        code = 86;
                        break;
                    }
                };
                if restored != current {
                    code = 87;
                    break;
                }

                if table.abort(&mut tx).is_err() {
                    code = 88;
                    break;
                }
            }

            // SAFETY:
            // child exits without unwinding.
            unsafe { libc::_exit(code) };
        }

        pids.push(pid);
    }

    for pid in pids {
        wait_for_child(pid);
    }

    let snapshot = shm.create_snapshot();
    assert_eq!(
        snapshot.len(),
        0,
        "all ProcArray slots must be released after forked recycle stress"
    );

    let end_head = shm.chunked_arena().head_offset();
    let consumed = end_head.saturating_sub(base_head);
    assert!(
        consumed <= MAX_HEAD_GROWTH_BYTES,
        "shared arena head grew unexpectedly under forked recycle stress (growth={} bytes)",
        consumed
    );

    for row_id in 0..ROWS {
        let mut verify = table
            .begin_transaction()
            .expect("verify begin_transaction failed");
        let value = table
            .read(&mut verify, row_id)
            .expect("verify read failed")
            .expect("verify row missing");
        table.abort(&mut verify).expect("verify abort failed");
        assert_eq!(
            value, row_id as u64,
            "row {} changed unexpectedly under rollback-only forked stress",
            row_id
        );
    }
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
