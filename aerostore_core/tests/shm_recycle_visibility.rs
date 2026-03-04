use std::sync::Arc;
use std::thread;

use aerostore_core::{ArenaClass, ShmArena};

#[test]
fn cross_thread_recycle_visibility_requires_flush() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("create shm"));
    let arena = shm.chunked_arena();

    let recycled = arena
        .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
        .expect("alloc spill32");
    arena
        .recycle_raw_in_class(recycled, 32, 8, ArenaClass::Spill32)
        .expect("recycle spill32");

    let shm_before = Arc::clone(&shm);
    let pre_flush = thread::spawn(move || {
        shm_before
            .chunked_arena()
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("pre-flush sibling alloc")
    })
    .join()
    .expect("pre-flush thread join failed");
    assert_ne!(
        pre_flush, recycled,
        "thread-local recycle cache should hide recycled block before flush"
    );

    shm.flush_local_recycle_caches()
        .expect("flush local recycle caches");

    let shm_after = Arc::clone(&shm);
    let post_flush = thread::spawn(move || {
        shm_after
            .chunked_arena()
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("post-flush sibling alloc")
    })
    .join()
    .expect("post-flush thread join failed");
    assert_eq!(
        post_flush, recycled,
        "flushed spill32 offset should be visible via shared class freelist"
    );
}

#[test]
fn cross_class_reuse_never_crosses_bins_after_flush() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("create shm"));
    let arena = shm.chunked_arena();

    let spill32 = arena
        .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
        .expect("alloc spill32");
    arena
        .recycle_raw_in_class(spill32, 32, 8, ArenaClass::Spill32)
        .expect("recycle spill32");
    shm.flush_local_recycle_caches()
        .expect("flush local recycle caches");

    let shm_for_thread = Arc::clone(&shm);
    let spill64 = thread::spawn(move || {
        shm_for_thread
            .chunked_arena()
            .alloc_raw_in_class(64, 8, ArenaClass::Spill64)
            .expect("alloc spill64 in sibling thread")
    })
    .join()
    .expect("spill64 thread join failed");
    assert_ne!(
        spill64, spill32,
        "spill64 allocation must not consume spill32 class recycled entries"
    );

    let spill32_reused = shm
        .chunked_arena()
        .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
        .expect("alloc spill32 after flush");
    assert_eq!(
        spill32_reused, spill32,
        "spill32 allocation should still reuse spill32 recycled entry"
    );
}
