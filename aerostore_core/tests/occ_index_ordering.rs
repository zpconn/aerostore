use std::sync::{mpsc, Arc};

use aerostore_core::{IndexValue, OccError, OccTable, SecondaryIndex, ShmArena};

#[test]
fn later_commit_cannot_overtake_pending_index_maintenance() {
    let shm = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = Arc::new(OccTable::<i64>::new(Arc::clone(&shm), 1).unwrap());
    table.seed_row(0, 100).unwrap();
    let index = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "value",
        Arc::clone(&shm),
    ));
    index.try_insert(IndexValue::I64(100), 0).unwrap();

    // Pause writer A at exactly the formerly unsafe boundary: the row is
    // committed, but the index still contains its old value.
    let guard = table.lock_indexed_rows(&[0]).unwrap();
    let mut tx = table.begin_transaction().unwrap();
    assert_eq!(table.read(&mut tx, 0).unwrap(), Some(100));
    table.write(&mut tx, 0, 101).unwrap();
    table.commit(&mut tx).unwrap();

    let (attempted, attempt) = mpsc::channel();
    let second_index = Arc::clone(&index);
    let attached = OccTable::<i64>::from_existing(
        Arc::clone(&shm),
        table.shared_header_offset(),
        table.index_slot_offsets(),
    )
    .unwrap();
    let writer = std::thread::spawn(move || {
        // An independently attached handle must observe the stable slot lock,
        // even though A's old MVCC version is no longer the current head.
        assert!(attached.try_lock_indexed_rows(&[0]).unwrap().is_none());
        attempted.send(()).unwrap();
        let _guard = attached.lock_indexed_rows(&[0]).unwrap();
        let mut tx = attached.begin_transaction().unwrap();
        let before = attached.read(&mut tx, 0).unwrap().unwrap();
        assert_eq!(before, 101);
        attached.write(&mut tx, 0, 102).unwrap();
        attached.commit(&mut tx).unwrap();
        second_index
            .try_move_payload(&IndexValue::I64(before), IndexValue::I64(102), &0)
            .unwrap();
    });

    attempt.recv().unwrap();
    index
        .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &0)
        .unwrap();
    drop(guard);
    writer.join().unwrap();
    assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, 102)]);
    assert_eq!(index.traverse(), vec![(IndexValue::I64(102), vec![0])]);
}

#[test]
fn batched_guards_deduplicate_and_release_partial_acquisitions() {
    let shm = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(shm, 3).unwrap();
    let guard = table.lock_indexed_rows(&[2, 1, 2]).unwrap();
    assert!(table.try_lock_indexed_rows(&[0, 1, 2]).unwrap().is_none());
    assert!(table.try_lock_indexed_rows(&[0]).unwrap().is_some());
    assert!(matches!(
        table.lock_indexed_rows(&[0, 3]),
        Err(OccError::RowOutOfBounds { row_id: 3, .. })
    ));
    assert!(table.try_lock_indexed_rows(&[0]).unwrap().is_some());
    drop(guard);
    assert!(table
        .try_lock_indexed_rows(&[2, 0, 1, 1])
        .unwrap()
        .is_some());
}

#[test]
fn guard_survives_abort_and_releases_on_scope_exit() {
    let shm = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(shm, 1).unwrap();
    table.seed_row(0, 1).unwrap();
    {
        let _guard = table.lock_indexed_rows(&[0]).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 2).unwrap();
        table.abort(&mut tx).unwrap();
        assert!(table.try_lock_indexed_rows(&[0]).unwrap().is_none());
    }
    assert!(table.try_lock_indexed_rows(&[0]).unwrap().is_some());
    assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, 1)]);
}

#[test]
fn indexed_guard_coordinates_separately_mapped_processes() {
    use std::process::Command;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed-ordering.mmap");
    let shm = Arc::new(
        aerostore_core::map_tmpfs_shared(&path, 8 << 20)
            .unwrap()
            .arena,
    );
    let table = OccTable::<u64>::new(shm, 1).unwrap();
    table.seed_row(0, 42).unwrap();
    let run_child = |locked: bool| {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "indexed_guard_child", "--nocapture"])
            .env("AEROSTORE_INDEXED_GUARD_TEST_PATH", &path)
            .env(
                "AEROSTORE_INDEXED_GUARD_TEST_HEADER",
                table.shared_header_offset().to_string(),
            )
            .env(
                "AEROSTORE_INDEXED_GUARD_TEST_SLOT",
                table.index_slot_offsets()[0].to_string(),
            )
            .env("AEROSTORE_INDEXED_GUARD_TEST_LOCKED", locked.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let guard = table.lock_indexed_rows(&[0]).unwrap();
    run_child(true);
    drop(guard);
    run_child(false);
}

#[test]
fn indexed_guard_child() {
    let Some(path) = std::env::var_os("AEROSTORE_INDEXED_GUARD_TEST_PATH") else {
        return;
    };
    let header: u32 = std::env::var("AEROSTORE_INDEXED_GUARD_TEST_HEADER")
        .unwrap()
        .parse()
        .unwrap();
    let slot: u32 = std::env::var("AEROSTORE_INDEXED_GUARD_TEST_SLOT")
        .unwrap()
        .parse()
        .unwrap();
    let locked: bool = std::env::var("AEROSTORE_INDEXED_GUARD_TEST_LOCKED")
        .unwrap()
        .parse()
        .unwrap();
    let mapped = aerostore_core::map_tmpfs_shared(path, 8 << 20).unwrap();
    assert_eq!(mapped.mode, aerostore_core::TmpfsAttachMode::WarmStart);
    let table = OccTable::<u64>::from_existing(Arc::new(mapped.arena), header, vec![slot]).unwrap();
    let guard = table.try_lock_indexed_rows(&[0]).unwrap();
    assert_eq!(guard.is_none(), locked);
    assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, 42)]);
}
