//! Schedules derived from the durability model, executed against production
//! commit/checkpoint/recovery APIs. These require neither timing nor a mock WAL.
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use aerostore_core::{
    deserialize_commit_record, deserialize_delta_wal_record, read_wal_file,
    recover_occ_table_from_checkpoint_and_wal, recover_occ_table_from_wal, spawn_wal_writer_daemon,
    write_occ_checkpoint_and_truncate_wal, DeltaWalRecord, IndexCompare, IndexValue, OccCommitter,
    OccTable, SecondaryIndex, SharedWalRing, ShmArena, SyncWalWriter, WalDeltaCodec, WalRingCommit,
    WalRingWrite,
};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
struct RejectEncoding(u64);

impl Serialize for RejectEncoding {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0 == 1 {
            return Err(serde::ser::Error::custom("injected WAL codec failure"));
        }
        serializer.serialize_u64(self.0)
    }
}

impl WalDeltaCodec for RejectEncoding {}

thread_local! {
    static CHECKPOINT_SERIALIZATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
struct PausingRow(u64);

impl Serialize for PausingRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CHECKPOINT_SERIALIZATION_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        serializer.serialize_u64(self.0)
    }
}

impl WalDeltaCodec for PausingRow {}

#[test]
fn failed_wal_encoding_does_not_publish_a_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<RejectEncoding>::new(arena, 1).unwrap();
    table.seed_row(0, RejectEncoding(0)).unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(dir.path().join("wal")).unwrap();
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, RejectEncoding(1)).unwrap();
    assert!(committer.commit(&table, &mut tx).is_err());
    assert_eq!(
        table.latest_value(0).unwrap(),
        Some(RejectEncoding(0)),
        "a WAL codec failure must leave the original row visible"
    );
    table.abort(&mut tx).unwrap();
}

#[test]
fn failed_wal_encoding_rolls_back_prepared_index_destinations() {
    let dir = tempfile::tempdir().unwrap();
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let mut table = OccTable::<RejectEncoding>::new(Arc::clone(&arena), 1).unwrap();
    let index = SecondaryIndex::<usize>::new_in_shared("value", arena);
    table.seed_row(0, RejectEncoding(0)).unwrap();
    index.try_insert(IndexValue::I64(0), 0).unwrap();
    table
        .bind_index(index.clone(), |row| Some(IndexValue::I64(row.0 as i64)))
        .unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(dir.path().join("wal")).unwrap();
    let mut failed = table.begin_transaction().unwrap();
    table.write(&mut failed, 0, RejectEncoding(1)).unwrap();
    assert!(committer.commit(&table, &mut failed).is_err());
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(0), 0)]);
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(
        table
            .index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::I64(0)))
            .unwrap(),
        vec![0]
    );
    assert!(table
        .index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::I64(1)))
        .unwrap()
        .is_empty());
    table.abort(&mut reader).unwrap();
    let mut retry = table.begin_transaction().unwrap();
    table.write(&mut retry, 0, RejectEncoding(2)).unwrap();
    committer.commit(&table, &mut retry).unwrap();
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(2), 0)]);
}

#[test]
fn acknowledged_dependent_commit_recovers_with_its_input() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let table =
        OccTable::<RejectEncoding>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 2).unwrap();
    table.seed_row(0, RejectEncoding(0)).unwrap();
    table.seed_row(1, RejectEncoding(0)).unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut first = table.begin_transaction().unwrap();
    table.write(&mut first, 0, RejectEncoding(1)).unwrap();
    assert!(committer.commit(&table, &mut first).is_err());
    table.abort(&mut first).unwrap();

    let mut dependent = table.begin_transaction().unwrap();
    let input = table.read(&mut dependent, 0).unwrap().unwrap().0;
    table
        .write(&mut dependent, 1, RejectEncoding(input + 2))
        .unwrap();
    committer.commit(&table, &mut dependent).unwrap();

    // Simulate loss of volatile state by using a fresh arena and the real WAL.
    let recovered =
        OccTable::<RejectEncoding>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 2).unwrap();
    recovered.seed_row(0, RejectEncoding(0)).unwrap();
    recovered.seed_row(1, RejectEncoding(0)).unwrap();
    recover_occ_table_from_wal(&recovered, &wal).unwrap();
    let source = recovered.latest_value(0).unwrap().unwrap().0;
    let output = recovered.latest_value(1).unwrap().unwrap().0;
    assert_eq!(
        output,
        source + 2,
        "acknowledged output lost its input on recovery"
    );
}

#[test]
fn checkpoint_preserves_a_transaction_that_started_before_its_cut() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let checkpoint = dir.path().join("checkpoint");
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(arena, 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();

    let mut old = table.begin_transaction().unwrap();
    table.write(&mut old, 0, 7).unwrap();
    write_occ_checkpoint_and_truncate_wal(&table, &checkpoint, &wal).unwrap();
    committer.commit(&table, &mut old).unwrap();

    let recovered = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    recover_occ_table_from_checkpoint_and_wal(&recovered, &checkpoint, &wal).unwrap();
    assert_eq!(
        recovered.latest_value(0).unwrap(),
        Some(7),
        "checkpoint start-ID cutoff must not discard a later durable commit"
    );
}

#[test]
fn checkpoint_cannot_truncate_a_commit_missing_from_its_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let checkpoint = dir.path().join("checkpoint");
    let table = Arc::new(
        OccTable::<PausingRow>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap(),
    );
    table.seed_row(0, PausingRow(0)).unwrap();
    // Establish the stream before this schedule. First-time binding itself
    // legitimately waits for the checkpoint's all-partition exclusion.
    let mut initial = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut initial_tx = table.begin_transaction().unwrap();
    initial.commit(&table, &mut initial_tx).unwrap();
    let (snapshot_taken, snapshot_received) = mpsc::channel();
    let (resume_checkpoint, checkpoint_resumed) = mpsc::channel();
    let checkpoint_worker = {
        let table = Arc::clone(&table);
        let wal = wal.clone();
        let checkpoint = checkpoint.clone();
        std::thread::spawn(move || {
            CHECKPOINT_SERIALIZATION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    snapshot_taken.send(()).unwrap();
                    checkpoint_resumed
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }));
            });
            write_occ_checkpoint_and_truncate_wal(&table, checkpoint, wal).unwrap();
        })
    };
    snapshot_received
        .recv_timeout(Duration::from_secs(10))
        .unwrap();

    let (attempted, attempt_received) = mpsc::channel();
    let (resume_writer, writer_resumed) = mpsc::channel();
    let writer = {
        let table = Arc::clone(&table);
        let wal = wal.clone();
        std::thread::spawn(move || {
            let mut committer = OccCommitter::<8, 1024>::new_synchronous(wal).unwrap();
            let mut tx = table.begin_transaction().unwrap();
            table.write(&mut tx, 0, PausingRow(9)).unwrap();
            let result = committer.commit(&table, &mut tx);
            let committed = result.is_ok();
            if !committed {
                assert!(matches!(
                    result,
                    Err(aerostore_core::WalWriterError::Occ(
                        aerostore_core::OccError::SerializationFailure
                    ))
                ));
            }
            attempted.send(()).unwrap();
            writer_resumed
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            if !committed {
                let mut retry = table.begin_transaction().unwrap();
                table.write(&mut retry, 0, PausingRow(9)).unwrap();
                committer.commit(&table, &mut retry).unwrap();
            }
        })
    };
    // The baseline succeeds here, between snapshot and truncation. The fixed
    // protocol rejects that attempt under its existing partition locks.
    attempt_received
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    resume_checkpoint.send(()).unwrap();
    checkpoint_worker.join().unwrap();
    resume_writer.send(()).unwrap();
    writer.join().unwrap();

    let recovered =
        OccTable::<PausingRow>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    recover_occ_table_from_checkpoint_and_wal(&recovered, checkpoint, wal).unwrap();
    assert_eq!(recovered.latest_value(0).unwrap(), Some(PausingRow(9)));
}

#[test]
fn rejected_async_enqueue_does_not_publish_a_transaction() {
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(Arc::clone(&arena), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let ring = SharedWalRing::<8, 1024>::create(arena).unwrap();
    ring.close().unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring);
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    assert!(committer.commit(&table, &mut tx).is_err());
    assert_eq!(table.latest_value(0).unwrap(), Some(0));
    table.abort(&mut tx).unwrap();
}

#[test]
fn rejected_async_encoding_does_not_claim_a_full_wal_baseline() {
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<RejectEncoding>::new(Arc::clone(&arena), 1).unwrap();
    table.seed_row(0, RejectEncoding(0)).unwrap();
    let ring = SharedWalRing::<8, 1024>::create(arena).unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring.clone());
    let mut failed = table.begin_transaction().unwrap();
    table.write(&mut failed, 0, RejectEncoding(1)).unwrap();
    assert!(committer.commit(&table, &mut failed).is_err());
    table.abort(&mut failed).unwrap();
    assert!(ring.pop_bytes().unwrap().is_none());

    let mut retry = table.begin_transaction().unwrap();
    table.write(&mut retry, 0, RejectEncoding(2)).unwrap();
    committer.commit(&table, &mut retry).unwrap();
    let record = deserialize_commit_record(&ring.pop_bytes().unwrap().unwrap()).unwrap();
    let change = deserialize_delta_wal_record(&record.writes[0].wal_record_payload).unwrap();
    assert!(matches!(change, DeltaWalRecord::UpdateFull { .. }));
}

#[test]
fn legacy_v1_checkpoint_remains_readable() {
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = dir.path().join("checkpoint");
    // The old bincode frame consists of version, cut and row-id/value pairs.
    let legacy = (1_u32, 17_u64, vec![(0_u64, 42_u64)]);
    std::fs::write(&checkpoint, bincode::serialize(&legacy).unwrap()).unwrap();
    let recovered = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    recover_occ_table_from_checkpoint_and_wal(&recovered, checkpoint, dir.path().join("wal"))
        .unwrap();
    assert_eq!(recovered.latest_value(0).unwrap(), Some(42));
}

#[test]
fn independent_wal_writers_preserve_complete_frames() {
    const WORKERS: u64 = 4;
    const RECORDS: u64 = 16;
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let barrier = Arc::new(std::sync::Barrier::new(WORKERS as usize));
    std::thread::scope(|scope| {
        for worker in 0..WORKERS {
            let wal = &wal;
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                let mut writer = SyncWalWriter::open(wal).unwrap();
                barrier.wait();
                for sequence in 0..RECORDS {
                    let id = worker * RECORDS + sequence;
                    writer
                        .append_commit(&WalRingCommit {
                            version: aerostore_core::wal_ring::WAL_RING_COMMIT_VERSION,
                            txid: id + 1,
                            writes: vec![WalRingWrite {
                                row_id: id,
                                base_offset: 0,
                                new_offset: 0,
                                value_payload: vec![id as u8; 16 * 1024],
                                wal_record_payload: Vec::new(),
                            }],
                        })
                        .unwrap();
                }
            });
        }
    });
    let records = read_wal_file(&wal).unwrap();
    assert_eq!(records.len(), (WORKERS * RECORDS) as usize);
    let mut identifiers = std::collections::BTreeSet::new();
    for record in records {
        assert!(identifiers.insert(record.txid));
        let write = &record.writes[0];
        assert_eq!(write.value_payload, vec![write.row_id as u8; 16 * 1024]);
    }
}

#[test]
fn synchronous_acknowledgement_cannot_lose_an_async_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(Arc::clone(&arena), 2).unwrap();
    table.seed_row(0, 0).unwrap();
    table.seed_row(1, 0).unwrap();
    // Deliberately no daemon: queued asynchronous bytes cannot reach the file.
    let ring = SharedWalRing::<8, 1024>::create(arena).unwrap();
    let mut asynchronous = OccCommitter::new_asynchronous(ring);
    let mut producer = table.begin_transaction().unwrap();
    table.write(&mut producer, 0, 1).unwrap();
    asynchronous.commit(&table, &mut producer).unwrap();

    let mut synchronous = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut dependent = table.begin_transaction().unwrap();
    let input = table.read(&mut dependent, 0).unwrap().unwrap();
    table.write(&mut dependent, 1, input + 2).unwrap();
    if synchronous.commit(&table, &mut dependent).is_err() {
        // An incompatible stream may be refused before publication; it must
        // not return success for an output whose input is only in the ring.
        assert_eq!(table.latest_value(1).unwrap(), Some(0));
        table.abort(&mut dependent).unwrap();
        return;
    }

    let recovered = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 2).unwrap();
    recovered.seed_row(0, 0).unwrap();
    recovered.seed_row(1, 0).unwrap();
    recover_occ_table_from_wal(&recovered, &wal).unwrap();
    assert_eq!(
        recovered.latest_value(1).unwrap().unwrap(),
        recovered.latest_value(0).unwrap().unwrap() + 2,
        "synchronous acknowledgement omitted its pending asynchronous input"
    );
}

#[test]
fn one_table_cannot_split_its_history_across_wal_files() {
    let dir = tempfile::tempdir().unwrap();
    let first_wal = dir.path().join("first.wal");
    let table = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 2).unwrap();
    table.seed_row(0, 0).unwrap();
    table.seed_row(1, 0).unwrap();
    let mut first = OccCommitter::<8, 1024>::new_synchronous(&first_wal).unwrap();
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    first.commit(&table, &mut tx).unwrap();
    let mut other = OccCommitter::<8, 1024>::new_synchronous(dir.path().join("other.wal")).unwrap();
    let mut rejected = table.begin_transaction().unwrap();
    table.write(&mut rejected, 1, 2).unwrap();
    assert!(other.commit(&table, &mut rejected).is_err());
    assert_eq!(table.latest_value(1).unwrap(), Some(0));
    table.abort(&mut rejected).unwrap();

    // Independent file handles for the same stream remain supported.
    let mut same = OccCommitter::<8, 1024>::new_synchronous(&first_wal).unwrap();
    let mut accepted = table.begin_transaction().unwrap();
    table.write(&mut accepted, 1, 3).unwrap();
    same.commit(&table, &mut accepted).unwrap();
    assert_eq!(table.latest_value(1).unwrap(), Some(3));
}

#[test]
fn one_table_cannot_split_its_history_across_async_rings() {
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(Arc::clone(&arena), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let first_ring = SharedWalRing::<8, 1024>::create(Arc::clone(&arena)).unwrap();
    let other_ring = SharedWalRing::<8, 1024>::create(arena).unwrap();
    let mut first = OccCommitter::new_asynchronous(first_ring);
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    first.commit(&table, &mut tx).unwrap();
    let mut other = OccCommitter::new_asynchronous(other_ring);
    let mut rejected = table.begin_transaction().unwrap();
    table.write(&mut rejected, 0, 2).unwrap();
    assert!(other.commit(&table, &mut rejected).is_err());
    assert_eq!(table.latest_value(0).unwrap(), Some(1));
    table.abort(&mut rejected).unwrap();
}

#[test]
fn bound_durable_table_rejects_unlogged_writes_but_allows_read_only_commit() {
    let dir = tempfile::tempdir().unwrap();
    let table = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(dir.path().join("wal")).unwrap();
    let mut initial = table.begin_transaction().unwrap();
    table.write(&mut initial, 0, 1).unwrap();
    committer.commit(&table, &mut initial).unwrap();
    let mut rejected = table.begin_transaction().unwrap();
    table.write(&mut rejected, 0, 2).unwrap();
    assert!(table.commit(&mut rejected).is_err());
    table.abort(&mut rejected).unwrap();
    assert_eq!(table.latest_value(0).unwrap(), Some(1));
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(table.read(&mut reader, 0).unwrap(), Some(1));
    table.commit(&mut reader).unwrap();
}

#[test]
fn checkpoint_rejects_a_different_wal_file_without_truncating_it() {
    let dir = tempfile::tempdir().unwrap();
    let table = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(dir.path().join("wal")).unwrap();
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    committer.commit(&table, &mut tx).unwrap();
    let other = dir.path().join("other.wal");
    std::fs::write(&other, b"another stream").unwrap();
    let checkpoint = dir.path().join("checkpoint");
    assert!(write_occ_checkpoint_and_truncate_wal(&table, &checkpoint, &other).is_err());
    assert_eq!(std::fs::read(other).unwrap(), b"another stream");
    assert!(!checkpoint.exists());
}

#[test]
fn file_checkpoint_refuses_an_undrained_async_stream() {
    let dir = tempfile::tempdir().unwrap();
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let table = OccTable::<u64>::new(Arc::clone(&arena), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let ring = SharedWalRing::<8, 1024>::create(arena).unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring.clone());
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    committer.commit(&table, &mut tx).unwrap();
    let checkpoint = dir.path().join("checkpoint");
    assert!(
        write_occ_checkpoint_and_truncate_wal(&table, &checkpoint, dir.path().join("wal")).is_err()
    );
    assert!(!checkpoint.exists());
    assert!(ring.pop_bytes().unwrap().is_some());
}

#[test]
fn async_stream_must_share_the_tables_local_mapping() {
    let table = OccTable::<u64>::new(Arc::new(ShmArena::new(8 << 20).unwrap()), 1).unwrap();
    table.seed_row(0, 0).unwrap();
    let ring = SharedWalRing::<8, 1024>::create(Arc::new(ShmArena::new(8 << 20).unwrap())).unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring);
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, 1).unwrap();
    assert!(committer.commit(&table, &mut tx).is_err());
    assert_eq!(table.latest_value(0).unwrap(), Some(0));
    table.abort(&mut tx).unwrap();
}

#[test]
fn async_ring_has_one_joined_writer_and_one_wal_file() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let ring = SharedWalRing::<8, 1024>::create(Arc::new(ShmArena::new(8 << 20).unwrap())).unwrap();
    let owner = spawn_wal_writer_daemon(ring.clone(), &wal).unwrap();
    let epoch = ring.writer_epoch().unwrap();
    assert!(spawn_wal_writer_daemon(ring.clone(), &wal).is_err());
    assert_eq!(ring.writer_epoch().unwrap(), epoch);
    ring.close().unwrap();
    owner.join().unwrap();
    // Exclusive restart keeps the immutable file identity, while allowing a
    // replacement owner after the old daemon has been reaped.
    ring.reset_for_restart().unwrap();
    assert!(spawn_wal_writer_daemon(ring.clone(), dir.path().join("other.wal")).is_err());
    let replacement = spawn_wal_writer_daemon(ring.clone(), &wal).unwrap();
    ring.close().unwrap();
    replacement.join().unwrap();
}

#[test]
fn killed_async_writer_must_be_joined_before_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let ring = SharedWalRing::<8, 1024>::create(Arc::new(ShmArena::new(8 << 20).unwrap())).unwrap();
    let owner = spawn_wal_writer_daemon(ring.clone(), &wal).unwrap();
    owner.terminate(libc::SIGKILL).unwrap();
    assert!(spawn_wal_writer_daemon(ring.clone(), &wal).is_err());
    owner.join_any_status().unwrap();
    let replacement = spawn_wal_writer_daemon(ring.clone(), &wal).unwrap();
    ring.close().unwrap();
    replacement.join().unwrap();
}

#[test]
fn inherited_sync_writer_reopens_its_file_lock_after_fork() {
    const RECORDS: u64 = 16;
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let mut writer = SyncWalWriter::open(&wal).unwrap();
    let append = |writer: &mut SyncWalWriter, id: u64| {
        writer.append_commit(&WalRingCommit {
            version: aerostore_core::wal_ring::WAL_RING_COMMIT_VERSION,
            txid: id + 1,
            writes: vec![WalRingWrite {
                row_id: id,
                base_offset: 0,
                new_offset: 0,
                value_payload: vec![id as u8; 16 * 1024],
                wal_record_payload: Vec::new(),
            }],
        })
    };
    // This deliberately inherits the existing writer/file description, as a
    // fork-based local worker can do. The child must create a distinct open.
    let child = unsafe { libc::fork() };
    assert!(child >= 0);
    if child == 0 {
        for id in RECORDS..2 * RECORDS {
            if append(&mut writer, id).is_err() {
                unsafe { libc::_exit(1) };
            }
        }
        unsafe { libc::_exit(0) };
    }
    for id in 0..RECORDS {
        append(&mut writer, id).unwrap();
    }
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    let records = read_wal_file(&wal).unwrap();
    assert_eq!(records.len(), 2 * RECORDS as usize);
    let mut identifiers = std::collections::BTreeSet::new();
    for record in records {
        assert!(identifiers.insert(record.txid));
        assert_eq!(
            record.writes[0].value_payload,
            vec![record.writes[0].row_id as u8; 16 * 1024]
        );
    }
}

fn encoding_releases_guards_and_revalidates(asynchronous: bool) {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let mut table = OccTable::<PausingRow>::new(Arc::clone(&arena), 1).unwrap();
    let index = SecondaryIndex::<usize>::new_in_shared("value", Arc::clone(&arena));
    table.seed_row(0, PausingRow(0)).unwrap();
    index.try_insert(IndexValue::I64(0), 0).unwrap();
    table
        .bind_index(index.clone(), |row| Some(IndexValue::I64(row.0 as i64)))
        .unwrap();
    let table = Arc::new(table);
    let ring = SharedWalRing::<8, 1024>::create(Arc::clone(&arena)).unwrap();
    let first_committer = if asynchronous {
        OccCommitter::new_asynchronous(ring.clone())
    } else {
        OccCommitter::new_synchronous(&wal).unwrap()
    };
    let mut competing_committer = if asynchronous {
        OccCommitter::new_asynchronous(ring.clone())
    } else {
        OccCommitter::new_synchronous(&wal).unwrap()
    };
    let (encoding_started, encoding_observed) = mpsc::channel();
    let (resume_encoding, encoding_resumed) = mpsc::channel();
    let first = {
        let table = Arc::clone(&table);
        std::thread::spawn(move || {
            let mut committer = first_committer;
            let mut tx = table.begin_transaction().unwrap();
            table.write(&mut tx, 0, PausingRow(1)).unwrap();
            let txid = tx.txid();
            CHECKPOINT_SERIALIZATION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    encoding_started.send(()).unwrap();
                    encoding_resumed
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }));
            });
            let result = committer.commit(&table, &mut tx);
            assert!(matches!(
                table.read(&mut tx, 0),
                Err(aerostore_core::OccError::TransactionClosed)
            ));
            (result, committer, txid)
        })
    };
    encoding_observed
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let (competing_done, competing_observed) = mpsc::channel();
    let competing = {
        let table = Arc::clone(&table);
        std::thread::spawn(move || {
            let mut tx = table.begin_transaction().unwrap();
            table.write(&mut tx, 0, PausingRow(2)).unwrap();
            let result = competing_committer.commit(&table, &mut tx);
            competing_done
                .send(
                    result
                        .as_ref()
                        .map(|count| *count)
                        .map_err(ToString::to_string),
                )
                .unwrap();
            result
        })
    };
    let before_release = competing_observed.recv_timeout(Duration::from_secs(2));
    // Always release and join both workers before asserting, including against
    // the previous implementation that ran codecs while holding these guards.
    resume_encoding.send(()).unwrap();
    let (first_result, mut first_committer, rejected_txid) = first.join().unwrap();
    let competing_result = competing.join().unwrap();
    assert!(
        matches!(before_release, Ok(Ok(1))),
        "competing indexed commit must finish while the codec is paused: {before_release:?}"
    );
    competing_result.unwrap();
    assert!(matches!(
        first_result,
        Err(aerostore_core::WalWriterError::Occ(
            aerostore_core::OccError::SerializationFailure
        ))
    ));
    assert_eq!(table.latest_value(0).unwrap(), Some(PausingRow(2)));
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(2), 0)]);
    assert!(arena
        .proc_array()
        .create_snapshot(arena.global_txid())
        .in_flight_txids()
        .is_empty());

    let committed = if asynchronous {
        vec![deserialize_commit_record(&ring.pop_bytes().unwrap().unwrap()).unwrap()]
    } else {
        read_wal_file(&wal).unwrap()
    };
    assert_eq!(
        committed.len(),
        1,
        "rejected prepared payload must never reach WAL"
    );
    assert_ne!(committed[0].txid, rejected_txid);
    if asynchronous {
        assert!(ring.pop_bytes().unwrap().is_none());
    }
    let mut retry = table.begin_transaction().unwrap();
    table.write(&mut retry, 0, PausingRow(3)).unwrap();
    first_committer.commit(&table, &mut retry).unwrap();
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(3), 0)]);
    if asynchronous {
        let retry_record = deserialize_commit_record(&ring.pop_bytes().unwrap().unwrap()).unwrap();
        assert!(
            matches!(
                deserialize_delta_wal_record(&retry_record.writes[0].wal_record_payload).unwrap(),
                DeltaWalRecord::UpdateFull { .. }
            ),
            "failed revalidation must not claim an enqueued full-row baseline"
        );
    } else {
        assert_eq!(read_wal_file(&wal).unwrap().len(), 2);
    }
}

#[test]
fn synchronous_wal_encoding_runs_outside_commit_guards_and_revalidates() {
    encoding_releases_guards_and_revalidates(false);
}

#[test]
fn asynchronous_wal_encoding_runs_outside_commit_guards_and_revalidates() {
    encoding_releases_guards_and_revalidates(true);
}

#[test]
fn panicking_prepared_codec_aborts_without_rows_indexes_wal_or_registration() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let mut table = OccTable::<PausingRow>::new(Arc::clone(&arena), 1).unwrap();
    let index = SecondaryIndex::<usize>::new_in_shared("value", Arc::clone(&arena));
    table.seed_row(0, PausingRow(0)).unwrap();
    index.try_insert(IndexValue::I64(0), 0).unwrap();
    table
        .bind_index(index.clone(), |row| Some(IndexValue::I64(row.0 as i64)))
        .unwrap();
    let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, PausingRow(1)).unwrap();
    CHECKPOINT_SERIALIZATION_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(|| panic!("injected preparation codec panic")));
    });
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        committer.commit(&table, &mut tx)
    }))
    .is_err());
    assert_eq!(table.latest_value(0).unwrap(), Some(PausingRow(0)));
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(0), 0)]);
    assert!(read_wal_file(&wal).unwrap().is_empty());
    assert!(matches!(
        table.read(&mut tx, 0),
        Err(aerostore_core::OccError::TransactionClosed)
    ));
    assert!(arena
        .proc_array()
        .create_snapshot(arena.global_txid())
        .in_flight_txids()
        .is_empty());
    let mut retry = table.begin_transaction().unwrap();
    table.write(&mut retry, 0, PausingRow(2)).unwrap();
    committer.commit(&table, &mut retry).unwrap();
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(2), 0)]);
    assert_eq!(read_wal_file(&wal).unwrap().len(), 1);
}

#[test]
fn async_epoch_change_during_preparation_retries_without_accepting_stale_bytes() {
    let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
    let mut table = OccTable::<PausingRow>::new(Arc::clone(&arena), 1).unwrap();
    let index = SecondaryIndex::<usize>::new_in_shared("value", Arc::clone(&arena));
    table.seed_row(0, PausingRow(0)).unwrap();
    index.try_insert(IndexValue::I64(0), 0).unwrap();
    table
        .bind_index(index.clone(), |row| Some(IndexValue::I64(row.0 as i64)))
        .unwrap();
    let ring = SharedWalRing::<8, 1024>::create(Arc::clone(&arena)).unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring.clone());
    let mut baseline = table.begin_transaction().unwrap();
    table.write(&mut baseline, 0, PausingRow(1)).unwrap();
    committer.commit(&table, &mut baseline).unwrap();
    let first = deserialize_commit_record(&ring.pop_bytes().unwrap().unwrap()).unwrap();
    assert!(matches!(
        deserialize_delta_wal_record(&first.writes[0].wal_record_payload).unwrap(),
        DeltaWalRecord::UpdateFull { .. }
    ));

    let mut stale = table.begin_transaction().unwrap();
    table.write(&mut stale, 0, PausingRow(2)).unwrap();
    CHECKPOINT_SERIALIZATION_HOOK.with(|hook| {
        let ring = ring.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            // Change metadata, not the ring contents: destructive reset still
            // requires all producers/consumers to have stopped exclusively.
            ring.bump_writer_epoch().unwrap();
        }));
    });
    assert!(matches!(
        committer.commit(&table, &mut stale),
        Err(aerostore_core::WalWriterError::Occ(
            aerostore_core::OccError::SerializationFailure
        ))
    ));
    assert!(ring.pop_bytes().unwrap().is_none());
    assert_eq!(table.latest_value(0).unwrap(), Some(PausingRow(1)));
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(1), 0)]);
    assert!(matches!(
        table.read(&mut stale, 0),
        Err(aerostore_core::OccError::TransactionClosed)
    ));
    assert!(arena
        .proc_array()
        .create_snapshot(arena.global_txid())
        .in_flight_txids()
        .is_empty());

    for (value, expect_full) in [(3, true), (4, false)] {
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, PausingRow(value)).unwrap();
        committer.commit(&table, &mut tx).unwrap();
        let record = deserialize_commit_record(&ring.pop_bytes().unwrap().unwrap()).unwrap();
        let decoded = deserialize_delta_wal_record(&record.writes[0].wal_record_payload).unwrap();
        assert_eq!(
            matches!(decoded, DeltaWalRecord::UpdateFull { .. }),
            expect_full,
            "only a successfully accepted full baseline enables later deltas in the new epoch"
        );
    }
}
