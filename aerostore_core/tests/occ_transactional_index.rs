//! Deterministic isolation regressions for native, transactionally maintained indexes.
//!
//! None of these tests supplies indexed-row guards or application predicate locks.
//! Assertions concern visible rows and commit outcomes, independently of the
//! particular clock, latch, or dependency representation used by the engine.

use std::process::Command;
use std::sync::Arc;

use aerostore_core::{
    IndexCompare, IndexValue, OccCommitter, OccError, OccTable, OccTransaction, SecondaryIndex,
    SharedWalRing, ShmArena, WalDeltaCodec, WalWriterError,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Row {
    key: i64,
    active: bool,
    value: i64,
}

impl Row {
    fn live(key: i64) -> Self {
        Self {
            key,
            active: true,
            value: 0,
        }
    }

    fn vacant() -> Self {
        Self {
            key: 0,
            active: false,
            value: 0,
        }
    }
}

impl WalDeltaCodec for Row {
    fn wal_primary_key(row_id: usize, _value: &Self) -> String {
        row_id.to_string()
    }
}

fn key(row: &Row) -> Option<IndexValue> {
    row.active.then_some(IndexValue::I64(row.key))
}

fn eq(value: i64) -> IndexCompare {
    IndexCompare::Eq(IndexValue::I64(value))
}

fn fixture(rows: &[Row]) -> (Arc<ShmArena>, OccTable<Row>, SecondaryIndex<usize>) {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let (table, index) = fixture_in(Arc::clone(&arena), rows);
    (arena, table, index)
}

fn fixture_in(arena: Arc<ShmArena>, rows: &[Row]) -> (OccTable<Row>, SecondaryIndex<usize>) {
    let mut table = OccTable::new(Arc::clone(&arena), rows.len()).unwrap();
    let index = SecondaryIndex::<usize>::new_in_shared("key", arena);
    for (id, row) in rows.iter().copied().enumerate() {
        table.seed_row(id, row).unwrap();
        if let Some(value) = key(&row) {
            index.try_insert(value, id).unwrap();
        }
    }
    table.bind_index(index.clone(), key).unwrap();
    (table, index)
}

fn scan(
    table: &OccTable<Row>,
    tx: &mut OccTransaction<Row>,
    index: &SecondaryIndex<usize>,
    predicate: &IndexCompare,
) -> Vec<usize> {
    table.index_lookup(tx, index, predicate).unwrap()
}

fn replace(table: &OccTable<Row>, id: usize, value: Row) {
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, id, value).unwrap();
    table.commit(&mut tx).unwrap();
}

fn assert_snapshot_scan_safe(
    table: &OccTable<Row>,
    index: &SecondaryIndex<usize>,
    tx: &mut OccTransaction<Row>,
    predicate: &IndexCompare,
    expected: &[usize],
) {
    match table.index_lookup(tx, index, predicate) {
        Ok(rows) => {
            assert_eq!(
                rows, expected,
                "a successful lookup must be snapshot-complete"
            );
            match table.commit(tx) {
                Ok(_) | Err(OccError::SerializationFailure) => {}
                Err(error) => panic!("unexpected commit error: {error}"),
            }
        }
        Err(OccError::SerializationFailure) => {
            let _ = table.abort(tx);
        }
        Err(error) => panic!("an infrastructure error is not an isolation rejection: {error}"),
    }
}

#[test]
fn absent_candidate_creation_rejects_one_disjoint_slot_writer() {
    let (_arena, table, index) = fixture(&[Row::vacant(), Row::vacant()]);
    let mut first = table.begin_transaction().unwrap();
    let mut second = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut first, &index, &eq(42)).is_empty());
    assert!(scan(&table, &mut second, &index, &eq(42)).is_empty());
    table.write(&mut first, 0, Row::live(42)).unwrap();
    table.write(&mut second, 1, Row::live(42)).unwrap();
    assert_eq!(table.commit(&mut first).unwrap(), 1);
    assert_eq!(
        table.commit(&mut second),
        Err(OccError::SerializationFailure)
    );
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(42), 0)]);
    assert_eq!(table.latest_value(1).unwrap(), Some(Row::vacant()));
}

#[test]
fn all_predicate_forms_detect_a_row_entering_their_result() {
    let cases = [
        (eq(10), 9, 10),
        (IndexCompare::Gt(IndexValue::I64(10)), 10, 11),
        (IndexCompare::Gte(IndexValue::I64(10)), 9, 10),
        (IndexCompare::Lt(IndexValue::I64(10)), 10, 9),
        (IndexCompare::Lte(IndexValue::I64(10)), 11, 10),
        (
            IndexCompare::In(vec![IndexValue::I64(10), IndexValue::I64(20)]),
            30,
            20,
        ),
    ];
    for (predicate, before, after) in cases {
        let (_arena, table, index) = fixture(&[Row::live(before), Row::vacant()]);
        let mut reader = table.begin_transaction().unwrap();
        assert!(scan(&table, &mut reader, &index, &predicate).is_empty());
        let mut unrelated = Row::vacant();
        unrelated.value = 1;
        table.write(&mut reader, 1, unrelated).unwrap();
        replace(&table, 0, Row::live(after));
        assert_eq!(
            table.commit(&mut reader),
            Err(OccError::SerializationFailure),
            "missed entering-row dependency for {predicate:?}",
        );
        assert_eq!(table.latest_value(1).unwrap(), Some(Row::vacant()));
        let mut fresh = table.begin_transaction().unwrap();
        assert_eq!(scan(&table, &mut fresh, &index, &predicate), vec![0]);
        table.commit(&mut fresh).unwrap();
    }
}

#[test]
fn old_snapshot_first_lookup_after_key_move_is_complete_or_rejected() {
    let (_arena, table, index) = fixture(&[Row::live(10)]);
    let mut old = table.begin_transaction().unwrap();
    let mut witness = table.begin_transaction().unwrap();
    replace(&table, 0, Row::live(20));
    assert_eq!(table.read(&mut witness, 0).unwrap(), Some(Row::live(10)));
    // The candidate reader itself has never performed a point read. Otherwise
    // existing concrete-row validation could accidentally mask this defect.
    assert_snapshot_scan_safe(&table, &index, &mut old, &eq(10), &[0]);
    table.abort(&mut witness).unwrap();
    let mut fresh = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut fresh, &index, &eq(10)).is_empty());
    assert_eq!(scan(&table, &mut fresh, &index, &eq(20)), vec![0]);
    table.commit(&mut fresh).unwrap();
}

#[test]
fn old_snapshot_first_lookup_after_delete_is_complete_or_rejected() {
    let (_arena, table, index) = fixture(&[Row::live(10)]);
    let mut old = table.begin_transaction().unwrap();
    replace(&table, 0, Row::vacant());
    assert_snapshot_scan_safe(&table, &index, &mut old, &eq(10), &[0]);
    assert!(index.try_entries().unwrap().is_empty());
}

#[test]
fn older_writer_committing_after_newer_writer_invalidates_later_snapshot() {
    let (_arena, table, index) = fixture(&[Row::vacant(), Row::vacant()]);
    let mut older_writer = table.begin_transaction().unwrap();
    table.write(&mut older_writer, 0, Row::live(42)).unwrap();
    replace(&table, 1, Row::live(42));
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut reader, &index, &eq(42)), vec![1]);
    table.commit(&mut older_writer).unwrap();
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    // A max(writer_txid) stamp would leave the older writer hidden behind the
    // newer writer's earlier publication and incorrectly accept this reader.
    let mut fresh = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut fresh, &index, &eq(42)), vec![0, 1]);
    table.commit(&mut fresh).unwrap();
}

#[test]
fn vacuum_preserves_snapshot_excluding_an_older_late_committing_writer() {
    let (_arena, table, index) = fixture(&[Row::live(42)]);
    let mut writer = table.begin_transaction().unwrap();
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(table.read(&mut reader, 0).unwrap(), Some(Row::live(42)));
    let mut updated = Row::live(42);
    updated.value = 99;
    table.write(&mut writer, 0, updated).unwrap();
    table.commit(&mut writer).unwrap();
    // The index key does not change. A key-change stamp therefore cannot hide
    // an incorrect vacuum horizon by simply rejecting this old reader.
    aerostore_core::run_vacuum_pass(&table).unwrap();
    assert_eq!(table.read(&mut reader, 0).unwrap(), Some(Row::live(42)));
    assert_eq!(scan(&table, &mut reader, &index, &eq(42)), vec![0]);
    table.abort(&mut reader).unwrap();
    let reclaimed = aerostore_core::run_vacuum_pass(&table).unwrap();
    assert!(
        !reclaimed.is_empty(),
        "ending the last snapshot must release its history"
    );
}

#[test]
fn newer_overlapping_reader_does_not_inherit_an_obsolete_vacuum_horizon() {
    let (_arena, table, index) = fixture(&[Row::live(42)]);
    let mut writer = table.begin_transaction().unwrap();
    let mut old_reader = table.begin_transaction().unwrap();
    let mut updated = Row::live(42);
    updated.value = 99;
    table.write(&mut writer, 0, updated).unwrap();
    table.commit(&mut writer).unwrap();
    let mut newer_reader = table.begin_transaction().unwrap();
    assert_eq!(table.read(&mut newer_reader, 0).unwrap(), Some(updated));
    assert!(aerostore_core::run_vacuum_pass(&table).unwrap().is_empty());
    table.abort(&mut old_reader).unwrap();
    // This reader began after the writer committed, so retaining its older
    // neighbor's inherited xmin would unnecessarily retain history forever
    // under a continuous chain of overlapping readers.
    assert!(!aerostore_core::run_vacuum_pass(&table).unwrap().is_empty());
    assert_eq!(scan(&table, &mut newer_reader, &index, &eq(42)), vec![0]);
    assert_eq!(table.read(&mut newer_reader, 0).unwrap(), Some(updated));
    table.commit(&mut newer_reader).unwrap();
}

#[test]
fn row_guard_outliving_commit_pins_its_version_without_unlocking_a_later_owner() {
    let (_arena, table, _index) = fixture(&[Row::live(42)]);
    let original_offset = table.row_head_offset(0).unwrap();
    let mut first = table.begin_transaction().unwrap();
    let old_guard = table.lock_for_update(&first, 0).unwrap();
    let mut next = Row::live(42);
    next.value = 1;
    table.write(&mut first, 0, next).unwrap();
    table.commit(&mut first).unwrap();
    assert!(
        aerostore_core::run_vacuum_pass(&table).unwrap().is_empty(),
        "a guard still refers to the retired row even after its transaction commits",
    );
    next.value = 2;
    replace(&table, 0, next);
    assert_ne!(table.row_head_offset(0).unwrap(), original_offset);
    let mut owner = table.begin_transaction().unwrap();
    let new_guard = table.lock_for_update(&owner, 0).unwrap();
    let mut before_drop = table.begin_transaction().unwrap();
    assert_eq!(
        table.read(&mut before_drop, 0),
        Err(OccError::SerializationFailure)
    );
    table.abort(&mut before_drop).unwrap();
    drop(old_guard);
    let mut after_drop = table.begin_transaction().unwrap();
    assert_eq!(
        table.read(&mut after_drop, 0),
        Err(OccError::SerializationFailure),
        "dropping the old guard must not clear the later owner's row lock",
    );
    table.abort(&mut after_drop).unwrap();
    let reclaimed = aerostore_core::run_vacuum_pass(&table).unwrap();
    assert!(
        reclaimed
            .iter()
            .any(|row| row.reclaimed_value == Row::live(42)),
        "dropping the old guard must permit its version to be reclaimed"
    );
    drop(new_guard);
    table.abort(&mut owner).unwrap();
    next.value = 3;
    replace(&table, 0, next);
    assert_eq!(table.latest_value(0).unwrap(), Some(next));
}

#[test]
fn different_equality_keys_do_not_force_table_wide_serialization() {
    let (_arena, table, index) = fixture(&[Row::vacant(), Row::vacant()]);
    let mut first = table.begin_transaction().unwrap();
    let mut second = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut first, &index, &eq(101)).is_empty());
    assert!(scan(&table, &mut second, &index, &eq(202)).is_empty());
    table.write(&mut first, 0, Row::live(101)).unwrap();
    table.write(&mut second, 1, Row::live(202)).unwrap();
    table.commit(&mut first).unwrap();
    table.commit(&mut second).unwrap();
    assert_eq!(index.try_entries().unwrap().len(), 2);
}

#[test]
fn repeated_in_members_and_many_colliding_keys_preserve_exact_candidates() {
    const ROWS: usize = 4100;
    let (_arena, table, index) = fixture(&vec![Row::vacant(); ROWS]);
    // More distinct keys than the bounded publication metadata can distinguish,
    // with duplicated In operands as well: collisions may reject transactions,
    // but must never duplicate candidates or acquire the same latch twice.
    let values: Vec<_> = (0..ROWS)
        .chain(0..ROWS)
        .map(|key| IndexValue::I64(key as i64))
        .collect();
    let predicate = IndexCompare::In(values);
    let mut absent_reader = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut absent_reader, &index, &predicate).is_empty());
    let mut writer = table.begin_transaction().unwrap();
    for row in 0..ROWS {
        table
            .write(&mut writer, row, Row::live(row as i64))
            .unwrap();
    }
    assert_eq!(
        scan(&table, &mut writer, &index, &predicate),
        (0..ROWS).collect::<Vec<_>>()
    );
    assert_eq!(table.commit(&mut writer).unwrap(), ROWS);
    assert_eq!(
        table.commit(&mut absent_reader),
        Err(OccError::SerializationFailure)
    );
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(
        scan(&table, &mut reader, &index, &predicate),
        (0..ROWS).collect::<Vec<_>>()
    );
    table.commit(&mut reader).unwrap();
    assert_eq!(index.try_entries().unwrap().len(), ROWS);
}

#[test]
fn ignored_lookup_conflict_cannot_be_cleared_by_savepoint_rollback() {
    let (_arena, table, index) = fixture(&[Row::live(10)]);
    let mut old = table.begin_transaction().unwrap();
    table.savepoint(&mut old, "before_search").unwrap();
    replace(&table, 0, Row::live(20));
    assert!(matches!(
        table.index_lookup(&mut old, &index, &eq(10)),
        Err(OccError::SerializationFailure)
    ));
    table.rollback_to(&mut old, "before_search").unwrap();
    assert_eq!(table.commit(&mut old), Err(OccError::SerializationFailure));
}

#[test]
fn own_writes_nested_savepoints_and_abort_preserve_exact_index_state() {
    let (_arena, table, index) = fixture(&[Row::live(10), Row::vacant()]);
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, Row::live(20)).unwrap();
    assert!(scan(&table, &mut tx, &index, &eq(10)).is_empty());
    assert_eq!(scan(&table, &mut tx, &index, &eq(20)), vec![0]);
    table.savepoint(&mut tx, "outer").unwrap();
    table.write(&mut tx, 0, Row::live(30)).unwrap();
    table.write(&mut tx, 1, Row::live(30)).unwrap();
    table.savepoint(&mut tx, "inner").unwrap();
    table.write(&mut tx, 0, Row::vacant()).unwrap();
    assert_eq!(scan(&table, &mut tx, &index, &eq(30)), vec![1]);
    table.rollback_to(&mut tx, "inner").unwrap();
    assert_eq!(scan(&table, &mut tx, &index, &eq(30)), vec![0, 1]);
    table.rollback_to(&mut tx, "outer").unwrap();
    assert!(scan(&table, &mut tx, &index, &eq(30)).is_empty());
    assert_eq!(scan(&table, &mut tx, &index, &eq(20)), vec![0]);
    table.commit(&mut tx).unwrap();
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(20), 0)]);
    let mut aborting = table.begin_transaction().unwrap();
    table.write(&mut aborting, 0, Row::live(40)).unwrap();
    assert_eq!(scan(&table, &mut aborting, &index, &eq(40)), vec![0]);
    table.abort(&mut aborting).unwrap();
    assert_eq!(table.latest_value(0).unwrap(), Some(Row::live(20)));
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(20), 0)]);
}

#[test]
fn rollback_to_savepoint_cannot_erase_an_outer_empty_predicate_dependency() {
    let (_arena, table, index) = fixture(&[Row::vacant(), Row::vacant()]);
    let mut reader = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut reader, &index, &eq(42)).is_empty());
    table.savepoint(&mut reader, "optional").unwrap();
    table.write(&mut reader, 1, Row::live(99)).unwrap();
    table.rollback_to(&mut reader, "optional").unwrap();
    replace(&table, 0, Row::live(42));
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    assert_eq!(table.latest_value(1).unwrap(), Some(Row::vacant()));
}

fn occasionally_invalid_key(row: &Row) -> Option<IndexValue> {
    row.active.then(|| {
        if row.key == 99 {
            IndexValue::String("x".repeat(129))
        } else {
            IndexValue::String(row.key.to_string())
        }
    })
}

#[test]
fn invalid_later_index_key_cannot_partially_publish_rows_or_earlier_indexes() {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
    table.seed_row(0, Row::live(10)).unwrap();
    let first = SecondaryIndex::<usize>::new_in_shared("key", Arc::clone(&arena));
    let second = SecondaryIndex::<usize>::new_in_shared("checked_key", arena);
    first.try_insert(IndexValue::I64(10), 0).unwrap();
    second
        .try_insert(IndexValue::String("10".into()), 0)
        .unwrap();
    table.bind_index(first.clone(), key).unwrap();
    table
        .bind_index(second.clone(), occasionally_invalid_key)
        .unwrap();
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, Row::live(99)).unwrap();
    let error = table
        .commit(&mut tx)
        .expect_err("invalid index key must fail");
    assert!(
        !matches!(error, OccError::SerializationFailure),
        "invalid keys are not retryable conflicts"
    );
    let _ = table.abort(&mut tx);
    assert_eq!(table.latest_value(0).unwrap(), Some(Row::live(10)));
    assert_eq!(first.try_entries().unwrap(), vec![(IndexValue::I64(10), 0)]);
    assert_eq!(
        second.try_entries().unwrap(),
        vec![(IndexValue::String("10".into()), 0)]
    );
    let mut fresh = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut fresh, &first, &eq(10)), vec![0]);
    table.commit(&mut fresh).unwrap();
}

#[test]
fn managed_indexes_reject_raw_mutations_through_attached_handles() {
    let (arena, table, index) = fixture(&[Row::live(10)]);
    let attached =
        SecondaryIndex::<usize>::from_existing("key", arena, index.header_offset()).unwrap();
    assert!(attached.try_insert(IndexValue::I64(20), 0).is_err());
    assert!(attached.try_remove(&IndexValue::I64(10), &0).is_err());
    assert!(attached
        .try_move_payload(&IndexValue::I64(10), IndexValue::I64(20), &0)
        .is_err());
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(10), 0)]);
    let mut tx = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut tx, &index, &eq(10)), vec![0]);
    table.commit(&mut tx).unwrap();
}

#[test]
fn attached_table_cannot_begin_without_complete_registered_index_set() {
    let (arena, table, index) = fixture(&[Row::live(10)]);
    let mut first = table.begin_transaction().unwrap();
    table.abort(&mut first).unwrap();
    let mut attached = OccTable::<Row>::from_existing(
        Arc::clone(&arena),
        table.shared_header_offset(),
        table.index_slot_offsets(),
    )
    .unwrap();
    assert!(matches!(
        attached.begin_transaction(),
        Err(OccError::IndexBindingsIncomplete)
    ));
    attached.bind_index(index, key).unwrap();
    let mut tx = attached.begin_transaction().unwrap();
    attached.abort(&mut tx).unwrap();
}

#[test]
fn rejected_wal_frame_preserves_row_and_managed_index() {
    let (arena, table, index) = fixture(&[Row::live(10)]);
    // The entire serialized commit cannot fit this intentionally tiny slot.
    // Rejection must roll back prepared destinations before row publication.
    let ring = SharedWalRing::<2, 32>::create(Arc::clone(&arena)).unwrap();
    let mut committer = OccCommitter::new_asynchronous(ring);
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, 0, Row::live(20)).unwrap();
    let result = committer.commit(&table, &mut tx);
    assert!(matches!(result, Err(WalWriterError::Ring(_))));
    assert_eq!(table.latest_value(0).unwrap(), Some(Row::live(10)));
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(10), 0)]);
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut reader, &index, &eq(10)), vec![0]);
    assert!(scan(&table, &mut reader, &index, &eq(20)).is_empty());
    table.commit(&mut reader).unwrap();
}

#[test]
fn concurrent_multirow_key_moves_never_return_a_mixed_successful_snapshot() {
    let (_arena, table, index) = fixture(&[Row::live(100), Row::live(100)]);
    let table = Arc::new(table);
    let start = Arc::new(std::sync::Barrier::new(2));
    let writer_table = Arc::clone(&table);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for iteration in 0..200 {
            let next = Row::live(if iteration % 2 == 0 { 200 } else { 100 });
            let mut committed = false;
            for _attempt in 0..10_000 {
                let mut tx = writer_table.begin_transaction().unwrap();
                writer_table.write(&mut tx, 0, next).unwrap();
                writer_table.write(&mut tx, 1, next).unwrap();
                match writer_table.commit(&mut tx) {
                    Ok(2) => {
                        committed = true;
                        break;
                    }
                    Err(OccError::SerializationFailure) => {
                        std::thread::yield_now();
                    }
                    result => panic!("unexpected writer outcome: {result:?}"),
                }
            }
            assert!(committed, "reader contention prevented writer progress");
        }
    });
    start.wait();
    for _attempt in 0..1_000 {
        let mut tx = table.begin_transaction().unwrap();
        let first = table.read(&mut tx, 0).unwrap().unwrap();
        let second = table.read(&mut tx, 1).unwrap().unwrap();
        assert_eq!(first, second, "row transaction itself must remain atomic");
        let low = table.index_lookup(&mut tx, &index, &eq(100));
        let high = table.index_lookup(&mut tx, &index, &eq(200));
        match (low, high) {
            (Ok(low), Ok(high)) => {
                assert_eq!(low, if first.key == 100 { vec![0, 1] } else { vec![] });
                assert_eq!(high, if first.key == 200 { vec![0, 1] } else { vec![] });
                match table.commit(&mut tx) {
                    Ok(_) | Err(OccError::SerializationFailure) => {}
                    result => panic!("unexpected reader outcome: {result:?}"),
                }
            }
            (Err(OccError::SerializationFailure), _) | (_, Err(OccError::SerializationFailure)) => {
                let _ = table.abort(&mut tx);
            }
            result => panic!("unexpected lookup outcomes: {result:?}"),
        }
    }
    writer.join().unwrap();
    let mut entries = index.try_entries().unwrap();
    entries.sort();
    assert_eq!(
        entries,
        vec![(IndexValue::I64(100), 0), (IndexValue::I64(100), 1)]
    );
}

#[test]
fn predicate_publication_is_visible_across_independently_mapped_processes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("transactional-index.mmap");
    let arena = Arc::new(
        aerostore_core::map_tmpfs_shared(&path, 16 << 20)
            .unwrap()
            .arena,
    );
    let (table, index) = fixture_in(arena, &[Row::live(10)]);
    let mut old = table.begin_transaction().unwrap();
    let mut earlier_scan = table.begin_transaction().unwrap();
    assert!(scan(&table, &mut earlier_scan, &index, &eq(20)).is_empty());
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "transactional_index_child", "--nocapture"])
        .env("AEROSTORE_TX_INDEX_TEST_PATH", &path)
        .env(
            "AEROSTORE_TX_INDEX_TEST_HEADER",
            table.shared_header_offset().to_string(),
        )
        .env(
            "AEROSTORE_TX_INDEX_TEST_SLOT",
            table.index_slot_offsets()[0].to_string(),
        )
        .env(
            "AEROSTORE_TX_INDEX_TEST_INDEX",
            index.header_offset().to_string(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_snapshot_scan_safe(&table, &index, &mut old, &eq(10), &[0]);
    assert_eq!(
        table.commit(&mut earlier_scan),
        Err(OccError::SerializationFailure)
    );
    let mut fresh = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut fresh, &index, &eq(20)), vec![0]);
    table.commit(&mut fresh).unwrap();
}

#[test]
fn transactional_index_child() {
    let Some(path) = std::env::var_os("AEROSTORE_TX_INDEX_TEST_PATH") else {
        return;
    };
    let parse = |name| std::env::var(name).unwrap().parse::<u32>().unwrap();
    let header = parse("AEROSTORE_TX_INDEX_TEST_HEADER");
    let slot = parse("AEROSTORE_TX_INDEX_TEST_SLOT");
    let index_header = parse("AEROSTORE_TX_INDEX_TEST_INDEX");
    let mapped = aerostore_core::map_tmpfs_shared(path, 16 << 20).unwrap();
    assert_eq!(mapped.mode, aerostore_core::TmpfsAttachMode::WarmStart);
    let arena = Arc::new(mapped.arena);
    let mut table = OccTable::<Row>::from_existing(Arc::clone(&arena), header, vec![slot]).unwrap();
    let index = SecondaryIndex::<usize>::from_existing("key", arena, index_header).unwrap();
    table.bind_index(index.clone(), key).unwrap();
    replace(&table, 0, Row::live(20));
    let mut tx = table.begin_transaction().unwrap();
    assert_eq!(scan(&table, &mut tx, &index, &eq(20)), vec![0]);
    table.commit(&mut tx).unwrap();
}
