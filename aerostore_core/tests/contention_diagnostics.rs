//! Controlled schedules identify conservative conflicts without changing the
//! production failure enum or adding counters to its verified hot path.
use aerostore_core::{IndexCompare, IndexValue, OccError, OccTable, SecondaryIndex, ShmArena};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Row {
    key: i64,
    payload: i64,
}
fn key(row: &Row) -> Option<IndexValue> {
    Some(IndexValue::I64(row.key))
}
fn fixture() -> (Arc<ShmArena>, OccTable<Row>, SecondaryIndex<usize>) {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let mut table = OccTable::new(arena.clone(), 3).unwrap();
    let index = SecondaryIndex::new_in_shared("controlled_key", arena.clone());
    for (id, key) in [10, 20, 30].into_iter().enumerate() {
        table.seed_row(id, Row { key, payload: 0 }).unwrap();
        index.try_insert(IndexValue::I64(key), id).unwrap();
    }
    table.bind_index(index.clone(), key).unwrap();
    (arena, table, index)
}
fn eq(k: i64) -> IndexCompare {
    IndexCompare::Eq(IndexValue::I64(k))
}
fn range() -> IndexCompare {
    IndexCompare::Lte(IndexValue::I64(10))
}
fn record(name: &str, value: Value) {
    if let Ok(dir) = std::env::var("AEROSTORE_CONTENTION_DIAGNOSTICS_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(
            dir.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn completed_unrelated_key_move_rejects_old_range_before_any_materialization() {
    let (arena, table, index) = fixture();
    let mut equality = table.begin_transaction().unwrap();
    let mut broad = table.begin_transaction().unwrap();
    let mut writer = table.begin_transaction().unwrap();
    table
        .write(
            &mut writer,
            2,
            Row {
                key: 31,
                payload: 0,
            },
        )
        .unwrap();
    table.commit(&mut writer).unwrap();
    // The writer is finished: no concurrent latch acquisition or scan race can
    // explain this rejection. Neither reader has read a concrete row yet.
    assert_eq!(
        table.index_lookup(&mut equality, &index, &eq(10)).unwrap(),
        vec![0]
    );
    assert_eq!(
        table.index_lookup(&mut broad, &index, &range()),
        Err(OccError::SerializationFailure)
    );
    table.abort(&mut broad).unwrap();
    table.commit(&mut equality).unwrap();
    let mut fresh = table.begin_transaction().unwrap();
    assert_eq!(
        table.index_lookup(&mut fresh, &index, &range()).unwrap(),
        vec![0]
    );
    table.commit(&mut fresh).unwrap();
    assert!(arena.create_snapshot().is_empty());
    record(
        "post_snapshot_range_rejection",
        json!({"passed":true,"writer_finished_before_first_lookup":true,
        "queried_range":"key <= 10","changed_row":{"id":2,"old_key":30,"new_key":31},
        "logical_results_before_and_after":[0],"old_equality_succeeds":true,"old_range_rejects":true,
        "fresh_range_succeeds":true,"diagnosis":"conservative post-snapshot predicate stamp rejection, without active writer latches or scan overlap",
        "limit":"isolates an available cause, not its fraction of sustained workload retries"}),
    );
}

#[test]
fn unchanged_captured_range_rejects_at_commit_after_disjoint_key_move() {
    let (arena, table, index) = fixture();
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(
        table.index_lookup(&mut reader, &index, &range()).unwrap(),
        vec![0]
    );
    let mut writer = table.begin_transaction().unwrap();
    table
        .write(
            &mut writer,
            2,
            Row {
                key: 31,
                payload: 0,
            },
        )
        .unwrap();
    table.commit(&mut writer).unwrap();
    assert_eq!(
        table.read(&mut reader, 0).unwrap(),
        Some(Row {
            key: 10,
            payload: 0
        })
    );
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    table.abort(&mut reader).unwrap();
    assert!(arena.create_snapshot().is_empty());
    record(
        "captured_range_validation",
        json!({"passed":true,"returned_rows":[0],"changed_row":2,
        "concrete_returned_row_unchanged":true,"commit_rejected":true,
        "diagnosis":"captured broad predicate dependency invalidation by an unrelated completed key move"}),
    );
}

#[test]
fn concrete_row_validation_is_separate_from_predicate_rejection() {
    let (arena, table, _) = fixture();
    let mut reader = table.begin_transaction().unwrap();
    table.read(&mut reader, 0).unwrap();
    let mut writer = table.begin_transaction().unwrap();
    table
        .write(
            &mut writer,
            0,
            Row {
                key: 10,
                payload: 1,
            },
        )
        .unwrap();
    table.commit(&mut writer).unwrap();
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    table.abort(&mut reader).unwrap();
    assert!(arena.create_snapshot().is_empty());
    record(
        "concrete_row_validation",
        json!({"passed":true,"index_queries":0,"key_changed":false,
        "payload_changed":true,"commit_rejected":true,"diagnosis":"concrete row dependency validation; no query predicate was registered"}),
    );
}

#[test]
fn historical_key_move_retains_row_history_but_rejects_old_index_search() {
    let (arena, table, index) = fixture();
    let mut reader = table.begin_transaction().unwrap();
    let mut writer = table.begin_transaction().unwrap();
    table
        .write(
            &mut writer,
            0,
            Row {
                key: 11,
                payload: 1,
            },
        )
        .unwrap();
    table.commit(&mut writer).unwrap();
    assert_eq!(
        table.read(&mut reader, 0).unwrap(),
        Some(Row {
            key: 10,
            payload: 0
        })
    );
    assert_eq!(
        table.index_lookup(&mut reader, &index, &eq(10)),
        Err(OccError::SerializationFailure)
    );
    table.abort(&mut reader).unwrap();
    let mut fresh = table.begin_transaction().unwrap();
    assert!(table
        .index_lookup(&mut fresh, &index, &eq(10))
        .unwrap()
        .is_empty());
    assert_eq!(
        table.index_lookup(&mut fresh, &index, &eq(11)).unwrap(),
        vec![0]
    );
    table.commit(&mut fresh).unwrap();
    assert!(arena.create_snapshot().is_empty());
    record(
        "historical_index_rejection",
        json!({"passed":true,"old_point_read_complete":true,
        "old_key_query_explicitly_rejected":true,"fresh_key_queries_complete":true,
        "diagnosis":"current-posting index cannot serve this historical key lookup; safe rejection, not a successful incomplete result"}),
    );
}
