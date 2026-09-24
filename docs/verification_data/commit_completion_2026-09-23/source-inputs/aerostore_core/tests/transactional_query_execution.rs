//! Regression coverage of the public STAPI planner/executor, rather than only
//! direct OCC index calls. A chosen access path cannot bypass isolation or drop
//! its driving predicate when an index is unavailable or unsafe to use.

use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use aerostore_core::{
    IndexValue, OccError, OccTable, PlannerError, RouteKind, RuleBasedOptimizer, SchemaCatalog,
    SecondaryIndex, ShmArena, ShmPrimaryKeyMap, SnapshotExecutionMode, StapiRow, StapiValue,
};

thread_local! {
    // Runs once during execution's driver-filter recheck, after index_lookup
    // has released its native predicate latches. This permits a deterministic
    // validation race without test-only hooks in production APIs.
    static ON_KEY_FILTER: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Row {
    pk: [u8; 8],
    key: i64,
    value: i64,
}

impl Row {
    fn new(pk: &str, key: i64, value: i64) -> Self {
        assert!(pk.len() <= 8);
        let mut encoded = [0; 8];
        encoded[..pk.len()].copy_from_slice(pk.as_bytes());
        Self {
            pk: encoded,
            key,
            value,
        }
    }

    fn pk_text(&self) -> String {
        let end = self.pk.iter().position(|byte| *byte == 0).unwrap_or(8);
        String::from_utf8(self.pk[..end].to_vec()).unwrap()
    }
}

impl StapiRow for Row {
    fn has_field(field: &str) -> bool {
        matches!(field, "pk" | "key" | "value")
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        if field == "key" {
            let callback = ON_KEY_FILTER.with(|callback| callback.borrow_mut().take());
            if let Some(callback) = callback {
                callback();
            }
        }
        match field {
            "pk" => Some(StapiValue::Text(self.pk_text())),
            "key" => Some(StapiValue::Int(self.key)),
            "value" => Some(StapiValue::Int(self.value)),
            _ => None,
        }
    }
}

fn key(row: &Row) -> Option<IndexValue> {
    Some(IndexValue::I64(row.key))
}
fn pk(row: &Row) -> Option<IndexValue> {
    Some(IndexValue::String(row.pk_text()))
}

fn table(rows: &[Row]) -> (Arc<ShmArena>, OccTable<Row>) {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let table = OccTable::new(Arc::clone(&arena), rows.len()).unwrap();
    for (id, row) in rows.iter().copied().enumerate() {
        table.seed_row(id, row).unwrap();
    }
    (arena, table)
}

fn index_for(
    table: &mut OccTable<Row>,
    field: &'static str,
    extractor: fn(&Row) -> Option<IndexValue>,
) -> Arc<SecondaryIndex<usize>> {
    let index = Arc::new(SecondaryIndex::new_in_shared(
        field,
        Arc::clone(table.shared_arena()),
    ));
    for (id, row) in table.snapshot_latest_rows().unwrap() {
        if let Some(value) = extractor(&row) {
            index.try_insert(value, id).unwrap();
        }
    }
    table.bind_index(index.as_ref().clone(), extractor).unwrap();
    index
}

fn replace(table: &OccTable<Row>, id: usize, row: Row) {
    let mut tx = table.begin_transaction().unwrap();
    table.write(&mut tx, id, row).unwrap();
    table.commit(&mut tx).unwrap();
}

#[test]
fn missing_primary_map_entry_still_enrolls_the_empty_predicate() {
    let (arena, table) = table(&[Row::new("", 0, 0), Row::new("", 0, 0)]);
    let map = Arc::new(ShmPrimaryKeyMap::new_in_shared(arena, 16, 2).unwrap());
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk").with_primary_key_map(map))
        .compile_from_stapi("-compare {{= pk NEW}}")
        .unwrap();
    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    let mut reader = table.begin_transaction().unwrap();
    assert!(plan.execute(&table, &mut reader).unwrap().is_empty());
    table.write(&mut reader, 1, Row::new("NEW", 10, 1)).unwrap();
    replace(&table, 0, Row::new("NEW", 10, 2));
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    // Even a permanently missing map entry cannot hide committed rows.
    assert_eq!(
        plan.execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
            .unwrap(),
        vec![Row::new("NEW", 10, 2)]
    );
}

#[test]
fn primary_key_plan_uses_registered_predicate_when_map_is_stale() {
    let (arena, mut table) = table(&[Row::new("OLD", 10, 1)]);
    let map = Arc::new(ShmPrimaryKeyMap::new_in_shared(arena, 16, 1).unwrap());
    map.insert_existing("OLD", 0).unwrap();
    let index = index_for(&mut table, "pk", pk);
    let optimizer = RuleBasedOptimizer::<Row>::new(
        SchemaCatalog::new("pk")
            .with_primary_key_map(map)
            .with_index("pk", index),
    );
    let plan = optimizer
        .compile_from_stapi("-compare {{= pk NEW}}")
        .unwrap();
    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    replace(&table, 0, Row::new("NEW", 20, 2));
    assert_eq!(
        plan.execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
            .unwrap(),
        vec![Row::new("NEW", 20, 2)]
    );
}

#[test]
fn numeric_primary_key_keeps_its_type_when_using_a_registered_index() {
    let (arena, mut table) = table(&[Row::new("A", 10, 1)]);
    let map = Arc::new(ShmPrimaryKeyMap::new_in_shared(arena, 16, 1).unwrap());
    map.insert_existing("10", 0).unwrap();
    let index = index_for(&mut table, "key", key);
    let plan = RuleBasedOptimizer::<Row>::new(
        SchemaCatalog::new("key")
            .with_primary_key_map(map)
            .with_index("key", index),
    )
    .compile_from_stapi("-compare {{= key 10}}")
    .unwrap();
    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    assert_eq!(
        plan.execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
            .unwrap(),
        vec![Row::new("A", 10, 1)],
    );
}

#[test]
fn indexed_plan_cannot_silently_lose_an_old_snapshots_moved_key() {
    let (_arena, mut table) = table(&[Row::new("A", 10, 1)]);
    let index = index_for(&mut table, "key", key);
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk").with_index("key", index))
        .compile_from_stapi("-compare {{= key 10}}")
        .unwrap();
    assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
    let mut old = table.begin_transaction().unwrap();
    replace(&table, 0, Row::new("A", 20, 2));
    match plan.execute(&table, &mut old) {
        Ok(rows) => assert_eq!(rows, vec![Row::new("A", 10, 1)]),
        Err(PlannerError::SerializationFailure) => {}
        Err(error) => panic!("unexpected execution failure: {error}"),
    }
    table.abort(&mut old).unwrap();
}

#[test]
fn unbound_stale_index_falls_back_and_rechecks_its_driving_filter() {
    let (arena, table) = table(&[
        Row::new("A", 10, 1),
        Row::new("B", 20, 2),
        Row::new("C", 10, 3),
    ]);
    let index = Arc::new(SecondaryIndex::new_in_shared("key", arena));
    // Both omission and a false positive: trusting these postings would return
    // the wrong row, while falling back but dropping the driver returns all rows.
    index.try_insert(IndexValue::I64(10), 1).unwrap();
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk").with_index("key", index))
        .compile_from_stapi("-compare {{= key 10}} -sort value")
        .unwrap();
    assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
    let expected = vec![Row::new("A", 10, 1), Row::new("C", 10, 3)];
    assert_eq!(
        plan.execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
            .unwrap(),
        expected
    );
    assert_eq!(
        plan.execute_with_snapshot_mode(
            &table,
            SnapshotExecutionMode::ChunkedEventual { chunk_rows: 1 }
        )
        .unwrap(),
        expected
    );
}

#[test]
fn strict_snapshot_retries_a_filter_time_write_before_returning_results() {
    let (arena, mut table) = table(&[Row::new("A", 10, 1)]);
    let index = index_for(&mut table, "key", key);
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk").with_index("key", index))
        .compile_from_stapi("-compare {{= key 10}}")
        .unwrap();
    let table = Arc::new(table);
    let writer_table = Arc::clone(&table);
    let invoked = Arc::new(AtomicBool::new(false));
    let invoked_clone = Arc::clone(&invoked);
    ON_KEY_FILTER.with(|callback| {
        *callback.borrow_mut() = Some(Box::new(move || {
            replace(&writer_table, 0, Row::new("A", 20, 2));
            invoked_clone.store(true, Ordering::Release);
        }))
    });
    let rows = plan
        .execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
        .unwrap();
    assert!(
        invoked.load(Ordering::Acquire),
        "must actually exercise validation race"
    );
    assert!(
        rows.is_empty(),
        "must retry after conflict rather than return an unvalidated result"
    );
    assert_eq!(
        arena.create_snapshot().len(),
        0,
        "retry must release both registrations"
    );
}

#[test]
fn strict_query_errors_release_transaction_registrations() {
    let (arena, mut table) = table(&[Row::new("A", 10, 1)]);
    let index = index_for(&mut table, "pk", pk);
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk").with_index("pk", index))
        .compile_from_stapi(&format!("-compare {{{{= pk {}}}}}", "x".repeat(129)))
        .unwrap();
    for _ in 0..32 {
        let error = plan
            .execute_with_snapshot_mode(&table, SnapshotExecutionMode::StrictSnapshot)
            .unwrap_err();
        assert!(
            matches!(error, PlannerError::Occ(_)),
            "invalid keys are fatal errors: {error}"
        );
        assert_eq!(
            arena.create_snapshot().len(),
            0,
            "error leaked an active transaction"
        );
    }
}

#[test]
fn chunked_query_read_error_releases_only_its_own_registration() {
    let (arena, table) = table(&[Row::new("A", 10, 1)]);
    let plan = RuleBasedOptimizer::<Row>::new(SchemaCatalog::new("pk"))
        .compile_from_stapi("-compare {{= key 10}}")
        .unwrap();
    let mut owner = table.begin_transaction().unwrap();
    let guard = table.lock_for_update(&owner, 0).unwrap();
    for _ in 0..16 {
        assert!(matches!(
            plan.execute_with_snapshot_mode(
                &table,
                SnapshotExecutionMode::ChunkedEventual { chunk_rows: 1 }
            ),
            Err(PlannerError::SerializationFailure)
        ));
        assert_eq!(arena.create_snapshot().len(), 1);
    }
    drop(guard);
    table.abort(&mut owner).unwrap();
    assert_eq!(arena.create_snapshot().len(), 0);
}
