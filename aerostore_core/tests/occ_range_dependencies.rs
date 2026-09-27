//! Semantic schedules for the optional ordered publication-dependency policy.
//! The hashed policy remains an explicit control. These checks assert complete
//! results and commit outcomes, not measured speed or whole-engine verification.
#![cfg(target_os = "linux")]

use aerostore_core::shm_index::IndexPublicationPolicy;
use aerostore_core::{IndexCompare, IndexValue, OccError, OccTable, SecondaryIndex, ShmArena};
use serde::{Deserialize, Serialize};
use std::process::Command;
use std::sync::Arc;

const ORDERED: IndexPublicationPolicy = IndexPublicationPolicy::OrderedI64 {
    origin: 0,
    width: 10,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Row {
    key: i64,
    active: bool,
    payload: u64,
}
impl Row {
    fn live(key: i64) -> Self {
        Self {
            key,
            active: true,
            payload: 0,
        }
    }
    fn vacant() -> Self {
        Self {
            key: 0,
            active: false,
            payload: 0,
        }
    }
}
fn key(row: &Row) -> Option<IndexValue> {
    row.active.then_some(IndexValue::I64(row.key))
}
fn lte(at: i64) -> IndexCompare {
    IndexCompare::Lte(IndexValue::I64(at))
}
fn fixture_in(
    arena: Arc<ShmArena>,
    policy: IndexPublicationPolicy,
    rows: &[Row],
) -> (OccTable<Row>, SecondaryIndex<usize>) {
    let mut table = OccTable::new(arena.clone(), rows.len()).unwrap();
    let index =
        SecondaryIndex::new_in_shared_with_publication_policy("ordered", arena, policy).unwrap();
    assert_eq!(index.publication_policy().unwrap(), policy);
    for (id, row) in rows.iter().copied().enumerate() {
        table.seed_row(id, row).unwrap();
        if let Some(key) = key(&row) {
            index.try_insert(key, id).unwrap();
        }
    }
    table.bind_index(index.clone(), key).unwrap();
    (table, index)
}
fn fixture(
    policy: IndexPublicationPolicy,
    rows: &[Row],
) -> (Arc<ShmArena>, OccTable<Row>, SecondaryIndex<usize>) {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let (table, index) = fixture_in(arena.clone(), policy, rows);
    (arena, table, index)
}
fn replace(table: &OccTable<Row>, id: usize, row: Row) {
    let mut writer = table.begin_transaction().unwrap();
    table.write(&mut writer, id, row).unwrap();
    table.commit(&mut writer).unwrap();
}
fn fresh_rows(
    table: &OccTable<Row>,
    index: &SecondaryIndex<usize>,
    query: &IndexCompare,
) -> Vec<usize> {
    let mut reader = table.begin_transaction().unwrap();
    let rows = table.index_lookup(&mut reader, index, query).unwrap();
    table.commit(&mut reader).unwrap();
    rows
}

#[test]
fn disjoint_move_allows_old_first_lookup_and_captured_commit_only_in_ordered_control() {
    for policy in [IndexPublicationPolicy::Hashed, ORDERED] {
        for before_first_lookup in [true, false] {
            let (arena, table, index) =
                fixture(policy, &[Row::live(10), Row::live(30), Row::vacant()]);
            let mut reader = table.begin_transaction().unwrap();
            if !before_first_lookup {
                assert_eq!(
                    table.index_lookup(&mut reader, &index, &lte(10)).unwrap(),
                    vec![0]
                );
            }
            // The staged, non-indexed write makes commit rejection observable.
            let mut staged = Row::vacant();
            staged.payload = 7;
            table.write(&mut reader, 2, staged).unwrap();
            replace(&table, 1, Row::live(40));
            if before_first_lookup {
                let result = table.index_lookup(&mut reader, &index, &lte(10));
                if policy == ORDERED {
                    assert_eq!(result, Ok(vec![0]));
                } else {
                    assert_eq!(result, Err(OccError::SerializationFailure));
                }
            }
            if policy == ORDERED {
                assert_eq!(table.commit(&mut reader), Ok(1));
                assert_eq!(table.latest_value(2).unwrap(), Some(staged));
            } else {
                assert_eq!(
                    table.commit(&mut reader),
                    Err(OccError::SerializationFailure)
                );
                assert_eq!(table.latest_value(2).unwrap(), Some(Row::vacant()));
            }
            assert_eq!(fresh_rows(&table, &index, &lte(10)), vec![0]);
            assert!(arena.create_snapshot().is_empty());
        }
    }
}

#[test]
fn disjoint_values_in_one_coarse_boundary_bucket_remain_conservative() {
    let (_arena, table, index) = fixture(ORDERED, &[Row::live(10), Row::live(18)]);
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(
        table.index_lookup(&mut reader, &index, &lte(10)).unwrap(),
        vec![0]
    );
    // 18 and 19 are outside the logical result, but share its boundary interval.
    replace(&table, 1, Row::live(19));
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    assert_eq!(fresh_rows(&table, &index, &lte(10)), vec![0]);
}

#[test]
fn empty_predicates_reject_creation_and_every_entering_range_boundary() {
    let cases = [
        (lte(10), Row::vacant(), Row::live(9)),
        (
            IndexCompare::Lt(IndexValue::I64(10)),
            Row::live(10),
            Row::live(9),
        ),
        (
            IndexCompare::Lte(IndexValue::I64(10)),
            Row::live(11),
            Row::live(10),
        ),
        (
            IndexCompare::Gt(IndexValue::I64(10)),
            Row::live(10),
            Row::live(11),
        ),
        (
            IndexCompare::Gte(IndexValue::I64(10)),
            Row::live(9),
            Row::live(10),
        ),
    ];
    for (predicate, before, after) in cases {
        let (_arena, table, index) = fixture(ORDERED, &[before, Row::vacant()]);
        let mut reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut reader, &index, &predicate)
            .unwrap()
            .is_empty());
        let mut staged = Row::vacant();
        staged.payload = 99;
        table.write(&mut reader, 1, staged).unwrap();
        replace(&table, 0, after);
        assert_eq!(
            table.commit(&mut reader),
            Err(OccError::SerializationFailure),
            "{predicate:?}"
        );
        assert_eq!(table.latest_value(1).unwrap(), Some(Row::vacant()));
        assert_eq!(fresh_rows(&table, &index, &predicate), vec![0]);
    }
}

#[test]
fn sentinel_and_extreme_configurations_preserve_matching_phantom_dependencies() {
    for policy in [
        ORDERED,
        IndexPublicationPolicy::OrderedI64 {
            origin: i64::MIN,
            width: u64::MAX,
        },
        IndexPublicationPolicy::OrderedI64 {
            origin: i64::MAX,
            width: 1,
        },
    ] {
        for value in [i64::MIN, -1, 0, 9, 10, 40_929, 40_930, i64::MAX] {
            let mut queries = vec![
                IndexCompare::Lte(IndexValue::I64(value)),
                IndexCompare::Gte(IndexValue::I64(value)),
            ];
            if let Some(bound) = value.checked_add(1) {
                queries.push(IndexCompare::Lt(IndexValue::I64(bound)));
            }
            if let Some(bound) = value.checked_sub(1) {
                queries.push(IndexCompare::Gt(IndexValue::I64(bound)));
            }
            for query in queries {
                let (_arena, table, index) = fixture(policy, &[Row::vacant()]);
                let mut reader = table.begin_transaction().unwrap();
                assert!(table
                    .index_lookup(&mut reader, &index, &query)
                    .unwrap()
                    .is_empty());
                replace(&table, 0, Row::live(value));
                assert_eq!(
                    table.commit(&mut reader),
                    Err(OccError::SerializationFailure),
                    "{policy:?} {query:?} inserted {value}"
                );
                assert_eq!(fresh_rows(&table, &index, &query), vec![0]);
            }
        }
    }
}

#[test]
fn repeated_in_members_preserve_union_dependencies_and_empty_in_has_no_phantom() {
    for (query, moved, must_reject) in [
        (
            IndexCompare::In(vec![
                IndexValue::I64(10),
                IndexValue::I64(30),
                IndexValue::I64(10),
            ]),
            10,
            true,
        ),
        (
            IndexCompare::In(vec![
                IndexValue::I64(10),
                IndexValue::I64(30),
                IndexValue::I64(10),
            ]),
            60,
            false,
        ),
        (IndexCompare::In(vec![]), 10, false),
    ] {
        let (_arena, table, index) = fixture(ORDERED, &[Row::live(50)]);
        let mut reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut reader, &index, &query)
            .unwrap()
            .is_empty());
        replace(&table, 0, Row::live(moved));
        if must_reject {
            assert_eq!(
                table.commit(&mut reader),
                Err(OccError::SerializationFailure)
            );
        } else {
            assert_eq!(table.commit(&mut reader), Ok(0));
        }
        assert_eq!(
            fresh_rows(&table, &index, &query),
            if must_reject { vec![0] } else { vec![] }
        );
    }
}

#[test]
fn removed_or_moved_historical_candidates_still_reject_before_any_row_read() {
    for after in [Row::vacant(), Row::live(30)] {
        let (_arena, table, index) = fixture(ORDERED, &[Row::live(9)]);
        let mut old = table.begin_transaction().unwrap();
        let mut witness = table.begin_transaction().unwrap();
        replace(&table, 0, after);
        // A separate point reader demonstrates the old version remains visible.
        // The index reader has no concrete row dependency to hide a missed stamp.
        assert_eq!(table.read(&mut witness, 0).unwrap(), Some(Row::live(9)));
        assert_eq!(
            table.index_lookup(&mut old, &index, &lte(10)),
            Err(OccError::SerializationFailure)
        );
        table.abort(&mut old).unwrap();
        table.abort(&mut witness).unwrap();
        assert!(fresh_rows(&table, &index, &lte(10)).is_empty());
    }
}

#[test]
fn moves_publish_both_source_and_destination_dependencies() {
    for (before, after) in [(9, 31), (31, 9)] {
        let (_arena, table, index) = fixture(ORDERED, &[Row::live(before)]);
        let mut lower = table.begin_transaction().unwrap();
        let mut upper = table.begin_transaction().unwrap();
        let high = IndexCompare::Gte(IndexValue::I64(30));
        assert_eq!(
            table.index_lookup(&mut lower, &index, &lte(10)).unwrap(),
            if before == 9 { vec![0] } else { vec![] }
        );
        assert_eq!(
            table.index_lookup(&mut upper, &index, &high).unwrap(),
            if before == 31 { vec![0] } else { vec![] }
        );
        replace(&table, 0, Row::live(after));
        assert_eq!(
            table.commit(&mut lower),
            Err(OccError::SerializationFailure)
        );
        assert_eq!(
            table.commit(&mut upper),
            Err(OccError::SerializationFailure)
        );
        assert_eq!(
            fresh_rows(&table, &index, &lte(10)),
            if after == 9 { vec![0] } else { vec![] }
        );
        assert_eq!(
            fresh_rows(&table, &index, &high),
            if after == 31 { vec![0] } else { vec![] }
        );
    }
}

#[test]
fn older_late_writer_is_not_hidden_behind_newer_publication_stamp() {
    let (_arena, table, index) = fixture(ORDERED, &[Row::vacant(), Row::vacant()]);
    let mut older = table.begin_transaction().unwrap();
    table.write(&mut older, 0, Row::live(9)).unwrap();
    replace(&table, 1, Row::live(9));
    let mut reader = table.begin_transaction().unwrap();
    assert_eq!(
        table.index_lookup(&mut reader, &index, &lte(10)).unwrap(),
        vec![1]
    );
    table.commit(&mut older).unwrap();
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
    assert_eq!(fresh_rows(&table, &index, &lte(10)), vec![0, 1]);
}

#[test]
fn own_write_savepoints_and_rolled_back_empty_search_keep_semantics() {
    let (_arena, table, index) = fixture(ORDERED, &[Row::live(30)]);
    let mut writer = table.begin_transaction().unwrap();
    table.write(&mut writer, 0, Row::live(9)).unwrap();
    assert_eq!(
        table.index_lookup(&mut writer, &index, &lte(10)).unwrap(),
        vec![0]
    );
    table.savepoint(&mut writer, "inside").unwrap();
    table.write(&mut writer, 0, Row::live(31)).unwrap();
    assert!(table
        .index_lookup(&mut writer, &index, &lte(10))
        .unwrap()
        .is_empty());
    table.rollback_to(&mut writer, "inside").unwrap();
    assert_eq!(
        table.index_lookup(&mut writer, &index, &lte(10)).unwrap(),
        vec![0]
    );
    table.abort(&mut writer).unwrap();
    assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(30), 0)]);

    let mut reader = table.begin_transaction().unwrap();
    assert!(table
        .index_lookup(&mut reader, &index, &lte(10))
        .unwrap()
        .is_empty());
    table.savepoint(&mut reader, "after-empty-search").unwrap();
    replace(&table, 0, Row::live(9));
    table
        .rollback_to(&mut reader, "after-empty-search")
        .unwrap();
    assert_eq!(
        table.commit(&mut reader),
        Err(OccError::SerializationFailure)
    );
}

#[derive(Clone, Copy, Debug)]
struct MixedRow {
    kind: u8,
    signed: i64,
    unsigned: u64,
    text: u8,
}
fn mixed_key(row: &MixedRow) -> Option<IndexValue> {
    Some(match row.kind {
        0 => IndexValue::I64(row.signed),
        1 => IndexValue::U64(row.unsigned),
        2 => IndexValue::String(
            match row.text {
                0 => "",
                1 => "a",
                2 => "a\0",
                _ => "é",
            }
            .into(),
        ),
        _ => return None,
    })
}
fn matches(query: &IndexCompare, value: &IndexValue) -> bool {
    match query {
        IndexCompare::Eq(bound) => value == bound,
        IndexCompare::Lt(bound) => value < bound,
        IndexCompare::Lte(bound) => value <= bound,
        IndexCompare::Gt(bound) => value > bound,
        IndexCompare::Gte(bound) => value >= bound,
        IndexCompare::In(values) => values.contains(value),
    }
}

#[test]
fn mixed_types_limits_and_repeated_in_keys_match_an_independent_ordered_model() {
    let mut rows = Vec::new();
    for signed in [i64::MIN, -1, 0, 9, 10, 11, 40_929, 40_930, i64::MAX] {
        rows.push(MixedRow {
            kind: 0,
            signed,
            unsigned: 0,
            text: 0,
        });
    }
    for unsigned in [0, 1, u64::MAX] {
        rows.push(MixedRow {
            kind: 1,
            signed: 0,
            unsigned,
            text: 0,
        });
    }
    for text in 0..4 {
        rows.push(MixedRow {
            kind: 2,
            signed: 0,
            unsigned: 0,
            text,
        });
    }
    let mut queries = Vec::new();
    for row in &rows {
        let bound = mixed_key(row).unwrap();
        queries.extend([
            IndexCompare::Eq(bound.clone()),
            IndexCompare::Lt(bound.clone()),
            IndexCompare::Lte(bound.clone()),
            IndexCompare::Gt(bound.clone()),
            IndexCompare::Gte(bound),
        ]);
    }
    queries.push(IndexCompare::In(vec![]));
    queries.push(IndexCompare::In(vec![
        IndexValue::I64(10),
        IndexValue::U64(0),
        IndexValue::I64(10),
        IndexValue::String("é".into()),
        IndexValue::U64(0),
    ]));
    for policy in [
        ORDERED,
        IndexPublicationPolicy::OrderedI64 {
            origin: i64::MIN,
            width: u64::MAX,
        },
        IndexPublicationPolicy::OrderedI64 {
            origin: i64::MAX,
            width: 1,
        },
    ] {
        let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
        let mut table = OccTable::new(arena.clone(), rows.len()).unwrap();
        let index =
            SecondaryIndex::new_in_shared_with_publication_policy("mixed", arena, policy).unwrap();
        for (id, row) in rows.iter().enumerate() {
            table.seed_row(id, *row).unwrap();
            index.try_insert(mixed_key(row).unwrap(), id).unwrap();
        }
        table.bind_index(index.clone(), mixed_key).unwrap();
        for query in &queries {
            let expected: Vec<_> = rows
                .iter()
                .enumerate()
                .filter_map(|(id, row)| matches(query, &mixed_key(row).unwrap()).then_some(id))
                .collect();
            let mut reader = table.begin_transaction().unwrap();
            assert_eq!(
                table.index_lookup(&mut reader, &index, query).unwrap(),
                expected,
                "{policy:?} {query:?}"
            );
            table.commit(&mut reader).unwrap();
        }
    }
}

#[test]
fn non_i64_insertion_invalidates_a_captured_i64_upper_range() {
    for inserted in [
        MixedRow {
            kind: 1,
            signed: 0,
            unsigned: 0,
            text: 0,
        },
        MixedRow {
            kind: 2,
            signed: 0,
            unsigned: 0,
            text: 1,
        },
    ] {
        let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
        let mut table = OccTable::new(arena.clone(), 1).unwrap();
        table
            .seed_row(
                0,
                MixedRow {
                    kind: 3,
                    signed: 0,
                    unsigned: 0,
                    text: 0,
                },
            )
            .unwrap();
        let index =
            SecondaryIndex::new_in_shared_with_publication_policy("mixed-phantom", arena, ORDERED)
                .unwrap();
        table.bind_index(index.clone(), mixed_key).unwrap();
        let mut reader = table.begin_transaction().unwrap();
        let query = IndexCompare::Gt(IndexValue::I64(i64::MAX));
        assert!(table
            .index_lookup(&mut reader, &index, &query)
            .unwrap()
            .is_empty());
        let mut writer = table.begin_transaction().unwrap();
        table.write(&mut writer, 0, inserted).unwrap();
        table.commit(&mut writer).unwrap();
        assert_eq!(
            table.commit(&mut reader),
            Err(OccError::SerializationFailure)
        );
        let mut fresh = table.begin_transaction().unwrap();
        assert_eq!(
            table.index_lookup(&mut fresh, &index, &query).unwrap(),
            vec![0]
        );
        table.commit(&mut fresh).unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct ChildConfig {
    path: std::path::PathBuf,
    table_header: u32,
    slots: Vec<u32>,
    index_header: u32,
    new_key: i64,
}
fn child_write(config: &ChildConfig) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "range_dependency_child", "--nocapture"])
        .env(
            "AEROSTORE_RANGE_DEPENDENCY_CHILD",
            serde_json::to_string(config).unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn independently_mapped_writer_uses_persisted_policy_for_success_and_invalidation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordered-index.mmap");
    let arena = Arc::new(
        aerostore_core::map_tmpfs_shared(&path, 16 << 20)
            .unwrap()
            .arena,
    );
    let (table, index) = fixture_in(arena, ORDERED, &[Row::live(10), Row::live(30)]);
    let mut config = ChildConfig {
        path,
        table_header: table.shared_header_offset(),
        slots: table.index_slot_offsets(),
        index_header: index.header_offset(),
        new_key: 40,
    };
    let mut old = table.begin_transaction().unwrap();
    child_write(&config);
    assert_eq!(
        table.index_lookup(&mut old, &index, &lte(10)).unwrap(),
        vec![0]
    );
    table.commit(&mut old).unwrap();
    let mut captured = table.begin_transaction().unwrap();
    assert_eq!(
        table.index_lookup(&mut captured, &index, &lte(10)).unwrap(),
        vec![0]
    );
    config.new_key = 9;
    child_write(&config);
    assert_eq!(
        table.commit(&mut captured),
        Err(OccError::SerializationFailure)
    );
    assert_eq!(fresh_rows(&table, &index, &lte(10)), vec![0, 1]);
}

#[test]
fn range_dependency_child() {
    let Some(config) = std::env::var_os("AEROSTORE_RANGE_DEPENDENCY_CHILD") else {
        return;
    };
    let config: ChildConfig = serde_json::from_str(&config.to_string_lossy()).unwrap();
    let mapped = aerostore_core::map_tmpfs_shared(&config.path, 16 << 20).unwrap();
    assert_eq!(mapped.mode, aerostore_core::TmpfsAttachMode::WarmStart);
    let arena = Arc::new(mapped.arena);
    let mut table =
        OccTable::<Row>::from_existing(arena.clone(), config.table_header, config.slots).unwrap();
    let index =
        SecondaryIndex::from_existing("attached-ordered", arena, config.index_header).unwrap();
    assert_eq!(index.publication_policy().unwrap(), ORDERED);
    table.bind_index(index.clone(), key).unwrap();
    replace(&table, 1, Row::live(config.new_key));
}
