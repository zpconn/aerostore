//! Native capture regressions. These inspect retained dependencies as well as
//! results/commit outcomes; they add no observation to the production hot path.
use super::*;
use crate::shm_index::IndexPublicationPolicy;

const POLICIES: [IndexPublicationPolicy; 2] = [
    IndexPublicationPolicy::Hashed,
    IndexPublicationPolicy::OrderedI64 {
        origin: 0,
        width: 10,
    },
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Row {
    left: Option<i64>,
    right: Option<i64>,
    payload: u64,
}

fn left_key(row: &Row) -> Option<IndexValue> {
    row.left.map(IndexValue::I64)
}

fn right_key(row: &Row) -> Option<IndexValue> {
    row.right.map(IndexValue::I64)
}

fn fixture(
    policy: IndexPublicationPolicy,
    rows: &[Row],
) -> (OccTable<Row>, SecondaryIndex<usize>, SecondaryIndex<usize>) {
    let arena = Arc::new(ShmArena::new(16 << 20).unwrap());
    let mut table = OccTable::new(arena.clone(), rows.len()).unwrap();
    let left = SecondaryIndex::new_in_shared_with_publication_policy("left", arena.clone(), policy)
        .unwrap();
    let right =
        SecondaryIndex::new_in_shared_with_publication_policy("right", arena, policy).unwrap();
    for (id, row) in rows.iter().copied().enumerate() {
        table.seed_row(id, row).unwrap();
        if let Some(key) = left_key(&row) {
            left.try_insert(key, id).unwrap();
        }
        if let Some(key) = right_key(&row) {
            right.try_insert(key, id).unwrap();
        }
    }
    table.bind_index(left.clone(), left_key).unwrap();
    table.bind_index(right.clone(), right_key).unwrap();
    (table, left, right)
}

fn eq(key: i64) -> IndexCompare {
    IndexCompare::Eq(IndexValue::I64(key))
}

fn broad() -> IndexCompare {
    IndexCompare::Gte(IndexValue::I64(i64::MIN))
}

fn dependencies(tx: &OccTransaction<Row>) -> Vec<(u32, usize, u64)> {
    tx.index_reads
        .iter()
        .map(|r| (r.index_offset, r.bucket, r.stamp))
        .collect()
}

fn assert_unique(tx: &OccTransaction<Row>) {
    let identities: BTreeSet<_> = tx
        .index_reads
        .iter()
        .map(|r| (r.index_offset, r.bucket))
        .collect();
    assert_eq!(
        identities.len(),
        tx.index_reads.len(),
        "one captured stamp per index/bucket identity"
    );
}

fn publish(table: &OccTable<Row>, row_id: usize, value: Row) {
    let mut writer = table.begin_transaction().unwrap();
    table.write(&mut writer, row_id, value).unwrap();
    table.commit(&mut writer).unwrap();
}

#[test]
fn empty_broad_query_and_repetition_capture_each_dependency_once() {
    for policy in POLICIES {
        let (table, index, _) = fixture(policy, &[Row::default()]);
        let mut reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut reader, &index, &broad())
            .unwrap()
            .is_empty());
        let expected: Vec<_> = (0..4096)
            .map(|bucket| (index.header_offset(), bucket, 0))
            .collect();
        assert_eq!(dependencies(&reader), expected);
        assert!(
            reader.read_set.is_empty(),
            "empty searches still capture complete predicate dependencies"
        );
        assert!(table
            .index_lookup(&mut reader, &index, &broad())
            .unwrap()
            .is_empty());
        assert_eq!(
            dependencies(&reader),
            expected,
            "repeat lookup must retain the original vector exactly"
        );
        assert_unique(&reader);
        assert_eq!(table.commit(&mut reader), Ok(0));
        assert!(table.shm.create_snapshot().is_empty());
    }
}

#[test]
fn equality_then_broad_and_duplicate_in_preserve_the_original_prefix() {
    for policy in POLICIES {
        let rows: Vec<_> = [5, 25, 35]
            .into_iter()
            .map(|left| Row {
                left: Some(left),
                ..Row::default()
            })
            .collect();
        let (table, index, _) = fixture(policy, &rows);
        let mut reader = table.begin_transaction().unwrap();
        assert_eq!(
            table.index_lookup(&mut reader, &index, &eq(25)).unwrap(),
            vec![1]
        );
        let original = dependencies(&reader);
        assert_eq!(original.len(), 1);
        assert_eq!(
            table.index_lookup(&mut reader, &index, &broad()).unwrap(),
            vec![0, 1, 2]
        );
        assert_eq!(
            &dependencies(&reader)[..original.len()],
            original.as_slice()
        );
        assert_eq!(reader.index_reads.len(), 4096);
        assert_unique(&reader);
        let before_in = dependencies(&reader);
        let repeated = IndexCompare::In(
            [25, 5, 25, 15, 35, 5]
                .into_iter()
                .map(IndexValue::I64)
                .collect(),
        );
        assert_eq!(
            table.index_lookup(&mut reader, &index, &repeated).unwrap(),
            vec![0, 1, 2]
        );
        assert_eq!(dependencies(&reader), before_in);
        assert!(table
            .index_lookup(&mut reader, &index, &IndexCompare::In(vec![]))
            .unwrap()
            .is_empty());
        assert_eq!(dependencies(&reader), before_in);
        assert_eq!(table.commit(&mut reader), Ok(0));
    }
}

#[test]
fn overlapping_ranges_keep_prior_stamps_and_only_add_missing_identities() {
    for policy in POLICIES {
        let rows: Vec<_> = [5, 25, 35]
            .into_iter()
            .map(|left| Row {
                left: Some(left),
                ..Row::default()
            })
            .collect();
        let (table, index, _) = fixture(policy, &rows);
        let mut reader = table.begin_transaction().unwrap();
        assert_eq!(
            table
                .index_lookup(&mut reader, &index, &IndexCompare::Lte(IndexValue::I64(25)))
                .unwrap(),
            vec![0, 1]
        );
        let prior = dependencies(&reader);
        assert_eq!(
            table
                .index_lookup(&mut reader, &index, &IndexCompare::Gte(IndexValue::I64(15)))
                .unwrap(),
            vec![1, 2]
        );
        assert_eq!(&dependencies(&reader)[..prior.len()], prior.as_slice());
        assert_eq!(reader.index_reads.len(), 4096);
        assert_unique(&reader);
        assert_eq!(table.commit(&mut reader), Ok(0));
    }
}

#[test]
fn equal_bucket_numbers_on_two_indexes_remain_separate_conflict_dependencies() {
    for policy in POLICIES {
        for query_right in [false, true] {
            let (table, left, right) = fixture(policy, &[Row::default(), Row::default()]);
            let bucket = left.transactional_key_bucket(&IndexValue::I64(42)).unwrap();
            assert_eq!(
                right
                    .transactional_key_bucket(&IndexValue::I64(42))
                    .unwrap(),
                bucket
            );
            let mut reader = table.begin_transaction().unwrap();
            assert!(table
                .index_lookup(&mut reader, &left, &eq(42))
                .unwrap()
                .is_empty());
            if query_right {
                assert!(table
                    .index_lookup(&mut reader, &right, &eq(42))
                    .unwrap()
                    .is_empty());
                assert_eq!(
                    dependencies(&reader),
                    vec![
                        (left.header_offset(), bucket, 0),
                        (right.header_offset(), bucket, 0)
                    ]
                );
            }
            assert_unique(&reader);
            let pending = Row {
                payload: 99,
                ..Row::default()
            };
            table.write(&mut reader, 1, pending).unwrap();
            publish(
                &table,
                0,
                Row {
                    right: Some(42),
                    ..Row::default()
                },
            );
            if query_right {
                assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
                assert_eq!(table.latest_value(1).unwrap(), Some(Row::default()));
            } else {
                assert_eq!(table.commit(&mut reader), Ok(1));
                assert_eq!(table.latest_value(1).unwrap(), Some(pending));
            }
            assert!(table.shm.create_snapshot().is_empty());
        }
    }
}

#[test]
fn dependencies_captured_after_savepoint_survive_rollback_and_later_queries() {
    for policy in POLICIES {
        let (table, index, _) = fixture(policy, &[Row::default(), Row::default()]);
        let mut reader = table.begin_transaction().unwrap();
        table.savepoint(&mut reader, "before-query").unwrap();
        assert!(table
            .index_lookup(&mut reader, &index, &eq(42))
            .unwrap()
            .is_empty());
        let prior = dependencies(&reader);
        table
            .write(
                &mut reader,
                1,
                Row {
                    payload: 7,
                    ..Row::default()
                },
            )
            .unwrap();
        table.rollback_to(&mut reader, "before-query").unwrap();
        assert_eq!(dependencies(&reader), prior);
        assert!(table
            .index_lookup(&mut reader, &index, &broad())
            .unwrap()
            .is_empty());
        assert_eq!(&dependencies(&reader)[..prior.len()], prior.as_slice());
        assert_unique(&reader);
        table
            .write(
                &mut reader,
                1,
                Row {
                    payload: 9,
                    ..Row::default()
                },
            )
            .unwrap();
        publish(
            &table,
            0,
            Row {
                left: Some(42),
                ..Row::default()
            },
        );
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert_eq!(table.latest_value(1).unwrap(), Some(Row::default()));
        assert!(table.shm.create_snapshot().is_empty());
    }
}

#[test]
fn partial_capture_error_keeps_prefix_releases_guards_and_makes_failure_sticky() {
    for policy in POLICIES {
        let (table, left, right) = fixture(policy, &[Row::default(), Row::default()]);
        let key = (0..40_000)
            .find(|key| {
                let bucket = left
                    .transactional_key_bucket(&IndexValue::I64(*key))
                    .unwrap();
                (1024..3072).contains(&bucket)
            })
            .unwrap();
        let failing_bucket = left
            .transactional_key_bucket(&IndexValue::I64(key))
            .unwrap();
        let mut reader = table.begin_transaction().unwrap();
        table.savepoint(&mut reader, "before-failure").unwrap();
        assert!(table
            .index_lookup(&mut reader, &right, &eq(42))
            .unwrap()
            .is_empty());
        let prior = dependencies(&reader);
        // This real committed insertion stamps one middle bucket after the
        // reader's snapshot, so a broad lookup accepts an initial suffix and
        // then fails before recording the conflicting bucket itself.
        publish(
            &table,
            0,
            Row {
                left: Some(key),
                ..Row::default()
            },
        );
        assert_eq!(
            table.index_lookup(&mut reader, &left, &broad()),
            Err(Error::SerializationFailure)
        );
        let mut expected = prior.clone();
        expected.extend((0..failing_bucket).map(|bucket| (left.header_offset(), bucket, 0)));
        assert_eq!(dependencies(&reader), expected);
        assert!(reader.index_conflict);
        assert!(
            reader.read_set.is_empty(),
            "capture failure precedes materialization"
        );
        assert_unique(&reader);
        for bucket in [0, failing_bucket, 4095] {
            let guard = left.transactional_try_lock_bucket(bucket).unwrap();
            assert!(
                guard.is_some(),
                "the complete guard vector must drop on early return"
            );
            drop(guard);
        }
        table.rollback_to(&mut reader, "before-failure").unwrap();
        assert_eq!(dependencies(&reader), expected);
        assert!(table
            .index_lookup(&mut reader, &right, &eq(42))
            .unwrap()
            .is_empty());
        assert_eq!(dependencies(&reader), expected);
        assert!(
            reader.index_conflict,
            "rollback and a successful lookup must not clear the failed capture"
        );
        table
            .write(
                &mut reader,
                1,
                Row {
                    payload: 9,
                    ..Row::default()
                },
            )
            .unwrap();
        // No captured stamp changed: only the sticky flag rejects this write.
        // The conflicting middle bucket never entered the dependency vector.
        assert!(reader.index_reads.iter().all(|read| {
            let index = if read.index_offset == left.header_offset() {
                &left
            } else {
                &right
            };
            index.transactional_stamp(read.bucket).unwrap() == read.stamp
        }));
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert_eq!(table.latest_value(1).unwrap(), Some(Row::default()));
        assert!(table.shm.create_snapshot().is_empty());
    }
}

#[test]
fn changed_prior_stamp_below_snapshot_is_rejected_without_appending_duplicates() {
    for policy in POLICIES {
        let (table, index, _) = fixture(policy, &[Row::default()]);
        // Advance transaction identity so two distinct stamps are older than
        // the reader. This deliberately perturbs internal metadata to exercise
        // the defensive repeated-stamp branch separately from post-snapshot
        // rejection; normal writers reserve a fresh publication stamp.
        let mut preceding = table.begin_transaction().unwrap();
        table.abort(&mut preceding).unwrap();
        let mut reader = table.begin_transaction().unwrap();
        assert!(reader.txid > 1);
        assert!(table
            .index_lookup(&mut reader, &index, &eq(42))
            .unwrap()
            .is_empty());
        let prior = dependencies(&reader);
        let bucket = prior[0].1;
        let guard = index
            .transactional_try_lock_bucket(bucket)
            .unwrap()
            .unwrap();
        index
            .transactional_publish_stamp(bucket, reader.txid - 1)
            .unwrap();
        drop(guard);
        assert_eq!(
            table.index_lookup(&mut reader, &index, &eq(42)),
            Err(Error::SerializationFailure)
        );
        assert_eq!(dependencies(&reader), prior);
        assert!(reader.index_conflict);
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert!(table.shm.create_snapshot().is_empty());
    }
}
