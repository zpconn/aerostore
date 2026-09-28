//! The optional housekeeping index must retain complete query results while
//! removing only ineligible postings. These are real native-engine contracts,
//! not performance samples or a proof of the whole database.
#![cfg(target_os = "linux")]
#![allow(dead_code, unused_imports)]

#[path = "../benches/extended_crucible/metrics.rs"]
pub mod metrics;
#[path = "../benches/extended_crucible/model.rs"]
pub mod model;
#[path = "../benches/extended_crucible/aerostore.rs"]
pub mod original_native;
mod extended_crucible {
    pub use crate::original_native as aerostore;
    pub use crate::{metrics, model};
}
#[path = "../benches/contention_crucible/fixture.rs"]
mod fixture;
#[path = "../benches/contention_crucible/aerostore.rs"]
mod native;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;

use aerostore_core::{IndexValue, OccError};
use fixture::{DueIndexPolicy, ExpiryIndexPolicy, ExpiryPublicationPolicy, Shared};
use model::{DbError, Record, DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use storage::{Query, Store};

const POLICIES: [ExpiryIndexPolicy; 2] = [
    ExpiryIndexPolicy::AllActive,
    ExpiryIndexPolicy::Housekeeping,
];
const PUBLICATION_POLICIES: [ExpiryPublicationPolicy; 2] = [
    ExpiryPublicationPolicy::Hashed,
    ExpiryPublicationPolicy::Ordered,
];

fn rows() -> Vec<Record> {
    let mut rows = Vec::new();
    for active in [false, true] {
        for kind in [FLIGHT, POSITION, SCHEDULED, OUTBOX, DEDUP] {
            for family in [3, 9] {
                for event_time in [i64::MIN, 9, 10, 11, i64::MAX] {
                    rows.push(Record {
                        id: rows.len(),
                        active,
                        kind,
                        family,
                        event_time,
                        callsign: 100 + family,
                        tail: 200 + family,
                        due: 7,
                        ..Record::default()
                    });
                }
            }
        }
    }
    rows
}

fn fixture(policy: ExpiryIndexPolicy, rows: &[Record]) -> (tempfile::TempDir, Shared) {
    let directory = tempfile::tempdir().unwrap();
    let shared =
        Shared::create_with_policy(&directory.path().join("arena"), 16 << 20, rows, policy)
            .unwrap();
    (directory, shared)
}

fn publication_fixture(
    eligibility: ExpiryIndexPolicy,
    publication: ExpiryPublicationPolicy,
    origin: i64,
    width: u64,
    rows: &[Record],
) -> (tempfile::TempDir, Shared) {
    let directory = tempfile::tempdir().unwrap();
    let shared = Shared::create_with_publication_policies(
        &directory.path().join("arena"),
        16 << 20,
        rows,
        eligibility,
        DueIndexPolicy::Hashed,
        fixture::default_due_index_origin(),
        fixture::default_due_index_width(),
        publication,
        origin,
        width,
    )
    .unwrap();
    (directory, shared)
}

fn matching(rows: &[Record], query: &Query) -> Vec<Record> {
    rows.iter()
        .copied()
        .filter(|row| query.matches(row))
        .collect()
}

fn postings(shared: &Shared) -> Vec<(IndexValue, usize)> {
    let mut entries = shared.indexes[4].try_entries().unwrap();
    entries.sort();
    entries
}

fn write_committed(shared: &Shared, row: Record) {
    // Query/completeness tests mutate through the production OCC table. WAL
    // durability is covered separately; no writer daemon is needed here.
    let mut transaction = shared.table.begin_transaction().unwrap();
    shared.table.write(&mut transaction, row.id, row).unwrap();
    shared.table.commit(&mut transaction).unwrap();
}

#[test]
fn filtered_expiry_results_match_all_kinds_cutoff_boundaries_and_family_plans() {
    let rows = rows();
    for policy in POLICIES {
        let (_directory, shared) = fixture(policy, &rows);
        for global_time in [false, true] {
            let mut reader = native::Adapter::new(&shared, global_time);
            reader.begin(&[]).unwrap();
            for before in [i64::MIN, 9, 10, 11, i64::MAX] {
                for query in [
                    Query::GlobalExpired { before },
                    Query::Expired { family: 3, before },
                    Query::Expired { family: 9, before },
                    Query::Expired {
                        family: 999,
                        before,
                    },
                ] {
                    assert_eq!(
                        reader.query(&query).unwrap(),
                        matching(&rows, &query),
                        "{policy:?} global={global_time} {query:?}"
                    );
                }
            }
            reader.abort().unwrap();
        }
        let mut expected: Vec<_> = rows
            .iter()
            .filter(|row| {
                row.active
                    && (policy == ExpiryIndexPolicy::AllActive
                        || matches!(row.kind, POSITION | OUTBOX | DEDUP))
            })
            .map(|row| (IndexValue::I64(row.event_time), row.id))
            .collect();
        expected.sort();
        assert_eq!(postings(&shared), expected);
        assert_eq!(
            shared.audit().unwrap()["expiry_index_policy"],
            policy.name()
        );
    }
}

#[test]
fn kind_activation_timestamp_and_savepoint_changes_preserve_overlay_and_postings() {
    let initial = vec![
        Record {
            id: 0,
            active: true,
            kind: FLIGHT,
            family: 3,
            event_time: 5,
            ..Record::default()
        },
        Record {
            id: 1,
            active: true,
            kind: POSITION,
            family: 3,
            event_time: 5,
            ..Record::default()
        },
    ];
    let query = Query::GlobalExpired { before: 10 };
    for policy in POLICIES {
        let (_directory, shared) = fixture(policy, &initial);
        let mut expected = initial.clone();
        let mut db = native::Adapter::new(&shared, true);
        db.begin(&[]).unwrap();
        let first = db.savepoint().unwrap();
        expected[0].kind = DEDUP;
        db.write(expected[0]).unwrap();
        expected[1].kind = SCHEDULED;
        db.write(expected[1]).unwrap();
        assert_eq!(db.query(&query).unwrap(), matching(&expected, &query));
        let second = db.savepoint().unwrap();
        expected[0].event_time = 10;
        db.write(expected[0]).unwrap();
        expected[1].kind = OUTBOX;
        expected[1].active = false;
        db.write(expected[1]).unwrap();
        assert!(db.query(&query).unwrap().is_empty());
        db.rollback_to(second).unwrap();
        expected[0] = Record {
            kind: DEDUP,
            ..initial[0]
        };
        expected[1] = Record {
            kind: SCHEDULED,
            ..initial[1]
        };
        assert_eq!(db.query(&query).unwrap(), matching(&expected, &query));
        db.rollback_to(first).unwrap();
        assert_eq!(db.query(&query).unwrap(), matching(&initial, &query));
        db.abort().unwrap();
        expected = initial.clone();
        // Reuse both physical slots through kind, active and timestamp changes.
        for (id, kind, active, event_time) in [
            (0, POSITION, true, 9),
            (1, FLIGHT, true, 5),
            (0, OUTBOX, true, 10),
            (0, DEDUP, true, i64::MIN),
            (0, DEDUP, false, 5),
            (1, POSITION, false, 5),
            (1, OUTBOX, true, 9),
            (0, SCHEDULED, true, 5),
        ] {
            let row = Record {
                id,
                kind,
                active,
                event_time,
                ..initial[id]
            };
            write_committed(&shared, row);
            expected[id] = row;
            db.begin(&[]).unwrap();
            assert_eq!(db.query(&query).unwrap(), matching(&expected, &query));
            db.abort().unwrap();
            shared.audit().unwrap();
        }
        assert_eq!(shared.snapshot().unwrap(), expected);
    }
}

#[test]
fn attached_writer_and_immutable_marker_prevent_policy_and_metadata_mismatch() {
    let initial = vec![
        Record {
            id: 0,
            active: true,
            kind: FLIGHT,
            event_time: 5,
            ..Record::default()
        },
        Record {
            id: 1,
            active: true,
            kind: POSITION,
            event_time: 5,
            ..Record::default()
        },
    ];
    for policy in POLICIES {
        let (directory, shared) = fixture(policy, &initial);
        let path = directory.path().join("arena");
        let attachment = shared.attachment(&path);
        let attached = Shared::attach(&attachment).unwrap();
        write_committed(
            &attached,
            Record {
                kind: OUTBOX,
                ..initial[0]
            },
        );
        write_committed(
            &attached,
            Record {
                kind: SCHEDULED,
                ..initial[1]
            },
        );
        assert_eq!(attached.snapshot().unwrap(), shared.snapshot().unwrap());
        assert_eq!(postings(&attached), postings(&shared));
        assert_eq!(
            shared.audit().unwrap()["indexes"],
            attached.audit().unwrap()["indexes"]
        );
        let head = shared.arena.chunked_arena().head_offset();
        for mutation in 0..4 {
            let mut wrong = attachment.clone();
            match mutation {
                0 => {
                    wrong.expiry_index_policy = if policy == ExpiryIndexPolicy::AllActive {
                        ExpiryIndexPolicy::Housekeeping
                    } else {
                        ExpiryIndexPolicy::AllActive
                    }
                }
                1 => wrong.indexes.swap(0, 4),
                2 => wrong.ring = wrong.table_header,
                3 => wrong.table_slots.swap(0, 1),
                _ => unreachable!(),
            }
            assert!(
                Shared::attach(&wrong).is_err(),
                "accepted corrupted metadata case {mutation}"
            );
            assert_eq!(
                shared.arena.chunked_arena().head_offset(),
                head,
                "rejected attach mutated arena"
            );
        }
        let mut json = serde_json::to_value(&attachment).unwrap();
        json.as_object_mut().unwrap().remove("expiry_index_policy");
        let old: fixture::Attachment = serde_json::from_value(json).unwrap();
        assert_eq!(old.expiry_index_policy, ExpiryIndexPolicy::AllActive);
        assert_eq!(
            Shared::attach(&old).is_ok(),
            policy == ExpiryIndexPolicy::AllActive
        );
        let marker = shared.arena.boot_layout_offset();
        shared.arena.set_boot_layout_offset(0);
        assert!(Shared::attach(&attachment).is_err());
        shared
            .arena
            .set_boot_layout_offset(shared.arena.len() as u32 - 1);
        assert!(Shared::attach(&attachment).is_err());
        shared.arena.set_boot_layout_offset(attachment.indexes[0]);
        assert!(Shared::attach(&attachment).is_err());
        shared.arena.set_boot_layout_offset(marker);
        Shared::attach(&attachment).unwrap();
        assert!(Shared::create_with_policy(&path, 16 << 20, &initial, policy).is_err());
    }
}

#[test]
fn empty_expiry_search_retains_phantom_validation_and_historical_rejection() {
    let initial = vec![Record {
        id: 0,
        active: true,
        kind: FLIGHT,
        event_time: 5,
        ..Record::default()
    }];
    let query = Query::GlobalExpired { before: 10 };
    for policy in POLICIES {
        let (_directory, shared) = fixture(policy, &initial);
        let mut observed_empty = native::Adapter::new(&shared, true);
        let mut historical = native::Adapter::new(&shared, true);
        observed_empty.begin(&[]).unwrap();
        historical.begin(&[]).unwrap();
        assert!(observed_empty.query(&query).unwrap().is_empty());
        assert_eq!(historical.read(0).unwrap(), initial[0]);
        let eligible = Record {
            kind: POSITION,
            ..initial[0]
        };
        write_committed(&shared, eligible);
        assert_eq!(observed_empty.commit(), Err(DbError::Conflict));
        // A historical lookup may return its complete old snapshot or reject;
        // it must never claim the new row belongs to the old snapshot.
        match historical.query(&query) {
            Ok(rows) => assert!(rows.is_empty()),
            Err(error) => assert_eq!(error, DbError::Conflict),
        }
        historical.abort().unwrap();
        let mut fresh = native::Adapter::new(&shared, true);
        fresh.begin(&[]).unwrap();
        assert_eq!(fresh.query(&query).unwrap(), vec![eligible]);
        fresh.abort().unwrap();
        shared.audit().unwrap();
    }
}

#[test]
fn excluded_key_move_reduces_conflict_scope_but_eligible_disjoint_move_still_rejects() {
    for changed_kind in [FLIGHT, DEDUP] {
        let initial = vec![
            Record {
                id: 0,
                active: true,
                kind: POSITION,
                event_time: 1,
                ..Record::default()
            },
            Record {
                id: 1,
                active: true,
                kind: changed_kind,
                event_time: 20,
                ..Record::default()
            },
        ];
        for policy in POLICIES {
            let (_directory, shared) = fixture(policy, &initial);
            let query = Query::GlobalExpired { before: 10 };
            let mut captured = native::Adapter::new(&shared, true);
            let mut historical = native::Adapter::new(&shared, true);
            captured.begin(&[]).unwrap();
            historical.begin(&[]).unwrap();
            assert_eq!(captured.query(&query).unwrap(), vec![initial[0]]);
            write_committed(
                &shared,
                Record {
                    event_time: 21,
                    ..initial[1]
                },
            );
            let harmless = policy == ExpiryIndexPolicy::Housekeeping && changed_kind == FLIGHT;
            if harmless {
                assert_eq!(historical.query(&query).unwrap(), vec![initial[0]]);
                historical.commit().unwrap();
                captured.commit().unwrap();
            } else {
                assert_eq!(historical.query(&query), Err(DbError::Conflict));
                historical.abort().unwrap();
                assert_eq!(captured.commit(), Err(DbError::Conflict));
            }
            shared.audit().unwrap();
        }
    }
}

#[test]
fn default_fixture_preserves_original_rows_postings_and_attachment_file_guards() {
    let rows = rows();
    let directory = tempfile::tempdir().unwrap();
    let old =
        original_native::Shared::create(&directory.path().join("old"), 16 << 20, &rows).unwrap();
    let path = directory.path().join("new");
    let new = Shared::create(&path, 16 << 20, &rows).unwrap();
    assert_eq!(new.expiry_index_policy, ExpiryIndexPolicy::AllActive);
    assert_eq!(old.snapshot().unwrap(), new.snapshot().unwrap());
    for (old_index, new_index) in old.indexes.iter().zip(&new.indexes) {
        assert_eq!(old_index.field(), new_index.field());
        let mut old_entries = old_index.try_entries().unwrap();
        let mut new_entries = new_index.try_entries().unwrap();
        old_entries.sort();
        new_entries.sort();
        assert_eq!(old_entries, new_entries);
    }
    assert_eq!(
        old.audit().unwrap()["indexes"],
        new.audit().unwrap()["indexes"]
    );
    let mut attachment = new.attachment(&path);
    attachment.path = directory.path().join("missing");
    assert!(Shared::attach(&attachment).is_err());
    assert!(!attachment.path.exists());
    attachment.path = path.clone();
    attachment.bytes += 4096;
    assert!(Shared::attach(&attachment).is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 16 << 20);
    attachment.bytes -= 4096;
    attachment.path = directory.path().join("corrupt");
    let mut bytes = vec![0_u8; attachment.bytes];
    bytes[..16].fill(0x45);
    std::fs::write(&attachment.path, &bytes).unwrap();
    assert!(Shared::attach(&attachment).is_err());
    assert_eq!(std::fs::read(&attachment.path).unwrap(), bytes);
}

#[test]
fn ordered_due_fixture_preserves_rows_postings_and_persists_only_due_policy() {
    use aerostore_core::IndexPublicationPolicy;
    use fixture::DueIndexPolicy;
    let initial = rows();
    let directory = tempfile::tempdir().unwrap();
    let baseline = Shared::create(&directory.path().join("hashed"), 16 << 20, &initial).unwrap();
    let path = directory.path().join("ordered");
    let ordered = Shared::create_with_policies(
        &path,
        16 << 20,
        &initial,
        ExpiryIndexPolicy::AllActive,
        DueIndexPolicy::Ordered,
        -100,
        7,
    )
    .unwrap();
    let attachment =
        serde_json::from_value(serde_json::to_value(ordered.attachment(&path)).unwrap()).unwrap();
    let attached = Shared::attach(&attachment).unwrap();
    assert_eq!(baseline.snapshot().unwrap(), attached.snapshot().unwrap());
    for (number, ((old, new), reopened)) in baseline
        .indexes
        .iter()
        .zip(&ordered.indexes)
        .zip(&attached.indexes)
        .enumerate()
    {
        assert_eq!(
            old.publication_policy().unwrap(),
            IndexPublicationPolicy::Hashed
        );
        let expected = if number == 3 {
            IndexPublicationPolicy::OrderedI64 {
                origin: -100,
                width: 7,
            }
        } else {
            IndexPublicationPolicy::Hashed
        };
        assert_eq!(new.publication_policy().unwrap(), expected);
        assert_eq!(reopened.publication_policy().unwrap(), expected);
        let mut old_entries = old.try_entries().unwrap();
        let mut new_entries = reopened.try_entries().unwrap();
        old_entries.sort();
        new_entries.sort();
        assert_eq!(old_entries, new_entries);
    }
    let audit = attached.audit().unwrap();
    assert_eq!(audit["due_index_policy"], "ordered");
    assert_eq!(audit["due_index_origin"], -100);
    assert_eq!(audit["due_index_width"], 7);
    for query in [
        Query::GlobalDue { at: 6 },
        Query::GlobalDue { at: 7 },
        Query::GlobalDue { at: 8 },
    ] {
        let mut adapter = native::Adapter::new(&attached, true);
        adapter.begin(&[]).unwrap();
        assert_eq!(adapter.query(&query).unwrap(), matching(&initial, &query));
        adapter.abort().unwrap();
    }
}

#[test]
fn due_attachment_parameters_cannot_select_a_different_persisted_policy() {
    use fixture::DueIndexPolicy;
    let initial = rows();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordered");
    let shared = Shared::create_with_policies(
        &path,
        16 << 20,
        &initial,
        ExpiryIndexPolicy::Housekeeping,
        DueIndexPolicy::Ordered,
        -100,
        7,
    )
    .unwrap();
    let before = shared.snapshot().unwrap();
    let before_marker = shared.arena.boot_layout_offset();
    let before_indexes = shared
        .indexes
        .iter()
        .map(|index| {
            let mut entries = index.try_entries().unwrap();
            entries.sort();
            (index.publication_policy().unwrap(), entries)
        })
        .collect::<Vec<_>>();
    let original = shared.attachment(&path);
    let mut different = original.clone();
    different.due_index_policy = DueIndexPolicy::Hashed;
    assert!(Shared::attach(&different).is_err());
    different = original.clone();
    different.due_index_origin += 1;
    assert!(Shared::attach(&different).is_err());
    different = original.clone();
    different.due_index_width += 1;
    assert!(Shared::attach(&different).is_err());
    different.due_index_width = 0;
    assert!(Shared::attach(&different).is_err());
    for missing in ["due_index_policy", "due_index_origin", "due_index_width"] {
        let mut encoded = serde_json::to_value(&original).unwrap();
        encoded.as_object_mut().unwrap().remove(missing);
        let missing = serde_json::from_value(encoded).unwrap();
        assert!(Shared::attach(&missing).is_err());
    }
    // Warm ShmArena destruction updates its existing clean_shutdown flag.
    // Rejected fixture attachment promises no policy/row/posting rebinding,
    // not byte-for-byte immutability or production recovery guarantees.
    assert_eq!(shared.snapshot().unwrap(), before);
    assert_eq!(shared.arena.boot_layout_offset(), before_marker);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 16 << 20);
    for (index, (policy, expected_entries)) in shared.indexes.iter().zip(before_indexes) {
        assert_eq!(index.publication_policy().unwrap(), policy);
        let mut entries = index.try_entries().unwrap();
        entries.sort();
        assert_eq!(entries, expected_entries);
    }
    let invalid_path = directory.path().join("invalid");
    assert!(Shared::create_with_policies(
        &invalid_path,
        16 << 20,
        &initial,
        ExpiryIndexPolicy::AllActive,
        DueIndexPolicy::Ordered,
        0,
        0
    )
    .is_err());
    assert!(
        !invalid_path.exists(),
        "validate policy before allocating a mapping"
    );
}

#[test]
fn expiry_publication_preserves_complete_results_across_kinds_cutoffs_and_extremes() {
    let initial = rows();
    for eligibility in POLICIES {
        for publication in PUBLICATION_POLICIES {
            for (origin, width) in [(0, 1), (-100, 7), (i64::MIN, u64::MAX), (i64::MAX - 7, 3)] {
                let (_directory, shared) =
                    publication_fixture(eligibility, publication, origin, width, &initial);
                for global_time in [false, true] {
                    let mut db = native::Adapter::new(&shared, global_time);
                    db.begin(&[]).unwrap();
                    for before in [i64::MIN, i64::MIN + 1, 9, 10, 11, i64::MAX] {
                        for query in [
                            Query::GlobalExpired { before },
                            Query::Expired { family: 3, before },
                            Query::Expired { family: 9, before },
                            Query::Expired {
                                family: 999,
                                before,
                            },
                        ] {
                            assert_eq!(db.query(&query).unwrap(), matching(&initial, &query),
                                "{eligibility:?}/{publication:?} origin={origin} width={width} {query:?}");
                        }
                    }
                    db.commit().unwrap();
                }
                let expected = match publication {
                    ExpiryPublicationPolicy::Hashed => {
                        aerostore_core::IndexPublicationPolicy::Hashed
                    }
                    ExpiryPublicationPolicy::Ordered => {
                        aerostore_core::IndexPublicationPolicy::OrderedI64 { origin, width }
                    }
                };
                for (number, index) in shared.indexes.iter().enumerate() {
                    assert_eq!(
                        index.publication_policy().unwrap(),
                        if number == 4 {
                            expected
                        } else {
                            aerostore_core::IndexPublicationPolicy::Hashed
                        }
                    );
                }
                let audit = shared.audit().unwrap();
                assert_eq!(audit["expiry_publication_policy"], publication.name());
                assert_eq!(audit["expiry_index_origin"], origin);
                assert_eq!(audit["expiry_index_width"], width);
            }
        }
    }
}

#[test]
fn ordered_expiry_empty_capture_and_historical_query_survive_future_insert_and_move() {
    let initial = vec![
        Record {
            id: 0,
            active: true,
            kind: POSITION,
            event_time: 20,
            ..Record::default()
        },
        Record {
            id: 1,
            active: false,
            kind: DEDUP,
            event_time: 30,
            ..Record::default()
        },
    ];
    let query = Query::GlobalExpired { before: 10 };
    for eligibility in POLICIES {
        for publication in PUBLICATION_POLICIES {
            let (_directory, shared) =
                publication_fixture(eligibility, publication, 0, 1, &initial);
            let mut captured = native::Adapter::new(&shared, true);
            let mut historical = native::Adapter::new(&shared, true);
            captured.begin(&[]).unwrap();
            historical.begin(&[]).unwrap();
            assert!(captured.query(&query).unwrap().is_empty());
            write_committed(
                &shared,
                Record {
                    event_time: 21,
                    ..initial[0]
                },
            );
            write_committed(
                &shared,
                Record {
                    active: true,
                    ..initial[1]
                },
            );
            if publication == ExpiryPublicationPolicy::Ordered {
                assert!(historical.query(&query).unwrap().is_empty());
                historical.commit().unwrap();
                captured.commit().unwrap();
            } else {
                assert_eq!(historical.query(&query), Err(DbError::Conflict));
                historical.abort().unwrap();
                assert_eq!(captured.commit(), Err(DbError::Conflict));
            }
            shared.audit().unwrap();
        }
    }
}

#[test]
fn expiry_insert_delete_and_cutoff_moves_invalidate_captured_complete_queries() {
    for eligibility in POLICIES {
        for publication in PUBLICATION_POLICIES {
            for (label, active, old_time, new_active, new_time) in [
                ("insert", false, 9, true, 9),
                ("delete", true, 9, false, 9),
                ("move in", true, 10, true, 9),
                ("move out", true, 9, true, 10),
            ] {
                let initial = vec![Record {
                    id: 0,
                    active,
                    kind: POSITION,
                    event_time: old_time,
                    ..Record::default()
                }];
                let query = Query::GlobalExpired { before: 10 };
                let (_directory, shared) =
                    publication_fixture(eligibility, publication, 0, 1, &initial);
                let mut captured = native::Adapter::new(&shared, true);
                let mut historical = native::Adapter::new(&shared, true);
                captured.begin(&[]).unwrap();
                historical.begin(&[]).unwrap();
                assert_eq!(captured.query(&query).unwrap(), matching(&initial, &query));
                let changed = Record {
                    active: new_active,
                    event_time: new_time,
                    ..initial[0]
                };
                write_committed(&shared, changed);
                assert_eq!(
                    captured.commit(),
                    Err(DbError::Conflict),
                    "{publication:?} {label}"
                );
                match historical.query(&query) {
                    Ok(found) => {
                        assert_eq!(found, matching(&initial, &query), "historical {label}")
                    }
                    Err(error) => assert_eq!(error, DbError::Conflict),
                }
                historical.abort().unwrap();
                let mut fresh = native::Adapter::new(&shared, true);
                fresh.begin(&[]).unwrap();
                assert_eq!(fresh.query(&query).unwrap(), matching(&[changed], &query));
                fresh.commit().unwrap();
                shared.audit().unwrap();
            }
        }
    }
}

#[test]
fn expiry_own_writes_rollback_preserves_complete_overlay_and_predicate_dependencies() {
    let initial = vec![
        Record {
            id: 0,
            kind: POSITION,
            event_time: 9,
            ..Record::default()
        },
        Record {
            id: 1,
            kind: OUTBOX,
            event_time: 8,
            ..Record::default()
        },
    ];
    let query = Query::GlobalExpired { before: 10 };
    for eligibility in POLICIES {
        for publication in PUBLICATION_POLICIES {
            let (_directory, shared) =
                publication_fixture(eligibility, publication, 0, 1, &initial);
            let mut db = native::Adapter::new(&shared, true);
            db.begin(&[]).unwrap();
            let first = db.savepoint().unwrap();
            assert!(db.query(&query).unwrap().is_empty());
            let own = Record {
                active: true,
                ..initial[0]
            };
            db.write(own).unwrap();
            assert_eq!(db.query(&query).unwrap(), vec![own]);
            let second = db.savepoint().unwrap();
            db.write(Record {
                event_time: 10,
                ..own
            })
            .unwrap();
            assert!(db.query(&query).unwrap().is_empty());
            db.rollback_to(second).unwrap();
            assert_eq!(db.query(&query).unwrap(), vec![own]);
            db.rollback_to(first).unwrap();
            assert!(db.query(&query).unwrap().is_empty());
            write_committed(
                &shared,
                Record {
                    active: true,
                    ..initial[1]
                },
            );
            // Rolling back the writes must not forget an observed absence.
            assert_eq!(db.commit(), Err(DbError::Conflict));
            assert!(!shared.snapshot().unwrap()[0].active);
            shared.audit().unwrap();
        }
    }
}

#[test]
fn shifted_and_saturated_expiry_windows_preserve_results_and_conservative_conflicts() {
    for (origin, width, before, future, changed) in [
        (0, 1, 10, 20, 21),       // Distinct precise intervals.
        (100, 1, 10, 20, 21),     // All keys underflow: safe, less precise.
        (0, 1, 5000, 6000, 6001), // All keys in the saturated upper bucket.
        (0, 10, 11, 12, 13),      // A strict boundary includes the whole interval.
        (i64::MAX - 3, 1, i64::MAX - 2, i64::MAX, i64::MAX - 1),
    ] {
        let initial = vec![Record {
            id: 0,
            active: true,
            kind: POSITION,
            event_time: future,
            ..Record::default()
        }];
        let (_directory, shared) = publication_fixture(
            ExpiryIndexPolicy::AllActive,
            ExpiryPublicationPolicy::Ordered,
            origin,
            width,
            &initial,
        );
        let mut db = native::Adapter::new(&shared, true);
        db.begin(&[]).unwrap();
        let query = Query::GlobalExpired { before };
        assert!(db.query(&query).unwrap().is_empty());
        write_committed(
            &shared,
            Record {
                event_time: changed,
                ..initial[0]
            },
        );
        if origin == 100 || before == 5000 || width == 10 {
            assert_eq!(db.commit(), Err(DbError::Conflict));
        } else {
            db.commit().unwrap();
        }
        let mut fresh = native::Adapter::new(&shared, true);
        fresh.begin(&[]).unwrap();
        assert!(fresh.query(&query).unwrap().is_empty());
        fresh.abort().unwrap();
        shared.audit().unwrap();
    }
}

#[test]
fn expiry_publication_attachment_binds_policy_parameters_and_rejects_old_identity() {
    let initial = rows();
    for publication in PUBLICATION_POLICIES {
        let (directory, shared) =
            publication_fixture(ExpiryIndexPolicy::AllActive, publication, -100, 7, &initial);
        let path = directory.path().join("arena");
        let attachment = shared.attachment(&path);
        let attached = Shared::attach(&attachment).unwrap();
        assert_eq!(attached.expiry_publication_policy, publication);
        assert_eq!(attached.expiry_index_origin, -100);
        assert_eq!(attached.expiry_index_width, 7);
        assert_eq!(attached.snapshot().unwrap(), initial);
        let marker = shared.arena.boot_layout_offset();
        let head = shared.arena.chunked_arena().head_offset();
        for change in 0..4 {
            let mut wrong = attachment.clone();
            match change {
                0 => {
                    wrong.expiry_publication_policy =
                        if publication == ExpiryPublicationPolicy::Ordered {
                            ExpiryPublicationPolicy::Hashed
                        } else {
                            ExpiryPublicationPolicy::Ordered
                        }
                }
                1 => wrong.expiry_index_origin += 1,
                2 => wrong.expiry_index_width += 1,
                3 => wrong.expiry_index_width = 0,
                _ => unreachable!(),
            }
            assert!(Shared::attach(&wrong).is_err());
        }
        for name in [
            "expiry_publication_policy",
            "expiry_index_origin",
            "expiry_index_width",
        ] {
            let mut json = serde_json::to_value(&attachment).unwrap();
            json.as_object_mut().unwrap().remove(name);
            let old: fixture::Attachment = serde_json::from_value(json).unwrap();
            // Missing hashed policy is its documented default; the nondefault
            // immutable origin/width still cannot be silently substituted.
            assert_eq!(
                Shared::attach(&old).is_ok(),
                name == "expiry_publication_policy"
                    && publication == ExpiryPublicationPolicy::Hashed
            );
        }
        // Version occupies byte24 in the stable all-integer v2/v3 prefix.
        // No worker is active and no reference to that marker is retained.
        let version = unsafe {
            shared
                .arena
                .mmap_base()
                .as_ptr()
                .add(marker as usize + 24)
                .cast::<u32>()
        };
        unsafe {
            version.write(2);
        }
        let error = Shared::attach(&attachment).err().unwrap();
        assert!(error.contains("unsupported contention fixture identity"));
        unsafe {
            version.write(3);
        }
        assert_eq!(shared.arena.chunked_arena().head_offset(), head);
        assert_eq!(shared.arena.boot_layout_offset(), marker);
        assert_eq!(shared.snapshot().unwrap(), initial);
        Shared::attach(&attachment).unwrap().audit().unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    for publication in PUBLICATION_POLICIES {
        let path = directory.path().join(publication.name());
        assert!(Shared::create_with_publication_policies(
            &path,
            16 << 20,
            &initial,
            ExpiryIndexPolicy::AllActive,
            DueIndexPolicy::Hashed,
            fixture::default_due_index_origin(),
            fixture::default_due_index_width(),
            publication,
            0,
            0
        )
        .is_err());
        assert!(!path.exists());
    }
}

#[test]
fn legacy_expiry_publication_attachment_defaults_preserve_hashed_fixture() {
    let initial = rows();
    let (directory, shared) = fixture(ExpiryIndexPolicy::AllActive, &initial);
    assert_eq!(
        shared.expiry_publication_policy,
        ExpiryPublicationPolicy::Hashed
    );
    let mut json =
        serde_json::to_value(shared.attachment(&directory.path().join("arena"))).unwrap();
    for field in [
        "expiry_publication_policy",
        "expiry_index_origin",
        "expiry_index_width",
    ] {
        json.as_object_mut().unwrap().remove(field);
    }
    let attachment: fixture::Attachment = serde_json::from_value(json).unwrap();
    assert_eq!(
        attachment.expiry_publication_policy,
        ExpiryPublicationPolicy::Hashed
    );
    assert_eq!(
        attachment.expiry_index_origin,
        fixture::default_expiry_index_origin()
    );
    assert_eq!(
        attachment.expiry_index_width,
        fixture::default_expiry_index_width()
    );
    let reopened = Shared::attach(&attachment).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), initial);
    reopened.audit().unwrap();
}
