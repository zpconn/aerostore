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
use fixture::{ExpiryIndexPolicy, Shared};
use model::{DbError, Record, DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use storage::{Query, Store};

const POLICIES: [ExpiryIndexPolicy; 2] = [
    ExpiryIndexPolicy::AllActive,
    ExpiryIndexPolicy::Housekeeping,
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
