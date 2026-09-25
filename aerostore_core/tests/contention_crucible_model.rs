//! The contention oracle is deliberately tested against corrupt histories,
//! not merely successful replay of its own preferred execution order.
#![allow(dead_code)]
#[path = "../benches/extended_crucible/model.rs"]
pub mod shared_model;
mod extended_crucible {
    pub use crate::shared_model as model;
}
#[path = "../benches/contention_crucible/model.rs"]
mod model;
#[path = "../benches/contention_crucible/oracle.rs"]
mod oracle;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;

use model::{Operation, RecordedQuery};
use oracle::{Receipt, Status};
use shared_model::Record;
use std::collections::BTreeMap;

#[test]
fn lifecycle_stress_corpus_keeps_its_pre_fleet_serialized_bytes() {
    let mut hash = 0xcbf29ce484222325_u64;
    for sequence in 0..1024 {
        let message = model::sustained_message_for("lifecycle", sequence, 0, 1, 16, 20260924, 80);
        for byte in serde_json::to_vec(&message).unwrap() {
            hash = (hash ^ byte as u64).wrapping_mul(0x100000001b3);
        }
    }
    // Captured from the unmodified pre-fleet generator before adding the new
    // profile. This checks the archived stress input bytes, not engine output.
    assert_eq!(hash, 9_033_575_116_728_179_252);
}

#[test]
fn fleet_corpus_is_worker_independent_and_distributes_creation_and_kinds() {
    let prefix: Vec<_> = (0..7)
        .map(|sequence| {
            model::sustained_message_for("fleet", sequence, 0, 1, 16, 20260925, 0)
                .kind
                .name()
        })
        .collect();
    assert_eq!(
        prefix,
        [
            "position",
            "global_projection",
            "global_reschedule",
            "arrival",
            "arrival",
            "arrival",
            "expire_family"
        ]
    );
    for families in [16, 32] {
        let seed = 20260925;
        for workers in [4, 8, 16, 32] {
            let mut owners = BTreeMap::<_, std::collections::BTreeSet<usize>>::new();
            let mut creators = std::collections::BTreeSet::new();
            let mut counts = BTreeMap::new();
            for sequence in 0..families as u64 * 64 {
                let worker = sequence as usize % workers;
                let message = model::sustained_message_for(
                    "fleet", sequence, worker, workers, families, seed, 0,
                );
                assert_eq!(
                    message,
                    model::sustained_message_for("fleet", sequence, 0, 1, families, seed, 0)
                );
                owners
                    .entry(message.kind.name())
                    .or_default()
                    .insert(worker);
                *counts.entry(message.kind.name()).or_insert(0_usize) += 1;
                if message.creation == model::CreationPolicy::IfVacant {
                    creators.insert(worker);
                }
            }
            assert_eq!(
                creators.len(),
                workers,
                "creation pinning: families={families} workers={workers}"
            );
            for (kind, owners) in owners {
                assert_eq!(
                    owners.len(),
                    workers,
                    "kind {kind} pinned: families={families} workers={workers}"
                );
            }
            let factor = families * 4;
            assert_eq!(
                counts,
                [
                    ("plan", 3 * factor),
                    ("position", 3 * factor),
                    ("arrival", 3 * factor),
                    ("global_projection", factor),
                    ("global_reschedule", factor),
                    ("global_cancel", factor),
                    ("global_housekeeping", 2 * factor),
                    ("expire_family", 2 * factor)
                ]
                .into()
            );
        }
    }
}

#[test]
fn fleet_recorded_prefix_has_a_serial_witness_and_record_free_effects_match() {
    let initial = model::sustained_initial_for("fleet", 16, 20260925);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let mut unrecorded = rows.clone();
    let mut receipts = Vec::new();
    for sequence in 0..64 {
        let message = model::sustained_message_for("fleet", sequence, 0, 1, 16, 20260925, 0);
        let body = model::serial_apply(&mut rows, &message).unwrap();
        let mut store = model::ReferenceStore::new(&unrecorded);
        assert_eq!(
            model::execute_without_recording(&mut store, &message).unwrap(),
            body.outcome
        );
        unrecorded.extend(store.writes);
        assert_eq!(unrecorded, rows);
        receipts.push(Receipt {
            message,
            started: sequence * 2 + 1,
            finished: sequence * 2 + 2,
            body,
        });
    }
    let final_rows: Vec<_> = rows.values().copied().collect();
    assert_eq!(
        oracle::check(&initial, &receipts, &final_rows, 64).status,
        Status::Valid
    );
    let observed = receipts
        .iter_mut()
        .flat_map(|receipt| &mut receipt.body.operations)
        .find_map(|operation| match operation {
            Operation::Query {
                query: RecordedQuery::GlobalDue { .. },
                rows,
            } if rows.len() > 16 => Some(rows),
            _ => None,
        })
        .expect("fleet history must contain a wide complete due observation");
    observed.pop();
    assert_eq!(
        oracle::check(&initial, &receipts, &final_rows, 128).status,
        Status::Invalid
    );
}

#[test]
fn fleet_warmup_is_the_exact_preceding_prefix_and_timed_ids_are_distinct() {
    let (families, seed) = (16, 20260925);
    let mut rows: BTreeMap<_, _> = shared_model::initial_records(families * 2)
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    let mut warmup_ids = std::collections::BTreeSet::new();
    for sequence in 0..16 * families as u64 {
        let message = model::fleet_message(sequence, families, seed);
        assert!(warmup_ids.insert(message.id));
        model::serial_apply(&mut rows, &message).unwrap();
    }
    assert_eq!(
        rows.values().copied().collect::<Vec<_>>(),
        model::sustained_initial_for("fleet", families, seed)
    );
    for sequence in 0..16 * families as u64 {
        let timed = model::sustained_message_for("fleet", sequence, 0, 1, families, seed, 0);
        assert!(!warmup_ids.contains(&timed.id));
        assert_eq!(
            timed,
            model::fleet_message(sequence + 16 * families as u64, families, seed)
        );
    }
}

#[test]
fn fleet_keeps_many_families_live_and_exercises_wide_mutating_background_queries() {
    for families in [16, 32] {
        let seed = 20260925;
        let initial = model::sustained_initial_for("fleet", families, seed);
        let mut rows: BTreeMap<_, _> = initial.into_iter().map(|row| (row.id, row)).collect();
        let mut min_live = usize::MAX;
        let mut max_due_rows = 0;
        let mut max_due_families = 0;
        let mut max_affected_families = 0;
        let mut positive = BTreeMap::<&str, usize>::new();
        let mut expired = 0;
        let mut created = 0;
        let mut generations_by_allocation = BTreeMap::<_, std::collections::BTreeSet<i64>>::new();
        for sequence in 0..64 * families as u64 {
            let message = model::sustained_message_for("fleet", sequence, 0, 1, families, seed, 0);
            let body = model::serial_apply(&mut rows, &message).unwrap();
            assert!(
                !body.outcome.missing_family && !body.outcome.allocation_deferred,
                "serial fleet should not collapse into missing/deferred work: {message:?}"
            );
            let live: std::collections::BTreeSet<_> = rows
                .values()
                .filter(|row| row.active && row.kind == shared_model::FLIGHT)
                .map(|row| row.family)
                .collect();
            min_live = min_live.min(live.len());
            let writes: Vec<_> = body
                .operations
                .iter()
                .filter_map(|operation| match operation {
                    Operation::Write { row } => Some(*row),
                    _ => None,
                })
                .collect();
            assert!(writes.len() <= shared_model::SLOTS_PER_FAMILY);
            assert!(
                writes
                    .iter()
                    .map(|row| row.id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    <= shared_model::SLOTS_PER_FAMILY
            );
            for operation in &body.operations {
                if let Operation::Query {
                    query: RecordedQuery::GlobalDue { .. },
                    rows,
                } = operation
                {
                    max_due_rows = max_due_rows.max(rows.len());
                    max_due_families = max_due_families.max(
                        rows.iter()
                            .map(|row| row.family)
                            .collect::<std::collections::BTreeSet<_>>()
                            .len(),
                    );
                }
            }
            if message.kind.name().starts_with("global_") && !writes.is_empty() {
                *positive.entry(message.kind.name()).or_default() += 1;
                max_affected_families = max_affected_families.max(
                    writes
                        .iter()
                        .map(|row| row.family)
                        .collect::<std::collections::BTreeSet<_>>()
                        .len(),
                );
            }
            expired += body.outcome.expired_families;
            created += body.outcome.created_views;
            for row in writes
                .iter()
                .filter(|row| row.active && row.kind == shared_model::FLIGHT)
            {
                generations_by_allocation
                    .entry(row.family)
                    .or_default()
                    .insert(row.tail);
            }
            model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
        }
        println!("fleet F={families} min_live={min_live} max_due_rows={max_due_rows} max_due_families={max_due_families} max_affected_families={max_affected_families} positive={positive:?} expired={expired} created={created}");
        assert!(min_live >= families / 2);
        assert!(max_due_rows > 16 && max_due_families >= 4 && max_affected_families >= 2);
        for kind in [
            "global_projection",
            "global_reschedule",
            "global_cancel",
            "global_housekeeping",
        ] {
            assert!(
                positive.get(kind).copied().unwrap_or(0) > 0,
                "background {kind} never mutates"
            );
        }
        assert!(expired >= families * 3 && created >= families * 3);
        assert!(
            generations_by_allocation
                .values()
                .any(|identities| identities.len() >= 2),
            "reserved physical slots must actually be reused for different generations"
        );
    }
}

fn history(scenario: &model::Scenario, reverse: bool) -> (Vec<Receipt>, Vec<Record>) {
    let mut rows: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let messages: Vec<_> = if reverse {
        scenario.messages.iter().rev().collect()
    } else {
        scenario.messages.iter().collect()
    };
    let receipts = messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| Receipt {
            message: message.clone(),
            started: index as u64 + 1,
            finished: 1000 - index as u64, // Response order deliberately opposes serialization.
            body: model::serial_apply(&mut rows, message).unwrap(),
        })
        .collect();
    (receipts, rows.values().copied().collect())
}

#[test]
fn every_scenario_accepts_both_serial_orders_with_reversed_response_order() {
    for scenario in model::scenarios(71) {
        for reverse in [false, true] {
            let (receipts, final_rows) = history(&scenario, reverse);
            let result = oracle::check(&scenario.initial, &receipts, &final_rows, 10000);
            assert_eq!(
                result.status,
                Status::Valid,
                "{}: {}",
                scenario.name,
                result.detail
            );
            let mut observed_ids = result.order.clone();
            let mut expected_ids: Vec<_> =
                receipts.iter().map(|receipt| receipt.message.id).collect();
            observed_ids.sort_unstable();
            expected_ids.sort_unstable();
            assert_eq!(observed_ids, expected_ids);
            model::validate_snapshot(&final_rows).unwrap();
        }
    }
}

#[test]
fn competing_creators_match_the_winner_instead_of_their_allocation_hint() {
    let scenario = model::scenarios(19).remove(0);
    let (receipts, rows) = history(&scenario, true);
    let winning_family = scenario.messages.last().unwrap().allocation_family as i64;
    assert!(receipts
        .iter()
        .all(|receipt| receipt.body.outcome.family == Some(winning_family)));
    assert_eq!(
        rows.iter()
            .filter(|row| row.active && row.kind == shared_model::FLIGHT)
            .count(),
        7
    );
    assert!(rows
        .iter()
        .filter(|row| row.active)
        .all(|row| row.family == winning_family));
    assert!(receipts[0].body.operations.iter().any(|op| matches!(op,
        Operation::Query { query: RecordedQuery::Candidates { .. }, rows } if rows.is_empty())));
}

#[test]
fn oracle_rejects_two_creators_that_both_commit_after_empty_searches() {
    let scenario = model::scenarios(1).remove(0);
    let mut final_rows: BTreeMap<_, _> =
        scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let mut receipts = Vec::new();
    for message in &scenario.messages[..2] {
        let mut isolated: BTreeMap<_, _> =
            scenario.initial.iter().map(|row| (row.id, *row)).collect();
        let body = model::serial_apply(&mut isolated, message).unwrap();
        for (id, row) in isolated {
            if row != final_rows[&id] && row.active {
                final_rows.insert(id, row);
            }
        }
        receipts.push(Receipt {
            message: message.clone(),
            started: 1,
            finished: 10,
            body,
        });
    }
    let result = oracle::check(
        &scenario.initial,
        &receipts,
        &final_rows.values().copied().collect::<Vec<_>>(),
        100,
    );
    assert_eq!(result.status, Status::Invalid);
}

#[test]
fn oracle_rejects_incomplete_candidate_results_even_when_final_state_matches() {
    let scenario = model::scenarios(2).remove(1);
    let (mut receipts, rows) = history(&scenario, false);
    let query = receipts[0]
        .body
        .operations
        .iter_mut()
        .find_map(|operation| match operation {
            Operation::Query {
                query: RecordedQuery::Candidates { .. },
                rows,
            } if !rows.is_empty() => Some(rows),
            _ => None,
        })
        .unwrap();
    query.pop();
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 10000).status,
        Status::Invalid
    );
}

#[test]
fn oracle_rejects_missing_write_wrong_output_and_lost_final_effect() {
    let scenario = model::scenarios(3).remove(1);
    let (receipts, rows) = history(&scenario, false);
    let mut missing = receipts.clone();
    let index = missing[0]
        .body
        .operations
        .iter()
        .position(|operation| matches!(operation, Operation::Write { .. }))
        .unwrap();
    missing[0].body.operations.remove(index);
    assert_eq!(
        oracle::check(&scenario.initial, &missing, &rows, 10000).status,
        Status::Invalid
    );
    let mut output = receipts.clone();
    output[0].body.outcome.outputs[0].latitude += 1;
    assert_eq!(
        oracle::check(&scenario.initial, &output, &rows, 10000).status,
        Status::Invalid
    );
    let mut lost = rows.clone();
    let index = lost
        .iter()
        .position(|row| *row != scenario.initial[row.id])
        .unwrap();
    lost[index] = scenario.initial[lost[index].id];
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &lost, 10000).status,
        Status::Invalid
    );
}

#[test]
fn nonoverlapping_intervals_are_binding_but_response_order_is_not() {
    let mut scenario = model::scenarios(4).remove(0);
    scenario.messages.truncate(2);
    let (mut receipts, rows) = history(&scenario, false);
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 100).status,
        Status::Valid
    );
    receipts[1].started = 1;
    receipts[1].finished = 10;
    receipts[0].started = 11;
    receipts[0].finished = 20;
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 100).status,
        Status::Invalid
    );
}

#[test]
fn insufficient_search_budget_is_inconclusive_and_never_passes() {
    let scenario = model::scenarios(5).remove(1);
    let (receipts, rows) = history(&scenario, false);
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 0).status,
        Status::Inconclusive
    );
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 1).status,
        Status::Inconclusive
    );
}

#[test]
fn fixed_seed_stream_has_mixed_kinds_hot_identity_and_real_key_changes() {
    let initial = model::sustained_initial(4, 99);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let mut renamed = false;
    let mut kinds = BTreeMap::<String, usize>::new();
    let mut receipts = Vec::new();
    for sequence in 0..128 {
        let message = model::sustained_message(sequence, sequence as usize % 4, 4, 4, 99, 100);
        assert_eq!(message.tail, 10000);
        *kinds
            .entry(format!("{:?}", std::mem::discriminant(&message.kind)))
            .or_default() += 1;
        let body = model::serial_apply(&mut rows, &message).unwrap();
        renamed |= rows
            .values()
            .any(|row| row.active && row.kind == shared_model::FLIGHT && row.callsign >= 1000);
        receipts.push(Receipt {
            message,
            started: sequence * 2 + 1,
            finished: sequence * 2 + 2,
            body,
        });
    }
    assert!(
        renamed,
        "sustained rename must actually change indexed keys"
    );
    assert_eq!(kinds.len(), 5);
    model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
    assert_eq!(
        oracle::check(
            &initial,
            &receipts,
            &rows.values().copied().collect::<Vec<_>>(),
            128
        )
        .status,
        Status::Valid
    );
}

#[test]
fn provenance_output_never_contains_an_excluded_source() {
    let scenario = model::scenarios(33).remove(1);
    let (receipts, rows) = history(&scenario, false);
    assert!(receipts
        .iter()
        .flat_map(|receipt| &receipt.body.outcome.outputs)
        .all(|output| output.source & !output.pedigree == 0));
    assert!(rows
        .iter()
        .filter(|row| row.active && row.kind != shared_model::DEDUP)
        .all(|row| row.source & !row.pedigree == 0));
}

#[test]
fn independent_structure_checker_rejects_duplicate_identity_and_changed_family_evidence() {
    let scenario = model::scenarios(77).remove(0);
    let mut combined: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    for message in &scenario.messages[..2] {
        let mut isolated: BTreeMap<_, _> =
            scenario.initial.iter().map(|row| (row.id, *row)).collect();
        model::serial_apply(&mut isolated, message).unwrap();
        combined.extend(isolated.into_iter().filter(|(_, row)| row.active));
    }
    let rows: Vec<_> = combined.values().copied().collect();
    shared_model::validate_snapshot(&rows).unwrap();
    assert!(model::validate_snapshot(&rows)
        .unwrap_err()
        .contains("independent families"));
    let mut rows = model::sustained_initial(4, 77);
    model::validate_snapshot(&rows).unwrap();
    rows.iter_mut()
        .find(|row| row.active && row.kind == shared_model::FLIGHT && row.pedigree == 3)
        .unwrap()
        .tail += 100;
    assert!(model::validate_snapshot(&rows)
        .unwrap_err()
        .contains("inconsistent immutable identity"));
}

#[test]
fn conflict_with_failed_cleanup_is_fatal_and_cannot_be_retried() {
    use storage::{DbError, Query, Store};
    struct FailedCleanup {
        at_commit: bool,
    }
    impl Store for FailedCleanup {
        fn begin(&mut self, slots: &[usize]) -> Result<(), DbError> {
            assert!(slots.is_empty());
            Ok(())
        }
        fn read(&mut self, _: usize) -> Result<Record, DbError> {
            unreachable!()
        }
        fn query(&mut self, _: &Query) -> Result<Vec<Record>, DbError> {
            if self.at_commit {
                Ok(vec![])
            } else {
                Err(DbError::Conflict)
            }
        }
        fn write(&mut self, _: Record) -> Result<(), DbError> {
            unreachable!()
        }
        fn savepoint(&mut self) -> Result<usize, DbError> {
            unreachable!()
        }
        fn rollback_to(&mut self, _: usize) -> Result<(), DbError> {
            unreachable!()
        }
        fn commit(&mut self) -> Result<(), DbError> {
            Err(DbError::Conflict)
        }
        fn abort(&mut self) -> Result<(), DbError> {
            Err(DbError::Fatal("injected cleanup failure".into()))
        }
    }
    // Empty maintenance returns before any physical reads/writes, allowing both
    // the handler-error and commit-error cleanup paths to be exercised.
    let mut message = model::scenarios(1).remove(0).messages.remove(0);
    message.kind = model::MessageKind::Project { at: 0 };
    for at_commit in [false, true] {
        for record in [false, true] {
            let error = model::execute_attempt_with_recording(
                &mut FailedCleanup { at_commit },
                &message,
                record,
            )
            .unwrap_err();
            assert!(
                matches!(error, DbError::Fatal(detail) if detail.contains("abort failed") && detail.contains("injected cleanup failure"))
            );
        }
    }
}

#[test]
fn new_global_predicates_include_other_families_and_keep_strict_expiry_boundary() {
    use storage::Query;
    let rows = model::sustained_initial(3, 7);
    let due: Vec<_> = rows
        .iter()
        .filter(|row| Query::GlobalDue { at: i64::MAX }.matches(row))
        .collect();
    assert_eq!(due.len(), 21);
    assert_eq!(
        due.iter()
            .map(|row| row.family)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 2, 4].into()
    );
    assert!(!Query::GlobalDue { at: i64::MAX }.matches(&Record {
        active: false,
        kind: shared_model::SCHEDULED,
        due: 0,
        ..Record::default()
    }));
    let row = Record {
        active: true,
        kind: shared_model::POSITION,
        family: 99,
        event_time: 40,
        ..Record::default()
    };
    assert!(!Query::GlobalExpired { before: 40 }.matches(&row));
    assert!(Query::GlobalExpired { before: 41 }.matches(&row));
    assert!(!Query::GlobalExpired { before: 41 }.matches(&Record {
        kind: shared_model::FLIGHT,
        ..row
    }));
    // Existing queries retain their old semantics exactly, including all
    // inactive rows and each scalar edge case in the original fixture.
    for row in &rows {
        for family in [0, 2, 4, 99] {
            assert_eq!(
                Query::Due {
                    family,
                    at: i64::MAX
                }
                .matches(row),
                shared_model::Query::Due {
                    family,
                    at: i64::MAX
                }
                .matches(row)
            );
            assert_eq!(
                Query::Expired {
                    family,
                    before: i64::MAX
                }
                .matches(row),
                shared_model::Query::Expired {
                    family,
                    before: i64::MAX
                }
                .matches(row)
            );
        }
    }
}

#[test]
fn global_claim_observes_all_due_events_but_mutates_only_bounded_selected_events() {
    let scenario = model::scenarios(41).remove(3);
    let mut rows: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let body = model::serial_apply(&mut rows, &scenario.messages[0]).unwrap();
    let observed = body
        .operations
        .iter()
        .find_map(|operation| match operation {
            Operation::Query {
                query: RecordedQuery::GlobalDue { .. },
                rows,
            } => Some(rows),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        observed.len(),
        21,
        "global query may not silently inherit the effect limit"
    );
    assert_eq!(
        observed
            .iter()
            .map(|row| row.family)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 2, 4].into()
    );
    assert_eq!(body.outcome.claimed_events, 8);
    assert_eq!(body.outcome.rescheduled_events, 8);
    assert_eq!(body.outcome.outputs.len(), 8);
    assert_eq!(
        body.operations
            .iter()
            .filter(|operation| matches!(operation, Operation::Write { .. }))
            .count(),
        16
    );
    assert_eq!(
        body.outcome
            .outputs
            .iter()
            .map(|output| output.family)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 2].into()
    );
    assert_eq!(
        rows.values()
            .filter(|row| row.active
                && row.kind == shared_model::SCHEDULED
                && row.due == 1_700_000_530)
            .count(),
        8
    );
    model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
}

#[test]
fn oracle_rejects_an_omitted_global_candidate_even_outside_selected_batch() {
    let mut scenario = model::scenarios(42).remove(3);
    scenario.messages.truncate(1);
    let (mut receipts, rows) = history(&scenario, false);
    let complete = receipts[0]
        .body
        .operations
        .iter_mut()
        .find_map(|operation| match operation {
            Operation::Query {
                query: RecordedQuery::GlobalDue { .. },
                rows,
            } => Some(rows),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        complete.pop().unwrap().family,
        4,
        "omitted event belongs to an unmodified family"
    );
    assert_eq!(
        oracle::check(&scenario.initial, &receipts, &rows, 100).status,
        Status::Invalid
    );
}

#[test]
fn duplicate_global_claims_cannot_both_consume_the_same_due_snapshot() {
    let mut scenario = model::scenarios(43).remove(3);
    scenario.messages.truncate(2);
    scenario.messages[1] = scenario.messages[0].clone();
    scenario.messages[1].id += 100;
    // Both isolated claims consume the same first eight scheduled rows.
    let initial: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let mut combined = initial.clone();
    let mut receipts = Vec::new();
    for message in &scenario.messages {
        let mut store = model::ReferenceStore::new(&initial);
        let body = model::execute_attempt(&mut store, message).unwrap();
        combined.extend(store.writes);
        receipts.push(Receipt {
            message: message.clone(),
            started: 1,
            finished: 10,
            body,
        });
    }
    assert_eq!(
        oracle::check(
            &scenario.initial,
            &receipts,
            &combined.values().copied().collect::<Vec<_>>(),
            100
        )
        .status,
        Status::Invalid
    );
}

#[test]
fn complete_family_expiry_requires_all_terminal_views_then_reuses_slots_cleanly() {
    let initial = model::sustained_initial_for("lifecycle", 1, 11);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    for sequence in 0..11 {
        let message = model::sustained_message_for("lifecycle", sequence, 0, 1, 1, 11, 100);
        model::serial_apply(&mut rows, &message).unwrap();
        model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
        if sequence == 8 {
            let before = rows.clone();
            let mut expiration = model::sustained_message_for("lifecycle", 11, 0, 1, 1, 11, 100);
            expiration.id += 1_000;
            assert_eq!(
                model::serial_apply(&mut rows, &expiration)
                    .unwrap()
                    .outcome
                    .expired_families,
                0
            );
            assert_eq!(
                rows, before,
                "one source arrival cannot expire still-active source views"
            );
        }
    }
    assert_eq!(
        rows.values()
            .filter(|row| row.active && row.kind == shared_model::FLIGHT)
            .count(),
        7
    );
    let expected_deleted = rows.values().filter(|row| row.active).count();
    let expiry = model::sustained_message_for("lifecycle", 11, 0, 1, 1, 11, 100);
    let outcome = model::serial_apply(&mut rows, &expiry).unwrap().outcome;
    assert_eq!(outcome.expired_families, 1);
    assert_eq!(outcome.expired_records, expected_deleted);
    assert!(rows.values().all(|row| !row.active));
    let mut creation = model::sustained_message_for("lifecycle", 32, 0, 1, 1, 11, 100);
    assert_eq!(creation.allocation_family, 0);
    creation.source = 2;
    let outcome = model::serial_apply(&mut rows, &creation).unwrap().outcome;
    assert_eq!(outcome.created_views, 1);
    let flights: Vec<_> = rows
        .values()
        .filter(|row| row.active && row.kind == shared_model::FLIGHT)
        .collect();
    assert_eq!(flights.len(), 1);
    assert_eq!(flights[0].tail, creation.tail);
    assert_eq!(flights[0].pedigree, 2);
    assert_eq!(flights[0].source, 2);
    assert_eq!(
        flights[0].latitude, 0,
        "old generation's position must not survive reuse"
    );
    model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
}

#[test]
fn lifecycle_corpus_is_independent_of_workers_and_preserves_legacy_generation() {
    for sequence in 0..128 {
        assert_eq!(
            model::sustained_message_for("legacy", sequence, 1, 4, 4, 99, 80),
            model::sustained_message(sequence, 1, 4, 4, 99, 80)
        );
        let expected = model::sustained_message_for("lifecycle", sequence, 0, 1, 4, 99, 80);
        for workers in [2, 4, 8, 16] {
            assert_eq!(
                model::sustained_message_for(
                    "lifecycle",
                    sequence,
                    sequence as usize % workers,
                    workers,
                    4,
                    99,
                    80
                ),
                expected
            );
        }
    }
    assert_eq!(
        model::sustained_initial_for("legacy", 4, 99),
        model::sustained_initial(4, 99)
    );
}

#[test]
fn lifecycle_cycles_grow_expire_and_reuse_without_unbounded_logical_state() {
    let initial = model::sustained_initial_for("lifecycle", 3, 99);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let mut receipts = Vec::new();
    let (mut created, mut expired) = (0, 0);
    for sequence in 0..16 * 12 {
        let message = model::sustained_message_for(
            "lifecycle",
            sequence,
            sequence as usize % 4,
            4,
            3,
            99,
            80,
        );
        let body = model::serial_apply(&mut rows, &message).unwrap();
        created += body.outcome.created_views;
        expired += body.outcome.expired_families;
        assert!(
            body.operations
                .iter()
                .filter(|operation| matches!(operation, Operation::Write { .. }))
                .count()
                <= shared_model::SLOTS_PER_FAMILY
        );
        model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
        receipts.push(Receipt {
            message,
            started: sequence * 2 + 1,
            finished: sequence * 2 + 2,
            body,
        });
    }
    assert_eq!(created, 12 * 7);
    assert_eq!(expired, 12);
    assert!(rows.values().all(|row| !row.active));
    assert_eq!(
        oracle::check(
            &initial,
            &receipts,
            &rows.values().copied().collect::<Vec<_>>(),
            192
        )
        .status,
        Status::Valid
    );
}

#[test]
fn occupied_pool_defers_new_generation_and_late_messages_do_not_resurrect_it() {
    let initial = model::sustained_initial_for("lifecycle", 1, 19);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let first = model::sustained_message_for("lifecycle", 0, 0, 1, 1, 19, 100);
    model::serial_apply(&mut rows, &first).unwrap();
    let before = rows.clone();
    let later = model::sustained_message_for("lifecycle", 32, 0, 1, 1, 19, 100);
    assert!(
        model::serial_apply(&mut rows, &later)
            .unwrap()
            .outcome
            .allocation_deferred
    );
    assert_eq!(rows, before);
    let late_observation = model::sustained_message_for("lifecycle", 33, 0, 1, 1, 19, 100);
    assert!(
        model::serial_apply(&mut rows, &late_observation)
            .unwrap()
            .outcome
            .missing_family
    );
    assert_eq!(rows, before);
}

#[test]
fn delayed_creation_after_all_arrivals_ages_out_and_lifecycle_turnover_resumes() {
    let initial = model::sustained_initial_for("lifecycle", 1, 29);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let generated =
        |sequence| model::sustained_message_for("lifecycle", sequence, 0, 1, 1, 29, 100);
    for sequence in 8..=10 {
        assert!(
            model::serial_apply(&mut rows, &generated(sequence))
                .unwrap()
                .outcome
                .missing_family
        );
    }
    let first = generated(0);
    assert_eq!(
        model::serial_apply(&mut rows, &first)
            .unwrap()
            .outcome
            .created_views,
        1
    );
    let mut deferred = 0;
    let mut stale_expirations = 0;
    let mut resumed_creation = false;
    for sequence in 16..96 {
        let message = generated(sequence);
        let old_nonterminal = rows.values().any(|row| {
            row.active
                && row.kind == shared_model::FLIGHT
                && row.tail == first.tail
                && row.status != shared_model::ARRIVED
        });
        let body = model::serial_apply(&mut rows, &message).unwrap();
        deferred += usize::from(body.outcome.allocation_deferred);
        if old_nonterminal && body.outcome.expired_families > 0 && body.outcome.family == Some(0) {
            stale_expirations += 1;
        }
        if sequence == 64 {
            resumed_creation = body.outcome.created_views == 1 && body.outcome.family == Some(0);
        }
        assert!(
            body.operations
                .iter()
                .filter(|operation| matches!(operation, Operation::Write { .. }))
                .count()
                <= shared_model::SLOTS_PER_FAMILY
        );
        model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
    }
    assert!(
        deferred > 0,
        "the held live allocation must initially defer reuse"
    );
    assert_eq!(
        stale_expirations, 1,
        "a stranded nonterminal generation must eventually age out"
    );
    assert!(
        resumed_creation,
        "later generations must reuse the released physical slot"
    );
    assert!(rows.values().all(|row| !row.active));
}

#[test]
fn stale_expiry_is_strict_and_one_fresh_view_retains_the_entire_family() {
    let initial = model::sustained_initial_for("lifecycle", 1, 31);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    for sequence in 0..6 {
        let message = model::sustained_message_for("lifecycle", sequence, 0, 1, 1, 31, 100);
        model::serial_apply(&mut rows, &message).unwrap();
    }
    let freshest = rows
        .values()
        .filter(|row| row.active && row.kind == shared_model::FLIGHT)
        .map(|row| row.event_time)
        .max()
        .unwrap();
    let before = rows.clone();
    let mut expiry = model::sustained_message_for("lifecycle", 11, 0, 1, 1, 31, 100);
    expiry.kind = model::MessageKind::ExpireFamily {
        family: 0,
        before: freshest + 1000,
        stale_before: Some(freshest),
    };
    assert_eq!(
        model::serial_apply(&mut rows, &expiry)
            .unwrap()
            .outcome
            .expired_families,
        0
    );
    assert_eq!(
        rows, before,
        "strict boundary and every-view freshness must hold"
    );
    let expected_deleted = rows.values().filter(|row| row.active).count();
    expiry.kind = model::MessageKind::ExpireFamily {
        family: 0,
        before: freshest + 1000,
        stale_before: Some(freshest + 1),
    };
    let body = model::serial_apply(&mut rows, &expiry).unwrap();
    assert_eq!(body.outcome.expired_families, 1);
    assert_eq!(body.outcome.expired_records, expected_deleted);
    assert!(rows.values().all(|row| !row.active));
}

#[test]
fn late_activity_and_stale_expiry_allow_both_orders_but_reject_stale_parallel_claims() {
    let initial = model::sustained_initial_for("lifecycle", 1, 37);
    let mut base: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let generated =
        |sequence| model::sustained_message_for("lifecycle", sequence, 0, 1, 1, 37, 100);
    let creation = generated(0);
    model::serial_apply(&mut base, &creation).unwrap();
    let initial: Vec<_> = base.values().copied().collect();
    let position = generated(1);
    let mut expiry = generated(11);
    expiry.kind = model::MessageKind::ExpireFamily {
        family: 0,
        before: creation.event_time + 100,
        stale_before: Some(creation.event_time + 1),
    };
    for messages in [
        [position.clone(), expiry.clone()],
        [expiry.clone(), position.clone()],
    ] {
        let mut rows = base.clone();
        let mut receipts = Vec::new();
        for message in messages {
            let body = model::serial_apply(&mut rows, &message).unwrap();
            receipts.push(Receipt {
                message,
                started: 1,
                finished: 10,
                body,
            });
        }
        assert_eq!(
            oracle::check(
                &initial,
                &receipts,
                &rows.values().copied().collect::<Vec<_>>(),
                10
            )
            .status,
            Status::Valid
        );
        if receipts[0].message.id == position.id {
            assert_eq!(receipts[1].body.outcome.expired_families, 0);
        } else {
            assert!(receipts[1].body.outcome.missing_family);
        }
    }
    let mut combined = base.clone();
    let mut receipts = Vec::new();
    for message in [position, expiry] {
        let mut store = model::ReferenceStore::new(&base);
        let body = model::execute_attempt(&mut store, &message).unwrap();
        combined.extend(store.writes);
        receipts.push(Receipt {
            message,
            started: 1,
            finished: 10,
            body,
        });
    }
    assert_eq!(
        oracle::check(
            &initial,
            &receipts,
            &combined.values().copied().collect::<Vec<_>>(),
            10
        )
        .status,
        Status::Invalid
    );
}

#[test]
fn global_housekeeping_cannot_erase_live_flights_or_due_events() {
    let initial = model::sustained_initial_for("lifecycle", 1, 41);
    let mut rows: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    for sequence in 0..6 {
        let message = model::sustained_message_for("lifecycle", sequence, 0, 1, 1, 41, 100);
        model::serial_apply(&mut rows, &message).unwrap();
    }
    let retained: BTreeMap<_, _> = rows
        .iter()
        .filter(|(_, row)| {
            row.active && matches!(row.kind, shared_model::FLIGHT | shared_model::SCHEDULED)
        })
        .map(|(id, row)| (*id, *row))
        .collect();
    let expected_expired = rows
        .values()
        .filter(|row| {
            row.active
                && matches!(
                    row.kind,
                    shared_model::POSITION | shared_model::OUTBOX | shared_model::DEDUP
                )
        })
        .count();
    assert!(expected_expired > 0 && expected_expired < model::MAX_GLOBAL_EXPIRED);
    let mut housekeeping = model::sustained_message_for("lifecycle", 12, 0, 1, 1, 41, 100);
    housekeeping.kind = model::MessageKind::GlobalHousekeeping {
        before: i64::MAX,
        limit: model::MAX_GLOBAL_EXPIRED,
    };
    let body = model::serial_apply(&mut rows, &housekeeping).unwrap();
    assert_eq!(body.outcome.expired_records, expected_expired);
    assert_eq!(
        rows.values().filter(|row| row.active).count(),
        retained.len()
    );
    for (id, row) in retained {
        assert_eq!(rows[&id], row);
    }
    model::validate_snapshot(&rows.values().copied().collect::<Vec<_>>()).unwrap();
}

#[test]
fn record_free_execution_has_identical_effects_and_outcomes_but_is_not_oracle_evidence() {
    for scenario in model::scenarios(91) {
        let mut full: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
        let mut unrecorded = full.clone();
        for message in &scenario.messages {
            let expected = model::serial_apply(&mut full, message).unwrap();
            let mut store = model::ReferenceStore::new(&unrecorded);
            let actual = model::execute_without_recording(&mut store, message).unwrap();
            unrecorded.extend(store.writes);
            assert_eq!(actual, expected.outcome);
            assert_eq!(unrecorded, full);
        }
    }
    let mut scenario = model::scenarios(91).remove(3);
    scenario.messages.truncate(1);
    let initial: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let mut store = model::ReferenceStore::new(&initial);
    let body =
        model::execute_attempt_with_recording(&mut store, &scenario.messages[0], false).unwrap();
    assert!(body.operations.is_empty());
    let mut final_rows = initial.clone();
    final_rows.extend(store.writes);
    let receipt = Receipt {
        message: scenario.messages[0].clone(),
        started: 1,
        finished: 2,
        body,
    };
    assert_eq!(
        oracle::check(
            &scenario.initial,
            &[receipt],
            &final_rows.values().copied().collect::<Vec<_>>(),
            100
        )
        .status,
        Status::Invalid
    );
}

#[test]
fn invalid_global_batch_is_rejected_before_any_mutation() {
    let scenario = model::scenarios(44).remove(3);
    let mut rows: BTreeMap<_, _> = scenario.initial.iter().map(|row| (row.id, *row)).collect();
    let before = rows.clone();
    let mut message = scenario.messages[0].clone();
    for kind in [
        model::MessageKind::GlobalProject {
            at: i64::MAX,
            limit: model::MAX_GLOBAL_EVENTS + 1,
        },
        model::MessageKind::GlobalHousekeeping {
            before: i64::MAX,
            limit: model::MAX_GLOBAL_EXPIRED + 1,
        },
        model::MessageKind::GlobalReschedule {
            at: 100,
            due: 100,
            limit: 1,
        },
        model::MessageKind::ExpireFamily {
            family: 0,
            before: 100,
            stale_before: Some(100),
        },
    ] {
        message.kind = kind;
        assert!(matches!(
            model::serial_apply(&mut rows, &message),
            Err(storage::DbError::Fatal(_))
        ));
        assert_eq!(rows, before);
    }
}
