//! Independent schedule, population and clock checks for the partially
//! calibrated fixed-population profile. These are functional model tests,
//! not evidence of waiting five minutes or running complete maintenance jobs.
#![allow(dead_code)]
#[path = "../benches/extended_crucible/model.rs"]
pub mod shared_model;
mod extended_crucible {
    pub use crate::shared_model as model;
}
#[path = "../benches/contention_crucible/calibrated.rs"]
mod calibrated;
#[path = "../benches/contention_crucible/model.rs"]
mod model;
#[path = "../benches/contention_crucible/oracle.rs"]
mod oracle;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;

use calibrated::{Config, EventClass, Schedule, ScheduledEvent, EVENT_EPOCH_NS, NANOS_PER_SECOND};
use model::{MessageKind, Operation};
use shared_model::{Record, FLIGHT, POSITION, SCHEDULED};
use std::collections::{BTreeMap, BTreeSet};

fn config() -> Config {
    Config {
        duration_ns: 8 * NANOS_PER_SECOND,
        foreground_rate: 24,
        foreground_workers: 4,
        families: 16,
        seed: 20260925,
        projection_interval_seconds: 1,
        housekeeping_interval_seconds: 2,
        dispatch: calibrated::Dispatch::Identity,
        affinity_ttl_ms: 0,
        signature_pattern: calibrated::SignaturePattern::Both,
    }
}
fn events(schedule: &Schedule) -> Vec<ScheduledEvent> {
    let mut events: Vec<_> = (0..schedule.worker_count())
        .flat_map(|worker| {
            (0..schedule.worker_offered(worker))
                .map(move |ordinal| schedule.event(worker, ordinal).unwrap())
        })
        .collect();
    events.sort_by_key(|event| (event.offset_ns, event.message.id));
    events
}
fn as_map(rows: &[Record]) -> BTreeMap<usize, Record> {
    rows.iter().map(|row| (row.id, *row)).collect()
}
fn live_families(rows: &[Record]) -> BTreeSet<i64> {
    rows.iter()
        .filter(|row| row.active && row.kind == FLIGHT)
        .map(|row| row.family)
        .collect()
}

#[test]
fn foreground_corpus_is_worker_independent_and_flight_fifo() {
    let baseline = events(&Schedule::new(config()).unwrap());
    for workers in [1, 2, 3, 4, 8, 16, 32] {
        let schedule = Schedule::new(Config {
            foreground_workers: workers,
            ..config()
        })
        .unwrap();
        assert_eq!(events(&schedule), baseline);
        let mut ids = BTreeSet::new();
        for worker in 0..schedule.worker_count() {
            let mut preceding = BTreeMap::new();
            let mut last_offset = 0;
            for ordinal in 0..schedule.worker_offered(worker) {
                let event = schedule.event(worker, ordinal).unwrap();
                assert!(ids.insert(event.message.id));
                assert!(event.offset_ns >= last_offset);
                assert!(event.offset_ns < schedule.config.duration_ns);
                last_offset = event.offset_ns;
                if let Some(identity) = event.logical_identity {
                    assert!(identity < 12, "quiet quarter must receive no foreground");
                    assert_eq!(identity % workers, worker);
                    let q = event.message.id - 1_000_000;
                    assert_eq!(identity as u64, q % 12);
                    assert_eq!(event.foreground_ordinal, Some(q / 12));
                    assert_eq!(event.offset_ns, q * NANOS_PER_SECOND / 24);
                    let expected_ordinal = preceding.get(&identity).map_or(0, |n| n + 1);
                    assert_eq!(event.foreground_ordinal, Some(expected_ordinal));
                    preceding.insert(identity, expected_ordinal);
                    assert_eq!(
                        event.message.kind.name(),
                        if expected_ordinal % 16 == 0 {
                            "plan"
                        } else {
                            "position"
                        }
                    );
                }
            }
            assert!(schedule
                .event(worker, schedule.worker_offered(worker))
                .is_none());
        }
        assert_eq!(ids.len(), 192 + 7 + 3);
    }
}

#[test]
fn real_timers_are_rate_independent_and_exclude_the_admission_endpoint() {
    for (seconds, expected_projection, expected_housekeeping) in [
        (299, vec![], vec![]),
        (300, vec![], vec![]),
        (600, vec![300], vec![]),
        (601, vec![300, 600], vec![600]),
        (1201, vec![300, 600, 900, 1200], vec![600, 1200]),
    ] {
        for rate in [1, 100, 1000] {
            let schedule = Schedule::new(Config {
                duration_ns: seconds * NANOS_PER_SECOND,
                foreground_rate: rate,
                projection_interval_seconds: 300,
                housekeeping_interval_seconds: 600,
                ..config()
            })
            .unwrap();
            assert!(schedule.cadence_in_operator_range());
            for (worker, expected) in [(4, &expected_projection), (5, &expected_housekeeping)] {
                let actual: Vec<_> = (0..schedule.worker_offered(worker))
                    .map(|n| schedule.event(worker, n).unwrap().offset_ns / NANOS_PER_SECOND)
                    .collect();
                assert_eq!(&actual, expected);
            }
        }
    }
    assert!(!Schedule::new(config()).unwrap().cadence_in_operator_range());
    assert_eq!(
        Schedule::new(config()).unwrap().maintenance_scope(),
        "bounded_batch_not_full_sweep"
    );
}

#[test]
fn sampled_backlog_obeys_nanosecond_boundaries_and_idle_workers_are_allowed() {
    let schedule = Schedule::new(Config {
        foreground_rate: 3,
        ..config()
    })
    .unwrap();
    for elapsed in [
        0,
        333_333_332,
        333_333_333,
        666_666_665,
        666_666_666,
        999_999_999,
        NANOS_PER_SECOND,
        7 * NANOS_PER_SECOND,
        u64::MAX,
    ] {
        for worker in 0..schedule.worker_count() + 2 {
            let due = (0..schedule.worker_offered(worker))
                .filter(|&ordinal| schedule.event(worker, ordinal).unwrap().offset_ns <= elapsed)
                .count() as u64;
            for completed in [0, 1, 3, 1000] {
                assert_eq!(
                    schedule.backlog(worker, completed, elapsed),
                    due.saturating_sub(completed)
                );
            }
        }
    }
    for (families, workers) in [(4, 8), (16, 4)] {
        let schedule = Schedule::new(Config {
            duration_ns: 1,
            foreground_rate: 1,
            foreground_workers: workers,
            families,
            ..config()
        })
        .unwrap();
        assert_eq!(schedule.foreground_offered(), 1);
        assert_eq!(schedule.worker_offered(0), 1);
        for worker in 1..schedule.worker_count() {
            assert_eq!(schedule.worker_offered(worker), 0);
            assert_eq!(schedule.backlog(worker, 0, u64::MAX), 0);
            assert!(schedule.event(worker, 0).is_none());
        }
    }
}

#[test]
fn malformed_or_unbounded_schedules_fail_before_arithmetic() {
    for invalid in [
        Config {
            duration_ns: 0,
            ..config()
        },
        Config {
            duration_ns: u64::MAX,
            ..config()
        },
        Config {
            foreground_rate: 0,
            ..config()
        },
        Config {
            foreground_rate: u64::MAX,
            ..config()
        },
        Config {
            foreground_workers: 0,
            ..config()
        },
        Config {
            foreground_workers: 33,
            ..config()
        },
        Config {
            families: 3,
            ..config()
        },
        Config {
            families: usize::MAX,
            ..config()
        },
        Config {
            projection_interval_seconds: 0,
            ..config()
        },
        Config {
            housekeeping_interval_seconds: u64::MAX,
            ..config()
        },
        Config {
            duration_ns: 3600 * NANOS_PER_SECOND,
            foreground_rate: 1000,
            ..config()
        },
    ] {
        assert!(Schedule::new(invalid).is_err());
    }
    assert!(calibrated::initial_records(0, 0).is_err());
}

#[test]
fn warmup_establishes_fixed_population_and_real_old_and_recent_quiet_history() {
    let schedule = Schedule::new(config()).unwrap();
    let initial = schedule.initial_records().unwrap();
    assert_eq!(
        initial,
        calibrated::initial_records(16, config().seed).unwrap()
    );
    assert_eq!(live_families(&initial).len(), 16);
    assert_eq!(
        initial
            .iter()
            .filter(|row| row.active && row.kind == FLIGHT)
            .count(),
        16 * 7
    );
    let due: Vec<_> = initial
        .iter()
        .filter(|row| storage::Query::GlobalDue { at: EVENT_EPOCH_NS }.matches(row))
        .collect();
    assert_eq!(due.len(), 4 * 7);
    assert!(due.iter().all(|row| row.family >= 24));
    let expired = initial
        .iter()
        .filter(|row| {
            storage::Query::GlobalExpired {
                before: EVENT_EPOCH_NS - 3600 * NANOS_PER_SECOND as i64,
            }
            .matches(row)
        })
        .count();
    assert_eq!(expired, 4 * 7 * 7);
    for family in [24, 26, 28, 30] {
        for pedigree in 1..=7 {
            let history: Vec<_> = initial
                .iter()
                .filter(|row| {
                    row.active
                        && row.kind == POSITION
                        && row.family == family
                        && row.pedigree == pedigree
                })
                .collect();
            assert_eq!(history.len(), 8);
            assert_eq!(
                history
                    .iter()
                    .filter(|row| row.event_time == EVENT_EPOCH_NS - 60 * NANOS_PER_SECOND as i64)
                    .count(),
                1
            );
        }
    }
    assert!(initial
        .iter()
        .filter(|row| row.active && row.sequence != 0)
        .all(|row| row.sequence < 1_000_000));
    model::validate_snapshot(&initial).unwrap();
}

#[test]
fn accelerated_serial_run_updates_every_foreground_and_keeps_maintenance_useful() {
    let schedule = Schedule::new(config()).unwrap();
    let initial = schedule.initial_records().unwrap();
    let mut rows = as_map(&initial);
    let mut unrecorded = rows.clone();
    let mut positive = BTreeMap::new();
    let mut projection_observed = 0;
    let mut expiry_observed = 0;
    for event in events(&schedule) {
        let body = model::serial_apply(&mut rows, &event.message).unwrap();
        let mut store = model::ReferenceStore::new(&unrecorded);
        assert_eq!(
            model::execute_without_recording(&mut store, &event.message).unwrap(),
            body.outcome
        );
        unrecorded.extend(store.writes);
        assert_eq!(rows, unrecorded);
        let writes: BTreeSet<_> = body
            .operations
            .iter()
            .filter_map(|op| match op {
                Operation::Write { row } => Some(row.id),
                _ => None,
            })
            .collect();
        assert!(writes.len() <= 128);
        assert!(!body.outcome.missing_family);
        assert!(!body.outcome.allocation_deferred);
        assert_eq!(body.outcome.created_views, 0);
        assert_eq!(body.outcome.expired_families, 0);
        assert_eq!(body.outcome.ignored_stale, 0);
        match event.class {
            EventClass::Foreground => assert!(body.outcome.updated_views > 0),
            EventClass::Projection => {
                assert_eq!(body.outcome.claimed_events, 4);
                assert_eq!(body.outcome.outputs.len(), 4);
                *positive.entry("projection").or_insert(0) += 1;
            }
            EventClass::Housekeeping => {
                assert_eq!(body.outcome.expired_records, 32);
                *positive.entry("housekeeping").or_insert(0) += 1;
            }
        }
        for op in &body.operations {
            match op {
                Operation::Query {
                    query: storage::Query::GlobalDue { .. },
                    rows,
                } => projection_observed = projection_observed.max(rows.len()),
                Operation::Query {
                    query: storage::Query::GlobalExpired { .. },
                    rows,
                } => expiry_observed = expiry_observed.max(rows.len()),
                _ => {}
            }
        }
    }
    assert_eq!(positive, [("projection", 7), ("housekeeping", 3)].into());
    assert_eq!(projection_observed, 28);
    assert_eq!(expiry_observed, 196);
    let final_rows: Vec<_> = rows.into_values().collect();
    assert_eq!(live_families(&final_rows), live_families(&initial));
    model::validate_snapshot(&final_rows).unwrap();
}

#[test]
fn ordered_subsecond_messages_update_and_schedule_thirty_physical_seconds() {
    let schedule = Schedule::new(Config {
        foreground_rate: 120,
        ..config()
    })
    .unwrap();
    let mut rows = as_map(&schedule.initial_records().unwrap());
    let messages: Vec<_> = events(&schedule)
        .into_iter()
        .filter(|event| event.logical_identity == Some(0))
        .take(3)
        .collect();
    for (ordinal, event) in messages.iter().enumerate() {
        assert_eq!(event.offset_ns, ordinal as u64 * 100_000_000);
        let body = model::serial_apply(&mut rows, &event.message).unwrap();
        assert!(body.outcome.updated_views > 0);
        assert_eq!(body.outcome.ignored_stale, 0);
        assert!(body
            .operations
            .iter()
            .filter_map(|op| match op {
                Operation::Write { row } if row.kind == SCHEDULED => Some(row),
                _ => None,
            })
            .all(|row| row.due == event.message.event_time + 30 * NANOS_PER_SECOND as i64));
    }
}

#[test]
fn projection_uses_seconds_for_motion_and_nanoseconds_for_rescheduling() {
    let schedule = Schedule::new(config()).unwrap();
    let initial = schedule.initial_records().unwrap();
    let mut rows = as_map(&initial);
    let event = schedule.event(4, 0).unwrap();
    assert_eq!(event.offset_ns, NANOS_PER_SECOND);
    let body = model::serial_apply(&mut rows, &event.message).unwrap();
    assert_eq!(body.outcome.outputs.len(), 4);
    for output in &body.outcome.outputs {
        let last = initial
            .iter()
            .filter(|row| {
                row.active
                    && row.kind == POSITION
                    && row.family == output.family
                    && row.pedigree == output.pedigree
            })
            .max_by_key(|row| (row.event_time, row.sequence, row.id))
            .unwrap();
        assert_eq!(
            output.latitude,
            last.latitude + 61 * last.ground_speed / 100
        );
        assert_eq!(
            output.longitude,
            last.longitude + 61 * last.ground_speed / 200
        );
        assert_eq!(output.event_time, EVENT_EPOCH_NS + NANOS_PER_SECOND as i64);
    }
    let due_writes: Vec<_> = body
        .operations
        .iter()
        .filter_map(|op| match op {
            Operation::Write { row } if row.kind == SCHEDULED => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(due_writes.len(), 4);
    assert!(due_writes
        .iter()
        .all(|row| row.due == EVENT_EPOCH_NS + 31 * NANOS_PER_SECOND as i64));
}

#[test]
fn unsupported_clock_units_and_due_overflow_fail_without_committing() {
    let schedule = Schedule::new(config()).unwrap();
    let initial = as_map(&schedule.initial_records().unwrap());
    let good = schedule.event(0, 0).unwrap().message;
    let mut bad = Vec::new();
    for units in [0, -1, 2, i64::MAX] {
        let mut message = good.clone();
        message.event_time_units_per_second = units;
        bad.push(message);
    }
    let mut overflow = good.clone();
    overflow.event_time = i64::MAX;
    bad.push(overflow);
    let mut projection = good;
    projection.kind = MessageKind::GlobalProject {
        at: i64::MAX,
        limit: 4,
    };
    bad.push(projection);
    for message in bad {
        let mut rows = initial.clone();
        assert!(matches!(
            model::serial_apply(&mut rows, &message),
            Err(storage::DbError::Fatal(_))
        ));
        assert_eq!(rows, initial);
    }
}

#[test]
fn legacy_message_json_omits_clock_units_and_deserializes_as_seconds() {
    let old = model::sustained_message_for("lifecycle", 123, 0, 1, 16, 20260925, 80);
    let json = serde_json::to_string(&old).unwrap();
    assert!(!json.contains("event_time_units_per_second"));
    let decoded: model::Message = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, old);
    assert_eq!(decoded.event_time_units_per_second, 1);
    let nano = Schedule::new(config())
        .unwrap()
        .event(0, 0)
        .unwrap()
        .message;
    let json = serde_json::to_string(&nano).unwrap();
    assert!(json.contains("\"event_time_units_per_second\":1000000000"));
    assert_eq!(serde_json::from_str::<model::Message>(&json).unwrap(), nano);
}

#[test]
fn oracle_rejects_incomplete_calibrated_maintenance_observation() {
    let schedule = Schedule::new(config()).unwrap();
    let initial = schedule.initial_records().unwrap();
    let mut rows = as_map(&initial);
    let selected = [
        schedule.event(0, 0).unwrap(),
        schedule.event(4, 0).unwrap(),
        schedule.event(5, 0).unwrap(),
    ];
    let mut receipts = Vec::new();
    for (index, event) in selected.into_iter().enumerate() {
        let body = model::serial_apply(&mut rows, &event.message).unwrap();
        receipts.push(oracle::Receipt {
            message: event.message,
            started: (2 * index + 1) as u64,
            finished: (2 * index + 2) as u64,
            body,
        });
    }
    let final_rows: Vec<_> = rows.into_values().collect();
    assert_eq!(
        oracle::check(&initial, &receipts, &final_rows, 100).status,
        oracle::Status::Valid
    );
    let observed = receipts
        .iter_mut()
        .flat_map(|receipt| &mut receipt.body.operations)
        .find_map(|op| match op {
            Operation::Query {
                query: storage::Query::GlobalDue { .. },
                rows,
            } => Some(rows),
            _ => None,
        })
        .unwrap();
    assert!(observed.len() > 4);
    observed.pop();
    assert_eq!(
        oracle::check(&initial, &receipts, &final_rows, 100).status,
        oracle::Status::Invalid
    );
}

#[test]
fn seconds_and_nanosecond_execution_have_identical_physical_effects() {
    // The same old foreground/rename/projection/housekeeping program executes
    // in both clocks. Only event/deadline fields change units; matching's
    // scheduled-flight identity remains seconds in both executions.
    let scale = NANOS_PER_SECOND as i64;
    let initial = model::sustained_initial(2, 20260925);
    let mut seconds = as_map(&initial);
    let mut nanos: BTreeMap<_, _> = initial
        .into_iter()
        .map(|mut row| {
            row.event_time *= scale;
            row.due *= scale;
            (row.id, row)
        })
        .collect();
    for sequence in 0..32 {
        let message = model::sustained_message(sequence, 0, 1, 2, 20260925, 0);
        let mut scaled = message.clone();
        scaled.event_time *= scale;
        scaled.event_time_units_per_second = scale;
        match &mut scaled.kind {
            MessageKind::Project { at } => *at *= scale,
            MessageKind::Housekeeping { before } => *before *= scale,
            MessageKind::Position { .. } | MessageKind::Plan | MessageKind::Rename { .. } => {}
            other => panic!("unexpected legacy corpus kind {other:?}"),
        }
        let expected = model::serial_apply(&mut seconds, &message).unwrap().outcome;
        let mut actual = model::serial_apply(&mut nanos, &scaled).unwrap().outcome;
        for output in &mut actual.outputs {
            output.event_time /= scale;
        }
        assert_eq!(actual, expected, "outcome diverged at input {sequence}");
        let normalized: BTreeMap<_, _> = nanos
            .values()
            .map(|row| {
                let mut row = *row;
                row.event_time /= scale;
                row.due /= scale;
                (row.id, row)
            })
            .collect();
        assert_eq!(
            normalized, seconds,
            "physical state diverged at input {sequence}"
        );
    }
}

fn affinity_config() -> Config {
    Config {
        duration_ns: 2 * NANOS_PER_SECOND,
        foreground_rate: 6,
        foreground_workers: 2,
        families: 4,
        dispatch: calibrated::Dispatch::SignatureAffinity,
        affinity_ttl_ms: 500,
        ..config()
    }
}
fn foreground_owners(schedule: &Schedule) -> Vec<usize> {
    let mut owners = vec![usize::MAX; schedule.foreground_offered() as usize];
    for worker in 0..schedule.config.foreground_workers {
        for ordinal in 0..schedule.worker_offered(worker) {
            let event = schedule.event(worker, ordinal).unwrap();
            let q = (event.message.id - 1_000_000) as usize;
            assert_eq!(owners[q], usize::MAX, "input assigned twice");
            owners[q] = worker;
        }
    }
    assert!(!owners.contains(&usize::MAX), "unassigned offered input");
    owners
}

#[test]
fn default_dispatch_preserves_old_config_json_and_input_fields() {
    let cfg = config();
    let expected = r#"{"duration_ns":8000000000,"foreground_rate":24,"foreground_workers":4,"families":16,"seed":20260925,"projection_interval_seconds":1,"housekeeping_interval_seconds":2}"#;
    assert_eq!(serde_json::to_string(&cfg).unwrap(), expected);
    assert_eq!(serde_json::from_str::<Config>(expected).unwrap(), cfg);
    let input = Schedule::new(cfg).unwrap().event(0, 0).unwrap().message;
    assert_eq!(
        (
            input.id,
            input.callsign,
            input.tail,
            input.origin,
            input.destination
        ),
        (1_000_000, 100, 10_000, 20, 1000)
    );
    assert_eq!(input.kind, MessageKind::Plan);
    assert_eq!(input.event_time, EVENT_EPOCH_NS);
    assert_eq!(input.source, 1);
    assert_eq!(input.scheduled, 1_700_001_000);
}

#[test]
fn ttl_equality_expires_and_round_robin_reassigns_the_same_signature() {
    let plan = Schedule::new(affinity_config()).unwrap();
    // Three signatures return exactly500ms later. At equality every entry
    // expires; an odd number of misses over two workers changes each owner.
    assert_eq!(
        foreground_owners(&plan),
        [0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
    );
    let report = plan.dispatch_report();
    assert_eq!(report["hits"], 0);
    assert_eq!(report["misses"], 12);
    assert_eq!(report["new_signature_misses"], 3);
    assert_eq!(report["expired_misses"], 9);
    assert_eq!(report["expired_owner_changes"], 9);
    assert_eq!(report["planned_flight_worker_changes"], 9);
    assert_eq!(report["flights_with_multiple_workers"], 3);
    assert_eq!(report["worker_counts"], serde_json::json!([6, 6]));
    assert_eq!(report["assignment_fingerprint"], "a9e22ff6968d40e5");
    assert_eq!(
        report["assignment_fingerprint_format"],
        "fnv1a64-q-u64le-owner-u64le"
    );
}

#[test]
fn ttl_hits_refresh_deadlines_beyond_the_original_insertion_time() {
    let plan = Schedule::new(Config {
        affinity_ttl_ms: 501,
        ..affinity_config()
    })
    .unwrap();
    assert_eq!(
        foreground_owners(&plan),
        [0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0]
    );
    let report = plan.dispatch_report();
    assert_eq!(report["hits"], 9);
    assert_eq!(report["misses"], 3);
    assert_eq!(report["expired_misses"], 0);
    assert_eq!(report["planned_flight_worker_changes"], 0);
    // The last arrival is more than one original TTL after insertion. Keeping
    // the same owner requires the earlier hits to refresh the deadline.
    assert_eq!(plan.event(0, 6).unwrap().message.id, 1_000_009);
}

#[test]
fn mixed_signatures_share_callsign_routes_and_hits_do_not_advance_round_robin() {
    let plan = Schedule::new(Config {
        duration_ns: 3 * NANOS_PER_SECOND,
        affinity_ttl_ms: 5000,
        signature_pattern: calibrated::SignaturePattern::Mixed,
        ..affinity_config()
    })
    .unwrap();
    // Distinct logical flights share the visible callsign-only signature.
    // Its first miss follows three new both-signature misses, so owner1.
    // Intervening hits must not advance the round-robin cursor.
    assert_eq!(
        foreground_owners(&plan),
        [0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 1, 0]
    );
    let report = plan.dispatch_report();
    assert_eq!(report["unique_signatures"], 7);
    assert_eq!(report["hits"], 11);
    assert_eq!(report["misses"], 7);
    assert_eq!(report["expired_misses"], 0);
    assert_eq!(report["expired_owner_changes"], 0);
    assert_eq!(report["planned_flight_worker_changes"], 4);
    assert_eq!(report["flights_with_multiple_workers"], 2);
    assert_eq!(report["worker_counts"], serde_json::json!([8, 10]));
    assert_eq!(report["assignment_fingerprint"], "5226764c10a68564");
    let messages = events(&plan);
    for ordinal in 0..6 {
        let event = messages
            .iter()
            .find(|e| e.logical_identity == Some(0) && e.foreground_ordinal == Some(ordinal))
            .unwrap();
        assert_eq!(
            (event.message.callsign, event.message.tail),
            match ordinal {
                0 | 1 => (100, 10_000),
                2 | 3 => (100, 0),
                _ => (0, 10_000),
            }
        );
    }
}

#[test]
fn expiry_does_not_falsely_imply_worker_migration() {
    let plan = Schedule::new(Config {
        duration_ns: 2 * NANOS_PER_SECOND,
        foreground_rate: 24,
        foreground_workers: 4,
        families: 16,
        affinity_ttl_ms: 1,
        ..affinity_config()
    })
    .unwrap();
    let report = plan.dispatch_report();
    assert_eq!(report["expired_misses"], 36);
    assert_eq!(report["expired_owner_changes"], 0);
    assert_eq!(report["planned_flight_worker_changes"], 0);
    assert_eq!(report["flights_with_multiple_workers"], 0);
}

#[test]
fn aliases_change_input_evidence_but_corpus_is_independent_of_dispatch_and_workers() {
    let baseline = Schedule::new(Config {
        signature_pattern: calibrated::SignaturePattern::Mixed,
        ..config()
    })
    .unwrap();
    let expected = events(&baseline);
    for workers in [1, 2, 4, 16, 32] {
        let affinity = Schedule::new(Config {
            foreground_workers: workers,
            dispatch: calibrated::Dispatch::SignatureAffinity,
            affinity_ttl_ms: 1200,
            signature_pattern: calibrated::SignaturePattern::Mixed,
            ..config()
        })
        .unwrap();
        assert_eq!(events(&affinity), expected);
        assert_eq!(
            affinity.initial_records().unwrap(),
            baseline.initial_records().unwrap()
        );
    }
}

#[test]
fn prepared_worker_plans_roundtrip_and_binary_search_backlog_matches_due_inputs() {
    let plan = Schedule::new(Config {
        signature_pattern: calibrated::SignaturePattern::Mixed,
        affinity_ttl_ms: 501,
        ..affinity_config()
    })
    .unwrap();
    for worker in 0..plan.worker_count() {
        let prepared = plan.worker_schedule(worker);
        prepared.validate().unwrap();
        let decoded: calibrated::WorkerSchedule =
            serde_json::from_slice(&serde_json::to_vec(&prepared).unwrap()).unwrap();
        assert_eq!(prepared, decoded);
        assert_eq!(prepared.offered(), plan.worker_offered(worker));
        for ordinal in 0..=prepared.offered() {
            assert_eq!(prepared.event(ordinal), plan.event(worker, ordinal));
        }
        for elapsed in [
            0,
            166_666_665,
            166_666_666,
            500_000_000,
            1_000_000_000,
            u64::MAX,
        ] {
            let due = (0..prepared.offered())
                .filter(|&n| prepared.event(n).unwrap().offset_ns <= elapsed)
                .count() as u64;
            for completed in [0, 1, 2, 100] {
                assert_eq!(
                    prepared.backlog(completed, elapsed),
                    due.saturating_sub(completed)
                );
                assert_eq!(
                    prepared.backlog(completed, elapsed),
                    plan.backlog(worker, completed, elapsed)
                );
            }
        }
    }
}

#[test]
fn malformed_prepared_worker_plans_are_rejected() {
    let plan = Schedule::new(affinity_config()).unwrap();
    let good = plan.worker_schedule(0);
    for indices in [vec![0, 0], vec![2, 0], vec![12], vec![u32::MAX]] {
        let mut bad = good.clone();
        bad.foreground_sequences = indices;
        assert!(bad.validate().is_err());
    }
    let mut bad = good.clone();
    bad.worker = plan.worker_count();
    assert!(bad.validate().is_err());
    let mut bad = plan.worker_schedule(2);
    bad.foreground_sequences = vec![0];
    assert!(
        bad.validate().is_err(),
        "timer worker cannot receive foreground indices"
    );
    let control = Schedule::new(config()).unwrap();
    let mut bad = control.worker_schedule(0);
    bad.foreground_sequences = vec![1];
    assert!(
        bad.validate().is_err(),
        "identity control uses only its analytical lane"
    );
    let mut bad = good;
    bad.config.affinity_ttl_ms = 0;
    assert!(bad.validate().is_err());
}

#[test]
fn dispatch_configuration_requires_explicit_bounded_ttl_only_for_affinity() {
    for invalid in [
        Config {
            affinity_ttl_ms: 1,
            ..config()
        },
        Config {
            affinity_ttl_ms: 0,
            ..affinity_config()
        },
        Config {
            affinity_ttl_ms: 3_600_001,
            ..affinity_config()
        },
        Config {
            affinity_ttl_ms: u64::MAX,
            ..affinity_config()
        },
    ] {
        assert!(Schedule::new(invalid).is_err());
    }
    let mut invalid = serde_json::to_value(config()).unwrap();
    invalid["dispatch"] = serde_json::json!("flight_hash");
    assert!(serde_json::from_value::<Config>(invalid).is_err());
    let plan = Schedule::new(Config {
        signature_pattern: calibrated::SignaturePattern::Mixed,
        ..config()
    })
    .unwrap();
    assert_eq!(plan.dispatch_report()["dispatch"], "identity");
    assert_eq!(plan.dispatch_report()["hits"], 0);
    assert_eq!(plan.dispatch_report()["misses"], 0);
}

#[test]
fn callsign_only_and_tail_only_queries_resolve_the_intended_family_and_forks() {
    let plan = Schedule::new(Config {
        duration_ns: 3 * NANOS_PER_SECOND,
        signature_pattern: calibrated::SignaturePattern::Mixed,
        ..affinity_config()
    })
    .unwrap();
    let mut rows = as_map(&plan.initial_records().unwrap());
    let mut aliases = BTreeSet::new();
    for event in events(&plan)
        .into_iter()
        .filter(|e| e.class == EventClass::Foreground)
    {
        let body = model::serial_apply(&mut rows, &event.message).unwrap();
        assert_eq!(
            body.outcome.family,
            Some((event.logical_identity.unwrap() * 2) as i64)
        );
        assert_eq!(body.outcome.updated_views, 4);
        assert_eq!(body.outcome.ignored_stale, 0);
        assert!(!body.outcome.missing_family);
        assert!(!body.outcome.allocation_deferred);
        aliases.insert((event.message.callsign == 0, event.message.tail == 0));
        let writes: BTreeSet<_> = body
            .operations
            .iter()
            .filter_map(|op| match op {
                Operation::Write { row } => Some(row.id),
                _ => None,
            })
            .collect();
        assert_eq!(
            writes.len(),
            if matches!(event.message.kind, MessageKind::Plan) {
                13
            } else {
                17
            }
        );
    }
    assert_eq!(
        aliases,
        [(false, false), (false, true), (true, false)].into()
    );
    model::validate_snapshot(&rows.into_values().collect::<Vec<_>>()).unwrap();
}

#[test]
fn an_expired_assignment_can_finish_after_a_newer_one_with_valid_partial_staleness() {
    let plan = Schedule::new(affinity_config()).unwrap();
    let initial = plan.initial_records().unwrap();
    let all = events(&plan);
    let old = all
        .iter()
        .find(|e| e.message.id == 1_000_000)
        .unwrap()
        .message
        .clone();
    let newer = all
        .iter()
        .find(|e| e.message.id == 1_000_003)
        .unwrap()
        .message
        .clone();
    assert_eq!((old.callsign, old.tail), (newer.callsign, newer.tail));
    assert_ne!(foreground_owners(&plan)[0], foreground_owners(&plan)[3]);
    let mut rows = as_map(&initial);
    let newer_body = model::serial_apply(&mut rows, &newer).unwrap();
    let old_body = model::serial_apply(&mut rows, &old).unwrap();
    assert_eq!(newer_body.outcome.updated_views, 4);
    assert_eq!(old_body.outcome.updated_views, 2);
    assert_eq!(old_body.outcome.ignored_stale, 2);
    let mut history = vec![
        oracle::Receipt {
            message: old,
            started: 4,
            finished: 5,
            body: old_body,
        },
        oracle::Receipt {
            message: newer,
            started: 2,
            finished: 3,
            body: newer_body,
        },
    ];
    let final_rows: Vec<_> = rows.into_values().collect();
    assert_eq!(
        oracle::check(&initial, &history, &final_rows, 100).status,
        oracle::Status::Valid
    );
    history[0].body.outcome.ignored_stale = 0;
    assert_eq!(
        oracle::check(&initial, &history, &final_rows, 100).status,
        oracle::Status::Invalid
    );
}

#[test]
fn worker_plans_store_each_affinity_input_once_and_do_not_depend_on_consumption() {
    let plan = Schedule::new(Config {
        duration_ns: 10 * NANOS_PER_SECOND,
        foreground_rate: 1000,
        foreground_workers: 8,
        families: 64,
        affinity_ttl_ms: 60,
        signature_pattern: calibrated::SignaturePattern::Mixed,
        ..affinity_config()
    })
    .unwrap();
    let prepared: Vec<_> = (0..plan.worker_count())
        .map(|worker| plan.worker_schedule(worker))
        .collect();
    assert_eq!(
        prepared
            .iter()
            .map(|p| p.foreground_sequences.len())
            .sum::<usize>(),
        10_000
    );
    let indices: BTreeSet<_> = prepared
        .iter()
        .flat_map(|p| p.foreground_sequences.iter().copied())
        .collect();
    assert_eq!(indices, (0..10_000).collect());
    for worker in prepared {
        worker.validate().unwrap();
        let before = serde_json::to_value(&worker).unwrap();
        for n in (0..worker.offered()).rev() {
            let _ = worker.event(n);
        }
        let _ = worker.backlog(0, u64::MAX);
        let _ = worker.backlog(worker.offered(), 0);
        assert_eq!(serde_json::to_value(&worker).unwrap(), before);
    }
    let control = Schedule::new(config()).unwrap();
    assert!((0..control.worker_count())
        .all(|w| control.worker_schedule(w).foreground_sequences.is_empty()));
}
