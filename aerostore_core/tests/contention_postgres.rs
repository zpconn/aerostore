//! PostgreSQL adapter contract probes. Live tests require a disposable local
//! PostgreSQL URL in AEROSTORE_CONTENTION_PG_URL and explicit --ignored.
#![allow(dead_code)]

#[path = "../benches/extended_crucible/metrics.rs"]
pub mod existing_metrics;
#[path = "../benches/extended_crucible/model.rs"]
pub mod existing_model;
mod extended_crucible {
    pub use super::existing_metrics as metrics;
    pub use super::existing_model as model;
}
#[path = "../benches/contention_crucible/postgres.rs"]
mod adapter;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;

use adapter::{Adapter, CandidateQuery, WriteMode};
use existing_model::{DbError, Record, FLIGHT, POSITION, SCHEDULED};
use storage::{MaintenanceSelection, Query, Store};

fn monotonic_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) },
        0
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

#[test]
fn statistics_policy_preserves_initial_only_default_and_native_noop_identity() {
    let initial = adapter::statistics_metadata(0, true);
    assert_eq!(initial["effective_policy"], "initial_only");
    assert_eq!(initial["initial_analyze_executed"], true);
    assert!(initial["runtime_analyze"].is_null());
    let native = adapter::statistics_metadata(5, false);
    assert_eq!(native["effective_policy"], "not_applicable");
    assert_eq!(native["requested_after_seconds"], 5);
    assert_eq!(native["initial_analyze_executed"], false);
    assert!(native["runtime_analyze"].is_null());
}

#[test]
fn candidate_query_defaults_and_effective_metadata_preserve_treatment_identity() {
    assert_eq!(CandidateQuery::default(), CandidateQuery::Or);
    for query in [CandidateQuery::Or, CandidateQuery::Split] {
        let postgres = adapter::candidate_query_metadata(query, true);
        let native = adapter::candidate_query_metadata(query, false);
        assert_eq!(postgres["format"], "postgres-candidate-query-v1");
        assert_eq!(postgres["requested"], serde_json::json!(query));
        assert_eq!(postgres["effective"], serde_json::json!(query));
        assert_eq!(native["requested"], serde_json::json!(query));
        assert_eq!(native["effective"], "not_applicable");
    }
    assert!(serde_json::from_str::<CandidateQuery>("\"other\"").is_err());
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_records_owned_identity_and_preserves_business_rows() {
    let seed = [Record {
        id: 0,
        active: true,
        kind: FLIGHT,
        ..Record::default()
    }];
    with_records("timed_statistics", &seed, |url, schema| {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("statistics.json");
        let mut control = adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
        let start = monotonic_ns();
        control.schedule(start, start + 5_000_000_000).unwrap();
        assert!(control
            .schedule(start, start + 5_000_000_000)
            .unwrap_err()
            .contains("already scheduled"));
        // The dedicated statistics connection neither owns nor changes this
        // normal SERIALIZABLE prepared statement transaction.
        let mut writer = Adapter::connect_with_mode(url, schema, WriteMode::Buffered).unwrap();
        writer.begin(&[]).unwrap();
        let mut row = writer.read(0).unwrap();
        row.revision = 7;
        writer.write(row).unwrap();
        writer.commit().unwrap();
        let report = control.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(path).unwrap()).unwrap(),
            report
        );
        let action = &report["runtime_analyze"];
        assert_eq!(action["status"], "succeeded");
        assert_eq!(action["command_succeeded"], true);
        assert!(action["error"].is_null());
        assert_eq!(action["scheduled_ns"], start + 1_000_000_000);
        assert!(action["dispatched_ns"].as_u64().unwrap() >= start + 1_000_000_000);
        assert!(action["dispatched_ns"].as_u64().unwrap() <= start + 2_000_000_000);
        assert!(action["finished_ns"].as_u64().unwrap() < start + 5_000_000_000);
        for field in ["schema_oid", "relation_oid", "backend_pid"] {
            assert!(action[field].as_u64().unwrap() > 0);
        }
        for when in ["before", "after"] {
            for field in ["analyze_count", "autoanalyze_count", "n_mod_since_analyze"] {
                assert!(action[when][field].as_i64().unwrap() >= 0);
            }
            assert_eq!(action[when]["counters_may_lag"], true);
        }
        assert_eq!(adapter::snapshot(url, schema).unwrap(), vec![row]);
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_rejects_missed_dispatch_and_retains_failure_receipt() {
    with_records("statistics_late", &[], |url, schema| {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("statistics.json");
        let mut control = adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
        let now = monotonic_ns();
        control
            .schedule(now - 3_000_000_000, now + 3_000_000_000)
            .unwrap();
        assert!(control.finish().unwrap_err().contains("dispatch window"));
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(report["runtime_analyze"]["status"], "failed");
        assert_eq!(report["runtime_analyze"]["command_succeeded"], false);
        assert!(report["runtime_analyze"]["finished_ns"].is_null());
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_cancellation_joins_without_waiting_for_timer() {
    with_records("statistics_cancel", &[], |url, schema| {
        let directory = tempfile::tempdir().unwrap();
        for scheduled in [false, true] {
            let path = directory
                .path()
                .join(format!("statistics-{scheduled}.json"));
            let mut control = adapter::StatisticsControl::prepare(url, schema, 30, &path).unwrap();
            let start = monotonic_ns();
            if scheduled {
                control.schedule(start, start + 60_000_000_000).unwrap();
            }
            control.cancel().unwrap();
            assert!(monotonic_ns() - start < 5_000_000_000);
            let report: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(report["runtime_analyze"]["status"], "cancelled");
            assert_eq!(report["runtime_analyze"]["command_succeeded"], false);
            assert!(report["runtime_analyze"]["dispatched_ns"].is_null());
        }
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_rechecks_ownership_at_dispatch() {
    with_records("statistics_marker", &[], |url, schema| {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("statistics.json");
        let mut control = adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
        let mut client = postgres::Client::connect(url, postgres::NoTls).unwrap();
        let marker: String = client
            .query_one(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
                &[&schema],
            )
            .unwrap()
            .get(0);
        client
            .batch_execute(&format!("COMMENT ON SCHEMA {schema} IS NULL"))
            .unwrap();
        let now = monotonic_ns();
        control.schedule(now, now + 5_000_000_000).unwrap();
        let outcome = control.finish();
        // Restore our own marker for the fixture's normal guarded cleanup.
        client
            .batch_execute(&format!("COMMENT ON SCHEMA {schema} IS '{marker}'"))
            .unwrap();
        assert!(outcome.unwrap_err().contains("ownership marker"));
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(report["runtime_analyze"]["status"], "failed");
        assert_eq!(report["runtime_analyze"]["command_succeeded"], false);
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_rejects_replaced_relation_and_terminated_backend() {
    for terminate in [false, true] {
        with_records(
            if terminate {
                "statistics_terminated"
            } else {
                "statistics_replaced"
            },
            &[],
            |url, schema| {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("statistics.json");
                let mut control =
                    adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
                let prepared: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                let mut client = postgres::Client::connect(url, postgres::NoTls).unwrap();
                if terminate {
                    let pid = prepared["runtime_analyze"]["backend_pid"].as_i64().unwrap() as i32;
                    assert!(client
                        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                        .unwrap()
                        .get::<_, bool>(0));
                } else {
                    client.batch_execute(&format!(
                        "ALTER TABLE {schema}.records RENAME TO replaced_records; \
                         CREATE TABLE {schema}.records (LIKE {schema}.replaced_records INCLUDING ALL)"
                    )).unwrap();
                }
                let start = monotonic_ns();
                control.schedule(start, start + 5_000_000_000).unwrap();
                let error = control.finish().unwrap_err();
                if !terminate {
                    assert!(error.contains("identity changed"), "{error}");
                }
                let failed: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                assert_eq!(failed["runtime_analyze"]["status"], "failed");
                assert_eq!(failed["runtime_analyze"]["command_succeeded"], false);
                assert_eq!(
                    failed["runtime_analyze"]["relation_oid"],
                    prepared["runtime_analyze"]["relation_oid"]
                );
            },
        );
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn timed_statistics_requires_scheduled_action_strictly_inside_admission() {
    with_records("statistics_invalid_schedule", &[], |url, schema| {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("statistics.json");
        let control = adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
        assert!(control.finish().unwrap_err().contains("never scheduled"));
        let mut control = adapter::StatisticsControl::prepare(url, schema, 1, &path).unwrap();
        let start = monotonic_ns();
        control.schedule(start, start + 1_000_000_000).unwrap();
        assert!(control.finish().unwrap_err().contains("within admission"));
        let failed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(failed["runtime_analyze"]["status"], "failed");
        assert_eq!(failed["runtime_analyze"]["command_succeeded"], false);
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn serializable_queries_discover_writes_without_predeclaring_them() {
    for query in [CandidateQuery::Or, CandidateQuery::Split] {
        empty_predicate_race(WriteMode::Immediate, query, false);
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn buffered_empty_predicates_reject_competing_creation() {
    for query in [CandidateQuery::Or, CandidateQuery::Split] {
        empty_predicate_race(WriteMode::Buffered, query, false);
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn tail_only_empty_predicates_reject_competing_creation_for_both_sql_forms() {
    for mode in [WriteMode::Immediate, WriteMode::Buffered] {
        for query in [CandidateQuery::Or, CandidateQuery::Split] {
            empty_predicate_race(mode, query, true);
        }
    }
}

fn empty_predicate_race(mode: WriteMode, query: CandidateQuery, tail_only: bool) {
    let url = std::env::var("AEROSTORE_CONTENTION_PG_URL").expect("disposable PostgreSQL URL");
    let schema = format!(
        "contention_contract_{mode:?}_{query:?}_{tail_only}_{}",
        std::process::id()
    );
    let records = vec![
        Record {
            id: 0,
            ..Record::default()
        },
        Record {
            id: 1,
            ..Record::default()
        },
    ];
    adapter::initialize(&url, &schema, &records).unwrap();
    let result = std::panic::catch_unwind(|| {
        let mut a =
            Adapter::connect_with_candidate_query(&url, &schema, false, mode, query).unwrap();
        let mut b =
            Adapter::connect_with_candidate_query(&url, &schema, false, mode, query).unwrap();
        assert!(matches!(a.begin(&[0]), Err(DbError::Fatal(_))));
        a.begin(&[]).unwrap();
        b.begin(&[]).unwrap();
        let predicate = Query::Candidates {
            callsign: if tail_only { 99 } else { 42 },
            tail: if tail_only { -7 } else { 0 },
            scheduled: 0,
            window: 1,
        };
        assert!(a.query(&predicate).unwrap().is_empty());
        assert!(b.query(&predicate).unwrap().is_empty());
        let create = |id| Record {
            id,
            active: true,
            kind: FLIGHT,
            callsign: 42,
            tail: if tail_only { -7 } else { 0 },
            ..Record::default()
        };
        a.write(create(0)).unwrap();
        let b_write = b.write(create(1));
        let a_commit = a.commit();
        let b_commit = b_write.and_then(|()| b.commit());
        let commits = usize::from(a_commit.is_ok()) + usize::from(b_commit.is_ok());
        assert_eq!(
            commits, 1,
            "two overlapping empty searches must not both create"
        );
        for rejected in [a_commit, b_commit].into_iter().filter_map(Result::err) {
            assert_eq!(rejected, DbError::Conflict);
        }
        let retry_count: u64 = a.retry_causes.values().chain(b.retry_causes.values()).sum();
        assert_eq!(retry_count, 1);
        assert!(a
            .retry_causes
            .keys()
            .chain(b.retry_causes.keys())
            .all(|key| key.ends_with(":40001")));
        assert_eq!(
            adapter::snapshot(&url, &schema)
                .unwrap()
                .into_iter()
                .filter(|row| predicate.matches(row))
                .count(),
            1
        );
        let storage = adapter::retention(&url, &schema).unwrap();
        assert!(storage["total_bytes"].as_i64().unwrap() > 0);
        assert_eq!(storage["resident_memory_measured"], false);
        assert_eq!(
            adapter::configuration(&url, false).unwrap()["synchronous_commit"],
            "off"
        );
        assert_eq!(
            adapter::configuration(&url, true).unwrap()["synchronous_commit"],
            "on"
        );
    });
    adapter::cleanup(&url, &schema).unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn scratch_schema_cleanup_refuses_unowned_schemas() {
    let url = std::env::var("AEROSTORE_CONTENTION_PG_URL").expect("disposable PostgreSQL URL");
    let schema = format!("contention_unowned_{}", std::process::id());
    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let result = adapter::cleanup(&url, &schema);
    let drain = adapter::drain(&url, &schema);
    // This test created the unmarked schema itself; clean it through that same
    // known ownership rather than bypassing the benchmark cleanup check.
    client
        .batch_execute(&format!("DROP SCHEMA {schema}"))
        .unwrap();
    assert!(result.unwrap_err().contains("ownership marker"));
    assert!(drain.unwrap_err().contains("ownership marker"));
}

fn with_records(
    label: &str,
    records: &[Record],
    test: impl FnOnce(&str, &str) + std::panic::UnwindSafe,
) {
    with_records_and_selection(label, records, MaintenanceSelection::Complete, test);
}

fn with_records_and_selection(
    label: &str,
    records: &[Record],
    selection: MaintenanceSelection,
    test: impl FnOnce(&str, &str) + std::panic::UnwindSafe,
) {
    let url = std::env::var("AEROSTORE_CONTENTION_PG_URL").expect("disposable PostgreSQL URL");
    let schema = format!("contention_{}_{}", label, std::process::id());
    adapter::initialize_with_maintenance_selection(&url, &schema, records, selection).unwrap();
    let result = std::panic::catch_unwind(|| test(&url, &schema));
    adapter::cleanup(&url, &schema).unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn buffered_overlay_merges_predicate_entries_exits_and_restores_savepoints() {
    for query in [CandidateQuery::Or, CandidateQuery::Split] {
        buffered_overlay_contract(query);
    }
}

fn buffered_overlay_contract(query: CandidateQuery) {
    let seed = vec![
        // Distinct nonzero payload columns also detect a transposed batch array
        // while the transaction changes only the callsign.
        Record {
            id: 0,
            active: true,
            kind: FLIGHT,
            family: 1,
            pedigree: 3,
            callsign: 42,
            tail: 7,
            origin: 11,
            destination: 13,
            scheduled: 0,
            event_time: 17,
            due: 19,
            latitude: 23,
            longitude: 29,
            altitude: 31,
            ground_speed: 37,
            status: 41,
            revision: 43,
            source: 2,
            parent: 47,
            sequence: 53,
        },
        Record {
            id: 1,
            active: true,
            kind: SCHEDULED,
            family: 1,
            due: 5,
            ..Record::default()
        },
        Record {
            id: 2,
            active: true,
            kind: POSITION,
            family: 1,
            pedigree: 3,
            event_time: 7,
            ..Record::default()
        },
        Record {
            id: 3,
            ..Record::default()
        },
    ];
    with_records("overlay", &seed, |url, schema| {
        let mut db =
            Adapter::connect_with_candidate_query(url, schema, false, WriteMode::Buffered, query)
                .unwrap();
        db.begin(&[]).unwrap();
        // SQL query populates the snapshot cache; changing a row must supersede
        // that cache for point reads and for predicates it enters or leaves.
        assert_eq!(db.query(&Query::All).unwrap().len(), 3);
        let mut flight = db.read(0).unwrap();
        flight.callsign = 888;
        db.write(flight).unwrap();
        assert_eq!(db.read(0).unwrap(), flight);
        let old = Query::Candidates {
            callsign: 42,
            tail: 0,
            scheduled: 0,
            window: 1,
        };
        let new = Query::Candidates {
            callsign: 888,
            tail: 0,
            scheduled: 0,
            window: 1,
        };
        assert!(db.query(&old).unwrap().is_empty());
        assert_eq!(db.query(&new).unwrap(), vec![flight]);
        let first = db.savepoint().unwrap();
        let mut scheduled = db.read(1).unwrap();
        scheduled.family = 2;
        scheduled.due = 30;
        db.write(scheduled).unwrap();
        let mut position = db.read(2).unwrap();
        position.active = false;
        db.write(position).unwrap();
        let entering = Record {
            id: 3,
            active: true,
            kind: SCHEDULED,
            family: 2,
            due: 2,
            ..Record::default()
        };
        db.write(entering).unwrap();
        assert!(db
            .query(&Query::Due { family: 1, at: 10 })
            .unwrap()
            .is_empty());
        assert_eq!(
            db.query(&Query::Due { family: 2, at: 50 }).unwrap(),
            vec![scheduled, entering]
        );
        assert_eq!(
            db.query(&Query::GlobalDue { at: 10 }).unwrap(),
            vec![entering]
        );
        assert_eq!(
            db.query(&Query::GlobalDue { at: 50 }).unwrap(),
            vec![scheduled, entering]
        );
        assert!(db
            .query(&Query::GlobalExpired { before: 8 })
            .unwrap()
            .is_empty());
        assert!(db
            .query(&Query::Positions {
                family: 1,
                pedigree: 3
            })
            .unwrap()
            .is_empty());
        let second = db.savepoint().unwrap();
        let mut cancelled = flight;
        cancelled.active = false;
        db.write(cancelled).unwrap();
        assert!(db.query(&new).unwrap().is_empty());
        db.rollback_to(second).unwrap();
        assert_eq!(db.query(&new).unwrap(), vec![flight]);
        // Nothing buffered is visible outside the transaction.
        assert_eq!(adapter::snapshot(url, schema).unwrap(), seed);
        db.rollback_to(first).unwrap();
        assert_eq!(db.read(1).unwrap(), seed[1]);
        assert_eq!(db.read(2).unwrap(), seed[2]);
        assert_eq!(db.read(3).unwrap(), seed[3]);
        assert_eq!(
            db.query(&Query::GlobalExpired { before: 8 }).unwrap(),
            vec![seed[2]]
        );
        db.commit().unwrap();
        let mut expected = seed.clone();
        expected[0] = flight;
        assert_eq!(adapter::snapshot(url, schema).unwrap(), expected);
        assert_eq!(db.sql_metrics.ordered_lock_statements, 1);
        assert_eq!(db.sql_metrics.batch_write_statements, 1);
        assert_eq!(db.sql_metrics.batch_written_rows, 1);
        assert_eq!(db.sql_metrics.point_read_statements, 1); // inactive slot 3
        assert!(db.sql_metrics.read_cache_hits > 0);
        assert_eq!(db.sql_metrics.predicate_statements, db.metrics.queries);
        // A later transaction cannot reuse the prior snapshot cache/overlay.
        db.begin(&[]).unwrap();
        assert_eq!(db.read(0).unwrap(), flight);
        let mut aborted = flight;
        aborted.callsign = 99;
        db.write(aborted).unwrap();
        db.abort().unwrap();
        db.begin(&[]).unwrap();
        assert_eq!(db.read(0).unwrap(), flight);
        db.abort().unwrap();
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn predicate_extremes_and_global_searches_match_both_modes() {
    let seed: Vec<_> = [i64::MIN, -1, 0, 1, i64::MAX]
        .into_iter()
        .enumerate()
        .map(|(id, scheduled)| Record {
            id,
            active: true,
            kind: FLIGHT,
            family: id as i64,
            callsign: 42,
            tail: 7,
            scheduled,
            ..Record::default()
        })
        .chain([
            Record {
                id: 5,
                active: true,
                kind: SCHEDULED,
                family: 17,
                due: 8,
                ..Record::default()
            },
            Record {
                id: 6,
                active: true,
                kind: SCHEDULED,
                family: 23,
                due: 9,
                ..Record::default()
            },
            Record {
                id: 7,
                active: true,
                kind: POSITION,
                family: 17,
                event_time: 8,
                ..Record::default()
            },
            Record {
                id: 8,
                active: true,
                kind: existing_model::DEDUP,
                family: 23,
                event_time: 9,
                ..Record::default()
            },
        ])
        .collect();
    with_records("extremes", &seed, |url, schema| {
        for (mode, query) in [WriteMode::Immediate, WriteMode::Buffered]
            .into_iter()
            .flat_map(|mode| [CandidateQuery::Or, CandidateQuery::Split].map(|query| (mode, query)))
        {
            let mut db =
                Adapter::connect_with_candidate_query(url, schema, false, mode, query).unwrap();
            db.begin(&[]).unwrap();
            for scheduled in [i64::MIN, -1, 0, 1, i64::MAX] {
                for window in [0, 1, i64::MAX, -1, i64::MIN] {
                    for (callsign, tail) in [(42, 0), (42, 7), (99, 7), (99, 0)] {
                        let q = Query::Candidates {
                            callsign,
                            tail,
                            scheduled,
                            window,
                        };
                        let expected: Vec<_> =
                            seed.iter().copied().filter(|row| q.matches(row)).collect();
                        assert_eq!(db.query(&q).unwrap(), expected, "{mode:?} {query:?} {q:?}");
                    }
                }
            }
            for q in [
                Query::GlobalDue { at: 9 },
                Query::GlobalExpired { before: 10 },
                Query::Due { family: 17, at: 9 },
                Query::Expired {
                    family: 23,
                    before: 10,
                },
            ] {
                assert_eq!(
                    db.query(&q).unwrap(),
                    seed.iter()
                        .copied()
                        .filter(|row| q.matches(row))
                        .collect::<Vec<_>>()
                );
            }
            db.commit().unwrap();
        }
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn candidate_forms_preserve_identity_boundaries_uniqueness_and_global_order() {
    // Deliberately interleave tail-only, dual, and callsign-only matches by id;
    // each branch's own ordering is insufficient for the required global order.
    let seed: Vec<_> = [
        (true, FLIGHT, 99, -7, 0),
        (true, FLIGHT, 42, -7, 0),
        (true, FLIGHT, 42, 0, 0),
        (false, FLIGHT, 42, -7, 0),
        (true, POSITION, 42, -7, 0),
        (true, FLIGHT, 42, -7, 2),
        (true, FLIGHT, 0, 0, 0),
        (true, FLIGHT, -42, 7, -1),
        (true, FLIGHT, i64::MIN, i64::MAX, i64::MIN),
        (true, FLIGHT, i64::MAX, i64::MIN, i64::MAX),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (active, kind, callsign, tail, scheduled))| Record {
        id,
        active,
        kind,
        callsign,
        tail,
        scheduled,
        ..Record::default()
    })
    .collect();
    with_records("candidate_identity", &seed, |url, schema| {
        for mode in [WriteMode::Immediate, WriteMode::Buffered] {
            for form in [CandidateQuery::Or, CandidateQuery::Split] {
                let mut db =
                    Adapter::connect_with_candidate_query(url, schema, false, mode, form).unwrap();
                db.begin(&[]).unwrap();
                for (callsign, tail) in [
                    (42, -7),
                    (42, 0),
                    (0, 0),
                    (-42, 7),
                    (99, -7),
                    (99, 0),
                    (i64::MIN, i64::MAX),
                    (i64::MAX, i64::MIN),
                ] {
                    for (scheduled, window) in [
                        (0, 1),
                        (-1, 0),
                        (i64::MIN, 0),
                        (i64::MAX, 0),
                        (0, -1),
                        (i64::MAX, i64::MIN),
                    ] {
                        let query = Query::Candidates {
                            callsign,
                            tail,
                            scheduled,
                            window,
                        };
                        let expected: Vec<_> = seed
                            .iter()
                            .copied()
                            .filter(|row| query.matches(row))
                            .collect();
                        let actual = db.query(&query).unwrap();
                        assert_eq!(actual, expected, "{form:?} {mode:?} {query:?}");
                        assert!(actual.windows(2).all(|pair| pair[0].id < pair[1].id));
                    }
                }
                db.abort().unwrap();
            }
        }
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn candidate_branch_changes_and_nested_savepoints_preserve_visible_rows() {
    let seed: Vec<_> = [(99, -7), (42, -7), (42, 0)]
        .into_iter()
        .enumerate()
        .map(|(id, (callsign, tail))| Record {
            id,
            active: true,
            kind: FLIGHT,
            callsign,
            tail,
            ..Record::default()
        })
        .collect();
    with_records("candidate_savepoints", &seed, |url, schema| {
        let predicate = Query::Candidates {
            callsign: 42,
            tail: -7,
            scheduled: 0,
            window: 1,
        };
        for mode in [WriteMode::Immediate, WriteMode::Buffered] {
            for form in [CandidateQuery::Or, CandidateQuery::Split] {
                let mut db =
                    Adapter::connect_with_candidate_query(url, schema, false, mode, form).unwrap();
                db.begin(&[]).unwrap();
                assert_eq!(db.query(&predicate).unwrap(), seed);
                let outer = db.savepoint().unwrap();
                let mut expected = seed.clone();
                // Move a tail-only row into both branches, and a dual-match
                // row into tail-only. Both must remain visible exactly once.
                expected[0].callsign = 42;
                expected[1].callsign = 99;
                db.write(expected[0]).unwrap();
                db.write(expected[1]).unwrap();
                assert_eq!(db.query(&predicate).unwrap(), expected);
                let inner = db.savepoint().unwrap();
                let mut removed = expected[0];
                removed.callsign = 99;
                removed.tail = 0;
                db.write(removed).unwrap();
                let mut inactive = expected[2];
                inactive.active = false;
                db.write(inactive).unwrap();
                assert_eq!(db.query(&predicate).unwrap(), vec![expected[1]]);
                db.rollback_to(inner).unwrap();
                assert_eq!(db.query(&predicate).unwrap(), expected);
                db.rollback_to(outer).unwrap();
                assert_eq!(db.query(&predicate).unwrap(), seed);
                db.commit().unwrap();
                assert_eq!(adapter::snapshot(url, schema).unwrap(), seed);
            }
        }
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn buffered_opposite_write_orders_retry_without_deadlock_or_partial_commit() {
    let seed: Vec<_> = (0..2)
        .map(|id| Record {
            id,
            active: true,
            kind: FLIGHT,
            family: 1,
            ..Record::default()
        })
        .collect();
    with_records("ordered", &seed, |url, schema| {
        let mut a = Adapter::connect_with_mode(url, schema, WriteMode::Buffered).unwrap();
        let mut b = Adapter::connect_with_mode(url, schema, WriteMode::Buffered).unwrap();
        for db in [&mut a, &mut b] {
            db.begin(&[]).unwrap();
            assert_eq!(
                db.query(&Query::Family {
                    family: 1,
                    kind: FLIGHT
                })
                .unwrap(),
                seed
            );
        }
        for id in [0, 1] {
            let mut row = a.read(id).unwrap();
            row.revision += 1;
            a.write(row).unwrap();
        }
        for id in [1, 0] {
            let mut row = b.read(id).unwrap();
            row.revision += 1;
            b.write(row).unwrap();
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let join = |mut db: Adapter, barrier: std::sync::Arc<std::sync::Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                let result = db.commit();
                (result, db.retry_causes.clone())
            })
        };
        let left = join(a, barrier.clone());
        let right = join(b, barrier);
        let results = [left.join().unwrap(), right.join().unwrap()];
        assert_eq!(
            results.iter().filter(|(result, _)| result.is_ok()).count(),
            1
        );
        assert!(results
            .iter()
            .flat_map(|(_, causes)| causes.keys())
            .all(|key| key.ends_with(":40001")));
        assert_eq!(
            results
                .iter()
                .filter(|(r, _)| matches!(r, Err(DbError::Conflict)))
                .count(),
            1
        );
        let committed = adapter::snapshot(url, schema).unwrap();
        assert!(committed.iter().all(|row| row.revision == 1));
        let mut retry = Adapter::connect_with_mode(url, schema, WriteMode::Buffered).unwrap();
        retry.begin(&[]).unwrap();
        for mut row in retry
            .query(&Query::Family {
                family: 1,
                kind: FLIGHT,
            })
            .unwrap()
        {
            row.revision += 1;
            retry.write(row).unwrap();
        }
        retry.commit().unwrap();
        assert!(adapter::snapshot(url, schema)
            .unwrap()
            .iter()
            .all(|row| row.revision == 2));
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn invalid_buffered_write_aborts_every_prior_overlay_change() {
    let seed = vec![Record {
        id: 0,
        active: true,
        kind: FLIGHT,
        ..Record::default()
    }];
    with_records("invalid", &seed, |url, schema| {
        for mode in [WriteMode::Immediate, WriteMode::Buffered] {
            let mut db = Adapter::connect_with_mode(url, schema, mode).unwrap();
            db.begin(&[]).unwrap();
            let mut row = db.read(0).unwrap();
            row.revision = 99;
            db.write(row).unwrap();
            row.id = 999;
            assert!(matches!(db.write(row), Err(DbError::Fatal(_))));
            assert!(matches!(db.commit(), Err(DbError::Fatal(_))));
            assert_eq!(adapter::snapshot(url, schema).unwrap(), seed);
            db.begin(&[]).unwrap();
            assert_eq!(db.read(0).unwrap(), seed[0]);
            db.abort().unwrap();
        }
    });
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn diagnostics_report_actual_settings_plans_and_wal_drain() {
    with_records(
        "diagnostics",
        &[Record {
            id: 0,
            active: true,
            kind: FLIGHT,
            callsign: 42,
            ..Record::default()
        }],
        |url, schema| {
            let config = adapter::configuration(url, false).unwrap();
            assert_eq!(config["transaction_isolation"], "serializable");
            assert_eq!(config["fsync"], "on");
            assert!(config["deadlock_timeout"].is_string());
            let audit = adapter::query_plan_audit(url, schema).unwrap();
            assert_eq!(audit["plans"].as_object().unwrap().len(), 8);
            assert_eq!(audit["indexes"].as_array().unwrap().len(), 9);
            assert_eq!(audit["postgres_candidate_query"]["effective"], "or");
            assert_eq!(audit["prepared_generic_plans_measured"], false);
            let split =
                adapter::query_plan_audit_with_candidate_query(url, schema, CandidateQuery::Split)
                    .unwrap();
            assert_eq!(split["postgres_candidate_query"]["effective"], "split");
            assert_eq!(split["plans"].as_object().unwrap().len(), 8);
            assert!(split["plans"]["candidates"]["sql"]
                .as_str()
                .unwrap()
                .contains("UNION ALL"));
            assert_eq!(split["prepared_generic_plans_measured"], false);
            let retention = adapter::retention(url, schema).unwrap();
            assert!(retention["database_counters"]["deadlocks"].is_number());
            assert!(retention["cluster_wal_counters"]["wal_bytes"].is_number());
            let drain = adapter::drain(url, schema).unwrap();
            assert_eq!(drain["completed"], true);
            assert_eq!(
                drain["method"],
                "synchronous_commit_update_on_owned_drain_fence"
            );
            assert_eq!(drain["fence_rows_updated"], 1);
            let mut client = postgres::Client::connect(url, postgres::NoTls).unwrap();
            let epoch: i64 = client
                .query_one(
                    &format!("SELECT epoch FROM {schema}.drain_fence WHERE id=1"),
                    &[],
                )
                .unwrap()
                .get(0);
            assert_eq!(epoch, 1);
            client
                .execute(&format!("DELETE FROM {schema}.drain_fence"), &[])
                .unwrap();
            assert!(adapter::drain(url, schema)
                .unwrap_err()
                .contains("updated 0 rows"));
        },
    );
}

fn expected_prefix(rows: &[Record], query: &Query) -> Vec<Record> {
    let mut selected: Vec<_> = rows
        .iter()
        .copied()
        .filter(|row| {
            row.active
                && match query {
                    Query::FirstDue { at, .. } => row.kind == SCHEDULED && row.due <= *at,
                    Query::FirstExpired { before, .. } => {
                        matches!(
                            row.kind,
                            POSITION | existing_model::OUTBOX | existing_model::DEDUP
                        ) && row.event_time < *before
                    }
                    _ => panic!("prefix test helper requires a prefix query"),
                }
        })
        .collect();
    let limit = match query {
        Query::FirstDue { limit, .. } => {
            selected.sort_by_key(|row| (row.due, row.id));
            *limit
        }
        Query::FirstExpired { limit, .. } => {
            selected.sort_by_key(|row| (row.event_time, row.id));
            *limit
        }
        _ => unreachable!(),
    };
    selected.truncate(limit);
    selected
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn maintenance_prefix_indexes_and_audits_are_explicit_opt_in() {
    for selection in [MaintenanceSelection::Complete, MaintenanceSelection::Prefix] {
        with_records_and_selection("prefix_indexes", &[], selection, |url, schema| {
            let audit = adapter::query_plan_audit_with_options(
                url,
                schema,
                CandidateQuery::Split,
                selection,
            )
            .unwrap();
            let indexes = audit["indexes"].as_array().unwrap();
            let prefix = selection == MaintenanceSelection::Prefix;
            assert_eq!(indexes.len(), 9);
            for name in ["due_idx", "event_time_idx"] {
                assert_eq!(indexes.iter().any(|row| row["name"] == name), !prefix);
            }
            for (name, columns) in [
                ("due_prefix_idx", "(due, id)"),
                ("expiry_prefix_idx", "(event_time, id)"),
            ] {
                let entry = indexes.iter().find(|row| row["name"] == name);
                assert_eq!(entry.is_some(), prefix);
                if let Some(entry) = entry {
                    assert!(entry["definition"].as_str().unwrap().contains(columns));
                }
            }
            assert_eq!(audit["maintenance_selection"], serde_json::json!(selection));
            assert_eq!(
                audit["plans"].as_object().unwrap().len(),
                if prefix { 10 } else { 8 }
            );
            assert_eq!(audit["prepared_generic_plans_measured"], false);
            if prefix {
                assert!(audit["plans"]["first_due"]["sql"]
                    .as_str()
                    .unwrap()
                    .contains("ORDER BY due,id LIMIT 4"));
                assert!(audit["plans"]["first_expired"]["sql"]
                    .as_str()
                    .unwrap()
                    .contains("ORDER BY event_time,id LIMIT 32"));
            }
        });
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn maintenance_prefix_matches_independent_selection_for_ties_extrema_and_short_results() {
    let mut seed: Vec<_> = (0..96)
        .map(|id| Record {
            id,
            active: true,
            kind: if id < 24 {
                SCHEDULED
            } else {
                [POSITION, existing_model::OUTBOX, existing_model::DEDUP][id % 3]
            },
            due: match id {
                0 => 10,
                22 => i64::MIN,
                23 => i64::MAX,
                _ => 0,
            },
            event_time: match id {
                24 => 10,
                94 => i64::MIN,
                95 => i64::MAX,
                _ => 0,
            },
            ..Record::default()
        })
        .collect();
    seed.extend([
        Record {
            id: 96,
            active: true,
            kind: FLIGHT,
            due: i64::MIN,
            event_time: i64::MIN,
            ..Record::default()
        },
        Record {
            id: 97,
            active: false,
            kind: SCHEDULED,
            due: i64::MIN,
            ..Record::default()
        },
        Record {
            id: 98,
            active: false,
            kind: POSITION,
            event_time: i64::MIN,
            ..Record::default()
        },
    ]);
    with_records_and_selection(
        "prefix_boundaries",
        &seed,
        MaintenanceSelection::Prefix,
        |url, schema| {
            for mode in [WriteMode::Immediate, WriteMode::Buffered] {
                for candidates in [CandidateQuery::Or, CandidateQuery::Split] {
                    let mut db =
                        Adapter::connect_with_candidate_query(url, schema, false, mode, candidates)
                            .unwrap();
                    db.begin(&[]).unwrap();
                    for cutoff in [i64::MIN, -1, 0, 1, 10, i64::MAX] {
                        for query in [
                            Query::FirstDue {
                                at: cutoff,
                                limit: 1,
                            },
                            Query::FirstDue {
                                at: cutoff,
                                limit: 16,
                            },
                            Query::FirstExpired {
                                before: cutoff,
                                limit: 1,
                            },
                            Query::FirstExpired {
                                before: cutoff,
                                limit: 64,
                            },
                        ] {
                            assert_eq!(
                                db.query(&query).unwrap(),
                                expected_prefix(&seed, &query),
                                "{mode:?} {candidates:?} {query:?}"
                            );
                        }
                    }
                    // Complete predicates retain their complete ID-ordered contract.
                    for query in [
                        Query::GlobalDue { at: i64::MAX },
                        Query::GlobalExpired { before: i64::MAX },
                    ] {
                        let expected: Vec<_> = seed
                            .iter()
                            .copied()
                            .filter(|row| query.matches(row))
                            .collect();
                        assert_eq!(db.query(&query).unwrap(), expected);
                    }
                    db.abort().unwrap();
                }
            }
        },
    );
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn maintenance_prefix_rejects_zero_and_oversized_limits_without_committing_writes() {
    let seed = [Record {
        id: 0,
        active: true,
        kind: SCHEDULED,
        ..Record::default()
    }];
    with_records_and_selection(
        "prefix_limits",
        &seed,
        MaintenanceSelection::Prefix,
        |url, schema| {
            for mode in [WriteMode::Immediate, WriteMode::Buffered] {
                let mut db = Adapter::connect_with_mode(url, schema, mode).unwrap();
                for query in [
                    Query::FirstDue { at: 0, limit: 0 },
                    Query::FirstDue { at: 0, limit: 17 },
                    Query::FirstExpired {
                        before: 1,
                        limit: 0,
                    },
                    Query::FirstExpired {
                        before: 1,
                        limit: 65,
                    },
                    Query::FirstExpired {
                        before: 1,
                        limit: usize::MAX,
                    },
                ] {
                    db.begin(&[]).unwrap();
                    db.write(Record {
                        revision: 99,
                        ..seed[0]
                    })
                    .unwrap();
                    assert!(matches!(db.query(&query), Err(DbError::Fatal(_))));
                    assert!(matches!(db.commit(), Err(DbError::Fatal(_))));
                    assert_eq!(adapter::snapshot(url, schema).unwrap(), seed);
                }
            }
        },
    );
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn maintenance_prefix_refills_overlay_holes_and_restores_nested_savepoints() {
    for expired in [false, true] {
        let seed: Vec<_> = [5, 0, 1, 2, 3, 9, 0]
            .into_iter()
            .enumerate()
            .map(|(id, time)| Record {
                id,
                active: id != 6,
                kind: if expired { POSITION } else { SCHEDULED },
                due: time,
                event_time: time,
                ..Record::default()
            })
            .collect();
        let query = if expired {
            Query::FirstExpired {
                before: 11,
                limit: 2,
            }
        } else {
            Query::FirstDue { at: 10, limit: 2 }
        };
        with_records_and_selection(
            "prefix_overlay",
            &seed,
            MaintenanceSelection::Prefix,
            |url, schema| {
                for mode in [WriteMode::Immediate, WriteMode::Buffered] {
                    let mut db = Adapter::connect_with_mode(url, schema, mode).unwrap();
                    db.begin(&[]).unwrap();
                    assert_eq!(db.query(&query).unwrap(), vec![seed[1], seed[2]]);
                    let outer = db.savepoint().unwrap();
                    let mut expected = seed.clone();
                    expected[1].active = false;
                    expected[2].due = 20;
                    expected[2].event_time = 20;
                    expected[6].active = true;
                    expected[6].due = -1;
                    expected[6].event_time = -1;
                    expected[0].due = -2;
                    expected[0].event_time = -2;
                    for id in [1, 2, 6, 0] {
                        db.write(expected[id]).unwrap();
                    }
                    assert_eq!(
                        db.query(&query).unwrap(),
                        expected_prefix(&expected, &query)
                    );
                    let inner = db.savepoint().unwrap();
                    let mut holes = expected.clone();
                    for id in [0, 6] {
                        holes[id].active = false;
                        db.write(holes[id]).unwrap();
                    }
                    // Both original leaders are now absent. Reading only the SQL
                    // first two before applying an overlay would incorrectly yield
                    // no result instead of these later reserved rows.
                    assert_eq!(db.query(&query).unwrap(), vec![seed[3], seed[4]]);
                    db.rollback_to(inner).unwrap();
                    assert_eq!(
                        db.query(&query).unwrap(),
                        expected_prefix(&expected, &query)
                    );
                    db.rollback_to(outer).unwrap();
                    assert_eq!(db.query(&query).unwrap(), vec![seed[1], seed[2]]);
                    db.commit().unwrap();
                    assert_eq!(adapter::snapshot(url, schema).unwrap(), seed);
                    // A later transaction must not reuse the rolled-back prefix
                    // membership or buffered row cache from this transaction.
                    db.begin(&[]).unwrap();
                    assert_eq!(db.query(&query).unwrap(), vec![seed[1], seed[2]]);
                    db.abort().unwrap();
                }
            },
        );
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn maintenance_prefix_serializable_reads_reject_competing_earlier_rows_and_empty_phantoms() {
    for mode in [WriteMode::Immediate, WriteMode::Buffered] {
        for expired in [false, true] {
            for empty in [false, true] {
                let kind = if expired { POSITION } else { SCHEDULED };
                let seed = [
                    Record {
                        id: 0,
                        active: !empty,
                        kind,
                        due: 10,
                        event_time: 10,
                        ..Record::default()
                    },
                    Record {
                        id: 1,
                        ..Record::default()
                    },
                    Record {
                        id: 2,
                        ..Record::default()
                    },
                ];
                let query = if expired {
                    Query::FirstExpired {
                        before: 100,
                        limit: 1,
                    }
                } else {
                    Query::FirstDue { at: 100, limit: 1 }
                };
                with_records_and_selection(
                    "prefix_phantom",
                    &seed,
                    MaintenanceSelection::Prefix,
                    |url, schema| {
                        let mut a = Adapter::connect_with_mode(url, schema, mode).unwrap();
                        let mut b = Adapter::connect_with_mode(url, schema, mode).unwrap();
                        for db in [&mut a, &mut b] {
                            db.begin(&[]).unwrap();
                            assert_eq!(db.query(&query).unwrap(), expected_prefix(&seed, &query));
                        }
                        let create = |id| Record {
                            id,
                            active: true,
                            kind,
                            due: 0,
                            event_time: 0,
                            ..Record::default()
                        };
                        a.write(create(1)).unwrap();
                        let b_write = b.write(create(2));
                        let results = [a.commit(), b_write.and_then(|()| b.commit())];
                        assert_eq!(results.iter().filter(|result|result.is_ok()).count(),1,"{mode:?} expired={expired} empty={empty}: both old prefixes cannot precede each other's earlier insert");
                        for rejected in results.into_iter().filter_map(Result::err) {
                            assert_eq!(rejected, DbError::Conflict);
                        }
                        assert!(a
                            .retry_causes
                            .keys()
                            .chain(b.retry_causes.keys())
                            .all(|key| key.ends_with(":40001")));
                        let committed = adapter::snapshot(url, schema).unwrap();
                        assert_eq!(
                            committed
                                .iter()
                                .filter(|row| row.id > 0 && row.active)
                                .count(),
                            1
                        );
                        let mut retry = Adapter::connect_with_mode(url, schema, mode).unwrap();
                        retry.begin(&[]).unwrap();
                        let prefix = retry.query(&query).unwrap();
                        assert_eq!(prefix, expected_prefix(&committed, &query));
                        assert_eq!(prefix.len(), 1);
                        assert_ne!(prefix[0].id, 0);
                        retry.abort().unwrap();
                    },
                );
            }
        }
    }
}
