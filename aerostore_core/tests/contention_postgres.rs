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

use adapter::{Adapter, WriteMode};
use existing_model::{DbError, Record, FLIGHT, POSITION, SCHEDULED};
use storage::{Query, Store};

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn serializable_queries_discover_writes_without_predeclaring_them() {
    empty_predicate_race(WriteMode::Immediate);
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn buffered_empty_predicates_reject_competing_creation() {
    empty_predicate_race(WriteMode::Buffered);
}

fn empty_predicate_race(mode: WriteMode) {
    let url = std::env::var("AEROSTORE_CONTENTION_PG_URL").expect("disposable PostgreSQL URL");
    let schema = format!("contention_contract_{mode:?}_{}", std::process::id());
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
        let mut a = Adapter::connect_with_mode(&url, &schema, mode).unwrap();
        let mut b = Adapter::connect_with_mode(&url, &schema, mode).unwrap();
        assert!(matches!(a.begin(&[0]), Err(DbError::Fatal(_))));
        a.begin(&[]).unwrap();
        b.begin(&[]).unwrap();
        let predicate = Query::Candidates {
            callsign: 42,
            tail: 0,
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
    let url = std::env::var("AEROSTORE_CONTENTION_PG_URL").expect("disposable PostgreSQL URL");
    let schema = format!("contention_{}_{}", label, std::process::id());
    adapter::initialize(&url, &schema, records).unwrap();
    let result = std::panic::catch_unwind(|| test(&url, &schema));
    adapter::cleanup(&url, &schema).unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires AEROSTORE_CONTENTION_PG_URL pointing to disposable PostgreSQL"]
fn buffered_overlay_merges_predicate_entries_exits_and_restores_savepoints() {
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
        let mut db = Adapter::connect_with_mode(url, schema, WriteMode::Buffered).unwrap();
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
        for mode in [WriteMode::Immediate, WriteMode::Buffered] {
            let mut db = Adapter::connect_with_mode(url, schema, mode).unwrap();
            db.begin(&[]).unwrap();
            for scheduled in [i64::MIN, -1, 0, 1, i64::MAX] {
                for window in [0, 1, i64::MAX, -1, i64::MIN] {
                    for (callsign, tail) in [(42, 0), (99, 7), (99, 0)] {
                        let q = Query::Candidates {
                            callsign,
                            tail,
                            scheduled,
                            window,
                        };
                        let expected: Vec<_> =
                            seed.iter().copied().filter(|row| q.matches(row)).collect();
                        assert_eq!(db.query(&q).unwrap(), expected, "{mode:?} {q:?}");
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
