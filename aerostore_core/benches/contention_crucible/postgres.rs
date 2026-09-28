//! Query-driven PostgreSQL adapter: SERIALIZABLE without predeclared writes or prelocks.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Duration;

use ::postgres::{Client, Config, NoTls, Row, Statement};

use super::storage::{Query, Store};
use crate::extended_crucible::metrics::StoreMetrics;
use crate::extended_crucible::model::{DbError, Record};

/// Both modes execute the same SERIALIZABLE predicate reads. Buffered mode
/// discovers its final write set before acquiring row locks at commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Immediate,
    Buffered,
}

/// Equivalent candidate predicates with different PostgreSQL access paths.
/// The default retains the original query for controlled comparisons.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateQuery {
    #[default]
    Or,
    Split,
}

pub fn candidate_query_metadata(query: CandidateQuery, postgres: bool) -> serde_json::Value {
    serde_json::json!({
        "format":"postgres-candidate-query-v1", "requested":query,
        "effective":if postgres {serde_json::json!(query)} else {serde_json::json!("not_applicable")},
    })
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SqlMetrics {
    pub point_read_statements: u64,
    pub predicate_statements: u64,
    pub read_cache_hits: u64,
    pub overlay_reads: u64,
    pub buffered_writes: u64,
    pub immediate_write_statements: u64,
    pub ordered_lock_statements: u64,
    pub batch_write_statements: u64,
    pub batch_written_rows: u64,
}

const OWNERSHIP_MARKER: &str = "aerostore contention-crucible disposable schema v1";
const COLUMNS: &str = "id, active, kind, family, pedigree, callsign, tail, origin, destination, \
    scheduled, event_time, due, latitude, longitude, altitude, ground_speed, status, revision, \
    source, parent, sequence";

fn candidate_sql(select: &str, query: CandidateQuery, parameters: [&str; 4]) -> String {
    let [callsign, tail, lower, upper] = parameters;
    // `callsign` and `tail` are NOT NULL: the second branch excludes exactly
    // the first branch's rows, so UNION ALL preserves both uniqueness and the
    // original predicate. Explicit tail<>0 makes the partial-index condition
    // visible even to a generic prepared plan. One statement retains one SSI
    // snapshot, and ORDER BY applies to the complete union.
    match query {
        CandidateQuery::Or => format!(
            "{select} WHERE active AND kind=1 \
             AND (callsign={callsign} OR (({tail})::bigint<>0 AND tail={tail})) \
             AND scheduled BETWEEN {lower} AND {upper} ORDER BY id"
        ),
        CandidateQuery::Split => format!(
            "{select} WHERE active AND kind=1 AND callsign={callsign} \
             AND scheduled BETWEEN {lower} AND {upper} \
             UNION ALL \
             {select} WHERE active AND kind=1 AND ({tail})::bigint<>0 \
             AND tail<>0 AND tail={tail} AND callsign<>{callsign} \
             AND scheduled BETWEEN {lower} AND {upper} ORDER BY id"
        ),
    }
}

pub const MAX_ANALYZE_DISPATCH_LATENESS_NS: u64 = 1_000_000_000;

/// Public treatment identity is present even when the PostgreSQL-only option is
/// carried through a matched native configuration.
pub fn statistics_metadata(after_seconds: u64, postgres: bool) -> serde_json::Value {
    serde_json::json!({
        "format":"postgres-statistics-v1", "requested_after_seconds":after_seconds,
        "effective_policy":if !postgres {"not_applicable"} else if after_seconds == 0 {"initial_only"} else {"initial_and_scheduled"},
        "initial_analyze_executed":postgres, "runtime_analyze":null,
    })
}

fn monotonic_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) },
        0
    );
    (time.tv_sec as u64) * 1_000_000_000 + time.tv_nsec as u64
}

fn statistics_file(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    let temporary = path.with_extension("tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    std::fs::rename(temporary, path).map_err(|e| e.to_string())
}

fn owned_relation(client: &mut Client, schema: &str) -> Result<(u32, u32, i32), String> {
    schema_name(schema)?;
    let row = client
        .query_opt(
            "SELECT n.oid AS schema_oid,c.oid AS relation_oid,pg_backend_pid() AS backend_pid, \
         obj_description(n.oid,'pg_namespace') AS marker \
         FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid \
         WHERE n.nspname=$1 AND c.relname='records' AND c.relkind='r'",
            &[&schema],
        )
        .map_err(pg_error)?
        .ok_or("owned records relation is missing")?;
    let marker: Option<String> = row.try_get("marker").map_err(pg_error)?;
    if marker.as_deref() != Some(OWNERSHIP_MARKER) {
        return Err("refusing ANALYZE: missing contention-crucible ownership marker".into());
    }
    let schema_oid: u32 = row.try_get("schema_oid").map_err(pg_error)?;
    let relation_oid: u32 = row.try_get("relation_oid").map_err(pg_error)?;
    let backend_pid: i32 = row.try_get("backend_pid").map_err(pg_error)?;
    if schema_oid == 0 || relation_oid == 0 || backend_pid <= 0 {
        return Err("invalid owned relation/backend identity".into());
    }
    Ok((schema_oid, relation_oid, backend_pid))
}

fn statistics_sample(client: &mut Client, relation_oid: u32) -> Result<serde_json::Value, String> {
    client
        .batch_execute("SELECT pg_stat_clear_snapshot()")
        .map_err(pg_error)?;
    let row = client
        .query_one(
            "SELECT analyze_count,autoanalyze_count,n_mod_since_analyze, \
         last_analyze::text,last_autoanalyze::text FROM pg_stat_all_tables WHERE relid=$1",
            &[&relation_oid],
        )
        .map_err(pg_error)?;
    let mut value = serde_json::Map::new();
    for field in ["analyze_count", "autoanalyze_count", "n_mod_since_analyze"] {
        let count: i64 = row.try_get(field).map_err(pg_error)?;
        if count < 0 {
            return Err(format!("negative PostgreSQL statistics counter {field}"));
        }
        value.insert(field.into(), count.into());
    }
    for field in ["last_analyze", "last_autoanalyze"] {
        value.insert(
            field.into(),
            serde_json::json!(row.try_get::<_, Option<String>>(field).map_err(pg_error)?),
        );
    }
    value.insert("observed_ns".into(), monotonic_ns().into());
    value.insert("counters_may_lag".into(), true.into());
    Ok(value.into())
}

enum StatisticsRequest {
    Schedule { start: u64, end: u64 },
    Cancel,
}

/// Dedicated, preconnected observer. It never blocks the coordinator's receipt
/// consumption while ANALYZE runs. Every exit joins it before schema cleanup.
pub struct StatisticsControl {
    requests: Sender<StatisticsRequest>,
    cancellation: ::postgres::CancelToken,
    thread: Option<JoinHandle<Result<serde_json::Value, String>>>,
    result: Option<Result<serde_json::Value, String>>,
    scheduled: bool,
}

impl StatisticsControl {
    pub fn prepare(
        url: &str,
        schema: &str,
        after_seconds: u64,
        path: &Path,
    ) -> Result<Self, String> {
        if after_seconds == 0 {
            return Err("scheduled ANALYZE requires a positive delay".into());
        }
        let mut client = connect_client(url, false)?;
        let identity = owned_relation(&mut client, schema)?;
        let cancellation = client.cancel_token();
        let mut report = statistics_metadata(after_seconds, true);
        report["runtime_analyze"] = serde_json::json!({
            "status":"prepared", "requested_after_seconds":after_seconds,
            "admission_started_ns":null,"admission_finished_ns":null,
            "scheduled_ns":null,"dispatched_ns":null,"finished_ns":null,
            "maximum_dispatch_lateness_ns":MAX_ANALYZE_DISPATCH_LATENESS_NS,
            "schema_oid":identity.0,"relation_oid":identity.1,"backend_pid":identity.2,
            "command_succeeded":false,"before":null,"after":null,"error":null,
        });
        statistics_file(path, &report)?;
        let schema = schema.to_owned();
        let path = path.to_owned();
        let (requests, receiver) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let outcome = scheduled_analyze(
                &mut client,
                &schema,
                identity,
                after_seconds,
                &path,
                &receiver,
                &mut report,
            );
            if let Err(error) = &outcome {
                if report["runtime_analyze"]["status"] != "cancelled" {
                    report["runtime_analyze"]["status"] = "failed".into();
                }
                report["runtime_analyze"]["error"] = error.clone().into();
            }
            let persisted = statistics_file(&path, &report);
            match (outcome, persisted) {
                (Ok(()), Ok(())) => Ok(report),
                (Err(error), Ok(())) => Err(error),
                (Ok(()), Err(error)) => {
                    Err(format!("statistics evidence persistence failed: {error}"))
                }
                (Err(error), Err(persist)) => Err(format!(
                    "{error}; statistics evidence persistence failed: {persist}"
                )),
            }
        });
        Ok(Self {
            requests,
            cancellation,
            thread: Some(thread),
            result: None,
            scheduled: false,
        })
    }

    pub fn schedule(&mut self, start: u64, end: u64) -> Result<(), String> {
        if self.scheduled {
            return Err("statistics observer already scheduled".into());
        }
        self.requests
            .send(StatisticsRequest::Schedule { start, end })
            .map_err(|e| e.to_string())?;
        self.scheduled = true;
        Ok(())
    }

    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.result = Some(
                thread
                    .join()
                    .unwrap_or_else(|_| Err("statistics observer panicked".into())),
            );
        }
    }

    pub fn check(&mut self) -> Result<(), String> {
        if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
            self.join();
        }
        match &self.result {
            Some(Err(error)) => Err(error.clone()),
            _ => Ok(()),
        }
    }

    pub fn finish(mut self) -> Result<serde_json::Value, String> {
        if !self.scheduled {
            return Err("statistics observer was never scheduled".into());
        }
        self.join();
        self.result
            .take()
            .ok_or("statistics observer result missing")?
    }

    pub fn cancel(mut self) -> Result<(), String> {
        self.stop();
        // Cancellation caused by an existing workload error is expected; its
        // receipt remains failed/cancelled and cannot qualify a successful run.
        match self.result.take() {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) if error == "scheduled ANALYZE cancelled" => Ok(()),
            Some(Err(error)) => Err(error),
            None => Err("statistics observer result missing after cancellation".into()),
        }
    }

    fn stop(&mut self) {
        if self.thread.is_some() {
            let _ = self.requests.send(StatisticsRequest::Cancel);
            // Interrupt an already-running statement; the regular 60-second
            // statement timeout remains the fallback if cancellation fails.
            let _ = self.cancellation.cancel_query(NoTls);
            self.join();
        }
    }
}

impl Drop for StatisticsControl {
    fn drop(&mut self) {
        self.stop();
    }
}

fn scheduled_analyze(
    client: &mut Client,
    schema: &str,
    identity: (u32, u32, i32),
    after_seconds: u64,
    path: &Path,
    requests: &Receiver<StatisticsRequest>,
    report: &mut serde_json::Value,
) -> Result<(), String> {
    let (start, end) = match requests.recv() {
        Ok(StatisticsRequest::Schedule { start, end }) => (start, end),
        _ => {
            report["runtime_analyze"]["status"] = "cancelled".into();
            return Err("scheduled ANALYZE cancelled".into());
        }
    };
    let scheduled = after_seconds
        .checked_mul(1_000_000_000)
        .and_then(|delay| start.checked_add(delay))
        .filter(|at| *at > start && *at < end)
        .ok_or("scheduled ANALYZE must fall within admission")?;
    report["runtime_analyze"]["status"] = "scheduled".into();
    report["runtime_analyze"]["admission_started_ns"] = start.into();
    report["runtime_analyze"]["admission_finished_ns"] = end.into();
    report["runtime_analyze"]["scheduled_ns"] = scheduled.into();
    statistics_file(path, report)?;
    loop {
        let now = monotonic_ns();
        if now >= scheduled {
            break;
        }
        match requests.recv_timeout(Duration::from_nanos(scheduled - now)) {
            Err(RecvTimeoutError::Timeout) => {}
            _ => {
                report["runtime_analyze"]["status"] = "cancelled".into();
                return Err("scheduled ANALYZE cancelled".into());
            }
        }
    }
    if requests.try_recv().is_ok() {
        report["runtime_analyze"]["status"] = "cancelled".into();
        return Err("scheduled ANALYZE cancelled".into());
    }
    // ANALYZE already takes this lock mode. Retain it across identity checks,
    // ANALYZE, and commit so a dropped/replaced relation cannot qualify. It is
    // compatible with the workload's normal RowExclusive write locks.
    client
        .batch_execute(&format!(
        "BEGIN; SET LOCAL lock_timeout='1s'; LOCK TABLE {}.records IN SHARE UPDATE EXCLUSIVE MODE",
        schema_name(schema)?))
        .map_err(pg_error)?;
    if owned_relation(client, schema)? != identity {
        return Err("owned ANALYZE relation/backend identity changed".into());
    }
    report["runtime_analyze"]["before"] = statistics_sample(client, identity.1)?;
    report["runtime_analyze"]["status"] = "running".into();
    statistics_file(path, report)?;
    let dispatched = monotonic_ns();
    report["runtime_analyze"]["dispatched_ns"] = dispatched.into();
    if dispatched < scheduled
        || dispatched - scheduled > MAX_ANALYZE_DISPATCH_LATENESS_NS
        || dispatched >= end
    {
        return Err("scheduled ANALYZE missed its dispatch window".into());
    }
    let command = client
        .batch_execute(&format!("ANALYZE {}.records; COMMIT", schema_name(schema)?))
        .map_err(pg_error);
    let finished = monotonic_ns();
    report["runtime_analyze"]["finished_ns"] = finished.into();
    report["runtime_analyze"]["command_succeeded"] = command.is_ok().into();
    command?;
    if finished >= end {
        return Err("scheduled ANALYZE completed outside admission".into());
    }
    report["runtime_analyze"]["after"] = statistics_sample(client, identity.1)?;
    if owned_relation(client, schema)? != identity {
        return Err("owned ANALYZE relation/backend identity changed".into());
    }
    report["runtime_analyze"]["status"] = "succeeded".into();
    Ok(())
}

struct Queries {
    candidates: Statement,
    family: Statement,
    positions: Statement,
    due: Statement,
    expired: Statement,
    global_due: Statement,
    global_expired: Statement,
    all: Statement,
}

pub struct Adapter {
    pub metrics: StoreMetrics,
    client: Client,
    read_statement: Statement,
    write_statement: Statement,
    lock_statement: Statement,
    batch_statement: Statement,
    mode: WriteMode,
    overlay: BTreeMap<usize, Record>,
    read_cache: BTreeMap<usize, Record>,
    pub sql_metrics: SqlMetrics,
    queries: Queries,
    open: bool,
    /// Rejections by operation and server SQLSTATE; never inferred from timings.
    pub retry_causes: BTreeMap<String, u64>,
    savepoints: Vec<(usize, BTreeMap<usize, Record>)>,
    next_savepoint: usize,
}

fn schema_name(schema: &str) -> Result<String, String> {
    if schema.is_empty()
        || schema.len() > 63
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(
            "scratch schema must contain 1..=63 ASCII letters, digits, or underscores".into(),
        );
    }
    Ok(format!("\"{schema}\""))
}

fn connect_client(url: &str, synchronous_commit: bool) -> Result<Client, String> {
    let mut config: Config = url.parse().map_err(pg_error)?;
    config.connect_timeout(Duration::from_secs(5));
    let mut client = config.connect(NoTls).map_err(pg_error)?;
    client
        .batch_execute(&format!(
            "SET application_name = 'aerostore-contention-crucible'; \
         SET synchronous_commit = {}; SET statement_timeout = '60s';",
            if synchronous_commit { "on" } else { "off" }
        ))
        .map_err(pg_error)?;
    Ok(client)
}

fn pg_error(error: ::postgres::Error) -> String {
    match error.as_db_error() {
        Some(database) => format!(
            "{} (SQLSTATE {}{})",
            database.message(),
            database.code().code(),
            database
                .detail()
                .map(|detail| format!("; {detail}"))
                .unwrap_or_default(),
        ),
        None => error.to_string(),
    }
}

fn retry_sqlstate(code: &str) -> bool {
    matches!(code, "40001" | "40P01")
}

fn decode(row: &Row) -> Result<Record, String> {
    let id: i64 = row.try_get("id").map_err(pg_error)?;
    Ok(Record {
        id: usize::try_from(id).map_err(|_| format!("invalid PostgreSQL row id {id}"))?,
        active: row.try_get("active").map_err(pg_error)?,
        kind: row.try_get("kind").map_err(pg_error)?,
        family: row.try_get("family").map_err(pg_error)?,
        pedigree: row.try_get("pedigree").map_err(pg_error)?,
        callsign: row.try_get("callsign").map_err(pg_error)?,
        tail: row.try_get("tail").map_err(pg_error)?,
        origin: row.try_get("origin").map_err(pg_error)?,
        destination: row.try_get("destination").map_err(pg_error)?,
        scheduled: row.try_get("scheduled").map_err(pg_error)?,
        event_time: row.try_get("event_time").map_err(pg_error)?,
        due: row.try_get("due").map_err(pg_error)?,
        latitude: row.try_get("latitude").map_err(pg_error)?,
        longitude: row.try_get("longitude").map_err(pg_error)?,
        altitude: row.try_get("altitude").map_err(pg_error)?,
        ground_speed: row.try_get("ground_speed").map_err(pg_error)?,
        status: row.try_get("status").map_err(pg_error)?,
        revision: row.try_get("revision").map_err(pg_error)?,
        source: row.try_get("source").map_err(pg_error)?,
        parent: row.try_get("parent").map_err(pg_error)?,
        sequence: row.try_get("sequence").map_err(pg_error)?,
    })
}

fn record_parameters<'a>(
    row: &'a Record,
    id: &'a i64,
) -> [&'a (dyn ::postgres::types::ToSql + Sync); 21] {
    [
        id,
        &row.active,
        &row.kind,
        &row.family,
        &row.pedigree,
        &row.callsign,
        &row.tail,
        &row.origin,
        &row.destination,
        &row.scheduled,
        &row.event_time,
        &row.due,
        &row.latitude,
        &row.longitude,
        &row.altitude,
        &row.ground_speed,
        &row.status,
        &row.revision,
        &row.source,
        &row.parent,
        &row.sequence,
    ]
}

pub fn initialize(url: &str, schema: &str, records: &[Record]) -> Result<(), String> {
    let schema = schema_name(schema)?;
    let mut client = connect_client(url, false)?;
    let fsync: String = client
        .query_one("SHOW fsync", &[])
        .map_err(pg_error)?
        .get(0);
    if fsync != "on" {
        return Err(format!("PostgreSQL requires fsync=on; observed {fsync}"));
    }
    let mut transaction = client.transaction().map_err(pg_error)?;
    // CREATE, rather than DROP/CREATE or IF NOT EXISTS, prevents an accidental
    // collision from mutating an existing user schema.
    transaction
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
         COMMENT ON SCHEMA {schema} IS '{OWNERSHIP_MARKER}'; \
         CREATE TABLE {schema}.records ( \
           id bigint PRIMARY KEY, active boolean NOT NULL, kind bigint NOT NULL, \
           family bigint NOT NULL, pedigree bigint NOT NULL, callsign bigint NOT NULL, \
           tail bigint NOT NULL, origin bigint NOT NULL, destination bigint NOT NULL, \
           scheduled bigint NOT NULL, event_time bigint NOT NULL, due bigint NOT NULL, \
           latitude bigint NOT NULL, longitude bigint NOT NULL, altitude bigint NOT NULL, \
           ground_speed bigint NOT NULL, status bigint NOT NULL, revision bigint NOT NULL, \
           source bigint NOT NULL, parent bigint NOT NULL, sequence bigint NOT NULL); \
         CREATE TABLE {schema}.drain_fence (id bigint PRIMARY KEY CHECK (id=1), epoch bigint NOT NULL); \
         INSERT INTO {schema}.drain_fence VALUES (1,0); \
         CREATE INDEX callsign_idx ON {schema}.records (callsign,scheduled) WHERE active AND kind=1; \
         CREATE INDEX tail_idx ON {schema}.records (tail,scheduled) WHERE active AND kind=1 AND tail<>0; \
         CREATE INDEX family_idx ON {schema}.records (family,kind) WHERE active; \
         CREATE INDEX positions_idx ON {schema}.records (family,pedigree) WHERE active AND kind=2; \
         CREATE INDEX family_due_idx ON {schema}.records (family,due) WHERE active AND kind=3; \
         CREATE INDEX due_idx ON {schema}.records (due) WHERE active AND kind=3; \
         CREATE INDEX family_expired_idx ON {schema}.records (family,event_time) WHERE active AND kind IN (2,4,5); \
         CREATE INDEX event_time_idx ON {schema}.records (event_time) WHERE active AND kind IN (2,4,5);"
        ))
        .map_err(pg_error)?;
    let statement = transaction
        .prepare(&format!(
            "INSERT INTO {schema}.records ({COLUMNS}) VALUES \
         ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)"
        ))
        .map_err(pg_error)?;
    for row in records {
        let id = i64::try_from(row.id).map_err(|_| "record id exceeds PostgreSQL bigint")?;
        transaction
            .execute(&statement, &record_parameters(row, &id))
            .map_err(pg_error)?;
    }
    transaction
        .batch_execute(&format!("ANALYZE {schema}.records"))
        .map_err(pg_error)?;
    transaction.commit().map_err(pg_error)
}

pub fn snapshot(url: &str, schema: &str) -> Result<Vec<Record>, String> {
    let schema = schema_name(schema)?;
    let mut client = connect_client(url, false)?;
    client
        .query(
            &format!("SELECT {COLUMNS} FROM {schema}.records ORDER BY id"),
            &[],
        )
        .map_err(pg_error)?
        .iter()
        .map(decode)
        .collect()
}

pub fn cleanup(url: &str, schema: &str) -> Result<(), String> {
    let quoted = schema_name(schema)?;
    let mut client = connect_client(url, false)?;
    let rows = client
        .query(
            "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            &[&schema],
        )
        .map_err(pg_error)?;
    if rows.is_empty() {
        return Ok(());
    }
    let marker: Option<String> = rows[0].try_get(0).map_err(pg_error)?;
    if marker.as_deref() != Some(OWNERSHIP_MARKER) {
        return Err(format!(
            "refusing to remove schema {schema}: missing contention-crucible ownership marker"
        ));
    }
    client
        .batch_execute(&format!("DROP SCHEMA {quoted} CASCADE"))
        .map_err(pg_error)
}

/// Configuration of a fresh worker-equivalent connection. The native baseline
/// uses asynchronous WAL acknowledgement; PostgreSQL's flush timing and recovery
/// implementation remain different even when both acknowledge asynchronously.
pub fn configuration(url: &str, synchronous_commit: bool) -> Result<serde_json::Value, String> {
    let mut client = connect_client(url, synchronous_commit)?;
    let mut values = serde_json::Map::new();
    for setting in [
        "server_version",
        "fsync",
        "full_page_writes",
        "synchronous_commit",
        "wal_writer_delay",
        "wal_level",
        "shared_buffers",
        "max_connections",
        "deadlock_timeout",
        "statement_timeout",
        "lock_timeout",
        "work_mem",
        "effective_cache_size",
        "random_page_cost",
        "seq_page_cost",
        "max_pred_locks_per_transaction",
        "max_pred_locks_per_relation",
        "max_pred_locks_per_page",
        "autovacuum",
        "autovacuum_naptime",
        "autovacuum_vacuum_threshold",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_threshold",
        "autovacuum_analyze_scale_factor",
        "default_statistics_target",
        "plan_cache_mode",
        "checkpoint_timeout",
        "max_wal_size",
        "track_counts",
    ] {
        let value: String = client
            .query_one("SELECT current_setting($1)", &[&setting])
            .map_err(pg_error)?
            .get(0);
        values.insert(setting.into(), value.into());
    }
    client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .map_err(pg_error)?;
    let isolation: String = client
        .query_one("SHOW transaction_isolation", &[])
        .map_err(pg_error)?
        .get(0);
    values.insert("transaction_isolation".into(), isolation.into());
    client.batch_execute("ROLLBACK").map_err(pg_error)?;
    values.insert("snapshot_established".into(), "first business query".into());
    values.insert("predeclared_write_sets".into(), false.into());
    values.insert("application_prelocks".into(), false.into());
    values.insert(
        "buffered_commit_locks".into(),
        "discovered row IDs in ascending order, after business queries".into(),
    );
    values.insert(
        "write_mode".into(),
        "chosen explicitly by worker configuration".into(),
    );
    Ok(values.into())
}

/// Relation storage and approximate tuple retention, sampled outside timed
/// message execution. These values do not represent server resident memory.
pub fn retention(url: &str, schema: &str) -> Result<serde_json::Value, String> {
    let quoted = schema_name(schema)?;
    let relation = format!("{quoted}.records");
    let mut client = connect_client(url, false)?;
    let row = client
        .query_one(
            "SELECT pg_total_relation_size($1::text::regclass)::bigint AS total_bytes, \
             pg_table_size($1::text::regclass)::bigint AS table_bytes, \
             pg_indexes_size($1::text::regclass)::bigint AS index_bytes, \
             COALESCE(s.n_live_tup,0)::bigint AS estimated_live_tuples, \
             COALESCE(s.n_dead_tup,0)::bigint AS estimated_dead_tuples, \
             COALESCE(s.vacuum_count,0)::bigint AS vacuum_count, \
             COALESCE(s.autovacuum_count,0)::bigint AS autovacuum_count, \
             COALESCE(s.n_tup_ins,0)::bigint AS inserted_tuples, \
             COALESCE(s.n_tup_upd,0)::bigint AS updated_tuples, \
             COALESCE(s.n_tup_hot_upd,0)::bigint AS hot_updated_tuples, \
             COALESCE(s.n_tup_del,0)::bigint AS deleted_tuples, \
             COALESCE(s.analyze_count,0)::bigint AS analyze_count, \
             COALESCE(s.autoanalyze_count,0)::bigint AS autoanalyze_count \
             FROM pg_class c LEFT JOIN pg_stat_user_tables s ON s.relid=c.oid \
             WHERE c.oid=$1::text::regclass",
            &[&relation],
        )
        .map_err(pg_error)?;
    let mut values = serde_json::Map::new();
    for key in [
        "total_bytes",
        "table_bytes",
        "index_bytes",
        "estimated_live_tuples",
        "estimated_dead_tuples",
        "vacuum_count",
        "autovacuum_count",
        "inserted_tuples",
        "updated_tuples",
        "hot_updated_tuples",
        "deleted_tuples",
        "analyze_count",
        "autoanalyze_count",
    ] {
        values.insert(key.into(), row.get::<_, i64>(key).into());
    }
    values.insert("tuple_counts_are_estimates".into(), true.into());
    values.insert("resident_memory_measured".into(), false.into());
    // Cumulative server/database counters are explicitly scoped. Subtract
    // before/after samples only on an otherwise idle, owned fixture; statistics
    // snapshots can lag and must not be read as exact per-schema work counts.
    values.insert("database_counters".into(), optional_json(&mut client,
        "SELECT to_jsonb(s)::text FROM (SELECT datname, xact_commit, xact_rollback, deadlocks, conflicts, \
         temp_files, temp_bytes, blk_read_time, blk_write_time, stats_reset \
         FROM pg_stat_database WHERE datname=current_database()) s"));
    values.insert(
        "cluster_wal_counters".into(),
        optional_json(&mut client, "SELECT to_jsonb(s)::text FROM pg_stat_wal s"),
    );
    values.insert(
        "cluster_checkpoint_counters".into(),
        optional_json(
            &mut client,
            "SELECT to_jsonb(s)::text FROM pg_stat_bgwriter s",
        ),
    );
    Ok(values.into())
}

fn optional_json(client: &mut Client, sql: &str) -> serde_json::Value {
    match client.query_one(sql, &[]) {
        Ok(row) => {
            let encoded: String = row.get(0);
            serde_json::from_str(&encoded)
                .unwrap_or_else(|error| serde_json::json!({"unavailable": error.to_string()}))
        }
        Err(error) => serde_json::json!({"unavailable": pg_error(error)}),
    }
}

/// Actively flush the WAL position observed after workload completion, using a
/// synchronous commit on a dedicated row in this owned fixture. Message commits
/// remain asynchronous. This post-work fence parallels closing/draining native
/// WAL; it is not a checkpoint, recovery proof, or equal-acknowledgement claim.
pub fn drain(url: &str, schema: &str) -> Result<serde_json::Value, String> {
    let quoted = schema_name(schema)?;
    let started = std::time::Instant::now();
    let mut client = connect_client(url, true)?;
    let marker = client
        .query_opt(
            "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            &[&schema],
        )
        .map_err(pg_error)?
        .and_then(|row| row.get::<_, Option<String>>(0));
    if marker.as_deref() != Some(OWNERSHIP_MARKER) {
        return Err(format!(
            "refusing WAL fence in schema {schema}: missing contention-crucible ownership marker"
        ));
    }
    let fsync: String = client
        .query_one("SHOW fsync", &[])
        .map_err(pg_error)?
        .get(0);
    if fsync != "on" {
        return Err(format!(
            "PostgreSQL WAL fence requires fsync=on; observed {fsync}"
        ));
    }
    let target: String = client
        .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
        .map_err(pg_error)?
        .get(0);
    // Autocommit completion with synchronous_commit=on flushes this update's
    // commit record and all preceding WAL, including the sampled target. The
    // connection's statement timeout bounds the fence; errors never pass.
    let updated = client
        .execute(
            &format!("UPDATE {quoted}.drain_fence SET epoch=epoch+1 WHERE id=1"),
            &[],
        )
        .map_err(pg_error)?;
    if updated != 1 {
        return Err(format!("WAL fence updated {updated} rows instead of one"));
    }
    loop {
        let row = client.query_one(
            "SELECT pg_current_wal_flush_lsn()::text, pg_current_wal_flush_lsn()>=$1::text::pg_lsn",
            &[&target],
        ).map_err(pg_error)?;
        let flushed: String = row.get(0);
        let completed: bool = row.get(1);
        if completed {
            return Ok(
                serde_json::json!({"completed":completed,"target_lsn":target,
                "flushed_lsn":flushed,"elapsed_ns":started.elapsed().as_nanos() as u64,
                "method":"synchronous_commit_update_on_owned_drain_fence",
                "target_sampled_before_fence":true,"fence_rows_updated":updated,
                "contract":"post-work synchronous flush fence; business acknowledgements stay async; no recovery replay or checkpoint"}),
            );
        }
        if started.elapsed() >= Duration::from_secs(60) {
            return Err(format!(
                "WAL fence failed to flush target {target} within 60 seconds; observed {flushed}"
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Untimed EXPLAIN audit. Parameters come from an active seed flight if one
/// exists; each sample is included so a small-fixture sequential scan is not
/// mistaken for evidence about a production-sized workload.
pub fn query_plan_audit(url: &str, schema: &str) -> Result<serde_json::Value, String> {
    query_plan_audit_with_candidate_query(url, schema, CandidateQuery::Or)
}

pub fn query_plan_audit_with_candidate_query(
    url: &str,
    schema: &str,
    candidate_query: CandidateQuery,
) -> Result<serde_json::Value, String> {
    let quoted = schema_name(schema)?;
    let mut client = connect_client(url, false)?;
    let seed = client.query_opt(&format!("SELECT {COLUMNS} FROM {quoted}.records WHERE active AND kind=1 ORDER BY id LIMIT 1"), &[])
        .map_err(pg_error)?.as_ref().map(decode).transpose()?.unwrap_or_default();
    let select = format!("SELECT {COLUMNS} FROM {quoted}.records");
    let lower = seed.scheduled.saturating_sub(1800);
    let upper = seed.scheduled.saturating_add(1800);
    let predicates = [
        (
            "family",
            format!("active AND family={} AND kind=1", seed.family),
        ),
        (
            "positions",
            format!(
                "active AND family={} AND pedigree={} AND kind=2",
                seed.family, seed.pedigree
            ),
        ),
        (
            "due",
            format!(
                "active AND family={} AND kind=3 AND due<={}",
                seed.family, seed.due
            ),
        ),
        (
            "expired",
            format!(
                "active AND family={} AND kind IN (2,4,5) AND event_time<{}",
                seed.family, seed.event_time
            ),
        ),
        (
            "global_due",
            format!("active AND kind=3 AND due<={}", seed.due),
        ),
        (
            "global_expired",
            format!(
                "active AND kind IN (2,4,5) AND event_time<{}",
                seed.event_time
            ),
        ),
        ("all", "active".into()),
    ];
    let mut plans = serde_json::Map::new();
    let candidates = candidate_sql(
        &select,
        candidate_query,
        [
            &seed.callsign.to_string(),
            &seed.tail.to_string(),
            &lower.to_string(),
            &upper.to_string(),
        ],
    );
    for (name, sql) in std::iter::once(("candidates", candidates)).chain(
        predicates
            .into_iter()
            .map(|(name, predicate)| (name, format!("{select} WHERE {predicate} ORDER BY id"))),
    ) {
        let row = client
            .query_one(&format!("EXPLAIN (FORMAT JSON, SETTINGS) {sql}"), &[])
            .map_err(pg_error)?;
        let plan: serde_json::Value = row.get(0);
        plans.insert(name.into(), serde_json::json!({"sql":sql,"plan":plan}));
    }
    let indexes = client.query(
        "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=$1 AND tablename='records' ORDER BY indexname", &[&schema]
    ).map_err(pg_error)?.into_iter().map(|row| serde_json::json!({"name":row.get::<_,String>(0),"definition":row.get::<_,String>(1)})).collect::<Vec<_>>();
    Ok(
        serde_json::json!({"parameters_from_seed":seed,"plans":plans,"indexes":indexes,
        "postgres_candidate_query":candidate_query_metadata(candidate_query, true),
        "analyze_executed":false,"prepared_generic_plans_measured":false}),
    )
}

impl Adapter {
    pub fn connect(url: &str, schema: &str) -> Result<Self, String> {
        Self::connect_with_durability(url, schema, false)
    }

    /// `false` acknowledges before WAL flush; `true` requests PostgreSQL's
    /// synchronous commit contract. Native runs currently use asynchronous WAL.
    pub fn connect_with_durability(
        url: &str,
        schema: &str,
        synchronous_commit: bool,
    ) -> Result<Self, String> {
        Self::connect_with_options(url, schema, synchronous_commit, WriteMode::Immediate)
    }

    pub fn connect_with_mode(url: &str, schema: &str, mode: WriteMode) -> Result<Self, String> {
        Self::connect_with_options(url, schema, false, mode)
    }

    pub fn connect_with_options(
        url: &str,
        schema: &str,
        synchronous_commit: bool,
        mode: WriteMode,
    ) -> Result<Self, String> {
        Self::connect_with_candidate_query(
            url,
            schema,
            synchronous_commit,
            mode,
            CandidateQuery::Or,
        )
    }

    pub fn connect_with_candidate_query(
        url: &str,
        schema: &str,
        synchronous_commit: bool,
        mode: WriteMode,
        candidate_query: CandidateQuery,
    ) -> Result<Self, String> {
        let schema = schema_name(schema)?;
        let mut client = connect_client(url, synchronous_commit)?;
        let select = format!("SELECT {COLUMNS} FROM {schema}.records");
        let read_statement = client
            .prepare(&format!("{select} WHERE id=$1"))
            .map_err(pg_error)?;
        let write_statement = client
            .prepare(&format!(
                "UPDATE {schema}.records SET active=$2,kind=$3,family=$4,pedigree=$5, \
             callsign=$6,tail=$7,origin=$8,destination=$9,scheduled=$10,event_time=$11, \
             due=$12,latitude=$13,longitude=$14,altitude=$15,ground_speed=$16,status=$17, \
             revision=$18,source=$19,parent=$20,sequence=$21 WHERE id=$1"
            ))
            .map_err(pg_error)?;
        let lock_statement = client
            .prepare(&format!(
                "SELECT id FROM {schema}.records WHERE id=ANY($1::bigint[]) ORDER BY id FOR UPDATE"
            ))
            .map_err(pg_error)?;
        let columns: Vec<_> = COLUMNS.split(',').map(str::trim).collect();
        let arrays = columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                format!(
                    "${}::{}[]",
                    i + 1,
                    if *column == "active" {
                        "boolean"
                    } else {
                        "bigint"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let assignments = columns
            .iter()
            .skip(1)
            .map(|column| format!("{column}=v.{column}"))
            .collect::<Vec<_>>()
            .join(",");
        let batch_statement = client
            .prepare(&format!(
                "UPDATE {schema}.records AS r SET {assignments} \
             FROM unnest({arrays}) AS v({COLUMNS}) WHERE r.id=v.id"
            ))
            .map_err(pg_error)?;
        let queries = Queries {
            candidates: client.prepare(&candidate_sql(&select, candidate_query, ["$1", "$2", "$3", "$4"])).map_err(pg_error)?,
            family: client.prepare(&format!(
                "{select} WHERE active AND family=$1 AND kind=$2 ORDER BY id"
            )).map_err(pg_error)?,
            positions: client.prepare(&format!(
                "{select} WHERE active AND family=$1 AND pedigree=$2 AND kind=2 ORDER BY id"
            )).map_err(pg_error)?,
            due: client.prepare(&format!(
                "{select} WHERE active AND family=$1 AND kind=3 AND due<=$2 ORDER BY id"
            )).map_err(pg_error)?,
            expired: client.prepare(&format!(
                "{select} WHERE active AND family=$1 AND event_time<$2 AND kind IN (2,4,5) ORDER BY id"
            )).map_err(pg_error)?,
            global_due: client.prepare(&format!(
                "{select} WHERE active AND kind=3 AND due<=$1 ORDER BY id"
            )).map_err(pg_error)?,
            global_expired: client.prepare(&format!(
                "{select} WHERE active AND kind IN (2,4,5) AND event_time<$1 ORDER BY id"
            )).map_err(pg_error)?,
            all: client.prepare(&format!("{select} WHERE active ORDER BY id"))
                .map_err(pg_error)?,
        };
        Ok(Self {
            metrics: StoreMetrics::default(),
            client,
            read_statement,
            write_statement,
            lock_statement,
            batch_statement,
            mode,
            overlay: BTreeMap::new(),
            read_cache: BTreeMap::new(),
            sql_metrics: SqlMetrics::default(),
            queries,
            open: false,
            retry_causes: BTreeMap::new(),
            savepoints: Vec::new(),
            next_savepoint: 0,
        })
    }

    fn ensure_open(&self) -> Result<(), DbError> {
        if self.open {
            Ok(())
        } else {
            Err(DbError::Fatal("PostgreSQL transaction is closed".into()))
        }
    }

    fn clear_transaction(&mut self) {
        self.savepoints.clear();
        self.overlay.clear();
        self.read_cache.clear();
    }

    fn flush_overlay(&mut self) -> Result<(), DbError> {
        if self.overlay.is_empty() {
            return Ok(());
        }
        let ids = self
            .overlay
            .keys()
            .map(|id| i64::try_from(*id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| self.fail(DbError::Fatal("id exceeds PostgreSQL bigint".into())))?;
        // This is the discovered write set, after business queries. PostgreSQL
        // checks concurrent row changes against our SERIALIZABLE snapshot while
        // locking; ordering every transaction's locks removes write-order cycles.
        self.sql_metrics.ordered_lock_statements += 1;
        let locked = self
            .client
            .query(&self.lock_statement, &[&ids])
            .map_err(|error| self.database_failure("ordered_lock", error))?;
        if locked.len() != ids.len()
            || locked
                .iter()
                .zip(&ids)
                .any(|(row, id)| row.get::<_, i64>(0) != *id)
        {
            return Err(self.fail(DbError::Fatal(
                "buffered write references a missing reserved row".into(),
            )));
        }
        let mut active = Vec::with_capacity(ids.len());
        let mut integers = vec![Vec::with_capacity(ids.len()); 20];
        for (id, row) in ids.iter().zip(self.overlay.values()) {
            active.push(row.active);
            for (column, value) in integers.iter_mut().zip([
                *id,
                row.kind,
                row.family,
                row.pedigree,
                row.callsign,
                row.tail,
                row.origin,
                row.destination,
                row.scheduled,
                row.event_time,
                row.due,
                row.latitude,
                row.longitude,
                row.altitude,
                row.ground_speed,
                row.status,
                row.revision,
                row.source,
                row.parent,
                row.sequence,
            ]) {
                column.push(value);
            }
        }
        let mut parameters: Vec<&(dyn ::postgres::types::ToSql + Sync)> =
            vec![&integers[0], &active];
        parameters.extend(
            integers
                .iter()
                .skip(1)
                .map(|column| column as &(dyn ::postgres::types::ToSql + Sync)),
        );
        self.sql_metrics.batch_write_statements += 1;
        let count = self
            .client
            .execute(&self.batch_statement, &parameters)
            .map_err(|error| self.database_failure("batch_write", error))?;
        if count != ids.len() as u64 {
            return Err(self.fail(DbError::Fatal(format!(
                "batch updated {count} rows; expected {}",
                ids.len()
            ))));
        }
        self.sql_metrics.batch_written_rows += count;
        Ok(())
    }

    fn classified(&mut self, stage: &str, error: ::postgres::Error) -> DbError {
        if let Some(code) = error
            .code()
            .map(|code| code.code())
            .filter(|code| retry_sqlstate(code))
        {
            *self
                .retry_causes
                .entry(format!("{stage}:{code}"))
                .or_default() += 1;
            DbError::Conflict
        } else {
            DbError::Fatal(pg_error(error))
        }
    }

    fn database_failure(&mut self, stage: &str, error: ::postgres::Error) -> DbError {
        let classified = self.classified(stage, error);
        self.fail(classified)
    }

    fn fail(&mut self, error: DbError) -> DbError {
        if self.open {
            let rollback = self.client.batch_execute("ROLLBACK");
            self.open = false;
            self.metrics.aborts += 1;
            self.clear_transaction();
            if let Err(rollback_error) = rollback {
                return DbError::Fatal(format!("{error}; rollback also failed: {rollback_error}"));
            }
        }
        error
    }
}

impl Store for Adapter {
    fn begin(&mut self, write_slots: &[usize]) -> Result<(), DbError> {
        if self.open {
            return Err(self.fail(DbError::Fatal("transaction already open".into())));
        }
        if !write_slots.is_empty() {
            return Err(DbError::Fatal(
                "contention workload may not predeclare transaction write slots".into(),
            ));
        }
        self.client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .map_err(|error| self.classified("begin", error))?;
        self.open = true;
        self.metrics.begins += 1;
        self.clear_transaction();
        self.next_savepoint = 0;
        // PostgreSQL fixes its snapshot at the first business query. No lock
        // query or synthetic snapshot query hides query-discovered contention.
        Ok(())
    }

    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.ensure_open()?;
        self.metrics.reads += 1;
        if self.mode == WriteMode::Buffered {
            if let Some(row) = self.overlay.get(&id) {
                self.sql_metrics.overlay_reads += 1;
                return Ok(row.clone());
            }
            if let Some(row) = self.read_cache.get(&id) {
                self.sql_metrics.read_cache_hits += 1;
                return Ok(row.clone());
            }
        }
        let sql_id =
            i64::try_from(id).map_err(|_| DbError::Fatal("id exceeds PostgreSQL bigint".into()))?;
        self.sql_metrics.point_read_statements += 1;
        match self.client.query_opt(&self.read_statement, &[&sql_id]) {
            Ok(Some(row)) => {
                let record = decode(&row).map_err(|error| self.fail(DbError::Fatal(error)))?;
                if self.mode == WriteMode::Buffered {
                    self.read_cache.insert(id, record.clone());
                }
                Ok(record)
            }
            Ok(None) => Err(self.fail(DbError::Fatal(format!("missing reserved row {id}")))),
            Err(error) => Err(self.database_failure("read", error)),
        }
    }

    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        self.ensure_open()?;
        self.metrics.queries += 1;
        self.sql_metrics.predicate_statements += 1;
        let rows = match query {
            Query::Candidates {
                callsign,
                tail,
                scheduled,
                window,
            } => {
                // Clamp in i128 to preserve abs_diff and the model's unsigned
                // window semantics even at i64 extremes, while keeping the SQL
                // scheduled range indexable.
                let distance = *window as u64 as i128;
                let lower = (*scheduled as i128 - distance).max(i64::MIN as i128) as i64;
                let upper = (*scheduled as i128 + distance).min(i64::MAX as i128) as i64;
                self.client
                    .query(&self.queries.candidates, &[callsign, tail, &lower, &upper])
            }
            Query::Family { family, kind } => {
                self.client.query(&self.queries.family, &[family, kind])
            }
            Query::Positions { family, pedigree } => self
                .client
                .query(&self.queries.positions, &[family, pedigree]),
            Query::Due { family, at } => self.client.query(&self.queries.due, &[family, at]),
            Query::Expired { family, before } => {
                self.client.query(&self.queries.expired, &[family, before])
            }
            Query::GlobalDue { at } => self.client.query(&self.queries.global_due, &[at]),
            Query::GlobalExpired { before } => {
                self.client.query(&self.queries.global_expired, &[before])
            }
            Query::All => self.client.query(&self.queries.all, &[]),
        }
        .map_err(|error| self.database_failure("query", error))?;
        let mut records = rows
            .iter()
            .map(decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| self.fail(DbError::Fatal(error)))?;
        if records.iter().any(|record| !query.matches(record)) {
            return Err(self.fail(DbError::Fatal(
                "PostgreSQL query predicate differs from model".into(),
            )));
        }
        if self.mode == WriteMode::Buffered {
            // Always perform the SQL predicate read, including empty searches:
            // its SSI dependencies are necessary even if our overlay supplies
            // the complete visible result. Overlay rows can enter OR leave a
            // predicate; merging only returned SQL IDs would miss the former.
            for row in &records {
                self.read_cache.entry(row.id).or_insert_with(|| row.clone());
            }
            records.retain(|row| !self.overlay.contains_key(&row.id));
            records.extend(
                self.overlay
                    .values()
                    .filter(|row| query.matches(row))
                    .cloned(),
            );
            records.sort_unstable_by_key(|row| row.id);
        }
        self.metrics.returned_rows += records.len() as u64;
        Ok(records)
    }

    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.ensure_open()?;
        let id = i64::try_from(row.id)
            .map_err(|_| DbError::Fatal("id exceeds PostgreSQL bigint".into()))?;
        if self.mode == WriteMode::Buffered {
            // Validate physical slot existence now, preserving the immediate
            // adapter's missing-row failure. Typical handlers have already read
            // this row, so the cache avoids an additional database roundtrip.
            if !self.read_cache.contains_key(&row.id) && !self.overlay.contains_key(&row.id) {
                self.read(row.id)?;
            }
            self.overlay.insert(row.id, row);
            self.sql_metrics.buffered_writes += 1;
            self.metrics.writes += 1;
            return Ok(());
        }
        self.sql_metrics.immediate_write_statements += 1;
        match self
            .client
            .execute(&self.write_statement, &record_parameters(&row, &id))
        {
            Ok(1) => {
                self.metrics.writes += 1;
                Ok(())
            }
            Ok(count) => Err(self.fail(DbError::Fatal(format!(
                "write affected {count} rows instead of one"
            )))),
            Err(error) => Err(self.database_failure("write", error)),
        }
    }

    fn savepoint(&mut self) -> Result<usize, DbError> {
        self.ensure_open()?;
        let id = self.next_savepoint;
        self.next_savepoint += 1;
        self.client
            .batch_execute(&format!("SAVEPOINT contention_{id}"))
            .map_err(|error| self.database_failure("savepoint", error))?;
        self.savepoints.push((id, self.overlay.clone()));
        self.metrics.savepoints += 1;
        Ok(id)
    }

    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError> {
        self.ensure_open()?;
        let Some(position) = self.savepoints.iter().position(|(id, _)| *id == savepoint) else {
            return Err(self.fail(DbError::Fatal(format!("unknown savepoint {savepoint}"))));
        };
        self.client
            .batch_execute(&format!("ROLLBACK TO SAVEPOINT contention_{savepoint}"))
            .map_err(|error| self.database_failure("rollback_to", error))?;
        self.overlay = self.savepoints[position].1.clone();
        self.savepoints.truncate(position + 1);
        self.metrics.savepoint_rollbacks += 1;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), DbError> {
        self.ensure_open()?;
        if self.mode == WriteMode::Buffered {
            self.flush_overlay()?;
        }
        match self.client.batch_execute("COMMIT") {
            Ok(()) => {
                self.open = false;
                self.clear_transaction();
                self.metrics.commits += 1;
                Ok(())
            }
            // Only explicit serialization/deadlock rejection is retryable.
            // An I/O failure may have occurred after commit, so it stays fatal.
            Err(error) => Err(self.database_failure("commit", error)),
        }
    }

    fn abort(&mut self) -> Result<(), DbError> {
        if !self.open {
            return Ok(());
        }
        let result = self.client.batch_execute("ROLLBACK");
        self.open = false;
        self.clear_transaction();
        self.metrics.aborts += 1;
        result.map_err(|error| self.classified("abort", error))
    }
}

impl Drop for Adapter {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}
