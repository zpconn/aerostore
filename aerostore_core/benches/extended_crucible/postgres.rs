//! Relational PostgreSQL counterpart of the bounded synthetic workload.
use std::collections::BTreeSet;
use std::time::Duration;

use ::postgres::{Client, Config, NoTls, Row, Statement};

use super::contracts::ContractResult;
use super::metrics::StoreMetrics;
use super::model::{DbError, Query, Record, Store, FLIGHT};

const OWNERSHIP_MARKER: &str = "aerostore extended-crucible disposable schema v1";
const COLUMNS: &str = "id, active, kind, family, pedigree, callsign, tail, origin, destination, \
    scheduled, event_time, due, latitude, longitude, altitude, ground_speed, status, revision, \
    source, parent, sequence";

struct Queries {
    candidates: Statement,
    family: Statement,
    positions: Statement,
    due: Statement,
    expired: Statement,
    all: Statement,
}

pub struct Adapter {
    pub metrics: StoreMetrics,
    client: Client,
    read_statement: Statement,
    write_statement: Statement,
    lock_statement: Statement,
    queries: Queries,
    open: bool,
    write_slots: BTreeSet<usize>,
    savepoints: Vec<usize>,
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

fn connect_client(url: &str) -> Result<Client, String> {
    let mut config: Config = url.parse().map_err(pg_error)?;
    config.connect_timeout(Duration::from_secs(5));
    let mut client = config.connect(NoTls).map_err(pg_error)?;
    client
        .batch_execute(
            "SET application_name = 'aerostore-extended-crucible'; \
         SET synchronous_commit = off; SET statement_timeout = '60s';",
        )
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

fn classified(error: ::postgres::Error) -> DbError {
    match error.code().map(|code| code.code()) {
        Some("40001" | "40P01") => DbError::Conflict,
        _ => DbError::Fatal(pg_error(error)),
    }
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
    let mut client = connect_client(url)?;
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
         CREATE INDEX callsign_idx ON {schema}.records (callsign) WHERE active AND kind=1; \
         CREATE INDEX tail_idx ON {schema}.records (tail) WHERE active AND kind=1 AND tail<>0; \
         CREATE INDEX family_idx ON {schema}.records (family) WHERE active; \
         CREATE INDEX due_idx ON {schema}.records (due) WHERE active AND kind=3; \
         CREATE INDEX event_time_idx ON {schema}.records (event_time) WHERE active;"
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
    let mut client = connect_client(url)?;
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
    let mut client = connect_client(url)?;
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
            "refusing to remove schema {schema}: missing extended-crucible ownership marker"
        ));
    }
    client
        .batch_execute(&format!("DROP SCHEMA {quoted} CASCADE"))
        .map_err(pg_error)
}

impl Adapter {
    pub fn connect(url: &str, schema: &str) -> Result<Self, String> {
        let schema = schema_name(schema)?;
        let mut client = connect_client(url)?;
        let select = format!("SELECT {COLUMNS} FROM {schema}.records");
        let read_statement = client
            .prepare(&format!("{select} WHERE id=$1"))
            .map_err(pg_error)?;
        let lock_statement = client
            .prepare(&format!(
                "SELECT id FROM {schema}.records WHERE id=ANY($1) ORDER BY id FOR UPDATE"
            ))
            .map_err(pg_error)?;
        let write_statement = client
            .prepare(&format!(
                "UPDATE {schema}.records SET active=$2,kind=$3,family=$4,pedigree=$5, \
             callsign=$6,tail=$7,origin=$8,destination=$9,scheduled=$10,event_time=$11, \
             due=$12,latitude=$13,longitude=$14,altitude=$15,ground_speed=$16,status=$17, \
             revision=$18,source=$19,parent=$20,sequence=$21 WHERE id=$1"
            ))
            .map_err(pg_error)?;
        let queries = Queries {
            candidates: client.prepare(&format!(
                "{select} WHERE active AND kind=1 \
                 AND (callsign=$1 OR ($2::bigint<>0 AND tail=$2)) \
                 AND abs(scheduled::numeric-$3::bigint::numeric)<=$4::text::numeric ORDER BY id"
            )).map_err(pg_error)?,
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
            all: client.prepare(&format!("{select} WHERE active ORDER BY id"))
                .map_err(pg_error)?,
        };
        Ok(Self {
            metrics: StoreMetrics::default(),
            client,
            read_statement,
            write_statement,
            lock_statement,
            queries,
            open: false,
            write_slots: BTreeSet::new(),
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

    fn fail(&mut self, error: DbError) -> DbError {
        if self.open {
            let rollback = self.client.batch_execute("ROLLBACK");
            self.open = false;
            self.metrics.aborts += 1;
            self.write_slots.clear();
            self.savepoints.clear();
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
        self.write_slots = write_slots.iter().copied().collect();
        let ids: Vec<i64> = self
            .write_slots
            .iter()
            .map(|id| {
                i64::try_from(*id)
                    .map_err(|_| DbError::Fatal("write slot exceeds PostgreSQL bigint".into()))
            })
            .collect::<Result<_, _>>()?;
        self.client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .map_err(classified)?;
        self.open = true;
        self.metrics.begins += 1;
        self.savepoints.clear();
        self.next_savepoint = 0;
        // Even an empty declared write set runs this SELECT. PostgreSQL then
        // establishes the snapshot here, matching native begin_transaction.
        match self.client.query(&self.lock_statement, &[&ids]) {
            Ok(rows) if rows.len() == ids.len() => Ok(()),
            Ok(rows) => Err(self.fail(DbError::Fatal(format!(
                "declared {} write slots but only {} exist",
                ids.len(),
                rows.len()
            )))),
            Err(error) => Err(self.fail(classified(error))),
        }
    }

    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.ensure_open()?;
        let sql_id =
            i64::try_from(id).map_err(|_| DbError::Fatal("id exceeds PostgreSQL bigint".into()))?;
        self.metrics.reads += 1;
        match self.client.query_opt(&self.read_statement, &[&sql_id]) {
            Ok(Some(row)) => decode(&row).map_err(|error| self.fail(DbError::Fatal(error))),
            Ok(None) => Err(self.fail(DbError::Fatal(format!("missing reserved row {id}")))),
            Err(error) => Err(self.fail(classified(error))),
        }
    }

    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        self.ensure_open()?;
        self.metrics.queries += 1;
        let rows = match query {
            Query::Candidates {
                callsign,
                tail,
                scheduled,
                window,
            } => {
                // Numeric arithmetic avoids overflow at i64 extremes and the
                // unsigned bound exactly mirrors Query::matches' abs_diff.
                let unsigned_window = (*window as u64).to_string();
                self.client.query(
                    &self.queries.candidates,
                    &[callsign, tail, scheduled, &unsigned_window],
                )
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
            Query::All => self.client.query(&self.queries.all, &[]),
        }
        .map_err(|error| self.fail(classified(error)))?;
        let records = rows
            .iter()
            .map(decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| self.fail(DbError::Fatal(error)))?;
        if records.iter().any(|record| !query.matches(record)) {
            return Err(self.fail(DbError::Fatal(
                "PostgreSQL query predicate differs from model".into(),
            )));
        }
        self.metrics.returned_rows += records.len() as u64;
        Ok(records)
    }

    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.ensure_open()?;
        if !self.write_slots.contains(&row.id) {
            return Err(self.fail(DbError::Fatal(format!(
                "write to undeclared row {}",
                row.id
            ))));
        }
        let id = i64::try_from(row.id)
            .map_err(|_| DbError::Fatal("id exceeds PostgreSQL bigint".into()))?;
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
            Err(error) => Err(self.fail(classified(error))),
        }
    }

    fn savepoint(&mut self) -> Result<usize, DbError> {
        self.ensure_open()?;
        let id = self.next_savepoint;
        self.next_savepoint += 1;
        self.client
            .batch_execute(&format!("SAVEPOINT extended_{id}"))
            .map_err(|error| self.fail(classified(error)))?;
        self.savepoints.push(id);
        self.metrics.savepoints += 1;
        Ok(id)
    }

    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError> {
        self.ensure_open()?;
        let Some(position) = self.savepoints.iter().position(|id| *id == savepoint) else {
            return Err(self.fail(DbError::Fatal(format!("unknown savepoint {savepoint}"))));
        };
        self.client
            .batch_execute(&format!("ROLLBACK TO SAVEPOINT extended_{savepoint}"))
            .map_err(|error| self.fail(classified(error)))?;
        self.savepoints.truncate(position + 1);
        self.metrics.savepoint_rollbacks += 1;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), DbError> {
        self.ensure_open()?;
        match self.client.batch_execute("COMMIT") {
            Ok(()) => {
                self.open = false;
                self.write_slots.clear();
                self.savepoints.clear();
                self.metrics.commits += 1;
                Ok(())
            }
            // Only explicit serialization/deadlock rejection is retryable.
            // An I/O failure may have occurred after commit, so it stays fatal.
            Err(error) => Err(self.fail(classified(error))),
        }
    }

    fn abort(&mut self) -> Result<(), DbError> {
        if !self.open {
            return Ok(());
        }
        let result = self.client.batch_execute("ROLLBACK");
        self.open = false;
        self.write_slots.clear();
        self.savepoints.clear();
        self.metrics.aborts += 1;
        result.map_err(classified)
    }
}

impl Drop for Adapter {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}

fn contract(name: &str, passed: bool, details: String) -> ContractResult {
    ContractResult {
        name: name.into(),
        passed,
        details,
    }
}

fn records(values: &[i64]) -> Vec<Record> {
    values
        .iter()
        .enumerate()
        .map(|(id, value)| Record {
            id,
            active: true,
            kind: FLIGHT,
            callsign: 10,
            altitude: *value,
            parent: -1,
            ..Record::default()
        })
        .collect()
}

fn candidate(key: i64) -> Query {
    Query::Candidates {
        callsign: key,
        tail: 0,
        scheduled: 0,
        window: 1,
    }
}

fn db<T>(value: Result<T, DbError>) -> Result<T, String> {
    value.map_err(|error| error.to_string())
}

fn commit_outcome(adapter: &mut Adapter) -> Result<bool, String> {
    match adapter.commit() {
        Ok(()) => Ok(true),
        Err(DbError::Conflict) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn write_outcome(adapter: &mut Adapter, record: Record) -> Result<bool, String> {
    match adapter.write(record) {
        Ok(()) => Ok(true),
        Err(DbError::Conflict) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

fn isolated_probe(
    url: &str,
    schema: &str,
    suffix: &str,
    rows: &[Record],
    run: impl FnOnce(&str) -> Result<ContractResult, String>,
) -> Result<ContractResult, String> {
    let probe_schema = format!("{schema}_{suffix}");
    initialize(url, &probe_schema, rows)?;
    let result = run(&probe_schema);
    let clean = cleanup(url, &probe_schema);
    match (result, clean) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!("probe cleanup failed: {error}")),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; probe cleanup failed: {cleanup_error}"))
        }
    }
}

pub fn contracts(url: &str, schema: &str) -> Result<Vec<ContractResult>, String> {
    schema_name(schema)?;
    let mut results = Vec::new();
    let inactive = vec![
        Record {
            id: 0,
            ..Record::default()
        },
        Record {
            id: 1,
            ..Record::default()
        },
    ];
    results.push(isolated_probe(url, schema, "absence", &inactive, |schema| {
        let mut first = Adapter::connect(url, schema)?;
        let mut second = Adapter::connect(url, schema)?;
        db(first.begin(&[0]))?;
        db(second.begin(&[1]))?;
        let first_candidates = db(first.query(&candidate(42)))?.len();
        let second_candidates = db(second.query(&candidate(42)))?.len();
        let first_wrote = write_outcome(&mut first, Record { id: 0, active: true, kind: FLIGHT, callsign: 42, ..Record::default() })?;
        let second_wrote = write_outcome(&mut second, Record { id: 1, active: true, kind: FLIGHT, callsign: 42, ..Record::default() })?;
        let first_committed = first_wrote && commit_outcome(&mut first)?;
        let second_committed = second_wrote && commit_outcome(&mut second)?;
        let committed = usize::from(first_committed) + usize::from(second_committed);
        let matches = snapshot(url, schema)?.iter().filter(|row| candidate(42).matches(row)).count();
        Ok(contract("serializable_absent_candidate_creation",
            first_candidates == 0 && second_candidates == 0 && committed == 1 && matches == 1,
            format!("PostgreSQL SERIALIZABLE: overlapping absent-key searches returned {first_candidates}/{second_candidates}; committed={committed}, rejected={}, matching_rows={matches}. Expected exactly one surviving flight; the other transaction must fail with SQLSTATE 40001/40P01.", 2 - committed)))
    })?);
    results.push(isolated_probe(url, schema, "publication", &records(&[10]), |schema| {
        let mut writer = Adapter::connect(url, schema)?;
        db(writer.begin(&[0]))?;
        let mut updated = db(writer.read(0))?;
        updated.callsign = 20;
        db(writer.write(updated))?;
        let mut before = Adapter::connect(url, schema)?;
        db(before.begin(&[]))?;
        let pre_row = db(before.read(0))?.callsign;
        let pre_new = db(before.query(&candidate(20)))?.len();
        db(before.commit())?;
        db(writer.commit())?;
        let mut after = Adapter::connect(url, schema)?;
        db(after.begin(&[]))?;
        let post_row = db(after.read(0))?.callsign;
        let post_new = db(after.query(&candidate(20)))?.len();
        db(after.commit())?;
        Ok(contract("committed_row_and_index_visibility",
            pre_row == 10 && pre_new == 0 && post_row == 20 && post_new == 1,
            format!("PostgreSQL publishes rows and indexes in one transaction: before COMMIT row_key={pre_row}, new-key candidates={pre_new}; after COMMIT row_key={post_row}, new-key candidates={post_new}. Expected 10/0 then 20/1; no externally split index maintenance exists.")))
    })?);
    results.push(isolated_probe(url, schema, "historical", &records(&[10]), |schema| {
        let mut reader = Adapter::connect(url, schema)?;
        db(reader.begin(&[]))?;
        let mut writer = Adapter::connect(url, schema)?;
        db(writer.begin(&[0]))?;
        let mut updated = db(writer.read(0))?;
        updated.callsign = 20;
        db(writer.write(updated))?;
        db(writer.commit())?;
        let historical = db(reader.query(&candidate(10)))?;
        let visible = db(reader.read(0))?.callsign;
        let committed = commit_outcome(&mut reader)?;
        Ok(contract("index_candidates_respect_transaction_snapshot",
            visible == 10 && (!committed || (historical.len() == 1 && historical[0].id == 0)),
            format!("PostgreSQL fixed snapshot started before a committed key move: visible_row_key={visible}, old-key candidates={}, reader_committed={committed}. Expected the old row to remain discoverable or transaction rejection.", historical.len())))
    })?);
    results.push(isolated_probe(url, schema, "savepoint", &records(&[100, 50, 0]), |schema| {
        let mut adapter = Adapter::connect(url, schema)?;
        db(adapter.begin(&[0, 1, 2]))?;
        let mut rows = records(&[90, 60, 0]);
        db(adapter.write(rows[0]))?;
        db(adapter.write(rows[1]))?;
        let outer = db(adapter.savepoint())?;
        rows[0].altitude = 1;
        rows[1].altitude = 999;
        db(adapter.write(rows[0]))?;
        db(adapter.write(rows[1]))?;
        let _inner = db(adapter.savepoint())?;
        rows[2].altitude = 777;
        db(adapter.write(rows[2]))?;
        db(adapter.rollback_to(outer))?;
        let restored = [db(adapter.read(0))?.altitude, db(adapter.read(1))?.altitude, db(adapter.read(2))?.altitude];
        db(adapter.commit())?;
        db(adapter.begin(&[0, 2]))?;
        rows[0].altitude = 456;
        rows[2].altitude = 123;
        db(adapter.write(rows[0]))?;
        db(adapter.write(rows[2]))?;
        db(adapter.abort())?;
        let final_values: Vec<_> = snapshot(url, schema)?.iter().map(|row| row.altitude).collect();
        Ok(contract("savepoint_and_whole_message_rollback", restored == [90,60,0] && final_values == vec![90,60,0],
            format!("PostgreSQL nested savepoint rollback restored={restored:?}; committed values after subsequent whole-message abort={final_values:?}. Expected [90,60,0] in both observations.")))
    })?);
    results.push(isolated_probe(url, schema, "multirow", &records(&[100, 50]), |schema| {
        let mut old = Adapter::connect(url, schema)?;
        db(old.begin(&[]))?;
        let first = db(old.read(0))?.altitude;
        let mut writer = Adapter::connect(url, schema)?;
        db(writer.begin(&[0,1]))?;
        for row in records(&[90,60]) { db(writer.write(row))?; }
        db(writer.commit())?;
        let second = db(old.read(1))?.altitude;
        db(old.abort())?;
        let mut current = Adapter::connect(url, schema)?;
        db(current.begin(&[]))?;
        let after = [db(current.read(0))?.altitude, db(current.read(1))?.altitude];
        db(current.commit())?;
        Ok(contract("multirow_table_snapshot_visibility", [first,second] == [100,50] && after == [90,60],
            format!("PostgreSQL old reader spanning a two-row transfer observed [{first},{second}]; new reader observed {after:?}. Expected [100,50] and [90,60], both conserving 150.")))
    })?);
    results.push(isolated_probe(url, schema, "skew", &records(&[1,1]), |schema| {
        let mut first = Adapter::connect(url, schema)?;
        let mut second = Adapter::connect(url, schema)?;
        db(first.begin(&[0]))?;
        db(second.begin(&[1]))?;
        let first_view = [db(first.read(0))?.altitude, db(first.read(1))?.altitude];
        let second_view = [db(second.read(0))?.altitude, db(second.read(1))?.altitude];
        let mut changed = records(&[0,0]);
        let first_wrote = write_outcome(&mut first, changed.remove(0))?;
        let second_wrote = write_outcome(&mut second, changed.remove(0))?;
        let first_committed = first_wrote && commit_outcome(&mut first)?;
        let second_committed = second_wrote && commit_outcome(&mut second)?;
        let committed = usize::from(first_committed) + usize::from(second_committed);
        let remaining: i64 = snapshot(url, schema)?.iter().map(|row| row.altitude).sum();
        Ok(contract("concrete_read_dependency_write_skew", first_view == [1,1] && second_view == [1,1] && committed == 1 && remaining == 1,
            format!("PostgreSQL overlapping transactions read both active records then deactivate different records: committed={committed}, rejected={}, remaining_active={remaining}. Expected one serialization rejection and one remaining record.", 2 - committed)))
    })?);
    Ok(results)
}
