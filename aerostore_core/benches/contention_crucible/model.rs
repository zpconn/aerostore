//! Query-discovered synthetic HyperFeed work. Physical slots are bounded;
//! identity, family, and actual writes are discovered after the snapshot starts.
//! This complements, rather than replaces, the deterministic pedigree fixture.
pub use super::storage::{Query, Store};
use crate::extended_crucible::model::{
    DbError, Emission, Record, AIRBORNE, ARRIVED, DEDUP, FLIGHT, OUTBOX, PLANNED, POSITION,
    PROJECTED, SCHEDULED, SLOTS_PER_FAMILY,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageKind {
    Plan,
    Position {
        latitude: i64,
        longitude: i64,
        altitude: i64,
        ground_speed: i64,
    },
    Project {
        at: i64,
    },
    Housekeeping {
        before: i64,
    },
    Rename {
        callsign: i64,
    },
    Arrival,
    GlobalProject {
        at: i64,
        limit: usize,
    },
    GlobalCancel {
        at: i64,
        limit: usize,
    },
    GlobalReschedule {
        at: i64,
        due: i64,
        limit: usize,
    },
    GlobalHousekeeping {
        before: i64,
        limit: usize,
    },
    /// Explicit physical-family maintenance key, not an allocation hint.
    ExpireFamily {
        family: usize,
        before: i64,
        /// Optional longer inactivity TTL, independent of terminal status.
        /// Every view must precede this cutoff; fresh late activity retains it.
        #[serde(default)]
        stale_before: Option<i64>,
    },
}
impl MessageKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Position { .. } => "position",
            Self::Project { .. } => "projection",
            Self::Housekeeping { .. } => "housekeeping",
            Self::Rename { .. } => "key_move",
            Self::Arrival => "arrival",
            Self::GlobalProject { .. } => "global_projection",
            Self::GlobalCancel { .. } => "global_cancel",
            Self::GlobalReschedule { .. } => "global_reschedule",
            Self::GlobalHousekeeping { .. } => "global_housekeeping",
            Self::ExpireFamily { .. } => "expire_family",
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CreationPolicy {
    /// Preserve the original fixture's ad-hoc creation/missed-query checks.
    #[default]
    AdHoc,
    /// Delayed observations must not recreate an already expired identity.
    ExistingOnly,
    /// New generations may defer while their reserved physical space is live.
    IfVacant,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: u64,
    /// Reserved physical space used ONLY following an empty identity query.
    /// It does not identify the family that an existing-flight message updates.
    pub allocation_family: usize,
    pub callsign: i64,
    pub tail: i64,
    pub origin: i64,
    pub destination: i64,
    pub scheduled: i64,
    pub event_time: i64,
    /// `event_time`, due times, and expiry cutoffs use this scale; immutable
    /// `scheduled` identity evidence remains in seconds. Legacy bytes omit it.
    #[serde(
        default = "event_time_units_default",
        skip_serializing_if = "event_time_units_are_seconds"
    )]
    pub event_time_units_per_second: i64,
    pub source: i64,
    pub kind: MessageKind,
    #[serde(default)]
    pub creation: CreationPolicy,
}
fn event_time_units_default() -> i64 {
    1
}
fn event_time_units_are_seconds(units: &i64) -> bool {
    *units == 1
}
impl Message {
    fn time_units(&self) -> Result<i64, DbError> {
        match self.event_time_units_per_second {
            1 | 1_000_000_000 => Ok(self.event_time_units_per_second),
            _ => Err(DbError::Fatal("unsupported event-time units".into())),
        }
    }
    fn next_due(&self, at: i64) -> Result<i64, DbError> {
        let delay = self
            .time_units()?
            .checked_mul(30)
            .ok_or_else(|| DbError::Fatal("event-time delay overflow".into()))?;
        at.checked_add(delay)
            .ok_or_else(|| DbError::Fatal("scheduled event-time overflow".into()))
    }
    fn validate_clock(&self) -> Result<(), DbError> {
        self.next_due(self.event_time)?;
        if let MessageKind::Project { at } | MessageKind::GlobalProject { at, .. } = self.kind {
            self.next_due(at)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Outcome {
    pub family: Option<i64>,
    pub duplicate: bool,
    pub created_views: usize,
    pub updated_views: usize,
    pub ignored_stale: usize,
    pub expired_records: usize,
    pub outputs: Vec<Emission>,
    pub missing_family: bool,
    pub allocation_deferred: bool,
    pub claimed_events: usize,
    pub cancelled_events: usize,
    pub rescheduled_events: usize,
    pub expired_families: usize,
}

/// Backward-compatible receipt spelling; Query now belongs to this workload.
pub type RecordedQuery = Query;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Read {
        row: Record,
    },
    Query {
        query: RecordedQuery,
        rows: Vec<Record>,
    },
    Write {
        row: Record,
    },
    Savepoint {
        id: usize,
    },
    Rollback {
        id: usize,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReceiptBody {
    pub operations: Vec<Operation>,
    pub outcome: Outcome,
}

struct Recorder<'a, S> {
    store: &'a mut S,
    operations: Vec<Operation>,
    record: bool,
}
impl<S: Store> Store for Recorder<'_, S> {
    fn begin(&mut self, slots: &[usize]) -> Result<(), DbError> {
        self.store.begin(slots)
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        let row = self.store.read(id)?;
        if self.record {
            self.operations.push(Operation::Read { row });
        }
        Ok(row)
    }
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        let mut rows = self.store.query(query)?;
        rows.sort_by_key(|row| row.id);
        if rows.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(DbError::Fatal("query returned duplicate row ids".into()));
        }
        if self.record {
            self.operations.push(Operation::Query {
                query: query.into(),
                rows: rows.clone(),
            });
        }
        Ok(rows)
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.store.write(row)?;
        if self.record {
            self.operations.push(Operation::Write { row });
        }
        Ok(())
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        let id = self.store.savepoint()?;
        if self.record {
            self.operations.push(Operation::Savepoint { id });
        }
        Ok(id)
    }
    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        self.store.rollback_to(id)?;
        if self.record {
            self.operations.push(Operation::Rollback { id });
        }
        Ok(())
    }
    fn commit(&mut self) -> Result<(), DbError> {
        self.store.commit()
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.store.abort()
    }
}

fn slot(family: i64, offset: usize) -> usize {
    family as usize * SLOTS_PER_FAMILY + offset
}
fn put(store: &mut impl Store, mut row: Record, id: usize) -> Result<Record, DbError> {
    let previous = store.read(id)?;
    row.id = id;
    row.revision = previous.revision + 1;
    store.write(row)?;
    Ok(row)
}
fn emit(
    store: &mut impl Store,
    row: Record,
    message: &Message,
    status: i64,
    outcome: &mut Outcome,
) -> Result<(), DbError> {
    outcome.outputs.push(Emission {
        family: row.family,
        sequence: message.id as i64,
        pedigree: row.pedigree,
        status,
        event_time: message.event_time,
        latitude: row.latitude,
        longitude: row.longitude,
        altitude: row.altitude,
        source: row.source,
    });
    put(
        store,
        Record {
            kind: OUTBOX,
            sequence: message.id as i64,
            status,
            event_time: message.event_time,
            ..row
        },
        slot(
            row.family,
            70 + ((message.id as usize * 7 + row.pedigree as usize - 1) % 32),
        ),
    )?;
    Ok(())
}
fn schedule(store: &mut impl Store, row: Record, due: i64) -> Result<(), DbError> {
    put(
        store,
        Record {
            kind: SCHEDULED,
            active: row.status != ARRIVED,
            due,
            ..row
        },
        slot(row.family, 63 + row.pedigree as usize - 1),
    )?;
    Ok(())
}

pub const MAX_GLOBAL_EVENTS: usize = 16;
pub const MAX_GLOBAL_EXPIRED: usize = 64;

fn check_batch_limit(limit: usize, maximum: usize) -> Result<(), DbError> {
    if limit > maximum {
        return Err(DbError::Fatal(format!(
            "batch limit {limit} exceeds {maximum}"
        )));
    }
    Ok(())
}

fn project_event(
    store: &mut impl Store,
    event: Record,
    at: i64,
    message: &Message,
    outcome: &mut Outcome,
) -> Result<(), DbError> {
    let mut flight = store.read(slot(event.family, event.pedigree as usize - 1))?;
    if !flight.active || flight.status == ARRIVED {
        put(
            store,
            Record {
                active: false,
                ..event
            },
            event.id,
        )?;
        return Ok(());
    }
    let history = store.query(&Query::Positions {
        family: event.family,
        pedigree: event.pedigree,
    })?;
    if let Some(last) = history
        .iter()
        .max_by_key(|row| (row.event_time, row.sequence, row.id))
    {
        // Convert ticks before motion arithmetic; i128 subtraction avoids
        // wrapping when comparing timestamps far apart.
        let elapsed = ((at as i128 - last.event_time as i128) / message.time_units()? as i128)
            .clamp(0, 120) as i64;
        flight.latitude = last.latitude + elapsed * last.ground_speed / 100;
        flight.longitude = last.longitude + elapsed * last.ground_speed / 200;
        flight.source = last.source;
        emit(store, flight, message, PROJECTED, outcome)?;
    }
    schedule(store, flight, message.next_due(at)?)?;
    Ok(())
}

/// Return Some for background messages whose query/effect scope crosses
/// families or expires a complete family. No application prelocks are added.
fn background(store: &mut impl Store, message: &Message) -> Result<Option<Outcome>, DbError> {
    let mut outcome = Outcome::default();
    match message.kind {
        MessageKind::GlobalProject { at, limit }
        | MessageKind::GlobalCancel { at, limit }
        | MessageKind::GlobalReschedule { at, limit, .. } => {
            check_batch_limit(limit, MAX_GLOBAL_EVENTS)?;
            if matches!(message.kind, MessageKind::GlobalReschedule { due, .. } if due <= at) {
                return Err(DbError::Fatal(
                    "rescheduled due time must follow the claim cutoff".into(),
                ));
            }
            // The complete result is observed before selecting a bounded
            // deterministic batch. Query completeness is never size-limited.
            let mut due = store.query(&Query::GlobalDue { at })?;
            due.sort_by_key(|event| (event.due, event.id));
            for event in due.into_iter().take(limit) {
                match message.kind {
                    MessageKind::GlobalProject { .. } => {
                        project_event(store, event, at, message, &mut outcome)?;
                        outcome.claimed_events += 1;
                        outcome.rescheduled_events += 1;
                    }
                    MessageKind::GlobalCancel { .. } => {
                        put(
                            store,
                            Record {
                                active: false,
                                ..event
                            },
                            event.id,
                        )?;
                        outcome.cancelled_events += 1;
                    }
                    MessageKind::GlobalReschedule { due, .. } => {
                        put(store, Record { due, ..event }, event.id)?;
                        outcome.rescheduled_events += 1;
                    }
                    _ => unreachable!(),
                }
            }
        }
        MessageKind::GlobalHousekeeping { before, limit } => {
            check_batch_limit(limit, MAX_GLOBAL_EXPIRED)?;
            let mut expired = store.query(&Query::GlobalExpired { before })?;
            expired.sort_by_key(|row| (row.event_time, row.id));
            for row in expired.into_iter().take(limit) {
                put(
                    store,
                    Record {
                        active: false,
                        ..row
                    },
                    row.id,
                )?;
                outcome.expired_records += 1;
            }
        }
        MessageKind::ExpireFamily {
            family,
            before,
            stale_before,
        } => {
            if stale_before.is_some_and(|cutoff| cutoff >= before) {
                return Err(DbError::Fatal(
                    "stale-flight cutoff must be older than terminal cutoff".into(),
                ));
            }
            let family = i64::try_from(family)
                .map_err(|_| DbError::Fatal("family id exceeds model range".into()))?;
            outcome.family = Some(family);
            let flights = store.query(&Query::Family {
                family,
                kind: FLIGHT,
            })?;
            let terminal = flights
                .iter()
                .all(|row| row.status == ARRIVED && row.event_time < before);
            let stale = stale_before
                .is_some_and(|cutoff| flights.iter().all(|row| row.event_time < cutoff));
            if flights.is_empty() || !(terminal || stale) {
                return Ok(Some(outcome));
            }
            // All five logical kinds occupy at most one 128-slot family. Do
            // not leave a dedup/output record behind after whole-family expiry.
            let mut expired = Vec::new();
            for kind in [FLIGHT, POSITION, SCHEDULED, OUTBOX, DEDUP] {
                expired.extend(store.query(&Query::Family { family, kind })?);
            }
            if expired.len() > SLOTS_PER_FAMILY
                || expired
                    .iter()
                    .any(|row| row.id / SLOTS_PER_FAMILY != family as usize)
            {
                return Err(DbError::Fatal(
                    "family expiry escaped its bounded physical pool".into(),
                ));
            }
            for row in expired {
                put(
                    store,
                    Record {
                        active: false,
                        ..row
                    },
                    row.id,
                )?;
                outcome.expired_records += 1;
            }
            outcome.expired_families = 1;
        }
        _ => return Ok(None),
    }
    Ok(Some(outcome))
}

fn process(store: &mut impl Store, message: &Message) -> Result<Outcome, DbError> {
    message.validate_clock()?;
    if let Some(outcome) = background(store, message)? {
        return Ok(outcome);
    }
    let mut outcome = Outcome::default();
    let candidates = store.query(&Query::Candidates {
        callsign: message.callsign,
        tail: message.tail,
        scheduled: message.scheduled,
        window: 1800,
    })?;
    let families: BTreeSet<_> = candidates
        .iter()
        .filter(|row| {
            row.origin == message.origin
                && row.scheduled.abs_diff(message.scheduled) <= 300
                && ((message.tail != 0 && row.tail == message.tail)
                    || (row.callsign == message.callsign && row.destination == message.destination))
        })
        .map(|row| row.family)
        .collect();
    if families.len() > 1 {
        return Err(DbError::Fatal(
            "multiple families match one identity".into(),
        ));
    }
    let family = match families.first().copied() {
        Some(family) => family,
        None if message.creation == CreationPolicy::ExistingOnly => {
            outcome.missing_family = true;
            return Ok(outcome);
        }
        None if matches!(
            message.kind,
            MessageKind::Project { .. }
                | MessageKind::Housekeeping { .. }
                | MessageKind::Rename { .. }
                | MessageKind::Arrival
        ) =>
        {
            return Ok(outcome)
        }
        None => {
            // Different competing creators reserve different space. A missed
            // empty predicate therefore cannot be rescued by a shared row lock.
            let family = message.allocation_family as i64;
            let mut occupied = Vec::new();
            for pedigree in 1..=7 {
                let row = store.read(slot(family, pedigree - 1))?;
                if row.active {
                    occupied.push(row);
                }
            }
            if !occupied.is_empty() {
                let missed_match = occupied.iter().any(|row| {
                    row.origin == message.origin
                        && row.scheduled.abs_diff(message.scheduled) <= 300
                        && ((message.tail != 0 && row.tail == message.tail)
                            || (row.callsign == message.callsign
                                && row.destination == message.destination))
                });
                if message.creation == CreationPolicy::IfVacant && !missed_match {
                    outcome.allocation_deferred = true;
                    return Ok(outcome);
                } else {
                    return Err(DbError::Fatal(
                        "empty candidate search missed occupied allocation".into(),
                    ));
                }
            }
            family
        }
    };
    outcome.family = Some(family);
    let dedup = slot(family, 102 + message.id as usize % 26);
    let seen = store.read(dedup)?;
    if seen.active && seen.kind == DEDUP && seen.sequence == message.id as i64 {
        outcome.duplicate = true;
        return Ok(outcome);
    }
    match message.kind {
        MessageKind::Housekeeping { before } => {
            for row in store.query(&Query::Expired { family, before })? {
                put(
                    store,
                    Record {
                        active: false,
                        ..row
                    },
                    row.id,
                )?;
                outcome.expired_records += 1;
            }
        }
        MessageKind::Project { at } => {
            for event in store.query(&Query::Due { family, at })? {
                project_event(store, event, at, message, &mut outcome)?;
            }
        }
        _ => {
            if !matches!(message.source, 1 | 2 | 4) {
                return Err(DbError::Fatal(
                    "external message requires one source bit".into(),
                ));
            }
            let existing = store.query(&Query::Family {
                family,
                kind: FLIGHT,
            })?;
            let union = existing
                .iter()
                .fold(message.source, |bits, row| bits | row.pedigree);
            for pedigree in 1..=7 {
                if pedigree & !union != 0 || existing.iter().any(|row| row.pedigree == pedigree) {
                    continue;
                }
                let ancestor = existing
                    .iter()
                    .filter(|row| row.pedigree & !pedigree == 0)
                    .max_by_key(|row| (row.pedigree.count_ones(), row.pedigree));
                let row = Record {
                    pedigree,
                    parent: ancestor.map_or(-1, |row| row.id as i64),
                    ..ancestor.copied().unwrap_or(Record {
                        active: true,
                        kind: FLIGHT,
                        family,
                        callsign: message.callsign,
                        tail: message.tail,
                        origin: message.origin,
                        destination: message.destination,
                        scheduled: message.scheduled,
                        parent: -1,
                        ..Record::default()
                    })
                };
                put(store, row, slot(family, pedigree as usize - 1))?;
                outcome.created_views += 1;
            }
            for mut flight in store.query(&Query::Family {
                family,
                kind: FLIGHT,
            })? {
                if flight.pedigree & message.source == 0 {
                    continue;
                }
                if message.event_time <= flight.event_time
                    || (flight.status == ARRIVED
                        && matches!(message.kind, MessageKind::Position { .. }))
                {
                    outcome.ignored_stale += 1;
                    continue;
                }
                flight.event_time = message.event_time;
                flight.sequence = message.id as i64;
                flight.source |= message.source;
                match message.kind {
                    MessageKind::Plan => {
                        flight.status = PLANNED;
                        flight.destination = message.destination;
                    }
                    MessageKind::Position {
                        latitude,
                        longitude,
                        altitude,
                        ground_speed,
                    } => {
                        flight.status = AIRBORNE;
                        flight.latitude = latitude;
                        flight.longitude = longitude;
                        flight.altitude = altitude;
                        flight.ground_speed = ground_speed;
                        put(
                            store,
                            Record {
                                kind: POSITION,
                                ..flight
                            },
                            slot(
                                family,
                                7 + (flight.pedigree as usize - 1) * 8 + message.id as usize % 8,
                            ),
                        )?;
                    }
                    MessageKind::Rename { callsign } => {
                        flight.callsign = callsign;
                    }
                    MessageKind::Arrival => {
                        flight.status = ARRIVED;
                    }
                    _ => unreachable!(),
                }
                flight = put(store, flight, flight.id)?;
                schedule(store, flight, message.next_due(message.event_time)?)?;
                emit(store, flight, message, flight.status, &mut outcome)?;
                outcome.updated_views += 1;
            }
        }
    }
    put(
        store,
        Record {
            active: true,
            kind: DEDUP,
            family,
            sequence: message.id as i64,
            event_time: message.event_time,
            parent: -1,
            ..Record::default()
        },
        dedup,
    )?;
    Ok(outcome)
}

pub fn execute_attempt(store: &mut impl Store, message: &Message) -> Result<ReceiptBody, DbError> {
    execute_attempt_with_recording(store, message, true)
}

/// Same business execution, query normalization and cleanup in both modes.
/// False omits transcript allocations/clones; its empty operation vector is
/// never complete evidence for the serial-history oracle.
pub fn execute_attempt_with_recording(
    store: &mut impl Store,
    message: &Message,
    record: bool,
) -> Result<ReceiptBody, DbError> {
    let mut recorder = Recorder {
        store,
        operations: Vec::new(),
        record,
    };
    recorder.begin(&[])?;
    let outcome = match process(&mut recorder, message) {
        Ok(outcome) => outcome,
        Err(error) => {
            if let Err(cleanup) = recorder.abort() {
                return Err(DbError::Fatal(format!(
                    "message failed: {error}; abort failed: {cleanup}"
                )));
            }
            return Err(error);
        }
    };
    if let Err(error) = recorder.commit() {
        if let Err(cleanup) = recorder.abort() {
            return Err(DbError::Fatal(format!(
                "commit failed: {error}; abort failed: {cleanup}"
            )));
        }
        return Err(error);
    }
    Ok(ReceiptBody {
        operations: recorder.operations,
        outcome,
    })
}

pub fn execute_without_recording(
    store: &mut impl Store,
    message: &Message,
) -> Result<Outcome, DbError> {
    Ok(execute_attempt_with_recording(store, message, false)?.outcome)
}

/// Independent serial map store: no engine indexes, locks, allocation, or OCC.
/// Writes are kept in an overlay until commit; read-your-writes applies equally
/// to point and predicate queries. No possible write set is declared.
pub struct ReferenceStore<'a> {
    committed: &'a BTreeMap<usize, Record>,
    pending: Option<BTreeMap<usize, Record>>,
    pub writes: BTreeMap<usize, Record>,
    savepoints: Vec<BTreeMap<usize, Record>>,
}
impl<'a> ReferenceStore<'a> {
    pub fn new(committed: &'a BTreeMap<usize, Record>) -> Self {
        Self {
            committed,
            pending: None,
            writes: BTreeMap::new(),
            savepoints: Vec::new(),
        }
    }
}
impl Store for ReferenceStore<'_> {
    fn begin(&mut self, slots: &[usize]) -> Result<(), DbError> {
        if !slots.is_empty() || self.pending.is_some() {
            return Err(DbError::Fatal("invalid discovered-write begin".into()));
        }
        self.pending = Some(BTreeMap::new());
        self.writes.clear();
        self.savepoints.clear();
        Ok(())
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.pending
            .as_ref()
            .ok_or_else(|| DbError::Fatal("read outside transaction".into()))?
            .get(&id)
            .or_else(|| self.committed.get(&id))
            .copied()
            .ok_or_else(|| DbError::Fatal(format!("unknown slot {id}")))
    }
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| DbError::Fatal("query outside transaction".into()))?;
        Ok(self
            .committed
            .iter()
            .map(|(id, row)| pending.get(id).unwrap_or(row))
            .filter(|row| query.matches(row))
            .copied()
            .collect())
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        if !self.committed.contains_key(&row.id) {
            return Err(DbError::Fatal("write outside reserved pool".into()));
        }
        self.pending
            .as_mut()
            .ok_or_else(|| DbError::Fatal("write outside transaction".into()))?
            .insert(row.id, row);
        Ok(())
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        self.savepoints.push(
            self.pending
                .as_ref()
                .ok_or_else(|| DbError::Fatal("savepoint outside transaction".into()))?
                .clone(),
        );
        Ok(self.savepoints.len() - 1)
    }
    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        self.pending = Some(
            self.savepoints
                .get(id)
                .ok_or_else(|| DbError::Fatal("unknown savepoint".into()))?
                .clone(),
        );
        self.savepoints.truncate(id + 1);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), DbError> {
        self.writes = self
            .pending
            .take()
            .ok_or_else(|| DbError::Fatal("commit outside transaction".into()))?;
        Ok(())
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.pending = None;
        self.writes.clear();
        Ok(())
    }
}

pub fn serial_apply(
    rows: &mut BTreeMap<usize, Record>,
    message: &Message,
) -> Result<ReceiptBody, DbError> {
    let mut store = ReferenceStore::new(rows);
    let receipt = execute_attempt(&mut store, message)?;
    let writes = store.writes;
    rows.extend(writes);
    Ok(receipt)
}

/// Independent structural checks complement executable-handler replay. Pedigree
/// closure, privacy, allocation, lineage and nonorphan records come from the
/// existing fixture's checker. Here stable tail/origin/schedule evidence must
/// also agree within a family and identify at most one family. Callsigns may
/// differ across views because this workload deliberately moves indexed keys.
pub fn validate_snapshot(
    rows: &[Record],
) -> Result<crate::extended_crucible::model::InvariantReport, String> {
    let report = crate::extended_crucible::model::validate_snapshot(rows)?;
    let mut family_identity = BTreeMap::new();
    let mut identity_family = BTreeMap::new();
    for row in rows.iter().filter(|row| row.active && row.kind == FLIGHT) {
        let identity = (row.tail, row.origin, row.scheduled);
        if let Some(previous) = family_identity.insert(row.family, identity) {
            if previous != identity {
                return Err(format!(
                    "family {} contains inconsistent immutable identity evidence",
                    row.family
                ));
            }
        }
        if row.tail != 0 {
            if let Some(previous) = identity_family.insert(identity, row.family) {
                if previous != row.family {
                    return Err(format!(
                        "stable flight identity belongs to independent families {previous} and {}",
                        row.family
                    ));
                }
            }
        }
    }
    Ok(report)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub initial: Vec<Record>,
    pub messages: Vec<Message>,
    pub synchronise_first_query: bool,
}
fn noise(mut x: u64) -> i64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x % 1000) as i64
}
fn message(
    id: u64,
    identity: usize,
    allocation_family: usize,
    source: i64,
    kind: MessageKind,
) -> Message {
    Message {
        id,
        allocation_family,
        callsign: 100 + (identity / 4) as i64,
        tail: 10_000 + identity as i64,
        origin: 20 + (identity / 4) as i64,
        destination: 1000 + identity as i64,
        scheduled: 1_700_001_000,
        event_time: 1_700_000_000 + id as i64 * 10,
        event_time_units_per_second: 1,
        source,
        kind,
        creation: CreationPolicy::AdHoc,
    }
}
fn position(seed: u64, id: u64) -> MessageKind {
    MessageKind::Position {
        latitude: 30_000_000 + noise(seed ^ id),
        longitude: -97_000_000 + noise(seed.wrapping_add(id)),
        altitude: 20_000 + (id % 100) as i64,
        ground_speed: 450,
    }
}

/// Each scenario is independently initialized and permits every overlapping
/// transaction order. Allocation slots differ from the matching identity.
pub fn scenarios(seed: u64) -> Vec<Scenario> {
    let empty = crate::extended_crucible::model::initial_records(4);
    let creators = Scenario {
        name: "competing-empty-creation".into(),
        initial: empty.clone(),
        synchronise_first_query: true,
        messages: vec![
            message(1, 0, 0, 1, MessageKind::Plan),
            message(2, 0, 1, 2, position(seed, 2)),
            message(3, 0, 2, 4, position(seed, 3)),
            message(4, 0, 3, 1, MessageKind::Plan),
        ],
    };
    let mut rows: BTreeMap<_, _> = empty.into_iter().map(|row| (row.id, row)).collect();
    for (id, source) in [(1, 1), (2, 2), (3, 4)] {
        serial_apply(&mut rows, &message(id, 0, 0, source, position(seed, id))).unwrap();
    }
    let mixed = Scenario {
        name: "mixed-matching-projection-housekeeping".into(),
        initial: rows.values().copied().collect(),
        synchronise_first_query: false,
        messages: vec![
            message(10, 0, 1, 1, position(seed, 10)),
            message(11, 0, 2, 2, position(seed, 11)),
            message(12, 0, 3, 4, MessageKind::Plan),
            message(13, 0, 1, 0, MessageKind::Project { at: 1_700_000_150 }),
            message(
                14,
                0,
                2,
                0,
                MessageKind::Housekeeping {
                    before: 1_700_000_110,
                },
            ),
            message(15, 0, 3, 0, MessageKind::Project { at: 1_700_000_170 }),
        ],
    };
    let mut moved = Scenario {
        name: "indexed-key-move".into(),
        initial: rows.values().copied().collect(),
        synchronise_first_query: false,
        messages: vec![
            message(20, 0, 1, 1, MessageKind::Rename { callsign: 900 }),
            message(21, 0, 2, 2, position(seed, 21)),
            message(22, 0, 3, 4, MessageKind::Rename { callsign: 901 }),
            message(23, 0, 1, 0, MessageKind::Project { at: 1_700_000_260 }),
        ],
    };
    // This case deliberately omits stable tail evidence. Old-key observations
    // must be justified by the callsign index itself; a second index cannot
    // mask incomplete historical candidate results.
    for message in &mut moved.messages {
        message.tail = 0;
    }
    let mut scenarios = vec![creators, mixed, moved];
    scenarios.extend(global_scenarios(seed));
    scenarios
}

fn global_scenarios(seed: u64) -> Vec<Scenario> {
    let mut rows: BTreeMap<_, _> = crate::extended_crucible::model::initial_records(8)
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    for identity in 0..3 {
        for (offset, source) in [(1, 1), (2, 2), (3, 4)] {
            let id = (identity * 3 + offset) as u64;
            serial_apply(
                &mut rows,
                &message(id, identity, identity * 2, source, position(seed, id)),
            )
            .unwrap();
        }
    }
    let global = Scenario {
        name: "global-due-claim-cancel-reschedule".into(),
        initial: rows.values().copied().collect(),
        synchronise_first_query: false,
        messages: vec![
            message(
                100,
                0,
                0,
                0,
                MessageKind::GlobalProject {
                    at: 1_700_000_500,
                    limit: 8,
                },
            ),
            message(
                101,
                0,
                0,
                0,
                MessageKind::GlobalCancel {
                    at: 1_700_000_500,
                    limit: 8,
                },
            ),
            message(
                102,
                0,
                0,
                0,
                MessageKind::GlobalReschedule {
                    at: 1_700_000_500,
                    due: 1_700_000_800,
                    limit: 8,
                },
            ),
            message(103, 1, 3, 2, position(seed, 103)),
            message(
                104,
                0,
                0,
                0,
                MessageKind::GlobalHousekeeping {
                    before: 1_700_000_080,
                    limit: 12,
                },
            ),
        ],
    };
    let mut rows: BTreeMap<_, _> = crate::extended_crucible::model::initial_records(4)
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    for (id, source) in [(1, 1), (2, 2), (3, 4)] {
        serial_apply(&mut rows, &message(id, 0, 0, source, position(seed, id))).unwrap();
    }
    for (id, source) in [(10, 1), (11, 2), (12, 4)] {
        serial_apply(&mut rows, &message(id, 0, 0, source, MessageKind::Arrival)).unwrap();
    }
    let mut first = message(21, 3, 0, 1, MessageKind::Plan);
    first.scheduled += 3600;
    first.creation = CreationPolicy::IfVacant;
    let mut second = first.clone();
    second.id = 22;
    second.allocation_family = 1;
    second.event_time += 10;
    second.source = 2;
    let mut update = first.clone();
    update.id = 23;
    update.event_time += 20;
    update.kind = position(seed, 23);
    update.creation = CreationPolicy::ExistingOnly;
    let reuse = Scenario {
        name: "creation-versus-whole-family-expiry".into(),
        initial: rows.values().copied().collect(),
        synchronise_first_query: false,
        messages: vec![
            message(
                20,
                0,
                0,
                0,
                MessageKind::ExpireFamily {
                    family: 0,
                    before: 1_700_000_200,
                    stale_before: None,
                },
            ),
            first,
            second,
            update,
            message(
                24,
                0,
                0,
                0,
                MessageKind::GlobalHousekeeping {
                    before: 1_700_000_150,
                    limit: 12,
                },
            ),
            message(
                25,
                0,
                0,
                0,
                MessageKind::GlobalProject {
                    at: 1_700_000_260,
                    limit: 8,
                },
            ),
        ],
    };
    vec![global, reuse]
}

/// A single unbarriered mixed stream. Two physical allocation families per
/// identity allow competing creators; all later writes follow query results.
/// Message order is shuffled within chunks so worker overlap is not tied to
/// one global event-time ordering. Timestamps remain logical fixture time.
pub fn sustained(identities: usize, messages: usize, seed: u64) -> Scenario {
    assert!(identities > 0);
    let mut work = Vec::with_capacity(messages);
    for index in 0..messages {
        let identity = index % identities;
        let round = index / identities;
        let id = index as u64 + 1;
        let kind = if round < 2 {
            if round == 0 {
                MessageKind::Plan
            } else {
                position(seed, id)
            }
        } else {
            match round % 8 {
                0 => MessageKind::Plan,
                1 | 2 | 5 => position(seed, id),
                3 => MessageKind::Project {
                    at: 1_700_000_000 + id as i64 * 10 + 45,
                },
                4 => MessageKind::Housekeeping {
                    before: 1_700_000_000 + id as i64 * 10 - 100,
                },
                6 => MessageKind::Rename {
                    callsign: 100 + (identity / 4) as i64 + ((round / 8) % 2) as i64 * 1000,
                },
                _ => MessageKind::Project {
                    at: 1_700_000_000 + id as i64 * 10 + 60,
                },
            }
        };
        work.push(message(
            id,
            identity,
            identity * 2 + round % 2,
            1 << (round % 3),
            kind,
        ));
    }
    for chunk in work.chunks_mut(16) {
        for i in (1..chunk.len()).rev() {
            let j = noise(seed ^ chunk[i].id) as usize % (i + 1);
            chunk.swap(i, j);
        }
    }
    Scenario {
        name: "sustained-mixed".into(),
        initial: crate::extended_crucible::model::initial_records(identities * 2),
        messages: work,
        synchronise_first_query: false,
    }
}

/// Seed all three provenance sources so projection and housekeeping immediately
/// have meaningful work. These seed transactions are outside timed history.
pub fn sustained_initial(families: usize, seed: u64) -> Vec<Record> {
    assert!(families > 0);
    let mut rows: BTreeMap<_, _> = crate::extended_crucible::model::initial_records(families * 2)
        .into_iter()
        .map(|row| (row.id, row))
        .collect();
    for identity in 0..families {
        for (offset, source) in [(1, 1), (2, 2), (3, 4)] {
            let id = (identity * 3 + offset) as u64;
            serial_apply(
                &mut rows,
                &message(id, identity, identity * 2, source, position(seed, id)),
            )
            .unwrap();
        }
    }
    rows.values().copied().collect()
}

/// Stateless generation permits free-running worker processes. `sequence`
/// must uniquely identify each logical message globally (including across
/// workers). Retry the identical returned message until success. `hot_percent`
/// routes that percentage to identity zero; the remainder shares callsigns
/// with nearby identities but is disambiguated by route/tail evidence.
pub fn sustained_message(
    sequence: u64,
    worker: usize,
    workers: usize,
    families: usize,
    seed: u64,
    hot_percent: u32,
) -> Message {
    assert!(families > 0 && workers > 0 && worker < workers && hot_percent <= 100);
    let random = noise(seed ^ sequence.wrapping_mul(0x9e3779b97f4a7c15)) as usize;
    let identity = if random % 100 < hot_percent as usize {
        0
    } else {
        random % families
    };
    // Seed ids and timed ids are disjoint even when sequence starts at zero.
    let id = (families as u64 * 3 + 1000)
        .checked_add(sequence)
        .expect("message sequence overflow");
    let kind = match (sequence / workers as u64 + worker as u64) % 8 {
        0 => MessageKind::Plan,
        1 | 2 | 5 => position(seed, id),
        3 => MessageKind::Project {
            at: 1_700_000_000 + id as i64 * 10 + 45,
        },
        4 => MessageKind::Housekeeping {
            before: 1_700_000_000 + id as i64 * 10 - 100,
        },
        6 => MessageKind::Rename {
            callsign: 100
                + (identity / 4) as i64
                + ((sequence / workers as u64 / 8) % 2) as i64 * 1000,
        },
        _ => MessageKind::Project {
            at: 1_700_000_000 + id as i64 * 10 + 60,
        },
    };
    message(
        id,
        identity,
        identity * 2 + worker % 2,
        1 << ((sequence / workers as u64) % 3),
        kind,
    )
}

/// The legacy fixture keeps its exact seeded workload. Lifecycle starts with
/// empty reserved space so creation and provenance growth remain timed work.
pub fn sustained_initial_for(workload: &str, families: usize, seed: u64) -> Vec<Record> {
    match workload {
        "legacy" => sustained_initial(families, seed),
        "lifecycle" => {
            assert!(families > 0);
            crate::extended_crucible::model::initial_records(families * 2)
        }
        "fleet" => {
            assert!(families >= 16);
            let mut rows: BTreeMap<_, _> =
                crate::extended_crucible::model::initial_records(families * 2)
                    .into_iter()
                    .map(|row| (row.id, row))
                    .collect();
            for sequence in 0..16 * families as u64 {
                serial_apply(&mut rows, &fleet_message(sequence, families, seed)).unwrap();
            }
            rows.into_values().collect()
        }
        _ => panic!("unknown contention workload {workload}"),
    }
}

/// Synthetic event-time retention windows, not a claim about HyperFeed's
/// production retention policy. A missing arrival must not pin a slot forever.
pub const LIFECYCLE_TERMINAL_TTL: i64 = 5;
pub const LIFECYCLE_STALE_TTL: i64 = 600;

/// A common lifecycle corpus, independent of concurrency and worker assignment.
/// Each group of sixteen global sequence numbers supplies a new flight
/// generation, source growth, positions, terminal events, whole-family expiry
/// and global background work. Runtime ordering remains unconstrained.
///
/// Two physical slots per selected identity are reused. Creation can defer if
/// the slot is live, and late noncreation messages cannot recreate an expired
/// generation. Recurring explicit family expiry visits both slots. Terminal
/// views expire after five synthetic event-time seconds; a whole family with
/// no view activity for 600 seconds expires even if an arrival preceded its
/// delayed creation. A late update renews retention through the same complete
/// transactional family query; no execution order or wall-clock barrier is added.
pub fn sustained_message_for(
    workload: &str,
    sequence: u64,
    worker: usize,
    workers: usize,
    families: usize,
    seed: u64,
    hot_percent: u32,
) -> Message {
    if workload == "legacy" {
        return sustained_message(sequence, worker, workers, families, seed, hot_percent);
    }
    if workload == "fleet" {
        assert!(families >= 16 && hot_percent == 0);
        let offset = 16_u64
            .checked_mul(families as u64)
            .expect("fleet warmup overflow");
        return fleet_message(
            sequence
                .checked_add(offset)
                .expect("fleet sequence overflow"),
            families,
            seed,
        );
    }
    assert_eq!(workload, "lifecycle", "unknown contention workload");
    assert!(families > 0 && hot_percent <= 100);
    let generation = sequence / 16;
    let step = sequence % 16;
    let random = noise(seed ^ generation.wrapping_mul(0x9e3779b97f4a7c15)) as usize;
    let identity = if random % 100 < hot_percent as usize {
        0
    } else {
        generation as usize % families
    };
    let allocation = identity * 2 + generation as usize % 2;
    let id = sequence
        .checked_add(1_000_000)
        .expect("message sequence overflow");
    let at = 1_700_000_000 + id as i64 * 10;
    let (source, kind) = match step {
        0 => (1, MessageKind::Plan),
        1 => (1, position(seed, id)),
        2 => (2, MessageKind::Plan),
        3 => (2, position(seed, id)),
        4 => (4, MessageKind::Plan),
        5 => (4, position(seed, id)),
        6 => (
            0,
            MessageKind::GlobalProject {
                at: at + 30,
                limit: 4,
            },
        ),
        7 => (
            0,
            MessageKind::GlobalReschedule {
                at: at + 30,
                due: at + 90,
                limit: 4,
            },
        ),
        8 => (1, MessageKind::Arrival),
        9 => (2, MessageKind::Arrival),
        10 => (4, MessageKind::Arrival),
        11 => (
            0,
            MessageKind::ExpireFamily {
                family: allocation,
                before: at - LIFECYCLE_TERMINAL_TTL,
                stale_before: Some(at - LIFECYCLE_STALE_TTL),
            },
        ),
        12 => (
            0,
            MessageKind::GlobalHousekeeping {
                before: at - 90,
                limit: 32,
            },
        ),
        13 => (
            0,
            MessageKind::ExpireFamily {
                family: identity * 2 + 1 - generation as usize % 2,
                before: at - LIFECYCLE_TERMINAL_TTL,
                stale_before: Some(at - LIFECYCLE_STALE_TTL),
            },
        ),
        14 => (
            0,
            MessageKind::GlobalCancel {
                at: at + 30,
                limit: 4,
            },
        ),
        _ => (
            0,
            MessageKind::GlobalHousekeeping {
                before: at - 30,
                limit: 32,
            },
        ),
    };
    let mut message = message(id, identity, allocation, source, kind);
    message.tail = 10_000 + (generation * families as u64 + identity as u64) as i64;
    message.scheduled += generation as i64 * 3600;
    message.creation = if step == 0 {
        CreationPolicy::IfVacant
    } else {
        CreationPolicy::ExistingOnly
    };
    message
}

/// Uniform staggered fleet, distinct from the concentrated lifecycle stress
/// corpus. Every round visits each configured identity exactly once. Rotating
/// the visit order by triangular round numbers distributes message phases
/// across strided workers without changing the offered corpus across engines.
///
/// The sixteen phases stay the same, but coexist across identities; creation
/// and expiry therefore do not empty the entire database between flights. The
/// live-family count is observed, not assumed equal to configured identities.
/// Generator background/expiry windows scale with identity count because a
/// family advances one phase per round. Ordinary event scheduling remains the
/// shared handler's 30-second rule. Warmup is this exact stream's serial prefix.
pub(crate) fn fleet_message(sequence: u64, families: usize, seed: u64) -> Message {
    let count = families as u64;
    let round = sequence / count;
    let visit = sequence % count;
    let rotation = ((round as u128 * (round as u128 + 1) / 2) % count as u128) as u64;
    let identity = ((visit + rotation) % count) as usize;
    let phase_clock = round + identity as u64 % 16 + seed % 16;
    let phase = phase_clock % 16;
    let generation = phase_clock / 16;
    let allocation = identity * 2 + generation as usize % 2;
    let id = sequence
        .checked_add(1_000_000)
        .expect("fleet message id overflow");
    let at = 1_700_000_000 + id as i64 * 10;
    let scale = families as i64;
    let (source, kind) = match phase {
        0 => (1, MessageKind::Plan),
        1 => (1, position(seed, id)),
        2 => (2, MessageKind::Plan),
        3 => (2, position(seed, id)),
        4 => (4, MessageKind::Plan),
        5 => (4, position(seed, id)),
        6 => (
            0,
            MessageKind::GlobalProject {
                at: at + 30 * scale,
                limit: 4,
            },
        ),
        7 => (
            0,
            MessageKind::GlobalReschedule {
                at: at + 30 * scale,
                due: at + 90 * scale,
                limit: 4,
            },
        ),
        8 => (1, MessageKind::Arrival),
        9 => (2, MessageKind::Arrival),
        10 => (4, MessageKind::Arrival),
        11 => (
            0,
            MessageKind::ExpireFamily {
                family: allocation,
                before: at - LIFECYCLE_TERMINAL_TTL * scale,
                stale_before: Some(at - LIFECYCLE_STALE_TTL * scale),
            },
        ),
        12 => (
            0,
            MessageKind::GlobalHousekeeping {
                before: at - 90 * scale,
                limit: 32,
            },
        ),
        13 => (
            0,
            MessageKind::ExpireFamily {
                family: identity * 2 + 1 - generation as usize % 2,
                before: at - LIFECYCLE_TERMINAL_TTL * scale,
                stale_before: Some(at - LIFECYCLE_STALE_TTL * scale),
            },
        ),
        14 => (
            0,
            MessageKind::GlobalCancel {
                at: at + 30 * scale,
                limit: 4,
            },
        ),
        _ => (
            0,
            MessageKind::GlobalHousekeeping {
                before: at - 30 * scale,
                limit: 32,
            },
        ),
    };
    let mut message = message(id, identity, allocation, source, kind);
    message.tail = 10_000 + (generation * count + identity as u64) as i64;
    message.scheduled += generation as i64 * 3600;
    message.creation = if phase == 0 {
        CreationPolicy::IfVacant
    } else {
        CreationPolicy::ExistingOnly
    };
    message
}
