//! Synthetic HyperFeed transaction model. This is a specification fixture, not
//! FlightAware's matching algorithm. Every assumption is documented by the runner.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const SLOTS_PER_FAMILY: usize = 128;
pub const FLIGHT: i64 = 1;
pub const POSITION: i64 = 2;
pub const SCHEDULED: i64 = 3;
pub const OUTBOX: i64 = 4;
pub const DEDUP: i64 = 5;
pub const POSITION_DEPTH: usize = 8;
const POSITION_START: usize = 7;
const SCHEDULE_START: usize = 63;
const OUTBOX_START: usize = 70;
const OUTBOX_DEPTH: usize = 32;
const DEDUP_START: usize = 102;
const DEDUP_DEPTH: usize = 26;
pub const PLANNED: i64 = 1;
pub const AIRBORNE: i64 = 2;
pub const ARRIVED: i64 = 3;
pub const CANCELLED: i64 = 4;
pub const DIVERTED: i64 = 5;
pub const PROJECTED: i64 = 6;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub id: usize,
    pub active: bool,
    pub kind: i64,
    pub family: i64,
    pub pedigree: i64,
    pub callsign: i64,
    pub tail: i64,
    pub origin: i64,
    pub destination: i64,
    pub scheduled: i64,
    pub event_time: i64,
    pub due: i64,
    pub latitude: i64,
    pub longitude: i64,
    pub altitude: i64,
    pub ground_speed: i64,
    pub status: i64,
    pub revision: i64,
    /// Provenance actually incorporated. Must be a subset of `pedigree`.
    pub source: i64,
    /// Synthetic clone lineage, not HyperFeed's all-provenance family parent.
    pub parent: i64,
    pub sequence: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Query {
    /// Identifier evidence plus a deliberately broad scheduling window. The
    /// handler performs route/tail disambiguation after fetching candidates.
    Candidates {
        callsign: i64,
        tail: i64,
        scheduled: i64,
        window: i64,
    },
    Family {
        family: i64,
        kind: i64,
    },
    Positions {
        family: i64,
        pedigree: i64,
    },
    Due {
        family: i64,
        at: i64,
    },
    Expired {
        family: i64,
        before: i64,
    },
    #[allow(dead_code)] // Available to adapter diagnostics outside the timed trace.
    All,
}
impl Query {
    pub fn matches(&self, row: &Record) -> bool {
        if !row.active {
            return false;
        }
        match *self {
            Self::Candidates {
                callsign,
                tail,
                scheduled,
                window,
            } => {
                row.kind == FLIGHT
                    && (row.callsign == callsign || (tail != 0 && row.tail == tail))
                    && row.scheduled.abs_diff(scheduled) <= window as u64
            }
            Self::Family { family, kind } => row.family == family && row.kind == kind,
            Self::Positions { family, pedigree } => {
                row.kind == POSITION && row.family == family && row.pedigree == pedigree
            }
            Self::Due { family, at } => {
                row.kind == SCHEDULED && row.family == family && row.due <= at
            }
            Self::Expired { family, before } => {
                row.family == family
                    && row.event_time < before
                    && matches!(row.kind, POSITION | OUTBOX | DEDUP)
            }
            Self::All => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DbError {
    Conflict,
    Fatal(String),
}
impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict => write!(f, "transaction conflict"),
            Self::Fatal(s) => write!(f, "{s}"),
        }
    }
}
impl std::error::Error for DbError {}

pub trait Store {
    /// A bounded fixture can declare its possible writes before its snapshot.
    /// This is not predicate locking and does not provide general serializability.
    fn begin(&mut self, write_slots: &[usize]) -> Result<(), DbError>;
    fn read(&mut self, id: usize) -> Result<Record, DbError>;
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError>;
    fn write(&mut self, row: Record) -> Result<(), DbError>;
    fn savepoint(&mut self) -> Result<usize, DbError>;
    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError>;
    fn commit(&mut self) -> Result<(), DbError>;
    fn abort(&mut self) -> Result<(), DbError>;
}

pub fn initial_records(families: usize) -> Vec<Record> {
    (0..families * SLOTS_PER_FAMILY)
        .map(|id| Record {
            id,
            family: (id / SLOTS_PER_FAMILY) as i64,
            parent: -1,
            ..Record::default()
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageKind {
    Plan,
    Position {
        latitude: i64,
        longitude: i64,
        altitude: i64,
        ground_speed: i64,
    },
    Divert {
        destination: i64,
    },
    Arrival,
    Cancel,
    Project {
        at: i64,
    },
    Housekeeping {
        before: i64,
    },
    /// Synthetic retention rule: remove all records only when every pedigree
    /// view is terminal and its latest accepted event predates the threshold.
    ExpireFamily {
        before: i64,
    },
    /// Simulates a failed creation of additional pedigree views, caught by a
    /// savepoint. The attempted fork and its buffered output must disappear.
    FailedFork,
    /// Deliberately writes state and output, then aborts the entire message.
    AbortAfterWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Fixture allocation hint only. Candidate matching never compares it.
    pub family: usize,
    pub sequence: i64,
    pub source: i64,
    pub callsign: i64,
    pub tail: i64,
    pub origin: i64,
    pub destination: i64,
    pub scheduled: i64,
    pub event_time: i64,
    pub kind: MessageKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Emission {
    pub family: i64,
    pub sequence: i64,
    pub pedigree: i64,
    pub status: i64,
    pub event_time: i64,
    pub latitude: i64,
    pub longitude: i64,
    pub altitude: i64,
    pub source: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Outcome {
    pub duplicate: bool,
    pub rejected: bool,
    pub aborted: bool,
    pub savepoint_rollback: bool,
    pub created_views: usize,
    pub updated_views: usize,
    pub ignored_stale: usize,
    pub expired_records: usize,
    pub outputs: Vec<Emission>,
}

pub fn mutation_slots(message: &Message) -> Vec<usize> {
    (message.family * SLOTS_PER_FAMILY..(message.family + 1) * SLOTS_PER_FAMILY).collect()
}

fn slot(family: i64, offset: usize) -> usize {
    family as usize * SLOTS_PER_FAMILY + offset
}
fn flight_slot(family: i64, pedigree: i64) -> usize {
    slot(family, pedigree as usize - 1)
}

fn put_in_slot(store: &mut impl Store, mut row: Record, id: usize) -> Result<Record, DbError> {
    let old = store.read(id)?;
    row.id = id;
    row.revision = old.revision + 1;
    store.write(row)?;
    Ok(row)
}

fn emit(
    store: &mut impl Store,
    flight: Record,
    message: &Message,
    status: i64,
    outcome: &mut Outcome,
) -> Result<(), DbError> {
    let output = Emission {
        family: flight.family,
        sequence: message.sequence,
        pedigree: flight.pedigree,
        status,
        event_time: message.event_time,
        latitude: flight.latitude,
        longitude: flight.longitude,
        altitude: flight.altitude,
        source: flight.source,
    };
    let id = slot(
        flight.family,
        OUTBOX_START
            + ((message.sequence as usize * 7 + flight.pedigree as usize - 1) % OUTBOX_DEPTH),
    );
    put_in_slot(
        store,
        Record {
            kind: OUTBOX,
            sequence: message.sequence,
            status,
            event_time: message.event_time,
            ..flight
        },
        id,
    )?;
    outcome.outputs.push(output);
    Ok(())
}

fn dedup_slot(message: &Message) -> usize {
    slot(
        message.family as i64,
        DEDUP_START + message.sequence as usize % DEDUP_DEPTH,
    )
}
fn mark_seen(store: &mut impl Store, message: &Message) -> Result<(), DbError> {
    put_in_slot(
        store,
        Record {
            active: true,
            kind: DEDUP,
            family: message.family as i64,
            sequence: message.sequence,
            event_time: message.event_time,
            parent: -1,
            ..Record::default()
        },
        dedup_slot(message),
    )?;
    Ok(())
}

fn find_family(store: &mut impl Store, message: &Message) -> Result<Option<i64>, DbError> {
    let candidates = store.query(&Query::Candidates {
        callsign: message.callsign,
        tail: message.tail,
        scheduled: message.scheduled,
        window: 1800,
    })?;
    let families: BTreeSet<i64> = candidates
        .into_iter()
        .filter(|row| {
            // Tail and origin are strong evidence; when a tail is absent require
            // both route endpoints. This deliberately modest rule is synthetic.
            row.origin == message.origin
                && ((message.tail != 0 && row.tail == message.tail)
                    || (row.callsign == message.callsign && row.destination == message.destination))
                && row.scheduled.abs_diff(message.scheduled) <= 300
        })
        .map(|row| row.family)
        .collect();
    match families.len() {
        0 => Ok(None),
        1 => Ok(families.iter().next().copied()),
        _ => Err(DbError::Fatal(
            "synthetic matcher found ambiguous independent families".into(),
        )),
    }
}

fn blank_flight(message: &Message, family: i64) -> Record {
    Record {
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
    }
}

fn ensure_views(
    store: &mut impl Store,
    message: &Message,
    family: i64,
    outcome: &mut Outcome,
    fail_after_first: bool,
) -> Result<bool, DbError> {
    let existing = store.query(&Query::Family {
        family,
        kind: FLIGHT,
    })?;
    let observed = existing
        .iter()
        .fold(message.source, |bits, row| bits | row.pedigree);
    for pedigree in 1..=7 {
        if pedigree & !observed != 0 || existing.iter().any(|row| row.pedigree == pedigree) {
            continue;
        }
        // Clone the largest already existing allowed subset. A disjoint new
        // source starts with identity only, never inherited restricted state.
        let ancestor = existing
            .iter()
            .filter(|row| row.pedigree & !pedigree == 0)
            .max_by_key(|row| row.pedigree.count_ones());
        let mut row = ancestor
            .copied()
            .unwrap_or_else(|| blank_flight(message, family));
        row.pedigree = pedigree;
        row.parent = ancestor.map_or(-1, |parent| parent.id as i64);
        row = put_in_slot(store, row, flight_slot(family, pedigree))?;
        outcome.created_views += 1;
        if fail_after_first {
            // Intentionally exercise both physical writes and output buffering
            // before the caller rolls this partial operation back.
            emit(store, row, message, PLANNED, outcome)?;
            return Ok(false);
        }
    }
    Ok(true)
}

fn schedule(store: &mut impl Store, flight: Record, due: i64) -> Result<(), DbError> {
    let id = slot(flight.family, SCHEDULE_START + flight.pedigree as usize - 1);
    put_in_slot(
        store,
        Record {
            active: !matches!(flight.status, ARRIVED | CANCELLED),
            kind: SCHEDULED,
            due,
            ..flight
        },
        id,
    )?;
    Ok(())
}

fn process_projection(
    store: &mut impl Store,
    message: &Message,
    at: i64,
    outcome: &mut Outcome,
) -> Result<(), DbError> {
    for event in store.query(&Query::Due {
        family: message.family as i64,
        at,
    })? {
        let mut flight = store.read(flight_slot(event.family, event.pedigree))?;
        if !flight.active || matches!(flight.status, ARRIVED | CANCELLED) {
            store.write(Record {
                active: false,
                revision: event.revision + 1,
                ..event
            })?;
            continue;
        }
        let history = store.query(&Query::Positions {
            family: event.family,
            pedigree: event.pedigree,
        })?;
        if let Some(last) = history.iter().max_by_key(|row| row.event_time) {
            let elapsed = (at - last.event_time).clamp(0, 120);
            // A deterministic integer surrogate for geographic projection.
            // It is not intended to model FlightAware's prediction algorithm.
            flight.latitude = last.latitude + elapsed * last.ground_speed / 100;
            flight.longitude = last.longitude + elapsed * last.ground_speed / 200;
            flight.source = last.source;
            emit(store, flight, message, PROJECTED, outcome)?;
        }
        schedule(store, flight, at + 30)?;
    }
    Ok(())
}

fn process_message(store: &mut impl Store, message: &Message) -> Result<Outcome, DbError> {
    let mut outcome = Outcome::default();
    let seen = store.read(dedup_slot(message))?;
    if seen.active && seen.kind == DEDUP && seen.sequence == message.sequence {
        outcome.duplicate = true;
        return Ok(outcome);
    }
    match message.kind {
        MessageKind::ExpireFamily { before } => {
            let family = message.family as i64;
            let flights = store.query(&Query::Family {
                family,
                kind: FLIGHT,
            })?;
            if flights.is_empty()
                || flights.iter().any(|row| {
                    !matches!(row.status, ARRIVED | CANCELLED) || row.event_time >= before
                })
            {
                return Ok(outcome);
            }
            // The entire related family, including pending events and output
            // records, is removed in one transaction. No dedup record is added
            // after deletion; rerunning this maintenance operation is a no-op.
            for kind in [FLIGHT, POSITION, SCHEDULED, OUTBOX, DEDUP] {
                for row in store.query(&Query::Family { family, kind })? {
                    store.write(Record {
                        active: false,
                        revision: row.revision + 1,
                        ..row
                    })?;
                    outcome.expired_records += 1;
                }
            }
            return Ok(outcome);
        }
        MessageKind::Housekeeping { before } => {
            for row in store.query(&Query::Expired {
                family: message.family as i64,
                before,
            })? {
                store.write(Record {
                    active: false,
                    revision: row.revision + 1,
                    ..row
                })?;
                outcome.expired_records += 1;
            }
            mark_seen(store, message)?;
            return Ok(outcome);
        }
        MessageKind::Project { at } => {
            process_projection(store, message, at, &mut outcome)?;
            mark_seen(store, message)?;
            return Ok(outcome);
        }
        MessageKind::Position {
            latitude,
            longitude,
            altitude,
            ..
        } if !(-90_000_000..=90_000_000).contains(&latitude)
            || !(-180_000_000..=180_000_000).contains(&longitude)
            || !(-2000..=70_000).contains(&altitude) =>
        {
            outcome.rejected = true;
            mark_seen(store, message)?;
            return Ok(outcome);
        }
        _ => {}
    }
    if !matches!(message.source, 1 | 2 | 4) {
        return Err(DbError::Fatal(
            "external message must have one of the three source bits".into(),
        ));
    }
    let family = match find_family(store, message)? {
        Some(family) => family,
        None => {
            // The allocation hint may reserve a *new* family, but must never
            // repair a missed candidate search by rediscovering existing rows.
            for pedigree in 1..=7 {
                if store
                    .read(flight_slot(message.family as i64, pedigree))?
                    .active
                {
                    return Err(DbError::Fatal(
                        "candidate lookup missed an existing allocated flight family".into(),
                    ));
                }
            }
            message.family as i64
        }
    };
    if family != message.family as i64 {
        return Err(DbError::Fatal(
            "fixture allocation hint disagrees with evidence-based candidate match".into(),
        ));
    }
    let failed_fork = matches!(message.kind, MessageKind::FailedFork);
    // An outer transaction write must survive rollback of the nested fork.
    if failed_fork {
        mark_seen(store, message)?;
    }
    let savepoint = store.savepoint()?;
    let output_len = outcome.outputs.len();
    let prior_created = outcome.created_views;
    let completed = ensure_views(store, message, family, &mut outcome, failed_fork)?;
    if failed_fork {
        if completed {
            return Err(DbError::Fatal(
                "failed-fork fixture did not attempt a new pedigree".into(),
            ));
        }
        store.rollback_to(savepoint)?;
        outcome.outputs.truncate(output_len);
        outcome.created_views = prior_created;
        outcome.savepoint_rollback = true;
        return Ok(outcome);
    }
    for mut flight in store.query(&Query::Family {
        family,
        kind: FLIGHT,
    })? {
        if flight.pedigree & message.source == 0 {
            continue;
        }
        if message.event_time <= flight.event_time {
            outcome.ignored_stale += 1;
            continue;
        }
        // Terminal states survive delayed positions, while a newer plan can
        // deliberately begin another synthetic processing cycle.
        if matches!(flight.status, ARRIVED | CANCELLED)
            && matches!(message.kind, MessageKind::Position { .. })
        {
            outcome.ignored_stale += 1;
            continue;
        }
        flight.event_time = message.event_time;
        flight.sequence = message.sequence;
        flight.source |= message.source;
        flight.revision += 1;
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
                flight.latitude = latitude;
                flight.longitude = longitude;
                flight.altitude = altitude;
                flight.ground_speed = ground_speed;
                flight.status = AIRBORNE;
                let position_id = slot(
                    family,
                    POSITION_START
                        + (flight.pedigree as usize - 1) * POSITION_DEPTH
                        + message.sequence as usize % POSITION_DEPTH,
                );
                put_in_slot(
                    store,
                    Record {
                        kind: POSITION,
                        ..flight
                    },
                    position_id,
                )?;
            }
            MessageKind::Divert { destination } => {
                flight.status = DIVERTED;
                flight.destination = destination;
            }
            MessageKind::Arrival => flight.status = ARRIVED,
            MessageKind::Cancel => flight.status = CANCELLED,
            MessageKind::AbortAfterWrite => {
                flight.altitude = 66_666;
                flight.status = AIRBORNE;
            }
            _ => {
                return Err(DbError::Fatal(
                    "unexpected message variant in update branch".into(),
                ))
            }
        }
        store.write(flight)?;
        schedule(store, flight, message.event_time + 30)?;
        emit(store, flight, message, flight.status, &mut outcome)?;
        outcome.updated_views += 1;
    }
    mark_seen(store, message)?;
    if matches!(message.kind, MessageKind::AbortAfterWrite) {
        outcome.aborted = true;
        outcome.created_views = 0;
        outcome.updated_views = 0;
        outcome.outputs.clear();
    }
    Ok(outcome)
}

/// Execute one complete message. The runner retries this entire function on
/// Conflict. Outputs become visible to the caller only after commit succeeds.
pub fn execute_message(store: &mut impl Store, message: &Message) -> Result<Outcome, DbError> {
    store.begin(&mutation_slots(message))?;
    match process_message(store, message) {
        Ok(outcome) if outcome.aborted => {
            store.abort()?;
            Ok(outcome)
        }
        Ok(outcome) => match store.commit() {
            Ok(()) => Ok(outcome),
            Err(error) => {
                let _ = store.abort();
                Err(error)
            }
        },
        Err(error) => {
            let _ = store.abort();
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Phase {
    pub name: String,
    pub messages: Vec<Message>,
}

fn noise(seed: u64, family: usize, cycle: usize) -> i64 {
    let mut x = seed
        ^ (family as u64).wrapping_mul(0x9e3779b97f4a7c15)
        ^ (cycle as u64).wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    (x % 1000) as i64
}

/// Distinct messages within each family are separated by phase barriers.
/// The first position phase contains four identical deliveries per family;
/// they may contend concurrently, with exactly one accepted and three deduped.
/// Families share callsigns but have distinct route/tail evidence. Thus final
/// state and the multiset of outcomes for each identical-message group remain
/// deterministic even though the winning worker may differ between runs.
pub fn generate_trace(families: usize, cycles: usize, seed: u64) -> Vec<Phase> {
    let mut phases = Vec::new();
    for cycle in 0..cycles {
        let base = 1_700_000_000 + cycle as i64 * 600;
        let seq = cycle as i64 * 1000;
        let make =
            |family: usize, step: i64, offset: i64, source: i64, kind: MessageKind| Message {
                family,
                sequence: seq + step,
                source,
                callsign: 100 + (family / 4) as i64,
                tail: 10_000 + family as i64,
                origin: 20 + (family / 4) as i64,
                destination: 1000 + family as i64,
                scheduled: 1_700_001_000 + (family % 3) as i64 * 60,
                event_time: base + offset,
                kind,
            };
        let position = |family: usize, step: i64| MessageKind::Position {
            latitude: 30_000_000 + family as i64 * 100 + step * 17 + noise(seed, family, cycle),
            longitude: -97_000_000 + family as i64 * 100 + step * 11,
            altitude: 20_000 + step * 100,
            ground_speed: 400 + family as i64 % 80,
        };
        let mut add = |name: &str, build: &dyn Fn(usize) -> Message| {
            phases.push(Phase {
                name: format!("cycle-{cycle:03}-{name}"),
                messages: (0..families)
                    .flat_map(|family| {
                        let copies = if name == "position-source-1" { 4 } else { 1 };
                        std::iter::repeat(build(family)).take(copies)
                    })
                    .collect(),
            })
        };
        add("plan-or-adhoc", &|f| {
            make(
                f,
                0,
                0,
                1,
                if f % 4 == 0 {
                    position(f, 0)
                } else {
                    MessageKind::Plan
                },
            )
        });
        add("position-source-1", &|f| make(f, 1, 10, 1, position(f, 1)));
        add("duplicate-position", &|f| make(f, 1, 10, 1, position(f, 1)));
        add("delayed-position", &|f| make(f, 3, 5, 1, position(f, 3)));
        add("second-provenance", &|f| {
            make(f, 4, 20, 2, MessageKind::Plan)
        });
        add("position-source-2", &|f| make(f, 5, 30, 2, position(f, 5)));
        add("failed-fork-savepoint", &|f| {
            make(f, 6, 31, 4, MessageKind::FailedFork)
        });
        add("position-after-failed-fork", &|f| {
            make(f, 7, 35, 1, position(f, 7))
        });
        add("third-provenance", &|f| {
            make(f, 8, 40, 4, MessageKind::Plan)
        });
        add("position-source-4", &|f| make(f, 9, 50, 4, position(f, 9)));
        add("invalid-position", &|f| {
            make(
                f,
                10,
                51,
                2,
                MessageKind::Position {
                    latitude: 190_000_000,
                    longitude: 0,
                    altitude: 20_000,
                    ground_speed: 450,
                },
            )
        });
        add("whole-message-abort", &|f| {
            make(f, 11, 55, 1, MessageKind::AbortAfterWrite)
        });
        add("reschedule", &|f| make(f, 12, 60, 1, position(f, 12)));
        add("project-due", &|f| {
            make(f, 13, 95, 0, MessageKind::Project { at: base + 95 })
        });
        add("diversion", &|f| {
            make(
                f,
                14,
                100,
                2,
                MessageKind::Divert {
                    destination: 2000 + f as i64,
                },
            )
        });
        add("position-after-diversion", &|f| {
            make(f, 15, 110, 4, position(f, 15))
        });
        add("position-refresh", &|f| {
            make(f, 16, 120, 1, position(f, 16))
        });
        add("project-again", &|f| {
            make(f, 17, 160, 0, MessageKind::Project { at: base + 160 })
        });
        for (step, source) in [(18, 1), (19, 2), (20, 4)] {
            add(&format!("terminal-source-{source}"), &|f| {
                make(
                    f,
                    step,
                    170 + step - 18,
                    source,
                    if f % 3 == 0 {
                        MessageKind::Cancel
                    } else {
                        MessageKind::Arrival
                    },
                )
            });
        }
        add("cancelled-projections", &|f| {
            make(f, 21, 220, 0, MessageKind::Project { at: base + 220 })
        });
        add("position-after-terminal", &|f| {
            make(f, 22, 173, 1, position(f, 22))
        });
        add("housekeeping", &|f| {
            make(
                f,
                23,
                230,
                0,
                MessageKind::Housekeeping { before: base + 100 },
            )
        });
        add("duplicate-terminal", &|f| {
            make(
                f,
                20,
                172,
                4,
                if f % 3 == 0 {
                    MessageKind::Cancel
                } else {
                    MessageKind::Arrival
                },
            )
        });
        add("project-after-housekeeping", &|f| {
            make(f, 25, 250, 0, MessageKind::Project { at: base + 250 })
        });
        add("expire-family", &|f| {
            make(
                f,
                26,
                300,
                0,
                MessageKind::ExpireFamily { before: base + 240 },
            )
        });
    }
    phases
}

/// Simple serial specification store. It deliberately uses no production
/// indexes, epochs, allocator, OCC validation, or row layout implementation.
#[derive(Clone, Debug)]
pub struct ReferenceStore {
    committed: BTreeMap<usize, Record>,
    pending: Option<BTreeMap<usize, Record>>,
    savepoints: Vec<BTreeMap<usize, Record>>,
    declared: BTreeSet<usize>,
}
impl ReferenceStore {
    pub fn new(families: usize) -> Self {
        Self {
            committed: initial_records(families)
                .into_iter()
                .map(|row| (row.id, row))
                .collect(),
            pending: None,
            savepoints: Vec::new(),
            declared: BTreeSet::new(),
        }
    }
    pub fn snapshot(&self) -> Vec<Record> {
        self.committed.values().copied().collect()
    }
}
impl Store for ReferenceStore {
    fn begin(&mut self, write_slots: &[usize]) -> Result<(), DbError> {
        if self.pending.is_some() {
            return Err(DbError::Fatal(
                "reference transaction already active".into(),
            ));
        }
        self.pending = Some(BTreeMap::new());
        self.savepoints.clear();
        self.declared = write_slots.iter().copied().collect();
        Ok(())
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.pending
            .as_ref()
            .ok_or_else(|| DbError::Fatal("reference read outside transaction".into()))?
            .get(&id)
            .or_else(|| self.committed.get(&id))
            .copied()
            .ok_or_else(|| DbError::Fatal(format!("unallocated fixture slot {id}")))
    }
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| DbError::Fatal("reference query outside transaction".into()))?;
        Ok(self
            .committed
            .iter()
            .map(|(id, row)| pending.get(id).unwrap_or(row))
            .filter(|row| query.matches(row))
            .copied()
            .collect())
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        if !self.declared.contains(&row.id) {
            return Err(DbError::Fatal(format!(
                "write to undeclared fixture slot {}",
                row.id
            )));
        }
        if !self.committed.contains_key(&row.id) {
            return Err(DbError::Fatal("write outside preallocated fixture".into()));
        }
        self.pending
            .as_mut()
            .ok_or_else(|| DbError::Fatal("reference write outside transaction".into()))?
            .insert(row.id, row);
        Ok(())
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| DbError::Fatal("reference savepoint outside transaction".into()))?;
        self.savepoints.push(pending.clone());
        Ok(self.savepoints.len() - 1)
    }
    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError> {
        self.pending = Some(
            self.savepoints
                .get(savepoint)
                .ok_or_else(|| DbError::Fatal("invalid reference savepoint".into()))?
                .clone(),
        );
        self.savepoints.truncate(savepoint + 1);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), DbError> {
        let pending = self
            .pending
            .take()
            .ok_or_else(|| DbError::Fatal("reference commit outside transaction".into()))?;
        self.committed.extend(pending);
        self.savepoints.clear();
        self.declared.clear();
        Ok(())
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.pending = None;
        self.savepoints.clear();
        self.declared.clear();
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvariantReport {
    pub active_families: usize,
    pub active_flights: usize,
    pub active_positions: usize,
    pub active_events: usize,
    pub active_outputs: usize,
    pub active_dedup: usize,
    pub provenance_checked: usize,
}

/// Independent assertions in addition to differential replay. This does not
/// reuse the handler's matching, forking, scheduling, or update procedures.
pub fn validate_snapshot(rows: &[Record]) -> Result<InvariantReport, String> {
    let mut report = InvariantReport::default();
    let mut rows_by_id = BTreeMap::new();
    let mut flights = BTreeMap::new();
    let mut families: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    for row in rows {
        if rows_by_id.insert(row.id, row).is_some() {
            return Err(format!("duplicate physical row {}", row.id));
        }
        if row.family < 0 || row.id / SLOTS_PER_FAMILY != row.family as usize {
            return Err(format!("row {} escaped its allocated family", row.id));
        }
        if !row.active {
            continue;
        }
        if row.revision <= 0 {
            return Err(format!("active row {} has no committed revision", row.id));
        }
        if row.kind != DEDUP {
            if !(1..=7).contains(&row.pedigree) || row.source & !row.pedigree != 0 {
                return Err(format!(
                    "row {} leaks provenance: source {} pedigree {}",
                    row.id, row.source, row.pedigree
                ));
            }
            report.provenance_checked += 1;
        }
        if row.altitude == 66_666 {
            return Err(format!("aborted altitude escaped at row {}", row.id));
        }
        match row.kind {
            FLIGHT => {
                if row.id != flight_slot(row.family, row.pedigree) {
                    return Err(format!("misallocated pedigree view {}", row.id));
                }
                report.active_flights += 1;
                families.entry(row.family).or_default().insert(row.pedigree);
                if flights.insert((row.family, row.pedigree), row).is_some() {
                    return Err("duplicate logical flight view".into());
                }
            }
            POSITION => {
                report.active_positions += 1;
                let offset = row.id % SLOTS_PER_FAMILY;
                let start = POSITION_START + (row.pedigree as usize - 1) * POSITION_DEPTH;
                if !(start..start + POSITION_DEPTH).contains(&offset) {
                    return Err(format!("position {} belongs to wrong view", row.id));
                }
            }
            SCHEDULED => report.active_events += 1,
            OUTBOX => report.active_outputs += 1,
            DEDUP => report.active_dedup += 1,
            kind => return Err(format!("unknown active record kind {kind}")),
        }
    }
    report.active_families = families.len();
    for (family, pedigrees) in &families {
        let union = pedigrees.iter().fold(0, |bits, pedigree| bits | pedigree);
        let expected: BTreeSet<i64> = (1..=7).filter(|pedigree| pedigree & !union == 0).collect();
        if *pedigrees != expected {
            return Err(format!(
                "incomplete provenance power set for family {family}"
            ));
        }
    }
    for row in rows.iter().filter(|row| row.active && row.kind != DEDUP) {
        let flight = flights
            .get(&(row.family, row.pedigree))
            .ok_or_else(|| format!("orphan record {}", row.id))?;
        if row.kind == SCHEDULED && matches!(flight.status, ARRIVED | CANCELLED) {
            return Err(format!(
                "terminal flight {} retains a scheduled event",
                flight.id
            ));
        }
        if row.kind == FLIGHT && row.parent >= 0 {
            let parent = rows_by_id
                .get(&(row.parent as usize))
                .ok_or_else(|| format!("missing pedigree ancestor {}", row.parent))?;
            if !parent.active
                || parent.kind != FLIGHT
                || parent.family != row.family
                || parent.pedigree == row.pedigree
                || parent.pedigree & !row.pedigree != 0
            {
                return Err(format!("invalid pedigree ancestor for {}", row.id));
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
#[allow(dead_code)] // A harness=false bench also compiles with cfg(test).
mod tests {
    use super::*;

    fn run_through(store: &mut ReferenceStore, last_phase: usize) {
        for phase in generate_trace(1, 1, 41).into_iter().take(last_phase + 1) {
            execute_message(store, &phase.messages[0]).unwrap();
        }
    }

    #[test]
    fn serial_trace_preserves_independent_invariants_every_phase() {
        let mut store = ReferenceStore::new(8);
        let mut duplicate_count = 0;
        let mut rollback_count = 0;
        let mut rejection_count = 0;
        let mut aborted_count = 0;
        for phase in generate_trace(8, 3, 987) {
            for message in phase.messages {
                let outcome = execute_message(&mut store, &message).unwrap();
                duplicate_count += usize::from(outcome.duplicate);
                rollback_count += usize::from(outcome.savepoint_rollback);
                rejection_count += usize::from(outcome.rejected);
                aborted_count += usize::from(outcome.aborted);
            }
            validate_snapshot(&store.snapshot())
                .unwrap_or_else(|error| panic!("{}: {error}", phase.name));
        }
        let report = validate_snapshot(&store.snapshot()).unwrap();
        assert_eq!(report.active_families, 0);
        assert_eq!(report.active_flights, 0);
        assert!(store.snapshot().iter().all(|row| !row.active));
        assert_eq!(report.active_events, 0);
        assert_eq!(duplicate_count, 120);
        assert_eq!(rollback_count, 24);
        assert_eq!(rejection_count, 24);
        assert_eq!(aborted_count, 24);
    }

    #[test]
    fn duplicate_delivery_group_has_one_accepted_message_and_three_duplicates() {
        let mut store = ReferenceStore::new(3);
        let trace = generate_trace(3, 1, 41);
        for message in &trace[0].messages {
            execute_message(&mut store, message).unwrap();
        }
        let group_phase = &trace[1];
        assert_eq!(group_phase.messages.len(), 12);
        for family in 0..3 {
            let group: Vec<_> = group_phase
                .messages
                .iter()
                .filter(|message| message.family == family)
                .collect();
            assert_eq!(group.len(), 4);
            assert!(group.iter().all(|message| *message == group[0]));
            let outcomes: Vec<_> = group
                .into_iter()
                .map(|message| execute_message(&mut store, message).unwrap())
                .collect();
            assert_eq!(
                outcomes.iter().filter(|outcome| !outcome.duplicate).count(),
                1
            );
            assert_eq!(
                outcomes.iter().filter(|outcome| outcome.duplicate).count(),
                3
            );
            assert_eq!(
                outcomes
                    .iter()
                    .map(|outcome| outcome.outputs.len())
                    .sum::<usize>(),
                1
            );
            assert!(outcomes
                .iter()
                .filter(|outcome| outcome.duplicate)
                .all(|outcome| outcome.outputs.is_empty()));
        }
        validate_snapshot(&store.snapshot()).unwrap();
    }

    #[test]
    fn failed_fork_rolls_back_physical_rows_and_buffered_outputs() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 5);
        let before = store.snapshot();
        assert_eq!(
            before
                .iter()
                .filter(|row| row.active && row.kind == FLIGHT)
                .count(),
            3
        );
        let message = generate_trace(1, 1, 41)[6].messages[0].clone();
        let outcome = execute_message(&mut store, &message).unwrap();
        assert!(outcome.savepoint_rollback);
        assert!(outcome.outputs.is_empty());
        assert_eq!(outcome.created_views, 0);
        let preserved_outer_write = store.snapshot()[dedup_slot(&message)];
        assert!(preserved_outer_write.active);
        assert_eq!(preserved_outer_write.sequence, message.sequence);
        assert_eq!(preserved_outer_write.revision, 1);
        for (old, new) in before.iter().zip(store.snapshot()) {
            if old.id != dedup_slot(&message) {
                assert_eq!(*old, new, "savepoint leaked slot {}", old.id);
            }
        }
    }

    #[test]
    fn whole_message_abort_changes_neither_state_nor_output() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 10);
        let before = store.snapshot();
        let outcome =
            execute_message(&mut store, &generate_trace(1, 1, 41)[11].messages[0]).unwrap();
        assert!(outcome.aborted);
        assert!(outcome.outputs.is_empty());
        assert_eq!(before, store.snapshot());
    }

    #[test]
    fn source_one_never_changes_source_two_or_four_only_views() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 11);
        let before = store.snapshot();
        let outcome =
            execute_message(&mut store, &generate_trace(1, 1, 41)[12].messages[0]).unwrap();
        assert_eq!(outcome.updated_views, 4);
        let after = store.snapshot();
        for pedigree in [2, 4, 6] {
            assert_eq!(
                before[flight_slot(0, pedigree)],
                after[flight_slot(0, pedigree)]
            );
        }
        for pedigree in [1, 3, 5, 7] {
            assert_ne!(
                before[flight_slot(0, pedigree)],
                after[flight_slot(0, pedigree)]
            );
        }
        validate_snapshot(&after).unwrap();
    }

    #[test]
    fn independently_expected_coordinates_prove_fork_privacy() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 9);
        let rows = store.snapshot();
        let trace = generate_trace(1, 1, 41);
        for (pedigree, phase) in [(1, 7), (2, 5), (3, 7), (4, 9), (5, 9), (6, 9), (7, 9)] {
            let flight = rows[flight_slot(0, pedigree)];
            if let MessageKind::Position {
                latitude,
                longitude,
                altitude,
                ..
            } = trace[phase].messages[0].kind
            {
                assert_eq!(
                    (flight.latitude, flight.longitude, flight.altitude),
                    (latitude, longitude, altitude),
                    "restricted coordinates leaked to view {pedigree}"
                );
                assert_eq!(flight.sequence, trace[phase].messages[0].sequence);
            } else {
                panic!("expected position fixture");
            }
        }
    }

    #[test]
    fn duplicate_has_no_observable_mutation_or_output() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 1);
        let before = store.snapshot();
        let outcome =
            execute_message(&mut store, &generate_trace(1, 1, 41)[2].messages[0]).unwrap();
        assert!(outcome.duplicate);
        assert!(outcome.outputs.is_empty());
        assert_eq!(before, store.snapshot());
    }

    #[test]
    fn allocation_hint_is_not_used_to_hide_a_candidate_match() {
        let mut store = ReferenceStore::new(2);
        let trace = generate_trace(2, 1, 41);
        execute_message(&mut store, &trace[0].messages[0]).unwrap();
        let before = store.snapshot();
        let mut message = trace[1].messages[0].clone();
        message.family = 1;
        let error = execute_message(&mut store, &message).unwrap_err();
        assert!(matches!(error, DbError::Fatal(message) if message.contains("allocation hint")));
        assert_eq!(before, store.snapshot());
    }

    #[test]
    fn same_callsign_is_disambiguated_using_route_and_tail() {
        let mut store = ReferenceStore::new(4);
        let trace = generate_trace(4, 1, 41);
        for message in &trace[0].messages {
            execute_message(&mut store, message).unwrap();
        }
        let flights: Vec<_> = store
            .snapshot()
            .into_iter()
            .filter(|row| row.active && row.kind == FLIGHT)
            .collect();
        assert_eq!(flights.len(), 4);
        assert!(flights.iter().all(|row| row.callsign == 100));
        assert_eq!(
            flights
                .iter()
                .map(|row| row.family)
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
    }

    #[test]
    fn empty_candidate_query_cannot_match_through_allocation_hint() {
        struct MissingCandidates(ReferenceStore);
        impl Store for MissingCandidates {
            fn begin(&mut self, slots: &[usize]) -> Result<(), DbError> {
                self.0.begin(slots)
            }
            fn read(&mut self, id: usize) -> Result<Record, DbError> {
                self.0.read(id)
            }
            fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
                if matches!(query, Query::Candidates { .. }) {
                    Ok(Vec::new())
                } else {
                    self.0.query(query)
                }
            }
            fn write(&mut self, row: Record) -> Result<(), DbError> {
                self.0.write(row)
            }
            fn savepoint(&mut self) -> Result<usize, DbError> {
                self.0.savepoint()
            }
            fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
                self.0.rollback_to(id)
            }
            fn commit(&mut self) -> Result<(), DbError> {
                self.0.commit()
            }
            fn abort(&mut self) -> Result<(), DbError> {
                self.0.abort()
            }
        }
        let trace = generate_trace(1, 1, 41);
        let mut reference = ReferenceStore::new(1);
        execute_message(&mut reference, &trace[0].messages[0]).unwrap();
        let before = reference.snapshot();
        let mut store = MissingCandidates(reference);
        assert!(
            matches!(execute_message(&mut store, &trace[1].messages[0]), Err(DbError::Fatal(message)) if message.contains("candidate lookup missed"))
        );
        assert_eq!(before, store.0.snapshot());
    }

    #[test]
    fn projection_uses_expected_history_without_replacing_real_observation() {
        let mut store = ReferenceStore::new(1);
        let trace = generate_trace(1, 1, 41);
        run_through(&mut store, 12);
        let before = store.snapshot();
        let outcome = execute_message(&mut store, &trace[13].messages[0]).unwrap();
        assert_eq!(outcome.outputs.len(), 7);
        let after = store.snapshot();
        for pedigree in 1..=7 {
            assert_eq!(
                before[flight_slot(0, pedigree)],
                after[flight_slot(0, pedigree)]
            );
        }
        for (pedigree, phase, elapsed) in [
            (1, 12, 35),
            (2, 5, 65),
            (3, 12, 35),
            (4, 9, 45),
            (5, 12, 35),
            (6, 9, 45),
            (7, 12, 35),
        ] {
            let emission = outcome
                .outputs
                .iter()
                .find(|output| output.pedigree == pedigree)
                .unwrap();
            match trace[phase].messages[0].kind {
                MessageKind::Position {
                    latitude,
                    longitude,
                    altitude,
                    ground_speed,
                } => {
                    assert_eq!(
                        (emission.latitude, emission.longitude, emission.altitude),
                        (
                            latitude + elapsed * ground_speed / 100,
                            longitude + elapsed * ground_speed / 200,
                            altitude
                        )
                    );
                    assert_eq!(emission.status, PROJECTED);
                }
                _ => panic!("expected real position fixture"),
            }
            let event = after[slot(0, SCHEDULE_START + pedigree as usize - 1)];
            assert!(event.active);
            assert_eq!(event.due, 1_700_000_125);
        }
        for phase in trace.iter().take(21).skip(14) {
            execute_message(&mut store, &phase.messages[0]).unwrap();
        }
        let before_cancelled_project = store.snapshot();
        let cancelled = execute_message(&mut store, &trace[21].messages[0]).unwrap();
        assert!(cancelled.outputs.is_empty());
        assert!(!store
            .snapshot()
            .iter()
            .any(|row| row.active && row.kind == SCHEDULED));
        for pedigree in 1..=7 {
            assert_eq!(
                before_cancelled_project[flight_slot(0, pedigree)],
                store.snapshot()[flight_slot(0, pedigree)]
            );
        }
    }

    #[test]
    fn housekeeping_retains_exact_expected_real_position_history() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 23);
        let actual: BTreeSet<_> = store
            .snapshot()
            .into_iter()
            .filter(|row| row.active && row.kind == POSITION)
            .map(|row| (row.pedigree, row.sequence))
            .collect();
        let expected = BTreeSet::from([
            (1, 16),
            (3, 16),
            (4, 15),
            (5, 15),
            (5, 16),
            (6, 15),
            (7, 15),
            (7, 16),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn family_expiration_requires_terminal_views_and_strict_age_threshold() {
        let trace = generate_trace(1, 2, 41);
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 13);
        let before = store.snapshot();
        let mut expiration = trace[26].messages[0].clone();
        expiration.kind = MessageKind::ExpireFamily {
            before: 1_700_010_000,
        };
        assert_eq!(
            execute_message(&mut store, &expiration)
                .unwrap()
                .expired_records,
            0
        );
        assert_eq!(before, store.snapshot(), "nonterminal family must survive");
        for phase in trace.iter().take(26).skip(14) {
            execute_message(&mut store, &phase.messages[0]).unwrap();
        }
        let terminal = store.snapshot();
        expiration.kind = MessageKind::ExpireFamily {
            before: 1_700_000_172,
        };
        assert_eq!(
            execute_message(&mut store, &expiration)
                .unwrap()
                .expired_records,
            0
        );
        assert_eq!(
            terminal,
            store.snapshot(),
            "threshold equality must preserve family"
        );
        expiration.kind = MessageKind::ExpireFamily {
            before: 1_700_000_173,
        };
        let expired = execute_message(&mut store, &expiration).unwrap();
        assert_eq!(
            expired.expired_records,
            terminal.iter().filter(|row| row.active).count()
        );
        assert!(expired.outputs.is_empty());
        assert!(store.snapshot().iter().all(|row| !row.active));
        assert_eq!(
            execute_message(&mut store, &expiration)
                .unwrap()
                .expired_records,
            0
        );
        let recreated = execute_message(&mut store, &trace[27].messages[0]).unwrap();
        assert_eq!(recreated.created_views, 1);
        let flights: Vec<_> = store
            .snapshot()
            .into_iter()
            .filter(|row| row.active && row.kind == FLIGHT)
            .collect();
        assert_eq!(flights.len(), 1);
        assert_eq!(flights[0].pedigree, 1);
        assert_eq!(flights[0].source, 1);
        validate_snapshot(&store.snapshot()).unwrap();
    }

    #[test]
    fn failed_family_expiration_rolls_back_every_related_row() {
        struct FailThirdWrite {
            inner: ReferenceStore,
            writes: usize,
        }
        impl Store for FailThirdWrite {
            fn begin(&mut self, slots: &[usize]) -> Result<(), DbError> {
                self.inner.begin(slots)
            }
            fn read(&mut self, id: usize) -> Result<Record, DbError> {
                self.inner.read(id)
            }
            fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
                self.inner.query(query)
            }
            fn write(&mut self, row: Record) -> Result<(), DbError> {
                self.writes += 1;
                if self.writes == 3 {
                    Err(DbError::Fatal("injected expiration write failure".into()))
                } else {
                    self.inner.write(row)
                }
            }
            fn savepoint(&mut self) -> Result<usize, DbError> {
                self.inner.savepoint()
            }
            fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
                self.inner.rollback_to(id)
            }
            fn commit(&mut self) -> Result<(), DbError> {
                self.inner.commit()
            }
            fn abort(&mut self) -> Result<(), DbError> {
                self.inner.abort()
            }
        }
        let mut reference = ReferenceStore::new(1);
        run_through(&mut reference, 25);
        let before = reference.snapshot();
        let mut store = FailThirdWrite {
            inner: reference,
            writes: 0,
        };
        let expiration = generate_trace(1, 1, 41)[26].messages[0].clone();
        assert!(execute_message(&mut store, &expiration).is_err());
        assert_eq!(before, store.inner.snapshot());
    }

    #[test]
    fn invariant_checker_detects_provenance_leak_and_terminal_event() {
        let mut store = ReferenceStore::new(1);
        run_through(&mut store, 25);
        let mut rows = store.snapshot();
        rows[0].source |= 4;
        assert!(validate_snapshot(&rows).unwrap_err().contains("provenance"));
        rows = store.snapshot();
        let event = &mut rows[SCHEDULE_START];
        event.active = true;
        assert!(validate_snapshot(&rows).unwrap_err().contains("terminal"));
    }
}
