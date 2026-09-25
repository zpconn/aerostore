//! Partially calibrated, fixed-population workload: per-flight foreground FIFO
//! and independent real-wall maintenance timers. The quiet quarter, source mix,
//! retained history and fixed lifetime are explicit synthetic assumptions.
//! Each timer performs one bounded batch, never an asserted complete sweep.
use super::model::{self, CreationPolicy, Message, MessageKind};
use super::storage::Record;
use crate::extended_crucible::model::{
    initial_records as empty_records, FLIGHT, POSITION, SLOTS_PER_FAMILY,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const NANOS_PER_SECOND: u64 = 1_000_000_000;
pub const EVENT_EPOCH_NS: i64 = 1_700_000_000_000_000_000;
pub const RECORD_RETENTION_SECONDS: i64 = 3600;
pub const PROJECTION_BATCH_LIMIT: usize = 4;
pub const HOUSEKEEPING_BATCH_LIMIT: usize = 32;
pub const MAINTENANCE_SCOPE: &str = "bounded_batch_not_full_sweep";
const FOREGROUND_ID_START: u64 = 1_000_000;
const MAINTENANCE_ID_START: u64 = 4_000_000_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub duration_ns: u64,
    pub foreground_rate: u64,
    pub foreground_workers: usize,
    pub families: usize,
    pub seed: u64,
    pub projection_interval_seconds: u64,
    pub housekeeping_interval_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventClass {
    Foreground,
    Projection,
    Housekeeping,
}
impl EventClass {
    pub fn name(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Projection => "projection",
            Self::Housekeeping => "housekeeping",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduledEvent {
    pub offset_ns: u64,
    pub class: EventClass,
    pub logical_identity: Option<usize>,
    pub foreground_ordinal: Option<u64>,
    pub message: Message,
}

#[derive(Clone, Debug)]
pub struct Schedule {
    pub config: Config,
}
impl Schedule {
    pub fn new(config: Config) -> Result<Self, String> {
        if !(1..=3600 * NANOS_PER_SECOND).contains(&config.duration_ns)
            || !(1..=1_000_000).contains(&config.foreground_rate)
            || !(4..=1024).contains(&config.families)
            || !(1..=32).contains(&config.foreground_workers)
            || !(1..=3600).contains(&config.projection_interval_seconds)
            || !(1..=3600).contains(&config.housekeeping_interval_seconds)
        {
            return Err("invalid bounded calibrated schedule configuration".into());
        }
        let schedule = Self { config };
        if schedule.foreground_offered() > 3_200_000 {
            return Err("calibrated foreground corpus must remain bounded".into());
        }
        Ok(schedule)
    }

    pub fn quiet_families(&self) -> usize {
        (self.config.families / 4).max(1)
    }
    pub fn active_families(&self) -> usize {
        self.config.families - self.quiet_families()
    }
    pub fn worker_count(&self) -> usize {
        self.config.foreground_workers + 2
    }
    pub fn foreground_offered(&self) -> u64 {
        (self.config.duration_ns as u128 * self.config.foreground_rate as u128)
            .div_ceil(NANOS_PER_SECOND as u128) as u64
    }
    pub fn maintenance_scope(&self) -> &'static str {
        MAINTENANCE_SCOPE
    }
    pub fn cadence_in_operator_range(&self) -> bool {
        (300..=600).contains(&self.config.projection_interval_seconds)
            && (300..=600).contains(&self.config.housekeeping_interval_seconds)
    }

    fn owned_identities(&self, worker: usize) -> u64 {
        if worker >= self.config.foreground_workers || worker >= self.active_families() {
            0
        } else {
            ((self.active_families() - 1 - worker) / self.config.foreground_workers + 1) as u64
        }
    }
    fn foreground_prefix_for_worker(&self, worker: usize, prefix: u64) -> u64 {
        let active = self.active_families() as u64;
        let whole = prefix / active * self.owned_identities(worker);
        let remainder = prefix % active;
        whole
            + if remainder <= worker as u64 {
                0
            } else {
                (remainder - 1 - worker as u64) / self.config.foreground_workers as u64 + 1
            }
    }
    fn interval_ns(&self, worker: usize) -> Option<u64> {
        if worker == self.config.foreground_workers {
            Some(self.config.projection_interval_seconds * NANOS_PER_SECOND)
        } else if worker == self.config.foreground_workers + 1 {
            Some(self.config.housekeeping_interval_seconds * NANOS_PER_SECOND)
        } else {
            None
        }
    }
    pub fn worker_offered(&self, worker: usize) -> u64 {
        if worker < self.config.foreground_workers {
            self.foreground_prefix_for_worker(worker, self.foreground_offered())
        } else {
            self.interval_ns(worker)
                .map_or(0, |interval| (self.config.duration_ns - 1) / interval)
        }
    }

    /// Deterministic FIFO input for one worker. Foreground routing establishes
    /// application arrival order, never a storage lock or declared write set.
    pub fn event(&self, worker: usize, ordinal: u64) -> Option<ScheduledEvent> {
        if ordinal >= self.worker_offered(worker) {
            return None;
        }
        if worker < self.config.foreground_workers {
            let owned = self.owned_identities(worker);
            let flight_ordinal = ordinal / owned;
            let identity = (ordinal % owned) as usize * self.config.foreground_workers + worker;
            let sequence = flight_ordinal * self.active_families() as u64 + identity as u64;
            let offset_ns = (sequence as u128 * NANOS_PER_SECOND as u128
                / self.config.foreground_rate as u128) as u64;
            let id = FOREGROUND_ID_START + sequence;
            let source = 1 << (flight_ordinal % 3);
            let kind = if flight_ordinal % 16 == 0 {
                MessageKind::Plan
            } else {
                position(self.config.seed, id)
            };
            Some(ScheduledEvent {
                offset_ns,
                class: EventClass::Foreground,
                logical_identity: Some(identity),
                foreground_ordinal: Some(flight_ordinal),
                message: message(
                    id,
                    identity,
                    source,
                    EVENT_EPOCH_NS + offset_ns as i64,
                    kind,
                ),
            })
        } else {
            let offset_ns = (ordinal + 1) * self.interval_ns(worker)?;
            let at = EVENT_EPOCH_NS + offset_ns as i64;
            let projection = worker == self.config.foreground_workers;
            let (class, kind) = if projection {
                (
                    EventClass::Projection,
                    MessageKind::GlobalProject {
                        at,
                        limit: PROJECTION_BATCH_LIMIT,
                    },
                )
            } else {
                (
                    EventClass::Housekeeping,
                    MessageKind::GlobalHousekeeping {
                        before: at - RECORD_RETENTION_SECONDS * NANOS_PER_SECOND as i64,
                        limit: HOUSEKEEPING_BATCH_LIMIT,
                    },
                )
            };
            Some(ScheduledEvent {
                offset_ns,
                class,
                logical_identity: None,
                foreground_ordinal: None,
                message: message(
                    MAINTENANCE_ID_START + ordinal * 2 + u64::from(!projection),
                    0,
                    0,
                    at,
                    kind,
                ),
            })
        }
    }

    /// Number of due but unfinished inputs, including the next waiting job.
    /// This is sampled backlog, not an instantaneous queue maximum.
    pub fn backlog(&self, worker: usize, completed: u64, elapsed_ns: u64) -> u64 {
        let due = if worker < self.config.foreground_workers {
            let prefix = ((elapsed_ns as u128 + 1) * self.config.foreground_rate as u128)
                .div_ceil(NANOS_PER_SECOND as u128)
                .min(self.foreground_offered() as u128) as u64;
            self.foreground_prefix_for_worker(worker, prefix)
        } else {
            self.interval_ns(worker).map_or(0, |interval| {
                (elapsed_ns / interval).min(self.worker_offered(worker))
            })
        };
        due.saturating_sub(completed)
    }

    /// Population is deliberately fixed for this bounded trial. Three source
    /// observations establish provenance; quiet families also retain seven
    /// obsolete positions and one recent position per view. Seed work is not
    /// timed and does not claim an empirical HyperFeed age distribution.
    pub fn initial_records(&self) -> Result<Vec<Record>, String> {
        let mut rows: BTreeMap<_, _> = empty_records(self.config.families * 2)
            .into_iter()
            .map(|row| (row.id, row))
            .collect();
        for identity in 0..self.config.families {
            let quiet = identity >= self.active_families();
            for (offset, source) in [1, 2, 4].into_iter().enumerate() {
                let id = (identity * 3 + offset + 1) as u64;
                let age = if quiet {
                    63 - offset as i64
                } else {
                    3 - offset as i64
                };
                let mut seed = message(
                    id,
                    identity,
                    source,
                    EVENT_EPOCH_NS - age * NANOS_PER_SECOND as i64,
                    position(self.config.seed, id),
                );
                seed.creation = CreationPolicy::AdHoc;
                let receipt = model::serial_apply(&mut rows, &seed).map_err(|e| e.to_string())?;
                if receipt.outcome.created_views + receipt.outcome.updated_views == 0 {
                    return Err("calibrated seed did not establish provenance".into());
                }
            }
            if quiet {
                let family = identity * 2;
                for pedigree in 1..=7 {
                    let flight = rows[&(family * SLOTS_PER_FAMILY + pedigree - 1)];
                    debug_assert_eq!(flight.kind, FLIGHT);
                    for history in 0..8 {
                        let id = family * SLOTS_PER_FAMILY + 7 + (pedigree - 1) * 8 + history;
                        let age = if history == 7 {
                            60
                        } else {
                            7200 + history as i64 * 60
                        };
                        rows.insert(
                            id,
                            Record {
                                id,
                                kind: POSITION,
                                event_time: EVENT_EPOCH_NS - age * NANOS_PER_SECOND as i64,
                                sequence: (20_000 + identity * 100 + pedigree * 8 + history) as i64,
                                revision: rows[&id].revision + 1,
                                ..flight
                            },
                        );
                    }
                }
            }
        }
        let rows: Vec<_> = rows.into_values().collect();
        model::validate_snapshot(&rows)?;
        Ok(rows)
    }
}

/// Shared initialization for local and remote owners. Only family count and
/// seed determine the population; timer/rate/worker settings do not affect it.
pub fn initial_records(families: usize, seed: u64) -> Result<Vec<Record>, String> {
    Schedule::new(Config {
        duration_ns: NANOS_PER_SECOND,
        foreground_rate: 1,
        foreground_workers: 1,
        families,
        seed,
        projection_interval_seconds: 300,
        housekeeping_interval_seconds: 600,
    })?
    .initial_records()
}

fn message(id: u64, identity: usize, source: i64, at: i64, kind: MessageKind) -> Message {
    Message {
        id,
        allocation_family: identity * 2,
        callsign: 100 + (identity / 4) as i64,
        tail: 10_000 + identity as i64,
        origin: 20 + (identity / 4) as i64,
        destination: 1000 + identity as i64,
        scheduled: 1_700_001_000,
        event_time: at,
        event_time_units_per_second: NANOS_PER_SECOND as i64,
        source,
        kind,
        creation: CreationPolicy::ExistingOnly,
    }
}

fn position(seed: u64, id: u64) -> MessageKind {
    let latitude = seed.wrapping_add(id.wrapping_mul(0x9e3779b97f4a7c15)) % 1000;
    let longitude = seed.wrapping_mul(31).wrapping_add(id) % 1000;
    MessageKind::Position {
        latitude: 30_000_000 + latitude as i64,
        longitude: -97_000_000 + longitude as i64,
        altitude: 20_000 + (id % 100) as i64,
        ground_speed: 450,
    }
}
