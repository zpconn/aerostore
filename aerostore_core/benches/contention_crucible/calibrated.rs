//! Partially calibrated, fixed-population workload with independent real-wall
//! maintenance timers. Dispatch selects permanent per-flight FIFO or temporary
//! input-signature affinity. TTL, alias mix, quiet quarter, source mix, retained
//! history and fixed lifetime are explicit synthetic assumptions. A timer
//! selects either one bounded batch or a job of successive batch transactions.
use super::maintenance;
use super::model::{self, CreationPolicy, Message, MessageKind};
use super::storage::Record;
use crate::extended_crucible::model::{
    initial_records as empty_records, FLIGHT, POSITION, SLOTS_PER_FAMILY,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const NANOS_PER_SECOND: u64 = 1_000_000_000;
pub const EVENT_EPOCH_NS: i64 = 1_700_000_000_000_000_000;
pub const RECORD_RETENTION_SECONDS: i64 = 3600;
pub const PROJECTION_BATCH_LIMIT: usize = 4;
pub const HOUSEKEEPING_BATCH_LIMIT: usize = 32;
pub const MAX_MAINTENANCE_BATCHES: u64 = 4096;
pub const MAX_HOUSEKEEPING_SEED_COHORTS: u64 = 3;
pub const MAINTENANCE_SCOPE: &str = "bounded_batch_not_full_sweep";
pub const DISPATCH_POLICY_VERSION: &str = "calibrated-dispatch-v1";
pub const ASSIGNMENT_FINGERPRINT_FORMAT: &str = "fnv1a64-q-u64le-owner-u64le";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Dispatch {
    #[default]
    #[serde(rename = "identity")]
    Identity,
    #[serde(rename = "signature-affinity")]
    SignatureAffinity,
}
impl Dispatch {
    pub fn name(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::SignatureAffinity => "signature-affinity",
        }
    }
    fn is_identity(&self) -> bool {
        *self == Self::Identity
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignaturePattern {
    #[default]
    Both,
    Mixed,
}
impl SignaturePattern {
    pub fn name(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Mixed => "mixed",
        }
    }
    fn is_both(&self) -> bool {
        *self == Self::Both
    }
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}

const FOREGROUND_ID_START: u64 = 1_000_000;
const MAINTENANCE_ID_START: u64 = maintenance::JOB_ID_START;

pub fn default_projection_batch_size() -> usize {
    PROJECTION_BATCH_LIMIT
}
pub fn default_housekeeping_batch_size() -> usize {
    HOUSEKEEPING_BATCH_LIMIT
}
pub fn default_max_maintenance_batches() -> u64 {
    MAX_MAINTENANCE_BATCHES
}
fn is_default_projection_batch_size(value: &usize) -> bool {
    *value == PROJECTION_BATCH_LIMIT
}
fn is_default_housekeeping_batch_size(value: &usize) -> bool {
    *value == HOUSEKEEPING_BATCH_LIMIT
}
fn is_default_max_maintenance_batches(value: &u64) -> bool {
    *value == MAX_MAINTENANCE_BATCHES
}

/// Fixed, finite retained-history cohorts are independent of admission duration,
/// rate, dispatch and worker count. Every timestamp remains before the epoch.
/// Zero labels the legacy control, which retains its exact original population.
pub fn housekeeping_seed_cohorts(config: &Config) -> u64 {
    if config.maintenance_mode == maintenance::Mode::Batch {
        0
    } else {
        MAX_HOUSEKEEPING_SEED_COHORTS.min(
            ((RECORD_RETENTION_SECONDS as u64 - 1) / config.housekeeping_interval_seconds).max(1),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub duration_ns: u64,
    pub foreground_rate: u64,
    pub foreground_workers: usize,
    pub families: usize,
    pub seed: u64,
    pub projection_interval_seconds: u64,
    pub housekeeping_interval_seconds: u64,
    #[serde(default, skip_serializing_if = "Dispatch::is_identity")]
    pub dispatch: Dispatch,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub affinity_ttl_ms: u64,
    #[serde(default, skip_serializing_if = "SignaturePattern::is_both")]
    pub signature_pattern: SignaturePattern,
    #[serde(default, skip_serializing_if = "maintenance::Mode::is_batch")]
    pub maintenance_mode: maintenance::Mode,
    #[serde(
        default = "default_projection_batch_size",
        skip_serializing_if = "is_default_projection_batch_size"
    )]
    pub projection_batch_size: usize,
    #[serde(
        default = "default_housekeeping_batch_size",
        skip_serializing_if = "is_default_housekeeping_batch_size"
    )]
    pub housekeeping_batch_size: usize,
    #[serde(
        default = "default_max_maintenance_batches",
        skip_serializing_if = "is_default_max_maintenance_batches"
    )]
    pub max_maintenance_batches: u64,
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

/// Shared input-signature router. Its API deliberately has no logical flight,
/// allocation-family, database-result or completion argument. TTL and owner
/// selection depend only on offered arrival times and visible message fields.
#[derive(Clone, Debug)]
struct SignatureRouter {
    workers: usize,
    ttl_ns: u64,
    next: usize,
    entries: BTreeMap<(i64, i64), (usize, u64)>,
}
#[derive(Clone, Copy)]
struct Decision {
    owner: usize,
    hit: bool,
    expired: bool,
    changed_owner: bool,
}
impl SignatureRouter {
    fn route(&mut self, key: (i64, i64), at: u64) -> Decision {
        let prior = self.entries.get(&key).copied();
        if let Some((owner, expires)) = prior {
            if at < expires {
                self.entries.insert(key, (owner, at + self.ttl_ns));
                return Decision {
                    owner,
                    hit: true,
                    expired: false,
                    changed_owner: false,
                };
            }
        }
        let owner = self.next;
        self.next = (self.next + 1) % self.workers;
        self.entries.insert(key, (owner, at + self.ttl_ns));
        Decision {
            owner,
            hit: false,
            expired: prior.is_some(),
            changed_owner: prior.is_some_and(|(old, _)| old != owner),
        }
    }
}
#[derive(Clone, Debug, Default)]
struct DispatchAudit {
    hits: u64,
    misses: u64,
    new_signature_misses: u64,
    expired_misses: u64,
    expired_owner_changes: u64,
    planned_flight_worker_changes: u64,
    flights_with_multiple_workers: usize,
    unique_signatures: usize,
    worker_counts: Vec<u64>,
    fingerprint: u64,
}

#[derive(Clone, Debug)]
pub struct Schedule {
    pub config: Config,
    /// Only affinity needs materialization. Each global foreground sequence is
    /// stored exactly once as u32, bounded by the 3.2-million-input limit.
    assignments: Option<Vec<Vec<u32>>>,
    audit: DispatchAudit,
}

/// Prepared by the coordinator before workers report Ready. Workers receive
/// only their own compact input sequence, never reconstruct the global router.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerSchedule {
    pub config: Config,
    pub worker: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreground_sequences: Vec<u32>,
}

pub fn validate_config(config: &Config) -> Result<(), String> {
    if !(1..=3600 * NANOS_PER_SECOND).contains(&config.duration_ns)
        || !(1..=1_000_000).contains(&config.foreground_rate)
        || !(4..=1024).contains(&config.families)
        || !(1..=32).contains(&config.foreground_workers)
        || !(1..=3600).contains(&config.projection_interval_seconds)
        || !(1..=3600).contains(&config.housekeeping_interval_seconds)
        || !(1..=16).contains(&config.projection_batch_size)
        || !(1..=64).contains(&config.housekeeping_batch_size)
        || !(1..=MAX_MAINTENANCE_BATCHES).contains(&config.max_maintenance_batches)
        || match config.dispatch {
            Dispatch::Identity => config.affinity_ttl_ms != 0,
            Dispatch::SignatureAffinity => !(1..=3_600_000).contains(&config.affinity_ttl_ms),
        }
    {
        return Err("invalid bounded calibrated schedule configuration".into());
    }
    if foreground_offered(config) > 3_200_000 {
        return Err("calibrated foreground corpus must remain bounded".into());
    }
    Ok(())
}
fn active_families(config: &Config) -> usize {
    config.families - (config.families / 4).max(1)
}
fn foreground_offered(config: &Config) -> u64 {
    (config.duration_ns as u128 * config.foreground_rate as u128).div_ceil(NANOS_PER_SECOND as u128)
        as u64
}
fn offset(config: &Config, sequence: u64) -> u64 {
    (sequence as u128 * NANOS_PER_SECOND as u128 / config.foreground_rate as u128) as u64
}
fn signature(config: &Config, sequence: u64) -> (i64, i64) {
    let active = active_families(config) as u64;
    let identity = sequence % active;
    let ordinal = sequence / active;
    let both = (100 + (identity / 4) as i64, 10_000 + identity as i64);
    match (config.signature_pattern, (ordinal / 2) % 3) {
        (SignaturePattern::Mixed, 1) => (both.0, 0),
        (SignaturePattern::Mixed, 2) => (0, both.1),
        _ => both,
    }
}
fn owned_identities(config: &Config, worker: usize) -> u64 {
    let active = active_families(config);
    if worker >= config.foreground_workers || worker >= active {
        0
    } else {
        ((active - 1 - worker) / config.foreground_workers + 1) as u64
    }
}
fn identity_prefix(config: &Config, worker: usize, prefix: u64) -> u64 {
    let active = active_families(config) as u64;
    let whole = prefix / active * owned_identities(config, worker);
    let remainder = prefix % active;
    whole
        + if remainder <= worker as u64 {
            0
        } else {
            (remainder - 1 - worker as u64) / config.foreground_workers as u64 + 1
        }
}
fn interval_ns(config: &Config, worker: usize) -> Option<u64> {
    if worker == config.foreground_workers {
        Some(config.projection_interval_seconds * NANOS_PER_SECOND)
    } else if worker == config.foreground_workers + 1 {
        Some(config.housekeeping_interval_seconds * NANOS_PER_SECOND)
    } else {
        None
    }
}
fn worker_offered(config: &Config, worker: usize, sequences: Option<&[u32]>) -> u64 {
    if worker < config.foreground_workers {
        sequences.map_or_else(
            || identity_prefix(config, worker, foreground_offered(config)),
            |s| s.len() as u64,
        )
    } else {
        interval_ns(config, worker).map_or(0, |period| (config.duration_ns - 1) / period)
    }
}
fn event_for(
    config: &Config,
    worker: usize,
    ordinal: u64,
    sequences: Option<&[u32]>,
) -> Option<ScheduledEvent> {
    if ordinal >= worker_offered(config, worker, sequences) {
        return None;
    }
    if worker < config.foreground_workers {
        let sequence = if let Some(sequences) = sequences {
            *sequences.get(ordinal as usize)? as u64
        } else {
            let owned = owned_identities(config, worker);
            let flight_ordinal = ordinal / owned;
            let identity = (ordinal % owned) as usize * config.foreground_workers + worker;
            flight_ordinal * active_families(config) as u64 + identity as u64
        };
        let identity = (sequence % active_families(config) as u64) as usize;
        let flight_ordinal = sequence / active_families(config) as u64;
        let offset_ns = offset(config, sequence);
        let id = FOREGROUND_ID_START + sequence;
        let source = 1 << (flight_ordinal % 3);
        let kind = if flight_ordinal % 16 == 0 {
            MessageKind::Plan
        } else {
            position(config.seed, id)
        };
        let mut input = message(
            id,
            identity,
            source,
            EVENT_EPOCH_NS + offset_ns as i64,
            kind,
        );
        (input.callsign, input.tail) = signature(config, sequence);
        Some(ScheduledEvent {
            offset_ns,
            class: EventClass::Foreground,
            logical_identity: Some(identity),
            foreground_ordinal: Some(flight_ordinal),
            message: input,
        })
    } else {
        let offset_ns = (ordinal + 1) * interval_ns(config, worker)?;
        let at = EVENT_EPOCH_NS + offset_ns as i64;
        let projection = worker == config.foreground_workers;
        let (class, kind) = if projection {
            (
                EventClass::Projection,
                MessageKind::GlobalProject {
                    at,
                    limit: config.projection_batch_size,
                },
            )
        } else {
            (
                EventClass::Housekeeping,
                MessageKind::GlobalHousekeeping {
                    before: at - RECORD_RETENTION_SECONDS * NANOS_PER_SECOND as i64,
                    limit: config.housekeeping_batch_size,
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
fn backlog(
    config: &Config,
    worker: usize,
    sequences: Option<&[u32]>,
    completed: u64,
    elapsed_ns: u64,
) -> u64 {
    let due = if worker < config.foreground_workers {
        let prefix = ((elapsed_ns as u128 + 1) * config.foreground_rate as u128)
            .div_ceil(NANOS_PER_SECOND as u128)
            .min(foreground_offered(config) as u128) as u64;
        sequences.map_or_else(
            || identity_prefix(config, worker, prefix),
            |s| s.partition_point(|q| (*q as u64) < prefix) as u64,
        )
    } else {
        interval_ns(config, worker).map_or(0, |period| {
            (elapsed_ns / period).min(worker_offered(config, worker, sequences))
        })
    };
    due.saturating_sub(completed)
}
impl WorkerSchedule {
    /// Checks transport shape/bounds. Coordinator receipt reconstruction is
    /// still authoritative for whether affinity indices name the right owner.
    pub fn validate(&self) -> Result<(), String> {
        validate_config(&self.config)?;
        if self.worker >= self.config.foreground_workers + 2
            || ((self.config.dispatch == Dispatch::Identity
                || self.worker >= self.config.foreground_workers)
                && !self.foreground_sequences.is_empty())
            || self
                .foreground_sequences
                .iter()
                .any(|&q| q as u64 >= foreground_offered(&self.config))
            || self
                .foreground_sequences
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err("invalid prepared calibrated worker schedule".into());
        }
        Ok(())
    }
    fn sequences(&self) -> Option<&[u32]> {
        (self.config.dispatch == Dispatch::SignatureAffinity
            && self.worker < self.config.foreground_workers)
            .then_some(self.foreground_sequences.as_slice())
    }
    pub fn offered(&self) -> u64 {
        worker_offered(&self.config, self.worker, self.sequences())
    }
    pub fn event(&self, ordinal: u64) -> Option<ScheduledEvent> {
        event_for(&self.config, self.worker, ordinal, self.sequences())
    }
    pub fn backlog(&self, completed: u64, elapsed_ns: u64) -> u64 {
        backlog(
            &self.config,
            self.worker,
            self.sequences(),
            completed,
            elapsed_ns,
        )
    }
}
impl Schedule {
    pub fn new(config: Config) -> Result<Self, String> {
        validate_config(&config)?;
        let workers = config.foreground_workers;
        let mut assignments =
            (config.dispatch == Dispatch::SignatureAffinity).then(|| vec![Vec::new(); workers]);
        let mut router = SignatureRouter {
            workers,
            ttl_ns: config.affinity_ttl_ms * 1_000_000,
            next: 0,
            entries: BTreeMap::new(),
        };
        let mut audit = DispatchAudit {
            worker_counts: vec![0; workers],
            fingerprint: 0xcbf29ce484222325,
            ..Default::default()
        };
        let mut unique = BTreeSet::new();
        // Logical identity is used only by the identity control and diagnostic
        // counters below, never passed to the signature-affinity router.
        let mut last_owner = vec![None; active_families(&config)];
        let mut owners = vec![0_u32; active_families(&config)];
        for sequence in 0..foreground_offered(&config) {
            let identity = (sequence % active_families(&config) as u64) as usize;
            let key = signature(&config, sequence);
            unique.insert(key);
            let owner = if config.dispatch == Dispatch::Identity {
                identity % workers
            } else {
                let decision = router.route(key, offset(&config, sequence));
                if decision.hit {
                    audit.hits += 1;
                } else {
                    audit.misses += 1;
                    if decision.expired {
                        audit.expired_misses += 1;
                    } else {
                        audit.new_signature_misses += 1;
                    }
                }
                audit.expired_owner_changes += u64::from(decision.changed_owner);
                decision.owner
            };
            if last_owner[identity].is_some_and(|prior| prior != owner) {
                audit.planned_flight_worker_changes += 1;
            }
            last_owner[identity] = Some(owner);
            owners[identity] |= 1_u32 << owner;
            audit.worker_counts[owner] += 1;
            if let Some(by_worker) = &mut assignments {
                by_worker[owner].push(sequence as u32);
            }
            for byte in sequence
                .to_le_bytes()
                .into_iter()
                .chain((owner as u64).to_le_bytes())
            {
                audit.fingerprint = (audit.fingerprint ^ byte as u64).wrapping_mul(0x100000001b3);
            }
        }
        audit.unique_signatures = unique.len();
        audit.flights_with_multiple_workers =
            owners.iter().filter(|bits| bits.count_ones() > 1).count();
        Ok(Self {
            config,
            assignments,
            audit,
        })
    }
    pub fn quiet_families(&self) -> usize {
        (self.config.families / 4).max(1)
    }
    pub fn active_families(&self) -> usize {
        active_families(&self.config)
    }
    pub fn worker_count(&self) -> usize {
        self.config.foreground_workers + 2
    }
    pub fn foreground_offered(&self) -> u64 {
        foreground_offered(&self.config)
    }
    pub fn maintenance_scope(&self) -> &'static str {
        match self.config.maintenance_mode {
            maintenance::Mode::Batch => MAINTENANCE_SCOPE,
            maintenance::Mode::Sweep => maintenance::SWEEP_SCOPE,
        }
    }
    pub fn cadence_in_operator_range(&self) -> bool {
        (300..=600).contains(&self.config.projection_interval_seconds)
            && (300..=600).contains(&self.config.housekeeping_interval_seconds)
    }
    fn sequences(&self, worker: usize) -> Option<&[u32]> {
        self.assignments
            .as_ref()
            .and_then(|all| all.get(worker))
            .map(Vec::as_slice)
    }
    pub fn worker_offered(&self, worker: usize) -> u64 {
        worker_offered(&self.config, worker, self.sequences(worker))
    }
    pub fn event(&self, worker: usize, ordinal: u64) -> Option<ScheduledEvent> {
        event_for(&self.config, worker, ordinal, self.sequences(worker))
    }
    pub fn backlog(&self, worker: usize, completed: u64, elapsed_ns: u64) -> u64 {
        backlog(
            &self.config,
            worker,
            self.sequences(worker),
            completed,
            elapsed_ns,
        )
    }
    pub fn worker_schedule(&self, worker: usize) -> WorkerSchedule {
        WorkerSchedule {
            config: self.config.clone(),
            worker,
            foreground_sequences: self
                .sequences(worker)
                .map_or_else(Vec::new, <[u32]>::to_vec),
        }
    }
    pub fn dispatch_report(&self) -> serde_json::Value {
        serde_json::json!({
            "policy_version":DISPATCH_POLICY_VERSION,"dispatch":self.config.dispatch.name(),
            "affinity_ttl_ms":self.config.affinity_ttl_ms,"signature_pattern":self.config.signature_pattern.name(),
            "clock":"scheduled_arrival_offset", "scope":"planned_offered_dispatch_not_execution_order",
            "hits":self.audit.hits,"misses":self.audit.misses,"new_signature_misses":self.audit.new_signature_misses,
            "expired_misses":self.audit.expired_misses,"expired_owner_changes":self.audit.expired_owner_changes,
            "planned_flight_worker_changes":self.audit.planned_flight_worker_changes,
            "flights_with_multiple_workers":self.audit.flights_with_multiple_workers,
            "unique_signatures":self.audit.unique_signatures,"worker_counts":self.audit.worker_counts,
            "assignment_fingerprint_format":ASSIGNMENT_FINGERPRINT_FORMAT,
            "assignment_fingerprint":format!("{:016x}",self.audit.fingerprint)
        })
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
                        let event_time = if self.config.maintenance_mode == maintenance::Mode::Sweep
                            && history < 7
                        {
                            // Spread finite retained history over at most three
                            // expiry ticks, independent of admission duration.
                            // Later jobs may be empty; this is not turnover.
                            let cohorts = housekeeping_seed_cohorts(&self.config);
                            let retained_index = (identity - self.active_families()) * 49
                                + (pedigree - 1) * 7
                                + history;
                            let tick = retained_index as u64 % cohorts + 1;
                            EVENT_EPOCH_NS - RECORD_RETENTION_SECONDS * NANOS_PER_SECOND as i64
                                + (tick
                                    * self.config.housekeeping_interval_seconds
                                    * NANOS_PER_SECOND) as i64
                                - 1
                        } else {
                            EVENT_EPOCH_NS - age * NANOS_PER_SECOND as i64
                        };
                        rows.insert(
                            id,
                            Record {
                                id,
                                kind: POSITION,
                                event_time,
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
        dispatch: Dispatch::Identity,
        affinity_ttl_ms: 0,
        signature_pattern: SignaturePattern::Both,
        maintenance_mode: maintenance::Mode::Batch,
        projection_batch_size: PROJECTION_BATCH_LIMIT,
        housekeeping_batch_size: HOUSEKEEPING_BATCH_LIMIT,
        max_maintenance_batches: MAX_MAINTENANCE_BATCHES,
    })?
    .initial_records()
}

/// Sweep initialization also depends on housekeeping cadence because retained
/// expiry records form explicit synthetic cohorts at its first deadlines.
pub fn initial_records_for(config: &Config) -> Result<Vec<Record>, String> {
    validate_config(config)?;
    // Initialization must not materialize the potentially large input router.
    Schedule {
        config: config.clone(),
        assignments: None,
        audit: DispatchAudit::default(),
    }
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
