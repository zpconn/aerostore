//! Storage contract owned by the contention workload. The deterministic
//! Extended Crucible remains independent and unchanged.
pub use crate::extended_crucible::model::{DbError, Record};
use crate::extended_crucible::model::{DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaintenanceSelection {
    #[default]
    Complete,
    Prefix,
}
impl MaintenanceSelection {
    pub fn name(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Prefix => "prefix",
        }
    }
    pub fn is_complete(&self) -> bool {
        *self == Self::Complete
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Query {
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
    /// Every due scheduled event, across all families.
    GlobalDue {
        at: i64,
    },
    /// Every expired position/output/dedup record, across all families.
    GlobalExpired {
        before: i64,
    },
    All,
    /// The first `limit` eligible events ordered by (due, id), including all
    /// earlier matches at the transaction's serialization position.
    FirstDue {
        at: i64,
        limit: usize,
    },
    /// The first `limit` eligible records ordered by (event_time, id).
    FirstExpired {
        before: i64,
        limit: usize,
    },
}
impl Query {
    pub fn validate(&self) -> Result<(), DbError> {
        match *self {
            Self::FirstDue { limit, .. } if !(1..=16).contains(&limit) => {
                Err(DbError::Fatal("due prefix limit must be in 1..=16".into()))
            }
            Self::FirstExpired { limit, .. } if !(1..=64).contains(&limit) => Err(DbError::Fatal(
                "expiry prefix limit must be in 1..=64".into(),
            )),
            _ => Ok(()),
        }
    }
    pub fn selection_limit(&self) -> Option<usize> {
        match *self {
            Self::FirstDue { limit, .. } | Self::FirstExpired { limit, .. } => Some(limit),
            _ => None,
        }
    }
    /// Adapter utility after complete eligibility and overlay reconciliation.
    /// The independent reference implementation does not use this helper.
    pub fn sort_and_limit(&self, rows: &mut Vec<Record>) {
        match self {
            Self::FirstDue { .. } => rows.sort_by_key(|row| (row.due, row.id)),
            Self::FirstExpired { .. } => rows.sort_by_key(|row| (row.event_time, row.id)),
            _ => rows.sort_by_key(|row| row.id),
        }
        if let Some(limit) = self.selection_limit() {
            rows.truncate(limit);
        }
    }
    /// Eligibility alone; membership in a bounded prefix also depends on the
    /// other eligible rows and their ordering, not just this record.
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
            Self::GlobalDue { at } | Self::FirstDue { at, .. } => {
                row.kind == SCHEDULED && row.due <= at
            }
            Self::GlobalExpired { before } | Self::FirstExpired { before, .. } => {
                row.event_time < before && matches!(row.kind, POSITION | OUTBOX | DEDUP)
            }
            Self::All => true,
        }
    }
    pub fn query(&self) -> Self {
        self.clone()
    }
}
impl From<&Query> for Query {
    fn from(query: &Query) -> Self {
        query.clone()
    }
}

pub trait Store {
    fn begin(&mut self, write_slots: &[usize]) -> Result<(), DbError>;
    fn read(&mut self, id: usize) -> Result<Record, DbError>;
    /// Return the complete result of the named query, including read-your-writes.
    /// Existing predicates remain unlimited and ordered by ID. FirstDue and
    /// FirstExpired instead promise the exact ordered prefix (time, ID): a
    /// short result means no other eligible row exists. Unordered truncation
    /// and silently limiting a complete predicate violate this contract.
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError>;
    fn write(&mut self, row: Record) -> Result<(), DbError>;
    fn savepoint(&mut self) -> Result<usize, DbError>;
    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError>;
    fn commit(&mut self) -> Result<(), DbError>;
    fn abort(&mut self) -> Result<(), DbError>;
}
