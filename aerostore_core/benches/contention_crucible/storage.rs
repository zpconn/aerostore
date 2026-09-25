//! Storage contract owned by the contention workload. The deterministic
//! Extended Crucible remains independent and unchanged.
pub use crate::extended_crucible::model::{DbError, Record};
use crate::extended_crucible::model::{DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use serde::{Deserialize, Serialize};

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
            Self::GlobalDue { at } => row.kind == SCHEDULED && row.due <= at,
            Self::GlobalExpired { before } => {
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
    /// Return the complete predicate result, including read-your-writes. Batch
    /// limits belong in business execution after this query, never here.
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError>;
    fn write(&mut self, row: Record) -> Result<(), DbError>;
    fn savepoint(&mut self) -> Result<usize, DbError>;
    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError>;
    fn commit(&mut self) -> Result<(), DbError>;
    fn abort(&mut self) -> Result<(), DbError>;
}
