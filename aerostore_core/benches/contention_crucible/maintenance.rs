//! Scheduled maintenance jobs consist of independently committed transactions.
//! An empty complete query ends a sweep at that transaction's serialization
//! position; this does not promise an atomic snapshot of the whole sweep.
use super::model::{Message, MessageKind, Operation, Outcome, ReceiptBody};
use super::storage::Query;
use serde::{Deserialize, Serialize};

pub const JOB_ID_START: u64 = 4_000_000_000;
pub const BATCH_ID_START: u64 = 8_000_000_000;
pub const BATCH_ID_STRIDE: u64 = 4096;
pub const SWEEP_SCOPE: &str = "complete_sweep_batched_transactions";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Batch,
    Sweep,
}
impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Sweep => "sweep",
        }
    }
    pub fn is_batch(&self) -> bool {
        *self == Self::Batch
    }
}

/// Immutable job fields, especially the cutoff, survive every batch and retry.
/// IDs are disjoint from foreground and timer job IDs. Even the terminal empty
/// transaction consumes one ordinal and counts against the execution cap.
pub fn batch_message(job: &Message, index: u64) -> Result<Message, String> {
    if index >= BATCH_ID_STRIDE || !(JOB_ID_START..BATCH_ID_START).contains(&job.id) {
        return Err("invalid maintenance job ID or batch ordinal".into());
    }
    query(job)?;
    let id = (job.id - JOB_ID_START)
        .checked_mul(BATCH_ID_STRIDE)
        .and_then(|offset| BATCH_ID_START.checked_add(offset))
        .and_then(|id| id.checked_add(index))
        .ok_or("maintenance batch ID overflow")?;
    Ok(Message { id, ..job.clone() })
}

pub fn query(message: &Message) -> Result<Query, String> {
    match message.kind {
        MessageKind::GlobalProject { at, limit } if (1..=16).contains(&limit) => {
            Ok(Query::GlobalDue { at })
        }
        MessageKind::GlobalHousekeeping { before, limit } if (1..=64).contains(&limit) => {
            Ok(Query::GlobalExpired { before })
        }
        _ => Err("maintenance batch requires a supported positive bounded limit".into()),
    }
}

/// A positive limit is essential: only then does zero selected rows imply an
/// empty complete query in the existing global handlers. Full-history callers
/// must additionally validate the recorded terminal query below.
pub fn processed(message: &Message, outcome: &Outcome) -> Result<usize, String> {
    query(message)?;
    let (processed, limit) = match message.kind {
        MessageKind::GlobalProject { limit, .. } => (outcome.claimed_events, limit),
        MessageKind::GlobalHousekeeping { limit, .. } => (outcome.expired_records, limit),
        _ => unreachable!(),
    };
    if processed > limit {
        return Err("maintenance processed count exceeds its batch limit".into());
    }
    if processed == 0 && *outcome != Outcome::default() {
        return Err("terminal maintenance outcome has unexpected effects".into());
    }
    Ok(processed)
}

pub fn validate_terminal(message: &Message, body: &ReceiptBody) -> Result<(), String> {
    if processed(message, &body.outcome)? != 0 {
        return Err("nonempty maintenance batch is not a terminal probe".into());
    }
    let expected = query(message)?;
    match body.operations.as_slice() {
        [Operation::Query { query, rows }] if *query == expected && rows.is_empty() => Ok(()),
        _ => Err("terminal maintenance receipt lacks its complete empty query".into()),
    }
}
