//! Exact serial-history witness search over complete successful transactions.
//! Response order is only a search heuristic. Overlapping transactions may be
//! serialized in either order; only non-overlapping intervals impose order.
//! Failed searches are INVALID only after exhaustive enumeration. A resource
//! limit is INCONCLUSIVE, never a successful correctness verdict.
use super::model::{execute_attempt, Message, ReceiptBody, ReferenceStore};
use crate::extended_crucible::model::Record;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub message: Message,
    /// Globally comparable monotonic ticks for the successful attempt only.
    /// Retries have no committed effects and are reported separately in metrics.
    pub started: u64,
    pub finished: u64,
    pub body: ReceiptBody,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Valid,
    Invalid,
    Inconclusive,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckResult {
    pub status: Status,
    pub order: Vec<u64>,
    pub explored: usize,
    pub detail: String,
}

struct Frame {
    candidates: Vec<usize>,
    next: usize,
}
fn frontier(
    starts: &BTreeSet<(u64, usize)>,
    finishes: &BTreeSet<(u64, usize)>,
    receipts: &[Receipt],
) -> Frame {
    let mut candidates: Vec<_> = match finishes.first() {
        None => Vec::new(),
        Some((earliest_finish, _)) => starts
            .range(..(*earliest_finish, 0))
            .map(|(_, index)| *index)
            .collect(),
    };
    // Trying earliest responses first usually finds a witness quickly, but
    // every alternative is retained and searched if that branch fails.
    candidates.sort_by_key(|index| (receipts[*index].finished, *index));
    Frame {
        candidates,
        next: 0,
    }
}
fn result(
    status: Status,
    order: &[usize],
    receipts: &[Receipt],
    explored: usize,
    detail: impl Into<String>,
) -> CheckResult {
    CheckResult {
        status,
        order: order
            .iter()
            .map(|index| receipts[*index].message.id)
            .collect(),
        explored,
        detail: detail.into(),
    }
}

/// Replay every point read, whole predicate result, write, and outcome against
/// the independent model. The final image must match as well. Enumeration is
/// iterative (no call-stack growth), with reversible changed-row journals.
/// Search width is bounded by overlapping successful transaction intervals;
/// no worker barrier or extra runtime lock is used to make checking easier.
pub fn check(
    initial: &[Record],
    receipts: &[Receipt],
    final_rows: &[Record],
    budget: usize,
) -> CheckResult {
    let mut state: BTreeMap<_, _> = initial.iter().map(|row| (row.id, *row)).collect();
    let final_state: BTreeMap<_, _> = final_rows.iter().map(|row| (row.id, *row)).collect();
    if state.len() != initial.len()
        || final_state.len() != final_rows.len()
        || !state.keys().eq(final_state.keys())
    {
        return result(
            Status::Invalid,
            &[],
            receipts,
            0,
            "initial/final row ids are duplicate or differ",
        );
    }
    if receipts
        .iter()
        .any(|receipt| receipt.started >= receipt.finished)
    {
        return result(
            Status::Invalid,
            &[],
            receipts,
            0,
            "successful attempt has invalid time interval",
        );
    }
    let mut starts: BTreeSet<_> = receipts
        .iter()
        .enumerate()
        .map(|(index, receipt)| (receipt.started, index))
        .collect();
    let mut finishes: BTreeSet<_> = receipts
        .iter()
        .enumerate()
        .map(|(index, receipt)| (receipt.finished, index))
        .collect();
    let mut frames = vec![frontier(&starts, &finishes, receipts)];
    let mut order = Vec::new();
    let mut undo: Vec<Vec<(usize, Record)>> = Vec::new();
    let mut explored = 0;
    let mut deepest = 0;
    loop {
        if order.len() == receipts.len() && state == final_state {
            return result(Status::Valid, &order, receipts, explored,
                "complete observations, effects, outcomes and final state have a serial witness respecting non-overlap");
        }
        let frame = frames
            .last_mut()
            .expect("root frame remains until search ends");
        if frame.next == frame.candidates.len() {
            frames.pop();
            let Some(index) = order.pop() else {
                return result(Status::Invalid, &[], receipts, explored,
                    format!("no valid serial order; exhausted all interval-respecting branches (deepest prefix {deepest}/{})", receipts.len()));
            };
            for (id, before) in undo.pop().unwrap() {
                state.insert(id, before);
            }
            starts.insert((receipts[index].started, index));
            finishes.insert((receipts[index].finished, index));
            continue;
        }
        if explored >= budget {
            return result(
                Status::Inconclusive,
                &order,
                receipts,
                explored,
                format!("search budget {budget} exhausted; no correctness conclusion"),
            );
        }
        let index = frame.candidates[frame.next];
        frame.next += 1;
        explored += 1;
        let receipt = &receipts[index];
        let mut reference = ReferenceStore::new(&state);
        let Ok(body) = execute_attempt(&mut reference, &receipt.message) else {
            continue;
        };
        if body != receipt.body {
            continue;
        }
        let writes = reference.writes;
        let mut previous = Vec::with_capacity(writes.len());
        for (id, row) in writes {
            previous.push((
                id,
                state
                    .insert(id, row)
                    .expect("reference checks reserved ids"),
            ));
        }
        undo.push(previous);
        starts.remove(&(receipt.started, index));
        finishes.remove(&(receipt.finished, index));
        order.push(index);
        deepest = deepest.max(order.len());
        frames.push(frontier(&starts, &finishes, receipts));
    }
}
