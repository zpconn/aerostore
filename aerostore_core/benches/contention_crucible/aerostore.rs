//! Query-discovered transactions over the unchanged native engine.
//! There is no application row lock or predeclared write-set admission here.
pub use super::fixture::{Attachment, Shared};
use super::storage::{Query, Store};
pub use crate::extended_crucible::aerostore::{WAL_SLOTS, WAL_SLOT_BYTES};
use crate::extended_crucible::metrics::StoreMetrics;
use crate::extended_crucible::model::{DbError, Record};
use aerostore_core::{
    IndexCompare, IndexValue, OccCommitter, OccError, OccTransaction, WalWriterError,
};
use std::collections::{BTreeMap, BTreeSet};

pub struct Adapter<'a> {
    shared: &'a Shared,
    tx: Option<OccTransaction<Record>>,
    saves: usize,
    committer: OccCommitter<WAL_SLOTS, WAL_SLOT_BYTES>,
    global_time_predicates: bool,
    detailed_diagnostics: bool,
    pub metrics: StoreMetrics,
    pub retry_causes: BTreeMap<String, u64>,
    /// Query work counters, not inferred conflict reasons. Units are in each key.
    pub diagnostics: BTreeMap<String, u64>,
}

fn classified(
    causes: &mut BTreeMap<String, u64>,
    diagnostics: &mut BTreeMap<String, u64>,
    detailed: bool,
    indexes: &[aerostore_core::SecondaryIndex<usize>],
    stage: &str,
    error: OccError,
) -> DbError {
    if error == OccError::SerializationFailure {
        *causes
            .entry(format!("{stage}:serialization_failure"))
            .or_default() += 1;
        if detailed {
            let event = aerostore_core::retry_diagnostics::take();
            let cause = event.map_or("unattributed", |event| event.cause.name());
            let index = event
                .and_then(|event| event.index_offset)
                .and_then(|offset| {
                    indexes
                        .iter()
                        .position(|index| index.header_offset() == offset)
                })
                .and_then(|number| {
                    ["callsign", "tail", "family", "due", "event_time"]
                        .get(number)
                        .copied()
                })
                .unwrap_or("none");
            *diagnostics
                .entry(format!("conflict_origin:{stage}:{cause}:{index}"))
                .or_default() += 1;
        }
        DbError::Conflict
    } else {
        // Fatal publication/recovery errors are never relabelled as retries.
        if detailed {
            let _ = aerostore_core::retry_diagnostics::take();
        }
        DbError::Fatal(format!("{stage}: {error}"))
    }
}

impl<'a> Adapter<'a> {
    pub fn new(shared: &'a Shared, global_time_predicates: bool) -> Self {
        Self::new_with_diagnostics(shared, global_time_predicates, false)
    }

    pub fn new_with_diagnostics(
        shared: &'a Shared,
        global_time_predicates: bool,
        detailed_diagnostics: bool,
    ) -> Self {
        Self {
            shared,
            tx: None,
            saves: 0,
            committer: OccCommitter::new_asynchronous(shared.ring.clone()),
            global_time_predicates,
            detailed_diagnostics,
            metrics: StoreMetrics::default(),
            retry_causes: BTreeMap::new(),
            diagnostics: BTreeMap::new(),
        }
    }
}

impl Store for Adapter<'_> {
    fn begin(&mut self, declared: &[usize]) -> Result<(), DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        if !declared.is_empty() || self.tx.is_some() {
            return Err(DbError::Fatal(
                "dynamic adapter requires no declared writes and a closed transaction".into(),
            ));
        }
        self.metrics.begins += 1;
        self.tx = Some(self.shared.table.begin_transaction().map_err(|e| {
            classified(
                &mut self.retry_causes,
                &mut self.diagnostics,
                self.detailed_diagnostics,
                &self.shared.indexes,
                "begin",
                e,
            )
        })?);
        self.saves = 0;
        Ok(())
    }

    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        self.metrics.reads += 1;
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| DbError::Fatal("read outside transaction".into()))?;
        self.shared
            .table
            .read(tx, id)
            .map_err(|e| {
                classified(
                    &mut self.retry_causes,
                    &mut self.diagnostics,
                    self.detailed_diagnostics,
                    &self.shared.indexes,
                    "row_read",
                    e,
                )
            })?
            .ok_or_else(|| DbError::Fatal(format!("unseeded physical slot {id}")))
    }

    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        self.metrics.queries += 1;
        let stage = match query {
            Query::Candidates { .. } => "candidate_lookup",
            Query::Due { .. } => "due_lookup",
            Query::Expired { .. } => "expiry_lookup",
            Query::GlobalDue { .. } => "global_due_lookup",
            Query::GlobalExpired { .. } => "global_expiry_lookup",
            Query::Positions { .. } => "history_lookup",
            Query::Family { .. } => "family_lookup",
            Query::All => "all_lookup",
        };
        let started = std::time::Instant::now();
        *self
            .diagnostics
            .entry(format!("{stage}:calls"))
            .or_default() += 1;
        let result = (|| {
            let eq = |v| IndexCompare::Eq(IndexValue::I64(v));
            let ids: BTreeSet<usize> = {
                let tx = self
                    .tx
                    .as_mut()
                    .ok_or_else(|| DbError::Fatal("query outside transaction".into()))?;
                let mut lookup = |number: usize, predicate: IndexCompare| {
                    self.shared
                        .table
                        .index_lookup(tx, &self.shared.indexes[number], &predicate)
                        .map_err(|e| {
                            classified(
                                &mut self.retry_causes,
                                &mut self.diagnostics,
                                self.detailed_diagnostics,
                                &self.shared.indexes,
                                stage,
                                e,
                            )
                        })
                };
                match *query {
                    Query::Candidates { callsign, tail, .. } => {
                        let mut rows = lookup(0, eq(callsign))?;
                        if tail != 0 {
                            rows.extend(lookup(1, eq(tail))?);
                        }
                        rows.into_iter().collect()
                    }
                    Query::Due { at, .. } if self.global_time_predicates => {
                        lookup(3, IndexCompare::Lte(IndexValue::I64(at)))?
                            .into_iter()
                            .collect()
                    }
                    Query::Expired { before, .. } if self.global_time_predicates => {
                        lookup(4, IndexCompare::Lt(IndexValue::I64(before)))?
                            .into_iter()
                            .collect()
                    }
                    Query::GlobalDue { at } => lookup(3, IndexCompare::Lte(IndexValue::I64(at)))?
                        .into_iter()
                        .collect(),
                    Query::GlobalExpired { before } => {
                        lookup(4, IndexCompare::Lt(IndexValue::I64(before)))?
                            .into_iter()
                            .collect()
                    }
                    Query::Family { family, .. }
                    | Query::Positions { family, .. }
                    | Query::Due { family, .. }
                    | Query::Expired { family, .. } => lookup(2, eq(family))?.into_iter().collect(),
                    Query::All => {
                        return Err(DbError::Fatal(
                            "unindexed All query is outside contention workload".into(),
                        ))
                    }
                }
            };
            *self
                .diagnostics
                .entry(format!("{stage}:candidate_rows"))
                .or_default() += ids.len() as u64;
            let mut rows = Vec::new();
            for id in ids {
                if self.detailed_diagnostics {
                    *self
                        .diagnostics
                        .entry(format!("{stage}:attempted_materializations"))
                        .or_default() += 1;
                }
                let row = self.read(id)?;
                if self.detailed_diagnostics {
                    *self
                        .diagnostics
                        .entry(format!("{stage}:completed_materializations"))
                        .or_default() += 1;
                }
                if query.matches(&row) {
                    rows.push(row);
                }
            }
            self.metrics.returned_rows += rows.len() as u64;
            *self
                .diagnostics
                .entry(format!("{stage}:returned_rows"))
                .or_default() += rows.len() as u64;
            *self
                .diagnostics
                .entry(format!("{stage}:successful_query_ns"))
                .or_default() += started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            Ok(rows)
        })();
        if self.detailed_diagnostics {
            let outcome = if result.is_ok() {
                "successful"
            } else {
                "failed"
            };
            *self
                .diagnostics
                .entry(format!("{stage}:{outcome}_attempts"))
                .or_default() += 1;
            *self
                .diagnostics
                .entry(format!("{stage}:{outcome}_attempt_ns"))
                .or_default() += started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        result
    }

    fn write(&mut self, row: Record) -> Result<(), DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| DbError::Fatal("write outside transaction".into()))?;
        self.shared.table.write(tx, row.id, row).map_err(|e| {
            classified(
                &mut self.retry_causes,
                &mut self.diagnostics,
                self.detailed_diagnostics,
                &self.shared.indexes,
                "write",
                e,
            )
        })?;
        self.metrics.writes += 1;
        Ok(())
    }

    fn savepoint(&mut self) -> Result<usize, DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        let id = self.saves;
        self.shared
            .table
            .savepoint(
                self.tx
                    .as_mut()
                    .ok_or_else(|| DbError::Fatal("savepoint outside transaction".into()))?,
                &format!("hard_{id}"),
            )
            .map_err(|e| {
                classified(
                    &mut self.retry_causes,
                    &mut self.diagnostics,
                    self.detailed_diagnostics,
                    &self.shared.indexes,
                    "savepoint",
                    e,
                )
            })?;
        self.saves += 1;
        self.metrics.savepoints += 1;
        Ok(id)
    }

    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        self.shared
            .table
            .rollback_to(
                self.tx
                    .as_mut()
                    .ok_or_else(|| DbError::Fatal("rollback outside transaction".into()))?,
                &format!("hard_{id}"),
            )
            .map_err(|e| {
                classified(
                    &mut self.retry_causes,
                    &mut self.diagnostics,
                    self.detailed_diagnostics,
                    &self.shared.indexes,
                    "rollback",
                    e,
                )
            })?;
        self.saves = id + 1;
        self.metrics.savepoint_rollbacks += 1;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        let result = self.committer.commit(
            &self.shared.table,
            self.tx
                .as_mut()
                .ok_or_else(|| DbError::Fatal("commit outside transaction".into()))?,
        );
        match result {
            Ok(_) => {
                self.tx = None;
                self.metrics.commits += 1;
                Ok(())
            }
            Err(WalWriterError::Occ(error)) => {
                let error = classified(
                    &mut self.retry_causes,
                    &mut self.diagnostics,
                    self.detailed_diagnostics,
                    &self.shared.indexes,
                    "commit",
                    error,
                );
                self.abort()?;
                Err(error)
            }
            Err(error) => {
                // A non-OCC error may have an ambiguous accepted/visible result.
                // Never retry the message automatically in this case.
                let cleanup = self.abort();
                Err(DbError::Fatal(format!(
                    "WAL commit outcome cannot be retried: {error}; cleanup={cleanup:?}"
                )))
            }
        }
    }

    fn abort(&mut self) -> Result<(), DbError> {
        aerostore_core::retry_diagnostics::set_enabled(self.detailed_diagnostics);
        if let Some(mut tx) = self.tx.take() {
            self.metrics.aborts += 1;
            self.shared.table.abort(&mut tx).map_err(|e| {
                classified(
                    &mut self.retry_causes,
                    &mut self.diagnostics,
                    self.detailed_diagnostics,
                    &self.shared.indexes,
                    "abort",
                    e,
                )
            })?;
        }
        Ok(())
    }
}

impl Drop for Adapter<'_> {
    fn drop(&mut self) {
        let _ = self.abort();
        aerostore_core::retry_diagnostics::set_enabled(false);
    }
}

pub fn retention(shared: &Shared) -> Result<serde_json::Value, String> {
    let snapshot = shared.arena.create_snapshot();
    let recycled = shared
        .table
        .recycle_telemetry()
        .map_err(|e| e.to_string())?;
    let indexes: Vec<_> = shared
        .indexes
        .iter()
        .enumerate()
        .map(|(i, index)| {
            let t = index.mutation_telemetry();
            serde_json::json!({"index":i,"retired_nodes":t.retired_backlog,
            "retired_postings":t.retired_postings,"reclaimed_postings":t.reclaimed_postings,
            "gc_recycle_errors":t.gc_recycle_errors,"alloc_failures":t.alloc_failure_events})
        })
        .collect();
    Ok(
        serde_json::json!({"arena_high_water_bytes":shared.arena.chunked_arena().head_offset(),
        "arena_capacity_bytes":shared.arena.len(),"active_transactions":snapshot.len(),
        "snapshot_xmin":snapshot.xmin,"snapshot_xmax":snapshot.xmax,
        "row_fresh_allocations":recycled.alloc_fresh,
        "row_reuse_allocations":recycled.alloc_from_primary+recycled.alloc_from_probe+recycled.alloc_from_starved,
        "row_recycled":recycled.push_success,"indexes":indexes,
        "measurement":"arena allocation high-water and retirement counts; not physical RSS or live bytes"}),
    )
}
