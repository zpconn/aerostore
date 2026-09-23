//! Small, explicitly scheduled compatibility probes against native engine APIs.
//!
//! These are deliberately independent of the synthetic flight model. A detected
//! isolation violation is a failed contract in the report, not an expected-bug
//! test that turns the overall compatibility gate green. Operational errors are
//! returned separately. No predicate lock or global harness lock is supplied.

use std::sync::Arc;

use aerostore_core::{
    IndexCompare, IndexValue, OccError, OccTable, OccTransaction, SecondaryIndex, ShmArena,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContractResult {
    pub name: String,
    pub passed: bool,
    pub details: String,
}

/// Run each scenario in a separate arena, with deterministic transaction
/// interleavings rather than scheduler-dependent sleeps.
pub fn aerostore_contracts() -> Result<Vec<ContractResult>, String> {
    Ok(vec![
        absent_candidate_creation()?,
        committed_index_visibility()?,
        historical_index_visibility()?,
        savepoint_rollback()?,
        multirow_snapshot_visibility()?,
        read_dependency_write_skew()?,
    ])
}

// Explicit abort on every early return also recycles any unpublished row
// versions. OccTransaction itself does not provide that cleanup on drop.
struct ProbeTransaction<'a> {
    table: &'a OccTable<u64>,
    tx: OccTransaction<u64>,
}

impl<'a> ProbeTransaction<'a> {
    fn begin(table: &'a OccTable<u64>) -> Result<Self, String> {
        Ok(Self {
            table,
            tx: table.begin_transaction().map_err(|err| err.to_string())?,
        })
    }

    fn read(&mut self, row: usize) -> Result<Option<u64>, String> {
        self.table
            .read(&mut self.tx, row)
            .map_err(|err| err.to_string())
    }

    fn write(&mut self, row: usize, value: u64) -> Result<(), String> {
        self.table
            .write(&mut self.tx, row, value)
            .map_err(|err| err.to_string())
    }

    /// A serialization rejection is a legitimate observable outcome. Other
    /// failures are infrastructure/engine errors and must not be counted as a
    /// successful safety mechanism.
    fn commit(&mut self) -> Result<bool, String> {
        match self.table.commit(&mut self.tx) {
            Ok(_) => Ok(true),
            Err(OccError::SerializationFailure) => Ok(false),
            Err(err) => Err(err.to_string()),
        }
    }
}

impl Drop for ProbeTransaction<'_> {
    fn drop(&mut self) {
        let _ = self.table.abort(&mut self.tx);
    }
}

fn fixture(values: &[u64]) -> Result<(Arc<ShmArena>, OccTable<u64>), String> {
    let arena = Arc::new(ShmArena::new(8 << 20).map_err(|err| err.to_string())?);
    let table = OccTable::new(Arc::clone(&arena), values.len()).map_err(|err| err.to_string())?;
    for (row, value) in values.iter().copied().enumerate() {
        table.seed_row(row, value).map_err(|err| err.to_string())?;
    }
    Ok((arena, table))
}

/// Use the fallible raw posting scan so an index error cannot masquerade as an
/// empty predicate. Filtering these native postings supplies no extra MVCC or
/// predicate dependencies, just as native key lookup supplies none.
fn candidates(index: &SecondaryIndex<usize>, key: u64) -> Result<Vec<usize>, String> {
    Ok(index
        .try_entries()
        .map_err(|err| err.to_string())?
        .into_iter()
        .filter_map(|(value, row)| (value == IndexValue::U64(key)).then_some(row))
        .collect())
}

fn materialized_candidates(
    index: &SecondaryIndex<usize>,
    tx: &mut ProbeTransaction<'_>,
    key: u64,
) -> Result<Vec<usize>, OccError> {
    tx.table
        .index_lookup(&mut tx.tx, index, &IndexCompare::Eq(IndexValue::U64(key)))
}

fn indexed_fixture(values: &[u64]) -> Result<(OccTable<u64>, SecondaryIndex<usize>), String> {
    let (arena, mut table) = fixture(values)?;
    let index = SecondaryIndex::<usize>::new_in_shared("flight_key", arena);
    for (row, value) in values.iter().copied().enumerate() {
        if value != 0 {
            index
                .try_insert(IndexValue::U64(value), row)
                .map_err(|err| err.to_string())?;
        }
    }
    table
        .bind_index(index.clone(), |value| {
            (*value != 0).then_some(IndexValue::U64(*value))
        })
        .map_err(|err| err.to_string())?;
    Ok((table, index))
}

fn result(name: &str, passed: bool, details: String) -> ContractResult {
    ContractResult {
        name: name.to_string(),
        passed,
        details,
    }
}

fn absent_candidate_creation() -> Result<ContractResult, String> {
    let (table, index) = indexed_fixture(&[0, 0])?;
    // Independent reserved slots, with no caller-held row or predicate locks.
    let mut first = ProbeTransaction::begin(&table)?;
    let mut second = ProbeTransaction::begin(&table)?;
    let first_search =
        materialized_candidates(&index, &mut first, 42).map_err(|e| e.to_string())?;
    let second_search =
        materialized_candidates(&index, &mut second, 42).map_err(|e| e.to_string())?;
    if !first_search.is_empty() || !second_search.is_empty() {
        return Err("absent-candidate fixture unexpectedly contained a flight".into());
    }
    first.write(0, 42)?;
    second.write(1, 42)?;
    let first_committed = first.commit()?;
    let second_committed = second.commit()?;
    let committed = usize::from(first_committed) + usize::from(second_committed);
    let matching_rows = table
        .snapshot_latest_rows()
        .map_err(|err| err.to_string())?
        .into_iter()
        .filter(|(_, value)| *value == 42)
        .count();
    let matching_postings = candidates(&index, 42)?.len();
    Ok(result(
        "serializable_absent_candidate_creation",
        committed == 1 && matching_rows == 1 && matching_postings == 1,
        format!(
            "Expected one commit and one serialization rejection after overlapping empty \
             candidate searches activate different slots for the same key. Observed initial \
             counts=0/0, committed={committed}, rejected={}, matching_rows={matching_rows}, \
             matching_index_postings={matching_postings}. Native predicate dependencies and \
             automatic index maintenance; no caller-supplied locks.",
            2 - committed,
        ),
    ))
}

fn committed_index_visibility() -> Result<ContractResult, String> {
    let (table, index) = indexed_fixture(&[10])?;
    let mut writer = ProbeTransaction::begin(&table)?;
    writer.write(0, 20)?;
    let mut before = ProbeTransaction::begin(&table)?;
    let before_row = before.read(0)?;
    let before_new_key =
        materialized_candidates(&index, &mut before, 20).map_err(|e| e.to_string())?;
    if !writer.commit()? {
        return Err("uncontended publication-probe writer was rejected".into());
    }
    // No separate index maintenance call exists at this boundary. A newly
    // committed row must already be discoverable through its new key.
    let mut reader = ProbeTransaction::begin(&table)?;
    let visible_value = reader.read(0)?;
    let found = materialized_candidates(&index, &mut reader, 20).map_err(|e| e.to_string())?;
    let reader_committed = reader.commit()?;
    let before_committed = before.commit()?;
    let old_postings = candidates(&index, 10)?;
    let after_publication = candidates(&index, 20)?;
    Ok(result(
        "committed_row_and_index_visibility",
        before_row == Some(10) && before_new_key.is_empty() && !before_committed
            && reader_committed && visible_value == Some(20) && found == vec![0]
            && old_postings.is_empty() && after_publication == vec![0],
        format!(
            "Expected staged writes to remain invisible, and native commit to publish row \
             and index together. Before commit row={before_row:?}, new_key={before_new_key:?}; \
             immediately after commit row={visible_value:?}, new_key={found:?}, \
             fresh_reader_committed={reader_committed}, overlapping_reader_committed={before_committed}, \
             raw_old_postings={old_postings:?}, raw_new_postings={after_publication:?}.",
        ),
    ))
}

fn historical_index_visibility() -> Result<ContractResult, String> {
    let (table, index) = indexed_fixture(&[10])?;
    let mut reader = ProbeTransaction::begin(&table)?;
    // Independent witness avoids masking an index omission with a point-read
    // dependency in the candidate-search reader.
    let mut witness = ProbeTransaction::begin(&table)?;
    let mut writer = ProbeTransaction::begin(&table)?;
    writer.write(0, 20)?;
    if !writer.commit()? {
        return Err("uncontended historical-index writer was rejected".into());
    }
    let expected_snapshot_value = witness.read(0)?;
    let (found, rejected_at_lookup, reader_committed) =
        match materialized_candidates(&index, &mut reader, 10) {
            Ok(found) => (found, false, reader.commit()?),
            Err(OccError::SerializationFailure) => (Vec::new(), true, false),
            Err(err) => return Err(err.to_string()),
        };
    Ok(result(
        "index_candidates_respect_transaction_snapshot",
        expected_snapshot_value == Some(10) && (!reader_committed || found == vec![0]),
        format!(
            "Expected the old snapshot's row to remain discoverable after a key move, \
             or explicit serialization rejection. Independent witness sees \
             row={expected_snapshot_value:?}; old_key_candidates={found:?}, \
             rejected_at_lookup={rejected_at_lookup}, candidate_reader_committed={reader_committed}. \
             Operational errors are not accepted as serialization protection.",
        ),
    ))
}

fn savepoint_rollback() -> Result<ContractResult, String> {
    let (_arena, table) = fixture(&[100, 50, 0])?;
    let _guard = table
        .lock_indexed_rows(&[0, 1, 2])
        .map_err(|err| err.to_string())?;
    let mut tx = ProbeTransaction::begin(&table)?;
    tx.write(0, 90)?;
    tx.write(1, 60)?;
    table
        .savepoint(&mut tx.tx, "candidate")
        .map_err(|err| err.to_string())?;
    tx.write(0, 1)?;
    tx.write(1, 999)?;
    table
        .savepoint(&mut tx.tx, "nested")
        .map_err(|err| err.to_string())?;
    tx.write(2, 777)?;
    table
        .rollback_to(&mut tx.tx, "candidate")
        .map_err(|err| err.to_string())?;
    let restored = [tx.read(0)?, tx.read(1)?, tx.read(2)?];
    let committed = tx.commit()?;
    let committed_rows = table
        .snapshot_latest_rows()
        .map_err(|err| err.to_string())?;
    // A later whole-message abort must also preserve the committed result.
    let mut abandoned = ProbeTransaction::begin(&table)?;
    abandoned.write(0, 456)?;
    abandoned.write(2, 123)?;
    table
        .abort(&mut abandoned.tx)
        .map_err(|err| err.to_string())?;
    let after_abort = table
        .snapshot_latest_rows()
        .map_err(|err| err.to_string())?;
    let expected = vec![(0, 90), (1, 60), (2, 0)];
    Ok(result(
        "savepoint_and_whole_message_rollback",
        committed && restored == [Some(90), Some(60), Some(0)]
            && committed_rows == expected && after_abort == expected,
        format!(
            "Expected rollback to preserve pre-savepoint multirow writes [90,60,0], discard \
             nested candidate writes, and leave the committed transfer unchanged after a later \
             whole-message abort. Observed restored={restored:?}, committed={committed}, \
             committed_rows={committed_rows:?}, after_abort={after_abort:?}. \
             This probe covers native table state; it makes no claim about an external index overlay.",
        ),
    ))
}

fn multirow_snapshot_visibility() -> Result<ContractResult, String> {
    let (_arena, table) = fixture(&[100, 50])?;
    let mut old_reader = ProbeTransaction::begin(&table)?;
    let first_before = old_reader.read(0)?;
    let _guard = table
        .lock_indexed_rows(&[0, 1])
        .map_err(|err| err.to_string())?;
    let mut writer = ProbeTransaction::begin(&table)?;
    writer.write(0, 90)?;
    writer.write(1, 60)?;
    let writer_committed = writer.commit()?;
    let second_after = old_reader.read(1)?;
    let mut new_reader = ProbeTransaction::begin(&table)?;
    let new_values = [new_reader.read(0)?, new_reader.read(1)?];
    let new_reader_committed = new_reader.commit()?;
    // Aborting the old read transaction is intentional: serializable OCC may
    // reject it at commit because the writer changed one of its dependencies.
    table
        .abort(&mut old_reader.tx)
        .map_err(|err| err.to_string())?;
    Ok(result(
        "multirow_table_snapshot_visibility",
        writer_committed && first_before == Some(100) && second_after == Some(50)
            && new_values == [Some(90), Some(60)] && new_reader_committed,
        format!(
            "Expected a reader spanning a two-row transfer to retain [100,50] in its snapshot, \
             and a new reader to observe [90,60]. Observed old_reader=[{first_before:?},{second_after:?}], \
             writer_committed={writer_committed}, new_reader={new_values:?}, \
             new_reader_committed={new_reader_committed}. Both views must conserve total=150.",
        ),
    ))
}

fn read_dependency_write_skew() -> Result<ContractResult, String> {
    let (_arena, table) = fixture(&[1, 1])?;
    let _first_guard = table
        .lock_indexed_rows(&[0])
        .map_err(|err| err.to_string())?;
    let _second_guard = table
        .lock_indexed_rows(&[1])
        .map_err(|err| err.to_string())?;
    let mut first = ProbeTransaction::begin(&table)?;
    let mut second = ProbeTransaction::begin(&table)?;
    let first_view = [first.read(0)?, first.read(1)?];
    let second_view = [second.read(0)?, second.read(1)?];
    if first_view != [Some(1), Some(1)] || second_view != first_view {
        return Err("write-skew fixture had unexpected initial state".into());
    }
    first.write(0, 0)?;
    second.write(1, 0)?;
    let first_committed = first.commit()?;
    let second_committed = second.commit()?;
    let committed = usize::from(first_committed) + usize::from(second_committed);
    let rows = table
        .snapshot_latest_rows()
        .map_err(|err| err.to_string())?;
    let remaining: u64 = rows.iter().map(|(_, value)| *value).sum();
    Ok(result(
        "concrete_read_dependency_write_skew",
        committed == 1 && remaining == 1,
        format!(
            "Expected one serialization rejection when overlapping transactions read both \
             active records and each deactivates a different record. Observed committed={committed}, \
             rejected={}, remaining_active={remaining}, final_rows={rows:?}. \
             This contrasts concrete row-read dependencies with the separate empty-candidate probe.",
            2 - committed,
        ),
    ))
}
