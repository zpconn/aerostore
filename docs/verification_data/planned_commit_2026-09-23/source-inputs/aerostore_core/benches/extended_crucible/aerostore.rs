//! Adapter over the process-shared engine. Native OccTable transactions own
//! predicate dependencies, row versions, savepoints, and index publication.
use super::metrics::StoreMetrics;
use super::model::{DbError, Query, Record, Store, FLIGHT, SCHEDULED};
use aerostore_core::occ_partitioned::{OccCommitRecord, OccCommittedWrite};
use aerostore_core::{
    map_tmpfs_shared, serialize_commit_record, wal_commit_from_occ_record_with_policy,
    IndexCompare, IndexValue, IndexedUpdateGuard, OccCommitter, OccError, OccTable, OccTransaction,
    RelPtr, SecondaryIndex, SharedWalRing, ShmArena, TmpfsAttachMode, WalDeltaCodec,
    WalEncodingPolicy, WalWriterError,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub const WAL_SLOTS: usize = 16;
pub const WAL_SLOT_BYTES: usize = 65_536;
pub type Ring = SharedWalRing<WAL_SLOTS, WAL_SLOT_BYTES>;
const INDEX_NAMES: [&str; 5] = ["callsign", "tail", "family", "due", "event_time"];
const INDEX_KEYS: [fn(&Record) -> Option<IndexValue>; 5] = [
    |row| keys(row)[0].map(IndexValue::I64),
    |row| keys(row)[1].map(IndexValue::I64),
    |row| keys(row)[2].map(IndexValue::I64),
    |row| keys(row)[3].map(IndexValue::I64),
    |row| keys(row)[4].map(IndexValue::I64),
];

impl WalDeltaCodec for Record {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attachment {
    pub path: PathBuf,
    pub bytes: usize,
    pub table_header: u32,
    pub table_slots: Vec<u32>,
    pub indexes: Vec<u32>,
    pub ring: u32,
}

pub struct Shared {
    pub arena: Arc<ShmArena>,
    pub table: Arc<OccTable<Record>>,
    pub indexes: Vec<SecondaryIndex<usize>>,
    pub ring: Ring,
}

fn fatal(error: impl std::fmt::Display) -> DbError {
    DbError::Fatal(error.to_string())
}
fn occ(error: OccError) -> DbError {
    match error {
        OccError::SerializationFailure => DbError::Conflict,
        other => fatal(other),
    }
}

fn keys(row: &Record) -> [Option<i64>; 5] {
    if !row.active {
        return [None; 5];
    }
    [
        (row.kind == FLIGHT).then_some(row.callsign),
        (row.kind == FLIGHT && row.tail != 0).then_some(row.tail),
        Some(row.family),
        (row.kind == SCHEDULED).then_some(row.due),
        Some(row.event_time),
    ]
}

fn maximum_wal_record_bytes() -> Result<usize, String> {
    // Record contains only fixed-width scalar values. Use the longest possible
    // row-id string and force every row to full encoding, which stores its value
    // in both value_payload and wal_record_payload. Let the actual codecs account
    // for framing, duplicate payloads, alignment and future encoding changes.
    let record = OccCommitRecord {
        txid: u64::MAX,
        writes: (0..super::model::SLOTS_PER_FAMILY)
            .map(|offset| {
                let row_id = usize::MAX - offset;
                OccCommittedWrite {
                    row_id,
                    base_offset: u32::MAX,
                    new_offset: u32::MAX,
                    base_value: Record::default(),
                    value: Record {
                        id: row_id,
                        ..Record::default()
                    },
                    dirty_columns_bitmask: u64::MAX,
                }
            })
            .collect(),
    };
    let encoded = wal_commit_from_occ_record_with_policy(&record, |_| WalEncodingPolicy::ForceFull)
        .map_err(|error| error.to_string())?;
    Ok(serialize_commit_record(&encoded)
        .map_err(|error| error.to_string())?
        .len())
}

fn immutable_arena_prefix(arena: &ShmArena) -> [u8; 16] {
    // ShmHeader is repr(C); its first four u32 values are immutable magic,
    // layout version, capacity and data_start. Together they are exactly the
    // fields checked by ShmArena::is_header_valid. Obtain current values from a
    // freshly initialized engine arena, rather than duplicating private constants.
    let mut prefix = [0; 16];
    // SAFETY: ShmArena validates a mapping larger than its complete initialized
    // header. These first 16 bytes contain no concurrently mutable atomic fields.
    unsafe {
        std::ptr::copy_nonoverlapping(
            arena.mmap_base().as_ptr(),
            prefix.as_mut_ptr(),
            prefix.len(),
        );
    }
    prefix
}

fn attach_existing_arena(path: &Path, bytes: usize) -> Result<ShmArena, String> {
    // Do not let map_tmpfs_shared create, resize, or reinitialize a damaged
    // attachment. Opening an existing inode first also pins the file across a
    // rename/unlink: the mapper opens this exact descriptor through procfs.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("open existing arena {}: {error}", path.display()))?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() != bytes as u64 {
        return Err(format!("arena attachment requires an existing regular file of exactly {bytes} bytes; observed {}", metadata.len()));
    }
    let expected = {
        let template = ShmArena::new(bytes).map_err(|error| error.to_string())?;
        immutable_arena_prefix(&template)
    };
    let mut observed = [0; 16];
    file.read_exact(&mut observed)
        .map_err(|error| error.to_string())?;
    if observed != expected {
        return Err(
            "arena attachment header is invalid or incompatible; refusing to reinitialize it"
                .into(),
        );
    }
    let descriptor_path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    let mapped = map_tmpfs_shared(&descriptor_path, bytes).map_err(|error| error.to_string())?;
    if mapped.mode != TmpfsAttachMode::WarmStart || !mapped.arena.is_header_valid() {
        return Err("arena attachment did not preserve a valid warm mapping".into());
    }
    Ok(mapped.arena)
}

impl Shared {
    pub fn create(path: &Path, bytes: usize, records: &[Record]) -> Result<Self, String> {
        // Reject oversized fixture transactions before any rows can commit.
        let maximum = maximum_wal_record_bytes()?;
        if maximum > WAL_SLOT_BYTES {
            return Err(format!("fixture's maximum encoded transaction is {maximum} bytes, exceeding {WAL_SLOT_BYTES}-byte WAL slots"));
        }
        let arena = Arc::new(
            map_tmpfs_shared(path, bytes)
                .map_err(|e| e.to_string())?
                .arena,
        );
        let mut table =
            OccTable::new(Arc::clone(&arena), records.len()).map_err(|e| e.to_string())?;
        let indexes = INDEX_NAMES
            .iter()
            .map(|name| SecondaryIndex::new_in_shared(name, Arc::clone(&arena)))
            .collect::<Vec<_>>();
        for row in records {
            table.seed_row(row.id, *row).map_err(|e| e.to_string())?;
            for (index, key) in indexes.iter().zip(keys(row)) {
                if let Some(key) = key {
                    index
                        .try_insert(IndexValue::I64(key), row.id)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        for (index, key) in indexes.iter().zip(INDEX_KEYS) {
            table
                .bind_index(index.clone(), key)
                .map_err(|e| e.to_string())?;
        }
        let table = Arc::new(table);
        let ring = Ring::create(Arc::clone(&arena)).map_err(|e| e.to_string())?;
        Ok(Self {
            arena,
            table,
            indexes,
            ring,
        })
    }

    pub fn attachment(&self, path: &Path) -> Attachment {
        Attachment {
            path: path.to_path_buf(),
            bytes: self.arena.len(),
            table_header: self.table.shared_header_offset(),
            table_slots: self.table.index_slot_offsets(),
            indexes: self
                .indexes
                .iter()
                .map(SecondaryIndex::header_offset)
                .collect(),
            ring: self.ring.ring_ptr().load(Ordering::Acquire),
        }
    }

    pub fn attach(a: &Attachment) -> Result<Self, String> {
        if a.indexes.len() != INDEX_NAMES.len() {
            return Err("invalid extended index attachment".into());
        }
        let arena = Arc::new(attach_existing_arena(&a.path, a.bytes)?);
        let mut table =
            OccTable::from_existing(Arc::clone(&arena), a.table_header, a.table_slots.clone())
                .map_err(|e| e.to_string())?;
        let indexes = INDEX_NAMES
            .iter()
            .zip(&a.indexes)
            .map(|(name, offset)| {
                SecondaryIndex::from_existing(name, Arc::clone(&arena), *offset)
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, key) in indexes.iter().zip(INDEX_KEYS) {
            table
                .bind_index(index.clone(), key)
                .map_err(|e| e.to_string())?;
        }
        let table = Arc::new(table);
        let ring = Ring::from_existing(Arc::clone(&arena), RelPtr::from_offset(a.ring));
        Ok(Self {
            arena,
            table,
            indexes,
            ring,
        })
    }

    pub fn snapshot(&self) -> Result<Vec<Record>, String> {
        let mut rows = self
            .table
            .snapshot_latest_rows()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(_, r)| r)
            .collect::<Vec<_>>();
        rows.sort_by_key(|r| r.id);
        Ok(rows)
    }

    /// Called only while every worker is at a phase barrier or has exited.
    pub fn audit(&self) -> Result<serde_json::Value, String> {
        let rows = self.snapshot()?;
        let mut audits = Vec::new();
        for (number, index) in self.indexes.iter().enumerate() {
            let mut expected = rows
                .iter()
                .filter_map(|r| keys(r)[number].map(|k| (IndexValue::I64(k), r.id)))
                .collect::<Vec<_>>();
            expected.sort();
            let mut actual = index.try_entries().map_err(|e| e.to_string())?;
            actual.sort();
            if actual != expected {
                return Err(format!(
                    "{} index disagrees with table: expected {} postings, observed {}",
                    INDEX_NAMES[number],
                    expected.len(),
                    actual.len()
                ));
            }
            for _ in 0..16 {
                index.collect_garbage_once(usize::MAX);
            }
            let telemetry = index.mutation_telemetry();
            if telemetry.retired_backlog != 0
                || telemetry.retired_postings != 0
                || telemetry.gc_recycle_errors != 0
            {
                return Err(format!(
                    "{} reclamation did not drain cleanly",
                    INDEX_NAMES[number]
                ));
            }
            let audit = index
                .audit_allocations()
                .map_err(|e| format!("{} allocation ownership: {e}", INDEX_NAMES[number]))?;
            audits.push(serde_json::json!({"index":INDEX_NAMES[number],"postings":actual.len(),"ownership":format!("{audit:?}"),"retired_postings":telemetry.retired_postings}));
        }
        Ok(
            serde_json::json!({"indexes":audits,"arena_high_water_bytes":self.arena.chunked_arena().head_offset()}),
        )
    }
}

type Intents = BTreeMap<usize, (Record, Record)>;
pub struct Adapter<'a> {
    shared: &'a Shared,
    tx: Option<OccTransaction<Record>>,
    guard: Option<IndexedUpdateGuard<'a>>,
    declared: BTreeSet<usize>,
    pending: Intents,
    saves: Vec<Intents>,
    committer: OccCommitter<WAL_SLOTS, WAL_SLOT_BYTES>,
    pub metrics: StoreMetrics,
}

impl<'a> Adapter<'a> {
    pub fn new(shared: &'a Shared) -> Self {
        Self {
            shared,
            tx: None,
            guard: None,
            declared: BTreeSet::new(),
            pending: BTreeMap::new(),
            saves: Vec::new(),
            committer: OccCommitter::new_asynchronous(shared.ring.clone()),
            metrics: StoreMetrics::default(),
        }
    }

    fn release(&mut self) {
        self.tx = None;
        self.pending.clear();
        self.saves.clear();
        self.declared.clear();
        self.guard = None;
    }
}

impl Store for Adapter<'_> {
    fn begin(&mut self, write_slots: &[usize]) -> Result<(), DbError> {
        if self.tx.is_some() {
            return Err(fatal("transaction already open"));
        }
        self.metrics.begins += 1;
        let table = &self.shared.table;
        self.guard = table.try_lock_indexed_rows(write_slots).map_err(occ)?;
        if self.guard.is_none() {
            return Err(DbError::Conflict);
        }
        match table.begin_transaction() {
            Ok(tx) => {
                self.tx = Some(tx);
                self.declared = write_slots.iter().copied().collect();
                Ok(())
            }
            Err(e) => {
                self.guard = None;
                Err(occ(e))
            }
        }
    }

    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.metrics.reads += 1;
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| fatal("no open transaction"))?;
        self.shared
            .table
            .read(tx, id)
            .map_err(occ)?
            .ok_or_else(|| fatal(format!("unseeded fixture slot {id}")))
    }

    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        self.metrics.queries += 1;
        let ix = &self.shared.indexes;
        let table = &self.shared.table;
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| fatal("no open transaction"))?;
        let eq = |value| IndexCompare::Eq(IndexValue::I64(value));
        let mut lookup = |index: usize, predicate: IndexCompare| {
            table.index_lookup(tx, &ix[index], &predicate).map_err(occ)
        };
        let ids: BTreeSet<usize> = match *query {
            Query::Candidates { callsign, tail, .. } => {
                let mut ids = lookup(0, eq(callsign))?;
                if tail != 0 {
                    ids.extend(lookup(1, eq(tail))?);
                }
                ids.into_iter().collect()
            }
            Query::Family { family, .. }
            | Query::Positions { family, .. }
            | Query::Due { family, .. }
            | Query::Expired { family, .. } => {
                // Family equality is the selective index predicate for these
                // conjunctive queries. Materialize it and apply the time/kind
                // residual below. This protects the complete matching family
                // without enrolling unrelated families in a global time range.
                lookup(2, eq(family))?.into_iter().collect()
            }
            Query::All => (0..table.capacity()).collect(),
        };
        let mut rows = Vec::new();
        for id in ids {
            let row = self.read(id)?;
            if query.matches(&row) {
                rows.push(row);
            }
        }
        self.metrics.returned_rows += rows.len() as u64;
        Ok(rows)
    }

    fn write(&mut self, row: Record) -> Result<(), DbError> {
        if !self.declared.contains(&row.id) {
            return Err(fatal(format!(
                "write {} outside declared fixture slots",
                row.id
            )));
        }
        let before = if let Some((before, _)) = self.pending.get(&row.id) {
            *before
        } else {
            self.read(row.id)?
        };
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| fatal("no open transaction"))?;
        self.shared.table.write(tx, row.id, row).map_err(occ)?;
        self.pending.insert(row.id, (before, row));
        self.metrics.writes += 1;
        Ok(())
    }

    fn savepoint(&mut self) -> Result<usize, DbError> {
        let id = self.saves.len();
        self.shared
            .table
            .savepoint(
                self.tx.as_mut().ok_or_else(|| fatal("no transaction"))?,
                &format!("s{id}"),
            )
            .map_err(occ)?;
        self.saves.push(self.pending.clone());
        self.metrics.savepoints += 1;
        Ok(id)
    }

    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        let pending = self
            .saves
            .get(id)
            .cloned()
            .ok_or_else(|| fatal("unknown savepoint"))?;
        self.shared
            .table
            .rollback_to(
                self.tx.as_mut().ok_or_else(|| fatal("no transaction"))?,
                &format!("s{id}"),
            )
            .map_err(occ)?;
        self.pending = pending;
        self.saves.truncate(id + 1);
        self.metrics.savepoint_rollbacks += 1;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), DbError> {
        let index_mutations = self
            .pending
            .values()
            .map(|(before, after)| {
                keys(before)
                    .into_iter()
                    .zip(keys(after))
                    .filter(|(old, new)| old != new)
                    .count() as u64
            })
            .sum::<u64>();
        let result = self.committer.commit(
            &self.shared.table,
            self.tx.as_mut().ok_or_else(|| fatal("no transaction"))?,
        );
        match result {
            Ok(_) => {
                self.metrics.index_mutations += index_mutations;
                self.release();
                self.metrics.commits += 1;
                Ok(())
            }
            Err(WalWriterError::Occ(e)) => {
                let _ = self.abort();
                Err(occ(e))
            }
            Err(e) => {
                // Row and index publication already completed atomically.
                // A subsequent WAL failure is fatal and cannot retry the input.
                self.release();
                Err(fatal(format!("fatal after row/index commit: WAL={e}")))
            }
        }
    }

    fn abort(&mut self) -> Result<(), DbError> {
        let result = if let Some(mut tx) = self.tx.take() {
            self.metrics.aborts += 1;
            self.shared.table.abort(&mut tx).map_err(occ)
        } else {
            Ok(())
        };
        self.release();
        result
    }
}

impl Drop for Adapter<'_> {
    fn drop(&mut self) {
        let _ = self.abort();
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)] // Also compiled by the harness=false bench.
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn fully_encoded_maximum_transaction_fits_wal_slot() {
        let bytes = maximum_wal_record_bytes().unwrap();
        let raw_rows = bincode::serialized_size(&Record::default()).unwrap() as usize
            * super::super::model::SLOTS_PER_FAMILY;
        assert!(
            bytes > 2 * raw_rows,
            "must account for duplicate full payloads and framing"
        );
        assert!(
            bytes <= WAL_SLOT_BYTES,
            "maximum transaction is {bytes} bytes"
        );
    }

    #[test]
    fn attachment_preserves_existing_rows() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena");
        let shared = Shared::create(
            &path,
            8 << 20,
            &[Record {
                active: true,
                altitude: 37,
                ..Record::default()
            }],
        )
        .unwrap();
        let attached = Shared::attach(&shared.attachment(&path)).unwrap();
        assert_eq!(attached.snapshot().unwrap(), shared.snapshot().unwrap());
        assert_eq!(
            attached.table.shared_header_offset(),
            shared.table.shared_header_offset()
        );
    }

    #[test]
    fn attachment_rejects_missing_wrong_size_and_corrupt_files_without_modification() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena");
        let shared = Shared::create(&path, 8 << 20, &[Record::default()]).unwrap();
        let mut attachment = shared.attachment(&path);
        attachment.path = directory.path().join("missing");
        assert!(Shared::attach(&attachment).is_err());
        assert!(
            !attachment.path.exists(),
            "attachment must not create an absent arena"
        );

        attachment.path = path.clone();
        attachment.bytes += 4096;
        assert!(Shared::attach(&attachment).is_err());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            8 << 20,
            "attachment must not resize an arena"
        );

        attachment.bytes -= 4096;
        attachment.path = directory.path().join("invalid");
        let mut invalid = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&attachment.path)
            .unwrap();
        invalid.set_len(attachment.bytes as u64).unwrap();
        let corruption = [0x45; 16];
        invalid.write_all(&corruption).unwrap();
        let error = match Shared::attach(&attachment) {
            Err(error) => error,
            Ok(_) => panic!("invalid header must be rejected"),
        };
        assert!(error.contains("header is invalid"), "{error}");
        let mut after = [0; 16];
        std::fs::File::open(&attachment.path)
            .unwrap()
            .read_exact(&mut after)
            .unwrap();
        assert_eq!(
            after, corruption,
            "attachment must not reinitialize an invalid header"
        );
    }

    #[test]
    fn family_range_queries_do_not_materialize_unrelated_families() {
        let directory = tempfile::tempdir().unwrap();
        let mut rows = super::super::model::initial_records(2);
        for (family, first) in [(0, 0), (1, super::super::model::SLOTS_PER_FAMILY)] {
            rows[first] = Record {
                id: first,
                active: true,
                family,
                kind: SCHEDULED,
                due: 5,
                event_time: 1,
                ..Record::default()
            };
            rows[first + 1] = Record {
                id: first + 1,
                active: true,
                family,
                kind: super::super::model::POSITION,
                event_time: 1,
                ..Record::default()
            };
        }
        let shared = Shared::create(&directory.path().join("arena"), 8 << 20, &rows).unwrap();
        let mut adapter = Adapter::new(&shared);
        adapter.begin(&[]).unwrap();
        let due = adapter.query(&Query::Due { family: 0, at: 10 }).unwrap();
        assert_eq!(due.iter().map(|row| row.id).collect::<Vec<_>>(), vec![0]);
        assert_eq!(
            adapter.metrics.reads, 2,
            "only the requested family's candidates should be materialized"
        );
        let expired = adapter
            .query(&Query::Expired {
                family: 0,
                before: 10,
            })
            .unwrap();
        assert_eq!(
            expired.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            adapter.metrics.reads, 4,
            "only the requested family's indexed candidates should be materialized"
        );
        // Changing an unrelated family's non-key state must not invalidate
        // either query through an accidentally enrolled point-read dependency.
        let other = super::super::model::SLOTS_PER_FAMILY;
        let mut writer = shared.table.begin_transaction().unwrap();
        let mut changed = shared.table.read(&mut writer, other).unwrap().unwrap();
        changed.altitude += 1;
        shared.table.write(&mut writer, other, changed).unwrap();
        shared.table.commit(&mut writer).unwrap();
        adapter.commit().unwrap();
    }
}
