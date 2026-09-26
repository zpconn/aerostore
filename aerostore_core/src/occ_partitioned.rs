use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::index::{IndexCompare, IndexValue, SecondaryIndex};
use crate::procarray::{ProcArrayError, ProcArrayRegistration, ProcSnapshot};
use crate::shm::{ArenaClass, RelPtr, ShmAllocError, ShmArena, OCC_PARTITION_LOCKS};
use crate::shm_index::ShmIndexError;
use crate::shm_lock::{ShmMutex, ShmMutexGuard};
use crate::TxId;

const EMPTY_PTR: u32 = 0;
const COMMIT_LOCK_SPIN_LIMIT: u32 = 4096;
const INDEX_LOCK_SPIN_LIMIT: u32 = 4096;
const RECYCLE_SHARD_PROBE_LIMIT: usize = 4;
const MAX_VISIBLE_CHAIN_STEPS: u32 = 262_144;
const MAX_TABLE_INDEXES: usize = 32;
const OCC_HEADER_FORMAT: u64 = 0xAEB0_0CC0_0000_0002;

#[cfg(test)]
thread_local! {
    // An explicit deterministic interleaving seam, only compiled into unit
    // tests. Thread-local ownership avoids interference between parallel tests.
    static INDEX_PUBLICATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static INDEX_DESTINATION_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(usize)>>> =
        std::cell::RefCell::new(None);
    static INDEX_CANDIDATES_CAPTURED_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static INDEX_BUCKET_CONTENDED_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static TRANSACTION_FINISHING_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static TRANSACTION_FINISHED_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static ROW_PUBLICATION_STEP_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(usize, bool)>>> =
        std::cell::RefCell::new(None);
    static ROW_TRAVERSAL_STEP_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce(u32, u32)>>> =
        std::cell::RefCell::new(None);
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct StarvedRecycleKey {
    mmap_base: usize,
    mmap_len: usize,
    shared_header_offset: u32,
    recycle_shard: usize,
    row_size: usize,
    row_align: usize,
}

#[derive(Clone, Copy)]
struct StarvedRecycleEntry {
    key: StarvedRecycleKey,
    offset: u32,
}

thread_local! {
    static STARVED_RECYCLE_SLOT: std::cell::Cell<Option<StarvedRecycleEntry>> = const { std::cell::Cell::new(None) };
}

#[repr(C, align(64))]
struct OccSharedHeader {
    format: u64,
    index_registry_lock: ShmMutex,
    index_registry_sealed: AtomicBool,
    index_count: AtomicU32,
    index_offsets: [AtomicU32; MAX_TABLE_INDEXES],
    index_poisoned: AtomicBool,
    recycled_heads: [AtomicU32; OCC_PARTITION_LOCKS],
    recycle_locks: [ShmMutex; OCC_PARTITION_LOCKS],
    vacuum_requested: AtomicBool,
    recycle_alloc_from_starved: AtomicU64,
    recycle_alloc_from_primary: AtomicU64,
    recycle_alloc_from_probe: AtomicU64,
    recycle_alloc_fresh: AtomicU64,
    recycle_pop_empty: AtomicU64,
    recycle_pop_cas_fail: AtomicU64,
    recycle_push_success: AtomicU64,
    recycle_push_cas_fail: AtomicU64,
    recycle_stash_starved: AtomicU64,
    // Published once, with word zero released last. Payload words are immutable
    // after publication. A single ordered WAL stream is required for dependency
    // closure; changing it requires exclusive cold recovery into a new table.
    wal_stream: [AtomicU64; 4],
}

impl OccSharedHeader {
    #[inline]
    fn new() -> Self {
        Self {
            format: OCC_HEADER_FORMAT,
            index_registry_lock: ShmMutex::new(),
            index_registry_sealed: AtomicBool::new(false),
            index_count: AtomicU32::new(0),
            index_offsets: std::array::from_fn(|_| AtomicU32::new(0)),
            index_poisoned: AtomicBool::new(false),
            recycled_heads: std::array::from_fn(|_| AtomicU32::new(EMPTY_PTR)),
            recycle_locks: std::array::from_fn(|_| ShmMutex::new()),
            vacuum_requested: AtomicBool::new(false),
            recycle_alloc_from_starved: AtomicU64::new(0),
            recycle_alloc_from_primary: AtomicU64::new(0),
            recycle_alloc_from_probe: AtomicU64::new(0),
            recycle_alloc_fresh: AtomicU64::new(0),
            recycle_pop_empty: AtomicU64::new(0),
            recycle_pop_cas_fail: AtomicU64::new(0),
            recycle_push_success: AtomicU64::new(0),
            recycle_push_cas_fail: AtomicU64::new(0),
            recycle_stash_starved: AtomicU64::new(0),
            wal_stream: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[repr(C)]
struct OccIndexSlot {
    head: AtomicU32,
    indexed_update: ShmMutex,
}

impl OccIndexSlot {
    #[inline]
    fn new() -> Self {
        Self {
            head: AtomicU32::new(EMPTY_PTR),
            indexed_update: ShmMutex::new(),
        }
    }
}

#[repr(C)]
pub struct OccRow<T: Copy> {
    pub xmin: TxId,
    pub xmax: AtomicU64,
    pub is_locked: AtomicBool,
    lock_owner_txid: AtomicU64,
    next: AtomicU32,
    recycle_next: AtomicU32,
    pub value: T,
}

impl<T: Copy> OccRow<T> {
    #[inline]
    fn new(value: T, xmin: TxId, next: u32) -> Self {
        Self {
            xmin,
            xmax: AtomicU64::new(0),
            is_locked: AtomicBool::new(false),
            lock_owner_txid: AtomicU64::new(0),
            next: AtomicU32::new(next),
            recycle_next: AtomicU32::new(EMPTY_PTR),
            value,
        }
    }
}

pub struct RowLockGuard<'a, T: Copy + Send + Sync + 'static> {
    table: &'a OccTable<T>,
    row_ptr: RelPtr<OccRow<T>>,
    release_on_drop: bool,
}

/// Serializes participating writers from snapshot creation through secondary-index
/// maintenance. Unlike `RowLockGuard`, these locks belong to stable row slots, so
/// publishing a new MVCC version does not release their protection.
///
/// Acquire the complete set of rows before beginning the transaction, and keep
/// this guard until both commit and all index updates finish. Acquire all rows in
/// one call: acquiring additional rows while holding a guard can deadlock.
/// This optional application coordination does not provide predicate isolation.
/// Bind indexes with `OccTable::bind_index` for automatic transactional maintenance;
/// stable row guards alone cannot make external post-commit index edits atomic.
#[must_use = "the guard must be held through commit and index maintenance"]
pub struct IndexedUpdateGuard<'a> {
    _locks: Vec<ShmMutexGuard<'a>>,
}

struct ReadSetEntry<T: Copy> {
    row_id: usize,
    row_ptr: RelPtr<OccRow<T>>,
    observed_xmin: TxId,
}

#[derive(Clone, Copy)]
struct IndexRead {
    index_offset: u32,
    bucket: usize,
    stamp: u64,
}

struct BoundIndex<T> {
    index: SecondaryIndex<usize>,
    key: fn(&T) -> Option<IndexValue>,
}

struct IndexChange {
    binding: usize,
    row_id: usize,
    before: Option<IndexValue>,
    after: Option<IndexValue>,
}

struct PendingWrite<T: Copy> {
    row_id: usize,
    base_ptr: RelPtr<OccRow<T>>,
    new_ptr: RelPtr<OccRow<T>>,
    dirty_columns_bitmask: u64,
}

#[derive(Clone)]
struct Savepoint {
    name: String,
    write_len: usize,
}

pub struct OccTransaction<T: Copy> {
    table_offset: u32,
    arena_base: usize,
    index_reads: Vec<IndexRead>,
    index_conflict: bool,
    txid: TxId,
    snapshot_xmin: TxId,
    snapshot_xmax: TxId,
    snapshot_active: HashSet<TxId>,
    registration: Option<ProcArrayRegistration>,
    read_set: Vec<ReadSetEntry<T>>,
    write_set: Vec<PendingWrite<T>>,
    savepoints: Vec<Savepoint>,
}

impl<T: Copy> OccTransaction<T> {
    #[inline]
    pub fn txid(&self) -> TxId {
        self.txid
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OccCommittedWrite<T: Copy> {
    pub row_id: usize,
    pub base_offset: u32,
    pub new_offset: u32,
    pub base_value: T,
    pub value: T,
    pub dirty_columns_bitmask: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OccCommitRecord<T: Copy> {
    pub txid: TxId,
    pub writes: Vec<OccCommittedWrite<T>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VacuumReclaimedRow<T: Copy> {
    pub row_id: usize,
    pub reclaimed_value: T,
    pub live_head_value: Option<T>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OccRecycleTelemetry {
    pub alloc_from_starved: u64,
    pub alloc_from_primary: u64,
    pub alloc_from_probe: u64,
    pub alloc_fresh: u64,
    pub pop_empty: u64,
    pub pop_cas_fail: u64,
    pub push_success: u64,
    pub push_cas_fail: u64,
    pub stash_starved: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    RowOutOfBounds { row_id: usize, capacity: usize },
    RowMissing { row_id: usize },
    RowAlreadyExists { row_id: usize },
    SavepointNotFound { name: String },
    TransactionClosed,
    SerializationFailure,
    InvalidPointer { offset: u32 },
    ProcArray(String),
    Allocation(String),
    Index(String),
    IndexBindingsIncomplete,
    IndexRegistrationClosed,
    TransactionTableMismatch,
    WalStreamMismatch,
    WalRequired,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RowOutOfBounds { row_id, capacity } => write!(
                f,
                "row id {} is out of bounds for OCC table capacity {}",
                row_id, capacity
            ),
            Error::RowMissing { row_id } => write!(f, "row {} is missing", row_id),
            Error::RowAlreadyExists { row_id } => write!(f, "row {} already exists", row_id),
            Error::SavepointNotFound { name } => write!(f, "savepoint '{}' was not found", name),
            Error::TransactionClosed => write!(f, "transaction is already closed"),
            Error::SerializationFailure => write!(f, "serialization failure"),
            Error::InvalidPointer { offset } => {
                write!(
                    f,
                    "invalid shared-memory relative pointer offset {}",
                    offset
                )
            }
            Error::ProcArray(msg) => write!(f, "procarray error: {}", msg),
            Error::Allocation(msg) => write!(f, "shared allocation failed: {}", msg),
            Error::Index(msg) => write!(f, "transactional index error: {}", msg),
            Error::IndexBindingsIncomplete => {
                write!(f, "table handle must bind every registered secondary index")
            }
            Error::IndexRegistrationClosed => write!(
                f,
                "index registration is sealed after the first transaction"
            ),
            Error::TransactionTableMismatch => {
                write!(f, "transaction belongs to a different table or mapping")
            }
            Error::WalStreamMismatch => write!(f,
                "table is bound to another WAL stream; drain and recover exclusively before changing mode, WAL file, or ring"),
            Error::WalRequired => write!(f,
                "table is bound to a WAL stream; writes must use its OccCommitter"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ShmIndexError> for Error {
    fn from(value: ShmIndexError) -> Self {
        Error::Index(value.to_string())
    }
}

impl From<ProcArrayError> for Error {
    fn from(value: ProcArrayError) -> Self {
        Error::ProcArray(value.to_string())
    }
}

impl From<ShmAllocError> for Error {
    fn from(value: ShmAllocError) -> Self {
        Error::Allocation(value.to_string())
    }
}

pub struct OccTable<T: Copy + Send + Sync + 'static> {
    shm: Arc<ShmArena>,
    shared_header: RelPtr<OccSharedHeader>,
    index_slots: Vec<RelPtr<OccIndexSlot>>,
    indexes: Vec<BoundIndex<T>>,
    _marker: PhantomData<T>,
}

impl<T: Copy + Send + Sync + 'static> OccTable<T> {
    pub fn new(shm: Arc<ShmArena>, row_capacity: usize) -> Result<Self, Error> {
        let arena = shm.chunked_arena();
        let shared_header = arena.alloc(OccSharedHeader::new())?;

        let mut index_slots = Vec::with_capacity(row_capacity);
        for _ in 0..row_capacity {
            index_slots.push(arena.alloc(OccIndexSlot::new())?);
        }

        Ok(Self {
            shm,
            shared_header,
            index_slots,
            indexes: Vec::new(),
            _marker: PhantomData,
        })
    }

    pub fn from_existing(
        shm: Arc<ShmArena>,
        shared_header_offset: u32,
        index_slot_offsets: Vec<u32>,
    ) -> Result<Self, Error> {
        let shared_header = RelPtr::<OccSharedHeader>::from_offset(shared_header_offset);
        if !shared_header
            .as_ref(shm.mmap_base())
            .is_some_and(|header| header.format == OCC_HEADER_FORMAT)
        {
            return Err(Error::InvalidPointer {
                offset: shared_header_offset,
            });
        }

        let mut index_slots = Vec::with_capacity(index_slot_offsets.len());
        for offset in index_slot_offsets {
            let ptr = RelPtr::<OccIndexSlot>::from_offset(offset);
            if ptr.as_ref(shm.mmap_base()).is_none() {
                return Err(Error::InvalidPointer { offset });
            }
            index_slots.push(ptr);
        }

        Ok(Self {
            shm,
            shared_header,
            index_slots,
            indexes: Vec::new(),
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.index_slots.len()
    }

    #[inline]
    pub fn shared_header_offset(&self) -> u32 {
        self.shared_header.load(Ordering::Acquire)
    }

    pub fn index_slot_offsets(&self) -> Vec<u32> {
        self.index_slots
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .collect()
    }

    /// Register a secondary index before the first transaction. The extractor
    /// must be deterministic and identical for all attached handles. Existing
    /// rows and postings must already agree when the index is first bound.
    ///
    /// Once bound, every ordinary commit maintains this index. Independent
    /// attachments must bind the complete shared registry before transactions
    /// can begin; omission therefore cannot silently leave an index stale.
    pub fn bind_index(
        &mut self,
        index: SecondaryIndex<usize>,
        key: fn(&T) -> Option<IndexValue>,
    ) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.shm, index.shared_arena()) {
            return Err(Error::Index(
                "index and table must share one arena handle".into(),
            ));
        }
        let offset = index.header_offset();
        if self
            .indexes
            .iter()
            .any(|bound| bound.index.header_offset() == offset)
        {
            return Err(Error::Index(
                "secondary index is already bound on this handle".into(),
            ));
        }
        {
            let header = self.shared_header_ref()?;
            let _registry = header.index_registry_lock.lock();
            if header.index_poisoned.load(Ordering::Acquire) {
                return Err(Error::Index(
                    "table is poisoned after failed index publication".into(),
                ));
            }
            let count = header.index_count.load(Ordering::Acquire) as usize;
            let existing = header.index_offsets[..count]
                .iter()
                .any(|value| value.load(Ordering::Acquire) == offset);
            if !existing {
                if header.index_registry_sealed.load(Ordering::Acquire) {
                    return Err(Error::IndexRegistrationClosed);
                }
                if count == MAX_TABLE_INDEXES {
                    return Err(Error::Index("too many secondary indexes for table".into()));
                }
                // Initial registration is a quiescent bootstrap operation.
                // Validate actual postings, preserving duplicates, so an empty
                // or stale index cannot become a silently trusted access path.
                let mut expected = self
                    .snapshot_latest_rows()?
                    .into_iter()
                    .filter_map(|(row_id, value)| key(&value).map(|key| (key, row_id)))
                    .collect::<Vec<_>>();
                let mut actual = index.try_entries()?;
                expected.sort_unstable();
                actual.sort_unstable();
                if expected != actual {
                    return Err(Error::Index(
                        "initial index postings do not match table rows".into(),
                    ));
                }
            }
            index.transactional_bind(self.shared_header_offset())?;
            if !existing {
                header.index_offsets[count].store(offset, Ordering::Release);
                header
                    .index_count
                    .store((count + 1) as u32, Ordering::Release);
            }
        }
        self.indexes.push(BoundIndex { index, key });
        self.indexes
            .sort_unstable_by_key(|bound| bound.index.header_offset());
        Ok(())
    }

    pub fn index_is_bound(&self, index: &SecondaryIndex<usize>) -> bool {
        Arc::ptr_eq(&self.shm, index.shared_arena())
            && self
                .indexes
                .iter()
                .any(|bound| bound.index.header_offset() == index.header_offset())
    }

    /// Return the complete matching row-id set for this transaction's snapshot,
    /// including its own pending writes. Empty predicates are dependencies too.
    /// Indexes hold only current postings: if a relevant bucket has changed
    /// since the transaction began, conservatively reject instead of silently
    /// omitting a historical candidate. Hash collisions and broad range scans
    /// can cause additional serialization retries, never missing rows.
    pub fn index_lookup(
        &self,
        tx: &mut OccTransaction<T>,
        index: &SecondaryIndex<usize>,
        predicate: &IndexCompare,
    ) -> Result<Vec<usize>, Error> {
        self.ensure_open(tx)?;
        if !self.index_is_bound(index) {
            return Err(Error::Index(
                "query index is not bound to this table".into(),
            ));
        }
        let binding = self
            .indexes
            .iter()
            .find(|bound| bound.index.header_offset() == index.header_offset())
            .expect("bound index checked above");
        index.transactional_check_owner(self.shared_header_offset())?;
        let buckets = index.transactional_bucket_ids(predicate)?;
        let mut guards = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            let guard = match Self::acquire_index_bucket(index, *bucket) {
                Ok(guard) => guard,
                Err(Error::SerializationFailure) => {
                    tx.index_conflict = true;
                    return Err(Error::SerializationFailure);
                }
                Err(err) => return Err(err),
            };
            guards.push(guard);
        }
        for bucket in &buckets {
            let stamp = index.transactional_stamp(*bucket)?;
            if !aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid) {
                tx.index_conflict = true;
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::LookupPostSnapshotStamp, Some(index.header_offset()), None);
                return Err(Error::SerializationFailure);
            }
            if let Some(previous) = tx
                .index_reads
                .iter()
                .find(|read| read.index_offset == index.header_offset() && read.bucket == *bucket)
            {
                if previous.stamp != stamp {
                    tx.index_conflict = true;
                    #[cfg(feature = "retry-diagnostics")]
                    crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::LookupChangedCapturedStamp, Some(index.header_offset()), None);
                    return Err(Error::SerializationFailure);
                }
            } else {
                tx.index_reads.push(IndexRead {
                    index_offset: index.header_offset(),
                    bucket: *bucket,
                    stamp,
                });
            }
        }
        let candidates = index.transactional_raw_lookup(predicate)?;
        // Candidate completeness and predicate stamps are now captured. Stable
        // row IDs and the pinned MVCC horizon let us materialize without holding
        // index latches; later mutations are checked again at commit. In
        // particular, a broad range query need not exclude writers while it
        // filters rows or grows its concrete row-read set.
        drop(guards);
        #[cfg(test)]
        INDEX_CANDIDATES_CAPTURED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let mut candidates: BTreeSet<usize> = candidates.into_iter().collect();
        candidates.extend(tx.write_set.iter().map(|write| write.row_id));
        let mut result = Vec::new();
        for row_id in candidates {
            if let Some(value) = self.read(tx, row_id)? {
                if (binding.key)(&value)
                    .as_ref()
                    .is_some_and(|key| index_predicate_matches(predicate, key))
                {
                    result.push(row_id);
                }
            }
        }
        Ok(result)
    }

    fn validate_index_bindings(&self) -> Result<(), Error> {
        let header = self.shared_header_ref()?;
        if header.index_poisoned.load(Ordering::Acquire) {
            return Err(Error::Index(
                "table is poisoned after failed index publication".into(),
            ));
        }
        let count = header.index_count.load(Ordering::Acquire) as usize;
        if count != self.indexes.len() || count > MAX_TABLE_INDEXES {
            return Err(Error::IndexBindingsIncomplete);
        }
        for offset in &header.index_offsets[..count] {
            let offset = offset.load(Ordering::Acquire);
            let Some(bound) = self
                .indexes
                .iter()
                .find(|bound| bound.index.header_offset() == offset)
            else {
                return Err(Error::IndexBindingsIncomplete);
            };
            bound
                .index
                .transactional_check_owner(self.shared_header_offset())?;
        }
        Ok(())
    }

    // Admission checks must also run after a potentially long preparation or
    // lock wait. This is a health observation, not cancellation of writers
    // already admitted on other partitions.
    pub(crate) fn check_not_poisoned(&self) -> Result<(), Error> {
        if self
            .shared_header_ref()?
            .index_poisoned
            .load(Ordering::Acquire)
        {
            return Err(Error::Index(
                "table is poisoned after failed index publication".into(),
            ));
        }
        Ok(())
    }

    fn ensure_unmanaged_write(&self) -> Result<(), Error> {
        self.ensure_unlogged_write_allowed()?;
        if self
            .shared_header_ref()?
            .index_count
            .load(Ordering::Acquire)
            != 0
        {
            return Err(Error::Index(
                "direct seed/recovery writes are forbidden after an index is bound".into(),
            ));
        }
        Ok(())
    }

    fn ensure_unlogged_write_allowed(&self) -> Result<(), Error> {
        if self.shared_header_ref()?.wal_stream[0].load(Ordering::Acquire) != 0 {
            return Err(Error::WalRequired);
        }
        Ok(())
    }

    pub(crate) fn check_wal_stream(&self, identity: [u64; 4]) -> Result<(), Error> {
        let header = self.shared_header_ref()?;
        let kind = header.wal_stream[0].load(Ordering::Acquire);
        if kind != 0
            && (kind != identity[0]
                || (1..4).any(|i| header.wal_stream[i].load(Ordering::Relaxed) != identity[i]))
        {
            return Err(Error::WalStreamMismatch);
        }
        Ok(())
    }

    pub(crate) fn bind_wal_stream(&self, identity: [u64; 4]) -> Result<(), Error> {
        let header = self.shared_header_ref()?;
        if header.wal_stream[0].load(Ordering::Acquire) != 0 {
            return self.check_wal_stream(identity);
        }
        // Only first binding needs exclusion from every publishing writer.
        // Follow index registration's existing registry -> partition order.
        let _registry = header.index_registry_lock.lock();
        let _partitions = self.acquire_all_partition_locks();
        if header.wal_stream[0].load(Ordering::Acquire) == 0 {
            for i in 1..4 {
                header.wal_stream[i].store(identity[i], Ordering::Relaxed);
            }
            header.wal_stream[0].store(identity[0], Ordering::Release);
        }
        self.check_wal_stream(identity)
    }

    fn poison_indexes(&self) {
        if let Ok(header) = self.shared_header_ref() {
            header.index_poisoned.store(true, Ordering::Release);
        }
        for bound in &self.indexes {
            let _ = bound.index.transactional_poison();
        }
    }

    fn index_changes(
        &self,
        tx: &OccTransaction<T>,
        final_writes: &[usize],
    ) -> Result<Vec<IndexChange>, Error> {
        let mut changes = Vec::new();
        for write_idx in final_writes {
            let write = &tx.write_set[*write_idx];
            let before = &self.resolve_row_ptr(&write.base_ptr)?.value;
            let after = &self.resolve_row_ptr(&write.new_ptr)?.value;
            for (binding, bound) in self.indexes.iter().enumerate() {
                let before = (bound.key)(before);
                let after = (bound.key)(after);
                if before == after {
                    continue;
                }
                if let Some(key) = &before {
                    bound.index.transactional_prevalidate(key, &write.row_id)?;
                }
                if let Some(key) = &after {
                    bound.index.transactional_prevalidate(key, &write.row_id)?;
                }
                changes.push(IndexChange {
                    binding,
                    row_id: write.row_id,
                    before,
                    after,
                });
            }
        }
        Ok(changes)
    }

    fn index_lock_keys(
        &self,
        tx: &OccTransaction<T>,
        changes: &[IndexChange],
    ) -> Result<Vec<(usize, usize)>, Error> {
        let mut keys = BTreeSet::new();
        for read in &tx.index_reads {
            let binding = self
                .indexes
                .iter()
                .position(|bound| bound.index.header_offset() == read.index_offset)
                .ok_or(Error::IndexBindingsIncomplete)?;
            keys.insert((binding, read.bucket));
        }
        for change in changes {
            let index = &self.indexes[change.binding].index;
            for key in [change.before.as_ref(), change.after.as_ref()]
                .into_iter()
                .flatten()
            {
                keys.insert((change.binding, index.transactional_key_bucket(key)?));
            }
        }
        Ok(keys.into_iter().collect())
    }

    fn acquire_index_locks(
        &self,
        keys: &[(usize, usize)],
    ) -> Result<Vec<ShmMutexGuard<'_>>, Error> {
        let mut guards = Vec::with_capacity(keys.len());
        for (binding, bucket) in keys {
            guards.push(Self::acquire_index_bucket(
                &self.indexes[*binding].index,
                *bucket,
            )?);
        }
        Ok(guards)
    }

    fn acquire_index_bucket(
        index: &SecondaryIndex<usize>,
        bucket: usize,
    ) -> Result<ShmMutexGuard<'_>, Error> {
        // Publication usually holds a bucket for only microseconds. Retry that
        // short contention locally instead of immediately turning it into an
        // application-level transaction retry and millisecond backoff. Every
        // caller acquires canonical bucket order, and this wait remains bounded.
        for attempt in 0..INDEX_LOCK_SPIN_LIMIT {
            if let Some(guard) = index.transactional_try_lock_bucket(bucket)? {
                return Ok(guard);
            }
            #[cfg(test)]
            if attempt == 0 {
                INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
            }
            if attempt & 0x3f == 0x3f {
                std::thread::yield_now();
            }
            std::hint::spin_loop();
        }
        #[cfg(feature = "retry-diagnostics")]
        crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::IndexBucketBusy, Some(index.header_offset()), None);
        Err(Error::SerializationFailure)
    }

    fn index_read_conflict(&self, tx: &OccTransaction<T>) -> Result<bool, Error> {
        if tx.index_conflict {
            #[cfg(feature = "retry-diagnostics")]
            crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::StickyIndexConflict, None, None);
            return Ok(true);
        }
        for read in &tx.index_reads {
            let bound = self
                .indexes
                .iter()
                .find(|bound| bound.index.header_offset() == read.index_offset)
                .ok_or(Error::IndexBindingsIncomplete)?;
            let stamp = bound.index.transactional_stamp(read.bucket)?;
            if stamp != read.stamp || !aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid) {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::PredicateValidationStamp, Some(read.index_offset), None);
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Destination insertion is the only allocating phase. If any insertion
    // fails, remove just the successful additions: source postings are intact,
    // and rollback needs no allocation. A failure to undo or remove is fatal
    // and poisons the shared table and every registered index.
    fn prepare_index_destinations(&self, changes: &[IndexChange]) -> Result<Vec<usize>, Error> {
        let mut inserted = Vec::new();
        for (change_idx, change) in changes.iter().enumerate() {
            if let Some(after) = &change.after {
                let index = &self.indexes[change.binding].index;
                if let Err(err) = index.transactional_insert(after.clone(), change.row_id) {
                    if let Err(undo) = self.rollback_index_destinations(changes, &inserted) {
                        return Err(Error::Index(format!("destination insertion failed ({err}); rollback failed ({undo}); table poisoned")));
                    }
                    return Err(err.into());
                }
                inserted.push(change_idx);
                #[cfg(test)]
                INDEX_DESTINATION_HOOK.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().as_mut() {
                        hook(inserted.len());
                    }
                });
            }
        }
        Ok(inserted)
    }

    fn rollback_index_destinations(
        &self,
        changes: &[IndexChange],
        inserted: &[usize],
    ) -> Result<(), Error> {
        let mut rollback_error = None;
        for idx in inserted.iter().rev() {
            let previous = &changes[*idx];
            if let Err(undo) = self.indexes[previous.binding].index.transactional_remove(
                previous.after.as_ref().expect("inserted destination"),
                &previous.row_id,
            ) {
                rollback_error = Some(undo.to_string());
            }
        }
        if let Some(undo) = rollback_error {
            self.poison_indexes();
            return Err(Error::Index(format!(
                "destination rollback failed ({undo}); table poisoned"
            )));
        }
        Ok(())
    }

    fn remove_index_sources(&self, changes: &[IndexChange]) -> Result<(), Error> {
        for change in changes {
            if let Some(before) = &change.before {
                if let Err(err) = self.indexes[change.binding]
                    .index
                    .transactional_remove(before, &change.row_id)
                {
                    self.poison_indexes();
                    return Err(Error::Index(format!(
                        "source removal failed ({err}); table poisoned"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn poison_after_wal_failure(&self) {
        self.poison_indexes();
    }

    fn publish_index_stamps(&self, changes: &[IndexChange]) -> Result<(), Error> {
        if changes.is_empty() {
            return Ok(());
        }
        // A publication clock shares the txid allocator, but is reserved only
        // AFTER deregistration. Thus even an older, late-committing writer gets
        // a stamp newer than every transaction whose snapshot might exclude it.
        // Bucket latches remain held until all stamps have been published.
        let stamp = self.shm.global_txid().fetch_add(1, Ordering::AcqRel);
        let mut touched = BTreeSet::new();
        for change in changes {
            for key in [change.before.as_ref(), change.after.as_ref()]
                .into_iter()
                .flatten()
            {
                touched.insert((
                    change.binding,
                    self.indexes[change.binding]
                        .index
                        .transactional_key_bucket(key)?,
                ));
            }
        }
        for (binding, bucket) in touched {
            self.indexes[binding]
                .index
                .transactional_publish_stamp(bucket, stamp)?;
        }
        Ok(())
    }

    /// Locks stable, shared-memory row slots in a deterministic order.
    ///
    /// These locks are separate from OCC's partition locks, so committing while
    /// holding the returned guard is safe. They coordinate independently
    /// attached table handles and processes as well as threads.
    pub fn lock_indexed_rows(&self, row_ids: &[usize]) -> Result<IndexedUpdateGuard<'_>, Error> {
        let slots = self.indexed_update_slots(row_ids)?;
        Ok(IndexedUpdateGuard {
            _locks: slots
                .iter()
                .map(|slot| slot.indexed_update.lock())
                .collect(),
        })
    }

    /// Attempts the same protocol without waiting. On contention, every lock
    /// acquired by this call is released before returning `None`.
    pub fn try_lock_indexed_rows(
        &self,
        row_ids: &[usize],
    ) -> Result<Option<IndexedUpdateGuard<'_>>, Error> {
        let slots = self.indexed_update_slots(row_ids)?;
        let mut locks = Vec::with_capacity(slots.len());
        for slot in slots {
            let Some(lock) = slot.indexed_update.try_lock() else {
                return Ok(None);
            };
            locks.push(lock);
        }
        Ok(Some(IndexedUpdateGuard { _locks: locks }))
    }

    fn indexed_update_slots(&self, row_ids: &[usize]) -> Result<Vec<&OccIndexSlot>, Error> {
        let mut row_ids = row_ids.to_vec();
        row_ids.sort_unstable();
        row_ids.dedup();
        // Validate every ID before taking any lock.
        row_ids
            .into_iter()
            .map(|row_id| self.slot_ref(row_id))
            .collect()
    }

    /// Bootstrap a stable physical row slot before concurrent transactions.
    /// The caller must ensure no transaction or reader is using this table.
    /// Runtime creation should activate a preseeded logical slot via `write`;
    /// seeding provides no predicate dependency and is forbidden after binding
    /// an index. Full-slot query scans rely on this stable-slot lifecycle.
    pub fn seed_row(&self, row_id: usize, value: T) -> Result<(), Error> {
        self.ensure_unmanaged_write()?;
        let slot = self.slot_ref(row_id)?;
        let seed_txid = self.shm.global_txid().fetch_add(1, Ordering::AcqRel);
        let row_ptr = self.allocate_row(row_id, value, seed_txid, EMPTY_PTR)?;
        let row_offset = row_ptr.load(Ordering::Acquire);

        if slot
            .head
            .compare_exchange(0, row_offset, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::RowAlreadyExists { row_id });
        }

        Ok(())
    }

    pub fn begin_transaction(&self) -> Result<OccTransaction<T>, Error> {
        {
            let header = self.shared_header_ref()?;
            let _registry = header.index_registry_lock.lock();
            self.validate_index_bindings()?;
            header.index_registry_sealed.store(true, Ordering::Release);
        }
        let registration = self.shm.begin_transaction()?;
        let snapshot = match self.shm.create_transaction_snapshot(registration) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let _ = self.shm.end_transaction(registration);
                return Err(err.into());
            }
        };
        let mut snapshot_active = HashSet::with_capacity(snapshot.len());
        for txid in snapshot.in_flight_txids() {
            if *txid != registration.txid {
                snapshot_active.insert(*txid);
            }
        }

        Ok(OccTransaction {
            table_offset: self.shared_header_offset(),
            arena_base: self.shm.mmap_base().as_ptr() as usize,
            index_reads: Vec::new(),
            index_conflict: false,
            txid: registration.txid,
            snapshot_xmin: snapshot.xmin,
            snapshot_xmax: snapshot.xmax,
            snapshot_active,
            registration: Some(registration),
            read_set: Vec::new(),
            write_set: Vec::new(),
            savepoints: Vec::new(),
        })
    }

    pub fn savepoint<S: Into<String>>(
        &self,
        tx: &mut OccTransaction<T>,
        name: S,
    ) -> Result<(), Error> {
        self.ensure_open(tx)?;
        tx.savepoints.push(Savepoint {
            name: name.into(),
            write_len: tx.write_set.len(),
        });
        Ok(())
    }

    pub fn rollback_to(&self, tx: &mut OccTransaction<T>, name: &str) -> Result<(), Error> {
        self.ensure_open(tx)?;
        let Some(savepoint_idx) = tx.savepoints.iter().rposition(|s| s.name == name) else {
            return Err(Error::SavepointNotFound {
                name: name.to_string(),
            });
        };

        let write_len = tx.savepoints[savepoint_idx].write_len;
        self.recycle_write_suffix(tx, write_len)?;
        tx.write_set.truncate(write_len);
        tx.savepoints.truncate(savepoint_idx + 1);
        Ok(())
    }

    pub fn lock_for_update(
        &self,
        tx: &OccTransaction<T>,
        row_id: usize,
    ) -> Result<RowLockGuard<'_, T>, Error> {
        self.ensure_open(tx)?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }

        let _lock = self.acquire_row_lock(row_id);
        let Some(row_ptr) = self.find_visible_row_ptr(tx, row_id)? else {
            return Err(Error::RowMissing { row_id });
        };
        let row = self.resolve_row_ptr(&row_ptr)?;

        if self.row_locked_by_other_tx(row, tx.txid) {
            std::thread::yield_now();
            #[cfg(feature = "retry-diagnostics")]
            crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::LockForUpdateHeld, None, Some(row_id));
            return Err(Error::SerializationFailure);
        }

        let release_on_drop = if row
            .is_locked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            row.lock_owner_txid.store(tx.txid, Ordering::Release);
            true
        } else {
            let owner = row.lock_owner_txid.load(Ordering::Acquire);
            if owner == tx.txid {
                false
            } else {
                std::thread::yield_now();
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::LockForUpdateRace, None, Some(row_id));
                return Err(Error::SerializationFailure);
            }
        };

        Ok(RowLockGuard {
            table: self,
            row_ptr,
            release_on_drop,
        })
    }

    pub fn read(&self, tx: &mut OccTransaction<T>, row_id: usize) -> Result<Option<T>, Error> {
        self.ensure_open(tx)?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }

        if let Some(pending) = tx
            .write_set
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
        {
            let pending_row = self.resolve_row_ptr(&pending.new_ptr)?;
            return Ok(Some(pending_row.value));
        }

        if let Some(row_ptr) = self.find_visible_row_ptr(tx, row_id)? {
            let row = self.resolve_row_ptr(&row_ptr)?;
            if self.row_locked_by_other_tx(row, tx.txid) {
                std::thread::yield_now();
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::ReadRowLocked, None, Some(row_id));
                return Err(Error::SerializationFailure);
            }
            let observed_xmin = row.xmin;
            self.record_read(tx, row_id, row_ptr, observed_xmin);
            return Ok(Some(row.value));
        }

        Ok(None)
    }

    pub fn write(&self, tx: &mut OccTransaction<T>, row_id: usize, value: T) -> Result<(), Error> {
        self.ensure_open(tx)?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }

        let base_ptr = if let Some(last_for_row) = tx
            .write_set
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
        {
            last_for_row.base_ptr.clone()
        } else {
            let visible_ptr = self
                .find_visible_row_ptr(tx, row_id)?
                .ok_or(Error::RowMissing { row_id })?;
            let row = self.resolve_row_ptr(&visible_ptr)?;
            if self.row_locked_by_other_tx(row, tx.txid) {
                std::thread::yield_now();
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::WriteRowLocked, None, Some(row_id));
                return Err(Error::SerializationFailure);
            }
            visible_ptr
        };

        let base_row = self.resolve_row_ptr(&base_ptr)?;
        let dirty_columns_bitmask =
            crate::wal_delta::coarse_dirty_mask_for_copy(&base_row.value, &value);

        let base_offset = base_ptr.load(Ordering::Acquire);
        let new_ptr = self.allocate_row_for_write(row_id, value, tx.txid, base_offset)?;

        tx.write_set.push(PendingWrite {
            row_id,
            base_ptr,
            new_ptr,
            dirty_columns_bitmask,
        });
        Ok(())
    }

    pub fn write_with_dirty_mask(
        &self,
        tx: &mut OccTransaction<T>,
        row_id: usize,
        value: T,
        dirty_columns_bitmask: u64,
    ) -> Result<(), Error> {
        self.ensure_open(tx)?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }

        let base_ptr = if let Some(last_for_row) = tx
            .write_set
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
        {
            last_for_row.base_ptr.clone()
        } else {
            let visible_ptr = self
                .find_visible_row_ptr(tx, row_id)?
                .ok_or(Error::RowMissing { row_id })?;
            let row = self.resolve_row_ptr(&visible_ptr)?;
            if self.row_locked_by_other_tx(row, tx.txid) {
                std::thread::yield_now();
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::WriteDirtyRowLocked, None, Some(row_id));
                return Err(Error::SerializationFailure);
            }
            visible_ptr
        };

        let base_offset = base_ptr.load(Ordering::Acquire);
        let new_ptr = self.allocate_row_for_write(row_id, value, tx.txid, base_offset)?;

        tx.write_set.push(PendingWrite {
            row_id,
            base_ptr,
            new_ptr,
            dirty_columns_bitmask,
        });
        Ok(())
    }

    pub fn abort(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        if tx.table_offset != self.shared_header_offset()
            || tx.arena_base != self.shm.mmap_base().as_ptr() as usize
        {
            return Err(Error::TransactionTableMismatch);
        }
        self.clear_local_sets(tx)?;
        let _ = self.shm.flush_local_recycle_caches();
        self.finish_transaction(tx)
    }

    pub fn commit(&self, tx: &mut OccTransaction<T>) -> Result<usize, Error> {
        Ok(self.commit_with_record(tx)?.writes.len())
    }

    pub fn commit_with_record(
        &self,
        tx: &mut OccTransaction<T>,
    ) -> Result<OccCommitRecord<T>, Error> {
        self.commit_with_record_impl::<false, Error, _, _>(tx, |_| {
            Ok(|_: &OccCommitRecord<T>| Ok(()))
        })
    }

    /// Invoke the durability step after validation and destination allocation,
    /// while the row and predicate locks still exclude conflicting publication.
    /// A rejected callback leaves all rows and source postings unchanged.
    pub(crate) fn commit_with_record_before_publish<E, F>(
        &self,
        tx: &mut OccTransaction<T>,
        before_publish: F,
    ) -> Result<OccCommitRecord<T>, E>
    where
        E: From<Error>,
        F: FnOnce(&OccCommitRecord<T>) -> Result<(), E>,
    {
        self.commit_with_record_prepared::<E, _, F>(tx, |_| Ok(before_publish))
    }

    /// Prepare an immutable payload before taking publication locks, then
    /// accept it after validating the unchanged transaction under those locks.
    /// Keeping both phases inside this call prevents transaction mutation
    /// between preparation and validation. The returned closure must not accept
    /// WAL bytes until invoked, and receives the same record used to publish.
    pub(crate) fn commit_with_record_prepared<E, P, F>(
        &self,
        tx: &mut OccTransaction<T>,
        prepare: P,
    ) -> Result<OccCommitRecord<T>, E>
    where
        E: From<Error>,
        P: FnOnce(&OccCommitRecord<T>) -> Result<F, E>,
        F: FnOnce(&OccCommitRecord<T>) -> Result<(), E>,
    {
        self.commit_with_record_impl::<true, E, P, F>(tx, prepare)
    }

    // The const parameter removes record preparation and callback handling from
    // the ordinary in-memory commit. Both paths use the same validation and
    // publication protocol; WAL commits reuse their prepared record allocation.
    fn commit_with_record_impl<const WRITE_AHEAD: bool, E, P, F>(
        &self,
        tx: &mut OccTransaction<T>,
        prepare: P,
    ) -> Result<OccCommitRecord<T>, E>
    where
        E: From<Error>,
        P: FnOnce(&OccCommitRecord<T>) -> Result<F, E>,
        F: FnOnce(&OccCommitRecord<T>) -> Result<(), E>,
    {
        self.ensure_open(tx)?;
        let final_write_indices = self.final_write_indices(tx);
        let index_changes = self.index_changes(tx, &final_write_indices)?;
        let index_keys = self.index_lock_keys(tx, &index_changes)?;
        // Values are immutable and retained by the live transaction, just as
        // for index_changes above. Revalidate their dependencies under locks
        // before accepting any prepared bytes or publishing any row.
        let prepared = if WRITE_AHEAD {
            Some(self.prepare_before_publish(tx, &final_write_indices, prepare)?)
        } else {
            None
        };
        let index_locks = match self.acquire_index_locks(&index_keys) {
            Ok(locks) => locks,
            Err(Error::SerializationFailure) => {
                self.abort_for_serialization_failure(tx);
                return Err(Error::SerializationFailure.into());
            }
            Err(err) => return Err(err.into()),
        };
        let locks = match self.acquire_partition_locks(tx) {
            Ok(locks) => locks,
            Err(Error::SerializationFailure) => {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::PartitionLockBusy, None, None);
                drop(index_locks);
                self.abort_for_serialization_failure(tx);
                return Err(Error::SerializationFailure.into());
            }
            Err(err) => return Err(err.into()),
        };

        if !WRITE_AHEAD && !final_write_indices.is_empty() {
            self.ensure_unlogged_write_allowed()?;
        }

        if let Err(err) = self.check_not_poisoned() {
            drop(locks);
            drop(index_locks);
            self.abort_preparation(tx)?;
            return Err(err.into());
        }

        if self.index_read_conflict(tx)?
            || self.has_row_lock_conflict(tx)?
            || self.has_serialization_conflict(tx)?
            || self.has_write_base_conflict(tx, &final_write_indices)?
        {
            drop(locks);
            drop(index_locks);
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure.into());
        }

        // All index destinations are allocated before any source is removed;
        // no table version is published until that preparation has succeeded.
        // Readers of affected predicates reject the held bucket latches.
        let inserted = self.prepare_index_destinations(&index_changes)?;
        let prepared = match prepared {
            Some((record, before_publish)) => {
                self.invoke_before_publish(tx, &index_changes, &inserted, &record, before_publish)?;
                Some(record)
            }
            None => None,
        };
        self.remove_index_sources(&index_changes)?;
        let publication = match prepared {
            Some(record) => self
                .publish_prepared_write_set(&record)
                .map(|()| record.writes),
            None => self.publish_write_set(tx, &final_write_indices),
        };
        let writes = match publication {
            Ok(writes) => writes,
            Err(err) => {
                // Pointer/CAS failure after validation indicates corruption.
                // Some heads might already be published: do not recycle them
                // as aborted private versions, and never allow more commits.
                self.poison_indexes();
                tx.write_set.clear();
                tx.read_set.clear();
                tx.index_reads.clear();
                let _ = self.finish_transaction(tx);
                return Err(Error::Index(format!(
                    "row publication failed after index preparation ({err}); table poisoned"
                ))
                .into());
            }
        };
        #[cfg(test)]
        INDEX_PUBLICATION_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let commit_record = OccCommitRecord {
            txid: tx.txid,
            writes,
        };
        if let Err(err) = self.recycle_non_final_writes(tx, &final_write_indices) {
            self.poison_indexes();
            tx.write_set.clear();
            let _ = self.finish_transaction(tx);
            return Err(err.into());
        }
        tx.read_set.clear();
        tx.index_reads.clear();
        tx.index_conflict = false;
        tx.write_set.clear();
        tx.savepoints.clear();
        let _ = self.shm.flush_local_recycle_caches();
        let finish = match self.finish_transaction(tx) {
            Ok(()) => self.publish_index_stamps(&index_changes),
            Err(err) => Err(err),
        };
        if let Err(err) = finish {
            self.poison_indexes();
            return Err(err.into());
        }
        // Keep both row partition locks and index predicate latches until the
        // complete transaction is visible and its publication stamps exist.
        drop(locks);
        drop(index_locks);
        Ok(commit_record)
    }

    fn prepare_before_publish<E, P, F>(
        &self,
        tx: &mut OccTransaction<T>,
        final_write_indices: &[usize],
        prepare: P,
    ) -> Result<(OccCommitRecord<T>, F), E>
    where
        E: From<Error>,
        P: FnOnce(&OccCommitRecord<T>) -> Result<F, E>,
    {
        let record = match self.prepare_commit_record(tx, final_write_indices) {
            Ok(record) => record,
            Err(err) => {
                self.abort_preparation(tx)?;
                return Err(err.into());
            }
        };
        // Application codecs may unwind. At this point there are no prepared
        // destinations to undo, but private versions and registration still
        // require cleanup before returning or resuming the panic.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prepare(&record))) {
            Ok(Ok(before_publish)) => Ok((record, before_publish)),
            Ok(Err(err)) => {
                self.abort_preparation(tx)?;
                Err(err)
            }
            Err(panic) => {
                let _ = self.abort_preparation(tx);
                std::panic::resume_unwind(panic)
            }
        }
    }

    fn abort_preparation(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        let abort = self.abort(tx);
        if abort.is_err() {
            self.poison_indexes();
        }
        abort
    }

    fn invoke_before_publish<E, F>(
        &self,
        tx: &mut OccTransaction<T>,
        changes: &[IndexChange],
        inserted: &[usize],
        record: &OccCommitRecord<T>,
        before_publish: F,
    ) -> Result<(), E>
    where
        E: From<Error>,
        F: FnOnce(&OccCommitRecord<T>) -> Result<(), E>,
    {
        // Codecs can be application code. Even an unwinding codec must not leave
        // an unlogged destination posting or a registered abandoned transaction.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| before_publish(record))) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                self.rollback_prepared_commit(tx, changes, inserted)?;
                Err(err)
            }
            Err(panic) => {
                let _ = self.rollback_prepared_commit(tx, changes, inserted);
                std::panic::resume_unwind(panic)
            }
        }
    }

    fn rollback_prepared_commit(
        &self,
        tx: &mut OccTransaction<T>,
        changes: &[IndexChange],
        inserted: &[usize],
    ) -> Result<(), Error> {
        let rollback = self.rollback_index_destinations(changes, inserted);
        let abort = self.abort(tx);
        if abort.is_err() {
            self.poison_indexes();
        }
        rollback?;
        abort
    }

    fn prepare_commit_record(
        &self,
        tx: &OccTransaction<T>,
        final_write_indices: &[usize],
    ) -> Result<OccCommitRecord<T>, Error> {
        let mut writes = Vec::with_capacity(final_write_indices.len());
        for idx in final_write_indices {
            let write = &tx.write_set[*idx];
            let base_offset = write.base_ptr.load(Ordering::Acquire);
            let new_offset = write.new_ptr.load(Ordering::Acquire);
            let new_row = self.resolve_row_ptr(&write.new_ptr)?;
            let base_value = if base_offset == EMPTY_PTR {
                new_row.value
            } else {
                self.resolve_row_ptr(&write.base_ptr)?.value
            };
            writes.push(OccCommittedWrite {
                row_id: write.row_id,
                base_offset,
                new_offset,
                base_value,
                value: new_row.value,
                dirty_columns_bitmask: write.dirty_columns_bitmask,
            });
        }
        Ok(OccCommitRecord {
            txid: tx.txid,
            writes,
        })
    }

    fn publish_prepared_write_set(&self, record: &OccCommitRecord<T>) -> Result<(), Error> {
        for write in &record.writes {
            let slot = self.slot_ref(write.row_id)?;
            if write.base_offset != EMPTY_PTR {
                let base_row = self.resolve_row_ptr(&RelPtr::from_offset(write.base_offset))?;
                if base_row
                    .xmax
                    .compare_exchange(0, record.txid, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(Error::SerializationFailure);
                }
            }
            #[cfg(test)]
            ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(write.row_id, false);
                }
            });
            let new_row = self.resolve_row_ptr(&RelPtr::from_offset(write.new_offset))?;
            new_row.next.store(write.base_offset, Ordering::Release);
            if slot
                .head
                .compare_exchange(
                    write.base_offset,
                    write.new_offset,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return Err(Error::SerializationFailure);
            }
            #[cfg(test)]
            ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(write.row_id, true);
                }
            });
        }
        Ok(())
    }

    fn publish_write_set(
        &self,
        tx: &OccTransaction<T>,
        final_write_indices: &[usize],
    ) -> Result<Vec<OccCommittedWrite<T>>, Error> {
        let mut published = Vec::with_capacity(final_write_indices.len());

        for idx in final_write_indices {
            let write = &tx.write_set[*idx];
            let slot = self.slot_ref(write.row_id)?;

            let base_offset = write.base_ptr.load(Ordering::Acquire);
            let new_offset = write.new_ptr.load(Ordering::Acquire);
            let base_value = if base_offset != EMPTY_PTR {
                let base_row = self.resolve_row_ptr(&write.base_ptr)?;
                if base_row
                    .xmax
                    .compare_exchange(0, tx.txid, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(Error::SerializationFailure);
                }
                base_row.value
            } else {
                let new_row = self.resolve_row_ptr(&write.new_ptr)?;
                new_row.value
            };

            #[cfg(test)]
            ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(write.row_id, false);
                }
            });
            let new_row = self.resolve_row_ptr(&write.new_ptr)?;
            new_row.next.store(base_offset, Ordering::Release);

            if slot
                .head
                .compare_exchange(base_offset, new_offset, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Error::SerializationFailure);
            }

            #[cfg(test)]
            ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(write.row_id, true);
                }
            });
            published.push(OccCommittedWrite {
                row_id: write.row_id,
                base_offset,
                new_offset,
                base_value,
                value: new_row.value,
                dirty_columns_bitmask: write.dirty_columns_bitmask,
            });
        }

        Ok(published)
    }

    fn has_write_base_conflict(
        &self,
        tx: &OccTransaction<T>,
        final_write_indices: &[usize],
    ) -> Result<bool, Error> {
        for idx in final_write_indices {
            let write = &tx.write_set[*idx];
            let slot = self.slot_ref(write.row_id)?;

            let current_head = slot.head.load(Ordering::Acquire);
            let expected_head = write.base_ptr.load(Ordering::Acquire);
            if current_head != expected_head {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::WriteBaseHeadChanged, None, Some(write.row_id));
                return Ok(true);
            }

            if expected_head != EMPTY_PTR {
                let base_row = self.resolve_row_ptr(&write.base_ptr)?;
                if base_row.xmax.load(Ordering::Acquire) != 0 {
                    #[cfg(feature = "retry-diagnostics")]
                    crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::WriteBaseXmaxSet, None, Some(write.row_id));
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn has_row_lock_conflict(&self, tx: &OccTransaction<T>) -> Result<bool, Error> {
        let mut seen_rows = BTreeMap::<usize, ()>::new();
        for read in &tx.read_set {
            seen_rows.insert(read.row_id, ());
        }
        for write in &tx.write_set {
            seen_rows.insert(write.row_id, ());
        }

        for row_id in seen_rows.keys() {
            let Some(visible_ptr) = self.find_visible_row_ptr(tx, *row_id)? else {
                continue;
            };
            let visible = self.resolve_row_ptr(&visible_ptr)?;
            if self.row_locked_by_other_tx(visible, tx.txid) {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::CommitRowLocked, None, Some(*row_id));
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn has_serialization_conflict(&self, tx: &OccTransaction<T>) -> Result<bool, Error> {
        for read in &tx.read_set {
            let row = self.resolve_row_ptr(&read.row_ptr)?;
            if row.xmin != read.observed_xmin {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::ReadVersionIdentityChanged, None, Some(read.row_id));
                return Ok(true);
            }

            let xmax = row.xmax.load(Ordering::Acquire);
            if xmax == 0 || xmax == tx.txid {
                continue;
            }

            let committed_after_snapshot =
                xmax >= tx.snapshot_xmax || tx.snapshot_active.contains(&xmax);
            if committed_after_snapshot {
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::ReadVersionDeletedAfterSnapshot, None, Some(read.row_id));
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn final_write_indices(&self, tx: &OccTransaction<T>) -> Vec<usize> {
        let mut by_row = BTreeMap::<usize, usize>::new();
        for (idx, write) in tx.write_set.iter().enumerate() {
            by_row.insert(write.row_id, idx);
        }
        by_row.into_values().collect()
    }

    fn recycle_non_final_writes(
        &self,
        tx: &OccTransaction<T>,
        final_write_indices: &[usize],
    ) -> Result<(), Error> {
        let mut keep = vec![false; tx.write_set.len()];
        for idx in final_write_indices {
            if *idx < keep.len() {
                keep[*idx] = true;
            }
        }

        for (idx, write) in tx.write_set.iter().enumerate() {
            if keep[idx] {
                continue;
            }
            self.recycle_row_ptr(write.row_id, &write.new_ptr)?;
        }

        Ok(())
    }

    fn recycle_write_suffix(&self, tx: &OccTransaction<T>, start: usize) -> Result<(), Error> {
        if start >= tx.write_set.len() {
            return Ok(());
        }

        for write in tx.write_set[start..].iter() {
            self.recycle_row_ptr(write.row_id, &write.new_ptr)?;
        }

        Ok(())
    }

    fn find_visible_row_ptr(
        &self,
        tx: &OccTransaction<T>,
        row_id: usize,
    ) -> Result<Option<RelPtr<OccRow<T>>>, Error> {
        let slot = self.slot_ref(row_id)?;
        let mut head_offset = slot.head.load(Ordering::Acquire);
        let mut steps = 0_u32;

        while head_offset != EMPTY_PTR {
            steps = steps.wrapping_add(1);
            if steps > MAX_VISIBLE_CHAIN_STEPS {
                std::thread::yield_now();
                #[cfg(feature = "retry-diagnostics")]
                crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::VisibleChainLimit, None, Some(row_id));
                return Err(Error::SerializationFailure);
            }
            let row_ptr = RelPtr::from_offset(head_offset);
            let row = self.resolve_row_ptr(&row_ptr)?;
            if self.is_visible(row, tx) {
                return Ok(Some(row_ptr));
            }
            head_offset = row.next.load(Ordering::Acquire);
            #[cfg(test)]
            ROW_TRAVERSAL_STEP_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook(row_ptr.load(Ordering::Acquire), head_offset);
                }
            });
        }

        Ok(None)
    }

    #[inline]
    fn row_locked_by_other_tx(&self, row: &OccRow<T>, txid: TxId) -> bool {
        if !row.is_locked.load(Ordering::Acquire) {
            return false;
        }
        let owner = row.lock_owner_txid.load(Ordering::Acquire);
        owner != txid
    }

    fn release_row_lock(&self, row_ptr: &RelPtr<OccRow<T>>) -> Result<(), Error> {
        let row = self.resolve_row_ptr(row_ptr)?;
        row.lock_owner_txid.store(0, Ordering::Release);
        row.is_locked.store(false, Ordering::Release);
        Ok(())
    }

    fn is_visible(&self, row: &OccRow<T>, tx: &OccTransaction<T>) -> bool {
        if row.xmin == tx.txid {
            return true;
        }

        if row.xmin < tx.snapshot_xmin {
            // definitely committed before our snapshot horizon.
        } else if row.xmin >= tx.snapshot_xmax || tx.snapshot_active.contains(&row.xmin) {
            return false;
        }

        let xmax = row.xmax.load(Ordering::Acquire);
        if xmax == 0 {
            return true;
        }
        if xmax == tx.txid {
            return false;
        }

        // If deleter was still in-flight at snapshot start (or started later),
        // this version remains visible to the snapshot.
        xmax >= tx.snapshot_xmax || tx.snapshot_active.contains(&xmax)
    }

    fn record_read(
        &self,
        tx: &mut OccTransaction<T>,
        row_id: usize,
        row_ptr: RelPtr<OccRow<T>>,
        xmin: TxId,
    ) {
        let row_offset = row_ptr.load(Ordering::Acquire);
        if tx
            .read_set
            .iter()
            .any(|entry| entry.row_ptr.load(Ordering::Acquire) == row_offset)
        {
            return;
        }

        tx.read_set.push(ReadSetEntry {
            row_id,
            row_ptr,
            observed_xmin: xmin,
        });
    }

    fn acquire_partition_locks(
        &self,
        tx: &OccTransaction<T>,
    ) -> Result<PartitionLockGuard<'_>, Error> {
        self.try_acquire_lock_indices(self.collect_lock_indices(tx), Some(COMMIT_LOCK_SPIN_LIMIT))
            .ok_or(Error::SerializationFailure)
    }

    fn acquire_row_lock(&self, row_id: usize) -> PartitionLockGuard<'_> {
        self.try_acquire_lock_indices(vec![Self::lock_bucket_for_row_id(row_id)], None)
            .expect("row lock acquisition should not fail in blocking mode")
    }

    fn acquire_all_partition_locks(&self) -> PartitionLockGuard<'_> {
        self.try_acquire_lock_indices((0..OCC_PARTITION_LOCKS).collect(), None)
            .expect("global partition lock acquisition should not fail in blocking mode")
    }

    fn collect_lock_indices(&self, tx: &OccTransaction<T>) -> Vec<usize> {
        let mut needed = [false; OCC_PARTITION_LOCKS];

        for read in &tx.read_set {
            let idx = Self::lock_bucket_for_row_id(read.row_id);
            needed[idx] = true;
        }

        for write in &tx.write_set {
            let idx = Self::lock_bucket_for_row_id(write.row_id);
            needed[idx] = true;
        }

        let mut lock_indices = Vec::new();
        for (idx, used) in needed.iter().enumerate() {
            if *used {
                lock_indices.push(idx);
            }
        }
        lock_indices
    }

    #[inline]
    fn lock_bucket_for_row_id(row_id: usize) -> usize {
        let mut mixed = row_id as u64;
        mixed ^= mixed >> 33;
        mixed = mixed.wrapping_mul(0xff51_afd7_ed55_8ccd);
        mixed ^= mixed >> 33;
        (mixed as usize) % OCC_PARTITION_LOCKS
    }

    fn try_acquire_lock_indices(
        &self,
        mut lock_indices: Vec<usize>,
        spin_limit: Option<u32>,
    ) -> Option<PartitionLockGuard<'_>> {
        lock_indices.sort_unstable();
        lock_indices.dedup();

        let locks = self.shm.occ_partition_locks();
        let mut acquired: Vec<usize> = Vec::with_capacity(lock_indices.len());
        for idx in &lock_indices {
            let mut spins = 0_u32;
            while !locks[*idx].try_lock() {
                spins = spins.wrapping_add(1);
                if let Some(limit) = spin_limit {
                    if spins >= limit {
                        for held in acquired.iter().rev() {
                            locks[*held].unlock();
                        }
                        // Commit callers can still hold predicate latches.
                        // Return immediately after releasing row locks so their
                        // retry backoff runs only after every latch is dropped.
                        return None;
                    }
                }
                if spins & 0x3f == 0 {
                    std::thread::yield_now();
                }
                // Exclusive bootstrap/vacuum may wait without a finite budget.
                // Commit callers must not sleep while holding predicate latches;
                // their retry policy runs after all guards have been released.
                if spin_limit.is_none() && spins & 0x3ff == 0 {
                    std::thread::sleep(Duration::from_micros(100));
                }
                std::hint::spin_loop();
            }
            acquired.push(*idx);
        }

        Some(PartitionLockGuard {
            locks,
            lock_indices: acquired,
        })
    }

    fn shared_header_ref(&self) -> Result<&OccSharedHeader, Error> {
        let offset = self.shared_header.load(Ordering::Acquire);
        self.shared_header
            .as_ref(self.shm.mmap_base())
            .ok_or(Error::InvalidPointer { offset })
    }

    fn slot_ref(&self, row_id: usize) -> Result<&OccIndexSlot, Error> {
        let Some(slot_ptr) = self.index_slots.get(row_id) else {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        };

        let offset = slot_ptr.load(Ordering::Acquire);
        slot_ptr
            .as_ref(self.shm.mmap_base())
            .ok_or(Error::InvalidPointer { offset })
    }

    fn resolve_row_ptr<'a>(&'a self, row_ptr: &RelPtr<OccRow<T>>) -> Result<&'a OccRow<T>, Error> {
        let offset = row_ptr.load(Ordering::Acquire);
        row_ptr
            .as_ref(self.shm.mmap_base())
            .ok_or(Error::InvalidPointer { offset })
    }

    #[inline]
    fn recycle_shard_for_row_id(row_id: usize) -> usize {
        Self::lock_bucket_for_row_id(row_id)
    }

    #[inline]
    fn recycle_probe_shard(primary_shard: usize, probe_idx: usize) -> usize {
        primary_shard.wrapping_add(probe_idx.wrapping_mul(131)) % OCC_PARTITION_LOCKS
    }

    pub(crate) fn take_vacuum_request(&self) -> bool {
        self.shared_header_ref()
            .map(|header| header.vacuum_requested.swap(false, Ordering::AcqRel))
            .unwrap_or(false)
    }

    fn allocate_row_for_write(
        &self,
        row_id: usize,
        value: T,
        xmin: TxId,
        next: u32,
    ) -> Result<RelPtr<OccRow<T>>, Error> {
        match self.allocate_row(row_id, value, xmin, next) {
            Ok(ptr) => return Ok(ptr),
            Err(err @ Error::Allocation(_)) if self.shm.vacuum_daemon_pid() != 0 => {
                // A fast writer can consume the arena before the periodic vacuum
                // wakes. Request a pass and allow its recycled rows to arrive
                // before declaring OOM. This path holds no OCC partition lock,
                // and keeps the transaction registered to protect its snapshot.
                self.shared_header_ref()?
                    .vacuum_requested
                    .store(true, Ordering::Release);
                let deadline = std::time::Instant::now() + Duration::from_secs(1);
                loop {
                    std::thread::sleep(Duration::from_micros(250));
                    match self.allocate_row(row_id, value, xmin, next) {
                        Ok(ptr) => return Ok(ptr),
                        Err(Error::Allocation(_)) => {}
                        Err(other) => return Err(other),
                    }
                    if std::time::Instant::now() >= deadline || self.shm.vacuum_daemon_pid() == 0 {
                        return Err(err);
                    }
                }
            }
            Err(err) => Err(err),
        }
    }

    fn allocate_row(
        &self,
        row_id: usize,
        value: T,
        xmin: TxId,
        next: u32,
    ) -> Result<RelPtr<OccRow<T>>, Error> {
        let header = self.shared_header_ref()?;
        let primary_shard = Self::recycle_shard_for_row_id(row_id);

        if let Some(recycled_ptr) = self.try_take_starved_recycled_row(primary_shard) {
            header
                .recycle_alloc_from_starved
                .fetch_add(1, Ordering::AcqRel);
            self.initialize_row(&recycled_ptr, value, xmin, next)?;
            return Ok(recycled_ptr);
        }

        if let Some(recycled_ptr) = self.try_pop_recycled_row_from_shard(header, primary_shard)? {
            header
                .recycle_alloc_from_primary
                .fetch_add(1, Ordering::AcqRel);
            self.initialize_row(&recycled_ptr, value, xmin, next)?;
            return Ok(recycled_ptr);
        }

        let probe_limit = RECYCLE_SHARD_PROBE_LIMIT.min(OCC_PARTITION_LOCKS.saturating_sub(1));
        for probe_idx in 1..=probe_limit {
            let probe_shard = Self::recycle_probe_shard(primary_shard, probe_idx);
            if probe_shard == primary_shard {
                continue;
            }
            if let Some(recycled_ptr) = self.try_pop_recycled_row_from_shard(header, probe_shard)? {
                header
                    .recycle_alloc_from_probe
                    .fetch_add(1, Ordering::AcqRel);
                self.initialize_row(&recycled_ptr, value, xmin, next)?;
                return Ok(recycled_ptr);
            }
        }

        let ptr = match self
            .shm
            .chunked_arena()
            .alloc_in_class(OccRow::new(value, xmin, next), ArenaClass::RowVersion)
        {
            Ok(ptr) => ptr,
            Err(err) => {
                // The fast path probes only a few shards. Exhaustion must not
                // strand usable rows in any of the other shared recycle pools.
                for shard in 0..OCC_PARTITION_LOCKS {
                    if let Some(ptr) = self.try_pop_recycled_row_from_shard(header, shard)? {
                        header
                            .recycle_alloc_from_probe
                            .fetch_add(1, Ordering::AcqRel);
                        self.initialize_row(&ptr, value, xmin, next)?;
                        return Ok(ptr);
                    }
                }
                return Err(err.into());
            }
        };
        header.recycle_alloc_fresh.fetch_add(1, Ordering::AcqRel);
        Ok(ptr)
    }

    fn initialize_row(
        &self,
        row_ptr: &RelPtr<OccRow<T>>,
        value: T,
        xmin: TxId,
        next: u32,
    ) -> Result<(), Error> {
        let offset = row_ptr.load(Ordering::Acquire);
        let row_mut = self.resolve_row_ptr_raw(offset)?;
        // SAFETY:
        // Recycled rows are only reused after being removed from the free list and are not
        // reachable from table heads or any live transaction write-set at this point.
        unsafe {
            std::ptr::write(row_mut, OccRow::new(value, xmin, next));
        }
        Ok(())
    }

    fn resolve_row_ptr_raw(&self, offset: u32) -> Result<*mut OccRow<T>, Error> {
        if offset == EMPTY_PTR {
            return Err(Error::InvalidPointer { offset });
        }

        let mmap = self.shm.mmap_base();
        let size = std::mem::size_of::<OccRow<T>>();
        let align = std::mem::align_of::<OccRow<T>>();

        let start = offset as usize;
        let end = start
            .checked_add(size)
            .ok_or(Error::InvalidPointer { offset })?;
        if end > mmap.len() {
            return Err(Error::InvalidPointer { offset });
        }

        let addr = (mmap.as_ptr() as usize)
            .checked_add(start)
            .ok_or(Error::InvalidPointer { offset })?;
        if addr % align != 0 {
            return Err(Error::InvalidPointer { offset });
        }

        Ok(addr as *mut OccRow<T>)
    }

    fn try_pop_recycled_row_from_shard(
        &self,
        header: &OccSharedHeader,
        recycle_shard: usize,
    ) -> Result<Option<RelPtr<OccRow<T>>>, Error> {
        // Protect both the head and the dereference of its next pointer. A tagged
        // CAS alone would still permit another allocator to reinitialize a node
        // while a losing pop reads its previous next field.
        let _guard = header.recycle_locks[recycle_shard].lock();
        let head_slot = &header.recycled_heads[recycle_shard];
        let head = head_slot.load(Ordering::Acquire);
        if head == EMPTY_PTR {
            header.recycle_pop_empty.fetch_add(1, Ordering::AcqRel);
            return Ok(None);
        }
        let head_ptr = RelPtr::<OccRow<T>>::from_offset(head);
        let row = self.resolve_row_ptr(&head_ptr)?;
        head_slot.store(row.recycle_next.load(Ordering::Acquire), Ordering::Release);
        Ok(Some(head_ptr))
    }

    fn recycle_row_ptr(&self, row_id: usize, row_ptr: &RelPtr<OccRow<T>>) -> Result<(), Error> {
        let offset = row_ptr.load(Ordering::Acquire);
        if offset == EMPTY_PTR {
            return Ok(());
        }
        let header = self.shared_header_ref()?;
        let shard = Self::recycle_shard_for_row_id(row_id);
        let _guard = header.recycle_locks[shard].lock();
        let head_slot = &header.recycled_heads[shard];
        let row = self.resolve_row_ptr(row_ptr)?;
        row.recycle_next
            .store(head_slot.load(Ordering::Acquire), Ordering::Release);
        head_slot.store(offset, Ordering::Release);
        header.recycle_push_success.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn try_take_starved_recycled_row(&self, recycle_shard: usize) -> Option<RelPtr<OccRow<T>>> {
        let key = self.starved_recycle_key(recycle_shard);
        STARVED_RECYCLE_SLOT.with(|slot| {
            let Some(entry) = slot.get() else {
                return None;
            };
            if entry.key != key {
                return None;
            }

            slot.set(None);
            Some(RelPtr::from_offset(entry.offset))
        })
    }

    #[cfg(test)]
    fn stash_starved_recycled_row(&self, recycle_shard: usize, offset: u32) -> bool {
        if offset == EMPTY_PTR {
            return true;
        }

        let key = self.starved_recycle_key(recycle_shard);
        STARVED_RECYCLE_SLOT.with(|slot| {
            if slot.get().is_some() {
                return false;
            }
            slot.set(Some(StarvedRecycleEntry { key, offset }));
            true
        })
    }

    fn starved_recycle_key(&self, recycle_shard: usize) -> StarvedRecycleKey {
        let mmap = self.shm.mmap_base();
        StarvedRecycleKey {
            mmap_base: mmap.as_ptr() as usize,
            mmap_len: mmap.len(),
            shared_header_offset: self.shared_header.load(Ordering::Acquire),
            recycle_shard,
            row_size: std::mem::size_of::<OccRow<T>>(),
            row_align: std::mem::align_of::<OccRow<T>>(),
        }
    }

    fn abort_for_serialization_failure(&self, tx: &mut OccTransaction<T>) {
        let _ = self.clear_local_sets(tx);
        let _ = self.finish_transaction(tx);
        std::thread::yield_now();
    }

    fn finish_transaction(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        let Some(registration) = tx.registration.take() else {
            return Ok(());
        };
        #[cfg(test)]
        TRANSACTION_FINISHING_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        self.shm.end_transaction(registration)?;
        #[cfg(test)]
        TRANSACTION_FINISHED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        Ok(())
    }

    #[inline]
    fn clear_local_sets(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        self.recycle_write_suffix(tx, 0)?;
        tx.read_set.clear();
        tx.index_reads.clear();
        tx.index_conflict = false;
        tx.write_set.clear();
        tx.savepoints.clear();
        Ok(())
    }

    #[inline]
    fn ensure_open(&self, tx: &OccTransaction<T>) -> Result<(), Error> {
        if tx.table_offset != self.shared_header_offset()
            || tx.arena_base != self.shm.mmap_base().as_ptr() as usize
        {
            return Err(Error::TransactionTableMismatch);
        }
        self.validate_index_bindings()?;
        if tx.registration.is_none() {
            return Err(Error::TransactionClosed);
        }
        Ok(())
    }

    pub fn latest_value(&self, row_id: usize) -> Result<Option<T>, Error> {
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }

        let slot = self.slot_ref(row_id)?;
        let head_offset = slot.head.load(Ordering::Acquire);
        if head_offset == EMPTY_PTR {
            return Ok(None);
        }

        let row_ptr = RelPtr::from_offset(head_offset);
        let row = self.resolve_row_ptr(&row_ptr)?;
        Ok(Some(row.value))
    }

    pub fn row_head_offset(&self, row_id: usize) -> Result<u32, Error> {
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }
        let slot = self.slot_ref(row_id)?;
        Ok(slot.head.load(Ordering::Acquire))
    }

    /// Replay only during exclusive bootstrap/recovery, before any index is
    /// bound and with no live transactions or concurrent readers. Rebuild the
    /// indexes from the recovered rows before binding them for runtime use.
    pub fn apply_recovered_write(&self, row_id: usize, txid: TxId, value: T) -> Result<(), Error> {
        self.ensure_unmanaged_write()?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }
        if txid == 0 {
            return Err(Error::SerializationFailure);
        }

        let _lock = self.acquire_row_lock(row_id);
        let slot = self.slot_ref(row_id)?;
        let base_offset = slot.head.load(Ordering::Acquire);
        let new_ptr = self.allocate_row(row_id, value, txid, base_offset)?;
        let new_offset = new_ptr.load(Ordering::Acquire);

        if base_offset != EMPTY_PTR {
            let base_ptr = RelPtr::<OccRow<T>>::from_offset(base_offset);
            let base_row = self.resolve_row_ptr(&base_ptr)?;
            let _ = base_row
                .xmax
                .compare_exchange(0, txid, Ordering::AcqRel, Ordering::Acquire);
        }

        let new_row = self.resolve_row_ptr(&new_ptr)?;
        new_row.next.store(base_offset, Ordering::Release);
        slot.head.store(new_offset, Ordering::Release);
        self.advance_global_txid_floor(txid.saturating_add(1));
        Ok(())
    }

    /// CAS replay variant with the same exclusive recovery requirements as
    /// `apply_recovered_write`; this is not a runtime transactional write API.
    pub fn apply_recovered_write_cas(
        &self,
        row_id: usize,
        txid: TxId,
        expected_base_offset: u32,
        value: T,
    ) -> Result<(), Error> {
        self.ensure_unmanaged_write()?;
        if row_id >= self.capacity() {
            return Err(Error::RowOutOfBounds {
                row_id,
                capacity: self.capacity(),
            });
        }
        if txid == 0 {
            return Err(Error::SerializationFailure);
        }

        let _lock = self.acquire_row_lock(row_id);
        let slot = self.slot_ref(row_id)?;
        let base_offset = slot.head.load(Ordering::Acquire);
        if base_offset != expected_base_offset {
            return Err(Error::SerializationFailure);
        }

        let new_ptr = self.allocate_row(row_id, value, txid, base_offset)?;
        let new_offset = new_ptr.load(Ordering::Acquire);

        if base_offset != EMPTY_PTR {
            let base_ptr = RelPtr::<OccRow<T>>::from_offset(base_offset);
            let base_row = self.resolve_row_ptr(&base_ptr)?;
            let _ = base_row
                .xmax
                .compare_exchange(0, txid, Ordering::AcqRel, Ordering::Acquire);
        }

        let new_row = self.resolve_row_ptr(&new_ptr)?;
        new_row.next.store(base_offset, Ordering::Release);
        if slot
            .head
            .compare_exchange(base_offset, new_offset, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::SerializationFailure);
        }
        self.advance_global_txid_floor(txid.saturating_add(1));
        Ok(())
    }

    pub fn advance_global_txid_floor(&self, floor: TxId) {
        let global = self.shm.global_txid();
        let mut observed = global.load(Ordering::Acquire);
        while observed < floor {
            match global.compare_exchange_weak(observed, floor, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }

    #[inline]
    pub fn current_global_txid(&self) -> TxId {
        self.shm.global_txid().load(Ordering::Acquire)
    }

    #[inline]
    pub fn shared_arena(&self) -> &Arc<ShmArena> {
        &self.shm
    }

    pub fn recycle_telemetry(&self) -> Result<OccRecycleTelemetry, Error> {
        let header = self.shared_header_ref()?;
        Ok(OccRecycleTelemetry {
            alloc_from_starved: header.recycle_alloc_from_starved.load(Ordering::Acquire),
            alloc_from_primary: header.recycle_alloc_from_primary.load(Ordering::Acquire),
            alloc_from_probe: header.recycle_alloc_from_probe.load(Ordering::Acquire),
            alloc_fresh: header.recycle_alloc_fresh.load(Ordering::Acquire),
            pop_empty: header.recycle_pop_empty.load(Ordering::Acquire),
            pop_cas_fail: header.recycle_pop_cas_fail.load(Ordering::Acquire),
            push_success: header.recycle_push_success.load(Ordering::Acquire),
            push_cas_fail: header.recycle_push_cas_fail.load(Ordering::Acquire),
            stash_starved: header.recycle_stash_starved.load(Ordering::Acquire),
        })
    }

    pub fn snapshot_latest_rows(&self) -> Result<Vec<(usize, T)>, Error> {
        let _lock = self.acquire_all_partition_locks();
        self.snapshot_latest_rows_locked()
    }

    /// Keep commit publication excluded until the durable checkpoint and WAL
    /// cut finish. The active set distinguishes old-starting transactions which
    /// can publish only after this checkpoint from already captured commits.
    pub(crate) fn with_checkpoint_snapshot<R, E, F>(&self, checkpoint: F) -> Result<R, E>
    where
        E: From<Error>,
        F: FnOnce(Vec<(usize, T)>, ProcSnapshot) -> Result<R, E>,
    {
        let _lock = self.acquire_all_partition_locks();
        self.validate_index_bindings()?;
        let rows = self.snapshot_latest_rows_locked()?;
        let snapshot = self.shm.create_snapshot();
        checkpoint(rows, snapshot)
    }

    fn snapshot_latest_rows_locked(&self) -> Result<Vec<(usize, T)>, Error> {
        let mut rows = Vec::with_capacity(self.capacity());

        for row_id in 0..self.capacity() {
            let slot = self.slot_ref(row_id)?;
            let head_offset = slot.head.load(Ordering::Acquire);
            if head_offset == EMPTY_PTR {
                continue;
            }

            let row_ptr = RelPtr::<OccRow<T>>::from_offset(head_offset);
            let row = self.resolve_row_ptr(&row_ptr)?;
            rows.push((row_id, row.value));
        }

        Ok(rows)
    }

    /// Reclaim obsolete row versions without passing any retained snapshot.
    /// `requested_xmin` may conservatively delay recycling; a value above the
    /// arena's actual retained horizon is clamped before scanning rows.
    pub fn vacuum_reclaim_once(
        &self,
        requested_xmin: TxId,
    ) -> Result<Vec<VacuumReclaimedRow<T>>, Error> {
        let retained_xmin = crate::vacuum::compute_global_xmin(self.shm.as_ref());
        self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))
    }

    /// Internal collector kernel. The caller must derive `global_xmin` from
    /// this table's arena retention metadata, rather than an arbitrary ID.
    pub(crate) fn vacuum_reclaim_before(
        &self,
        global_xmin: TxId,
    ) -> Result<Vec<VacuumReclaimedRow<T>>, Error> {
        let mut reclaimed = Vec::new();

        for row_id in 0..self.capacity() {
            let _lock = self.acquire_row_lock(row_id);
            let slot = self.slot_ref(row_id)?;
            let head_offset = slot.head.load(Ordering::Acquire);
            if head_offset == EMPTY_PTR {
                continue;
            }

            let head_ptr = RelPtr::<OccRow<T>>::from_offset(head_offset);
            let head_row = self.resolve_row_ptr(&head_ptr)?;
            let live_head_value = head_row.value;

            let mut prev_offset = head_offset;
            let mut curr_offset = head_row.next.load(Ordering::Acquire);
            while curr_offset != EMPTY_PTR {
                let curr_ptr = RelPtr::<OccRow<T>>::from_offset(curr_offset);
                let curr_row = self.resolve_row_ptr(&curr_ptr)?;
                let next_offset = curr_row.next.load(Ordering::Acquire);
                let xmax = curr_row.xmax.load(Ordering::Acquire);

                // A RowLockGuard can outlive its transaction. Its Drop writes
                // to this version, so recycling it before the guard releases
                // could let an old guard unlock an unrelated later owner.
                if xmax != 0 && xmax < global_xmin && !curr_row.is_locked.load(Ordering::Acquire) {
                    let prev_ptr = RelPtr::<OccRow<T>>::from_offset(prev_offset);
                    let prev_row = self.resolve_row_ptr(&prev_ptr)?;
                    prev_row.next.store(next_offset, Ordering::Release);
                    let reclaimed_value = curr_row.value;
                    self.recycle_row_ptr(row_id, &curr_ptr)?;
                    reclaimed.push(VacuumReclaimedRow {
                        row_id,
                        reclaimed_value,
                        live_head_value: Some(live_head_value),
                    });
                    curr_offset = next_offset;
                    continue;
                }

                prev_offset = curr_offset;
                curr_offset = next_offset;
            }
        }

        Ok(reclaimed)
    }
}

struct PartitionLockGuard<'a> {
    locks: &'a [crate::shm::OccPartitionLock; OCC_PARTITION_LOCKS],
    lock_indices: Vec<usize>,
}

impl Drop for PartitionLockGuard<'_> {
    fn drop(&mut self) {
        for idx in self.lock_indices.iter().rev() {
            self.locks[*idx].unlock();
        }
    }
}

impl<T: Copy + Send + Sync + 'static> Drop for RowLockGuard<'_, T> {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        let _ = self.table.release_row_lock(&self.row_ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_table() -> OccTable<u64> {
        let shm = Arc::new(ShmArena::new(8 << 20).expect("create shm"));
        let table = OccTable::<u64>::new(shm, 4).expect("create table");
        for row_id in 0..4 {
            table.seed_row(row_id, row_id as u64).expect("seed row");
        }
        table
    }

    fn alloc_detached_row(table: &OccTable<u64>, value: u64) -> RelPtr<OccRow<u64>> {
        table
            .shm
            .chunked_arena()
            .alloc_in_class(OccRow::new(value, 1, EMPTY_PTR), ArenaClass::RowVersion)
            .expect("alloc detached row")
    }

    fn clear_starved_slot() {
        STARVED_RECYCLE_SLOT.with(|slot| slot.set(None));
    }

    #[test]
    fn starved_slot_roundtrip_isolated_by_recycle_key() {
        clear_starved_slot();
        let table = make_table();
        let row_id = 0;
        let shard = OccTable::<u64>::recycle_shard_for_row_id(row_id);
        let wrong_shard = (shard + 1) % OCC_PARTITION_LOCKS;
        let ptr = alloc_detached_row(&table, 777);
        let offset = ptr.load(Ordering::Acquire);

        assert!(table.stash_starved_recycled_row(shard, offset));
        assert!(
            table.try_take_starved_recycled_row(wrong_shard).is_none(),
            "wrong shard must not consume starved slot"
        );
        let taken = table
            .try_take_starved_recycled_row(shard)
            .expect("expected starved slot for matching shard");
        assert_eq!(taken.load(Ordering::Acquire), offset);
        assert!(
            table.try_take_starved_recycled_row(shard).is_none(),
            "starved slot should be empty after consume"
        );
    }

    #[test]
    fn occupied_starved_slot_does_not_abandon_previous_allocation() {
        clear_starved_slot();
        let table = make_table();
        let shard = OccTable::<u64>::recycle_shard_for_row_id(0);
        let first = alloc_detached_row(&table, 1).load(Ordering::Acquire);
        let second = alloc_detached_row(&table, 2).load(Ordering::Acquire);
        assert!(table.stash_starved_recycled_row(shard, first));
        assert!(!table.stash_starved_recycled_row(shard, second));
        assert_eq!(
            table
                .try_take_starved_recycled_row(shard)
                .unwrap()
                .load(Ordering::Acquire),
            first
        );
        // The rejected caller still owns `second` and can return it normally.
        table
            .recycle_row_ptr(0, &RelPtr::from_offset(second))
            .unwrap();
    }

    #[test]
    fn failed_fresh_row_allocation_does_not_increment_success_counter() {
        clear_starved_slot();
        let table = make_table();
        let arena = table.shm.chunked_arena();
        arena.alloc_raw(arena.remaining_bytes(), 1).unwrap();
        let before = table.recycle_telemetry().unwrap().alloc_fresh;
        assert!(matches!(
            table.allocate_row(0, 1, 99, EMPTY_PTR),
            Err(Error::Allocation(_))
        ));
        assert_eq!(table.recycle_telemetry().unwrap().alloc_fresh, before);
    }

    #[test]
    fn exhaustion_uses_recycled_rows_outside_fast_probe_set() {
        clear_starved_slot();
        let table = make_table();
        let primary = OccTable::<u64>::recycle_shard_for_row_id(0);
        let shard = (0..OCC_PARTITION_LOCKS)
            .find(|shard| {
                *shard != primary
                    && (1..=RECYCLE_SHARD_PROBE_LIMIT)
                        .all(|probe| OccTable::<u64>::recycle_probe_shard(primary, probe) != *shard)
            })
            .unwrap();
        let recycled = alloc_detached_row(&table, 99);
        let offset = recycled.load(Ordering::Acquire);
        table.shared_header_ref().unwrap().recycled_heads[shard].store(offset, Ordering::Release);
        let arena = table.shm.chunked_arena();
        arena.alloc_raw(arena.remaining_bytes(), 1).unwrap();
        let allocated = table.allocate_row(0, 101, 99, EMPTY_PTR).unwrap();
        assert_eq!(allocated.load(Ordering::Acquire), offset);
        assert_eq!(table.resolve_row_ptr(&allocated).unwrap().value, 101);
    }

    #[test]
    fn concurrent_recycler_never_loses_or_double_owns_a_row() {
        use std::collections::BTreeSet;
        use std::sync::Mutex;

        let table = Arc::new(make_table());
        for value in 0..32 {
            let ptr = alloc_detached_row(&table, value);
            table.recycle_row_ptr(0, &ptr).unwrap();
        }
        let owned = Arc::new(Mutex::new(BTreeSet::new()));
        let mut workers = Vec::new();
        for worker in 0..8_u64 {
            let table = Arc::clone(&table);
            let owned = Arc::clone(&owned);
            workers.push(std::thread::spawn(move || {
                let shard = OccTable::<u64>::recycle_shard_for_row_id(0);
                for sequence in 0..10_000 {
                    let ptr = table
                        .try_pop_recycled_row_from_shard(table.shared_header_ref().unwrap(), shard)
                        .unwrap()
                        .expect("pool has more rows than workers");
                    let offset = ptr.load(Ordering::Acquire);
                    assert!(
                        owned.lock().unwrap().insert(offset),
                        "two workers own one recycled row"
                    );
                    let value = (worker << 32) | sequence;
                    table.initialize_row(&ptr, value, 1, EMPTY_PTR).unwrap();
                    std::thread::yield_now();
                    assert_eq!(table.resolve_row_ptr(&ptr).unwrap().value, value);
                    assert!(owned.lock().unwrap().remove(&offset));
                    table.recycle_row_ptr(0, &ptr).unwrap();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let shard = OccTable::<u64>::recycle_shard_for_row_id(0);
        let mut free = BTreeSet::new();
        while let Some(ptr) = table
            .try_pop_recycled_row_from_shard(table.shared_header_ref().unwrap(), shard)
            .unwrap()
        {
            assert!(
                free.insert(ptr.load(Ordering::Acquire)),
                "duplicate/cyclic free row"
            );
        }
        assert_eq!(free.len(), 32, "recycler lost an allocation");
    }

    #[test]
    fn allocate_row_prefers_starved_before_other_sources() {
        clear_starved_slot();
        let table = make_table();
        let row_id = 1;
        let shard = OccTable::<u64>::recycle_shard_for_row_id(row_id);
        let ptr = alloc_detached_row(&table, 900);
        let offset = ptr.load(Ordering::Acquire);
        let before = table.recycle_telemetry().expect("recycle telemetry before");

        assert!(table.stash_starved_recycled_row(shard, offset));
        let allocated = table
            .allocate_row(row_id, 1234, 42, EMPTY_PTR)
            .expect("allocate row from starved slot");
        assert_eq!(allocated.load(Ordering::Acquire), offset);

        let row = table
            .resolve_row_ptr(&allocated)
            .expect("resolve allocated recycled row");
        assert_eq!(row.value, 1234);
        assert_eq!(row.xmin, 42);

        let after = table.recycle_telemetry().expect("recycle telemetry after");
        assert_eq!(after.alloc_from_starved, before.alloc_from_starved + 1);
        assert_eq!(after.alloc_from_primary, before.alloc_from_primary);
        assert_eq!(after.alloc_from_probe, before.alloc_from_probe);
    }

    #[test]
    fn allocate_row_uses_primary_recycle_head_before_probe() {
        clear_starved_slot();
        let table = make_table();
        let row_id = 2;
        let primary = OccTable::<u64>::recycle_shard_for_row_id(row_id);
        let probe = OccTable::<u64>::recycle_probe_shard(primary, 1);
        assert_ne!(probe, primary);

        let primary_ptr = alloc_detached_row(&table, 111);
        let probe_ptr = alloc_detached_row(&table, 222);
        let primary_offset = primary_ptr.load(Ordering::Acquire);
        let probe_offset = probe_ptr.load(Ordering::Acquire);

        let header = table.shared_header_ref().expect("shared header");
        let primary_row = table
            .resolve_row_ptr(&primary_ptr)
            .expect("resolve primary recycled row");
        primary_row.recycle_next.store(EMPTY_PTR, Ordering::Release);
        let probe_row = table
            .resolve_row_ptr(&probe_ptr)
            .expect("resolve probe recycled row");
        probe_row.recycle_next.store(EMPTY_PTR, Ordering::Release);

        header.recycled_heads[probe].store(probe_offset, Ordering::Release);
        header.recycled_heads[primary].store(primary_offset, Ordering::Release);
        let before = table.recycle_telemetry().expect("recycle telemetry before");

        let allocated = table
            .allocate_row(row_id, 700, 7, EMPTY_PTR)
            .expect("allocate row");
        assert_eq!(
            allocated.load(Ordering::Acquire),
            primary_offset,
            "primary shard recycled row should be consumed before probe shard rows"
        );
        assert_eq!(
            header.recycled_heads[probe].load(Ordering::Acquire),
            probe_offset,
            "probe shard head should remain untouched when primary has an entry"
        );

        let after = table.recycle_telemetry().expect("recycle telemetry after");
        assert_eq!(after.alloc_from_primary, before.alloc_from_primary + 1);
        assert_eq!(after.alloc_from_probe, before.alloc_from_probe);
    }

    #[test]
    fn allocate_row_uses_probe_when_primary_empty() {
        clear_starved_slot();
        let table = make_table();
        let row_id = 3;
        let primary = OccTable::<u64>::recycle_shard_for_row_id(row_id);
        let probe = OccTable::<u64>::recycle_probe_shard(primary, 1);
        assert_ne!(probe, primary);

        let probe_ptr = alloc_detached_row(&table, 333);
        let probe_offset = probe_ptr.load(Ordering::Acquire);
        let probe_row = table
            .resolve_row_ptr(&probe_ptr)
            .expect("resolve probe recycled row");
        probe_row.recycle_next.store(EMPTY_PTR, Ordering::Release);

        let header = table.shared_header_ref().expect("shared header");
        header.recycled_heads[primary].store(EMPTY_PTR, Ordering::Release);
        header.recycled_heads[probe].store(probe_offset, Ordering::Release);
        let before = table.recycle_telemetry().expect("recycle telemetry before");

        let allocated = table
            .allocate_row(row_id, 444, 9, EMPTY_PTR)
            .expect("allocate row");
        assert_eq!(
            allocated.load(Ordering::Acquire),
            probe_offset,
            "probe shard recycled row should be consumed when primary is empty"
        );

        let after = table.recycle_telemetry().expect("recycle telemetry after");
        assert_eq!(after.alloc_from_probe, before.alloc_from_probe + 1);
        assert_eq!(after.alloc_from_primary, before.alloc_from_primary);
    }

    #[test]
    fn recycle_telemetry_invariant_holds_in_single_recycle_cycle() {
        clear_starved_slot();
        let table = make_table();
        let row_id = 0;
        let recycled_ptr = alloc_detached_row(&table, 999);

        table
            .recycle_row_ptr(row_id, &recycled_ptr)
            .expect("recycle row ptr");
        let _ = table
            .allocate_row(row_id, 1000, 12, EMPTY_PTR)
            .expect("allocate from recycled row");

        let telemetry = table.recycle_telemetry().expect("recycle telemetry");
        let pops = telemetry
            .alloc_from_primary
            .saturating_add(telemetry.alloc_from_probe)
            .saturating_add(telemetry.alloc_from_starved);
        assert!(telemetry.push_success > 0, "expected recycle push");
        assert!(pops > 0, "expected recycled allocation pop");
        assert!(
            pops <= telemetry.push_success,
            "recycler invariant violated: pops={} pushes={}",
            pops,
            telemetry.push_success
        );
    }
}

fn index_predicate_matches(predicate: &IndexCompare, key: &IndexValue) -> bool {
    match predicate {
        IndexCompare::Eq(value) => key == value,
        IndexCompare::Gt(value) => key > value,
        IndexCompare::Gte(value) => key >= value,
        IndexCompare::Lt(value) => key < value,
        IndexCompare::Lte(value) => key <= value,
        IndexCompare::In(values) => values.contains(key),
    }
}

#[cfg(test)]
mod transactional_publication_tests {
    use super::*;
    use std::sync::Barrier;

    fn key(value: &u64) -> Option<IndexValue> {
        Some(IndexValue::U64(*value))
    }

    #[test]
    fn bounded_partition_contention_releases_every_partial_lock_before_retry() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let table = OccTable::new(Arc::clone(&arena), 2).unwrap();
        table.seed_row(0, 10).unwrap();
        table.seed_row(1, 20).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 11).unwrap();
        table.write(&mut tx, 1, 21).unwrap();
        let partitions = table.collect_lock_indices(&tx);
        assert_eq!(partitions.len(), 2);
        let locks = arena.occ_partition_locks();
        assert!(locks[partitions[1]].try_lock());
        let committed = table.commit(&mut tx);
        locks[partitions[1]].unlock();
        assert_eq!(committed, Err(Error::SerializationFailure));
        // The first partition was acquired before the second hit its finite
        // contention budget. It must be available before application backoff.
        assert!(locks[partitions[0]].try_lock());
        locks[partitions[0]].unlock();
        assert_eq!(table.latest_value(0).unwrap(), Some(10));
        assert_eq!(table.latest_value(1).unwrap(), Some(20));
    }

    #[test]
    fn brief_predicate_latch_contention_does_not_force_a_transaction_retry() {
        use std::sync::mpsc;
        for write_transaction in [false, true] {
            let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
            let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
            table.seed_row(0, 10).unwrap();
            let index = SecondaryIndex::new_in_shared("key", arena);
            index.try_insert(IndexValue::U64(10), 0).unwrap();
            table.bind_index(index.clone(), key).unwrap();
            let bucket = index
                .transactional_key_bucket(&IndexValue::U64(10))
                .unwrap();
            let mut tx = table.begin_transaction().unwrap();
            let holder_index = index.clone();
            let (ready_send, ready_recv) = mpsc::channel();
            let (release_send, release_recv) = mpsc::channel();
            let (released_send, released_recv) = mpsc::channel();
            let holder = std::thread::spawn(move || {
                let guard = holder_index
                    .transactional_try_lock_bucket(bucket)
                    .unwrap()
                    .unwrap();
                ready_send.send(()).unwrap();
                release_recv.recv().unwrap();
                drop(guard);
                released_send.send(()).unwrap();
            });
            ready_recv.recv().unwrap();
            INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    // Force exactly one failed lock attempt, then release the
                    // existing holder before acquisition retries. No timing
                    // assumption or sleep determines the test result.
                    release_send.send(()).unwrap();
                    released_recv.recv().unwrap();
                }));
            });
            if write_transaction {
                table.write(&mut tx, 0, 20).unwrap();
                assert_eq!(table.commit(&mut tx).unwrap(), 1);
            } else {
                assert_eq!(
                    table
                        .index_lookup(&mut tx, &index, &IndexCompare::Eq(IndexValue::U64(10)))
                        .unwrap(),
                    vec![0]
                );
                table.commit(&mut tx).unwrap();
            }
            holder.join().unwrap();
        }
    }

    #[test]
    fn captured_candidates_survive_concurrent_key_move_and_vacuum_after_latch_release() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
        table.seed_row(0, 10).unwrap();
        let index = SecondaryIndex::new_in_shared("key", arena);
        index.try_insert(IndexValue::U64(10), 0).unwrap();
        table.bind_index(index.clone(), key).unwrap();
        let table = Arc::new(table);
        // The writer is older but still active in the reader's snapshot. That
        // snapshot must retain the old version even after the writer finishes.
        let mut writer = table.begin_transaction().unwrap();
        let mut reader = table.begin_transaction().unwrap();
        let writer_table = Arc::clone(&table);
        INDEX_CANDIDATES_CAPTURED_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                writer_table.write(&mut writer, 0, 20).unwrap();
                // This can succeed only after the reader releases its bucket
                // latches. Schedule a real vacuum before row materialization.
                writer_table.commit(&mut writer).unwrap();
                assert!(crate::run_vacuum_pass(&writer_table).unwrap().is_empty());
            }));
        });
        assert_eq!(
            table
                .index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::U64(10)))
                .unwrap(),
            vec![0]
        );
        assert_eq!(table.read(&mut reader, 0).unwrap(), Some(10));
        assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(20), 0)]);
        // The materialized snapshot was complete, but serializable commit must
        // still reject its changed predicate dependency.
        assert!(table.index_read_conflict(&reader).unwrap());
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert_eq!(crate::run_vacuum_pass(&table).unwrap().len(), 1);
    }

    #[test]
    fn preencoded_record_is_revalidated_before_its_acceptance() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
        table.seed_row(0, 10).unwrap();
        let index = SecondaryIndex::new_in_shared("key", Arc::clone(&arena));
        index.try_insert(IndexValue::U64(10), 0).unwrap();
        table.bind_index(index.clone(), key).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 19).unwrap();
        table.write(&mut tx, 0, 20).unwrap();
        let accepted = std::cell::Cell::new(false);
        let result = table.commit_with_record_prepared::<Error, _, _>(&mut tx, |record| {
            assert_eq!(record.writes.len(), 1);
            assert_eq!(record.writes[0].base_value, 10);
            assert_eq!(record.writes[0].value, 20);
            // This indexed conflicting commit can succeed only if neither
            // class of publication guards is held during payload preparation.
            let mut competing = table.begin_transaction().unwrap();
            table.write(&mut competing, 0, 30).unwrap();
            table.commit(&mut competing).unwrap();
            let accepted = &accepted;
            Ok(move |_: &OccCommitRecord<u64>| {
                accepted.set(true);
                Ok(())
            })
        });
        assert!(matches!(result, Err(Error::SerializationFailure)));
        assert!(!accepted.get());
        assert!(tx.registration.is_none());
        assert!(arena.create_snapshot().in_flight_txids().is_empty());
        assert_eq!(table.snapshot_latest_rows().unwrap(), vec![(0, 30)]);
        assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(30), 0)]);
    }

    #[test]
    fn accepted_preencoded_record_is_the_record_published() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let table = OccTable::new(arena, 2).unwrap();
        table.seed_row(0, 0).unwrap();
        table.seed_row(1, 1).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 19).unwrap();
        table.write(&mut tx, 0, 20).unwrap();
        table.write(&mut tx, 1, 21).unwrap();
        let accepted = std::cell::Cell::new(false);
        let returned = table
            .commit_with_record_prepared::<Error, _, _>(&mut tx, |record| {
                let payload = record
                    .writes
                    .iter()
                    .map(|w| (w.row_id, w.base_offset, w.new_offset, w.base_value, w.value))
                    .collect::<Vec<_>>();
                let txid = record.txid;
                let accepted = &accepted;
                let table = &table;
                Ok(move |record: &OccCommitRecord<u64>| {
                    assert_eq!(record.txid, txid);
                    assert_eq!(record.writes.len(), payload.len());
                    for (write, copied) in record.writes.iter().zip(&payload) {
                        assert_eq!(
                            (
                                write.row_id,
                                write.base_offset,
                                write.new_offset,
                                write.base_value,
                                write.value
                            ),
                            *copied
                        );
                        assert_eq!(
                            table.latest_value(write.row_id).unwrap(),
                            Some(write.base_value)
                        );
                    }
                    accepted.set(true);
                    Ok(())
                })
            })
            .unwrap();
        assert!(accepted.get());
        assert_eq!(returned.writes.len(), 2);
        for write in returned.writes {
            assert_eq!(table.latest_value(write.row_id).unwrap(), Some(write.value));
        }
        assert!(tx.registration.is_none());
    }

    #[test]
    fn rejected_or_panicking_durability_step_rolls_back_all_destinations_and_closes_transaction() {
        for panics in [false, true] {
            let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
            let mut table = OccTable::new(Arc::clone(&arena), 2).unwrap();
            let first = SecondaryIndex::new_in_shared("first", Arc::clone(&arena));
            let second = SecondaryIndex::new_in_shared("second", Arc::clone(&arena));
            for row_id in 0..2 {
                table.seed_row(row_id, 10 + row_id as u64).unwrap();
                for index in [&first, &second] {
                    index
                        .try_insert(IndexValue::U64(10 + row_id as u64), row_id)
                        .unwrap();
                }
            }
            for index in [&first, &second] {
                table.bind_index(index.clone(), key).unwrap();
            }
            let mut tx = table.begin_transaction().unwrap();
            table.write(&mut tx, 0, 19).unwrap();
            table.write(&mut tx, 0, 20).unwrap();
            table.write(&mut tx, 1, 21).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                table.commit_with_record_before_publish::<Error, _>(&mut tx, |record| {
                    assert_eq!(record.writes.len(), 2);
                    for (row_id, write) in record.writes.iter().enumerate() {
                        assert_eq!(write.row_id, row_id);
                        assert_eq!(write.base_value, 10 + row_id as u64);
                        assert_eq!(write.value, 20 + row_id as u64);
                        assert_eq!(table.latest_value(row_id).unwrap(), Some(write.base_value));
                    }
                    if panics {
                        panic!("codec panic before WAL acceptance");
                    }
                    Err(Error::Index("codec rejected commit".into()))
                })
            }));
            if panics {
                assert!(result.is_err());
            } else {
                assert!(matches!(result.unwrap(), Err(Error::Index(_))));
            }
            assert!(tx.registration.is_none());
            assert!(arena.create_snapshot().in_flight_txids().is_empty());
            for index in [&first, &second] {
                assert_eq!(
                    index.try_entries().unwrap(),
                    vec![(IndexValue::U64(10), 0), (IndexValue::U64(11), 1),]
                );
            }
            assert_eq!(
                table.snapshot_latest_rows().unwrap(),
                vec![(0, 10), (1, 11)]
            );
            // The rollback leaves an operational table and recyclable private
            // versions, rather than merely hiding the failed transaction.
            let mut retry = table.begin_transaction().unwrap();
            table.write(&mut retry, 0, 30).unwrap();
            table
                .commit_with_record_before_publish::<Error, _>(&mut retry, |_| Ok(()))
                .unwrap();
            let mut reader = table.begin_transaction().unwrap();
            assert_eq!(
                table
                    .index_lookup(&mut reader, &first, &IndexCompare::Eq(IndexValue::U64(30)))
                    .unwrap(),
                vec![0]
            );
            table.commit(&mut reader).unwrap();
        }
    }

    #[test]
    fn real_allocation_failure_after_first_destination_rolls_back_without_publishing_rows() {
        let arena = Arc::new(ShmArena::new(2 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
        table.seed_row(0, 10).unwrap();
        let first = SecondaryIndex::new_in_shared("first", Arc::clone(&arena));
        let second = SecondaryIndex::new_in_shared("second", Arc::clone(&arena));
        for index in [&first, &second] {
            index.try_insert(IndexValue::U64(10), 0).unwrap();
            table.bind_index(index.clone(), key).unwrap();
        }
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 20).unwrap();
        let fill_arena = Arc::clone(&arena);
        INDEX_DESTINATION_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |count| {
                if count == 1 {
                    // Fill real shared allocation space AFTER one destination was
                    // installed, so the second fails allocation rather than key
                    // validation. Remaining slack is consumed with tiny blocks.
                    while fill_arena.chunked_arena().alloc([0_u8; 1024]).is_ok() {}
                    while fill_arena.chunked_arena().alloc([0_u8; 1]).is_ok() {}
                }
            }))
        });
        let error = table
            .commit(&mut tx)
            .expect_err("second destination must exhaust arena");
        INDEX_DESTINATION_HOOK.with(|hook| *hook.borrow_mut() = None);
        assert!(matches!(error, Error::Index(_)), "{error:?}");
        table.abort(&mut tx).unwrap();
        assert_eq!(table.latest_value(0).unwrap(), Some(10));
        for index in [&first, &second] {
            assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(10), 0)]);
        }
        let mut read = table
            .begin_transaction()
            .expect("rollback must not poison clean table");
        assert_eq!(
            table
                .index_lookup(&mut read, &first, &IndexCompare::Eq(IndexValue::U64(10)))
                .unwrap(),
            vec![0]
        );
        table.commit(&mut read).unwrap();
    }

    #[test]
    fn reader_paused_inside_row_index_publication_cannot_commit_incomplete_view() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
        table.seed_row(0, 10).unwrap();
        let index = SecondaryIndex::new_in_shared("key", arena);
        index.try_insert(IndexValue::U64(10), 0).unwrap();
        table.bind_index(index.clone(), key).unwrap();
        let table = Arc::new(table);
        let paused = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let writer_table = Arc::clone(&table);
        let writer_paused = Arc::clone(&paused);
        let writer_resume = Arc::clone(&resume);
        let writer = std::thread::spawn(move || {
            let mut tx = writer_table.begin_transaction().unwrap();
            writer_table.write(&mut tx, 0, 20).unwrap();
            INDEX_PUBLICATION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    writer_paused.wait();
                    writer_resume.wait();
                }))
            });
            writer_table.commit(&mut tx).unwrap();
        });
        paused.wait();
        let mut reader = table.begin_transaction().unwrap();
        // Index mutations and heads exist, but writer registration still pins
        // the old visible row. Querying either key must reject the busy latch.
        assert_eq!(table.read(&mut reader, 0).unwrap(), Some(10));
        assert_eq!(
            table.index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::U64(20))),
            Err(Error::SerializationFailure)
        );
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        resume.wait();
        writer.join().unwrap();
        let mut reader = table.begin_transaction().unwrap();
        assert_eq!(
            table
                .index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::U64(20)))
                .unwrap(),
            vec![0]
        );
        assert_eq!(table.read(&mut reader, 0).unwrap(), Some(20));
        assert!(table
            .index_lookup(&mut reader, &index, &IndexCompare::Eq(IndexValue::U64(10)))
            .unwrap()
            .is_empty());
        table.commit(&mut reader).unwrap();
    }
}

#[cfg(test)]
mod predicate_completion_tests {
    use super::*;
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct PredicateRow {
        key: Option<u64>,
        payload: u64,
    }

    fn left_key(row: &PredicateRow) -> Option<IndexValue> {
        row.key.map(IndexValue::U64)
    }

    fn right_key(row: &PredicateRow) -> Option<IndexValue> {
        (row.payload != 0).then_some(IndexValue::U64(row.payload))
    }

    fn fixture(keys: &[Option<u64>]) -> (Arc<OccTable<PredicateRow>>, SecondaryIndex<usize>) {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), keys.len()).unwrap();
        let index = SecondaryIndex::new_in_shared("left", arena);
        for (id, key) in keys.iter().copied().enumerate() {
            table
                .seed_row(id, PredicateRow { key, payload: 0 })
                .unwrap();
            if let Some(key) = key {
                index.try_insert(IndexValue::U64(key), id).unwrap();
            }
        }
        table.bind_index(index.clone(), left_key).unwrap();
        (Arc::new(table), index)
    }

    fn eq(key: u64) -> IndexCompare {
        IndexCompare::Eq(IndexValue::U64(key))
    }

    fn no_live_transactions(table: &OccTable<PredicateRow>) {
        assert!(table
            .shm
            .proc_array()
            .create_snapshot(table.shm.global_txid())
            .in_flight_txids()
            .is_empty());
    }

    #[test]
    fn empty_capture_then_create_or_key_move_rejects_without_concrete_row_dependency() {
        for previous in [None, Some(10)] {
            let (table, index) = fixture(&[previous, None]);
            // The writer is active in the reader's snapshot, so materialization
            // may correctly return the old empty result even after it commits.
            let mut writer = table.begin_transaction().unwrap();
            let mut reader = table.begin_transaction().unwrap();
            let writer_table = Arc::clone(&table);
            INDEX_CANDIDATES_CAPTURED_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    writer_table
                        .write(
                            &mut writer,
                            0,
                            PredicateRow {
                                key: Some(42),
                                payload: 0,
                            },
                        )
                        .unwrap();
                    writer_table.commit(&mut writer).unwrap();
                }));
            });
            assert!(table
                .index_lookup(&mut reader, &index, &eq(42))
                .unwrap()
                .is_empty());
            assert!(
                reader.read_set.is_empty(),
                "concrete-row validation must not mask a missing predicate check"
            );
            table
                .write(
                    &mut reader,
                    1,
                    PredicateRow {
                        key: None,
                        payload: 1,
                    },
                )
                .unwrap();
            assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
            assert_eq!(
                table.latest_value(1).unwrap(),
                Some(PredicateRow {
                    key: None,
                    payload: 0
                })
            );
            assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(42), 0)]);
            no_live_transactions(&table);
            let mut fresh = table.begin_transaction().unwrap();
            assert_eq!(
                table.index_lookup(&mut fresh, &index, &eq(42)).unwrap(),
                vec![0]
            );
            table.commit(&mut fresh).unwrap();
        }
    }

    #[test]
    fn empty_predicate_changed_during_payload_preparation_rejects_before_acceptance() {
        for previous in [None, Some(10)] {
            let (table, index) = fixture(&[previous, None]);
            let mut writer = table.begin_transaction().unwrap();
            let mut reader = table.begin_transaction().unwrap();
            assert!(table
                .index_lookup(&mut reader, &index, &eq(42))
                .unwrap()
                .is_empty());
            assert!(reader.read_set.is_empty());
            table
                .write(
                    &mut reader,
                    1,
                    PredicateRow {
                        key: None,
                        payload: 1,
                    },
                )
                .unwrap();
            let accepted = Cell::new(false);
            let result = table.commit_with_record_prepared::<Error, _, _>(&mut reader, |record| {
                assert_eq!(record.writes.len(), 1);
                assert_eq!(record.writes[0].row_id, 1);
                table
                    .write(
                        &mut writer,
                        0,
                        PredicateRow {
                            key: Some(42),
                            payload: 0,
                        },
                    )
                    .unwrap();
                table.commit(&mut writer).unwrap();
                Ok(|_: &OccCommitRecord<PredicateRow>| {
                    accepted.set(true);
                    Ok(())
                })
            });
            assert_eq!(result, Err(Error::SerializationFailure));
            assert!(
                !accepted.get(),
                "a changed empty predicate must reject before accepting prepared WAL bytes"
            );
            assert_eq!(
                table.latest_value(1).unwrap(),
                Some(PredicateRow {
                    key: None,
                    payload: 0
                })
            );
            assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(42), 0)]);
            no_live_transactions(&table);
        }
    }

    #[test]
    fn equal_bucket_numbers_in_different_indexes_keep_independent_dependencies() {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(Arc::clone(&arena), 1).unwrap();
        table
            .seed_row(
                0,
                PredicateRow {
                    key: None,
                    payload: 0,
                },
            )
            .unwrap();
        let left = SecondaryIndex::new_in_shared("left", Arc::clone(&arena));
        let right = SecondaryIndex::new_in_shared("right", arena);
        table.bind_index(left.clone(), left_key).unwrap();
        table.bind_index(right.clone(), right_key).unwrap();
        assert_ne!(left.header_offset(), right.header_offset());
        assert_eq!(
            left.transactional_key_bucket(&IndexValue::U64(42)).unwrap(),
            right
                .transactional_key_bucket(&IndexValue::U64(42))
                .unwrap()
        );
        let mut reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut reader, &right, &eq(42))
            .unwrap()
            .is_empty());
        assert!(reader.read_set.is_empty());
        let mut unrelated = table.begin_transaction().unwrap();
        table
            .write(
                &mut unrelated,
                0,
                PredicateRow {
                    key: Some(42),
                    payload: 0,
                },
            )
            .unwrap();
        table.commit(&mut unrelated).unwrap();
        table
            .commit(&mut reader)
            .expect("publishing the left index must not change the right predicate");

        let mut reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut reader, &right, &eq(42))
            .unwrap()
            .is_empty());
        let mut relevant = table.begin_transaction().unwrap();
        table
            .write(
                &mut relevant,
                0,
                PredicateRow {
                    key: Some(42),
                    payload: 42,
                },
            )
            .unwrap();
        table.commit(&mut relevant).unwrap();
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert_eq!(left.try_entries().unwrap(), vec![(IndexValue::U64(42), 0)]);
        assert_eq!(right.try_entries().unwrap(), vec![(IndexValue::U64(42), 0)]);
        no_live_transactions(&table);
    }

    #[test]
    fn colliding_predicates_filter_final_own_writes_and_revalidate_an_unread_creation() {
        let (table, index) = fixture(&[Some(1), None, None, None]);
        let bucket = index.transactional_key_bucket(&IndexValue::U64(1)).unwrap();
        let collision = (2..=65_536)
            .find(|value| {
                index
                    .transactional_key_bucket(&IndexValue::U64(*value))
                    .unwrap()
                    == bucket
            })
            .expect("find a real canonical-bucket collision");
        let mut seed = table.begin_transaction().unwrap();
        table
            .write(
                &mut seed,
                1,
                PredicateRow {
                    key: Some(collision),
                    payload: 0,
                },
            )
            .unwrap();
        table.commit(&mut seed).unwrap();
        let mut reader = table.begin_transaction().unwrap();
        table
            .write(
                &mut reader,
                0,
                PredicateRow {
                    key: Some(collision),
                    payload: 0,
                },
            )
            .unwrap();
        table
            .write(
                &mut reader,
                0,
                PredicateRow {
                    key: None,
                    payload: 0,
                },
            )
            .unwrap();
        table
            .write(
                &mut reader,
                2,
                PredicateRow {
                    key: Some(1),
                    payload: 0,
                },
            )
            .unwrap();
        table
            .write(
                &mut reader,
                2,
                PredicateRow {
                    key: Some(collision),
                    payload: 0,
                },
            )
            .unwrap();
        assert!(table
            .index_lookup(&mut reader, &index, &eq(1))
            .unwrap()
            .is_empty());
        assert_eq!(
            table
                .index_lookup(&mut reader, &index, &eq(collision))
                .unwrap(),
            vec![1, 2]
        );
        let repeated = IndexCompare::In(vec![
            IndexValue::U64(1),
            IndexValue::U64(collision),
            IndexValue::U64(1),
            IndexValue::U64(collision),
        ]);
        assert_eq!(
            table.index_lookup(&mut reader, &index, &repeated).unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            reader.index_reads.len(),
            1,
            "one dependency represents this actual collision"
        );
        assert!(reader.read_set.iter().all(|read| read.row_id != 3));
        let mut creator = table.begin_transaction().unwrap();
        table
            .write(
                &mut creator,
                3,
                PredicateRow {
                    key: Some(1),
                    payload: 0,
                },
            )
            .unwrap();
        table.commit(&mut creator).unwrap();
        assert_eq!(table.commit(&mut reader), Err(Error::SerializationFailure));
        assert_eq!(table.latest_value(0).unwrap().unwrap().key, Some(1));
        assert_eq!(table.latest_value(2).unwrap().unwrap().key, None);
        let mut entries = index.try_entries().unwrap();
        entries.sort_unstable();
        assert_eq!(
            entries,
            vec![
                (IndexValue::U64(1), 0),
                (IndexValue::U64(1), 3),
                (IndexValue::U64(collision), 1)
            ]
        );
        no_live_transactions(&table);
    }

    #[test]
    fn crossed_empty_predicates_cannot_both_publish_through_disjoint_write_buckets() {
        use std::sync::mpsc;
        let (table, index) = fixture(&[None, None]);
        assert_ne!(
            index
                .transactional_key_bucket(&IndexValue::U64(42))
                .unwrap(),
            index
                .transactional_key_bucket(&IndexValue::U64(77))
                .unwrap()
        );
        let mut first = table.begin_transaction().unwrap();
        let mut second = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut first, &index, &eq(42))
            .unwrap()
            .is_empty());
        assert!(table
            .index_lookup(&mut second, &index, &eq(77))
            .unwrap()
            .is_empty());
        assert!(first.read_set.is_empty() && second.read_set.is_empty());
        table
            .write(
                &mut first,
                1,
                PredicateRow {
                    key: Some(77),
                    payload: 0,
                },
            )
            .unwrap();
        table
            .write(
                &mut second,
                0,
                PredicateRow {
                    key: Some(42),
                    payload: 0,
                },
            )
            .unwrap();
        assert_ne!(
            table.collect_lock_indices(&first),
            table.collect_lock_indices(&second)
        );
        let (accepted, accepted_rx) = mpsc::channel();
        let (resume, resume_rx) = mpsc::channel();
        let first_table = Arc::clone(&table);
        let worker = std::thread::spawn(move || {
            first_table.commit_with_record_before_publish::<Error, _>(&mut first, |_| {
                accepted.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(())
            })
        });
        accepted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second_accepted = Cell::new(false);
        let second_result =
            table.commit_with_record_before_publish::<Error, _>(&mut second, |_| {
                second_accepted.set(true);
                Ok(())
            });
        // Release/join even in the unsafe mutant, before reporting its failure.
        resume.send(()).unwrap();
        assert_eq!(worker.join().unwrap().unwrap().writes.len(), 1);
        assert_eq!(second_result, Err(Error::SerializationFailure));
        assert!(!second_accepted.get());
        assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::U64(77), 1)]);
        assert_eq!(table.latest_value(0).unwrap().unwrap().key, None);
        no_live_transactions(&table);
    }

    #[test]
    fn actual_publication_stamps_all_old_and_new_buckets_once_and_leaves_others_unchanged() {
        let (table, index) = fixture(&[Some(10), Some(20)]);
        let touched: BTreeSet<_> = [10, 20, 30, 40]
            .into_iter()
            .map(|key| {
                index
                    .transactional_key_bucket(&IndexValue::U64(key))
                    .unwrap()
            })
            .collect();
        let untouched = (0..65_536)
            .map(|key| {
                index
                    .transactional_key_bucket(&IndexValue::U64(key))
                    .unwrap()
            })
            .find(|bucket| !touched.contains(bucket))
            .unwrap();
        let untouched_before = index.transactional_stamp(untouched).unwrap();
        let mut older = table.begin_transaction().unwrap();
        let mut later = table.begin_transaction().unwrap();
        let later_txid = later.txid;
        table.abort(&mut later).unwrap();
        table
            .write(
                &mut older,
                0,
                PredicateRow {
                    key: Some(30),
                    payload: 0,
                },
            )
            .unwrap();
        table
            .write(
                &mut older,
                1,
                PredicateRow {
                    key: Some(40),
                    payload: 0,
                },
            )
            .unwrap();
        table.commit(&mut older).unwrap();
        let stamp = table.current_global_txid() - 1;
        assert!(stamp > later_txid);
        for bucket in touched {
            assert_eq!(index.transactional_stamp(bucket).unwrap(), stamp);
        }
        assert_eq!(
            index.transactional_stamp(untouched).unwrap(),
            untouched_before
        );
        let mut same_key = table.begin_transaction().unwrap();
        table
            .write(
                &mut same_key,
                0,
                PredicateRow {
                    key: Some(30),
                    payload: 1,
                },
            )
            .unwrap();
        let before_commit = table.current_global_txid();
        table.commit(&mut same_key).unwrap();
        assert_eq!(
            table.current_global_txid(),
            before_commit,
            "a payload-only update has no predicate publication to stamp"
        );
        no_live_transactions(&table);
    }

    fn exercise_predicate_finish_cut(initial_key: Option<u64>, after_deregistration: bool) {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::mpsc;

        let (table, index) = fixture(&[initial_key, None]);
        let mut writer = table.begin_transaction().unwrap();
        let writer_txid = writer.txid;
        let mut empty_reader = table.begin_transaction().unwrap();
        assert!(table
            .index_lookup(&mut empty_reader, &index, &eq(42))
            .unwrap()
            .is_empty());
        assert!(empty_reader.read_set.is_empty());
        table
            .write(
                &mut writer,
                0,
                PredicateRow {
                    key: Some(42),
                    payload: 0,
                },
            )
            .unwrap();
        let (parked_send, parked_receive) = mpsc::channel();
        let (resume_send, resume_receive) = mpsc::channel();
        let writer_table = Arc::clone(&table);
        let worker = std::thread::spawn(move || {
            // Install only after all posting operations: their short ProcArray
            // pins must not consume the lifecycle hook intended for this commit.
            INDEX_PUBLICATION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let pause: Box<dyn FnOnce()> = Box::new(move || {
                        parked_send.send(()).unwrap();
                        resume_receive.recv().unwrap();
                    });
                    if after_deregistration {
                        TRANSACTION_FINISHED_HOOK.with(|hook| *hook.borrow_mut() = Some(pause));
                    } else {
                        TRANSACTION_FINISHING_HOOK.with(|hook| *hook.borrow_mut() = Some(pause));
                    }
                }));
            });
            writer_table.commit(&mut writer)
        });
        parked_receive.recv_timeout(Duration::from_secs(5)).unwrap();
        let active_at_cut = table
            .shm
            .create_snapshot()
            .in_flight_txids()
            .contains(&writer_txid);
        let mut late_reader = table.begin_transaction().unwrap();
        let late_excludes_writer = late_reader.snapshot_active.contains(&writer_txid);
        let late_row = table.read(&mut late_reader, 0).unwrap();
        let late_txid = late_reader.txid;

        // Exercise both public lookup and commit, not just the raw latch. The
        // existing contention hook proves the rejection came from a held bucket.
        let mut blocked_reader = table.begin_transaction().unwrap();
        let lookup_contended = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&lookup_contended);
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || observed.store(true, Ordering::Release)));
        });
        let queried_key = initial_key.unwrap_or(42);
        let blocked_lookup = table.index_lookup(&mut blocked_reader, &index, &eq(queried_key));
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| *hook.borrow_mut() = None);
        table.abort(&mut blocked_reader).unwrap();
        let commit_contended = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&commit_contended);
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || observed.store(true, Ordering::Release)));
        });
        let empty_commit = table.commit(&mut empty_reader);
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| *hook.borrow_mut() = None);

        // Release and join before assertions, so every negative control cleans
        // up its parked writer even when a broken publication order is exposed.
        resume_send.send(()).unwrap();
        let writer_result = worker.join().unwrap();
        let late_lookup = table.index_lookup(&mut late_reader, &index, &eq(queried_key));
        table.abort(&mut late_reader).unwrap();
        let stamp = index
            .transactional_stamp(
                index
                    .transactional_key_bucket(&IndexValue::U64(queried_key))
                    .unwrap(),
            )
            .unwrap();
        let mut fresh = table.begin_transaction().unwrap();
        let fresh_new = table.index_lookup(&mut fresh, &index, &eq(42)).unwrap();
        let fresh_old =
            initial_key.map(|key| table.index_lookup(&mut fresh, &index, &eq(key)).unwrap());
        table.commit(&mut fresh).unwrap();
        no_live_transactions(&table);

        assert_eq!(writer_result, Ok(1));
        assert_eq!(active_at_cut, !after_deregistration);
        assert_eq!(late_excludes_writer, !after_deregistration);
        let visible_key = if after_deregistration {
            Some(42)
        } else {
            initial_key
        };
        assert_eq!(late_row.unwrap().key, visible_key);
        assert!(
            lookup_contended.load(Ordering::Acquire),
            "lookup must encounter the held predicate guard"
        );
        assert_eq!(blocked_lookup, Err(Error::SerializationFailure));
        assert!(
            commit_contended.load(Ordering::Acquire),
            "empty-query commit must encounter the held predicate guard"
        );
        assert_eq!(empty_commit, Err(Error::SerializationFailure));
        assert_eq!(late_lookup, Err(Error::SerializationFailure),
                "a reader registered at this publication cut must retry; stale stamps could hide the moved old key");
        assert!(stamp >= late_txid && stamp > writer_txid,
                "publication must reserve a fresh stamp after deregistration and after this reader registered");
        assert_eq!(fresh_new, vec![0]);
        assert!(fresh_old.is_none_or(|rows| rows.is_empty()));
    }

    #[test]
    fn predicate_lifecycle_empty_creation_guards_both_deregistration_stamp_cuts() {
        for after_deregistration in [false, true] {
            exercise_predicate_finish_cut(None, after_deregistration);
        }
    }

    #[test]
    fn predicate_lifecycle_key_move_guards_both_deregistration_stamp_cuts() {
        for after_deregistration in [false, true] {
            exercise_predicate_finish_cut(Some(10), after_deregistration);
        }
    }

    #[test]
    fn captured_lookup_retains_history_through_delete_aba_and_own_write_overlay() {
        use std::sync::Mutex;

        for writer_was_active in [true, false] {
            let (table, index) = fixture(&[Some(42), Some(42), None]);
            let original_offset = table.row_head_offset(0).unwrap();
            let older = writer_was_active.then(|| table.begin_transaction().unwrap());
            let mut reader = table.begin_transaction().unwrap();
            let reader_xmax = reader.snapshot_xmax;
            let reader_active = reader.snapshot_active.clone();
            table
                .write(
                    &mut reader,
                    1,
                    PredicateRow {
                        key: None,
                        payload: 10,
                    },
                )
                .unwrap();
            table
                .write(
                    &mut reader,
                    2,
                    PredicateRow {
                        key: Some(42),
                        payload: 900,
                    },
                )
                .unwrap();
            table
                .write(
                    &mut reader,
                    2,
                    PredicateRow {
                        key: Some(42),
                        payload: 901,
                    },
                )
                .unwrap();
            let first_lookup = table.index_lookup(&mut reader, &index, &eq(42)).unwrap();
            let dependencies = reader.index_reads.len();

            // The hook runs after raw IDs and dependencies are captured and
            // every query bucket guard has been dropped. The second lookup
            // reuses the same dependency before this actual intervening work.
            let observations = Arc::new(Mutex::new((Vec::new(), Vec::new(), Vec::new(), 0)));
            let observed = Arc::clone(&observations);
            let writer_table = Arc::clone(&table);
            INDEX_CANDIDATES_CAPTURED_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let mut deleting =
                        older.unwrap_or_else(|| writer_table.begin_transaction().unwrap());
                    observed.lock().unwrap().3 = deleting.txid;
                    writer_table
                        .write(
                            &mut deleting,
                            0,
                            PredicateRow {
                                key: None,
                                payload: 100,
                            },
                        )
                        .unwrap();
                    let committed = writer_table.commit(&mut deleting);
                    observed.lock().unwrap().0.push(committed.is_ok());
                    if committed.is_err() {
                        let _ = writer_table.abort(&mut deleting);
                        return;
                    }
                    observed
                        .lock()
                        .unwrap()
                        .1
                        .push(crate::run_vacuum_pass(&writer_table).unwrap().len());
                    // Delete the posting, move it twice, then restore the same
                    // key/row pair with a different value: predicate ABA.
                    for (n, key) in [Some(77), Some(42), None, Some(42)].into_iter().enumerate() {
                        let mut changing = writer_table.begin_transaction().unwrap();
                        writer_table
                            .write(
                                &mut changing,
                                0,
                                PredicateRow {
                                    key,
                                    payload: 200 + n as u64,
                                },
                            )
                            .unwrap();
                        let committed = writer_table.commit(&mut changing);
                        observed.lock().unwrap().0.push(committed.is_ok());
                        if committed.is_err() {
                            let _ = writer_table.abort(&mut changing);
                            return;
                        }
                        observed
                            .lock()
                            .unwrap()
                            .2
                            .push(writer_table.row_head_offset(0).unwrap());
                        observed
                            .lock()
                            .unwrap()
                            .1
                            .push(crate::run_vacuum_pass(&writer_table).unwrap().len());
                    }
                }));
            });
            let captured_lookup = table.index_lookup(&mut reader, &index, &eq(42));
            let historical = table.read(&mut reader, 0);
            let own_final = table.read(&mut reader, 2);
            let predicate_conflict = table.index_read_conflict(&reader);
            let repeated_lookup = table.index_lookup(&mut reader, &index, &eq(42));
            let dependencies_after = reader.index_reads.len();
            let reader_commit = table.commit(&mut reader);
            let reclaimed = crate::run_vacuum_pass(&table).unwrap();
            let (commits, vacuum_counts, head_offsets, deleting_txid) =
                observations.lock().unwrap().clone();

            assert_eq!(
                commits,
                vec![true; 5],
                "captured candidate materialization must not retain predicate guards"
            );
            assert_eq!(reader_active.contains(&deleting_txid), writer_was_active);
            assert_eq!(deleting_txid < reader_xmax, writer_was_active);
            assert_eq!(
                first_lookup,
                vec![0, 2],
                "final own writes must remove and add raw candidates"
            );
            assert_eq!(
                captured_lookup,
                Ok(vec![0, 2]),
                "captured candidates must materialize the pinned historical predicate"
            );
            assert_eq!(
                historical,
                Ok(Some(PredicateRow {
                    key: Some(42),
                    payload: 0
                })),
                "older-active and newer versions must both remain invisible"
            );
            assert_eq!(
                own_final,
                Ok(Some(PredicateRow {
                    key: Some(42),
                    payload: 901
                }))
            );
            assert!(
                vacuum_counts.iter().all(|count| *count == 0),
                "reader must retain history while vacuum attempts reclamation"
            );
            assert!(head_offsets.iter().all(|offset| *offset != original_offset));
            assert_eq!(predicate_conflict, Ok(true));
            assert_eq!(
                repeated_lookup,
                Err(Error::SerializationFailure),
                "restoring the same posting must not erase its publication dependency"
            );
            assert_eq!(dependencies_after, dependencies);
            assert_eq!(reader_commit, Err(Error::SerializationFailure));
            assert_eq!(reclaimed.len(), 5);
            assert_eq!(table.latest_value(1).unwrap().unwrap().key, Some(42));
            assert_eq!(table.latest_value(2).unwrap().unwrap().key, None);
            let mut reuse = table.begin_transaction().unwrap();
            table
                .write(
                    &mut reuse,
                    0,
                    PredicateRow {
                        key: Some(99),
                        payload: 999,
                    },
                )
                .unwrap();
            table.commit(&mut reuse).unwrap();
            assert_eq!(
                table.row_head_offset(0).unwrap(),
                original_offset,
                "the protected version becomes reusable after its reader finishes"
            );
            no_live_transactions(&table);
        }
    }

    fn retention_native_table(rows: usize) -> Arc<OccTable<u64>> {
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let table = Arc::new(OccTable::new(arena, rows).unwrap());
        for id in 0..rows {
            table.seed_row(id, 10 + id as u64).unwrap();
        }
        table
    }

    fn retention_native_replace(table: &OccTable<u64>, row: usize, value: u64) {
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, row, value).unwrap();
        table.commit(&mut tx).unwrap();
    }

    #[test]
    fn retention_native_partial_publication_retains_snapshot_until_reuse() {
        use std::cell::RefCell;
        use std::rc::Rc;
        for prepared in [false, true] {
            for after_head in [false, true] {
                let table = retention_native_table(2);
                let old_offsets = [
                    table.row_head_offset(0).unwrap(),
                    table.row_head_offset(1).unwrap(),
                ];
                let mut writer = table.begin_transaction().unwrap();
                let writer_id = writer.txid;
                table.write(&mut writer, 0, 20).unwrap();
                table.write(&mut writer, 1, 21).unwrap();
                let captured = Rc::new(RefCell::new(None));
                let observation = Rc::clone(&captured);
                let during = Arc::clone(&table);
                ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move |row_id, head_published| {
                        if row_id != 0
                            || head_published != after_head
                            || observation.borrow().is_some()
                        {
                            return;
                        }
                        let mut reader = during.begin_transaction().unwrap();
                        let values = [during.read(&mut reader, 0), during.read(&mut reader, 1)];
                        let heads = [
                            during.row_head_offset(0).unwrap(),
                            during.row_head_offset(1).unwrap(),
                        ];
                        *observation.borrow_mut() = Some((reader, values, heads));
                    }));
                });
                let committed = if prepared {
                    table.commit_with_record_before_publish(&mut writer, |_| Ok::<(), Error>(()))
                } else {
                    table.commit_with_record(&mut writer)
                };
                ROW_PUBLICATION_STEP_HOOK.with(|hook| {
                    hook.borrow_mut().take();
                });
                committed.unwrap();
                let (mut reader, values, heads) = captured
                    .borrow_mut()
                    .take()
                    .expect("publication cut executed");
                assert_eq!(heads[0] != old_offsets[0], after_head);
                assert_eq!(
                    heads[1], old_offsets[1],
                    "second row must still be unpublished at the cut"
                );
                assert!(reader.snapshot_active.contains(&writer_id));
                assert_eq!(
                    values,
                    [Ok(Some(10)), Ok(Some(11))],
                    "retention-native: partial publication must expose one old snapshot"
                );
                assert_eq!(
                    table.read(&mut reader, 0),
                    Ok(Some(10)),
                    "retention-native: partial publication must expose one old snapshot"
                );
                assert_eq!(
                    table.read(&mut reader, 1),
                    Ok(Some(11)),
                    "retention-native: partial publication must expose one old snapshot"
                );
                assert!(
                    crate::run_vacuum_pass(&table).unwrap().is_empty(),
                    "retention-native: older active writer horizon must retain both bases"
                );
                assert!(table.has_serialization_conflict(&reader).unwrap());
                table.abort(&mut reader).unwrap();
                assert_eq!(
                    crate::run_vacuum_pass(&table).unwrap().len(),
                    2,
                    "retention-native: last reader must release both retired bases"
                );
                for row in 0..2 {
                    retention_native_replace(&table, row, 30 + row as u64);
                }
                assert_eq!(
                    [
                        table.row_head_offset(0).unwrap(),
                        table.row_head_offset(1).unwrap()
                    ],
                    old_offsets,
                    "retention-native: eligible retired bases must be reused"
                );
                assert_eq!(table.latest_value(0).unwrap(), Some(30));
                assert_eq!(table.latest_value(1).unwrap(), Some(31));
            }
        }
    }

    #[test]
    fn retention_native_loaded_cursor_survives_pruned_tail_reuse() {
        use std::cell::RefCell;
        use std::rc::Rc;
        for writer_was_active in [false, true] {
            let table = retention_native_table(1);
            let recyclable_tail = table.row_head_offset(0).unwrap();
            retention_native_replace(&table, 0, 20);
            let visible_anchor = table.row_head_offset(0).unwrap();
            let older = writer_was_active.then(|| table.begin_transaction().unwrap());
            let mut reader = table.begin_transaction().unwrap();
            if let Some(mut writer) = older {
                assert!(reader.snapshot_active.contains(&writer.txid));
                table.write(&mut writer, 0, 30).unwrap();
                table.commit(&mut writer).unwrap();
            } else {
                retention_native_replace(&table, 0, 30);
            }
            let middle = table.row_head_offset(0).unwrap();
            retention_native_replace(&table, 0, 40);
            let head = table.row_head_offset(0).unwrap();
            let observed = Rc::new(RefCell::new(None));
            let captured = Rc::clone(&observed);
            let during = Arc::clone(&table);
            ROW_TRAVERSAL_STEP_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |from, next| {
                    // The real native traversal has loaded `next` but has not yet
                    // resolved that pointer. Vacuum may prune the obsolete tail,
                    // while the cursor and its visible anchor must stay retained.
                    let reclaimed = crate::run_vacuum_pass(&during).unwrap();
                    assert_eq!(
                        reclaimed
                            .iter()
                            .map(|row| row.reclaimed_value)
                            .collect::<Vec<_>>(),
                        vec![10],
                        "retention-native: vacuum may prune only below the retained visible anchor"
                    );
                    let anchor = during
                        .resolve_row_ptr(&RelPtr::from_offset(visible_anchor))
                        .unwrap();
                    assert_eq!(
                        anchor.next.load(Ordering::Acquire),
                        EMPTY_PTR,
                        "retention-native: vacuum must unlink a retired tail before reuse"
                    );
                    retention_native_replace(&during, 0, 50);
                    let reused = during.row_head_offset(0).unwrap();
                    let head_row = during
                        .resolve_row_ptr(&RelPtr::from_offset(reused))
                        .unwrap();
                    assert_eq!(
                        head_row.value, 50,
                        "retention-native: reused storage must contain the new value"
                    );
                    assert_eq!(
                        head_row.xmax.load(Ordering::Acquire),
                        0,
                        "retention-native: reused storage must clear retired deletion metadata"
                    );
                    *captured.borrow_mut() = Some((from, next, reused));
                }));
            });
            let value = table.read(&mut reader, 0);
            let (from, next, reused) = observed.borrow_mut().take().expect("cursor cut executed");
            assert_eq!((from, next), (head, middle));
            assert_ne!(next, recyclable_tail);
            assert_eq!(
                reused, recyclable_tail,
                "retention-native: an unreachable obsolete tail must be reusable during traversal"
            );
            assert_eq!(
                value,
                Ok(Some(20)),
                "retention-native: loaded cursor must reach its retained snapshot anchor"
            );
            assert_eq!(
                reader.read_set[0].row_ptr.load(Ordering::Acquire),
                visible_anchor
            );
            assert_eq!(table.latest_value(0).unwrap(), Some(50));
            assert!(crate::run_vacuum_pass(&table).unwrap().is_empty());
            table.abort(&mut reader).unwrap();
            assert_eq!(
                crate::run_vacuum_pass(&table).unwrap().len(),
                3,
                "retention-native: all formerly traversed versions release after the reader"
            );
        }
    }
}
