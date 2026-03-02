use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::procarray::{ProcArrayError, ProcArrayRegistration};
use crate::shm::{RelPtr, ShmAllocError, ShmArena, OCC_PARTITION_LOCKS};
use crate::TxId;

const EMPTY_PTR: u32 = 0;

#[repr(C, align(64))]
struct OccSharedHeader {
    recycled_head: AtomicU32,
}

impl OccSharedHeader {
    #[inline]
    fn new() -> Self {
        Self {
            recycled_head: AtomicU32::new(EMPTY_PTR),
        }
    }
}

#[repr(C)]
struct OccIndexSlot {
    head: AtomicU32,
}

impl OccIndexSlot {
    #[inline]
    fn new() -> Self {
        Self {
            head: AtomicU32::new(EMPTY_PTR),
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

struct ReadSetEntry<T: Copy> {
    row_id: usize,
    row_ptr: RelPtr<OccRow<T>>,
    observed_xmin: TxId,
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
        }
    }
}

impl std::error::Error for Error {}

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
            _marker: PhantomData,
        })
    }

    pub fn from_existing(
        shm: Arc<ShmArena>,
        shared_header_offset: u32,
        index_slot_offsets: Vec<u32>,
    ) -> Result<Self, Error> {
        let shared_header = RelPtr::<OccSharedHeader>::from_offset(shared_header_offset);
        if shared_header.as_ref(shm.mmap_base()).is_none() {
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

    pub fn seed_row(&self, row_id: usize, value: T) -> Result<(), Error> {
        let slot = self.slot_ref(row_id)?;
        let seed_txid = self.shm.global_txid().fetch_add(1, Ordering::AcqRel);
        let row_ptr = self.allocate_row(value, seed_txid, EMPTY_PTR)?;
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
        let registration = self.shm.begin_transaction()?;
        let snapshot = self.shm.create_snapshot();
        let mut snapshot_active = HashSet::with_capacity(snapshot.len());
        for txid in snapshot.in_flight_txids() {
            if *txid != registration.txid {
                snapshot_active.insert(*txid);
            }
        }

        Ok(OccTransaction {
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
                return Err(Error::SerializationFailure);
            }
            visible_ptr
        };

        let base_row = self.resolve_row_ptr(&base_ptr)?;
        let dirty_columns_bitmask =
            crate::wal_delta::coarse_dirty_mask_for_copy(&base_row.value, &value);

        let base_offset = base_ptr.load(Ordering::Acquire);
        let new_ptr = self.allocate_row(value, tx.txid, base_offset)?;

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
                return Err(Error::SerializationFailure);
            }
            visible_ptr
        };

        let base_offset = base_ptr.load(Ordering::Acquire);
        let new_ptr = self.allocate_row(value, tx.txid, base_offset)?;

        tx.write_set.push(PendingWrite {
            row_id,
            base_ptr,
            new_ptr,
            dirty_columns_bitmask,
        });
        Ok(())
    }

    pub fn abort(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        self.clear_local_sets(tx)?;
        self.finish_transaction(tx)
    }

    pub fn commit(&self, tx: &mut OccTransaction<T>) -> Result<usize, Error> {
        Ok(self.commit_with_record(tx)?.writes.len())
    }

    pub fn commit_with_record(
        &self,
        tx: &mut OccTransaction<T>,
    ) -> Result<OccCommitRecord<T>, Error> {
        self.ensure_open(tx)?;
        let final_write_indices = self.final_write_indices(tx);
        let locks = self.acquire_partition_locks(tx);

        if self.has_row_lock_conflict(tx)? {
            drop(locks);
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure);
        }

        if self.has_serialization_conflict(tx)? {
            drop(locks);
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure);
        }

        if self.has_write_base_conflict(tx, &final_write_indices)? {
            drop(locks);
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure);
        }

        let writes = match self.publish_write_set(tx, &final_write_indices) {
            Ok(writes) => writes,
            Err(Error::SerializationFailure) => {
                drop(locks);
                self.abort_for_serialization_failure(tx);
                return Err(Error::SerializationFailure);
            }
            Err(err) => {
                drop(locks);
                return Err(err);
            }
        };
        drop(locks);

        let commit_record = OccCommitRecord {
            txid: tx.txid,
            writes,
        };

        self.recycle_non_final_writes(tx, &final_write_indices)?;
        tx.read_set.clear();
        tx.write_set.clear();
        tx.savepoints.clear();
        self.finish_transaction(tx)?;
        Ok(commit_record)
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

            let new_row = self.resolve_row_ptr(&write.new_ptr)?;
            new_row.next.store(base_offset, Ordering::Release);

            if slot
                .head
                .compare_exchange(base_offset, new_offset, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Error::SerializationFailure);
            }

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
                return Ok(true);
            }

            if expected_head != EMPTY_PTR {
                let base_row = self.resolve_row_ptr(&write.base_ptr)?;
                if base_row.xmax.load(Ordering::Acquire) != 0 {
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
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn has_serialization_conflict(&self, tx: &OccTransaction<T>) -> Result<bool, Error> {
        for read in &tx.read_set {
            let row = self.resolve_row_ptr(&read.row_ptr)?;
            if row.xmin != read.observed_xmin {
                return Ok(true);
            }

            let xmax = row.xmax.load(Ordering::Acquire);
            if xmax == 0 || xmax == tx.txid {
                continue;
            }

            let committed_after_snapshot =
                xmax >= tx.snapshot_xmax || tx.snapshot_active.contains(&xmax);
            if committed_after_snapshot {
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
            self.recycle_row_ptr(&write.new_ptr)?;
        }

        Ok(())
    }

    fn recycle_write_suffix(&self, tx: &OccTransaction<T>, start: usize) -> Result<(), Error> {
        if start >= tx.write_set.len() {
            return Ok(());
        }

        for write in tx.write_set[start..].iter() {
            self.recycle_row_ptr(&write.new_ptr)?;
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

        while head_offset != EMPTY_PTR {
            let row_ptr = RelPtr::from_offset(head_offset);
            let row = self.resolve_row_ptr(&row_ptr)?;
            if self.is_visible(row, tx) {
                return Ok(Some(row_ptr));
            }
            head_offset = row.next.load(Ordering::Acquire);
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

    fn acquire_partition_locks(&self, tx: &OccTransaction<T>) -> PartitionLockGuard<'_> {
        self.acquire_lock_indices(self.collect_lock_indices(tx))
    }

    fn acquire_row_lock(&self, row_id: usize) -> PartitionLockGuard<'_> {
        self.acquire_lock_indices(vec![Self::lock_bucket_for_row_id(row_id)])
    }

    fn acquire_all_partition_locks(&self) -> PartitionLockGuard<'_> {
        self.acquire_lock_indices((0..OCC_PARTITION_LOCKS).collect())
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

    fn acquire_lock_indices(&self, mut lock_indices: Vec<usize>) -> PartitionLockGuard<'_> {
        lock_indices.sort_unstable();
        lock_indices.dedup();

        let locks = self.shm.occ_partition_locks();
        for idx in &lock_indices {
            let mut spins = 0_u32;
            while !locks[*idx].try_lock() {
                spins = spins.wrapping_add(1);
                if spins & 0x3f == 0 {
                    std::thread::yield_now();
                }
                // Under heavy cross-process contention, periodic micro-sleeps
                // reduce lock-holder starvation from pure busy spinning.
                if spins & 0x3ff == 0 {
                    std::thread::sleep(Duration::from_micros(25));
                }
                std::hint::spin_loop();
            }
        }

        PartitionLockGuard {
            locks,
            lock_indices,
        }
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

    fn allocate_row(&self, value: T, xmin: TxId, next: u32) -> Result<RelPtr<OccRow<T>>, Error> {
        if let Some(recycled_ptr) = self.try_pop_recycled_row()? {
            self.initialize_row(&recycled_ptr, value, xmin, next)?;
            return Ok(recycled_ptr);
        }

        Ok(self
            .shm
            .chunked_arena()
            .alloc(OccRow::new(value, xmin, next))?)
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

    fn try_pop_recycled_row(&self) -> Result<Option<RelPtr<OccRow<T>>>, Error> {
        let header = self.shared_header_ref()?;
        let mut spins = 0_u32;

        loop {
            let head = header.recycled_head.load(Ordering::Acquire);
            if head == EMPTY_PTR {
                return Ok(None);
            }

            let head_ptr = RelPtr::<OccRow<T>>::from_offset(head);
            let row = self.resolve_row_ptr(&head_ptr)?;
            let next = row.recycle_next.load(Ordering::Acquire);

            if header
                .recycled_head
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(Some(head_ptr));
            }

            spins = spins.wrapping_add(1);
            if spins & 0x3f == 0 {
                std::thread::yield_now();
            }
            std::hint::spin_loop();
        }
    }

    fn recycle_row_ptr(&self, row_ptr: &RelPtr<OccRow<T>>) -> Result<(), Error> {
        let offset = row_ptr.load(Ordering::Acquire);
        if offset == EMPTY_PTR {
            return Ok(());
        }

        let header = self.shared_header_ref()?;
        let row = self.resolve_row_ptr(row_ptr)?;
        let mut spins = 0_u32;

        loop {
            let old_head = header.recycled_head.load(Ordering::Acquire);
            row.recycle_next.store(old_head, Ordering::Release);

            if header
                .recycled_head
                .compare_exchange(old_head, offset, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }

            spins = spins.wrapping_add(1);
            if spins & 0x3f == 0 {
                std::thread::yield_now();
            }
            std::hint::spin_loop();
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
        self.shm.end_transaction(registration)?;
        Ok(())
    }

    #[inline]
    fn clear_local_sets(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        self.recycle_write_suffix(tx, 0)?;
        tx.read_set.clear();
        tx.write_set.clear();
        tx.savepoints.clear();
        Ok(())
    }

    #[inline]
    fn ensure_open(&self, tx: &OccTransaction<T>) -> Result<(), Error> {
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

    pub fn apply_recovered_write(&self, row_id: usize, txid: TxId, value: T) -> Result<(), Error> {
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
        let new_ptr = self.allocate_row(value, txid, base_offset)?;
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

    pub fn apply_recovered_write_cas(
        &self,
        row_id: usize,
        txid: TxId,
        expected_base_offset: u32,
        value: T,
    ) -> Result<(), Error> {
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

        let new_ptr = self.allocate_row(value, txid, base_offset)?;
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

    pub fn snapshot_latest_rows(&self) -> Result<Vec<(usize, T)>, Error> {
        let _lock = self.acquire_all_partition_locks();
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
