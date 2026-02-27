use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crate::procarray::{ProcArrayError, ProcArrayRegistration};
use crate::shm::{RelPtr, ShmAllocError, ShmArena};
use crate::TxId;

const EMPTY_PTR: u32 = 0;

#[repr(C, align(64))]
struct OccSharedHeader {
    commit_lock: AtomicU32,
}

impl OccSharedHeader {
    #[inline]
    fn new() -> Self {
        Self {
            commit_lock: AtomicU32::new(0),
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
    next: AtomicU32,
    pub value: T,
}

impl<T: Copy> OccRow<T> {
    #[inline]
    fn new(value: T, xmin: TxId, next: u32) -> Self {
        Self {
            xmin,
            xmax: AtomicU64::new(0),
            next: AtomicU32::new(next),
            value,
        }
    }
}

struct ReadSetEntry<T: Copy> {
    row_ptr: RelPtr<OccRow<T>>,
    observed_xmin: TxId,
}

struct PendingWrite<T: Copy> {
    row_id: usize,
    base_ptr: RelPtr<OccRow<T>>,
    new_ptr: RelPtr<OccRow<T>>,
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

    #[inline]
    pub fn capacity(&self) -> usize {
        self.index_slots.len()
    }

    pub fn seed_row(&self, row_id: usize, value: T) -> Result<(), Error> {
        let slot = self.slot_ref(row_id)?;
        let seed_txid = self.shm.global_txid().fetch_add(1, Ordering::AcqRel);
        let row_ptr = self
            .shm
            .chunked_arena()
            .alloc(OccRow::new(value, seed_txid, 0))?;
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
        tx.write_set.truncate(write_len);
        tx.savepoints.truncate(savepoint_idx + 1);
        Ok(())
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

        let slot = self.slot_ref(row_id)?;
        let mut head_offset = slot.head.load(Ordering::Acquire);

        while head_offset != EMPTY_PTR {
            let row_ptr = RelPtr::from_offset(head_offset);
            let row = self.resolve_row_ptr(&row_ptr)?;

            if self.is_visible(row, tx) {
                let observed_xmin = row.xmin;
                self.record_read(tx, row_ptr, observed_xmin);
                return Ok(Some(row.value));
            }

            head_offset = row.next.load(Ordering::Acquire);
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
            let slot = self.slot_ref(row_id)?;
            let mut head_offset = slot.head.load(Ordering::Acquire);
            let mut visible_ptr: Option<RelPtr<OccRow<T>>> = None;

            while head_offset != EMPTY_PTR {
                let row_ptr = RelPtr::from_offset(head_offset);
                let row = self.resolve_row_ptr(&row_ptr)?;
                if self.is_visible(row, tx) {
                    visible_ptr = Some(row_ptr);
                    break;
                }
                head_offset = row.next.load(Ordering::Acquire);
            }

            visible_ptr.ok_or(Error::RowMissing { row_id })?
        };

        let base_offset = base_ptr.load(Ordering::Acquire);
        let new_ptr = self
            .shm
            .chunked_arena()
            .alloc(OccRow::new(value, tx.txid, base_offset))?;

        tx.write_set.push(PendingWrite {
            row_id,
            base_ptr,
            new_ptr,
        });
        Ok(())
    }

    pub fn abort(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        self.clear_local_sets(tx);
        self.finish_transaction(tx)
    }

    pub fn commit(&self, tx: &mut OccTransaction<T>) -> Result<usize, Error> {
        self.ensure_open(tx)?;
        let _lock = self.acquire_commit_lock()?;

        if self.has_serialization_conflict(tx)? {
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure);
        }

        let final_write_indices = self.final_write_indices(tx);

        if self.has_write_base_conflict(tx, &final_write_indices)? {
            self.abort_for_serialization_failure(tx);
            return Err(Error::SerializationFailure);
        }

        self.publish_write_set(tx, &final_write_indices)?;
        let applied = final_write_indices.len();

        self.clear_local_sets(tx);
        self.finish_transaction(tx)?;
        Ok(applied)
    }

    fn publish_write_set(
        &self,
        tx: &OccTransaction<T>,
        final_write_indices: &[usize],
    ) -> Result<(), Error> {
        for idx in final_write_indices {
            let write = &tx.write_set[*idx];
            let slot = self.slot_ref(write.row_id)?;

            let base_offset = write.base_ptr.load(Ordering::Acquire);
            let new_offset = write.new_ptr.load(Ordering::Acquire);

            if base_offset != EMPTY_PTR {
                let base_row = self.resolve_row_ptr(&write.base_ptr)?;
                if base_row
                    .xmax
                    .compare_exchange(0, tx.txid, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(Error::SerializationFailure);
                }
            }

            let new_row = self.resolve_row_ptr(&write.new_ptr)?;
            new_row.next.store(base_offset, Ordering::Release);

            if slot
                .head
                .compare_exchange(base_offset, new_offset, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Error::SerializationFailure);
            }
        }

        Ok(())
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

    fn record_read(&self, tx: &mut OccTransaction<T>, row_ptr: RelPtr<OccRow<T>>, xmin: TxId) {
        let row_offset = row_ptr.load(Ordering::Acquire);
        if tx
            .read_set
            .iter()
            .any(|entry| entry.row_ptr.load(Ordering::Acquire) == row_offset)
        {
            return;
        }

        tx.read_set.push(ReadSetEntry {
            row_ptr,
            observed_xmin: xmin,
        });
    }

    fn acquire_commit_lock(&self) -> Result<CommitLockGuard<'_>, Error> {
        let header = self.shared_header_ref()?;
        while header
            .commit_lock
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            std::hint::spin_loop();
        }

        Ok(CommitLockGuard {
            lock: &header.commit_lock,
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

    fn abort_for_serialization_failure(&self, tx: &mut OccTransaction<T>) {
        self.clear_local_sets(tx);
        let _ = self.finish_transaction(tx);
    }

    fn finish_transaction(&self, tx: &mut OccTransaction<T>) -> Result<(), Error> {
        let Some(registration) = tx.registration.take() else {
            return Ok(());
        };
        self.shm.end_transaction(registration)?;
        Ok(())
    }

    #[inline]
    fn clear_local_sets(&self, tx: &mut OccTransaction<T>) {
        tx.read_set.clear();
        tx.write_set.clear();
        tx.savepoints.clear();
    }

    #[inline]
    fn ensure_open(&self, tx: &OccTransaction<T>) -> Result<(), Error> {
        if tx.registration.is_none() {
            return Err(Error::TransactionClosed);
        }
        Ok(())
    }
}

struct CommitLockGuard<'a> {
    lock: &'a AtomicU32,
}

impl Drop for CommitLockGuard<'_> {
    fn drop(&mut self) {
        self.lock.store(0, Ordering::Release);
    }
}
