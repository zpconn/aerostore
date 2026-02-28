use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::occ::{OccTable, OccTransaction};
use crate::rbo_planner::{compare_optional, AccessPath, CompiledPlan, PlannerError, StapiRow};
use crate::shm::{RelPtr, ShmAllocError, ShmArena};

const EMPTY_OFFSET: u32 = 0;
const PK_INLINE_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimaryKeyMapError {
    InvalidBucketCount(usize),
    InvalidRowCapacity(usize),
    KeyTooLong { len: usize, max: usize },
    CapacityExceeded { capacity: usize },
    InvalidHeader(u32),
    InvalidBucket(u32),
    InvalidEntry(u32),
    Allocation(String),
}

impl fmt::Display for PrimaryKeyMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrimaryKeyMapError::InvalidBucketCount(count) => {
                write!(f, "primary key map bucket count {} is invalid", count)
            }
            PrimaryKeyMapError::InvalidRowCapacity(capacity) => {
                write!(f, "primary key map row capacity {} is invalid", capacity)
            }
            PrimaryKeyMapError::KeyTooLong { len, max } => {
                write!(f, "primary key length {} exceeds max {}", len, max)
            }
            PrimaryKeyMapError::CapacityExceeded { capacity } => {
                write!(f, "primary key map capacity {} exhausted", capacity)
            }
            PrimaryKeyMapError::InvalidHeader(offset) => {
                write!(f, "invalid primary key map header offset {}", offset)
            }
            PrimaryKeyMapError::InvalidBucket(offset) => {
                write!(f, "invalid primary key map bucket offset {}", offset)
            }
            PrimaryKeyMapError::InvalidEntry(offset) => {
                write!(f, "invalid primary key map entry offset {}", offset)
            }
            PrimaryKeyMapError::Allocation(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PrimaryKeyMapError {}

impl From<ShmAllocError> for PrimaryKeyMapError {
    fn from(value: ShmAllocError) -> Self {
        PrimaryKeyMapError::Allocation(value.to_string())
    }
}

#[repr(C, align(64))]
struct PkMapHeader {
    bucket_count: u32,
    row_capacity: u32,
    next_row_id: AtomicU32,
}

impl PkMapHeader {
    #[inline]
    fn new(bucket_count: u32, row_capacity: u32) -> Self {
        Self {
            bucket_count,
            row_capacity,
            next_row_id: AtomicU32::new(0),
        }
    }
}

#[repr(C, align(64))]
struct PkBucket {
    head: AtomicU32,
}

impl PkBucket {
    #[inline]
    fn new() -> Self {
        Self {
            head: AtomicU32::new(EMPTY_OFFSET),
        }
    }
}

#[repr(C)]
struct PkEntry {
    hash: u64,
    key_len: u16,
    _pad: [u8; 2],
    row_id: u32,
    next: AtomicU32,
    key: [u8; PK_INLINE_BYTES],
}

impl PkEntry {
    #[inline]
    fn new(hash: u64, key: &[u8], row_id: u32) -> Self {
        let mut encoded = [0_u8; PK_INLINE_BYTES];
        encoded[..key.len()].copy_from_slice(key);
        Self {
            hash,
            key_len: key.len() as u16,
            _pad: [0_u8; 2],
            row_id,
            next: AtomicU32::new(EMPTY_OFFSET),
            key: encoded,
        }
    }

    #[inline]
    fn key_equals(&self, key: &[u8], hash: u64) -> bool {
        if self.hash != hash {
            return false;
        }
        if self.key_len as usize != key.len() {
            return false;
        }
        self.key[..key.len()] == *key
    }
}

#[derive(Clone)]
pub struct ShmPrimaryKeyMap {
    shm: Arc<ShmArena>,
    header_offset: u32,
    bucket_offsets: Arc<[u32]>,
}

impl ShmPrimaryKeyMap {
    pub fn new_in_shared(
        shm: Arc<ShmArena>,
        bucket_count: usize,
        row_capacity: usize,
    ) -> Result<Self, PrimaryKeyMapError> {
        if bucket_count == 0 {
            return Err(PrimaryKeyMapError::InvalidBucketCount(bucket_count));
        }
        if row_capacity == 0 || row_capacity > u32::MAX as usize {
            return Err(PrimaryKeyMapError::InvalidRowCapacity(row_capacity));
        }
        if bucket_count > u32::MAX as usize {
            return Err(PrimaryKeyMapError::InvalidBucketCount(bucket_count));
        }

        let arena = shm.chunked_arena();
        let mut bucket_offsets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            let offset = arena.alloc(PkBucket::new())?.load(AtomicOrdering::Acquire);
            bucket_offsets.push(offset);
        }

        let header = PkMapHeader::new(bucket_count as u32, row_capacity as u32);
        let header_offset = arena.alloc(header)?.load(AtomicOrdering::Acquire);

        Ok(Self {
            shm,
            header_offset,
            bucket_offsets: Arc::from(bucket_offsets.into_boxed_slice()),
        })
    }

    #[inline]
    pub fn row_capacity(&self) -> usize {
        self.header_ref()
            .map(|header| header.row_capacity as usize)
            .unwrap_or(0)
    }

    pub fn get(&self, key: &str) -> Result<Option<usize>, PrimaryKeyMapError> {
        let key_bytes = key.as_bytes();
        if key_bytes.len() > PK_INLINE_BYTES {
            return Err(PrimaryKeyMapError::KeyTooLong {
                len: key_bytes.len(),
                max: PK_INLINE_BYTES,
            });
        }
        if key_bytes.is_empty() {
            return Ok(None);
        }

        let hash = hash_key(key_bytes);
        let bucket = self.bucket_ref_for_hash(hash)?;
        let row_id = self.find_in_bucket(bucket, key_bytes, hash)?;
        Ok(row_id.map(|id| id as usize))
    }

    pub fn insert_existing(&self, key: &str, row_id: usize) -> Result<usize, PrimaryKeyMapError> {
        let key_bytes = key.as_bytes();
        if key_bytes.is_empty() {
            return Ok(row_id);
        }
        if key_bytes.len() > PK_INLINE_BYTES {
            return Err(PrimaryKeyMapError::KeyTooLong {
                len: key_bytes.len(),
                max: PK_INLINE_BYTES,
            });
        }

        let row_id_u32 = u32::try_from(row_id)
            .map_err(|_| PrimaryKeyMapError::CapacityExceeded { capacity: row_id })?;
        let header = self.header_ref()?;
        if row_id_u32 >= header.row_capacity {
            return Err(PrimaryKeyMapError::CapacityExceeded {
                capacity: header.row_capacity as usize,
            });
        }

        if let Some(existing) = self.get(key)? {
            return Ok(existing);
        }

        let hash = hash_key(key_bytes);
        let bucket = self.bucket_ref_for_hash(hash)?;
        let entry_offset = self.allocate_entry(hash, key_bytes, row_id_u32)?;

        loop {
            let head = bucket.head.load(AtomicOrdering::Acquire);
            let entry = self.entry_ref(entry_offset)?;
            entry.next.store(head, AtomicOrdering::Release);

            if bucket
                .head
                .compare_exchange(
                    head,
                    entry_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                self.bump_next_row_id(row_id_u32.saturating_add(1))?;
                return Ok(row_id_u32 as usize);
            }

            if let Some(existing) = self.find_in_bucket(bucket, key_bytes, hash)? {
                return Ok(existing as usize);
            }

            std::hint::spin_loop();
        }
    }

    pub fn get_or_insert(&self, key: &str) -> Result<usize, PrimaryKeyMapError> {
        let key_bytes = key.as_bytes();
        if key_bytes.is_empty() {
            return Err(PrimaryKeyMapError::KeyTooLong {
                len: 0,
                max: PK_INLINE_BYTES,
            });
        }
        if key_bytes.len() > PK_INLINE_BYTES {
            return Err(PrimaryKeyMapError::KeyTooLong {
                len: key_bytes.len(),
                max: PK_INLINE_BYTES,
            });
        }

        if let Some(existing) = self.get(key)? {
            return Ok(existing);
        }

        let reserved_row_id = self.reserve_row_id()?;
        let hash = hash_key(key_bytes);
        let bucket = self.bucket_ref_for_hash(hash)?;
        let entry_offset = self.allocate_entry(hash, key_bytes, reserved_row_id)?;

        loop {
            let head = bucket.head.load(AtomicOrdering::Acquire);
            let entry = self.entry_ref(entry_offset)?;
            entry.next.store(head, AtomicOrdering::Release);

            if bucket
                .head
                .compare_exchange(
                    head,
                    entry_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(reserved_row_id as usize);
            }

            if let Some(existing) = self.find_in_bucket(bucket, key_bytes, hash)? {
                return Ok(existing as usize);
            }

            std::hint::spin_loop();
        }
    }

    fn reserve_row_id(&self) -> Result<u32, PrimaryKeyMapError> {
        let header = self.header_ref()?;
        loop {
            let current = header.next_row_id.load(AtomicOrdering::Acquire);
            if current >= header.row_capacity {
                return Err(PrimaryKeyMapError::CapacityExceeded {
                    capacity: header.row_capacity as usize,
                });
            }

            if header
                .next_row_id
                .compare_exchange(
                    current,
                    current + 1,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(current);
            }
            std::hint::spin_loop();
        }
    }

    fn bump_next_row_id(&self, min_next: u32) -> Result<(), PrimaryKeyMapError> {
        let header = self.header_ref()?;
        loop {
            let current = header.next_row_id.load(AtomicOrdering::Acquire);
            if current >= min_next {
                return Ok(());
            }

            if header
                .next_row_id
                .compare_exchange(
                    current,
                    min_next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    fn allocate_entry(
        &self,
        hash: u64,
        key: &[u8],
        row_id: u32,
    ) -> Result<u32, PrimaryKeyMapError> {
        let entry = PkEntry::new(hash, key, row_id);
        Ok(self
            .shm
            .chunked_arena()
            .alloc(entry)?
            .load(AtomicOrdering::Acquire))
    }

    fn find_in_bucket(
        &self,
        bucket: &PkBucket,
        key: &[u8],
        hash: u64,
    ) -> Result<Option<u32>, PrimaryKeyMapError> {
        let mut curr = bucket.head.load(AtomicOrdering::Acquire);
        while curr != EMPTY_OFFSET {
            let entry = self.entry_ref(curr)?;
            if entry.key_equals(key, hash) {
                return Ok(Some(entry.row_id));
            }
            curr = entry.next.load(AtomicOrdering::Acquire);
        }
        Ok(None)
    }

    fn bucket_ref_for_hash(&self, hash: u64) -> Result<&PkBucket, PrimaryKeyMapError> {
        let idx = (hash as usize) % self.bucket_offsets.len();
        self.bucket_ref(idx)
    }

    fn header_ref(&self) -> Result<&PkMapHeader, PrimaryKeyMapError> {
        RelPtr::<PkMapHeader>::from_offset(self.header_offset)
            .as_ref(self.shm.mmap_base())
            .ok_or(PrimaryKeyMapError::InvalidHeader(self.header_offset))
    }

    fn bucket_ref(&self, idx: usize) -> Result<&PkBucket, PrimaryKeyMapError> {
        let offset = self
            .bucket_offsets
            .get(idx)
            .copied()
            .ok_or(PrimaryKeyMapError::InvalidBucket(EMPTY_OFFSET))?;
        RelPtr::<PkBucket>::from_offset(offset)
            .as_ref(self.shm.mmap_base())
            .ok_or(PrimaryKeyMapError::InvalidBucket(offset))
    }

    fn entry_ref(&self, offset: u32) -> Result<&PkEntry, PrimaryKeyMapError> {
        RelPtr::<PkEntry>::from_offset(offset)
            .as_ref(self.shm.mmap_base())
            .ok_or(PrimaryKeyMapError::InvalidEntry(offset))
    }
}

#[derive(Clone, Default)]
pub struct ExecutionEngine;

impl ExecutionEngine {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    pub fn execute<T: StapiRow>(
        &self,
        plan: &CompiledPlan<T>,
        table: &OccTable<T>,
        tx: &mut OccTransaction<T>,
    ) -> Result<Vec<T>, PlannerError> {
        let candidate_row_ids = self.candidate_row_ids(plan, table)?;
        let mut rows = Vec::new();

        for row_id in candidate_row_ids {
            if row_id >= table.capacity() {
                continue;
            }
            let Some(row) = table.read(tx, row_id)? else {
                continue;
            };

            if plan
                .residual_filters
                .iter()
                .all(|compiled| (compiled.predicate)(&row))
            {
                rows.push(row);
            }
        }

        if let Some(sort_field) = &plan.sort {
            rows.sort_unstable_by(|left, right| {
                compare_optional(
                    left.field_value(sort_field.as_str()),
                    right.field_value(sort_field.as_str()),
                )
            });
        }

        if let Some(limit) = plan.limit {
            rows.truncate(limit);
        }

        Ok(rows)
    }

    fn candidate_row_ids<T: StapiRow>(
        &self,
        plan: &CompiledPlan<T>,
        table: &OccTable<T>,
    ) -> Result<Vec<usize>, PlannerError> {
        match &plan.access_path {
            AccessPath::PrimaryKeyEq { key, .. } => {
                let Some(pk_map) = plan.catalog.primary_key_map() else {
                    return Ok((0..table.capacity()).collect());
                };

                let found = pk_map.get(key.as_str())?;
                Ok(found.into_iter().collect())
            }
            AccessPath::Indexed { field, compare } => Ok(plan
                .catalog
                .get_index(field.as_str())
                .map(|index| index.lookup(compare))
                .unwrap_or_else(|| (0..table.capacity()).collect())),
            AccessPath::FullScan => Ok((0..table.capacity()).collect()),
        }
    }
}

impl<T: StapiRow> CompiledPlan<T> {
    pub fn execute(
        &self,
        table: &OccTable<T>,
        tx: &mut OccTransaction<T>,
    ) -> Result<Vec<T>, PlannerError> {
        ExecutionEngine::new().execute(self, table, tx)
    }
}

#[inline]
fn hash_key(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
