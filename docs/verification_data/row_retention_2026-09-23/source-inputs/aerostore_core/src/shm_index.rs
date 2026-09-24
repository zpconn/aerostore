use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(all(feature = "verified-buckets-sort", feature = "verified-buckets-bitmap"))]
compile_error!("choose only one verified bucket-set candidate feature");

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::index::{IndexCompare, IndexValue};
use crate::procarray::ProcArrayError;
use crate::shm::{RelPtr, ShmArena};
use crate::shm_lock::{ShmMutex, ShmMutexGuard};
use crate::shm_skiplist::{
    ScanBound, ShmSkipKey, ShmSkipList, ShmSkipListError, ShmSkipListGcDaemon,
    ShmSkipMutationTelemetry, MAX_PAYLOAD_BYTES,
};

const KEY_INLINE_BYTES: usize = 128;
const ROWID_INLINE_BYTES: usize = MAX_PAYLOAD_BYTES;
const DEFAULT_INDEX_ARENA_BYTES: usize = 64 << 20;
const INDEX_INSERT_RETRY_LIMIT: usize = 128;
const INDEX_REMOVE_RETRY_LIMIT: usize = 1;
const INDEX_RECLAIM_BATCH: usize = 131_072;
const INDEX_RECLAIM_BATCH_ALLOC_MID: usize = 16_384;
const INDEX_RECLAIM_BATCH_ALLOC_HIGH: usize = 65_536;
const INDEX_RECLAIM_BATCH_STRUCTURAL: usize = INDEX_RECLAIM_BATCH / 4;
const INDEX_ALLOC_RETRY_PHASE_B_START: usize = 16;
const INDEX_ALLOC_RETRY_PHASE_C_START: usize = 64;
const INDEX_ALLOC_RETRY_FLUSH_PERIOD_B: usize = 16;
const INDEX_ALLOC_RETRY_FLUSH_PERIOD_C: usize = 8;

const KEY_TAG_I64: u8 = 1;
const KEY_TAG_U64: u8 = 2;
const KEY_TAG_STRING: u8 = 3;
const KEY_TAG_SENTINEL: u8 = 255;

/// Equality predicates protect a stable hash bucket. Range predicates protect
/// every bucket, conservatively covering insertions into currently empty gaps.
pub(crate) const INDEX_TX_BUCKETS: usize = 4096;
const INDEX_HEADER_MAGIC: u64 = 0x4145_524F_494E_4458;
// Version 1 used 256 publication buckets in the initial development build.
// Reject those headers before interpreting the expanded bucket array.
const INDEX_HEADER_VERSION: u32 = 2;

#[repr(C)]
struct IndexPublicationBucket {
    lock: ShmMutex,
    stamp: AtomicU64,
}

#[repr(C, align(64))]
struct SecondaryIndexHeader {
    magic: u64,
    version: u32,
    skiplist_offset: u32,
    // Binding and unbound raw writes share this lock, so an in-progress raw
    // mutation cannot slip past publication of the managed owner.
    management_lock: ShmMutex,
    owner_table_header: AtomicU32,
    poisoned: AtomicBool,
    buckets: [IndexPublicationBucket; INDEX_TX_BUCKETS],
}

impl SecondaryIndexHeader {
    fn new(skiplist_offset: u32) -> Self {
        Self {
            magic: INDEX_HEADER_MAGIC,
            version: INDEX_HEADER_VERSION,
            skiplist_offset,
            management_lock: ShmMutex::new(),
            owner_table_header: AtomicU32::new(0),
            poisoned: AtomicBool::new(false),
            buckets: std::array::from_fn(|_| IndexPublicationBucket {
                lock: ShmMutex::new(),
                stamp: AtomicU64::new(0),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IndexMutationTelemetry {
    pub insert_ops: u64,
    pub remove_ops: u64,
    pub retry_loops: u64,
    pub retry_alloc: u64,
    pub retry_structural: u64,
    pub retry_epoch: u64,
    pub max_insert_attempts: u64,
    pub max_remove_attempts: u64,
    pub gc_nodes_examined: u64,
    pub gc_nodes_requeued: u64,
    pub gc_recycle_errors: u64,
    pub gc_assist_calls: u64,
    pub gc_assist_reclaimed: u64,
    pub gc_daemon_cycles: u64,
    pub gc_daemon_reclaimed: u64,
    pub pressure_window_failures: u64,
    pub pressure_window_reclaimed: u64,
    pub pressure_consecutive_healthy_windows: u32,
    pub retired_backlog: u64,
    pub retired_postings: u64,
    pub reclaimed_postings: u64,
    pub pressure_state: u32,
    pub pressure_to_normal: u64,
    pub pressure_to_warm: u64,
    pub pressure_to_hot: u64,
    pub alloc_failure_events: u64,
    pub reserve_node_pushes: u64,
    pub reserve_node_hits: u64,
    pub reserve_node_misses: u64,
    pub reserve_posting_pushes: u64,
    pub reserve_posting_hits: u64,
    pub reserve_posting_misses: u64,
    pub reserve_tower_pushes: u64,
    pub reserve_tower_hits: u64,
    pub reserve_tower_misses: u64,
    pub retry_phase_b_hits: u64,
    pub retry_phase_c_hits: u64,
}

impl From<ShmSkipMutationTelemetry> for IndexMutationTelemetry {
    fn from(value: ShmSkipMutationTelemetry) -> Self {
        Self {
            insert_ops: value.insert_ops,
            remove_ops: value.remove_ops,
            retry_loops: value.retry_loops,
            retry_alloc: value.retry_alloc,
            retry_structural: value.retry_structural,
            retry_epoch: value.retry_epoch,
            max_insert_attempts: value.max_insert_attempts,
            max_remove_attempts: value.max_remove_attempts,
            gc_nodes_examined: value.gc_nodes_examined,
            gc_nodes_requeued: value.gc_nodes_requeued,
            gc_recycle_errors: value.gc_recycle_errors,
            gc_assist_calls: value.gc_assist_calls,
            gc_assist_reclaimed: value.gc_assist_reclaimed,
            gc_daemon_cycles: value.gc_daemon_cycles,
            gc_daemon_reclaimed: value.gc_daemon_reclaimed,
            pressure_window_failures: value.pressure_window_failures,
            pressure_window_reclaimed: value.pressure_window_reclaimed,
            pressure_consecutive_healthy_windows: value.pressure_consecutive_healthy_windows,
            retired_backlog: value.retired_backlog,
            retired_postings: value.retired_postings,
            reclaimed_postings: value.reclaimed_postings,
            pressure_state: value.pressure_state,
            pressure_to_normal: value.pressure_to_normal,
            pressure_to_warm: value.pressure_to_warm,
            pressure_to_hot: value.pressure_to_hot,
            alloc_failure_events: value.alloc_failure_events,
            reserve_node_pushes: value.reserve_node_pushes,
            reserve_node_hits: value.reserve_node_hits,
            reserve_node_misses: value.reserve_node_misses,
            reserve_posting_pushes: value.reserve_posting_pushes,
            reserve_posting_hits: value.reserve_posting_hits,
            reserve_posting_misses: value.reserve_posting_misses,
            reserve_tower_pushes: value.reserve_tower_pushes,
            reserve_tower_hits: value.reserve_tower_hits,
            reserve_tower_misses: value.reserve_tower_misses,
            retry_phase_b_hits: value.retry_phase_b_hits,
            retry_phase_c_hits: value.retry_phase_c_hits,
        }
    }
}

#[derive(Debug)]
pub enum ShmIndexError {
    AllocationAudit(String),
    InvalidHeader(u32),
    InvalidNode(u32),
    InvalidPosting(u32),
    InvalidEncoding(&'static str),
    KeyTooLong { len: usize, max: usize },
    RowIdTooLarge { len: usize, max: usize },
    ManagedMutation { owner_table_header: u32 },
    OwnerMismatch { expected: u32, actual: u32 },
    InvalidBucket(usize),
    Poisoned,
    Alloc(crate::shm::ShmAllocError),
    Epoch(ProcArrayError),
    Fork(std::io::Error),
    Wait(std::io::Error),
    Signal(std::io::Error),
}

impl fmt::Display for ShmIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmIndexError::AllocationAudit(message) => {
                write!(f, "shared index allocation audit failed: {}", message)
            }
            ShmIndexError::InvalidHeader(offset) => {
                write!(f, "invalid shared index header offset {}", offset)
            }
            ShmIndexError::InvalidNode(offset) => {
                write!(f, "invalid shared index node offset {}", offset)
            }
            ShmIndexError::InvalidPosting(offset) => {
                write!(f, "invalid shared posting offset {}", offset)
            }
            ShmIndexError::InvalidEncoding(kind) => {
                write!(f, "invalid shared index {} encoding", kind)
            }
            ShmIndexError::KeyTooLong { len, max } => {
                write!(f, "index key length {} exceeds max {}", len, max)
            }
            ShmIndexError::RowIdTooLarge { len, max } => {
                write!(f, "row-id payload length {} exceeds max {}", len, max)
            }
            ShmIndexError::ManagedMutation { owner_table_header } => write!(
                f,
                "index belongs to OCC table {}; mutate it through the table transaction",
                owner_table_header
            ),
            ShmIndexError::OwnerMismatch { expected, actual } => write!(
                f,
                "index owner mismatch: expected table {}, observed {}",
                expected, actual
            ),
            ShmIndexError::InvalidBucket(bucket) => {
                write!(f, "invalid transactional index bucket {}", bucket)
            }
            ShmIndexError::Poisoned => write!(f, "transactional index requires recovery"),
            ShmIndexError::Alloc(err) => write!(f, "shared index allocation failed: {}", err),
            ShmIndexError::Epoch(err) => write!(f, "index ProcArray registration failed: {}", err),
            ShmIndexError::Fork(err) => write!(f, "fork failed: {}", err),
            ShmIndexError::Wait(err) => write!(f, "wait failed: {}", err),
            ShmIndexError::Signal(err) => write!(f, "signal failed: {}", err),
        }
    }
}

impl std::error::Error for ShmIndexError {}

impl From<ShmSkipListError> for ShmIndexError {
    fn from(value: ShmSkipListError) -> Self {
        match value {
            ShmSkipListError::AllocationAudit(message) => ShmIndexError::AllocationAudit(message),
            ShmSkipListError::InvalidHeader(offset) => ShmIndexError::InvalidHeader(offset),
            ShmSkipListError::InvalidNode(offset) => ShmIndexError::InvalidNode(offset),
            ShmSkipListError::InvalidPosting(offset) => ShmIndexError::InvalidPosting(offset),
            ShmSkipListError::InvalidLane { node_offset, .. } => {
                ShmIndexError::InvalidNode(node_offset)
            }
            ShmSkipListError::PayloadTooLarge { len, max } => {
                ShmIndexError::RowIdTooLarge { len, max }
            }
            ShmSkipListError::Alloc(err) => ShmIndexError::Alloc(err),
            ShmSkipListError::Epoch(err) => ShmIndexError::Epoch(err),
            ShmSkipListError::Fork(err) => ShmIndexError::Fork(err),
            ShmSkipListError::Wait(err) => ShmIndexError::Wait(err),
            ShmSkipListError::Signal(err) => ShmIndexError::Signal(err),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct EncodedKey {
    tag: u8,
    len: u16,
    _pad: u8,
    data: [u8; KEY_INLINE_BYTES],
}

impl EncodedKey {
    #[inline]
    fn sentinel() -> Self {
        Self {
            tag: KEY_TAG_SENTINEL,
            len: 0,
            _pad: 0,
            data: [0_u8; KEY_INLINE_BYTES],
        }
    }

    fn from_index_value(value: &IndexValue) -> Result<Self, ShmIndexError> {
        match value {
            IndexValue::I64(v) => Ok(Self::from_i64(*v)),
            IndexValue::U64(v) => Ok(Self::from_u64(*v)),
            IndexValue::String(v) => {
                let mut out = Self {
                    tag: KEY_TAG_STRING,
                    len: 0,
                    _pad: 0,
                    data: [0_u8; KEY_INLINE_BYTES],
                };
                let bytes = v.as_bytes();
                if bytes.len() > KEY_INLINE_BYTES {
                    return Err(ShmIndexError::KeyTooLong {
                        len: bytes.len(),
                        max: KEY_INLINE_BYTES,
                    });
                }
                out.len = bytes.len() as u16;
                out.data[..bytes.len()].copy_from_slice(bytes);
                Ok(out)
            }
        }
    }

    #[inline]
    fn from_u64(value: u64) -> Self {
        let mut out = Self {
            tag: KEY_TAG_U64,
            len: 8,
            _pad: 0,
            data: [0_u8; KEY_INLINE_BYTES],
        };
        out.data[..8].copy_from_slice(&value.to_le_bytes());
        out
    }

    #[inline]
    fn from_i64(value: i64) -> Self {
        let mut out = Self {
            tag: KEY_TAG_I64,
            len: 8,
            _pad: 0,
            data: [0_u8; KEY_INLINE_BYTES],
        };
        out.data[..8].copy_from_slice(&value.to_le_bytes());
        out
    }

    fn as_index_value(&self) -> Option<IndexValue> {
        match self.tag {
            KEY_TAG_I64 => {
                let mut buf = [0_u8; 8];
                buf.copy_from_slice(&self.data[..8]);
                Some(IndexValue::I64(i64::from_le_bytes(buf)))
            }
            KEY_TAG_U64 => {
                let mut buf = [0_u8; 8];
                buf.copy_from_slice(&self.data[..8]);
                Some(IndexValue::U64(u64::from_le_bytes(buf)))
            }
            KEY_TAG_STRING => {
                let len = self.len as usize;
                Some(IndexValue::String(
                    String::from_utf8_lossy(&self.data[..len]).to_string(),
                ))
            }
            _ => None,
        }
    }

    fn validate_encoding(&self) -> Result<(), ShmIndexError> {
        let valid = match self.tag {
            KEY_TAG_I64 | KEY_TAG_U64 => self.len == 8,
            KEY_TAG_STRING => self
                .data
                .get(..self.len as usize)
                .is_some_and(|bytes| std::str::from_utf8(bytes).is_ok()),
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(ShmIndexError::InvalidEncoding("key"))
        }
    }

    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self.tag == KEY_TAG_SENTINEL && other.tag == KEY_TAG_SENTINEL {
            return Ordering::Equal;
        }
        if self.tag == KEY_TAG_SENTINEL {
            return Ordering::Less;
        }
        if other.tag == KEY_TAG_SENTINEL {
            return Ordering::Greater;
        }
        if self.tag != other.tag {
            return self.tag.cmp(&other.tag);
        }

        match self.tag {
            KEY_TAG_I64 => read_i64_le(self.data.as_ptr()).cmp(&read_i64_le(other.data.as_ptr())),
            KEY_TAG_U64 => read_u64_le(self.data.as_ptr()).cmp(&read_u64_le(other.data.as_ptr())),
            KEY_TAG_STRING => {
                let lhs_len = self.len as usize;
                let rhs_len = other.len as usize;
                self.data[..lhs_len].cmp(&other.data[..rhs_len])
            }
            _ => Ordering::Equal,
        }
    }
}

impl ShmSkipKey for EncodedKey {
    #[inline]
    fn sentinel() -> Self {
        EncodedKey::sentinel()
    }

    #[inline]
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

#[inline]
fn read_u64_le(ptr: *const u8) -> u64 {
    // SAFETY:
    // `ptr` points to at least 8 bytes inside EncodedKey::data.
    let raw = unsafe { std::ptr::read_unaligned(ptr.cast::<u64>()) };
    u64::from_le(raw)
}

#[inline]
fn read_i64_le(ptr: *const u8) -> i64 {
    // SAFETY:
    // `ptr` points to at least 8 bytes inside EncodedKey::data.
    let raw = unsafe { std::ptr::read_unaligned(ptr.cast::<i64>()) };
    i64::from_le(raw)
}

#[derive(Clone)]
pub struct SecondaryIndex<RowId>
where
    RowId: Ord + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    field: &'static str,
    header: RelPtr<SecondaryIndexHeader>,
    skiplist: ShmSkipList<EncodedKey>,
    _marker: PhantomData<RowId>,
}

impl<RowId> SecondaryIndex<RowId>
where
    RowId: Ord + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub fn new(field: &'static str) -> Self {
        let shm = Arc::new(
            ShmArena::new(DEFAULT_INDEX_ARENA_BYTES)
                .expect("failed to allocate shared-memory secondary index arena"),
        );
        Self::new_in_shared(field, shm)
    }

    pub fn new_in_shared(field: &'static str, shm: Arc<ShmArena>) -> Self {
        let skiplist = ShmSkipList::<EncodedKey>::new_in_shared(Arc::clone(&shm))
            .expect("failed to allocate shared-memory skiplist index");
        let header = shm
            .chunked_arena()
            .alloc(SecondaryIndexHeader::new(skiplist.header_offset()))
            .expect("failed to allocate shared-memory index publication metadata");
        Self {
            field,
            header,
            skiplist,
            _marker: PhantomData,
        }
    }

    pub fn from_existing(
        field: &'static str,
        shm: Arc<ShmArena>,
        header_offset: u32,
    ) -> Result<Self, ShmIndexError> {
        let header = RelPtr::<SecondaryIndexHeader>::from_offset(header_offset);
        let metadata = header
            .as_ref(shm.mmap_base())
            .filter(|header| {
                header.magic == INDEX_HEADER_MAGIC && header.version == INDEX_HEADER_VERSION
            })
            .ok_or(ShmIndexError::InvalidHeader(header_offset))?;
        let skiplist =
            ShmSkipList::<EncodedKey>::from_existing(Arc::clone(&shm), metadata.skiplist_offset)?;
        Ok(Self {
            field,
            header,
            skiplist,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn field(&self) -> &'static str {
        self.field
    }

    #[inline]
    pub fn header_offset(&self) -> u32 {
        self.header.load(AtomicOrdering::Acquire)
    }

    #[inline]
    pub fn shared_arena(&self) -> &Arc<ShmArena> {
        self.skiplist.shared_arena()
    }

    fn publication_header(&self) -> Result<&SecondaryIndexHeader, ShmIndexError> {
        self.header
            .as_ref(self.shared_arena().mmap_base())
            .filter(|header| {
                header.magic == INDEX_HEADER_MAGIC && header.version == INDEX_HEADER_VERSION
            })
            .ok_or(ShmIndexError::InvalidHeader(self.header_offset()))
    }

    fn raw_mutation_guard(&self) -> Result<ShmMutexGuard<'_>, ShmIndexError> {
        let header = self.publication_header()?;
        let guard = header.management_lock.lock();
        if header.poisoned.load(AtomicOrdering::Acquire) {
            return Err(ShmIndexError::Poisoned);
        }
        let owner_table_header = header.owner_table_header.load(AtomicOrdering::Acquire);
        if owner_table_header != 0 {
            return Err(ShmIndexError::ManagedMutation { owner_table_header });
        }
        Ok(guard)
    }

    pub(crate) fn transactional_bind(&self, owner_table_header: u32) -> Result<(), ShmIndexError> {
        let header = self.publication_header()?;
        let _guard = header.management_lock.lock();
        if header.poisoned.load(AtomicOrdering::Acquire) {
            return Err(ShmIndexError::Poisoned);
        }
        let actual = header.owner_table_header.load(AtomicOrdering::Acquire);
        if owner_table_header == 0 || (actual != 0 && actual != owner_table_header) {
            return Err(ShmIndexError::OwnerMismatch {
                expected: owner_table_header,
                actual,
            });
        }
        header
            .owner_table_header
            .store(owner_table_header, AtomicOrdering::Release);
        Ok(())
    }

    pub(crate) fn transactional_check_owner(
        &self,
        owner_table_header: u32,
    ) -> Result<(), ShmIndexError> {
        let header = self.publication_header()?;
        if header.poisoned.load(AtomicOrdering::Acquire) {
            return Err(ShmIndexError::Poisoned);
        }
        let actual = header.owner_table_header.load(AtomicOrdering::Acquire);
        if owner_table_header == 0 || actual != owner_table_header {
            return Err(ShmIndexError::OwnerMismatch {
                expected: owner_table_header,
                actual,
            });
        }
        Ok(())
    }

    /// Hash the tagged, canonical bytes explicitly. Rust's randomized hashers
    /// and native-endian encodings must not select process-dependent locks.
    pub(crate) fn transactional_key_bucket(
        &self,
        value: &IndexValue,
    ) -> Result<usize, ShmIndexError> {
        let key = EncodedKey::from_index_value(value)?;
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in std::iter::once(key.tag).chain(key.data[..key.len as usize].iter().copied()) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
        }
        Ok((hash as usize) % INDEX_TX_BUCKETS)
    }

    pub(crate) fn transactional_bucket_ids(
        &self,
        predicate: &IndexCompare,
    ) -> Result<Vec<usize>, ShmIndexError> {
        let buckets = match predicate {
            IndexCompare::Eq(value) => vec![self.transactional_key_bucket(value)?],
            IndexCompare::In(values) => values
                .iter()
                .map(|value| self.transactional_key_bucket(value))
                .collect::<Result<Vec<_>, _>>()?,
            IndexCompare::Gt(value)
            | IndexCompare::Gte(value)
            | IndexCompare::Lt(value)
            | IndexCompare::Lte(value) => {
                EncodedKey::from_index_value(value)?;
                (0..INDEX_TX_BUCKETS).collect()
            }
        };
        #[cfg(feature = "verified-buckets-sort")]
        {
            return aerostore_verified::canonical_buckets_sort(&buckets, INDEX_TX_BUCKETS)
                .map_err(ShmIndexError::InvalidBucket);
        }
        #[cfg(feature = "verified-buckets-bitmap")]
        {
            return aerostore_verified::canonical_buckets_bitmap(&buckets, INDEX_TX_BUCKETS)
                .map_err(ShmIndexError::InvalidBucket);
        }
        #[cfg(not(any(feature = "verified-buckets-sort", feature = "verified-buckets-bitmap")))]
        {
            let mut buckets = buckets;
            buckets.sort_unstable();
            buckets.dedup();
            Ok(buckets)
        }
    }

    pub(crate) fn transactional_try_lock_bucket(
        &self,
        bucket: usize,
    ) -> Result<Option<ShmMutexGuard<'_>>, ShmIndexError> {
        let header = self.publication_header()?;
        if header.poisoned.load(AtomicOrdering::Acquire) {
            return Err(ShmIndexError::Poisoned);
        }
        let bucket = header
            .buckets
            .get(bucket)
            .ok_or(ShmIndexError::InvalidBucket(bucket))?;
        Ok(bucket.lock.try_lock())
    }

    pub(crate) fn transactional_stamp(&self, bucket: usize) -> Result<u64, ShmIndexError> {
        let header = self.publication_header()?;
        if header.poisoned.load(AtomicOrdering::Acquire) {
            return Err(ShmIndexError::Poisoned);
        }
        Ok(header
            .buckets
            .get(bucket)
            .ok_or(ShmIndexError::InvalidBucket(bucket))?
            .stamp
            .load(AtomicOrdering::Acquire))
    }

    /// The caller holds this bucket until every affected row and index is
    /// published and the committing transaction has left the ProcArray.
    pub(crate) fn transactional_publish_stamp(
        &self,
        bucket: usize,
        stamp: u64,
    ) -> Result<(), ShmIndexError> {
        self.publication_header()?
            .buckets
            .get(bucket)
            .ok_or(ShmIndexError::InvalidBucket(bucket))?
            .stamp
            .store(stamp, AtomicOrdering::Release);
        Ok(())
    }

    pub(crate) fn transactional_poison(&self) {
        if let Ok(header) = self.publication_header() {
            header.poisoned.store(true, AtomicOrdering::Release);
        }
    }

    pub(crate) fn transactional_prevalidate(
        &self,
        key: &IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        EncodedKey::from_index_value(key)?;
        Self::encode_row_id(row_id)?;
        Ok(())
    }

    pub(crate) fn transactional_raw_lookup(
        &self,
        predicate: &IndexCompare,
    ) -> Result<Vec<RowId>, ShmIndexError> {
        self.try_lookup(predicate)
    }

    pub(crate) fn transactional_insert(
        &self,
        indexed_value: IndexValue,
        row_id: RowId,
    ) -> Result<(), ShmIndexError> {
        let key = EncodedKey::from_index_value(&indexed_value)?;
        let (payload_len, payload) = Self::encode_row_id(&row_id)?;
        self.try_insert_encoded(key, payload_len, &payload[..payload_len as usize])
    }

    pub(crate) fn transactional_remove(
        &self,
        indexed_value: &IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        let key = EncodedKey::from_index_value(indexed_value)?;
        let (payload_len, payload) = Self::encode_row_id(row_id)?;
        self.try_remove_encoded(&key, payload_len, &payload[..payload_len as usize])
    }

    pub fn insert(&self, indexed_value: IndexValue, row_id: RowId) {
        let _ = self.try_insert(indexed_value, row_id);
    }

    pub fn try_insert(
        &self,
        indexed_value: IndexValue,
        row_id: RowId,
    ) -> Result<(), ShmIndexError> {
        let _guard = self.raw_mutation_guard()?;
        self.transactional_insert(indexed_value, row_id)
    }

    pub fn remove(&self, indexed_value: &IndexValue, row_id: &RowId) {
        let _ = self.try_remove(indexed_value, row_id);
    }

    pub fn try_remove(
        &self,
        indexed_value: &IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        let _guard = self.raw_mutation_guard()?;
        self.transactional_remove(indexed_value, row_id)
    }

    pub fn try_move_payload(
        &self,
        old_indexed_value: &IndexValue,
        new_indexed_value: IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        let _guard = self.raw_mutation_guard()?;
        let old_key = EncodedKey::from_index_value(old_indexed_value)?;
        let new_key = EncodedKey::from_index_value(&new_indexed_value)?;

        let (payload_len, payload) = Self::encode_row_id(row_id)?;
        let payload = &payload[..payload_len as usize];
        // The skiplist handles missing sources, destination publication and
        // source removal under one mutation guard. Never split a move into two
        // public operations: that loses both isolation and allocation rollback.
        self.try_move_relink_fast(&old_key, new_key, payload_len, payload)?;
        Ok(())
    }

    pub fn lookup(&self, predicate: &IndexCompare) -> Vec<RowId> {
        self.try_lookup(predicate).unwrap_or_default()
    }

    pub fn lookup_with_limit(&self, predicate: &IndexCompare, limit: usize) -> Vec<RowId> {
        self.try_lookup_with_limit(predicate, limit)
            .unwrap_or_default()
    }

    /// Materialize matching row IDs without hiding invalid keys, payloads, or
    /// structural scan errors. Results are sorted and deduplicated.
    pub fn try_lookup(&self, predicate: &IndexCompare) -> Result<Vec<RowId>, ShmIndexError> {
        self.try_lookup_with_limit(predicate, usize::MAX)
    }

    /// Fallible counterpart of `lookup_with_limit`.
    ///
    /// Range scans retain the existing bounded scan budget before sorting and
    /// deduplication. `In` forms the complete union before applying the limit so
    /// overlapping postings do not consume the result budget. A zero limit is
    /// a no-op. Multiple `In` lookups do not form a transactional snapshot.
    pub fn try_lookup_with_limit(
        &self,
        predicate: &IndexCompare,
        limit: usize,
    ) -> Result<Vec<RowId>, ShmIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let scan_limit = if limit == usize::MAX {
            usize::MAX
        } else {
            limit.saturating_mul(4).max(limit)
        };
        match predicate {
            IndexCompare::Eq(v) => {
                let key = EncodedKey::from_index_value(v)?;
                let mut invalid = None;
                self.skiplist.scan_payloads_bounded(
                    Some((&key, ScanBound::Inclusive)),
                    Some((&key, ScanBound::Inclusive)),
                    |stored_key, _, payload| {
                        if let Err(err) = stored_key.validate_encoding() {
                            invalid = Some(err);
                            return;
                        }
                        match Self::decode_row_id(payload) {
                            Some(row_id) if out.len() < scan_limit => out.push(row_id),
                            Some(_) => {}
                            None => invalid = Some(ShmIndexError::InvalidEncoding("posting")),
                        }
                    },
                )?;
                if let Some(err) = invalid {
                    return Err(err);
                }
            }
            IndexCompare::Gt(v) => {
                out = self.try_scan_rows_with_limit(
                    Some((v, ScanBound::Exclusive)),
                    None,
                    scan_limit,
                )?;
            }
            IndexCompare::Gte(v) => {
                out = self.try_scan_rows_with_limit(
                    Some((v, ScanBound::Inclusive)),
                    None,
                    scan_limit,
                )?;
            }
            IndexCompare::Lt(v) => {
                out = self.try_scan_rows_with_limit(
                    None,
                    Some((v, ScanBound::Exclusive)),
                    scan_limit,
                )?;
            }
            IndexCompare::Lte(v) => {
                out = self.try_scan_rows_with_limit(
                    None,
                    Some((v, ScanBound::Inclusive)),
                    scan_limit,
                )?;
            }
            IndexCompare::In(values) => {
                // Limiting individual branches before union can spend capacity
                // on duplicate rows and skip later values (including errors).
                for value in values {
                    out.extend(self.try_lookup(&IndexCompare::Eq(value.clone()))?);
                }
            }
        }

        if out.len() <= 1 {
            return Ok(out);
        }

        out.sort_unstable();
        out.dedup();
        if out.len() > limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    pub fn lookup_posting_count(&self, indexed_value: &IndexValue) -> usize {
        let Ok(key) = EncodedKey::from_index_value(indexed_value) else {
            return 0;
        };
        self.skiplist.count_payloads(&key).unwrap_or(0)
    }

    pub fn lookup_count_with_limit(&self, predicate: &IndexCompare, limit: usize) -> usize {
        self.try_lookup_count_with_limit(predicate, limit)
            .unwrap_or(0)
    }

    /// Count postings without hiding invalid bounds or structural scan errors.
    pub fn try_lookup_count_with_limit(
        &self,
        predicate: &IndexCompare,
        limit: usize,
    ) -> Result<usize, ShmIndexError> {
        if limit == 0 {
            return Ok(0);
        }

        match predicate {
            IndexCompare::Eq(v) => {
                let key = EncodedKey::from_index_value(v)?;
                Ok(self.skiplist.count_payloads(&key)?.min(limit))
            }
            IndexCompare::Gt(v) => {
                self.try_scan_count_with_limit(Some((v, ScanBound::Exclusive)), None, limit)
            }
            IndexCompare::Gte(v) => {
                self.try_scan_count_with_limit(Some((v, ScanBound::Inclusive)), None, limit)
            }
            IndexCompare::Lt(v) => {
                self.try_scan_count_with_limit(None, Some((v, ScanBound::Exclusive)), limit)
            }
            IndexCompare::Lte(v) => {
                self.try_scan_count_with_limit(None, Some((v, ScanBound::Inclusive)), limit)
            }
            IndexCompare::In(values) => {
                let mut total = 0_usize;
                for value in values {
                    if total >= limit {
                        break;
                    }
                    let remaining = limit.saturating_sub(total);
                    total = total.saturating_add(self.try_lookup_count_with_limit(
                        &IndexCompare::Eq(value.clone()),
                        remaining,
                    )?);
                }
                Ok(total.min(limit))
            }
        }
    }

    #[inline]
    pub fn lookup_u64_posting_count(&self, indexed_value: u64) -> usize {
        let key = EncodedKey::from_u64(indexed_value);
        self.skiplist.count_payloads(&key).unwrap_or(0)
    }

    pub fn traverse(&self) -> Vec<(IndexValue, Vec<RowId>)> {
        let mut out: BTreeMap<IndexValue, BTreeSet<RowId>> = BTreeMap::new();
        let _ = self.skiplist.scan_payloads(
            |_| true,
            |key, _, payload| {
                if let (Some(value), Some(row_id)) =
                    (key.as_index_value(), Self::decode_row_id(payload))
                {
                    out.entry(value).or_default().insert(row_id);
                }
            },
        );

        out.into_iter()
            .map(|(k, rows)| (k, rows.into_iter().collect()))
            .collect()
    }

    /// Inspect every visible posting in physical scan order, preserving duplicates.
    ///
    /// Unlike `traverse`, this is suitable for quiescent integrity checks: it does
    /// not sort, deduplicate, or silently discard malformed keys and payloads.
    /// It is not a transactionally consistent snapshot of concurrent mutations.
    pub fn try_entries(&self) -> Result<Vec<(IndexValue, RowId)>, ShmIndexError> {
        let mut out = Vec::new();
        let mut invalid = None;
        self.skiplist
            .scan_payloads_bounded(None, None, |key, _, payload| {
                let valid_key = match key.tag {
                    KEY_TAG_I64 | KEY_TAG_U64 => key.len == 8,
                    KEY_TAG_STRING => (key.len as usize) <= KEY_INLINE_BYTES,
                    _ => false,
                };
                if !valid_key {
                    invalid = Some(ShmIndexError::InvalidEncoding("key"));
                    return;
                }
                match (key.as_index_value(), Self::decode_row_id(payload)) {
                    (Some(value), Some(row_id)) => out.push((value, row_id)),
                    _ => invalid = Some(ShmIndexError::InvalidEncoding("posting")),
                }
            })?;
        match invalid {
            Some(err) => Err(err),
            None => Ok(out),
        }
    }

    /// Account for every structural index allocation under the shared index lock.
    /// Includes the sentinel and physical tower capacity; payload spill storage
    /// and table versions are outside this audit.
    pub fn audit_allocations(
        &self,
    ) -> Result<crate::shm_skiplist::ShmSkipAllocationAudit, ShmIndexError> {
        self.skiplist.audit_allocations().map_err(Into::into)
    }

    pub fn collect_garbage_once(&self, max_nodes: usize) -> usize {
        self.skiplist.collect_garbage_once(max_nodes)
    }

    pub fn spawn_gc_daemon(&self, interval: Duration) -> Result<ShmIndexGcDaemon, ShmIndexError> {
        let daemon = self.skiplist.spawn_gc_daemon(interval)?;
        Ok(ShmIndexGcDaemon { inner: daemon })
    }

    #[inline]
    pub fn retired_nodes(&self) -> u64 {
        self.skiplist.retired_nodes()
    }

    #[inline]
    pub fn reclaimed_nodes(&self) -> u64 {
        self.skiplist.reclaimed_nodes()
    }

    #[inline]
    pub fn distinct_key_count(&self) -> usize {
        self.skiplist.distinct_key_count()
    }

    #[inline]
    pub fn mutation_telemetry(&self) -> IndexMutationTelemetry {
        self.skiplist.mutation_telemetry().into()
    }

    fn encode_row_id(row_id: &RowId) -> Result<(u16, [u8; ROWID_INLINE_BYTES]), ShmIndexError> {
        let encoded = bincode::serialize(row_id).map_err(|_| ShmIndexError::RowIdTooLarge {
            len: ROWID_INLINE_BYTES + 1,
            max: ROWID_INLINE_BYTES,
        })?;
        if encoded.len() > ROWID_INLINE_BYTES {
            return Err(ShmIndexError::RowIdTooLarge {
                len: encoded.len(),
                max: ROWID_INLINE_BYTES,
            });
        }
        let mut out = [0_u8; ROWID_INLINE_BYTES];
        out[..encoded.len()].copy_from_slice(encoded.as_slice());
        Ok((encoded.len() as u16, out))
    }

    fn decode_row_id(bytes: &[u8]) -> Option<RowId> {
        bincode::deserialize::<RowId>(bytes).ok()
    }

    fn try_scan_rows_with_limit(
        &self,
        lower: Option<(&IndexValue, ScanBound)>,
        upper: Option<(&IndexValue, ScanBound)>,
        limit: usize,
    ) -> Result<Vec<RowId>, ShmIndexError> {
        let lower = lower
            .map(|(v, mode)| EncodedKey::from_index_value(v).map(|k| (k, mode)))
            .transpose()?;
        let upper = upper
            .map(|(v, mode)| EncodedKey::from_index_value(v).map(|k| (k, mode)))
            .transpose()?;
        let mut out = Vec::new();
        let mut invalid = None;
        self.skiplist.scan_payloads_bounded_with_limit(
            lower.as_ref().map(|(k, mode)| (k, *mode)),
            upper.as_ref().map(|(k, mode)| (k, *mode)),
            limit,
            |key, _, payload| {
                if let Err(err) = key.validate_encoding() {
                    invalid = Some(err);
                    return;
                }
                match Self::decode_row_id(payload) {
                    Some(row_id) => out.push(row_id),
                    None => invalid = Some(ShmIndexError::InvalidEncoding("posting")),
                }
            },
        )?;
        match invalid {
            Some(err) => Err(err),
            None => Ok(out),
        }
    }

    fn try_scan_count_with_limit(
        &self,
        lower: Option<(&IndexValue, ScanBound)>,
        upper: Option<(&IndexValue, ScanBound)>,
        limit: usize,
    ) -> Result<usize, ShmIndexError> {
        let lower = lower
            .map(|(v, mode)| EncodedKey::from_index_value(v).map(|k| (k, mode)))
            .transpose()?;
        let upper = upper
            .map(|(v, mode)| EncodedKey::from_index_value(v).map(|k| (k, mode)))
            .transpose()?;
        let mut total = 0_usize;
        self.skiplist.scan_payloads_bounded_with_limit(
            lower.as_ref().map(|(k, mode)| (k, *mode)),
            upper.as_ref().map(|(k, mode)| (k, *mode)),
            limit,
            |_, _, _| {
                total = total.saturating_add(1);
            },
        )?;
        Ok(total.min(limit))
    }

    fn try_insert_encoded(
        &self,
        key: EncodedKey,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmIndexError> {
        let mut retry_alloc = 0_u32;
        let mut retry_structural = 0_u32;
        let mut retry_epoch = 0_u32;

        for attempt in 0..=INDEX_INSERT_RETRY_LIMIT {
            match self.skiplist.insert_payload(key, payload_len, payload) {
                Ok(()) => {
                    self.skiplist.record_mutation_telemetry(
                        true,
                        attempt as u32 + 1,
                        retry_alloc,
                        retry_structural,
                        retry_epoch,
                    );
                    return Ok(());
                }
                Err(err) => {
                    let Some(kind) = classify_transient_skiplist_error(&err) else {
                        self.skiplist.record_mutation_telemetry(
                            true,
                            attempt as u32 + 1,
                            retry_alloc,
                            retry_structural,
                            retry_epoch,
                        );
                        return Err(err.into());
                    };
                    match kind {
                        RetryKind::Alloc => {
                            retry_alloc = retry_alloc.saturating_add(1);
                            match maybe_collect_alloc_retry(self, attempt) {
                                RetryAllocPhase::PhaseA => {}
                                RetryAllocPhase::PhaseB => self.skiplist.record_retry_phase_b_hit(),
                                RetryAllocPhase::PhaseC => self.skiplist.record_retry_phase_c_hit(),
                            }
                        }
                        RetryKind::Structural => {
                            retry_structural = retry_structural.saturating_add(1);
                            maybe_collect_structural_retry(self, attempt);
                        }
                        RetryKind::Epoch => {
                            retry_epoch = retry_epoch.saturating_add(1);
                        }
                    }
                    if attempt == INDEX_INSERT_RETRY_LIMIT {
                        self.skiplist.record_mutation_telemetry(
                            true,
                            attempt as u32 + 1,
                            retry_alloc,
                            retry_structural,
                            retry_epoch,
                        );
                        return Err(err.into());
                    }
                    retry_pause_with_kind(attempt, kind);
                }
            }
        }

        unreachable!("insert retry loop must return before exhaustion")
    }

    fn try_remove_encoded(
        &self,
        key: &EncodedKey,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmIndexError> {
        let mut retry_alloc = 0_u32;
        let mut retry_structural = 0_u32;
        let mut retry_epoch = 0_u32;

        for attempt in 0..=INDEX_REMOVE_RETRY_LIMIT {
            match self.skiplist.remove_payload(key, payload_len, payload) {
                Ok(()) => {
                    self.skiplist.record_mutation_telemetry(
                        false,
                        attempt as u32 + 1,
                        retry_alloc,
                        retry_structural,
                        retry_epoch,
                    );
                    return Ok(());
                }
                Err(err) => {
                    let Some(kind) = classify_transient_skiplist_error(&err) else {
                        self.skiplist.record_mutation_telemetry(
                            false,
                            attempt as u32 + 1,
                            retry_alloc,
                            retry_structural,
                            retry_epoch,
                        );
                        return Err(err.into());
                    };
                    match kind {
                        RetryKind::Alloc => {
                            retry_alloc = retry_alloc.saturating_add(1);
                            match maybe_collect_alloc_retry(self, attempt) {
                                RetryAllocPhase::PhaseA => {}
                                RetryAllocPhase::PhaseB => self.skiplist.record_retry_phase_b_hit(),
                                RetryAllocPhase::PhaseC => self.skiplist.record_retry_phase_c_hit(),
                            }
                        }
                        RetryKind::Structural => {
                            retry_structural = retry_structural.saturating_add(1);
                            maybe_collect_structural_retry(self, attempt);
                        }
                        RetryKind::Epoch => {
                            retry_epoch = retry_epoch.saturating_add(1);
                        }
                    }
                    if attempt == INDEX_REMOVE_RETRY_LIMIT {
                        self.skiplist.record_mutation_telemetry(
                            false,
                            attempt as u32 + 1,
                            retry_alloc,
                            retry_structural,
                            retry_epoch,
                        );
                        return Err(err.into());
                    }
                    retry_pause_with_kind(attempt, kind);
                }
            }
        }

        unreachable!("remove retry loop must return before exhaustion")
    }

    fn try_move_relink_fast(
        &self,
        old_key: &EncodedKey,
        new_key: EncodedKey,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<bool, ShmIndexError> {
        let mut retry_alloc = 0_u32;
        let mut retry_structural = 0_u32;
        let mut retry_epoch = 0_u32;
        // A move includes destination insertion. Include its attempts in the
        // insertion counters so sustained churn cannot bypass retry telemetry.
        let record = |attempt, alloc, structural, epoch| {
            self.skiplist.record_mutation_telemetry(
                true,
                attempt as u32 + 1,
                alloc,
                structural,
                epoch,
            );
        };
        for attempt in 0..=INDEX_INSERT_RETRY_LIMIT {
            match self
                .skiplist
                .move_payload_relink(old_key, new_key, payload_len, payload)
            {
                Ok(moved) => {
                    record(attempt, retry_alloc, retry_structural, retry_epoch);
                    return Ok(moved);
                }
                Err(err) => {
                    let Some(kind) = classify_transient_skiplist_error(&err) else {
                        record(attempt, retry_alloc, retry_structural, retry_epoch);
                        return Err(err.into());
                    };
                    match kind {
                        RetryKind::Alloc => {
                            retry_alloc = retry_alloc.saturating_add(1);
                            match maybe_collect_alloc_retry(self, attempt) {
                                RetryAllocPhase::PhaseA => {}
                                RetryAllocPhase::PhaseB => self.skiplist.record_retry_phase_b_hit(),
                                RetryAllocPhase::PhaseC => self.skiplist.record_retry_phase_c_hit(),
                            }
                        }
                        RetryKind::Structural => {
                            retry_structural = retry_structural.saturating_add(1);
                            maybe_collect_structural_retry(self, attempt);
                        }
                        RetryKind::Epoch => {
                            retry_epoch = retry_epoch.saturating_add(1);
                        }
                    }
                    if attempt == INDEX_INSERT_RETRY_LIMIT {
                        record(attempt, retry_alloc, retry_structural, retry_epoch);
                        return Err(err.into());
                    }
                    retry_pause_with_kind(attempt, kind);
                }
            }
        }
        unreachable!("fast move retry loop must return before exhaustion")
    }
}

#[inline]
fn maybe_collect_alloc_retry<RowId>(
    index: &SecondaryIndex<RowId>,
    attempt: usize,
) -> RetryAllocPhase
where
    RowId: Ord + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    if attempt < INDEX_ALLOC_RETRY_PHASE_B_START {
        if attempt >= 32 && (attempt & 0x7) == 0 {
            let _ = index
                .skiplist
                .collect_garbage_once(INDEX_RECLAIM_BATCH_ALLOC_MID / 4);
        }
        return RetryAllocPhase::PhaseA;
    }
    if attempt < INDEX_ALLOC_RETRY_PHASE_C_START {
        if attempt & 0x3 == 0 {
            let _ = index
                .skiplist
                .collect_garbage_once(INDEX_RECLAIM_BATCH_ALLOC_MID);
        }
        if attempt % INDEX_ALLOC_RETRY_FLUSH_PERIOD_B == 0 {
            index.skiplist.flush_local_recycle_caches();
        }
        return RetryAllocPhase::PhaseB;
    }
    if attempt & 0x1 == 0 {
        let _ = index
            .skiplist
            .collect_garbage_once(INDEX_RECLAIM_BATCH_ALLOC_HIGH);
    }
    if attempt % INDEX_ALLOC_RETRY_FLUSH_PERIOD_C == 0 {
        index.skiplist.flush_local_recycle_caches();
    }
    RetryAllocPhase::PhaseC
}

#[inline]
fn maybe_collect_structural_retry<RowId>(index: &SecondaryIndex<RowId>, attempt: usize)
where
    RowId: Ord + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    if attempt >= 32 && attempt & 0x7 == 0 {
        let _ = index
            .skiplist
            .collect_garbage_once(INDEX_RECLAIM_BATCH_STRUCTURAL);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryKind {
    Alloc,
    Structural,
    Epoch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryAllocPhase {
    PhaseA,
    PhaseB,
    PhaseC,
}

#[inline]
fn classify_transient_skiplist_error(err: &ShmSkipListError) -> Option<RetryKind> {
    match err {
        ShmSkipListError::Alloc(_) => Some(RetryKind::Alloc),
        ShmSkipListError::Epoch(_) => Some(RetryKind::Epoch),
        ShmSkipListError::InvalidHeader(_)
        | ShmSkipListError::InvalidNode(_)
        | ShmSkipListError::InvalidPosting(_)
        | ShmSkipListError::InvalidLane { .. } => Some(RetryKind::Structural),
        _ => None,
    }
}

#[inline]
fn retry_pause_with_kind(attempt: usize, kind: RetryKind) {
    match kind {
        RetryKind::Alloc => {
            if attempt < 4 {
                std::hint::spin_loop();
                return;
            }
            if attempt < 16 {
                thread::yield_now();
                return;
            }

            // Allow GC/vacuum daemons to run and replenish recyclable index nodes.
            let shift = ((attempt - 16) / 8).min(4);
            let sleep_us = 100_u64 << shift;
            thread::sleep(Duration::from_micros(sleep_us));
        }
        RetryKind::Structural | RetryKind::Epoch => {
            if attempt < 8 {
                std::hint::spin_loop();
            } else if attempt < 64 {
                thread::yield_now();
            } else {
                // Heavy structural churn benefits from short sleeps to break CAS herd effects.
                let shift = ((attempt - 64) / 32).min(5);
                let sleep_us = 50_u64 << shift;
                thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }
}

pub struct ShmIndexGcDaemon {
    inner: ShmSkipListGcDaemon,
}

impl ShmIndexGcDaemon {
    /// Stop between collection passes, releasing shared locks before exiting.
    pub fn stop(&self) -> Result<(), ShmIndexError> {
        self.inner.stop().map_err(Into::into)
    }

    #[inline]
    pub fn pid(&self) -> i32 {
        self.inner.pid()
    }

    pub fn terminate(&self, signal: i32) -> Result<(), ShmIndexError> {
        self.inner.terminate(signal).map_err(Into::into)
    }

    pub fn join(&self) -> Result<(), ShmIndexError> {
        self.inner.join().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::{SecondaryIndex, ShmIndexError, DEFAULT_INDEX_ARENA_BYTES, ROWID_INLINE_BYTES};
    use crate::index::{IndexCompare, IndexValue};
    use crate::shm::ShmArena;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    #[test]
    fn managed_index_binding_survives_attachment_and_rejects_raw_writes() {
        let shm = Arc::new(ShmArena::new(4 << 20).unwrap());
        let index = SecondaryIndex::<usize>::new_in_shared("key", Arc::clone(&shm));
        index.try_insert(IndexValue::I64(10), 0).unwrap();
        index.transactional_bind(256).unwrap();
        let attached =
            SecondaryIndex::<usize>::from_existing("key", shm, index.header_offset()).unwrap();
        attached.transactional_bind(256).unwrap();
        assert!(matches!(
            attached.transactional_bind(512),
            Err(ShmIndexError::OwnerMismatch { .. })
        ));
        assert!(matches!(
            attached.try_insert(IndexValue::I64(20), 1),
            Err(ShmIndexError::ManagedMutation { .. })
        ));
        assert!(matches!(
            attached.try_remove(&IndexValue::I64(10), &0),
            Err(ShmIndexError::ManagedMutation { .. })
        ));
        assert!(matches!(
            attached.try_move_payload(&IndexValue::I64(10), IndexValue::I64(20), &0),
            Err(ShmIndexError::ManagedMutation { .. })
        ));
        assert_eq!(
            attached.try_entries().unwrap(),
            vec![(IndexValue::I64(10), 0)]
        );
    }

    #[test]
    fn publication_locks_and_stamps_are_shared_with_attached_handles() {
        let shm = Arc::new(ShmArena::new(4 << 20).unwrap());
        let index = SecondaryIndex::<usize>::new_in_shared("key", Arc::clone(&shm));
        let attached =
            SecondaryIndex::<usize>::from_existing("key", shm, index.header_offset()).unwrap();
        index.transactional_bind(256).unwrap();
        let bucket = index
            .transactional_key_bucket(&IndexValue::I64(42))
            .unwrap();
        let guard = index
            .transactional_try_lock_bucket(bucket)
            .unwrap()
            .unwrap();
        assert!(attached
            .transactional_try_lock_bucket(bucket)
            .unwrap()
            .is_none());
        index.transactional_publish_stamp(bucket, 97).unwrap();
        assert_eq!(attached.transactional_stamp(bucket).unwrap(), 97);
        drop(guard);
        assert!(attached
            .transactional_try_lock_bucket(bucket)
            .unwrap()
            .is_some());
        index.transactional_poison();
        assert!(matches!(
            attached.transactional_check_owner(256),
            Err(ShmIndexError::Poisoned)
        ));
        assert!(matches!(
            attached.transactional_stamp(bucket),
            Err(ShmIndexError::Poisoned)
        ));
        // Recovery diagnostics remain available even when transactions are blocked.
        assert!(attached.try_entries().unwrap().is_empty());
    }

    #[test]
    fn predicate_buckets_cover_unions_ranges_and_validate_every_key() {
        let index = SecondaryIndex::<usize>::new("key");
        let first = IndexValue::I64(1);
        let second = IndexValue::I64(2);
        let mut expected = vec![
            index.transactional_key_bucket(&first).unwrap(),
            index.transactional_key_bucket(&second).unwrap(),
        ];
        assert_ne!(expected[0], expected[1]);
        expected.sort_unstable();
        assert_eq!(
            index
                .transactional_bucket_ids(&IndexCompare::In(vec![
                    first.clone(),
                    second,
                    first.clone()
                ]))
                .unwrap(),
            expected
        );
        for predicate in [
            IndexCompare::Lt(first.clone()),
            IndexCompare::Lte(first.clone()),
            IndexCompare::Gt(first.clone()),
            IndexCompare::Gte(first),
        ] {
            assert_eq!(
                index.transactional_bucket_ids(&predicate).unwrap(),
                (0..super::INDEX_TX_BUCKETS).collect::<Vec<_>>()
            );
        }
        let invalid = IndexValue::String("x".repeat(super::KEY_INLINE_BYTES + 1));
        assert!(matches!(
            index.transactional_bucket_ids(&IndexCompare::In(vec![
                IndexValue::I64(1),
                invalid.clone()
            ])),
            Err(ShmIndexError::KeyTooLong { .. })
        ));
        assert!(matches!(
            index.transactional_bucket_ids(&IndexCompare::Lt(invalid)),
            Err(ShmIndexError::KeyTooLong { .. })
        ));
        assert!(index
            .transactional_bucket_ids(&IndexCompare::In(Vec::new()))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn attachment_rejects_skiplist_header_used_as_transactional_header() {
        let shm = Arc::new(ShmArena::new(4 << 20).unwrap());
        let index = SecondaryIndex::<usize>::new_in_shared("key", Arc::clone(&shm));
        assert!(matches!(
            SecondaryIndex::<usize>::from_existing("key", shm, index.skiplist.header_offset()),
            Err(ShmIndexError::InvalidHeader(_))
        ));
    }

    #[test]
    fn attachment_rejects_prior_publication_bucket_layout() {
        let shm = Arc::new(ShmArena::new(4 << 20).unwrap());
        let index = SecondaryIndex::<usize>::new_in_shared("key", Arc::clone(&shm));
        let mut old_header = super::SecondaryIndexHeader::new(index.skiplist.header_offset());
        old_header.version = 1;
        let old = shm.chunked_arena().alloc(old_header).unwrap();
        assert!(matches!(
            SecondaryIndex::<usize>::from_existing(
                "key",
                shm,
                old.load(super::AtomicOrdering::Acquire)
            ),
            Err(ShmIndexError::InvalidHeader(_))
        ));
    }

    #[test]
    fn exhausted_epoch_slots_surface_remove_errors_and_move_retries() {
        let shm = Arc::new(ShmArena::new(4 << 20).unwrap());
        let index = SecondaryIndex::<u32>::new_in_shared("key", Arc::clone(&shm));
        index.try_insert(IndexValue::I64(1), 7).unwrap();
        let registrations: Vec<_> = (0..crate::procarray::PROCARRAY_SLOTS)
            .map(|_| shm.begin_transaction().unwrap())
            .collect();
        assert!(matches!(
            index.try_lookup(&IndexCompare::Eq(IndexValue::I64(1))),
            Err(ShmIndexError::Epoch(_))
        ));
        assert!(matches!(
            index.try_lookup(&IndexCompare::Gte(IndexValue::I64(1))),
            Err(ShmIndexError::Epoch(_))
        ));
        assert!(matches!(
            index.try_remove(&IndexValue::I64(1), &7),
            Err(ShmIndexError::Epoch(_))
        ));
        let before = index.mutation_telemetry();
        assert!(matches!(
            index.try_move_payload(&IndexValue::I64(1), IndexValue::I64(2), &7),
            Err(ShmIndexError::Epoch(_))
        ));
        let after = index.mutation_telemetry();
        assert_eq!(after.insert_ops, before.insert_ops + 1);
        assert_eq!(
            after.retry_epoch - before.retry_epoch,
            (super::INDEX_INSERT_RETRY_LIMIT + 1) as u64
        );
        assert_eq!(
            after.max_insert_attempts,
            (super::INDEX_INSERT_RETRY_LIMIT + 1) as u64
        );
        for registration in registrations {
            shm.end_transaction(registration).unwrap();
        }
        assert_eq!(index.try_entries().unwrap(), vec![(IndexValue::I64(1), 7)]);
        index.try_remove(&IndexValue::I64(1), &7).unwrap();
        assert!(index.try_entries().unwrap().is_empty());
    }

    #[test]
    fn fallible_count_rejects_invalid_bounds_instead_of_widening_the_scan() {
        let index = SecondaryIndex::<u32>::new("key");
        index.try_insert(IndexValue::I64(1), 7).unwrap();
        let invalid = IndexValue::String("x".repeat(super::KEY_INLINE_BYTES + 1));
        assert!(matches!(
            index.try_lookup_count_with_limit(&IndexCompare::Gt(invalid), usize::MAX),
            Err(ShmIndexError::KeyTooLong { .. })
        ));
    }

    #[test]
    fn fallible_lookup_preserves_predicate_ordering_deduplication_and_limits() {
        let index = SecondaryIndex::<u32>::new("key");
        for (key, row) in [(10, 7), (10, 3), (20, 7), (20, 9), (30, 1)] {
            index.try_insert(IndexValue::U64(key), row).unwrap();
        }
        for (predicate, expected) in [
            (IndexCompare::Eq(IndexValue::U64(10)), vec![3, 7]),
            (IndexCompare::Gt(IndexValue::U64(10)), vec![1, 7, 9]),
            (IndexCompare::Gte(IndexValue::U64(10)), vec![1, 3, 7, 9]),
            (IndexCompare::Lt(IndexValue::U64(20)), vec![3, 7]),
            (IndexCompare::Lte(IndexValue::U64(20)), vec![3, 7, 9]),
            (IndexCompare::Eq(IndexValue::U64(99)), vec![]),
            (IndexCompare::In(vec![]), vec![]),
        ] {
            assert_eq!(index.try_lookup(&predicate).unwrap(), expected);
            assert_eq!(
                index.try_lookup_with_limit(&predicate, 2).unwrap(),
                expected.into_iter().take(2).collect::<Vec<_>>()
            );
            assert!(index
                .try_lookup_with_limit(&predicate, 0)
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn fallible_in_lookup_limits_the_complete_union_not_overlapping_branches() {
        let index = SecondaryIndex::<u32>::new("key");
        for (key, row) in [(10, 1), (10, 2), (20, 1), (20, 2), (20, 3), (30, 0)] {
            index.try_insert(IndexValue::U64(key), row).unwrap();
        }
        for keys in [[10, 20, 10, 30], [30, 10, 20, 20]] {
            let predicate = IndexCompare::In(keys.into_iter().map(IndexValue::U64).collect());
            assert_eq!(index.try_lookup(&predicate).unwrap(), vec![0, 1, 2, 3]);
            assert_eq!(
                index.try_lookup_with_limit(&predicate, 3).unwrap(),
                vec![0, 1, 2]
            );
        }
    }

    #[test]
    fn fallible_lookup_rejects_invalid_keys_even_in_later_union_branches() {
        let index = SecondaryIndex::<u32>::new("key");
        index.try_insert(IndexValue::U64(1), 7).unwrap();
        let invalid = IndexValue::String("x".repeat(super::KEY_INLINE_BYTES + 1));
        for predicate in [
            IndexCompare::Eq(invalid.clone()),
            IndexCompare::Gt(invalid.clone()),
            IndexCompare::Gte(invalid.clone()),
            IndexCompare::Lt(invalid.clone()),
            IndexCompare::Lte(invalid.clone()),
            IndexCompare::In(vec![IndexValue::U64(1), invalid]),
        ] {
            assert!(matches!(
                index.try_lookup_with_limit(&predicate, 1),
                Err(ShmIndexError::KeyTooLong { .. })
            ));
            assert!(index.lookup_with_limit(&predicate, 1).is_empty());
        }
    }

    #[test]
    fn fallible_lookup_rejects_corrupt_postings_instead_of_returning_partial_rows() {
        let index = SecondaryIndex::<u32>::new("key");
        index.try_insert(IndexValue::I64(1), 7).unwrap();
        index
            .skiplist
            .insert_payload(super::EncodedKey::from_i64(2), 1, &[0])
            .unwrap();
        for predicate in [
            IndexCompare::Eq(IndexValue::I64(2)),
            IndexCompare::Gte(IndexValue::I64(1)),
            IndexCompare::In(vec![IndexValue::I64(1), IndexValue::I64(2)]),
        ] {
            assert!(matches!(
                index.try_lookup_with_limit(&predicate, 1),
                Err(ShmIndexError::InvalidEncoding("posting"))
            ));
            assert!(index.lookup_with_limit(&predicate, 1).is_empty());
        }
    }

    #[test]
    fn fallible_lookup_rejects_malformed_stored_keys() {
        let index = SecondaryIndex::<u32>::new("key");
        let mut key = super::EncodedKey::from_i64(1);
        key.len = 7;
        let (len, payload) = SecondaryIndex::<u32>::encode_row_id(&7).unwrap();
        index
            .skiplist
            .insert_payload(key, len, &payload[..len as usize])
            .unwrap();
        for predicate in [
            IndexCompare::Eq(IndexValue::I64(1)),
            IndexCompare::Gte(IndexValue::I64(0)),
        ] {
            assert!(matches!(
                index.try_lookup(&predicate),
                Err(ShmIndexError::InvalidEncoding("key"))
            ));
        }
    }

    #[test]
    fn integrity_scan_exposes_stale_postings_and_malformed_payloads() {
        let index = SecondaryIndex::<u32>::new("key");
        index.try_insert(IndexValue::I64(2), 7).unwrap();
        index.try_insert(IndexValue::I64(1), 7).unwrap();
        assert_eq!(
            index.try_entries().unwrap(),
            vec![(IndexValue::I64(1), 7), (IndexValue::I64(2), 7),]
        );
        // This represents a corrupt posting, not a serializable u32 row ID.
        index
            .skiplist
            .insert_payload(super::EncodedKey::from_i64(3), 1, &[0])
            .unwrap();
        assert!(matches!(
            index.try_entries(),
            Err(ShmIndexError::InvalidEncoding("posting"))
        ));
    }

    fn collect_from_model(
        model: &BTreeMap<u64, BTreeSet<u32>>,
        predicate: &IndexCompare,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        match predicate {
            IndexCompare::Eq(IndexValue::U64(v)) => {
                if let Some(rows) = model.get(v) {
                    out.extend(rows.iter().copied());
                }
            }
            IndexCompare::Gt(IndexValue::U64(v)) => {
                for (_, rows) in
                    model.range((std::ops::Bound::Excluded(*v), std::ops::Bound::Unbounded))
                {
                    out.extend(rows.iter().copied());
                }
            }
            IndexCompare::Gte(IndexValue::U64(v)) => {
                for (_, rows) in model.range(*v..) {
                    out.extend(rows.iter().copied());
                }
            }
            IndexCompare::Lt(IndexValue::U64(v)) => {
                for (_, rows) in model.range(..*v) {
                    out.extend(rows.iter().copied());
                }
            }
            IndexCompare::Lte(IndexValue::U64(v)) => {
                for (_, rows) in model.range(..=*v) {
                    out.extend(rows.iter().copied());
                }
            }
            IndexCompare::In(values) => {
                for value in values {
                    if let IndexValue::U64(v) = value {
                        if let Some(rows) = model.get(v) {
                            out.extend(rows.iter().copied());
                        }
                    }
                }
            }
            _ => {}
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    #[test]
    fn lookup_and_remove_match_expected_postings() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("altitude");
        index.insert(IndexValue::U64(30000), 1);
        index.insert(IndexValue::U64(30000), 2);
        index.insert(IndexValue::U64(31000), 3);
        index.insert(IndexValue::U64(32000), 4);

        assert_eq!(index.lookup_posting_count(&IndexValue::U64(30000)), 2);
        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::U64(30000))),
            vec![1, 2]
        );
        assert_eq!(
            index.lookup(&IndexCompare::Gt(IndexValue::U64(30000))),
            vec![3, 4]
        );
        assert_eq!(
            index.lookup(&IndexCompare::Lt(IndexValue::U64(32000))),
            vec![1, 2, 3]
        );
        assert_eq!(
            index.lookup(&IndexCompare::In(vec![
                IndexValue::U64(30000),
                IndexValue::U64(32000),
                IndexValue::U64(30000),
            ])),
            vec![1, 2, 4]
        );

        index.remove(&IndexValue::U64(30000), &1);
        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::U64(30000))),
            vec![2]
        );
        assert_eq!(index.lookup_posting_count(&IndexValue::U64(30000)), 1);
    }

    #[test]
    fn move_payload_rekeys_posting_without_duplication() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        index.insert(IndexValue::I64(100), 42);

        index
            .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &42)
            .expect("move should succeed");

        assert!(index
            .lookup(&IndexCompare::Eq(IndexValue::I64(100)))
            .is_empty());
        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::I64(101))),
            vec![42]
        );
    }

    #[test]
    fn move_payload_same_key_is_noop() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        index.insert(IndexValue::I64(100), 7);

        index
            .try_move_payload(&IndexValue::I64(100), IndexValue::I64(100), &7)
            .expect("same-key move should be a no-op");

        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::I64(100))),
            vec![7]
        );
    }

    #[test]
    fn move_payload_relinks_into_existing_target_key() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        index.insert(IndexValue::I64(100), 42);
        index.insert(IndexValue::I64(101), 7);

        index
            .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &42)
            .expect("move into existing key should succeed");

        assert_eq!(index.lookup_posting_count(&IndexValue::I64(100)), 0);
        assert_eq!(index.lookup_posting_count(&IndexValue::I64(101)), 2);
        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::I64(101))),
            vec![7, 42]
        );
    }

    #[test]
    fn move_payload_avoids_duplicate_when_target_already_contains_row() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        index.insert(IndexValue::I64(100), 42);
        index.insert(IndexValue::I64(101), 42);

        index
            .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &42)
            .expect("move should not duplicate existing target payload");

        assert_eq!(index.lookup_posting_count(&IndexValue::I64(100)), 0);
        assert_eq!(index.lookup_posting_count(&IndexValue::I64(101)), 1);
    }

    #[test]
    fn move_payload_old_missing_still_ensures_target_contains_row() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");

        index
            .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &42)
            .expect("move should upsert target posting when old key is absent");

        assert_eq!(index.lookup_posting_count(&IndexValue::I64(100)), 0);
        assert_eq!(index.lookup_posting_count(&IndexValue::I64(101)), 1);
        assert_eq!(
            index.lookup(&IndexCompare::Eq(IndexValue::I64(101))),
            vec![42]
        );
    }

    #[test]
    fn mutation_telemetry_counts_insert_and_remove_ops() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        let before = index.mutation_telemetry();
        index
            .try_insert(IndexValue::I64(100), 7)
            .expect("insert should succeed");
        index
            .try_remove(&IndexValue::I64(100), &7)
            .expect("remove should succeed");
        let after = index.mutation_telemetry();

        assert_eq!(after.insert_ops.saturating_sub(before.insert_ops), 1);
        assert_eq!(after.remove_ops.saturating_sub(before.remove_ops), 1);
        assert!(after.max_insert_attempts >= 1);
        assert!(after.max_remove_attempts >= 1);
    }

    #[test]
    fn mutation_telemetry_exposes_retry_phase_counters() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        index.skiplist.record_retry_phase_b_hit();
        index.skiplist.record_retry_phase_c_hit();

        let telemetry = index.mutation_telemetry();
        assert!(telemetry.retry_phase_b_hits >= 1);
        assert!(telemetry.retry_phase_c_hits >= 1);
    }

    #[test]
    fn lookup_with_limit_caps_range_results() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        for row_id in 0_u32..10 {
            index.insert(IndexValue::I64(100 + row_id as i64), row_id);
        }

        let rows = index.lookup_with_limit(&IndexCompare::Gt(IndexValue::I64(99)), 3);
        assert_eq!(rows, vec![0, 1, 2]);
    }

    #[test]
    fn lookup_count_with_limit_caps_range_results() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
        for row_id in 0_u32..10 {
            index.insert(IndexValue::I64(100 + row_id as i64), row_id);
        }

        let count = index.lookup_count_with_limit(&IndexCompare::Gt(IndexValue::I64(99)), 4);
        assert_eq!(count, 4);
    }

    #[test]
    fn from_existing_attaches_to_same_shared_skiplist() {
        let shm = Arc::new(ShmArena::new(DEFAULT_INDEX_ARENA_BYTES).expect("alloc shm"));
        let index: SecondaryIndex<u32> = SecondaryIndex::new_in_shared("gs", Arc::clone(&shm));
        index.insert(IndexValue::U64(450), 7);
        index.insert(IndexValue::U64(450), 9);
        let header = index.header_offset();

        let attached =
            SecondaryIndex::<u32>::from_existing("gs", shm, header).expect("attach existing index");
        assert_eq!(
            attached.lookup(&IndexCompare::Eq(IndexValue::U64(450))),
            vec![7, 9]
        );
    }

    #[test]
    fn rejects_oversized_string_keys() {
        let index: SecondaryIndex<u32> = SecondaryIndex::new("flight");
        let huge = "X".repeat(129);
        let err = index
            .try_insert(IndexValue::String(huge.clone()), 1)
            .expect_err("oversized key must fail");
        match err {
            ShmIndexError::KeyTooLong { len, max } => {
                assert_eq!(len, huge.len());
                assert_eq!(max, 128);
            }
            other => panic!("expected KeyTooLong, got {other:?}"),
        }
    }

    #[derive(
        Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
    )]
    struct LargeRowId {
        bytes: Vec<u8>,
    }

    #[test]
    fn rejects_oversized_row_id_payloads() {
        let index: SecondaryIndex<LargeRowId> = SecondaryIndex::new("oversized_rowid");
        let err = index
            .try_insert(
                IndexValue::U64(1),
                LargeRowId {
                    bytes: vec![7_u8; ROWID_INLINE_BYTES + 16],
                },
            )
            .expect_err("oversized row-id must fail");
        match err {
            ShmIndexError::RowIdTooLarge { len, max } => {
                assert!(len > max);
                assert_eq!(max, ROWID_INLINE_BYTES);
            }
            other => panic!("expected RowIdTooLarge, got {other:?}"),
        }
    }

    proptest! {
        #[test]
        fn range_queries_match_btree_model(
            rows in prop::collection::vec((0_u16..500_u16, 0_u16..400_u16), 1..120),
            bound in 0_u16..500_u16,
            in_values in prop::collection::vec(0_u16..500_u16, 1..8),
        ) {
            let index: SecondaryIndex<u32> = SecondaryIndex::new("prop_altitude");
            let mut model: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();

            for (k, row_id) in &rows {
                let key = u64::from(*k);
                let row = u32::from(*row_id);
                index.try_insert(IndexValue::U64(key), row).expect("insert");
                model.entry(key).or_default().insert(row);
            }

            let predicates = vec![
                IndexCompare::Eq(IndexValue::U64(u64::from(bound))),
                IndexCompare::Gt(IndexValue::U64(u64::from(bound))),
                IndexCompare::Gte(IndexValue::U64(u64::from(bound))),
                IndexCompare::Lt(IndexValue::U64(u64::from(bound))),
                IndexCompare::Lte(IndexValue::U64(u64::from(bound))),
                IndexCompare::In(
                    in_values
                        .iter()
                        .map(|v| IndexValue::U64(u64::from(*v)))
                        .collect(),
                ),
            ];

            for predicate in predicates {
                let mut actual = index.lookup(&predicate);
                actual.sort_unstable();
                actual.dedup();

                let expected = collect_from_model(&model, &predicate);
                prop_assert_eq!(actual, expected);
            }
        }
    }
}
