use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::index::{IndexCompare, IndexValue};
use crate::procarray::ProcArrayError;
use crate::shm::ShmArena;
use crate::shm_skiplist::{
    ScanBound, ShmSkipKey, ShmSkipList, ShmSkipListError, ShmSkipListGcDaemon,
    ShmSkipMutationTelemetry, MAX_PAYLOAD_BYTES,
};

const KEY_INLINE_BYTES: usize = 128;
const ROWID_INLINE_BYTES: usize = MAX_PAYLOAD_BYTES;
const DEFAULT_INDEX_ARENA_BYTES: usize = 64 << 20;
const INDEX_INSERT_RETRY_LIMIT: usize = 4096;
const INDEX_REMOVE_RETRY_LIMIT: usize = 1;
const INDEX_RECLAIM_BATCH: usize = 131_072;

const KEY_TAG_I64: u8 = 1;
const KEY_TAG_U64: u8 = 2;
const KEY_TAG_STRING: u8 = 3;
const KEY_TAG_SENTINEL: u8 = 255;

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
        }
    }
}

#[derive(Debug)]
pub enum ShmIndexError {
    InvalidHeader(u32),
    InvalidNode(u32),
    InvalidPosting(u32),
    KeyTooLong { len: usize, max: usize },
    RowIdTooLarge { len: usize, max: usize },
    Alloc(crate::shm::ShmAllocError),
    Epoch(ProcArrayError),
    Fork(std::io::Error),
    Wait(std::io::Error),
    Signal(std::io::Error),
}

impl fmt::Display for ShmIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmIndexError::InvalidHeader(offset) => {
                write!(f, "invalid shared index header offset {}", offset)
            }
            ShmIndexError::InvalidNode(offset) => {
                write!(f, "invalid shared index node offset {}", offset)
            }
            ShmIndexError::InvalidPosting(offset) => {
                write!(f, "invalid shared posting offset {}", offset)
            }
            ShmIndexError::KeyTooLong { len, max } => {
                write!(f, "index key length {} exceeds max {}", len, max)
            }
            ShmIndexError::RowIdTooLarge { len, max } => {
                write!(f, "row-id payload length {} exceeds max {}", len, max)
            }
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
        let skiplist = ShmSkipList::<EncodedKey>::new_in_shared(shm)
            .expect("failed to allocate shared-memory skiplist index");
        Self {
            field,
            skiplist,
            _marker: PhantomData,
        }
    }

    pub fn from_existing(
        field: &'static str,
        shm: Arc<ShmArena>,
        header_offset: u32,
    ) -> Result<Self, ShmIndexError> {
        let skiplist = ShmSkipList::<EncodedKey>::from_existing(shm, header_offset)?;
        Ok(Self {
            field,
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
        self.skiplist.header_offset()
    }

    #[inline]
    pub fn shared_arena(&self) -> &Arc<ShmArena> {
        self.skiplist.shared_arena()
    }

    pub fn insert(&self, indexed_value: IndexValue, row_id: RowId) {
        let _ = self.try_insert(indexed_value, row_id);
    }

    pub fn try_insert(
        &self,
        indexed_value: IndexValue,
        row_id: RowId,
    ) -> Result<(), ShmIndexError> {
        let key = EncodedKey::from_index_value(&indexed_value)?;
        let (payload_len, payload) = Self::encode_row_id(&row_id)?;
        self.try_insert_encoded(key, payload_len, &payload[..payload_len as usize])
    }

    pub fn remove(&self, indexed_value: &IndexValue, row_id: &RowId) {
        let _ = self.try_remove(indexed_value, row_id);
    }

    pub fn try_remove(
        &self,
        indexed_value: &IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        let key = EncodedKey::from_index_value(indexed_value)?;
        let (payload_len, payload) = Self::encode_row_id(row_id)?;
        self.try_remove_encoded(&key, payload_len, &payload[..payload_len as usize])
    }

    pub fn try_move_payload(
        &self,
        old_indexed_value: &IndexValue,
        new_indexed_value: IndexValue,
        row_id: &RowId,
    ) -> Result<(), ShmIndexError> {
        let old_key = EncodedKey::from_index_value(old_indexed_value)?;
        let new_key = EncodedKey::from_index_value(&new_indexed_value)?;
        if old_key.cmp(&new_key) == Ordering::Equal {
            return Ok(());
        }

        let (payload_len, payload) = Self::encode_row_id(row_id)?;
        let payload = &payload[..payload_len as usize];
        if self.try_move_relink_fast(&old_key, new_key, payload_len, payload)? {
            return Ok(());
        }

        self.try_remove_encoded(&old_key, payload_len, payload)?;
        if let Err(err) = self.try_insert_encoded(new_key, payload_len, payload) {
            // Keep strict-sync semantics best-effort: if move fails mid-flight,
            // attempt to restore the previous key posting before surfacing error.
            let _ = self.try_insert_encoded(old_key, payload_len, payload);
            return Err(err);
        }
        Ok(())
    }

    pub fn lookup(&self, predicate: &IndexCompare) -> Vec<RowId> {
        self.lookup_with_limit(predicate, usize::MAX)
    }

    pub fn lookup_with_limit(&self, predicate: &IndexCompare, limit: usize) -> Vec<RowId> {
        if limit == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        let scan_limit = if limit == usize::MAX {
            usize::MAX
        } else {
            limit.saturating_mul(4).max(limit)
        };
        match predicate {
            IndexCompare::Eq(v) => {
                let Ok(key) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let _ = self.skiplist.lookup_payloads(&key, |_, payload| {
                    if let Some(row_id) = Self::decode_row_id(payload) {
                        if out.len() < scan_limit {
                            out.push(row_id);
                        }
                    }
                });
            }
            IndexCompare::Gt(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let _ = self.skiplist.scan_payloads_bounded_with_limit(
                    Some((&bound, ScanBound::Exclusive)),
                    None,
                    scan_limit,
                    |_, _, payload| {
                        if let Some(row_id) = Self::decode_row_id(payload) {
                            out.push(row_id);
                        }
                    },
                );
            }
            IndexCompare::Gte(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let _ = self.skiplist.scan_payloads_bounded_with_limit(
                    Some((&bound, ScanBound::Inclusive)),
                    None,
                    scan_limit,
                    |_, _, payload| {
                        if let Some(row_id) = Self::decode_row_id(payload) {
                            out.push(row_id);
                        }
                    },
                );
            }
            IndexCompare::Lt(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let _ = self.skiplist.scan_payloads_bounded_with_limit(
                    None,
                    Some((&bound, ScanBound::Exclusive)),
                    scan_limit,
                    |_, _, payload| {
                        if let Some(row_id) = Self::decode_row_id(payload) {
                            out.push(row_id);
                        }
                    },
                );
            }
            IndexCompare::Lte(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let _ = self.skiplist.scan_payloads_bounded_with_limit(
                    None,
                    Some((&bound, ScanBound::Inclusive)),
                    scan_limit,
                    |_, _, payload| {
                        if let Some(row_id) = Self::decode_row_id(payload) {
                            out.push(row_id);
                        }
                    },
                );
            }
            IndexCompare::In(values) => {
                for value in values {
                    if out.len() >= limit {
                        break;
                    }
                    let remaining = limit.saturating_sub(out.len());
                    let rows = self.lookup_with_limit(&IndexCompare::Eq(value.clone()), remaining);
                    out.extend(rows);
                }
            }
        }

        if out.len() <= 1 {
            return out;
        }

        out.sort_unstable();
        out.dedup();
        if out.len() > limit {
            out.truncate(limit);
        }
        out
    }

    pub fn lookup_posting_count(&self, indexed_value: &IndexValue) -> usize {
        let Ok(key) = EncodedKey::from_index_value(indexed_value) else {
            return 0;
        };
        self.skiplist.count_payloads(&key).unwrap_or(0)
    }

    pub fn lookup_count_with_limit(&self, predicate: &IndexCompare, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }

        match predicate {
            IndexCompare::Eq(v) => {
                let Ok(key) = EncodedKey::from_index_value(v) else {
                    return 0;
                };
                self.skiplist.count_payloads(&key).unwrap_or(0).min(limit)
            }
            IndexCompare::Gt(v) => {
                self.scan_count_with_limit(Some((v, ScanBound::Exclusive)), None, limit)
            }
            IndexCompare::Gte(v) => {
                self.scan_count_with_limit(Some((v, ScanBound::Inclusive)), None, limit)
            }
            IndexCompare::Lt(v) => {
                self.scan_count_with_limit(None, Some((v, ScanBound::Exclusive)), limit)
            }
            IndexCompare::Lte(v) => {
                self.scan_count_with_limit(None, Some((v, ScanBound::Inclusive)), limit)
            }
            IndexCompare::In(values) => {
                let mut total = 0_usize;
                for value in values {
                    if total >= limit {
                        break;
                    }
                    let remaining = limit.saturating_sub(total);
                    total = total.saturating_add(
                        self.lookup_count_with_limit(&IndexCompare::Eq(value.clone()), remaining),
                    );
                }
                total.min(limit)
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

    fn scan_count_with_limit(
        &self,
        lower: Option<(&IndexValue, ScanBound)>,
        upper: Option<(&IndexValue, ScanBound)>,
        limit: usize,
    ) -> usize {
        let lower =
            lower.and_then(|(v, mode)| EncodedKey::from_index_value(v).ok().map(|k| (k, mode)));
        let upper =
            upper.and_then(|(v, mode)| EncodedKey::from_index_value(v).ok().map(|k| (k, mode)));
        let mut total = 0_usize;
        let _ = self.skiplist.scan_payloads_bounded_with_limit(
            lower.as_ref().map(|(k, mode)| (k, *mode)),
            upper.as_ref().map(|(k, mode)| (k, *mode)),
            limit,
            |_, _, _| {
                total = total.saturating_add(1);
            },
        );
        total.min(limit)
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
                            let _ = self.skiplist.collect_garbage_once(INDEX_RECLAIM_BATCH);
                        }
                        RetryKind::Structural => {
                            retry_structural = retry_structural.saturating_add(1);
                            let _ = self.skiplist.collect_garbage_once(INDEX_RECLAIM_BATCH / 4);
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
                            let _ = self.skiplist.collect_garbage_once(INDEX_RECLAIM_BATCH);
                        }
                        RetryKind::Structural => {
                            retry_structural = retry_structural.saturating_add(1);
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
                        return Ok(());
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
        for attempt in 0..=INDEX_INSERT_RETRY_LIMIT {
            match self
                .skiplist
                .move_payload_relink(old_key, new_key, payload_len, payload)
            {
                Ok(moved) => return Ok(moved),
                Err(err) => {
                    let Some(kind) = classify_transient_skiplist_error(&err) else {
                        return Err(err.into());
                    };
                    match kind {
                        RetryKind::Alloc => {
                            let _ = self.skiplist.collect_garbage_once(INDEX_RECLAIM_BATCH);
                        }
                        RetryKind::Structural => {
                            let _ = self.skiplist.collect_garbage_once(INDEX_RECLAIM_BATCH / 4);
                        }
                        RetryKind::Epoch => {}
                    }
                    if attempt == INDEX_INSERT_RETRY_LIMIT {
                        return Err(err.into());
                    }
                    retry_pause_with_kind(attempt, kind);
                }
            }
        }
        unreachable!("fast move retry loop must return before exhaustion")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryKind {
    Alloc,
    Structural,
    Epoch,
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
            if attempt < 8 {
                std::hint::spin_loop();
                return;
            }
            if attempt < 32 {
                thread::yield_now();
                return;
            }

            // Allow GC/vacuum daemons to run and replenish recyclable index nodes.
            let shift = ((attempt - 32) / 8).min(5);
            let sleep_us = 50_u64 << shift;
            thread::sleep(Duration::from_micros(sleep_us));
        }
        RetryKind::Structural | RetryKind::Epoch => {
            if attempt < 16 {
                std::hint::spin_loop();
            } else if attempt < 128 {
                thread::yield_now();
            } else {
                // Heavy structural churn benefits from short sleeps to break CAS herd effects.
                let shift = ((attempt - 128) / 64).min(4);
                let sleep_us = 25_u64 << shift;
                thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }
}

pub struct ShmIndexGcDaemon {
    inner: ShmSkipListGcDaemon,
}

impl ShmIndexGcDaemon {
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
