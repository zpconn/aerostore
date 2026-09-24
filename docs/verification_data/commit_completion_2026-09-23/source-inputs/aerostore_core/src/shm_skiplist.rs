use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use crate::procarray::{ProcArrayError, ProcArrayRegistration};
use crate::shm::{ArenaClass, RelPtr, ShmAllocError, ShmArena};
use crate::shm_lock::{ShmMutex, ShmMutexGuard};

const NULL_OFFSET: u32 = 0;
pub const MAX_HEIGHT: usize = 32;
pub const MAX_PAYLOAD_BYTES: usize = 192;
const POSTING_INLINE_BYTES: usize = 24;
const POSTING_STORAGE_INLINE: u8 = 0;
const POSTING_STORAGE_SPILL: u8 = 1;
const SPILL_CLASS_NONE: u8 = 0;
const SPILL_CLASS_32: u8 = 1;
const SPILL_CLASS_64: u8 = 2;
const SPILL_CLASS_128: u8 = 3;
const SPILL_CLASS_256: u8 = 4;
const GC_MIN_BATCH: usize = 8_192;
const GC_MAX_BATCH: usize = 131_072;
const GC_MAX_PASSES_PER_WAKE: usize = 16;
const PRESSURE_STATE_NORMAL: u32 = 0;
const PRESSURE_STATE_WARM: u32 = 1;
const PRESSURE_STATE_HOT: u32 = 2;
const GC_ASSIST_BATCH_WARM: usize = 8_192;
const GC_ASSIST_BATCH_HOT: usize = 32_768;
const GC_ASSIST_BATCH_NORMAL: usize = 2_048;
const GC_ASSIST_FAILURE_CADENCE_NORMAL: u64 = 16;
const GC_ASSIST_FAILURE_CADENCE_WARM: u64 = 4;
const GC_ASSIST_FAILURE_CADENCE_HOT: u64 = 1;
const PRESSURE_WINDOW_MIN_FAILURES: u64 = 256;
const PRESSURE_HEALTHY_WINDOWS_REQUIRED: u32 = 3;
const PRESSURE_EFFICIENCY_HOT_FLOOR: f64 = 0.10;
const PRESSURE_EFFICIENCY_WARM_FLOOR: f64 = 0.25;
const PRESSURE_EFFICIENCY_HEALTHY: f64 = 0.60;
const RESERVE_REFILL_NODE_BATCH_WARM: usize = 256;
const RESERVE_REFILL_NODE_BATCH_HOT: usize = 1_024;
const RESERVE_REFILL_POSTING_BATCH_WARM: usize = 512;
const RESERVE_REFILL_POSTING_BATCH_HOT: usize = 2_048;
const RESERVE_REFILL_TOWER_BATCH_PER_LEVEL_WARM: usize = 8;
const RESERVE_REFILL_TOWER_BATCH_PER_LEVEL_HOT: usize = 32;

const NODE_FLAG_MARKED: u32 = 1 << 0;
const NODE_FLAG_FULLY_LINKED: u32 = 1 << 1;
const NODE_FLAG_RETIRED: u32 = 1 << 2;

#[inline]
fn stack_head_offset(packed: u64) -> u32 {
    packed as u32
}

#[inline]
fn stack_head_tag(packed: u64) -> u32 {
    (packed >> 32) as u32
}

#[inline]
fn pack_stack_head(offset: u32, tag: u32) -> u64 {
    ((tag as u64) << 32) | (offset as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanBound {
    Inclusive,
    Exclusive,
}

pub trait ShmSkipKey: Copy + Send + Sync + 'static {
    fn sentinel() -> Self;
    fn cmp_key(&self, other: &Self) -> Ordering;
}

#[derive(Debug)]
pub enum ShmSkipListError {
    AllocationAudit(String),
    InvalidHeader(u32),
    InvalidNode(u32),
    InvalidPosting(u32),
    InvalidLane { node_offset: u32, level: usize },
    PayloadTooLarge { len: usize, max: usize },
    Alloc(ShmAllocError),
    Epoch(ProcArrayError),
    Fork(std::io::Error),
    Wait(std::io::Error),
    Signal(std::io::Error),
}

impl fmt::Display for ShmSkipListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmSkipListError::AllocationAudit(message) => {
                write!(f, "skiplist allocation audit failed: {}", message)
            }
            ShmSkipListError::InvalidHeader(offset) => {
                write!(f, "invalid shared skiplist header offset {}", offset)
            }
            ShmSkipListError::InvalidNode(offset) => {
                write!(f, "invalid shared skiplist node offset {}", offset)
            }
            ShmSkipListError::InvalidPosting(offset) => {
                write!(f, "invalid shared skiplist posting offset {}", offset)
            }
            ShmSkipListError::InvalidLane { node_offset, level } => {
                write!(
                    f,
                    "invalid shared skiplist lane for node {} at level {}",
                    node_offset, level
                )
            }
            ShmSkipListError::PayloadTooLarge { len, max } => {
                write!(f, "posting payload length {} exceeds max {}", len, max)
            }
            ShmSkipListError::Alloc(err) => write!(f, "shared skiplist allocation failed: {}", err),
            ShmSkipListError::Epoch(err) => {
                write!(f, "skiplist ProcArray registration failed: {}", err)
            }
            ShmSkipListError::Fork(err) => write!(f, "fork failed: {}", err),
            ShmSkipListError::Wait(err) => write!(f, "wait failed: {}", err),
            ShmSkipListError::Signal(err) => write!(f, "signal failed: {}", err),
        }
    }
}

impl std::error::Error for ShmSkipListError {}

impl From<ShmAllocError> for ShmSkipListError {
    fn from(value: ShmAllocError) -> Self {
        ShmSkipListError::Alloc(value)
    }
}

impl From<ProcArrayError> for ShmSkipListError {
    fn from(value: ProcArrayError) -> Self {
        ShmSkipListError::Epoch(value)
    }
}

#[repr(C, align(64))]
struct ShmSkipHeader<K: ShmSkipKey> {
    head: RelPtr<ShmSkipNode<K>>,
    // Covers every structural writer, retire queue, and index-local recycle pool.
    mutation_lock: ShmMutex,
    allocated_nodes: AtomicU64,
    allocated_postings: AtomicU64,
    allocated_towers: AtomicU64,
    allocated_tower_lanes: AtomicU64,
    current_height: AtomicU32,
    rng_state: AtomicU64,
    distinct_key_count: AtomicUsize,
    has_tombstones: AtomicU32,
    pressure_state: AtomicU32,
    pressure_to_normal: AtomicU64,
    pressure_to_warm: AtomicU64,
    pressure_to_hot: AtomicU64,
    alloc_failure_events: AtomicU64,
    recycled_towers: [AtomicU64; MAX_HEIGHT],
    recycled_nodes: AtomicU64,
    recycled_postings: AtomicU64,
    recycled_tower_nonempty_mask: AtomicU64,
    reserve_towers: [AtomicU64; MAX_HEIGHT],
    reserve_nodes: AtomicU64,
    reserve_postings: AtomicU64,
    reserve_tower_nonempty_mask: AtomicU64,
    reserve_node_pushes: AtomicU64,
    reserve_node_hits: AtomicU64,
    reserve_node_misses: AtomicU64,
    reserve_posting_pushes: AtomicU64,
    reserve_posting_hits: AtomicU64,
    reserve_posting_misses: AtomicU64,
    reserve_tower_pushes: AtomicU64,
    reserve_tower_hits: AtomicU64,
    reserve_tower_misses: AtomicU64,
    retired_posting_head: AtomicU32,
    retired_posting_tail: AtomicU32,
    retired_postings: AtomicU64,
    reclaimed_postings: AtomicU64,
    retired_head: AtomicU32,
    retired_tail: AtomicU32,
    retired_nodes: AtomicU64,
    reclaimed_nodes: AtomicU64,
    gc_collect_lock: AtomicU32,
    gc_nodes_examined: AtomicU64,
    gc_nodes_requeued: AtomicU64,
    gc_recycle_errors: AtomicU64,
    gc_assist_calls: AtomicU64,
    gc_assist_reclaimed: AtomicU64,
    gc_daemon_cycles: AtomicU64,
    gc_daemon_reclaimed: AtomicU64,
    pressure_window_last_failures: AtomicU64,
    pressure_window_last_reclaimed: AtomicU64,
    pressure_window_consecutive_healthy: AtomicU32,
    retry_insert_ops: AtomicU64,
    retry_remove_ops: AtomicU64,
    retry_loops: AtomicU64,
    retry_alloc: AtomicU64,
    retry_structural: AtomicU64,
    retry_epoch: AtomicU64,
    retry_phase_b_hits: AtomicU64,
    retry_phase_c_hits: AtomicU64,
    retry_max_insert_attempts: AtomicU64,
    retry_max_remove_attempts: AtomicU64,
    gc_daemon_pid: AtomicI32,
    gc_stop_requested: AtomicU32,
}

impl<K: ShmSkipKey> ShmSkipHeader<K> {
    #[inline]
    fn new(head_offset: u32, seed: u64) -> Self {
        Self {
            head: RelPtr::from_offset(head_offset),
            mutation_lock: ShmMutex::new(),
            allocated_nodes: AtomicU64::new(1), // sentinel
            allocated_postings: AtomicU64::new(0),
            allocated_towers: AtomicU64::new(1), // sentinel tower
            allocated_tower_lanes: AtomicU64::new(MAX_HEIGHT as u64),
            current_height: AtomicU32::new(1),
            rng_state: AtomicU64::new(seed.max(1)),
            distinct_key_count: AtomicUsize::new(0),
            has_tombstones: AtomicU32::new(0),
            pressure_state: AtomicU32::new(PRESSURE_STATE_NORMAL),
            pressure_to_normal: AtomicU64::new(0),
            pressure_to_warm: AtomicU64::new(0),
            pressure_to_hot: AtomicU64::new(0),
            alloc_failure_events: AtomicU64::new(0),
            recycled_towers: std::array::from_fn(|_| AtomicU64::new(0)),
            recycled_nodes: AtomicU64::new(0),
            recycled_postings: AtomicU64::new(0),
            recycled_tower_nonempty_mask: AtomicU64::new(0),
            reserve_towers: std::array::from_fn(|_| AtomicU64::new(0)),
            reserve_nodes: AtomicU64::new(0),
            reserve_postings: AtomicU64::new(0),
            reserve_tower_nonempty_mask: AtomicU64::new(0),
            reserve_node_pushes: AtomicU64::new(0),
            reserve_node_hits: AtomicU64::new(0),
            reserve_node_misses: AtomicU64::new(0),
            reserve_posting_pushes: AtomicU64::new(0),
            reserve_posting_hits: AtomicU64::new(0),
            reserve_posting_misses: AtomicU64::new(0),
            reserve_tower_pushes: AtomicU64::new(0),
            reserve_tower_hits: AtomicU64::new(0),
            reserve_tower_misses: AtomicU64::new(0),
            retired_posting_head: AtomicU32::new(0),
            retired_posting_tail: AtomicU32::new(0),
            retired_postings: AtomicU64::new(0),
            reclaimed_postings: AtomicU64::new(0),
            retired_head: AtomicU32::new(0),
            retired_tail: AtomicU32::new(0),
            retired_nodes: AtomicU64::new(0),
            reclaimed_nodes: AtomicU64::new(0),
            gc_collect_lock: AtomicU32::new(0),
            gc_nodes_examined: AtomicU64::new(0),
            gc_nodes_requeued: AtomicU64::new(0),
            gc_recycle_errors: AtomicU64::new(0),
            gc_assist_calls: AtomicU64::new(0),
            gc_assist_reclaimed: AtomicU64::new(0),
            gc_daemon_cycles: AtomicU64::new(0),
            gc_daemon_reclaimed: AtomicU64::new(0),
            pressure_window_last_failures: AtomicU64::new(0),
            pressure_window_last_reclaimed: AtomicU64::new(0),
            pressure_window_consecutive_healthy: AtomicU32::new(0),
            retry_insert_ops: AtomicU64::new(0),
            retry_remove_ops: AtomicU64::new(0),
            retry_loops: AtomicU64::new(0),
            retry_alloc: AtomicU64::new(0),
            retry_structural: AtomicU64::new(0),
            retry_epoch: AtomicU64::new(0),
            retry_phase_b_hits: AtomicU64::new(0),
            retry_phase_c_hits: AtomicU64::new(0),
            retry_max_insert_attempts: AtomicU64::new(0),
            retry_max_remove_attempts: AtomicU64::new(0),
            gc_daemon_pid: AtomicI32::new(0),
            gc_stop_requested: AtomicU32::new(0),
        }
    }
}

#[repr(C)]
struct SkipLane<K: ShmSkipKey> {
    next: RelPtr<ShmSkipNode<K>>,
    marked: AtomicU32,
}

impl<K: ShmSkipKey> SkipLane<K> {
    #[inline]
    fn new(next: u32) -> Self {
        Self {
            next: RelPtr::from_offset(next),
            marked: AtomicU32::new(0),
        }
    }
}

#[repr(C)]
struct ShmSkipNode<K: ShmSkipKey> {
    key: K,
    height: u8,
    tower_capacity: u8,
    _pad: [u8; 2],
    flags: AtomicU32,
    tower_offset: u32,
    postings_head: RelPtr<PostingEntry>,
    live_postings: AtomicU32,
    retire_txid: AtomicU64,
    retire_next: RelPtr<ShmSkipNode<K>>,
}

impl<K: ShmSkipKey> ShmSkipNode<K> {
    #[inline]
    fn new(
        key: K,
        height: u8,
        tower_capacity: u8,
        tower_offset: u32,
        postings_head: u32,
        live_postings: u32,
    ) -> Self {
        debug_assert!(tower_capacity >= height);
        Self {
            key,
            height,
            tower_capacity,
            _pad: [0_u8; 2],
            flags: AtomicU32::new(0),
            tower_offset,
            postings_head: RelPtr::from_offset(postings_head),
            live_postings: AtomicU32::new(live_postings),
            retire_txid: AtomicU64::new(0),
            retire_next: RelPtr::null(),
        }
    }

    #[inline]
    fn with_flags(mut self, flags: u32) -> Self {
        self.flags = AtomicU32::new(flags);
        self
    }
}

#[repr(C)]
struct PostingEntry {
    len: u16,
    storage_kind: u8,
    spill_class: u8,
    deleted: AtomicU32,
    next: RelPtr<PostingEntry>,
    spill_offset: u32,
    inline_payload: [u8; POSTING_INLINE_BYTES],
    retire_txid: AtomicU64,
    retire_next: RelPtr<PostingEntry>,
}

impl PostingEntry {
    #[inline]
    fn new_inline(payload_len: u16, payload: &[u8], next: u32) -> Self {
        let mut inline_payload = [0_u8; POSTING_INLINE_BYTES];
        let len = payload_len as usize;
        inline_payload[..len].copy_from_slice(&payload[..len]);
        Self {
            len: payload_len,
            storage_kind: POSTING_STORAGE_INLINE,
            spill_class: SPILL_CLASS_NONE,
            deleted: AtomicU32::new(0),
            next: RelPtr::from_offset(next),
            spill_offset: NULL_OFFSET,
            inline_payload,
            retire_txid: AtomicU64::new(0),
            retire_next: RelPtr::null(),
        }
    }

    #[inline]
    fn new_spilled(payload_len: u16, spill_class: u8, spill_offset: u32, next: u32) -> Self {
        Self {
            len: payload_len,
            storage_kind: POSTING_STORAGE_SPILL,
            spill_class,
            deleted: AtomicU32::new(0),
            next: RelPtr::from_offset(next),
            spill_offset,
            inline_payload: [0_u8; POSTING_INLINE_BYTES],
            retire_txid: AtomicU64::new(0),
            retire_next: RelPtr::null(),
        }
    }
}

#[derive(Clone)]
pub struct ShmSkipList<K: ShmSkipKey> {
    shm: Arc<ShmArena>,
    header_offset: u32,
    _marker: PhantomData<K>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ShmSkipMutationTelemetry {
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

/// Index-owned structural storage. Towers count slots; tower_lanes counts their
/// physical capacity, including extra lanes retained when a tall tower is reused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShmSkipAllocationCounts {
    pub nodes: u64,
    pub postings: u64,
    pub towers: u64,
    pub tower_lanes: u64,
}

impl ShmSkipAllocationCounts {
    fn plus(self, rhs: Self) -> Self {
        Self {
            nodes: self.nodes + rhs.nodes,
            postings: self.postings + rhs.postings,
            towers: self.towers + rhs.towers,
            tower_lanes: self.tower_lanes + rhs.tower_lanes,
        }
    }
}

/// A locked ownership census of this index, including the sentinel. This covers
/// structural slots, not payload spill blocks, arena padding, or table row versions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShmSkipAllocationAudit {
    pub allocated: ShmSkipAllocationCounts,
    pub reachable: ShmSkipAllocationCounts,
    pub retired: ShmSkipAllocationCounts,
    pub reusable: ShmSkipAllocationCounts,
}

#[derive(Default)]
struct AllocationAuditSeen {
    nodes: HashSet<u32>,
    postings: HashSet<u32>,
    towers: HashSet<u32>,
}

fn audit_claim_offset(
    seen: &mut HashSet<u32>,
    offset: u32,
    kind: &str,
) -> Result<(), ShmSkipListError> {
    if offset == NULL_OFFSET || !seen.insert(offset) {
        return Err(ShmSkipListError::AllocationAudit(format!(
            "duplicate ownership or cycle for {kind} at {offset}"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct TowerAlloc {
    offset: u32,
    capacity: usize,
}

struct ProcArrayEpochGuard<'a> {
    shm: &'a ShmArena,
    registration: Option<ProcArrayRegistration>,
}

impl<'a> ProcArrayEpochGuard<'a> {
    #[inline]
    fn acquire(shm: &'a ShmArena) -> Result<Self, ShmSkipListError> {
        let registration = shm.begin_transaction().map_err(ShmSkipListError::Epoch)?;
        Ok(Self {
            shm,
            registration: Some(registration),
        })
    }
}

impl Drop for ProcArrayEpochGuard<'_> {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            let _ = self.shm.end_transaction(registration);
        }
    }
}

struct GcCollectGuard<'a> {
    lock: &'a AtomicU32,
}

impl Drop for GcCollectGuard<'_> {
    fn drop(&mut self) {
        self.lock.store(0, AtomicOrdering::Release);
    }
}

impl<K: ShmSkipKey> ShmSkipList<K> {
    pub fn new_in_shared(shm: Arc<ShmArena>) -> Result<Self, ShmSkipListError> {
        let lane_bytes = size_of::<SkipLane<K>>()
            .checked_mul(MAX_HEIGHT)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let tower_offset = shm.chunked_arena().alloc_raw_in_class(
            lane_bytes,
            align_of::<SkipLane<K>>().max(align_of::<u64>()),
            ArenaClass::SkipTower,
        )?;
        let base = shm.mmap_base();

        for level in 0..MAX_HEIGHT {
            let ptr = tower_ptr::<K>(base, tower_offset, level, MAX_HEIGHT).ok_or(
                ShmSkipListError::InvalidLane {
                    node_offset: NULL_OFFSET,
                    level,
                },
            )?;
            // SAFETY:
            // `alloc_raw` reserved this region uniquely and `tower_ptr` validates bounds/alignment.
            unsafe { ptr.write(SkipLane::new(NULL_OFFSET)) };
        }

        let head_offset = match shm.chunked_arena().alloc_in_class(
            ShmSkipNode::new(
                K::sentinel(),
                MAX_HEIGHT as u8,
                MAX_HEIGHT as u8,
                tower_offset,
                NULL_OFFSET,
                0,
            )
            .with_flags(NODE_FLAG_FULLY_LINKED),
            ArenaClass::SkipNode,
        ) {
            Ok(ptr) => ptr.load(AtomicOrdering::Acquire),
            Err(err) => {
                shm.chunked_arena().recycle_raw_in_class(
                    tower_offset,
                    lane_bytes,
                    align_of::<SkipLane<K>>().max(align_of::<u64>()),
                    ArenaClass::SkipTower,
                )?;
                return Err(err.into());
            }
        };

        let seed = shm.global_txid().load(AtomicOrdering::Acquire)
            ^ ((head_offset as u64) << 32)
            ^ 0x9E37_79B9_7F4A_7C15;
        let header_offset = match shm
            .chunked_arena()
            .alloc(ShmSkipHeader::<K>::new(head_offset, seed))
        {
            Ok(ptr) => ptr,
            Err(err) => {
                shm.chunked_arena().recycle_raw_in_class(
                    tower_offset,
                    lane_bytes,
                    align_of::<SkipLane<K>>().max(align_of::<u64>()),
                    ArenaClass::SkipTower,
                )?;
                shm.chunked_arena().recycle_raw_in_class(
                    head_offset,
                    size_of::<ShmSkipNode<K>>(),
                    align_of::<ShmSkipNode<K>>(),
                    ArenaClass::SkipNode,
                )?;
                return Err(err.into());
            }
        };

        Ok(Self {
            shm,
            header_offset: header_offset.load(AtomicOrdering::Acquire),
            _marker: PhantomData,
        })
    }

    pub fn from_existing(shm: Arc<ShmArena>, header_offset: u32) -> Result<Self, ShmSkipListError> {
        let out = Self {
            shm,
            header_offset,
            _marker: PhantomData,
        };
        if out.header_ref().is_none() {
            return Err(ShmSkipListError::InvalidHeader(header_offset));
        }
        Ok(out)
    }

    #[inline]
    pub fn shared_arena(&self) -> &Arc<ShmArena> {
        &self.shm
    }

    #[inline]
    pub fn header_offset(&self) -> u32 {
        self.header_offset
    }

    #[inline]
    fn validate_payload_args(payload_len: u16, payload: &[u8]) -> Result<usize, ShmSkipListError> {
        let len = payload_len as usize;
        if len > MAX_PAYLOAD_BYTES || payload.len() < len {
            return Err(ShmSkipListError::PayloadTooLarge {
                len,
                max: MAX_PAYLOAD_BYTES,
            });
        }
        Ok(len)
    }

    #[inline]
    fn spill_spec_for_len(len: usize) -> Option<(ArenaClass, u8, usize)> {
        if len <= POSTING_INLINE_BYTES {
            return None;
        }
        if len <= 32 {
            return Some((ArenaClass::Spill32, SPILL_CLASS_32, 32));
        }
        if len <= 64 {
            return Some((ArenaClass::Spill64, SPILL_CLASS_64, 64));
        }
        if len <= 128 {
            return Some((ArenaClass::Spill128, SPILL_CLASS_128, 128));
        }
        if len <= 256 {
            return Some((ArenaClass::Spill256, SPILL_CLASS_256, 256));
        }
        None
    }

    #[inline]
    fn spill_spec_from_tag(spill_class: u8) -> Option<(ArenaClass, usize)> {
        match spill_class {
            SPILL_CLASS_32 => Some((ArenaClass::Spill32, 32)),
            SPILL_CLASS_64 => Some((ArenaClass::Spill64, 64)),
            SPILL_CLASS_128 => Some((ArenaClass::Spill128, 128)),
            SPILL_CLASS_256 => Some((ArenaClass::Spill256, 256)),
            _ => None,
        }
    }

    fn build_posting_entry(
        &self,
        payload_len: u16,
        payload: &[u8],
        next: u32,
    ) -> Result<PostingEntry, ShmSkipListError> {
        let len = Self::validate_payload_args(payload_len, payload)?;
        if let Some((class, spill_tag, spill_size)) = Self::spill_spec_for_len(len) {
            let spill_offset = self.shm.chunked_arena().alloc_raw_in_class(
                spill_size,
                align_of::<u64>(),
                class,
            )?;
            let spill_ptr = self.spill_ptr(spill_offset, spill_size)?;
            // SAFETY:
            // `alloc_raw_in_class` reserved `spill_size` bytes starting at `spill_offset`.
            unsafe {
                std::ptr::write_bytes(spill_ptr, 0, spill_size);
                std::ptr::copy_nonoverlapping(payload.as_ptr(), spill_ptr, len);
            }
            Ok(PostingEntry::new_spilled(
                payload_len,
                spill_tag,
                spill_offset,
                next,
            ))
        } else {
            Ok(PostingEntry::new_inline(payload_len, payload, next))
        }
    }

    fn recycle_posting_payload_storage(
        &self,
        posting: &PostingEntry,
    ) -> Result<(), ShmSkipListError> {
        if posting.storage_kind != POSTING_STORAGE_SPILL {
            return Ok(());
        }
        let Some((class, spill_size)) = Self::spill_spec_from_tag(posting.spill_class) else {
            return Err(ShmSkipListError::InvalidPosting(posting.spill_offset));
        };
        if posting.spill_offset == NULL_OFFSET {
            return Ok(());
        }
        self.shm.chunked_arena().recycle_raw_in_class(
            posting.spill_offset,
            spill_size,
            align_of::<u64>(),
            class,
        )?;
        Ok(())
    }

    fn spill_ptr(&self, offset: u32, size: usize) -> Result<*mut u8, ShmSkipListError> {
        let start = offset as usize;
        let end = start
            .checked_add(size)
            .ok_or(ShmSkipListError::InvalidPosting(offset))?;
        let base = self.shm.mmap_base();
        if end > base.len() {
            return Err(ShmSkipListError::InvalidPosting(offset));
        }
        Ok(unsafe { base.as_ptr().add(start) })
    }

    fn posting_payload_equals(
        &self,
        post: &PostingEntry,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<bool, ShmSkipListError> {
        if post.len != payload_len {
            return Ok(false);
        }
        let len = payload_len as usize;
        if post.storage_kind == POSTING_STORAGE_INLINE {
            return Ok(post.inline_payload[..len] == payload[..len]);
        }
        let Some((_class, spill_size)) = Self::spill_spec_from_tag(post.spill_class) else {
            return Err(ShmSkipListError::InvalidPosting(post.spill_offset));
        };
        if post.spill_offset == NULL_OFFSET || len > spill_size {
            return Err(ShmSkipListError::InvalidPosting(post.spill_offset));
        }
        let spill_ptr = self.spill_ptr(post.spill_offset, spill_size)?;
        // SAFETY:
        // `spill_ptr` points to at least `len` readable bytes in shared memory.
        let spill = unsafe { std::slice::from_raw_parts(spill_ptr, len) };
        Ok(spill == &payload[..len])
    }

    fn with_posting_payload<F>(
        &self,
        post: &PostingEntry,
        mut visit: F,
    ) -> Result<(), ShmSkipListError>
    where
        F: FnMut(u16, &[u8]),
    {
        let len = post.len as usize;
        if len > MAX_PAYLOAD_BYTES {
            return Err(ShmSkipListError::InvalidPosting(post.spill_offset));
        }
        if post.storage_kind == POSTING_STORAGE_INLINE {
            visit(post.len, &post.inline_payload[..len]);
            return Ok(());
        }
        let Some((_class, spill_size)) = Self::spill_spec_from_tag(post.spill_class) else {
            return Err(ShmSkipListError::InvalidPosting(post.spill_offset));
        };
        if post.spill_offset == NULL_OFFSET || len > spill_size {
            return Err(ShmSkipListError::InvalidPosting(post.spill_offset));
        }
        let spill_ptr = self.spill_ptr(post.spill_offset, spill_size)?;
        // SAFETY:
        // `spill_ptr` points to at least `len` readable bytes in shared memory.
        let spill = unsafe { std::slice::from_raw_parts(spill_ptr, len) };
        visit(post.len, spill);
        Ok(())
    }

    fn lock_mutation(&self) -> Result<ShmMutexGuard<'_>, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        Ok(header.mutation_lock.lock())
    }

    pub fn insert_payload(
        &self,
        key: K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        self.insert_payload_inner(key, payload_len, payload)
    }

    /// Moves a payload under the shared mutation lock. Published keys and posting links
    /// are never repurposed while epoch readers can retain pointers to them. The legacy
    /// method name is retained for callers; storage is copied, then retired for reuse.
    pub fn move_payload_relink(
        &self,
        old_key: &K,
        new_key: K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<bool, ShmSkipListError> {
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        Self::validate_payload_args(payload_len, payload)?;
        // Allocate/upsert the destination even when the source is absent. The
        // complete move stays under one guard; callers must not need an unlocked
        // remove/insert fallback. An OOM leaves any existing source untouched.
        self.insert_payload_inner(new_key, payload_len, payload)?;
        if old_key.cmp_key(&new_key) != Ordering::Equal {
            self.remove_payload_inner(old_key, payload_len, payload)?;
        }
        Ok(true)
    }

    pub fn remove_payload(
        &self,
        key: &K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        self.remove_payload_inner(key, payload_len, payload)
    }

    fn remove_payload_inner(
        &self,
        key: &K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        if Self::validate_payload_args(payload_len, payload).is_err() {
            return Ok(());
        }
        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        let Some(node_offset) = self.find_live_node_offset(key, &mut preds, &mut succs)? else {
            return Ok(());
        };
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        let mut previous: Option<&PostingEntry> = None;
        let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while post_offset != NULL_OFFSET {
            let post = self
                .posting_ref(post_offset)
                .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
            let next = post.next.load(AtomicOrdering::Acquire);
            if post.deleted.load(AtomicOrdering::Acquire) == 0
                && self.posting_payload_equals(post, payload_len, payload)?
            {
                post.deleted.store(1, AtomicOrdering::Release);
                self.mark_tombstone_seen();
                if self.decrement_live_postings(node) == 0 {
                    // The last posting is reclaimed with its node.
                    self.unlink_node(key, node_offset, &mut preds, &mut succs)?;
                } else {
                    // Readers holding `post` may still follow its next pointer. Keep
                    // that pointer and payload immutable until their epochs finish.
                    match previous {
                        Some(previous) => previous.next.store(next, AtomicOrdering::Release),
                        None => node.postings_head.store(next, AtomicOrdering::Release),
                    }
                    self.retire_posting(post_offset)?;
                }
                return Ok(());
            }
            previous = Some(post);
            post_offset = next;
        }
        Ok(())
    }

    fn retire_posting(&self, offset: u32) -> Result<(), ShmSkipListError> {
        let post = self
            .posting_ref(offset)
            .ok_or(ShmSkipListError::InvalidPosting(offset))?;
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        post.retire_txid.store(
            self.shm
                .global_txid()
                .load(AtomicOrdering::Acquire)
                .saturating_sub(1),
            AtomicOrdering::Release,
        );
        post.retire_next.store(NULL_OFFSET, AtomicOrdering::Relaxed);
        let tail = header.retired_posting_tail.load(AtomicOrdering::Relaxed);
        if tail == NULL_OFFSET {
            header
                .retired_posting_head
                .store(offset, AtomicOrdering::Release);
        } else {
            self.posting_ref(tail)
                .ok_or(ShmSkipListError::InvalidPosting(tail))?
                .retire_next
                .store(offset, AtomicOrdering::Release);
        }
        header
            .retired_posting_tail
            .store(offset, AtomicOrdering::Release);
        header
            .retired_postings
            .fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    fn insert_payload_inner(
        &self,
        key: K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let _ = Self::validate_payload_args(payload_len, payload)?;

        if let Some(offset) = self.find_readonly_exact(&key)? {
            let node = self
                .node_ref(offset)
                .ok_or(ShmSkipListError::InvalidNode(offset))?;
            if self.node_contains_live_payload(node, payload_len, payload)? {
                return Ok(());
            }
        }
        let posting_offset = self.alloc_posting_entry(payload_len, payload)?;
        match self.attach_posting_to_key(key, posting_offset, payload_len, payload) {
            Ok(()) => Ok(()),
            Err(err) => {
                // attach only returns an error before publishing ownership.
                self.push_recycled_posting(posting_offset)?;
                Err(err)
            }
        }
    }

    /// The visitor executes under the shared list lock and must not reenter this list.
    pub fn lookup_payloads<F>(&self, key: &K, mut visit: F) -> Result<(), ShmSkipListError>
    where
        F: FnMut(u16, &[u8]),
    {
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        let found = self.find_readonly_exact(key)?;
        let Some(node_offset) = found else {
            return Ok(());
        };
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        let flags = node.flags.load(AtomicOrdering::Acquire);
        if flags & NODE_FLAG_MARKED != 0 || flags & NODE_FLAG_FULLY_LINKED == 0 {
            return Ok(());
        }
        if node.key.cmp_key(key) != Ordering::Equal {
            return Ok(());
        }
        if node.live_postings.load(AtomicOrdering::Acquire) == 0 {
            return Ok(());
        }

        let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while post_offset != NULL_OFFSET {
            let post = self
                .posting_ref(post_offset)
                .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
            if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                self.with_posting_payload(post, |len, payload| visit(len, payload))?;
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }

        Ok(())
    }

    pub fn count_payloads(&self, key: &K) -> Result<usize, ShmSkipListError> {
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        let Some(node_offset) = self.find_readonly_exact(key)? else {
            return Ok(0);
        };
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        let flags = node.flags.load(AtomicOrdering::Acquire);
        if flags & NODE_FLAG_MARKED != 0 || flags & NODE_FLAG_FULLY_LINKED == 0 {
            return Ok(0);
        }
        if node.key.cmp_key(key) != Ordering::Equal {
            return Ok(0);
        }
        Ok(node.live_postings.load(AtomicOrdering::Acquire) as usize)
    }

    /// The visitor executes under the shared list lock and must not reenter this list.
    pub fn scan_payloads<P, F>(&self, predicate: P, mut visit: F) -> Result<(), ShmSkipListError>
    where
        P: Fn(&K) -> bool,
        F: FnMut(&K, u16, &[u8]),
    {
        self.scan_payloads_bounded(None, None, |key, len, payload| {
            if predicate(key) {
                visit(key, len, payload);
            }
        })
    }

    /// The visitor executes under the shared list lock and must not reenter this list.
    pub fn scan_payloads_bounded<F>(
        &self,
        lower: Option<(&K, ScanBound)>,
        upper: Option<(&K, ScanBound)>,
        mut visit: F,
    ) -> Result<(), ShmSkipListError>
    where
        F: FnMut(&K, u16, &[u8]),
    {
        self.scan_payloads_bounded_with_limit(lower, upper, usize::MAX, |key, len, payload| {
            visit(key, len, payload);
        })
    }

    /// The visitor executes under the shared list lock and must not reenter this list.
    pub fn scan_payloads_bounded_with_limit<F>(
        &self,
        lower: Option<(&K, ScanBound)>,
        upper: Option<(&K, ScanBound)>,
        limit: usize,
        mut visit: F,
    ) -> Result<(), ShmSkipListError>
    where
        F: FnMut(&K, u16, &[u8]),
    {
        if limit == 0 {
            return Ok(());
        }
        let _mutation_guard = self.lock_mutation()?;
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        let mut emitted = 0_usize;
        let mut curr_offset = match lower {
            Some((bound, mode)) => {
                let mut start = self.seek_ge(bound)?.load(AtomicOrdering::Acquire);
                if start != NULL_OFFSET && matches!(mode, ScanBound::Exclusive) {
                    let node = self
                        .node_ref(start)
                        .ok_or(ShmSkipListError::InvalidNode(start))?;
                    if node.key.cmp_key(bound) == Ordering::Equal {
                        start = self.node_next_offset(start, 0)?;
                    }
                }
                start
            }
            None => {
                let header = self
                    .header_ref()
                    .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
                let head_offset = header.head.load(AtomicOrdering::Acquire);
                let head_lane = self.lane_ref_by_offset(head_offset, 0)?;
                head_lane.next.load(AtomicOrdering::Acquire)
            }
        };

        while curr_offset != NULL_OFFSET {
            let node = self
                .node_ref(curr_offset)
                .ok_or(ShmSkipListError::InvalidNode(curr_offset))?;
            let next = self.node_next_offset(curr_offset, 0)?;
            if let Some((bound, mode)) = upper {
                let cmp = node.key.cmp_key(bound);
                let should_break = match mode {
                    ScanBound::Inclusive => cmp == Ordering::Greater,
                    ScanBound::Exclusive => matches!(cmp, Ordering::Equal | Ordering::Greater),
                };
                if should_break {
                    break;
                }
            }
            let flags = node.flags.load(AtomicOrdering::Acquire);
            if flags & NODE_FLAG_MARKED == 0 && flags & NODE_FLAG_FULLY_LINKED != 0 {
                if let Some((bound, mode)) = lower {
                    let cmp = node.key.cmp_key(bound);
                    let below_lower = match mode {
                        ScanBound::Inclusive => cmp == Ordering::Less,
                        ScanBound::Exclusive => !matches!(cmp, Ordering::Greater),
                    };
                    if below_lower {
                        curr_offset = next;
                        continue;
                    }
                }
                let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
                while post_offset != NULL_OFFSET {
                    let post = self
                        .posting_ref(post_offset)
                        .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
                    if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                        self.with_posting_payload(post, |len, payload| {
                            visit(&node.key, len, payload)
                        })?;
                        emitted = emitted.saturating_add(1);
                        if emitted >= limit {
                            return Ok(());
                        }
                    }
                    post_offset = post.next.load(AtomicOrdering::Acquire);
                }
            }
            curr_offset = next;
        }

        Ok(())
    }

    fn seek_ge(&self, bound: &K) -> Result<RelPtr<ShmSkipNode<K>>, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        if header.has_tombstones.load(AtomicOrdering::Acquire) == 0 {
            return self.seek_ge_fast(header, bound);
        }
        self.seek_ge_with_tombstones(header, bound)
    }

    fn seek_ge_fast(
        &self,
        header: &ShmSkipHeader<K>,
        bound: &K,
    ) -> Result<RelPtr<ShmSkipNode<K>>, ShmSkipListError> {
        let base = self.shm.mmap_base().as_ptr();
        let head_offset = header.head.load(AtomicOrdering::Acquire);
        let mut pred_offset = head_offset;

        let top = header
            .current_height
            .load(AtomicOrdering::Acquire)
            .clamp(1, MAX_HEIGHT as u32) as usize
            - 1;
        for level in (0..=top).rev() {
            let mut curr_offset = unsafe {
                self.lane_ref_by_offset_unchecked(base, pred_offset, level)
                    .next
                    .load(AtomicOrdering::Acquire)
            };
            loop {
                if curr_offset == NULL_OFFSET {
                    break;
                }
                let curr = unsafe { self.node_ref_unchecked(base, curr_offset) };
                let curr_lane = unsafe { self.lane_ref_node_unchecked(base, curr, level) };
                let curr_next = curr_lane.next.load(AtomicOrdering::Acquire);

                if curr.key.cmp_key(bound) == Ordering::Less {
                    pred_offset = curr_offset;
                    curr_offset = curr_next;
                } else {
                    break;
                }
            }
        }

        let candidate = unsafe {
            self.lane_ref_by_offset_unchecked(base, pred_offset, 0)
                .next
                .load(AtomicOrdering::Acquire)
        };
        Ok(RelPtr::from_offset(candidate))
    }

    fn seek_ge_with_tombstones(
        &self,
        header: &ShmSkipHeader<K>,
        bound: &K,
    ) -> Result<RelPtr<ShmSkipNode<K>>, ShmSkipListError> {
        let head_offset = header.head.load(AtomicOrdering::Acquire);
        let mut pred_offset = head_offset;

        let top = header
            .current_height
            .load(AtomicOrdering::Acquire)
            .clamp(1, MAX_HEIGHT as u32) as usize
            - 1;
        for level in (0..=top).rev() {
            let mut curr_offset = self
                .lane_ref_by_offset(pred_offset, level)?
                .next
                .load(AtomicOrdering::Acquire);
            loop {
                if curr_offset == NULL_OFFSET {
                    break;
                }
                let curr = self
                    .node_ref(curr_offset)
                    .ok_or(ShmSkipListError::InvalidNode(curr_offset))?;
                let curr_lane = self.lane_ref(curr_offset, curr, level)?;
                let curr_next = curr_lane.next.load(AtomicOrdering::Acquire);

                let flags = curr.flags.load(AtomicOrdering::Acquire);
                if flags & NODE_FLAG_MARKED != 0 {
                    curr_offset = curr_next;
                    continue;
                }
                if curr_lane.marked.load(AtomicOrdering::Acquire) != 0 {
                    curr_offset = curr_next;
                    continue;
                }

                if curr.key.cmp_key(bound) == Ordering::Less {
                    pred_offset = curr_offset;
                    curr_offset = curr_next;
                } else {
                    break;
                }
            }
        }

        let candidate = self
            .lane_ref_by_offset(pred_offset, 0)?
            .next
            .load(AtomicOrdering::Acquire);
        Ok(RelPtr::from_offset(candidate))
    }

    /// Verify allocated = reachable + retired + reusable for this index's nodes,
    /// postings, and physical tower storage. The list lock makes the census
    /// quiescent; no operation can temporarily own an unclassified slot.
    pub fn audit_allocations(&self) -> Result<ShmSkipAllocationAudit, ShmSkipListError> {
        let _guard = self.lock_mutation()?;
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let mut audit = ShmSkipAllocationAudit {
            allocated: ShmSkipAllocationCounts {
                nodes: header.allocated_nodes.load(AtomicOrdering::Relaxed),
                postings: header.allocated_postings.load(AtomicOrdering::Relaxed),
                towers: header.allocated_towers.load(AtomicOrdering::Relaxed),
                tower_lanes: header.allocated_tower_lanes.load(AtomicOrdering::Relaxed),
            },
            ..Default::default()
        };
        let mut seen = AllocationAuditSeen::default();
        let head = header.head.load(AtomicOrdering::Acquire);
        let mut cursor = head;
        let mut previous_key: Option<K> = None;
        while cursor != NULL_OFFSET {
            let node = self.audit_owned_node(cursor, &mut audit.reachable, &mut seen)?;
            if node.flags.load(AtomicOrdering::Acquire)
                & (NODE_FLAG_MARKED | NODE_FLAG_FULLY_LINKED)
                != NODE_FLAG_FULLY_LINKED
            {
                return Err(ShmSkipListError::AllocationAudit(format!(
                    "nonlive node {cursor} remains reachable"
                )));
            }
            if cursor != head {
                if node.live_postings.load(AtomicOrdering::Acquire) == 0 {
                    return Err(ShmSkipListError::AllocationAudit(format!(
                        "empty node {cursor} remains reachable"
                    )));
                }
                if previous_key.is_some_and(|key| key.cmp_key(&node.key) != Ordering::Less) {
                    return Err(ShmSkipListError::AllocationAudit(
                        "level zero keys are not strictly ordered".into(),
                    ));
                }
                previous_key = Some(node.key);
            }
            cursor = self.node_next_offset(cursor, 0)?;
        }
        // All upper links must refer to live level-zero nodes; validate every lane,
        // including levels above current_height so stale pointers cannot hide there.
        for level in 1..MAX_HEIGHT {
            let mut lane_seen = HashSet::new();
            let mut cursor = self.node_next_offset(head, level)?;
            let mut previous_key: Option<K> = None;
            while cursor != NULL_OFFSET {
                audit_claim_offset(&mut lane_seen, cursor, "upper-lane node")?;
                if !seen.nodes.contains(&cursor) {
                    return Err(ShmSkipListError::AllocationAudit(format!(
                        "upper lane {level} links detached node {cursor}"
                    )));
                }
                let node = self
                    .node_ref(cursor)
                    .ok_or(ShmSkipListError::InvalidNode(cursor))?;
                if previous_key.is_some_and(|key| key.cmp_key(&node.key) != Ordering::Less) {
                    return Err(ShmSkipListError::AllocationAudit(format!(
                        "lane {level} is not strictly ordered"
                    )));
                }
                previous_key = Some(node.key);
                cursor = self.node_next_offset(cursor, level)?;
            }
        }
        let mut cursor = header.retired_head.load(AtomicOrdering::Acquire);
        let mut last = NULL_OFFSET;
        while cursor != NULL_OFFSET {
            let node = self.audit_owned_node(cursor, &mut audit.retired, &mut seen)?;
            if node.flags.load(AtomicOrdering::Acquire) & (NODE_FLAG_MARKED | NODE_FLAG_RETIRED)
                != (NODE_FLAG_MARKED | NODE_FLAG_RETIRED)
            {
                return Err(ShmSkipListError::AllocationAudit(format!(
                    "retired queue contains unretired node {cursor}"
                )));
            }
            last = cursor;
            cursor = node.retire_next.load(AtomicOrdering::Acquire);
        }
        if last != header.retired_tail.load(AtomicOrdering::Acquire)
            || audit.retired.nodes != header.retired_nodes.load(AtomicOrdering::Acquire)
        {
            return Err(ShmSkipListError::AllocationAudit(
                "retired node queue metadata disagrees with traversal".into(),
            ));
        }
        let mut cursor = header.retired_posting_head.load(AtomicOrdering::Acquire);
        let mut retired_postings = 0;
        let mut last = NULL_OFFSET;
        while cursor != NULL_OFFSET {
            audit_claim_offset(&mut seen.postings, cursor, "posting")?;
            let post = self
                .posting_ref(cursor)
                .ok_or(ShmSkipListError::InvalidPosting(cursor))?;
            audit.retired.postings += 1;
            retired_postings += 1;
            last = cursor;
            cursor = post.retire_next.load(AtomicOrdering::Acquire);
        }
        if last != header.retired_posting_tail.load(AtomicOrdering::Acquire)
            || retired_postings != header.retired_postings.load(AtomicOrdering::Acquire)
        {
            return Err(ShmSkipListError::AllocationAudit(
                "retired posting queue metadata disagrees with traversal".into(),
            ));
        }
        for stack in [&header.recycled_nodes, &header.reserve_nodes] {
            let mut cursor = stack_head_offset(stack.load(AtomicOrdering::Acquire));
            while cursor != NULL_OFFSET {
                audit_claim_offset(&mut seen.nodes, cursor, "node")?;
                audit.reusable.nodes += 1;
                cursor = self
                    .node_ref(cursor)
                    .ok_or(ShmSkipListError::InvalidNode(cursor))?
                    .retire_next
                    .load(AtomicOrdering::Acquire);
            }
        }
        for stack in [&header.recycled_postings, &header.reserve_postings] {
            let mut cursor = stack_head_offset(stack.load(AtomicOrdering::Acquire));
            while cursor != NULL_OFFSET {
                audit_claim_offset(&mut seen.postings, cursor, "posting")?;
                audit.reusable.postings += 1;
                cursor = self
                    .posting_ref(cursor)
                    .ok_or(ShmSkipListError::InvalidPosting(cursor))?
                    .next
                    .load(AtomicOrdering::Acquire);
            }
        }
        for height in 1..=MAX_HEIGHT {
            for stack in [
                &header.recycled_towers[height - 1],
                &header.reserve_towers[height - 1],
            ] {
                let mut cursor = stack_head_offset(stack.load(AtomicOrdering::Acquire));
                while cursor != NULL_OFFSET {
                    self.audit_owned_tower(cursor, height, &mut audit.reusable, &mut seen)?;
                    let lane = lane_from_node::<K>(self.shm.mmap_base(), cursor, 0, height).ok_or(
                        ShmSkipListError::InvalidLane {
                            node_offset: cursor,
                            level: 0,
                        },
                    )?;
                    cursor = lane.next.load(AtomicOrdering::Acquire);
                }
            }
        }
        let accounted = audit.reachable.plus(audit.retired).plus(audit.reusable);
        if accounted != audit.allocated {
            return Err(ShmSkipListError::AllocationAudit(format!("unaccounted structural storage: allocated={:?}, reachable={:?}, retired={:?}, reusable={:?}", audit.allocated, audit.reachable, audit.retired, audit.reusable)));
        }
        if audit.reachable.nodes.saturating_sub(1)
            != header.distinct_key_count.load(AtomicOrdering::Acquire) as u64
        {
            return Err(ShmSkipListError::AllocationAudit(
                "distinct key count disagrees with live traversal".into(),
            ));
        }
        Ok(audit)
    }

    fn audit_owned_node<'a>(
        &'a self,
        offset: u32,
        counts: &mut ShmSkipAllocationCounts,
        seen: &mut AllocationAuditSeen,
    ) -> Result<&'a ShmSkipNode<K>, ShmSkipListError> {
        audit_claim_offset(&mut seen.nodes, offset, "node")?;
        let node = self
            .node_ref(offset)
            .ok_or(ShmSkipListError::InvalidNode(offset))?;
        counts.nodes += 1;
        self.audit_owned_tower(
            node.tower_offset,
            node.tower_capacity as usize,
            counts,
            seen,
        )?;
        if node.height == 0 || node.height > node.tower_capacity {
            return Err(ShmSkipListError::AllocationAudit(format!(
                "node {offset} has invalid logical/physical height"
            )));
        }
        let mut cursor = node.postings_head.load(AtomicOrdering::Acquire);
        let mut live_postings = 0;
        while cursor != NULL_OFFSET {
            audit_claim_offset(&mut seen.postings, cursor, "posting")?;
            let post = self
                .posting_ref(cursor)
                .ok_or(ShmSkipListError::InvalidPosting(cursor))?;
            counts.postings += 1;
            live_postings += u32::from(post.deleted.load(AtomicOrdering::Acquire) == 0);
            cursor = post.next.load(AtomicOrdering::Acquire);
        }
        if live_postings != node.live_postings.load(AtomicOrdering::Acquire) {
            return Err(ShmSkipListError::AllocationAudit(format!(
                "node {offset} posting count disagrees with traversal"
            )));
        }
        Ok(node)
    }

    fn audit_owned_tower(
        &self,
        offset: u32,
        height: usize,
        counts: &mut ShmSkipAllocationCounts,
        seen: &mut AllocationAuditSeen,
    ) -> Result<(), ShmSkipListError> {
        audit_claim_offset(&mut seen.towers, offset, "tower")?;
        if !(1..=MAX_HEIGHT).contains(&height)
            || tower_ptr::<K>(self.shm.mmap_base(), offset, height - 1, height).is_none()
        {
            return Err(ShmSkipListError::AllocationAudit(format!(
                "invalid physical tower capacity {height} at {offset}"
            )));
        }
        counts.towers += 1;
        counts.tower_lanes += height as u64;
        Ok(())
    }

    pub fn collect_garbage_once(&self, max_nodes: usize) -> usize {
        let Some(header) = self.header_ref() else {
            return 0;
        };
        // A one-shot try_lock can starve GC indefinitely under continuous writes.
        // Priority admission prevents a stream of foreground callers from barging
        // ahead of the collector until the arena is exhausted.
        let _mutation_guard = header.mutation_lock.lock_priority();
        self.collect_garbage_inner(max_nodes)
    }

    // Called either by a locked public collector or by a locked allocation assist.
    fn collect_garbage_inner(&self, max_nodes: usize) -> usize {
        if max_nodes == 0 {
            return 0;
        }
        let Some(header) = self.header_ref() else {
            return 0;
        };
        if header
            .gc_collect_lock
            .compare_exchange(0, 1, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return 0;
        }
        let _guard = GcCollectGuard {
            lock: &header.gc_collect_lock,
        };

        let snapshot = self.shm.create_snapshot();
        let horizon = snapshot.xmin;
        self.collect_retired_postings(header, horizon, max_nodes);

        let mut reclaimed = 0_usize;
        let mut examined = 0_u64;
        let mut requeued = 0_u64;
        let mut blocked_without_reclaim = 0_usize;
        let blocked_limit = max_nodes
            .min(header.retired_nodes.load(AtomicOrdering::Acquire) as usize)
            .max(1);

        while reclaimed < max_nodes {
            let head_offset = header.retired_head.load(AtomicOrdering::Acquire);
            if head_offset == NULL_OFFSET {
                break;
            }

            let Some(node) = self.node_ref(head_offset) else {
                break;
            };
            examined = examined.saturating_add(1);
            let retire_txid = node.retire_txid.load(AtomicOrdering::Acquire);
            if self.pop_retired_head_offset(header, head_offset).is_none() {
                continue;
            }
            header.retired_nodes.fetch_sub(1, AtomicOrdering::AcqRel);

            if retire_txid == 0 || retire_txid >= horizon {
                if self
                    .enqueue_retired_node_offset(header, head_offset)
                    .is_ok()
                {
                    requeued = requeued.saturating_add(1);
                }
                blocked_without_reclaim = blocked_without_reclaim.saturating_add(1);
                if blocked_without_reclaim >= blocked_limit {
                    break;
                }
                continue;
            }
            blocked_without_reclaim = 0;

            if let Some(popped) = self.node_ref(head_offset) {
                popped
                    .retire_next
                    .store(NULL_OFFSET, AtomicOrdering::Release);
            }

            match self.recycle_retired_node(head_offset) {
                Ok(()) => {
                    reclaimed += 1;
                    header.reclaimed_nodes.fetch_add(1, AtomicOrdering::AcqRel);
                }
                Err(_) => {
                    header
                        .gc_recycle_errors
                        .fetch_add(1, AtomicOrdering::AcqRel);
                    if self
                        .enqueue_retired_node_offset(header, head_offset)
                        .is_ok()
                    {
                        requeued = requeued.saturating_add(1);
                    }
                }
            }
        }

        if examined != 0 {
            header
                .gc_nodes_examined
                .fetch_add(examined, AtomicOrdering::AcqRel);
        }
        if requeued != 0 {
            header
                .gc_nodes_requeued
                .fetch_add(requeued, AtomicOrdering::AcqRel);
        }
        reclaimed
    }

    fn collect_retired_postings(
        &self,
        header: &ShmSkipHeader<K>,
        horizon: u64,
        max_postings: usize,
    ) {
        let mut cursor = header.retired_posting_head.load(AtomicOrdering::Acquire);
        let mut previous: Option<&PostingEntry> = None;
        let mut previous_offset = NULL_OFFSET;
        let mut examined = 0;
        while cursor != NULL_OFFSET && examined < max_postings {
            examined += 1;
            let Some(post) = self.posting_ref(cursor) else {
                break;
            };
            let next = post.retire_next.load(AtomicOrdering::Acquire);
            let retired = post.retire_txid.load(AtomicOrdering::Acquire);
            if retired != 0 && retired < horizon {
                if self.push_recycled_posting(cursor).is_err() {
                    header
                        .gc_recycle_errors
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    break;
                }
                match previous {
                    Some(previous) => previous.retire_next.store(next, AtomicOrdering::Release),
                    None => header
                        .retired_posting_head
                        .store(next, AtomicOrdering::Release),
                }
                if next == NULL_OFFSET {
                    header
                        .retired_posting_tail
                        .store(previous_offset, AtomicOrdering::Release);
                }
                header
                    .retired_postings
                    .fetch_sub(1, AtomicOrdering::Relaxed);
                header
                    .reclaimed_postings
                    .fetch_add(1, AtomicOrdering::Relaxed);
            } else {
                previous = Some(post);
                previous_offset = cursor;
            }
            cursor = next;
        }
    }

    fn pop_retired_head_offset(&self, header: &ShmSkipHeader<K>, head_offset: u32) -> Option<u32> {
        let node = self.node_ref(head_offset)?;
        let mut next = node.retire_next.load(AtomicOrdering::Acquire);
        if next == NULL_OFFSET {
            let tail = header.retired_tail.load(AtomicOrdering::Acquire);
            if tail == head_offset {
                if header
                    .retired_tail
                    .compare_exchange(
                        head_offset,
                        NULL_OFFSET,
                        AtomicOrdering::AcqRel,
                        AtomicOrdering::Acquire,
                    )
                    .is_err()
                {
                    return None;
                }
                if header
                    .retired_head
                    .compare_exchange(
                        head_offset,
                        NULL_OFFSET,
                        AtomicOrdering::AcqRel,
                        AtomicOrdering::Acquire,
                    )
                    .is_err()
                {
                    return None;
                }
                return Some(head_offset);
            }
            let mut spins = 0_u32;
            while next == NULL_OFFSET && spins < 256 {
                next = node.retire_next.load(AtomicOrdering::Acquire);
                spins = spins.wrapping_add(1);
                std::hint::spin_loop();
            }
            if next == NULL_OFFSET {
                return None;
            }
        }
        if header
            .retired_head
            .compare_exchange(
                head_offset,
                next,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_err()
        {
            return None;
        }
        Some(head_offset)
    }

    fn enqueue_retired_node_offset(
        &self,
        header: &ShmSkipHeader<K>,
        node_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        node.retire_next.store(NULL_OFFSET, AtomicOrdering::Release);

        let prev = header
            .retired_tail
            .swap(node_offset, AtomicOrdering::AcqRel);
        if prev == NULL_OFFSET {
            header
                .retired_head
                .store(node_offset, AtomicOrdering::Release);
        } else {
            let prev_node = self
                .node_ref(prev)
                .ok_or(ShmSkipListError::InvalidNode(prev))?;
            prev_node
                .retire_next
                .store(node_offset, AtomicOrdering::Release);
        }
        header.retired_nodes.fetch_add(1, AtomicOrdering::AcqRel);
        Ok(())
    }

    pub fn spawn_gc_daemon(
        &self,
        interval: Duration,
    ) -> Result<ShmSkipListGcDaemon, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        header.gc_stop_requested.store(0, AtomicOrdering::Release);
        let stop_offset = (&header.gc_stop_requested as *const AtomicU32 as usize
            - self.shm.mmap_base().as_ptr() as usize) as u32;
        let parent_pid = unsafe { libc::getpid() };
        let list = self.clone();
        // SAFETY:
        // A dedicated process is used for cross-process GC coordination.
        let fork_result = unsafe { rustix::runtime::fork() }.map_err(|err| {
            ShmSkipListError::Fork(std::io::Error::from_raw_os_error(err.raw_os_error()))
        })?;

        match fork_result {
            rustix::runtime::Fork::Child(_) => {
                let _ = arm_parent_death_signal(parent_pid);
                loop {
                    let backlog = list.retired_nodes() as usize;
                    let Some(header) = list.header_ref() else {
                        std::thread::sleep(interval);
                        continue;
                    };
                    if header.gc_stop_requested.load(AtomicOrdering::Acquire) != 0 {
                        // No mutation/GC guard is held at this point.
                        unsafe { libc::_exit(0) };
                    }
                    header.gc_daemon_cycles.fetch_add(1, AtomicOrdering::AcqRel);
                    let pressure = header.pressure_state.load(AtomicOrdering::Acquire);
                    let failures = header.alloc_failure_events.load(AtomicOrdering::Acquire);
                    let reclaimed = header
                        .gc_assist_reclaimed
                        .load(AtomicOrdering::Acquire)
                        .saturating_add(header.gc_daemon_reclaimed.load(AtomicOrdering::Acquire));
                    let reclaim_efficiency = if failures == 0 {
                        1.0
                    } else {
                        reclaimed as f64 / failures as f64
                    };
                    let pressure_batch_boost = match pressure {
                        PRESSURE_STATE_HOT => 4,
                        PRESSURE_STATE_WARM => 2,
                        _ => 1,
                    };
                    let batch = backlog
                        .clamp(GC_MIN_BATCH, GC_MAX_BATCH)
                        .saturating_mul(pressure_batch_boost)
                        .min(GC_MAX_BATCH);
                    let max_passes = match pressure {
                        PRESSURE_STATE_HOT => GC_MAX_PASSES_PER_WAKE * 4,
                        PRESSURE_STATE_WARM => GC_MAX_PASSES_PER_WAKE * 2,
                        _ => GC_MAX_PASSES_PER_WAKE,
                    };
                    let mut reclaimed_any = false;
                    let mut daemon_reclaimed = 0_u64;
                    for _ in 0..max_passes {
                        let reclaimed = list.collect_garbage_once(batch);
                        if reclaimed == 0 {
                            break;
                        }
                        daemon_reclaimed = daemon_reclaimed.saturating_add(reclaimed as u64);
                        reclaimed_any = true;
                        if reclaimed < batch {
                            break;
                        }
                    }
                    if daemon_reclaimed != 0 {
                        header
                            .gc_daemon_reclaimed
                            .fetch_add(daemon_reclaimed, AtomicOrdering::AcqRel);
                    }
                    if reclaimed_any {
                        std::thread::yield_now();
                    } else {
                        let sleep_for = match pressure {
                            PRESSURE_STATE_HOT => Duration::from_millis(2),
                            PRESSURE_STATE_WARM => Duration::from_millis(10),
                            _ => interval,
                        };
                        let sleep_for =
                            if reclaim_efficiency < 0.25 && pressure != PRESSURE_STATE_NORMAL {
                                sleep_for.min(Duration::from_millis(2))
                            } else {
                                sleep_for
                            };
                        // Bound stop latency even when configured with a long interval.
                        let deadline = std::time::Instant::now() + sleep_for;
                        while header.gc_stop_requested.load(AtomicOrdering::Acquire) == 0 {
                            let remaining =
                                deadline.saturating_duration_since(std::time::Instant::now());
                            if remaining.is_zero() {
                                break;
                            }
                            std::thread::sleep(remaining.min(Duration::from_millis(10)));
                        }
                    }
                }
            }
            rustix::runtime::Fork::Parent(pid) => {
                let pid_raw = pid.as_raw_nonzero().get();
                if let Some(header) = self.header_ref() {
                    header.gc_daemon_pid.store(pid_raw, AtomicOrdering::Release);
                }
                Ok(ShmSkipListGcDaemon {
                    pid: pid_raw,
                    shm: Arc::clone(&self.shm),
                    stop_offset,
                })
            }
        }
    }

    #[inline]
    pub fn retired_nodes(&self) -> u64 {
        self.header_ref()
            .map(|header| header.retired_nodes.load(AtomicOrdering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn distinct_key_count(&self) -> usize {
        self.header_ref()
            .map(|header| header.distinct_key_count.load(AtomicOrdering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn reclaimed_nodes(&self) -> u64 {
        self.header_ref()
            .map(|header| header.reclaimed_nodes.load(AtomicOrdering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn mutation_telemetry(&self) -> ShmSkipMutationTelemetry {
        let Some(header) = self.header_ref() else {
            return ShmSkipMutationTelemetry::default();
        };
        let total_reclaimed = header
            .gc_assist_reclaimed
            .load(AtomicOrdering::Acquire)
            .saturating_add(header.gc_daemon_reclaimed.load(AtomicOrdering::Acquire));
        let last_failures = header
            .pressure_window_last_failures
            .load(AtomicOrdering::Acquire);
        let last_reclaimed = header
            .pressure_window_last_reclaimed
            .load(AtomicOrdering::Acquire);
        ShmSkipMutationTelemetry {
            insert_ops: header.retry_insert_ops.load(AtomicOrdering::Acquire),
            remove_ops: header.retry_remove_ops.load(AtomicOrdering::Acquire),
            retry_loops: header.retry_loops.load(AtomicOrdering::Acquire),
            retry_alloc: header.retry_alloc.load(AtomicOrdering::Acquire),
            retry_structural: header.retry_structural.load(AtomicOrdering::Acquire),
            retry_epoch: header.retry_epoch.load(AtomicOrdering::Acquire),
            max_insert_attempts: header
                .retry_max_insert_attempts
                .load(AtomicOrdering::Acquire),
            max_remove_attempts: header
                .retry_max_remove_attempts
                .load(AtomicOrdering::Acquire),
            gc_nodes_examined: header.gc_nodes_examined.load(AtomicOrdering::Acquire),
            gc_nodes_requeued: header.gc_nodes_requeued.load(AtomicOrdering::Acquire),
            gc_recycle_errors: header.gc_recycle_errors.load(AtomicOrdering::Acquire),
            gc_assist_calls: header.gc_assist_calls.load(AtomicOrdering::Acquire),
            gc_assist_reclaimed: header.gc_assist_reclaimed.load(AtomicOrdering::Acquire),
            gc_daemon_cycles: header.gc_daemon_cycles.load(AtomicOrdering::Acquire),
            gc_daemon_reclaimed: header.gc_daemon_reclaimed.load(AtomicOrdering::Acquire),
            pressure_window_failures: header
                .alloc_failure_events
                .load(AtomicOrdering::Acquire)
                .saturating_sub(last_failures),
            pressure_window_reclaimed: total_reclaimed.saturating_sub(last_reclaimed),
            pressure_consecutive_healthy_windows: header
                .pressure_window_consecutive_healthy
                .load(AtomicOrdering::Acquire),
            retired_backlog: header.retired_nodes.load(AtomicOrdering::Acquire),
            retired_postings: header.retired_postings.load(AtomicOrdering::Acquire),
            reclaimed_postings: header.reclaimed_postings.load(AtomicOrdering::Acquire),
            pressure_state: header.pressure_state.load(AtomicOrdering::Acquire),
            pressure_to_normal: header.pressure_to_normal.load(AtomicOrdering::Acquire),
            pressure_to_warm: header.pressure_to_warm.load(AtomicOrdering::Acquire),
            pressure_to_hot: header.pressure_to_hot.load(AtomicOrdering::Acquire),
            alloc_failure_events: header.alloc_failure_events.load(AtomicOrdering::Acquire),
            reserve_node_pushes: header.reserve_node_pushes.load(AtomicOrdering::Acquire),
            reserve_node_hits: header.reserve_node_hits.load(AtomicOrdering::Acquire),
            reserve_node_misses: header.reserve_node_misses.load(AtomicOrdering::Acquire),
            reserve_posting_pushes: header.reserve_posting_pushes.load(AtomicOrdering::Acquire),
            reserve_posting_hits: header.reserve_posting_hits.load(AtomicOrdering::Acquire),
            reserve_posting_misses: header.reserve_posting_misses.load(AtomicOrdering::Acquire),
            reserve_tower_pushes: header.reserve_tower_pushes.load(AtomicOrdering::Acquire),
            reserve_tower_hits: header.reserve_tower_hits.load(AtomicOrdering::Acquire),
            reserve_tower_misses: header.reserve_tower_misses.load(AtomicOrdering::Acquire),
            retry_phase_b_hits: header.retry_phase_b_hits.load(AtomicOrdering::Acquire),
            retry_phase_c_hits: header.retry_phase_c_hits.load(AtomicOrdering::Acquire),
        }
    }

    #[inline]
    pub(crate) fn flush_local_recycle_caches(&self) {
        let _ = self.shm.flush_local_recycle_caches();
    }

    #[inline]
    pub(crate) fn record_mutation_telemetry(
        &self,
        is_insert: bool,
        attempts: u32,
        retry_alloc: u32,
        retry_structural: u32,
        retry_epoch: u32,
    ) {
        let Some(header) = self.header_ref() else {
            return;
        };
        if is_insert {
            header.retry_insert_ops.fetch_add(1, AtomicOrdering::AcqRel);
            atomic_max_u64(&header.retry_max_insert_attempts, attempts as u64);
        } else {
            header.retry_remove_ops.fetch_add(1, AtomicOrdering::AcqRel);
            atomic_max_u64(&header.retry_max_remove_attempts, attempts as u64);
        }

        let loops = attempts.saturating_sub(1) as u64;
        if loops > 0 {
            header.retry_loops.fetch_add(loops, AtomicOrdering::AcqRel);
        }
        if retry_alloc != 0 {
            header
                .retry_alloc
                .fetch_add(retry_alloc as u64, AtomicOrdering::AcqRel);
        }
        if retry_structural != 0 {
            header
                .retry_structural
                .fetch_add(retry_structural as u64, AtomicOrdering::AcqRel);
        }
        if retry_epoch != 0 {
            header
                .retry_epoch
                .fetch_add(retry_epoch as u64, AtomicOrdering::AcqRel);
        }
    }

    #[inline]
    pub(crate) fn record_retry_phase_b_hit(&self) {
        if let Some(header) = self.header_ref() {
            header
                .retry_phase_b_hits
                .fetch_add(1, AtomicOrdering::AcqRel);
        }
    }

    #[inline]
    pub(crate) fn record_retry_phase_c_hit(&self) {
        if let Some(header) = self.header_ref() {
            header
                .retry_phase_c_hits
                .fetch_add(1, AtomicOrdering::AcqRel);
        }
    }

    fn find(
        &self,
        key: &K,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) -> Result<Option<u32>, ShmSkipListError> {
        'retry: loop {
            let header = self
                .header_ref()
                .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
            let head_offset = header.head.load(AtomicOrdering::Acquire);
            let mut pred_offset = head_offset;
            let mut found = None;

            let top = header
                .current_height
                .load(AtomicOrdering::Acquire)
                .clamp(1, MAX_HEIGHT as u32) as usize
                - 1;
            for level in (0..=top).rev() {
                let mut curr_offset = self
                    .lane_ref_by_offset(pred_offset, level)?
                    .next
                    .load(AtomicOrdering::Acquire);
                loop {
                    if curr_offset == NULL_OFFSET {
                        break;
                    }
                    let curr = self
                        .node_ref(curr_offset)
                        .ok_or(ShmSkipListError::InvalidNode(curr_offset))?;
                    let curr_lane = self.lane_ref(curr_offset, curr, level)?;
                    let curr_next = curr_lane.next.load(AtomicOrdering::Acquire);

                    let node_marked =
                        curr.flags.load(AtomicOrdering::Acquire) & NODE_FLAG_MARKED != 0;
                    let lane_marked = curr_lane.marked.load(AtomicOrdering::Acquire) != 0;
                    if node_marked || lane_marked {
                        let pred_lane = self.lane_ref_by_offset(pred_offset, level)?;
                        if pred_lane
                            .next
                            .compare_exchange(
                                curr_offset,
                                curr_next,
                                AtomicOrdering::AcqRel,
                                AtomicOrdering::Acquire,
                            )
                            .is_err()
                        {
                            continue 'retry;
                        }
                        curr_offset = curr_next;
                        continue;
                    }

                    match curr.key.cmp_key(key) {
                        Ordering::Less => {
                            pred_offset = curr_offset;
                            curr_offset = curr_next;
                        }
                        Ordering::Equal => {
                            if found.is_none() {
                                found = Some(curr_offset);
                            }
                            break;
                        }
                        Ordering::Greater => break,
                    }
                }

                preds[level] = pred_offset;
                succs[level] = curr_offset;
            }

            for level in top + 1..MAX_HEIGHT {
                preds[level] = head_offset;
                succs[level] = NULL_OFFSET;
            }

            return Ok(found);
        }
    }

    fn find_readonly_exact(&self, key: &K) -> Result<Option<u32>, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        if header.has_tombstones.load(AtomicOrdering::Acquire) == 0 {
            return self.find_readonly_exact_fast(header, key);
        }
        self.find_readonly_exact_with_tombstones(header, key)
    }

    fn find_readonly_exact_fast(
        &self,
        header: &ShmSkipHeader<K>,
        key: &K,
    ) -> Result<Option<u32>, ShmSkipListError> {
        let base = self.shm.mmap_base().as_ptr();
        let head_offset = header.head.load(AtomicOrdering::Acquire);
        let mut pred_offset = head_offset;

        let top = header
            .current_height
            .load(AtomicOrdering::Acquire)
            .clamp(1, MAX_HEIGHT as u32) as usize
            - 1;
        for level in (0..=top).rev() {
            let mut curr_offset = unsafe {
                self.lane_ref_by_offset_unchecked(base, pred_offset, level)
                    .next
                    .load(AtomicOrdering::Acquire)
            };
            loop {
                if curr_offset == NULL_OFFSET {
                    break;
                }
                let curr = unsafe { self.node_ref_unchecked(base, curr_offset) };
                let curr_lane = unsafe { self.lane_ref_node_unchecked(base, curr, level) };
                let curr_next = curr_lane.next.load(AtomicOrdering::Acquire);

                match curr.key.cmp_key(key) {
                    Ordering::Less => {
                        pred_offset = curr_offset;
                        curr_offset = curr_next;
                    }
                    Ordering::Equal => return Ok(Some(curr_offset)),
                    Ordering::Greater => break,
                }
            }
        }

        Ok(None)
    }

    fn find_readonly_exact_with_tombstones(
        &self,
        header: &ShmSkipHeader<K>,
        key: &K,
    ) -> Result<Option<u32>, ShmSkipListError> {
        let head_offset = header.head.load(AtomicOrdering::Acquire);
        let mut pred_offset = head_offset;

        let top = header
            .current_height
            .load(AtomicOrdering::Acquire)
            .clamp(1, MAX_HEIGHT as u32) as usize
            - 1;
        for level in (0..=top).rev() {
            let mut curr_offset = self
                .lane_ref_by_offset(pred_offset, level)?
                .next
                .load(AtomicOrdering::Acquire);
            loop {
                if curr_offset == NULL_OFFSET {
                    break;
                }
                let curr = self
                    .node_ref(curr_offset)
                    .ok_or(ShmSkipListError::InvalidNode(curr_offset))?;
                let curr_lane = self.lane_ref(curr_offset, curr, level)?;
                let curr_next = curr_lane.next.load(AtomicOrdering::Acquire);

                let flags = curr.flags.load(AtomicOrdering::Acquire);
                if flags & NODE_FLAG_MARKED != 0 {
                    curr_offset = curr_next;
                    continue;
                }
                if curr_lane.marked.load(AtomicOrdering::Acquire) != 0 {
                    curr_offset = curr_next;
                    continue;
                }

                match curr.key.cmp_key(key) {
                    Ordering::Less => {
                        pred_offset = curr_offset;
                        curr_offset = curr_next;
                    }
                    Ordering::Equal => return Ok(Some(curr_offset)),
                    Ordering::Greater => break,
                }
            }
        }

        Ok(None)
    }

    // Requires the same mutation guard that protected the search producing
    // preds/succs. No structural mutation may intervene before this call.
    fn unlink_node(
        &self,
        key: &K,
        node_offset: u32,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) -> Result<(), ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;

        let mut flags = node.flags.load(AtomicOrdering::Acquire);
        let mut newly_marked = false;
        loop {
            if flags & NODE_FLAG_MARKED != 0 {
                break;
            }
            match node.flags.compare_exchange(
                flags,
                flags | NODE_FLAG_MARKED,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => {
                    newly_marked = true;
                    break;
                }
                Err(observed) => flags = observed,
            }
        }

        if newly_marked {
            self.mark_tombstone_seen();
            self.decrement_distinct_key_count();
        }

        let height = node.height as usize;
        for level in (0..height).rev() {
            self.lane_ref(node_offset, node, level)?
                .marked
                .store(1, AtomicOrdering::Release);
        }

        // Ensure the node is detached from every lane before retiring its offsets.
        // If we retire after only level-0 is unlinked, stale upper-lane pointers can
        // survive long enough to hit recycled offsets and trigger structural retries.
        // The initial find already supplies every predecessor of this fully
        // linked node. The mutation guard excludes insertion, unlink and GC;
        // posting deletion and marking above cannot invalidate those links.
        // Reuse that window instead of searching the entire list a second time.
        let mut detached_all = true;
        for level in (0..height).rev() {
            if succs[level] != node_offset {
                detached_all = false;
                break;
            }
            let pred_lane = self.lane_ref_by_offset(preds[level], level)?;
            let next = self.node_next_offset(node_offset, level)?;
            if pred_lane
                .next
                .compare_exchange(
                    node_offset,
                    next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_err()
            {
                detached_all = false;
                break;
            }
        }
        // Preserve the original helping/recheck path if a window is incomplete
        // or a link unexpectedly changed. Never retire after a partial detach.
        while !detached_all {
            let _ = self.find(key, preds, succs)?;
            detached_all = true;
            for level in (0..height).rev() {
                if succs[level] != node_offset {
                    continue;
                }
                detached_all = false;
                let pred_lane = self.lane_ref_by_offset(preds[level], level)?;
                let next = self.node_next_offset(node_offset, level)?;
                let _ = pred_lane.next.compare_exchange(
                    node_offset,
                    next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                );
            }
            if detached_all {
                break;
            }
        }

        self.retire_node(node_offset);
        Ok(())
    }

    fn decrement_distinct_key_count(&self) {
        let Some(header) = self.header_ref() else {
            return;
        };
        loop {
            let current = header.distinct_key_count.load(AtomicOrdering::Acquire);
            if current == 0 {
                return;
            }
            if header
                .distinct_key_count
                .compare_exchange(
                    current,
                    current - 1,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return;
            }
            std::hint::spin_loop();
        }
    }

    fn retire_node(&self, node_offset: u32) {
        let Some(node) = self.node_ref(node_offset) else {
            return;
        };
        let mut flags = node.flags.load(AtomicOrdering::Acquire);
        loop {
            if flags & NODE_FLAG_RETIRED != 0 {
                return;
            }
            match node.flags.compare_exchange(
                flags,
                flags | NODE_FLAG_RETIRED,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => flags = observed,
            }
        }
        // Use the already-issued transaction timeline without advancing it. Retirements are
        // metadata for GC visibility; minting fresh txids here inflates horizons and delays
        // reclamation under sustained churn.
        let retire_txid = self
            .shm
            .global_txid()
            .load(AtomicOrdering::Acquire)
            .saturating_sub(1);
        node.retire_txid.store(retire_txid, AtomicOrdering::Release);

        let Some(header) = self.header_ref() else {
            return;
        };
        let _ = self.enqueue_retired_node_offset(header, node_offset);
    }

    fn prepend_existing_posting(
        &self,
        node: &ShmSkipNode<K>,
        posting_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        let posting = self
            .posting_ref(posting_offset)
            .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;

        loop {
            let old = node.postings_head.load(AtomicOrdering::Acquire);
            posting.next.store(old, AtomicOrdering::Release);
            posting.deleted.store(0, AtomicOrdering::Release);
            if node
                .postings_head
                .compare_exchange(
                    old,
                    posting_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                node.live_postings.fetch_add(1, AtomicOrdering::AcqRel);
                return Ok(());
            }
        }
    }

    fn find_live_node_offset(
        &self,
        key: &K,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) -> Result<Option<u32>, ShmSkipListError> {
        let Some(node_offset) = self.find(key, preds, succs)? else {
            return Ok(None);
        };
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        let flags = node.flags.load(AtomicOrdering::Acquire);
        if flags & NODE_FLAG_MARKED != 0 || flags & NODE_FLAG_FULLY_LINKED == 0 {
            return Ok(None);
        }
        Ok(Some(node_offset))
    }

    fn node_contains_live_payload(
        &self,
        node: &ShmSkipNode<K>,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<bool, ShmSkipListError> {
        let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while post_offset != NULL_OFFSET {
            let post = self
                .posting_ref(post_offset)
                .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
            if post.deleted.load(AtomicOrdering::Acquire) == 0
                && self.posting_payload_equals(post, payload_len, payload)?
            {
                return Ok(true);
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }
        Ok(false)
    }

    // Requires mutation_lock. Errors occur only before any node/posting publication.
    fn attach_posting_to_key(
        &self,
        key: K,
        posting_offset: u32,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        if let Some(offset) = self.find(&key, &mut preds, &mut succs)? {
            let node = self
                .node_ref(offset)
                .ok_or(ShmSkipListError::InvalidNode(offset))?;
            if self.node_contains_live_payload(node, payload_len, payload)? {
                self.push_recycled_posting(posting_offset)?;
            } else {
                self.prepend_existing_posting(node, posting_offset)?;
            }
            return Ok(());
        }
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let height = self.random_height(header) as usize;
        // Validate every destination before acquiring more storage. Holding the
        // mutation lock keeps these predecessors attached until publication ends.
        let mut pred_lanes = [None; MAX_HEIGHT];
        for level in 0..height {
            pred_lanes[level] = Some(self.lane_ref_by_offset(preds[level], level)?);
        }
        let tower = self.alloc_tower(height, &succs)?;
        let node_offset = match self.alloc_node(
            key,
            height as u8,
            tower.capacity as u8,
            tower.offset,
            posting_offset,
        ) {
            Ok(offset) => offset,
            Err(err) => {
                self.push_recycled_tower(tower.offset, tower.capacity)?;
                return Err(err);
            }
        };
        let Some(node) = self.node_ref(node_offset) else {
            self.recycle_unlinked_insert_node_and_tower(node_offset, tower.offset, tower.capacity)?;
            return Err(ShmSkipListError::InvalidNode(node_offset));
        };
        // No fallible operation after this point: ownership transfers to the list.
        for lane in pred_lanes.into_iter().take(height).flatten() {
            lane.next.store(node_offset, AtomicOrdering::Release);
        }
        node.flags
            .store(NODE_FLAG_FULLY_LINKED, AtomicOrdering::Release);
        header
            .distinct_key_count
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.maybe_raise_height(header, height as u32);
        Ok(())
    }

    #[inline]
    fn pressure_thresholds(capacity_bytes: usize) -> (usize, usize, usize, usize) {
        let warm_enter = (capacity_bytes / 4).clamp(4 << 20, 512 << 20);
        let warm_exit = (capacity_bytes / 3).clamp(6 << 20, 640 << 20);
        let hot_enter = (capacity_bytes / 16).clamp(1 << 20, 128 << 20);
        let hot_exit = (capacity_bytes / 12).clamp(2 << 20, 160 << 20);
        let warm_enter = warm_enter.max(hot_enter);
        let warm_exit = warm_exit.max(hot_exit);
        (warm_enter, warm_exit, hot_enter, hot_exit)
    }

    #[inline]
    fn update_pressure_state(&self, header: &ShmSkipHeader<K>, remaining_bytes: usize) -> u32 {
        let (warm_enter, warm_exit, hot_enter, hot_exit) =
            Self::pressure_thresholds(self.shm.len());
        let failure_events = header.alloc_failure_events.load(AtomicOrdering::Acquire);
        let total_reclaimed = header
            .gc_assist_reclaimed
            .load(AtomicOrdering::Acquire)
            .saturating_add(header.gc_daemon_reclaimed.load(AtomicOrdering::Acquire));
        let lifetime_efficiency = if failure_events == 0 {
            1.0
        } else {
            total_reclaimed as f64 / failure_events as f64
        };
        let last_failures = header
            .pressure_window_last_failures
            .load(AtomicOrdering::Acquire);
        let last_reclaimed = header
            .pressure_window_last_reclaimed
            .load(AtomicOrdering::Acquire);
        let window_failures = failure_events.saturating_sub(last_failures);
        let window_reclaimed = total_reclaimed.saturating_sub(last_reclaimed);
        let window_efficiency = if window_failures == 0 {
            lifetime_efficiency
        } else {
            window_reclaimed as f64 / window_failures as f64
        };

        if window_failures >= PRESSURE_WINDOW_MIN_FAILURES
            && header
                .pressure_window_last_failures
                .compare_exchange(
                    last_failures,
                    failure_events,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
        {
            header
                .pressure_window_last_reclaimed
                .store(total_reclaimed, AtomicOrdering::Release);
            if window_efficiency >= PRESSURE_EFFICIENCY_HEALTHY {
                header
                    .pressure_window_consecutive_healthy
                    .fetch_add(1, AtomicOrdering::AcqRel);
            } else {
                header
                    .pressure_window_consecutive_healthy
                    .store(0, AtomicOrdering::Release);
            }
        }

        let effective_failures = if window_failures != 0 {
            window_failures
        } else {
            failure_events
        };
        let effective_efficiency = if window_failures != 0 {
            window_efficiency
        } else {
            lifetime_efficiency
        };
        let force_hot_efficiency =
            effective_failures >= 32 && effective_efficiency <= PRESSURE_EFFICIENCY_HOT_FLOOR;
        let force_warm_efficiency =
            effective_failures >= 16 && effective_efficiency <= PRESSURE_EFFICIENCY_WARM_FLOOR;
        let healthy_efficiency = effective_efficiency >= PRESSURE_EFFICIENCY_HEALTHY;
        let healthy_windows = header
            .pressure_window_consecutive_healthy
            .load(AtomicOrdering::Acquire);

        loop {
            let current = header.pressure_state.load(AtomicOrdering::Acquire);
            let next = match current {
                PRESSURE_STATE_NORMAL => {
                    if remaining_bytes <= hot_enter || force_hot_efficiency {
                        PRESSURE_STATE_HOT
                    } else if remaining_bytes <= warm_enter || force_warm_efficiency {
                        PRESSURE_STATE_WARM
                    } else {
                        PRESSURE_STATE_NORMAL
                    }
                }
                PRESSURE_STATE_WARM => {
                    if remaining_bytes <= hot_enter || force_hot_efficiency {
                        PRESSURE_STATE_HOT
                    } else if remaining_bytes >= warm_exit && healthy_efficiency {
                        PRESSURE_STATE_NORMAL
                    } else {
                        PRESSURE_STATE_WARM
                    }
                }
                PRESSURE_STATE_HOT => {
                    if remaining_bytes >= hot_exit && !force_hot_efficiency {
                        if remaining_bytes >= warm_exit
                            && healthy_efficiency
                            && healthy_windows >= PRESSURE_HEALTHY_WINDOWS_REQUIRED
                        {
                            PRESSURE_STATE_NORMAL
                        } else {
                            PRESSURE_STATE_WARM
                        }
                    } else {
                        PRESSURE_STATE_HOT
                    }
                }
                _ => {
                    if remaining_bytes <= hot_enter || force_hot_efficiency {
                        PRESSURE_STATE_HOT
                    } else if remaining_bytes <= warm_enter || force_warm_efficiency {
                        PRESSURE_STATE_WARM
                    } else {
                        PRESSURE_STATE_NORMAL
                    }
                }
            };
            if next == current {
                return current;
            }
            if header
                .pressure_state
                .compare_exchange(
                    current,
                    next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                match next {
                    PRESSURE_STATE_NORMAL => {
                        header
                            .pressure_to_normal
                            .fetch_add(1, AtomicOrdering::AcqRel);
                    }
                    PRESSURE_STATE_WARM => {
                        header.pressure_to_warm.fetch_add(1, AtomicOrdering::AcqRel);
                    }
                    PRESSURE_STATE_HOT => {
                        header.pressure_to_hot.fetch_add(1, AtomicOrdering::AcqRel);
                    }
                    _ => {}
                }
                if next != current {
                    self.refill_reserve_stacks_for_pressure(header, next);
                }
                return next;
            }
            std::hint::spin_loop();
        }
    }

    fn refill_reserve_stacks_for_pressure(&self, header: &ShmSkipHeader<K>, state: u32) {
        let (node_batch, posting_batch, tower_batch_per_level) = match state {
            PRESSURE_STATE_HOT => (
                RESERVE_REFILL_NODE_BATCH_HOT,
                RESERVE_REFILL_POSTING_BATCH_HOT,
                RESERVE_REFILL_TOWER_BATCH_PER_LEVEL_HOT,
            ),
            PRESSURE_STATE_WARM => (
                RESERVE_REFILL_NODE_BATCH_WARM,
                RESERVE_REFILL_POSTING_BATCH_WARM,
                RESERVE_REFILL_TOWER_BATCH_PER_LEVEL_WARM,
            ),
            _ => return,
        };

        for _ in 0..node_batch {
            let Some(offset) = self.pop_node_stack(&header.recycled_nodes) else {
                break;
            };
            let _ = self.push_reserve_node(header, offset);
        }

        for _ in 0..posting_batch {
            let Some(offset) = self.pop_posting_stack(&header.recycled_postings) else {
                break;
            };
            let _ = self.push_reserve_posting(header, offset);
        }

        for tower_height in 1..=MAX_HEIGHT {
            for _ in 0..tower_batch_per_level {
                let Some(offset) = self.pop_recycled_tower_with_header(header, tower_height) else {
                    break;
                };
                let _ = self.push_reserve_tower(header, offset, tower_height);
            }
        }
    }

    #[inline]
    fn maybe_collect_garbage_on_alloc_failure(&self, header: &ShmSkipHeader<K>) {
        let failure_seq = header
            .alloc_failure_events
            .fetch_add(1, AtomicOrdering::AcqRel)
            .wrapping_add(1);
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let state = self.update_pressure_state(header, remaining);

        let cadence = match state {
            PRESSURE_STATE_HOT => GC_ASSIST_FAILURE_CADENCE_HOT,
            PRESSURE_STATE_WARM => GC_ASSIST_FAILURE_CADENCE_WARM,
            _ => GC_ASSIST_FAILURE_CADENCE_NORMAL,
        };
        if cadence > 1 && (failure_seq % cadence) != 0 {
            return;
        }

        let batch = match state {
            PRESSURE_STATE_HOT => GC_ASSIST_BATCH_HOT,
            PRESSURE_STATE_WARM => GC_ASSIST_BATCH_WARM,
            _ => GC_ASSIST_BATCH_NORMAL,
        };
        header.gc_assist_calls.fetch_add(1, AtomicOrdering::AcqRel);
        let reclaimed = self.collect_garbage_inner(batch);
        if reclaimed > 0 {
            header
                .gc_assist_reclaimed
                .fetch_add(reclaimed as u64, AtomicOrdering::AcqRel);
        }
    }

    #[inline]
    fn mark_tombstone_seen(&self) {
        if let Some(header) = self.header_ref() {
            header.has_tombstones.store(1, AtomicOrdering::Release);
        }
    }

    fn decrement_live_postings(&self, node: &ShmSkipNode<K>) -> u32 {
        loop {
            let current = node.live_postings.load(AtomicOrdering::Acquire);
            if current == 0 {
                return 0;
            }
            let next = current - 1;
            if node
                .live_postings
                .compare_exchange(
                    current,
                    next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return next;
            }
            std::hint::spin_loop();
        }
    }

    fn initialize_recycled_posting(
        &self,
        offset: u32,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<u32, ShmSkipListError> {
        let ptr = self.posting_ptr(offset)?;
        // This slot is exclusively owned and not visible to readers. Clear old spill
        // metadata before a fallible spill allocation so rollback cannot free it twice.
        unsafe { ptr.write(PostingEntry::new_inline(0, &[], NULL_OFFSET)) };
        let entry = match self.build_posting_entry(payload_len, payload, NULL_OFFSET) {
            Ok(entry) => entry,
            Err(err) => {
                self.push_recycled_posting(offset)?;
                return Err(err);
            }
        };
        unsafe { ptr.write(entry) };
        Ok(offset)
    }

    fn alloc_posting_entry(
        &self,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<u32, ShmSkipListError> {
        Self::validate_payload_args(payload_len, payload)?;
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let pressure_state = self.update_pressure_state(header, remaining);
        if pressure_state != PRESSURE_STATE_NORMAL {
            if let Some(offset) = self.pop_reserve_posting() {
                return self.initialize_recycled_posting(offset, payload_len, payload);
            }
        }
        if let Some(offset) = self.pop_recycled_posting() {
            return self.initialize_recycled_posting(offset, payload_len, payload);
        }
        // Reserve the posting slot before its spill. No by-value posting containing
        // an allocated spill is discarded on a failed posting allocation.
        let allocate_slot = || {
            let offset = self.shm.chunked_arena().alloc_raw_in_class(
                size_of::<PostingEntry>(),
                align_of::<PostingEntry>(),
                ArenaClass::SkipPosting,
            )?;
            header
                .allocated_postings
                .fetch_add(1, AtomicOrdering::Relaxed);
            Ok::<_, ShmAllocError>(offset)
        };
        let offset = match allocate_slot() {
            Ok(offset) => offset,
            Err(_) => {
                self.maybe_collect_garbage_on_alloc_failure(header);
                if let Some(offset) = self
                    .pop_reserve_posting()
                    .or_else(|| self.pop_recycled_posting())
                {
                    offset
                } else {
                    allocate_slot()?
                }
            }
        };
        self.initialize_recycled_posting(offset, payload_len, payload)
    }

    fn alloc_fresh_node(
        &self,
        header: &ShmSkipHeader<K>,
        node: ShmSkipNode<K>,
    ) -> Result<RelPtr<ShmSkipNode<K>>, ShmAllocError> {
        let ptr = self
            .shm
            .chunked_arena()
            .alloc_in_class(node, ArenaClass::SkipNode)?;
        header.allocated_nodes.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(ptr)
    }

    fn alloc_node(
        &self,
        key: K,
        height: u8,
        tower_capacity: u8,
        tower_offset: u32,
        posting_offset: u32,
    ) -> Result<u32, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let pressure_state = self.update_pressure_state(header, remaining);

        if pressure_state != PRESSURE_STATE_NORMAL {
            if let Some(offset) = self.pop_reserve_node() {
                let ptr = self.node_ptr(offset)?;
                // SAFETY:
                // `offset` was previously allocated as `ShmSkipNode<K>` and was popped from a
                // reserve node stack, so this write reinitializes owned memory.
                unsafe {
                    ptr.write(ShmSkipNode::new(
                        key,
                        height,
                        tower_capacity,
                        tower_offset,
                        posting_offset,
                        1,
                    ))
                };
                return Ok(offset);
            }
        }
        if let Some(offset) = self.pop_recycled_node() {
            let ptr = self.node_ptr(offset)?;
            // SAFETY:
            // `offset` was previously allocated as `ShmSkipNode<K>` and was popped from the
            // skiplist-local recycled-node stack, so this write reinitializes owned memory.
            unsafe {
                ptr.write(ShmSkipNode::new(
                    key,
                    height,
                    tower_capacity,
                    tower_offset,
                    posting_offset,
                    1,
                ))
            };
            return Ok(offset);
        }

        match self.alloc_fresh_node(
            header,
            ShmSkipNode::new(key, height, tower_capacity, tower_offset, posting_offset, 1),
        ) {
            Ok(ptr) => Ok(ptr.load(AtomicOrdering::Acquire)),
            Err(err) => {
                self.maybe_collect_garbage_on_alloc_failure(header);
                if header.pressure_state.load(AtomicOrdering::Acquire) != PRESSURE_STATE_NORMAL {
                    if let Some(offset) = self.pop_reserve_node() {
                        let ptr = self.node_ptr(offset)?;
                        // SAFETY:
                        // `offset` was previously allocated as `ShmSkipNode<K>` and was popped
                        // from a node reserve stack, so this write reinitializes owned memory.
                        unsafe {
                            ptr.write(ShmSkipNode::new(
                                key,
                                height,
                                tower_capacity,
                                tower_offset,
                                posting_offset,
                                1,
                            ))
                        };
                        return Ok(offset);
                    }
                }
                if let Some(offset) = self.pop_recycled_node() {
                    let ptr = self.node_ptr(offset)?;
                    // SAFETY:
                    // `offset` was previously allocated as `ShmSkipNode<K>` and was popped
                    // from the recycled node stack, so this write reinitializes owned memory.
                    unsafe {
                        ptr.write(ShmSkipNode::new(
                            key,
                            height,
                            tower_capacity,
                            tower_offset,
                            posting_offset,
                            1,
                        ))
                    };
                    return Ok(offset);
                }
                let _ = err;
                Ok(self
                    .alloc_fresh_node(
                        header,
                        ShmSkipNode::new(
                            key,
                            height,
                            tower_capacity,
                            tower_offset,
                            posting_offset,
                            1,
                        ),
                    )?
                    .load(AtomicOrdering::Acquire))
            }
        }
    }

    fn push_recycled_posting(&self, posting_offset: u32) -> Result<(), ShmSkipListError> {
        let posting = self
            .posting_ref(posting_offset)
            .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;
        self.recycle_posting_payload_storage(posting)?;
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let state = self.update_pressure_state(header, remaining);
        if state != PRESSURE_STATE_NORMAL {
            self.push_reserve_posting(header, posting_offset)?;
            return Ok(());
        }
        self.push_posting_stack(&header.recycled_postings, posting_offset)
    }

    fn push_posting_stack(
        &self,
        stack_head: &AtomicU64,
        posting_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        let posting = self
            .posting_ref(posting_offset)
            .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            posting.next.store(old_head, AtomicOrdering::Release);
            let new = pack_stack_head(posting_offset, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    fn pop_posting_stack(&self, stack_head: &AtomicU64) -> Option<u32> {
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let head = stack_head_offset(old);
            if head == NULL_OFFSET {
                return None;
            }
            let posting = self.posting_ref(head)?;
            let next = posting.next.load(AtomicOrdering::Acquire);
            let new = pack_stack_head(next, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(head);
            }
            std::hint::spin_loop();
        }
    }

    fn push_reserve_posting(
        &self,
        header: &ShmSkipHeader<K>,
        posting_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        self.push_posting_stack(&header.reserve_postings, posting_offset)?;
        header
            .reserve_posting_pushes
            .fetch_add(1, AtomicOrdering::AcqRel);
        Ok(())
    }

    fn pop_reserve_posting(&self) -> Option<u32> {
        let header = self.header_ref()?;
        let out = self.pop_posting_stack(&header.reserve_postings);
        if out.is_some() {
            header
                .reserve_posting_hits
                .fetch_add(1, AtomicOrdering::AcqRel);
        } else {
            header
                .reserve_posting_misses
                .fetch_add(1, AtomicOrdering::AcqRel);
        }
        out
    }

    fn pop_recycled_posting(&self) -> Option<u32> {
        self.header_ref()
            .and_then(|header| self.pop_posting_stack(&header.recycled_postings))
    }

    fn push_recycled_node(&self, node_offset: u32) -> Result<(), ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let state = self.update_pressure_state(header, remaining);
        if state != PRESSURE_STATE_NORMAL {
            self.push_reserve_node(header, node_offset)?;
            return Ok(());
        }
        self.push_node_stack(&header.recycled_nodes, node_offset)
    }

    fn push_node_stack(
        &self,
        stack_head: &AtomicU64,
        node_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            node.retire_next.store(old_head, AtomicOrdering::Release);
            let new = pack_stack_head(node_offset, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    fn pop_node_stack(&self, stack_head: &AtomicU64) -> Option<u32> {
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let head = stack_head_offset(old);
            if head == NULL_OFFSET {
                return None;
            }
            let node = self.node_ref(head)?;
            let next = node.retire_next.load(AtomicOrdering::Acquire);
            let new = pack_stack_head(next, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(head);
            }
            std::hint::spin_loop();
        }
    }

    fn push_reserve_node(
        &self,
        header: &ShmSkipHeader<K>,
        node_offset: u32,
    ) -> Result<(), ShmSkipListError> {
        self.push_node_stack(&header.reserve_nodes, node_offset)?;
        header
            .reserve_node_pushes
            .fetch_add(1, AtomicOrdering::AcqRel);
        Ok(())
    }

    fn pop_reserve_node(&self) -> Option<u32> {
        let header = self.header_ref()?;
        let out = self.pop_node_stack(&header.reserve_nodes);
        if out.is_some() {
            header
                .reserve_node_hits
                .fetch_add(1, AtomicOrdering::AcqRel);
        } else {
            header
                .reserve_node_misses
                .fetch_add(1, AtomicOrdering::AcqRel);
        }
        out
    }

    fn pop_recycled_node(&self) -> Option<u32> {
        self.header_ref()
            .and_then(|header| self.pop_node_stack(&header.recycled_nodes))
    }

    fn push_recycled_tower(
        &self,
        tower_offset: u32,
        tower_height: usize,
    ) -> Result<(), ShmSkipListError> {
        if !(1..=MAX_HEIGHT).contains(&tower_height) {
            return Err(ShmSkipListError::InvalidLane {
                node_offset: NULL_OFFSET,
                level: tower_height,
            });
        }
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let state = self.update_pressure_state(header, remaining);
        if state != PRESSURE_STATE_NORMAL {
            self.push_reserve_tower(header, tower_offset, tower_height)?;
            return Ok(());
        }
        self.push_tower_stack(
            &header.recycled_towers[tower_height - 1],
            tower_offset,
            tower_height,
        )?;
        Self::set_tower_mask_bit(&header.recycled_tower_nonempty_mask, tower_height);
        Ok(())
    }

    fn push_tower_stack(
        &self,
        stack_head: &AtomicU64,
        tower_offset: u32,
        tower_height: usize,
    ) -> Result<(), ShmSkipListError> {
        let lane0_ptr = tower_ptr::<K>(self.shm.mmap_base(), tower_offset, 0, tower_height).ok_or(
            ShmSkipListError::InvalidLane {
                node_offset: NULL_OFFSET,
                level: 0,
            },
        )?;
        // SAFETY:
        // `tower_ptr` validated the pointer bounds and alignment for lane-0.
        let lane0 = unsafe { &*lane0_ptr };
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            lane0.next.store(old_head, AtomicOrdering::Release);
            lane0.marked.store(0, AtomicOrdering::Release);
            let new = pack_stack_head(tower_offset, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    fn pop_tower_stack(&self, stack_head: &AtomicU64, tower_height: usize) -> Option<u32> {
        if !(1..=MAX_HEIGHT).contains(&tower_height) {
            return None;
        }
        loop {
            let old = stack_head.load(AtomicOrdering::Acquire);
            let tower_offset = stack_head_offset(old);
            if tower_offset == NULL_OFFSET {
                return None;
            }
            let lane0_ptr = tower_ptr::<K>(self.shm.mmap_base(), tower_offset, 0, tower_height)?;
            // SAFETY:
            // `tower_ptr` validated the pointer bounds and alignment for lane-0.
            let lane0 = unsafe { &*lane0_ptr };
            let next = lane0.next.load(AtomicOrdering::Acquire);
            let new = pack_stack_head(next, stack_head_tag(old).wrapping_add(1));
            if stack_head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(tower_offset);
            }
            std::hint::spin_loop();
        }
    }

    fn push_reserve_tower(
        &self,
        header: &ShmSkipHeader<K>,
        tower_offset: u32,
        tower_height: usize,
    ) -> Result<(), ShmSkipListError> {
        self.push_tower_stack(
            &header.reserve_towers[tower_height - 1],
            tower_offset,
            tower_height,
        )?;
        header
            .reserve_tower_pushes
            .fetch_add(1, AtomicOrdering::AcqRel);
        Self::set_tower_mask_bit(&header.reserve_tower_nonempty_mask, tower_height);
        Ok(())
    }

    fn pop_reserve_tower(&self, tower_height: usize) -> Option<u32> {
        if !(1..=MAX_HEIGHT).contains(&tower_height) {
            return None;
        }
        let header = self.header_ref()?;
        let out = self.pop_reserve_tower_with_header(header, tower_height);
        if out.is_some() {
            header
                .reserve_tower_hits
                .fetch_add(1, AtomicOrdering::AcqRel);
        } else {
            header
                .reserve_tower_misses
                .fetch_add(1, AtomicOrdering::AcqRel);
        }
        out
    }

    fn pop_recycled_tower(&self, tower_height: usize) -> Option<u32> {
        if !(1..=MAX_HEIGHT).contains(&tower_height) {
            return None;
        }
        let header = self.header_ref()?;
        self.pop_recycled_tower_with_header(header, tower_height)
    }

    fn pop_reserve_tower_at_least(&self, min_height: usize) -> Option<TowerAlloc> {
        if min_height > MAX_HEIGHT {
            return None;
        }
        let header = self.header_ref()?;
        if let Some(mut tower_height) = Self::first_set_tower_height_at_or_above(
            header
                .reserve_tower_nonempty_mask
                .load(AtomicOrdering::Acquire),
            min_height,
        ) {
            loop {
                if let Some(offset) = self.pop_reserve_tower_with_header(header, tower_height) {
                    header
                        .reserve_tower_hits
                        .fetch_add(1, AtomicOrdering::AcqRel);
                    return Some(TowerAlloc {
                        offset,
                        capacity: tower_height,
                    });
                }
                let mask = header
                    .reserve_tower_nonempty_mask
                    .load(AtomicOrdering::Acquire);
                if let Some(next) =
                    Self::first_set_tower_height_at_or_above(mask, tower_height.saturating_add(1))
                {
                    tower_height = next;
                    continue;
                }
                break;
            }
        }
        for tower_height in min_height..=MAX_HEIGHT {
            if let Some(offset) = self.pop_reserve_tower_with_header(header, tower_height) {
                header
                    .reserve_tower_hits
                    .fetch_add(1, AtomicOrdering::AcqRel);
                return Some(TowerAlloc {
                    offset,
                    capacity: tower_height,
                });
            }
        }
        header
            .reserve_tower_misses
            .fetch_add(1, AtomicOrdering::AcqRel);
        None
    }

    fn pop_recycled_tower_at_least(&self, min_height: usize) -> Option<TowerAlloc> {
        if min_height > MAX_HEIGHT {
            return None;
        }
        let header = self.header_ref()?;
        if let Some(mut tower_height) = Self::first_set_tower_height_at_or_above(
            header
                .recycled_tower_nonempty_mask
                .load(AtomicOrdering::Acquire),
            min_height,
        ) {
            loop {
                if let Some(offset) = self.pop_recycled_tower_with_header(header, tower_height) {
                    return Some(TowerAlloc {
                        offset,
                        capacity: tower_height,
                    });
                }
                let mask = header
                    .recycled_tower_nonempty_mask
                    .load(AtomicOrdering::Acquire);
                if let Some(next) =
                    Self::first_set_tower_height_at_or_above(mask, tower_height.saturating_add(1))
                {
                    tower_height = next;
                    continue;
                }
                break;
            }
        }

        for tower_height in min_height..=MAX_HEIGHT {
            if let Some(offset) = self.pop_recycled_tower_with_header(header, tower_height) {
                return Some(TowerAlloc {
                    offset,
                    capacity: tower_height,
                });
            }
        }
        None
    }

    #[inline]
    fn tower_bit(tower_height: usize) -> u64 {
        1_u64 << (tower_height - 1)
    }

    #[inline]
    fn set_tower_mask_bit(mask: &AtomicU64, tower_height: usize) {
        mask.fetch_or(Self::tower_bit(tower_height), AtomicOrdering::AcqRel);
    }

    #[inline]
    fn clear_tower_mask_if_stack_empty(
        mask: &AtomicU64,
        stack_head: &AtomicU64,
        tower_height: usize,
    ) {
        if stack_head_offset(stack_head.load(AtomicOrdering::Acquire)) == NULL_OFFSET {
            mask.fetch_and(!Self::tower_bit(tower_height), AtomicOrdering::AcqRel);
        }
    }

    #[inline]
    fn first_set_tower_height_at_or_above(mask: u64, min_height: usize) -> Option<usize> {
        if min_height == 0 || min_height > MAX_HEIGHT {
            return None;
        }
        let filtered = mask & (!0_u64 << (min_height - 1));
        if filtered == 0 {
            return None;
        }
        Some(filtered.trailing_zeros() as usize + 1)
    }

    #[inline]
    fn pop_reserve_tower_with_header(
        &self,
        header: &ShmSkipHeader<K>,
        tower_height: usize,
    ) -> Option<u32> {
        let stack = &header.reserve_towers[tower_height - 1];
        let out = self.pop_tower_stack(stack, tower_height);
        if out.is_none() {
            Self::clear_tower_mask_if_stack_empty(
                &header.reserve_tower_nonempty_mask,
                stack,
                tower_height,
            );
        } else {
            Self::clear_tower_mask_if_stack_empty(
                &header.reserve_tower_nonempty_mask,
                stack,
                tower_height,
            );
        }
        out
    }

    #[inline]
    fn pop_recycled_tower_with_header(
        &self,
        header: &ShmSkipHeader<K>,
        tower_height: usize,
    ) -> Option<u32> {
        let stack = &header.recycled_towers[tower_height - 1];
        let out = self.pop_tower_stack(stack, tower_height);
        if out.is_none() {
            Self::clear_tower_mask_if_stack_empty(
                &header.recycled_tower_nonempty_mask,
                stack,
                tower_height,
            );
        } else {
            Self::clear_tower_mask_if_stack_empty(
                &header.recycled_tower_nonempty_mask,
                stack,
                tower_height,
            );
        }
        out
    }

    fn node_ptr(&self, offset: u32) -> Result<*mut ShmSkipNode<K>, ShmSkipListError> {
        let start = offset as usize;
        let end = start
            .checked_add(size_of::<ShmSkipNode<K>>())
            .ok_or(ShmSkipListError::InvalidNode(offset))?;
        let base = self.shm.mmap_base();
        if end > base.len() {
            return Err(ShmSkipListError::InvalidNode(offset));
        }
        let ptr = unsafe { base.as_ptr().add(start).cast::<ShmSkipNode<K>>() };
        if (ptr as usize) % align_of::<ShmSkipNode<K>>() != 0 {
            return Err(ShmSkipListError::InvalidNode(offset));
        }
        Ok(ptr)
    }

    fn posting_ptr(&self, offset: u32) -> Result<*mut PostingEntry, ShmSkipListError> {
        let start = offset as usize;
        let end = start
            .checked_add(size_of::<PostingEntry>())
            .ok_or(ShmSkipListError::InvalidPosting(offset))?;
        let base = self.shm.mmap_base();
        if end > base.len() {
            return Err(ShmSkipListError::InvalidPosting(offset));
        }
        let ptr = unsafe { base.as_ptr().add(start).cast::<PostingEntry>() };
        if (ptr as usize) % align_of::<PostingEntry>() != 0 {
            return Err(ShmSkipListError::InvalidPosting(offset));
        }
        Ok(ptr)
    }

    fn recycle_unlinked_insert_node_and_tower(
        &self,
        node_offset: u32,
        tower_offset: u32,
        tower_capacity: usize,
    ) -> Result<(), ShmSkipListError> {
        self.push_recycled_tower(tower_offset, tower_capacity)?;
        self.push_recycled_node(node_offset)?;
        Ok(())
    }

    fn recycle_retired_node(&self, node_offset: u32) -> Result<(), ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;

        let mut posting_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while posting_offset != NULL_OFFSET {
            let posting = self
                .posting_ref(posting_offset)
                .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;
            let next = posting.next.load(AtomicOrdering::Acquire);
            self.push_recycled_posting(posting_offset)?;
            posting_offset = next;
        }

        self.push_recycled_tower(node.tower_offset, node.tower_capacity as usize)?;
        self.push_recycled_node(node_offset)?;
        Ok(())
    }

    fn alloc_fresh_tower(
        &self,
        header: &ShmSkipHeader<K>,
        bytes: usize,
        height: usize,
    ) -> Result<u32, ShmAllocError> {
        let offset = self.shm.chunked_arena().alloc_raw_in_class(
            bytes,
            align_of::<SkipLane<K>>(),
            ArenaClass::SkipTower,
        )?;
        header
            .allocated_towers
            .fetch_add(1, AtomicOrdering::Relaxed);
        header
            .allocated_tower_lanes
            .fetch_add(height as u64, AtomicOrdering::Relaxed);
        Ok(offset)
    }

    fn alloc_tower(
        &self,
        height: usize,
        succs: &[u32; MAX_HEIGHT],
    ) -> Result<TowerAlloc, ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let remaining = self.shm.chunked_arena().remaining_bytes();
        let pressure_state = self.update_pressure_state(header, remaining);
        let bytes = size_of::<SkipLane<K>>()
            .checked_mul(height)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let tower_alloc = if pressure_state != PRESSURE_STATE_NORMAL {
            if let Some(offset) = self.pop_reserve_tower(height) {
                TowerAlloc {
                    offset,
                    capacity: height,
                }
            } else if let Some(offset) = self.pop_recycled_tower(height) {
                TowerAlloc {
                    offset,
                    capacity: height,
                }
            } else if let Some(tower) = self.pop_reserve_tower_at_least(height + 1) {
                tower
            } else if let Some(tower) = self.pop_recycled_tower_at_least(height + 1) {
                tower
            } else {
                match self.alloc_fresh_tower(header, bytes, height) {
                    Ok(offset) => TowerAlloc {
                        offset,
                        capacity: height,
                    },
                    Err(err) => {
                        self.maybe_collect_garbage_on_alloc_failure(header);
                        if let Some(offset) = self.pop_reserve_tower(height) {
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        } else if let Some(offset) = self.pop_recycled_tower(height) {
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        } else if let Some(tower) = self.pop_reserve_tower_at_least(height + 1) {
                            tower
                        } else if let Some(tower) = self.pop_recycled_tower_at_least(height + 1) {
                            tower
                        } else {
                            let _ = err;
                            let offset = self.alloc_fresh_tower(header, bytes, height)?;
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        }
                    }
                }
            }
        } else if let Some(offset) = self.pop_recycled_tower(height) {
            TowerAlloc {
                offset,
                capacity: height,
            }
        } else if let Some(tower) = self.pop_recycled_tower_at_least(height + 1) {
            tower
        } else {
            match self.alloc_fresh_tower(header, bytes, height) {
                Ok(offset) => TowerAlloc {
                    offset,
                    capacity: height,
                },
                Err(err) => {
                    self.maybe_collect_garbage_on_alloc_failure(header);
                    if header.pressure_state.load(AtomicOrdering::Acquire) != PRESSURE_STATE_NORMAL
                    {
                        if let Some(offset) = self.pop_reserve_tower(height) {
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        } else if let Some(offset) = self.pop_recycled_tower(height) {
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        } else if let Some(tower) = self.pop_reserve_tower_at_least(height + 1) {
                            tower
                        } else if let Some(tower) = self.pop_recycled_tower_at_least(height + 1) {
                            tower
                        } else {
                            let _ = err;
                            let offset = self.alloc_fresh_tower(header, bytes, height)?;
                            TowerAlloc {
                                offset,
                                capacity: height,
                            }
                        }
                    } else if let Some(offset) = self.pop_recycled_tower(height) {
                        TowerAlloc {
                            offset,
                            capacity: height,
                        }
                    } else if let Some(tower) = self.pop_recycled_tower_at_least(height + 1) {
                        tower
                    } else {
                        let _ = err;
                        let offset = self.alloc_fresh_tower(header, bytes, height)?;
                        TowerAlloc {
                            offset,
                            capacity: height,
                        }
                    }
                }
            }
        };
        let base = self.shm.mmap_base();
        for level in 0..height {
            let ptr = tower_ptr::<K>(base, tower_alloc.offset, level, tower_alloc.capacity).ok_or(
                ShmSkipListError::InvalidLane {
                    node_offset: NULL_OFFSET,
                    level,
                },
            )?;
            // SAFETY:
            // `alloc_raw` reserved this memory and `tower_ptr` validated bounds/alignment.
            unsafe { ptr.write(SkipLane::new(succs[level])) };
        }
        Ok(tower_alloc)
    }

    fn maybe_raise_height(&self, header: &ShmSkipHeader<K>, node_height: u32) {
        loop {
            let current = header.current_height.load(AtomicOrdering::Acquire);
            if node_height <= current {
                return;
            }
            if header
                .current_height
                .compare_exchange(
                    current,
                    node_height,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    fn random_height(&self, header: &ShmSkipHeader<K>) -> u8 {
        let mut old = header.rng_state.load(AtomicOrdering::Acquire).max(1);
        let mut new;
        loop {
            new = xorshift64(old);
            if header
                .rng_state
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                break;
            }
            old = header.rng_state.load(AtomicOrdering::Acquire).max(1);
        }

        let mut h = 1_u8;
        let mut bits = new;
        while h < MAX_HEIGHT as u8 && (bits & 0b1) == 0 {
            h += 1;
            bits >>= 1;
        }
        h
    }

    #[inline]
    fn header_ref(&self) -> Option<&ShmSkipHeader<K>> {
        RelPtr::<ShmSkipHeader<K>>::from_offset(self.header_offset).as_ref(self.shm.mmap_base())
    }

    #[inline]
    fn node_ref(&self, offset: u32) -> Option<&ShmSkipNode<K>> {
        RelPtr::<ShmSkipNode<K>>::from_offset(offset).as_ref(self.shm.mmap_base())
    }

    #[inline]
    unsafe fn node_ref_unchecked(&self, base: *mut u8, offset: u32) -> &ShmSkipNode<K> {
        // SAFETY:
        // Callers guarantee `offset` is a valid `ShmSkipNode<K>` inside this arena.
        unsafe { &*base.add(offset as usize).cast::<ShmSkipNode<K>>() }
    }

    #[inline]
    fn posting_ref(&self, offset: u32) -> Option<&PostingEntry> {
        RelPtr::<PostingEntry>::from_offset(offset).as_ref(self.shm.mmap_base())
    }

    fn lane_ref_by_offset(
        &self,
        node_offset: u32,
        level: usize,
    ) -> Result<&SkipLane<K>, ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        self.lane_ref(node_offset, node, level)
    }

    #[inline]
    unsafe fn lane_ref_by_offset_unchecked(
        &self,
        base: *mut u8,
        node_offset: u32,
        level: usize,
    ) -> &SkipLane<K> {
        let node = unsafe { self.node_ref_unchecked(base, node_offset) };
        unsafe { self.lane_ref_node_unchecked(base, node, level) }
    }

    fn lane_ref(
        &self,
        node_offset: u32,
        node: &ShmSkipNode<K>,
        level: usize,
    ) -> Result<&SkipLane<K>, ShmSkipListError> {
        if level >= node.height as usize {
            return Err(ShmSkipListError::InvalidLane { node_offset, level });
        }
        lane_from_node::<K>(
            self.shm.mmap_base(),
            node.tower_offset,
            level,
            node.height as usize,
        )
        .ok_or(ShmSkipListError::InvalidLane { node_offset, level })
    }

    #[inline]
    unsafe fn lane_ref_node_unchecked(
        &self,
        base: *mut u8,
        node: &ShmSkipNode<K>,
        level: usize,
    ) -> &SkipLane<K> {
        debug_assert!(level < node.height as usize);
        let lane_offset =
            node.tower_offset
                .wrapping_add((level * size_of::<SkipLane<K>>()) as u32) as usize;
        // SAFETY:
        // Callers guarantee tower offsets/levels are valid and aligned.
        unsafe { &*base.add(lane_offset).cast::<SkipLane<K>>() }
    }

    fn node_next_offset(&self, node_offset: u32, level: usize) -> Result<u32, ShmSkipListError> {
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        let lane = self.lane_ref(node_offset, node, level)?;
        Ok(lane.next.load(AtomicOrdering::Acquire))
    }
}

pub struct ShmSkipListGcDaemon {
    pid: i32,
    shm: Arc<ShmArena>,
    stop_offset: u32,
}

impl ShmSkipListGcDaemon {
    #[inline]
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Request shutdown outside the shared mutation critical section, then reap the child.
    pub fn stop(&self) -> Result<(), ShmSkipListError> {
        self.request_stop()?;
        self.join()
    }

    fn request_stop(&self) -> Result<(), ShmSkipListError> {
        let flag = RelPtr::<AtomicU32>::from_offset(self.stop_offset)
            .as_ref(self.shm.mmap_base())
            .ok_or(ShmSkipListError::InvalidHeader(self.stop_offset))?;
        flag.store(1, AtomicOrdering::Release);
        Ok(())
    }

    pub fn terminate(&self, signal: i32) -> Result<(), ShmSkipListError> {
        if signal == libc::SIGTERM || signal == libc::SIGINT {
            return self.request_stop();
        }
        // Abrupt signals are reserved for crash/recovery tests: they may strand
        // a shared lock, so the arena must be recovered before further use.
        // SAFETY:
        // `pid` came from a successful fork call and belongs to this process group.
        let rc = unsafe { libc::kill(self.pid, signal) };
        if rc == 0 {
            Ok(())
        } else {
            Err(ShmSkipListError::Signal(std::io::Error::last_os_error()))
        }
    }

    pub fn join(&self) -> Result<(), ShmSkipListError> {
        let pid = rustix::process::Pid::from_raw(self.pid).ok_or_else(|| {
            ShmSkipListError::Wait(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid pid for gc daemon",
            ))
        })?;
        let status = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::empty())
            .map_err(|err| {
                ShmSkipListError::Wait(std::io::Error::from_raw_os_error(err.raw_os_error()))
            })?;
        let Some(status) = status else {
            return Err(ShmSkipListError::Wait(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "waitpid returned no status",
            )));
        };

        if status.exited() {
            if status.exit_status() == Some(0) {
                Ok(())
            } else {
                Err(ShmSkipListError::Wait(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("gc daemon exited with status {:?}", status.exit_status()),
                )))
            }
        } else if status.signaled() {
            Ok(())
        } else {
            Err(ShmSkipListError::Wait(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("gc daemon exited unexpectedly: {:?}", status),
            )))
        }
    }
}

fn tower_ptr<K: ShmSkipKey>(
    base: crate::shm::MmapBase<'_>,
    tower_offset: u32,
    level: usize,
    height: usize,
) -> Option<*mut SkipLane<K>> {
    if level >= height {
        return None;
    }
    let lane_size = size_of::<SkipLane<K>>();
    let byte_offset = (tower_offset as usize).checked_add(level.checked_mul(lane_size)?)?;
    let end = byte_offset.checked_add(lane_size)?;
    if end > base.len() {
        return None;
    }
    let addr = (base.as_ptr() as usize).checked_add(byte_offset)?;
    if addr % align_of::<SkipLane<K>>() != 0 {
        return None;
    }
    Some(addr as *mut SkipLane<K>)
}

fn lane_from_node<K: ShmSkipKey>(
    base: crate::shm::MmapBase<'_>,
    tower_offset: u32,
    level: usize,
    height: usize,
) -> Option<&SkipLane<K>> {
    let ptr = tower_ptr::<K>(base, tower_offset, level, height)?;
    // SAFETY:
    // `tower_ptr` validates pointer bounds and alignment against the mapped segment.
    Some(unsafe { &*ptr.cast_const() })
}

#[inline]
fn atomic_max_u64(slot: &AtomicU64, candidate: u64) {
    let mut current = slot.load(AtomicOrdering::Acquire);
    while candidate > current {
        match slot.compare_exchange_weak(
            current,
            candidate,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[inline]
fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    if x == 0 {
        1
    } else {
        x
    }
}

#[cfg(target_os = "linux")]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> Result<(), ShmSkipListError> {
    // SAFETY:
    // called immediately in the fork child before creating new threads.
    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if rc != 0 {
        return Err(ShmSkipListError::Fork(std::io::Error::last_os_error()));
    }

    // SAFETY:
    // getppid is async-signal-safe and used for race-checking parent liveness.
    let observed_parent = unsafe { libc::getppid() };
    if observed_parent != expected_parent {
        return Err(ShmSkipListError::Fork(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "parent exited before gc daemon armed PDEATHSIG",
        )));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn arm_parent_death_signal(_expected_parent: libc::pid_t) -> Result<(), ShmSkipListError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(i64);

    impl ShmSkipKey for TestKey {
        fn sentinel() -> Self {
            TestKey(i64::MIN)
        }

        fn cmp_key(&self, other: &Self) -> Ordering {
            self.0.cmp(&other.0)
        }
    }

    fn make_list() -> ShmSkipList<TestKey> {
        let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to allocate test shm"));
        ShmSkipList::<TestKey>::new_in_shared(shm).expect("failed to build test skiplist")
    }

    fn top_height(list: &ShmSkipList<TestKey>) -> usize {
        let header = list.header_ref().expect("missing skiplist header");
        header
            .current_height
            .load(AtomicOrdering::Acquire)
            .clamp(1, MAX_HEIGHT as u32) as usize
    }

    fn level_entries(list: &ShmSkipList<TestKey>, level: usize) -> Vec<(u32, i64, u32, u8)> {
        let header = list.header_ref().expect("missing skiplist header");
        let head = header.head.load(AtomicOrdering::Acquire);
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        let mut curr = list
            .lane_ref_by_offset(head, level)
            .expect("invalid head lane")
            .next
            .load(AtomicOrdering::Acquire);
        while curr != NULL_OFFSET {
            assert!(
                seen.insert(curr),
                "cycle detected in level {} traversal at offset {}",
                level,
                curr
            );
            let node = list
                .node_ref(curr)
                .expect("invalid node in level traversal");
            let flags = node.flags.load(AtomicOrdering::Acquire);
            out.push((curr, node.key.0, flags, node.height));
            curr = list
                .node_next_offset(curr, level)
                .expect("invalid next link in level traversal");
        }
        out
    }

    fn has_visible_key(list: &ShmSkipList<TestKey>, key: i64) -> bool {
        let mut visible = false;
        list.lookup_payloads(&TestKey(key), |_, _| visible = true)
            .expect("lookup failed");
        visible
    }

    // Seed the existing height generator rather than altering published lanes.
    fn insert_with_height(list: &ShmSkipList<TestKey>, key: i64, payload: u32, height: usize) {
        fn undo_left(value: u64, shift: u32) -> u64 {
            let mut restored = value;
            for _ in 0..64 / shift {
                restored = value ^ (restored << shift);
            }
            restored
        }
        fn undo_right(value: u64, shift: u32) -> u64 {
            let mut restored = value;
            for _ in 0..64 / shift {
                restored = value ^ (restored >> shift);
            }
            restored
        }
        assert!((1..=MAX_HEIGHT).contains(&height));
        let random = 1_u64 << (height - 1);
        let seed = undo_left(undo_right(undo_left(random, 17), 7), 13);
        assert_eq!(xorshift64(seed), random);
        list.header_ref()
            .unwrap()
            .rng_state
            .store(seed, AtomicOrdering::Release);
        list.insert_payload(TestKey(key), 4, &payload.to_le_bytes())
            .unwrap();
        let node = list.find_readonly_exact(&TestKey(key)).unwrap().unwrap();
        assert_eq!(list.node_ref(node).unwrap().height as usize, height);
    }

    #[test]
    fn removal_window_detaches_every_lane_before_pinned_retirement_and_shorter_reuse() {
        let list = make_list();
        // Different predecessors and successors at different levels prevent
        // accidentally treating the level-zero window as the whole tower.
        let shapes = [
            (0, MAX_HEIGHT),
            (10, 1),
            (20, 7),
            (30, MAX_HEIGHT),
            (40, 3),
            (50, MAX_HEIGHT),
        ];
        for &(key, height) in &shapes {
            insert_with_height(&list, key, key as u32, height);
        }
        list.insert_payload(TestKey(30), 4, &31_u32.to_le_bytes())
            .unwrap();
        let original_offset = list.find_readonly_exact(&TestKey(30)).unwrap().unwrap();
        let original_tower = list.node_ref(original_offset).unwrap().tower_offset;
        let original_next: Vec<_> = (0..MAX_HEIGHT)
            .map(|level| list.node_next_offset(original_offset, level).unwrap())
            .collect();
        let epoch = ProcArrayEpochGuard::acquire(list.shm.as_ref()).unwrap();

        // Removing one posting must preserve every lane of a still-live key.
        list.remove_payload(&TestKey(30), 4, &31_u32.to_le_bytes())
            .unwrap();
        assert_eq!(list.retired_nodes(), 0);
        for level in 0..MAX_HEIGHT {
            assert!(level_entries(&list, level)
                .iter()
                .any(|(offset, _, _, _)| *offset == original_offset));
        }
        assert_eq!(list.count_payloads(&TestKey(30)).unwrap(), 1);
        list.remove_payload(&TestKey(30), 4, &30_u32.to_le_bytes())
            .unwrap();
        assert_eq!(list.retired_nodes(), 1);
        assert_eq!(list.distinct_key_count(), 5);
        for level in 0..MAX_HEIGHT {
            let expected: Vec<_> = shapes
                .iter()
                .filter(|(key, height)| *key != 30 && *height > level)
                .map(|(key, _)| *key)
                .collect();
            let actual: Vec<_> = level_entries(&list, level)
                .iter()
                .map(|(_, key, _, _)| *key)
                .collect();
            assert_eq!(
                actual, expected,
                "removed node remains reachable in lane {level}"
            );
            assert_eq!(
                list.node_next_offset(original_offset, level).unwrap(),
                original_next[level],
                "a pinned reader's successor changed in lane {level}"
            );
        }
        assert_eq!(list.node_ref(original_offset).unwrap().key, TestKey(30));
        let retired = list.audit_allocations().unwrap();
        assert_eq!(retired.retired.nodes, 1);
        assert_eq!(retired.retired.postings, 2);
        assert_eq!(list.collect_garbage_once(usize::MAX), 0);
        assert_eq!(list.audit_allocations().unwrap(), retired);
        // Removing a missing payload/key cannot enqueue duplicate retirement.
        list.remove_payload(&TestKey(30), 4, &30_u32.to_le_bytes())
            .unwrap();
        assert_eq!(list.audit_allocations().unwrap(), retired);
        drop(epoch);
        assert_eq!(list.collect_garbage_once(usize::MAX), 1);
        assert_eq!(list.retired_nodes(), 0);
        let reclaimed = list.audit_allocations().unwrap();
        assert_eq!(reclaimed.retired, ShmSkipAllocationCounts::default());

        // Reuse that same node and its tall physical tower for a one-lane key.
        // Any stale upper predecessor would now point at a too-short live node.
        insert_with_height(&list, 35, 35, 1);
        let reused = list.find_readonly_exact(&TestKey(35)).unwrap().unwrap();
        assert_eq!(reused, original_offset);
        assert_eq!(list.node_ref(reused).unwrap().tower_offset, original_tower);
        assert_eq!(
            list.node_ref(reused).unwrap().tower_capacity as usize,
            MAX_HEIGHT
        );
        for level in 1..MAX_HEIGHT {
            assert!(!level_entries(&list, level)
                .iter()
                .any(|(offset, _, _, _)| *offset == reused));
        }
        for key in [0, 10, 20, 35, 40, 50] {
            assert_eq!(list.count_payloads(&TestKey(key)).unwrap(), 1);
        }
        assert_eq!(list.count_payloads(&TestKey(30)).unwrap(), 0);
        let reused_audit = list.audit_allocations().unwrap();
        assert_eq!(reused_audit.allocated, reclaimed.allocated);
        // Remove head, tail, middle and the shortened reused tower in turn.
        let mut remaining = vec![0, 10, 20, 35, 40, 50];
        for key in [0, 50, 35, 20, 10, 40] {
            list.remove_payload(&TestKey(key), 4, &(key as u32).to_le_bytes())
                .unwrap();
            remaining.retain(|value| *value != key);
            assert_eq!(
                level_entries(&list, 0)
                    .iter()
                    .map(|(_, key, _, _)| *key)
                    .collect::<Vec<_>>(),
                remaining
            );
            list.audit_allocations().unwrap();
            list.collect_garbage_once(usize::MAX);
            list.audit_allocations().unwrap();
        }
        for level in 0..MAX_HEIGHT {
            assert!(level_entries(&list, level).is_empty());
        }
    }

    #[test]
    fn allocation_audit_accounts_for_churn_retirement_and_reuse() {
        let list = make_list();
        let initial = list.audit_allocations().unwrap();
        assert_eq!(initial.allocated.nodes, 1);
        assert_eq!(initial.allocated.postings, 0);
        assert_eq!(initial.allocated.towers, 1);
        assert_eq!(initial.allocated.tower_lanes, MAX_HEIGHT as u64);
        assert_eq!(initial.allocated, initial.reachable);
        for id in 0..64_u32 {
            list.insert_payload(TestKey((id % 4) as i64), 4, &id.to_le_bytes())
                .unwrap();
        }
        let epoch = ProcArrayEpochGuard::acquire(list.shm.as_ref()).unwrap();
        for id in 0..64_u32 {
            list.move_payload_relink(
                &TestKey((id % 4) as i64),
                TestKey(100 + id as i64),
                4,
                &id.to_le_bytes(),
            )
            .unwrap();
        }
        let pending = list.audit_allocations().unwrap();
        assert_eq!(pending.reachable.nodes, 65); // includes sentinel
        assert_eq!(pending.reachable.postings, 64);
        assert_eq!(pending.retired.nodes, 4);
        assert_eq!(pending.retired.postings, 64); // node-owned + individually retired
        list.collect_garbage_once(usize::MAX);
        assert_eq!(
            list.audit_allocations().unwrap(),
            pending,
            "pinned epoch retains all retired storage"
        );
        drop(epoch);
        list.collect_garbage_once(usize::MAX);
        let reclaimed = list.audit_allocations().unwrap();
        assert_eq!(reclaimed.retired, ShmSkipAllocationCounts::default());
        assert_eq!(reclaimed.allocated, pending.allocated);
        assert_eq!(reclaimed.reusable.nodes, 4);
        assert_eq!(reclaimed.reusable.postings, 64);
        for id in 0..64_u32 {
            list.move_payload_relink(
                &TestKey(100 + id as i64),
                TestKey((id % 4) as i64),
                4,
                &id.to_le_bytes(),
            )
            .unwrap();
            list.collect_garbage_once(usize::MAX);
        }
        let reused = list.audit_allocations().unwrap();
        assert_eq!(reused.reachable.nodes, 5);
        assert_eq!(reused.reachable.postings, 64);
        assert_eq!(reused.retired, ShmSkipAllocationCounts::default());
        assert_eq!(
            reused.allocated.nodes, reclaimed.allocated.nodes,
            "recycled node slots remain owned by this index"
        );
        assert_eq!(reused.allocated.postings, reclaimed.allocated.postings);
    }

    #[test]
    fn allocation_audit_accounts_for_failed_insert_rollbacks() {
        let list = make_list();
        leave_arena_bytes(
            &list,
            size_of::<PostingEntry>() + 64 + size_of::<SkipLane<TestKey>>(),
        );
        let payload = [7_u8; 64];
        for _ in 0..20 {
            list.header_ref()
                .unwrap()
                .rng_state
                .store(1, AtomicOrdering::Relaxed);
            assert!(matches!(
                list.insert_payload(TestKey(1), 64, &payload),
                Err(ShmSkipListError::Alloc(_))
            ));
            let audit = list.audit_allocations().unwrap();
            assert_eq!(audit.reachable.nodes, 1);
            assert_eq!(audit.reachable.postings, 0);
            assert_eq!(audit.reusable.postings, 1);
            assert_eq!(audit.reusable.towers, 1);
            assert_eq!(audit.reusable.tower_lanes, 1);
            assert_eq!(audit.retired, ShmSkipAllocationCounts::default());
        }
    }

    #[test]
    fn allocation_audit_counts_physical_capacity_after_tower_reuse() {
        let list = make_list();
        let succs = [NULL_OFFSET; MAX_HEIGHT];
        let tower = list.alloc_tower(8, &succs).unwrap();
        list.push_recycled_tower(tower.offset, tower.capacity)
            .unwrap();
        let shorter = list.alloc_tower(1, &succs).unwrap();
        assert_eq!(shorter.offset, tower.offset);
        assert_eq!(shorter.capacity, 8);
        list.push_recycled_tower(shorter.offset, shorter.capacity)
            .unwrap();
        let audit = list.audit_allocations().unwrap();
        assert_eq!(audit.allocated.towers, 2);
        assert_eq!(audit.allocated.tower_lanes, MAX_HEIGHT as u64 + 8);
        assert_eq!(audit.reusable.towers, 1);
        assert_eq!(audit.reusable.tower_lanes, 8);
    }

    #[test]
    fn allocation_audit_rejects_an_unreachable_unretired_node() {
        let list = make_list();
        list.header_ref()
            .unwrap()
            .rng_state
            .store(1, AtomicOrdering::Relaxed);
        list.insert_payload(TestKey(1), 1, b"x").unwrap();
        list.audit_allocations().unwrap();
        // Reproduce the leaked ownership state: allocation counters and logical
        // distinct count still include a node that no live/retired/free root owns.
        let head = list
            .header_ref()
            .unwrap()
            .head
            .load(AtomicOrdering::Acquire);
        list.lane_ref_by_offset(head, 0)
            .unwrap()
            .next
            .store(NULL_OFFSET, AtomicOrdering::Release);
        match list.audit_allocations() {
            Err(ShmSkipListError::AllocationAudit(message)) => assert!(
                message.contains("unaccounted structural storage"),
                "{message}"
            ),
            result => panic!("audit accepted an unreachable allocated node: {result:?}"),
        }
    }

    #[test]
    fn allocation_audit_rejects_duplicate_pool_ownership() {
        let list = make_list();
        list.insert_payload(TestKey(1), 1, b"x").unwrap();
        list.remove_payload(&TestKey(1), 1, b"x").unwrap();
        list.collect_garbage_once(usize::MAX);
        list.audit_allocations().unwrap();
        let header = list.header_ref().unwrap();
        let offset = stack_head_offset(header.recycled_nodes.load(AtomicOrdering::Acquire));
        assert_ne!(offset, NULL_OFFSET);
        // The same slot cannot belong to both recycling pools.
        header
            .reserve_nodes
            .store(pack_stack_head(offset, 1), AtomicOrdering::Release);
        assert!(
            matches!(list.audit_allocations(), Err(ShmSkipListError::AllocationAudit(message)) if message.contains("duplicate ownership"))
        );
    }

    #[test]
    fn insert_cannot_publish_through_a_deleted_predecessor() {
        use std::cell::{Cell, RefCell};
        use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
        thread_local! {
            // Skip the duplicate preflight lookup; pause inside the mutating find.
            static SKIP_PREFLIGHT: Cell<bool> = const { Cell::new(false) };
            static PAUSE: RefCell<Option<(SyncSender<()>, Receiver<()>)>> = const { RefCell::new(None) };
        }
        #[derive(Clone, Copy)]
        struct PausingKey(i64);
        impl ShmSkipKey for PausingKey {
            fn sentinel() -> Self {
                Self(i64::MIN)
            }
            fn cmp_key(&self, other: &Self) -> Ordering {
                let result = self.0.cmp(&other.0);
                if self.0 == 1 && other.0 == 2 && !SKIP_PREFLIGHT.with(|skip| skip.replace(false)) {
                    if let Some((observed, resume)) = PAUSE.with(|pause| pause.borrow_mut().take())
                    {
                        observed.send(()).unwrap();
                        resume.recv().unwrap();
                    }
                }
                result
            }
        }
        let arena = Arc::new(ShmArena::new(1 << 20).unwrap());
        let list = ShmSkipList::<PausingKey>::new_in_shared(arena).unwrap();
        list.insert_payload(PausingKey(1), 1, b"a").unwrap();
        let (observed_tx, observed_rx) = sync_channel(0);
        let (resume_tx, resume_rx) = sync_channel(0);
        let inserting = list.clone();
        let insert = std::thread::spawn(move || {
            SKIP_PREFLIGHT.with(|skip| skip.set(true));
            PAUSE.with(|pause| *pause.borrow_mut() = Some((observed_tx, resume_rx)));
            inserting.insert_payload(PausingKey(2), 1, b"b")
        });
        observed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (started_tx, started_rx) = sync_channel(0);
        let (removed_tx, removed_rx) = sync_channel(1);
        let deleting = list.clone();
        let delete = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = deleting.remove_payload(&PausingKey(1), 1, b"a");
            removed_tx.send(()).unwrap();
            result
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // The old protocol completes deletion here, then publishes through the
        // detached node. The guarded protocol prevents that destructive interleave.
        let removed_while_insert_paused =
            removed_rx.recv_timeout(Duration::from_millis(30)).is_ok();
        resume_tx.send(()).unwrap();
        insert.join().unwrap().unwrap();
        delete.join().unwrap().unwrap();
        assert!(
            !removed_while_insert_paused,
            "deletion must wait for predecessor publication"
        );
        list.collect_garbage_once(usize::MAX);
        assert_eq!(list.count_payloads(&PausingKey(2)).unwrap(), 1);
        let mut keys = Vec::new();
        list.scan_payloads_bounded(None, None, |key, _, _| keys.push(key.0))
            .unwrap();
        assert_eq!(keys, vec![2]);
        assert_eq!(list.distinct_key_count(), 1);
        assert_eq!(list.retired_nodes(), 0);
    }

    fn leave_arena_bytes(list: &ShmSkipList<TestKey>, bytes: usize) {
        let remaining = list.shm.chunked_arena().remaining_bytes();
        list.shm
            .chunked_arena()
            .alloc_raw(remaining - bytes, 1)
            .unwrap();
    }

    #[test]
    fn failed_constructor_returns_its_head_node_and_tower() {
        let shm = Arc::new(ShmArena::new(1 << 20).unwrap());
        let arena = shm.chunked_arena();
        let available =
            size_of::<SkipLane<TestKey>>() * MAX_HEIGHT + size_of::<ShmSkipNode<TestKey>>();
        arena
            .alloc_raw(arena.remaining_bytes() - available, 1)
            .unwrap();
        assert!(matches!(
            ShmSkipList::<TestKey>::new_in_shared(Arc::clone(&shm)),
            Err(ShmSkipListError::Alloc(_))
        ));
        let head = arena.head_offset();
        for _ in 0..100 {
            assert!(matches!(
                ShmSkipList::<TestKey>::new_in_shared(Arc::clone(&shm)),
                Err(ShmSkipListError::Alloc(_))
            ));
        }
        assert_eq!(arena.head_offset(), head);
        assert!(arena
            .alloc_raw_in_class(
                size_of::<SkipLane<TestKey>>() * MAX_HEIGHT,
                align_of::<u64>(),
                ArenaClass::SkipTower
            )
            .is_ok());
        assert!(arena
            .alloc_raw_in_class(
                size_of::<ShmSkipNode<TestKey>>(),
                align_of::<ShmSkipNode<TestKey>>(),
                ArenaClass::SkipNode
            )
            .is_ok());
    }

    #[test]
    fn failed_insert_returns_posting_and_tower_for_reuse() {
        let list = make_list();
        let header = list.header_ref().unwrap();
        leave_arena_bytes(
            &list,
            size_of::<PostingEntry>() + size_of::<SkipLane<TestKey>>(),
        );
        header.rng_state.store(1, AtomicOrdering::Relaxed); // one-lane tower
        assert!(matches!(
            list.insert_payload(TestKey(1), 1, b"x"),
            Err(ShmSkipListError::Alloc(_))
        ));
        let head = list.shm.chunked_arena().head_offset();
        for _ in 0..100 {
            header.rng_state.store(1, AtomicOrdering::Relaxed);
            assert!(matches!(
                list.insert_payload(TestKey(1), 1, b"x"),
                Err(ShmSkipListError::Alloc(_))
            ));
        }
        assert_eq!(list.shm.chunked_arena().head_offset(), head);
        let posting = list
            .pop_reserve_posting()
            .or_else(|| list.pop_recycled_posting())
            .expect("failed insert must return its posting");
        let tower = list
            .pop_reserve_tower(1)
            .or_else(|| list.pop_recycled_tower(1))
            .expect("failed node allocation must return its tower");
        assert_ne!(posting, 0);
        assert_ne!(tower, 0);
        assert!(
            list.pop_reserve_posting()
                .or_else(|| list.pop_recycled_posting())
                .is_none(),
            "rollback must not double-free a posting"
        );
        assert_eq!(list.distinct_key_count(), 0);
        assert_eq!(list.retired_nodes(), 0);
    }

    #[test]
    fn failed_spill_allocation_returns_the_reserved_posting_slot() {
        let list = make_list();
        leave_arena_bytes(&list, size_of::<PostingEntry>());
        let payload = [7_u8; 64];
        assert!(matches!(
            list.insert_payload(TestKey(1), 64, &payload),
            Err(ShmSkipListError::Alloc(_))
        ));
        let head = list.shm.chunked_arena().head_offset();
        for _ in 0..100 {
            assert!(matches!(
                list.insert_payload(TestKey(1), 64, &payload),
                Err(ShmSkipListError::Alloc(_))
            ));
        }
        assert_eq!(list.shm.chunked_arena().head_offset(), head);
        let slot = list
            .pop_reserve_posting()
            .or_else(|| list.pop_recycled_posting())
            .expect("spill failure must not consume posting ownership");
        assert_eq!(
            list.posting_ref(slot).unwrap().storage_kind,
            POSTING_STORAGE_INLINE
        );
    }

    #[test]
    fn collector_waits_for_a_busy_list_instead_of_skipping_reclamation() {
        use std::sync::mpsc::sync_channel;
        let list = make_list();
        list.insert_payload(TestKey(1), 1, b"x").unwrap();
        list.remove_payload(&TestKey(1), 1, b"x").unwrap();
        let guard = list.lock_mutation().unwrap();
        let (started_tx, started_rx) = sync_channel(0);
        let (completed_tx, completed_rx) = sync_channel(1);
        let collecting = list.clone();
        let collector = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            completed_tx
                .send(collecting.collect_garbage_once(usize::MAX))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let skipped = completed_rx.recv_timeout(Duration::from_millis(30)).ok();
        drop(guard);
        let reclaimed =
            skipped.unwrap_or_else(|| completed_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        collector.join().unwrap();
        assert_eq!(
            reclaimed, 1,
            "busy writers must not cause a GC wake to be discarded"
        );
        assert_eq!(list.retired_nodes(), 0);
    }

    #[test]
    fn failed_insert_reuses_spill_storage_as_well_as_posting_and_tower() {
        let list = make_list();
        let header = list.header_ref().unwrap();
        leave_arena_bytes(
            &list,
            size_of::<PostingEntry>() + 64 + size_of::<SkipLane<TestKey>>(),
        );
        let payload = [7_u8; 64];
        header.rng_state.store(1, AtomicOrdering::Relaxed);
        assert!(matches!(
            list.insert_payload(TestKey(1), 64, &payload),
            Err(ShmSkipListError::Alloc(_))
        ));
        let head = list.shm.chunked_arena().head_offset();
        for _ in 0..100 {
            header.rng_state.store(1, AtomicOrdering::Relaxed);
            assert!(matches!(
                list.insert_payload(TestKey(1), 64, &payload),
                Err(ShmSkipListError::Alloc(_))
            ));
        }
        assert_eq!(list.shm.chunked_arena().head_offset(), head);
        assert!(
            list.shm
                .chunked_arena()
                .alloc_raw_in_class(64, align_of::<u64>(), ArenaClass::Spill64)
                .is_ok(),
            "failed insert must return its spill allocation"
        );
        assert_eq!(list.distinct_key_count(), 0);
    }

    #[test]
    fn move_upserts_a_missing_source_without_an_external_fallback() {
        let list = make_list();
        assert!(list
            .move_payload_relink(&TestKey(1), TestKey(2), 1, b"x")
            .unwrap());
        assert_eq!(list.count_payloads(&TestKey(1)).unwrap(), 0);
        assert_eq!(list.count_payloads(&TestKey(2)).unwrap(), 1);
        assert!(list
            .move_payload_relink(&TestKey(1), TestKey(2), 1, b"x")
            .unwrap());
        assert_eq!(
            list.count_payloads(&TestKey(2)).unwrap(),
            1,
            "retry must remain idempotent"
        );
        assert!(list
            .move_payload_relink(&TestKey(3), TestKey(3), 1, b"y")
            .unwrap());
        assert_eq!(list.count_payloads(&TestKey(3)).unwrap(), 1);
    }

    #[test]
    fn failed_move_keeps_source_posting_visible() {
        let list = make_list();
        list.insert_payload(TestKey(1), 1, b"x").unwrap();
        leave_arena_bytes(&list, 0);
        assert!(matches!(
            list.move_payload_relink(&TestKey(1), TestKey(2), 1, b"x"),
            Err(ShmSkipListError::Alloc(_))
        ));
        assert_eq!(list.count_payloads(&TestKey(1)).unwrap(), 1);
        assert_eq!(list.count_payloads(&TestKey(2)).unwrap(), 0);
    }

    #[test]
    fn persistent_key_churn_reclaims_individual_postings_after_readers_finish() {
        let list = make_list();
        list.insert_payload(TestKey(1), 1, b"a").unwrap();
        list.insert_payload(TestKey(2), 1, b"b").unwrap();
        list.insert_payload(TestKey(1), 1, b"x").unwrap();
        let epoch = ProcArrayEpochGuard::acquire(list.shm.as_ref()).unwrap();
        let old_node = list
            .node_ref(list.find_readonly_exact(&TestKey(1)).unwrap().unwrap())
            .unwrap();
        let old_posting_offset = old_node.postings_head.load(AtomicOrdering::Acquire);
        let old_posting = list.posting_ref(old_posting_offset).unwrap();
        let old_next = old_posting.next.load(AtomicOrdering::Acquire);
        assert!(list
            .move_payload_relink(&TestKey(1), TestKey(2), 1, b"x")
            .unwrap());
        list.collect_garbage_once(usize::MAX);
        assert_eq!(
            old_posting.next.load(AtomicOrdering::Acquire),
            old_next,
            "retained posting must not point into another key's chain"
        );
        assert_eq!(
            list.header_ref()
                .unwrap()
                .retired_postings
                .load(AtomicOrdering::Acquire),
            1
        );
        drop(epoch);
        list.collect_garbage_once(usize::MAX);
        assert_eq!(
            list.header_ref()
                .unwrap()
                .retired_postings
                .load(AtomicOrdering::Acquire),
            0
        );
        // After warmup, two permanent keys can move a posting indefinitely without
        // retaining tombstones or allocating more storage.
        for _ in 0..2 {
            assert!(list
                .move_payload_relink(&TestKey(2), TestKey(1), 1, b"x")
                .unwrap());
            list.collect_garbage_once(usize::MAX);
            assert!(list
                .move_payload_relink(&TestKey(1), TestKey(2), 1, b"x")
                .unwrap());
            list.collect_garbage_once(usize::MAX);
        }
        let head = list.shm.chunked_arena().head_offset();
        for _ in 0..1_000 {
            assert!(list
                .move_payload_relink(&TestKey(2), TestKey(1), 1, b"x")
                .unwrap());
            list.collect_garbage_once(usize::MAX);
            assert!(list
                .move_payload_relink(&TestKey(1), TestKey(2), 1, b"x")
                .unwrap());
            list.collect_garbage_once(usize::MAX);
        }
        assert_eq!(list.shm.chunked_arena().head_offset(), head);
        assert_eq!(list.count_payloads(&TestKey(1)).unwrap(), 1);
        assert_eq!(list.count_payloads(&TestKey(2)).unwrap(), 2);
        assert_eq!(
            list.header_ref()
                .unwrap()
                .retired_postings
                .load(AtomicOrdering::Acquire),
            0
        );
    }

    #[test]
    fn skiplist_levels_are_sorted_acyclic_and_fully_linked() {
        const KEYS: usize = 512;
        let list = make_list();

        for i in 0..KEYS {
            let key = ((i * 73) % KEYS) as i64;
            let payload = (i as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        let levels = top_height(&list);
        assert!(levels >= 1);
        for level in 0..levels {
            let entries = level_entries(&list, level);
            for pair in entries.windows(2) {
                assert!(
                    pair[0].1 < pair[1].1,
                    "level {} is not strictly sorted: {:?} then {:?}",
                    level,
                    pair[0],
                    pair[1]
                );
            }
            for (_, _, flags, height) in entries {
                assert_eq!(
                    flags & NODE_FLAG_MARKED,
                    0,
                    "visible node in level {} should not be marked",
                    level
                );
                assert_ne!(
                    flags & NODE_FLAG_FULLY_LINKED,
                    0,
                    "visible node in level {} should be fully linked",
                    level
                );
                assert!(
                    level < height as usize,
                    "visible node in level {} has insufficient height {}",
                    level,
                    height
                );
            }
        }

        assert_eq!(
            level_entries(&list, 0).len(),
            KEYS,
            "level 0 should contain one visible node per inserted key"
        );
    }

    #[test]
    fn skiplist_higher_levels_are_subsets_of_lower_levels() {
        const KEYS: usize = 384;
        let list = make_list();

        for i in 0..KEYS {
            let key = ((i * 97) % KEYS) as i64;
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        let levels = top_height(&list);
        assert!(
            levels >= 2,
            "expected promoted levels with this cardinality"
        );

        let mut level_offsets: Vec<HashSet<u32>> = Vec::with_capacity(levels);
        for level in 0..levels {
            let set = level_entries(&list, level)
                .into_iter()
                .map(|(offset, _, _, _)| offset)
                .collect();
            level_offsets.push(set);
        }

        for level in 1..levels {
            for offset in &level_offsets[level] {
                assert!(
                    level_offsets[level - 1].contains(offset),
                    "level {} contains node {} that is missing from level {}",
                    level,
                    offset,
                    level - 1
                );
            }
        }
    }

    #[test]
    fn move_payload_keeps_old_key_immutable_for_pinned_readers() {
        let list = make_list();
        let old_key = TestKey(10);
        let new_key = TestKey(10_000);
        let payload = 1234_u64.to_le_bytes();
        let payload_len = payload.len() as u16;

        list.insert_payload(old_key, payload_len, &payload)
            .expect("insert failed");
        let old_node_offset = list
            .find_readonly_exact(&old_key)
            .expect("find old key failed")
            .expect("old node should exist");
        let reader_epoch = ProcArrayEpochGuard::acquire(list.shm.as_ref()).unwrap();

        assert!(
            list.move_payload_relink(&old_key, new_key, payload_len, &payload)
                .expect("move relink failed"),
            "single-posting absent-target move should succeed"
        );

        assert_eq!(
            list.collect_garbage_once(usize::MAX),
            0,
            "pinned reader retains the old node"
        );
        assert_eq!(
            list.node_ref(old_node_offset).unwrap().key,
            old_key,
            "a retained node key must stay immutable"
        );
        assert_eq!(
            list.count_payloads(&old_key).expect("count old failed"),
            0,
            "old key should be empty after relink move"
        );
        assert_eq!(
            list.count_payloads(&new_key).expect("count new failed"),
            1,
            "new key should contain moved payload"
        );

        let new_node_offset = list
            .find_readonly_exact(&new_key)
            .expect("find new key failed")
            .expect("new node should exist");
        assert_ne!(
            new_node_offset, old_node_offset,
            "a live reader prevents old node reuse"
        );
        drop(reader_epoch);
        assert_eq!(list.collect_garbage_once(usize::MAX), 1);
        assert_eq!(
            list.distinct_key_count(),
            1,
            "distinct key count should remain stable when moving between absent keys"
        );
    }

    #[test]
    fn marked_nodes_are_not_visible_after_deletions() {
        const KEYS: usize = 256;
        let list = make_list();

        for key in 0..KEYS as i64 {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        for key in 0..KEYS as i64 {
            if key % 3 == 0 {
                let payload = (key as u32).to_le_bytes();
                list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                    .expect("remove failed");
            }
        }

        for key in 0..KEYS as i64 {
            if key % 3 == 0 {
                assert!(
                    !has_visible_key(&list, key),
                    "removed key {} should not be visible",
                    key
                );
            } else {
                assert!(
                    has_visible_key(&list, key),
                    "live key {} should be visible",
                    key
                );
            }
        }

        let levels = top_height(&list);
        for level in 0..levels {
            let entries = level_entries(&list, level);
            for (_, key, _, _) in entries {
                assert_ne!(
                    key % 3,
                    0,
                    "removed key {} leaked into visible level {} traversal",
                    key,
                    level
                );
            }
        }
    }

    #[test]
    fn seek_ge_returns_exact_gap_and_end_candidates() {
        let list = make_list();
        for key in [10_i64, 20_i64, 30_i64] {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        let key_at = |offset: u32| -> i64 {
            list.node_ref(offset)
                .expect("seek_ge returned invalid node")
                .key
                .0
        };

        let ge_10 = list
            .seek_ge(&TestKey(10))
            .expect("seek_ge(10) failed")
            .load(AtomicOrdering::Acquire);
        assert_ne!(ge_10, NULL_OFFSET, "seek_ge(10) should find first node");
        assert_eq!(key_at(ge_10), 10, "seek_ge(10) should return key 10");

        let ge_25 = list
            .seek_ge(&TestKey(25))
            .expect("seek_ge(25) failed")
            .load(AtomicOrdering::Acquire);
        assert_ne!(ge_25, NULL_OFFSET, "seek_ge(25) should find successor node");
        assert_eq!(key_at(ge_25), 30, "seek_ge(25) should return key 30");

        let ge_40 = list
            .seek_ge(&TestKey(40))
            .expect("seek_ge(40) failed")
            .load(AtomicOrdering::Acquire);
        assert_eq!(
            ge_40, NULL_OFFSET,
            "seek_ge(40) should return NULL when bound exceeds max key"
        );
    }

    #[test]
    fn bounded_scan_gt_starts_at_seek_ge_and_collects_tail_only() {
        const KEYS: i64 = 100;
        let list = make_list();
        for key in 0..KEYS {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        let bound = TestKey(90);
        let seek_offset = list
            .seek_ge(&bound)
            .expect("seek_ge failed")
            .load(AtomicOrdering::Acquire);
        let seek_key = list
            .node_ref(seek_offset)
            .expect("seek_ge should return a node for key 90")
            .key
            .0;
        assert_eq!(seek_key, 90, "seek_ge should land on key 90 exactly");

        let mut keys = Vec::new();
        list.scan_payloads_bounded(Some((&bound, ScanBound::Exclusive)), None, |key, _, _| {
            keys.push(key.0);
        })
        .expect("bounded gt scan failed");

        let expected: Vec<i64> = (91..KEYS).collect();
        assert_eq!(
            keys, expected,
            "GT bounded scan should yield strict tail keys"
        );
    }

    #[test]
    fn bounded_scan_lt_and_lte_apply_strict_early_termination() {
        const KEYS: i64 = 32;
        let list = make_list();
        for key in 0..KEYS {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        let mut lt_keys = Vec::new();
        let lt_bound = TestKey(10);
        list.scan_payloads_bounded(
            None,
            Some((&lt_bound, ScanBound::Exclusive)),
            |key, _, _| {
                lt_keys.push(key.0);
            },
        )
        .expect("bounded lt scan failed");
        assert_eq!(lt_keys, (0..10).collect::<Vec<_>>(), "LT scan mismatch");

        let mut lte_keys = Vec::new();
        let lte_bound = TestKey(10);
        list.scan_payloads_bounded(
            None,
            Some((&lte_bound, ScanBound::Inclusive)),
            |key, _, _| {
                lte_keys.push(key.0);
            },
        )
        .expect("bounded lte scan failed");
        assert_eq!(lte_keys, (0..=10).collect::<Vec<_>>(), "LTE scan mismatch");
    }

    #[test]
    fn seek_ge_and_bounded_scan_stay_correct_with_tombstone_heavy_keys() {
        const KEYS: i64 = 128;
        let list = make_list();

        for key in 0..KEYS {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }

        // Delete 75% of keys so seek/scans are forced through a tombstone-heavy structure.
        for key in 0..KEYS {
            if key % 4 != 0 {
                let payload = (key as u32).to_le_bytes();
                list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                    .expect("remove failed");
            }
        }

        let seek_offset = list
            .seek_ge(&TestKey(50))
            .expect("seek_ge failed for tombstone-heavy lower bound")
            .load(AtomicOrdering::Acquire);
        assert_ne!(
            seek_offset, NULL_OFFSET,
            "seek_ge should return a live successor"
        );
        let seek_key = list
            .node_ref(seek_offset)
            .expect("seek_ge returned invalid node")
            .key
            .0;
        assert_eq!(
            seek_key, 52,
            "seek_ge should skip deleted bound and land at 52"
        );

        let mut tail_keys = Vec::new();
        list.scan_payloads_bounded(
            Some((&TestKey(50), ScanBound::Exclusive)),
            None,
            |key, _, _| {
                tail_keys.push(key.0);
            },
        )
        .expect("bounded GT scan failed under tombstones");
        let expected_tail: Vec<i64> = (0..KEYS).filter(|key| *key > 50 && key % 4 == 0).collect();
        assert_eq!(
            tail_keys, expected_tail,
            "GT bounded scan should return only live tail keys under tombstones"
        );

        let mut window_keys = Vec::new();
        list.scan_payloads_bounded(
            Some((&TestKey(52), ScanBound::Inclusive)),
            Some((&TestKey(80), ScanBound::Exclusive)),
            |key, _, _| {
                window_keys.push(key.0);
            },
        )
        .expect("bounded [52,80) scan failed under tombstones");
        let expected_window: Vec<i64> = (0..KEYS)
            .filter(|key| *key >= 52 && *key < 80 && key % 4 == 0)
            .collect();
        assert_eq!(
            window_keys, expected_window,
            "bounded [52,80) scan mismatch under tombstones"
        );

        let mut mixed_bound_keys = Vec::new();
        list.scan_payloads_bounded(
            Some((&TestKey(52), ScanBound::Exclusive)),
            Some((&TestKey(64), ScanBound::Inclusive)),
            |key, _, _| {
                mixed_bound_keys.push(key.0);
            },
        )
        .expect("bounded (52,64] scan failed under tombstones");
        let expected_mixed: Vec<i64> = (0..KEYS)
            .filter(|key| *key > 52 && *key <= 64 && key % 4 == 0)
            .collect();
        assert_eq!(
            mixed_bound_keys, expected_mixed,
            "bounded (52,64] scan mismatch under tombstones"
        );
    }

    #[test]
    fn gc_unlinks_nodes_from_all_levels_before_reclaim_horizon() {
        const KEYS: usize = 512;
        let list = make_list();
        let blocker = list
            .shared_arena()
            .begin_transaction()
            .expect("failed to start snapshot blocker");

        for key in 0..KEYS as i64 {
            let payload = (key as u32).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }
        for key in 0..KEYS as i64 {
            let payload = (key as u32).to_le_bytes();
            list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                .expect("remove failed");
        }

        let levels = top_height(&list);
        for level in 0..levels {
            assert!(
                level_entries(&list, level).is_empty(),
                "level {} should have no visible nodes after full deletion",
                level
            );
        }

        let retired_before = list.retired_nodes();
        assert!(
            retired_before > 0,
            "expected retired nodes after deleting all inserted keys"
        );
        assert_eq!(
            list.collect_garbage_once(usize::MAX),
            0,
            "GC should not reclaim nodes while blocker is active"
        );

        list.shared_arena()
            .end_transaction(blocker)
            .expect("failed to end snapshot blocker");

        let mut reclaimed = 0_usize;
        for _ in 0..32 {
            reclaimed += list.collect_garbage_once(usize::MAX);
            if list.retired_nodes() == 0 {
                break;
            }
        }
        assert!(
            reclaimed > 0,
            "expected GC to reclaim retired nodes once horizon advanced"
        );
        assert_eq!(
            list.retired_nodes(),
            0,
            "retired queue should drain after horizon advances"
        );
    }

    #[test]
    fn retired_queue_preserves_fifo_order() {
        let list = make_list();
        let keys = [11_i64, 22_i64, 33_i64];

        for key in keys {
            let payload = (key as u64).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }
        for key in keys {
            let payload = (key as u64).to_le_bytes();
            list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                .expect("remove failed");
        }

        let header = list.header_ref().expect("missing skiplist header");
        let mut seen = Vec::new();
        let mut current = header.retired_head.load(AtomicOrdering::Acquire);
        while current != NULL_OFFSET {
            let node = list
                .node_ref(current)
                .expect("retired queue contains invalid node offset");
            seen.push(node.key.0);
            current = node.retire_next.load(AtomicOrdering::Acquire);
        }

        assert_eq!(
            seen,
            keys.to_vec(),
            "retired queue must preserve FIFO retire ordering"
        );

        let tail_offset = header.retired_tail.load(AtomicOrdering::Acquire);
        let tail_key = list
            .node_ref(tail_offset)
            .expect("retired tail offset should resolve")
            .key
            .0;
        assert_eq!(tail_key, keys[keys.len() - 1]);
    }

    #[test]
    fn gc_rotates_unreclaimable_head_and_preserves_backlog() {
        let list = make_list();
        let blocker = list
            .shared_arena()
            .begin_transaction()
            .expect("failed to start blocker tx");

        for key in 0..8_i64 {
            let payload = (key as u64).to_le_bytes();
            list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                .expect("insert failed");
        }
        for key in 0..8_i64 {
            let payload = (key as u64).to_le_bytes();
            list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                .expect("remove failed");
        }

        let retired_before = list.retired_nodes();
        assert!(retired_before > 0, "expected retired nodes before GC");
        let telemetry_before = list.mutation_telemetry();
        assert_eq!(
            list.collect_garbage_once(usize::MAX),
            0,
            "GC should not reclaim while blocker keeps horizon behind retired head",
        );
        let telemetry_after = list.mutation_telemetry();
        assert_eq!(
            list.retired_nodes(),
            retired_before,
            "retired backlog should remain unchanged when horizon blocks reclamation",
        );
        let examined_delta = telemetry_after.gc_nodes_examined - telemetry_before.gc_nodes_examined;
        assert!(
            examined_delta >= 1 && examined_delta <= retired_before,
            "GC should examine at most one full blocked queue rotation when head nodes are not reclaimable",
        );
        assert!(
            telemetry_after.gc_nodes_requeued > telemetry_before.gc_nodes_requeued,
            "GC should rotate unreclaimable head nodes to allow progress on later eligible entries",
        );

        list.shared_arena()
            .end_transaction(blocker)
            .expect("failed to end blocker tx");
    }

    #[test]
    fn pressure_state_transitions_use_hysteresis() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let (warm_enter, warm_exit, hot_enter, hot_exit) =
            ShmSkipList::<TestKey>::pressure_thresholds(list.shared_arena().len());

        assert!(warm_exit >= warm_enter);
        assert!(hot_exit >= hot_enter);

        let initial = list.update_pressure_state(header, warm_exit.saturating_add(1));
        assert_eq!(initial, PRESSURE_STATE_NORMAL);

        let warm = list.update_pressure_state(header, warm_enter);
        assert_eq!(warm, PRESSURE_STATE_WARM);

        let hot = list.update_pressure_state(header, hot_enter);
        assert_eq!(hot, PRESSURE_STATE_HOT);

        let cooled = list.update_pressure_state(header, hot_exit.saturating_add(1));
        assert_eq!(cooled, PRESSURE_STATE_WARM);

        let normal = list.update_pressure_state(header, warm_exit.saturating_add(1));
        assert_eq!(normal, PRESSURE_STATE_NORMAL);

        let telemetry = list.mutation_telemetry();
        assert!(telemetry.pressure_to_warm >= 1);
        assert!(telemetry.pressure_to_hot >= 1);
        assert!(telemetry.pressure_to_normal >= 1);
    }

    #[test]
    fn alloc_posting_prefers_reserve_pool_offsets() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let payload_a = 111_u32.to_le_bytes();
        let payload_b = 222_u32.to_le_bytes();

        let offset = list
            .alloc_posting_entry(payload_a.len() as u16, &payload_a)
            .expect("first posting alloc failed");
        list.push_reserve_posting(header, offset)
            .expect("reserve push failed");

        let (_, _, hot_enter, _) = ShmSkipList::<TestKey>::pressure_thresholds(list.shm.len());
        while list.shm.chunked_arena().remaining_bytes() > hot_enter {
            let _ = list
                .shm
                .chunked_arena()
                .alloc_raw(4096, align_of::<u64>())
                .expect("failed to force pressure state into HOT");
        }

        let reused = list
            .alloc_posting_entry(payload_b.len() as u16, &payload_b)
            .expect("second posting alloc failed");
        assert_eq!(reused, offset, "reserve pool offset should be reused first");

        let telemetry = list.mutation_telemetry();
        assert!(telemetry.reserve_posting_pushes >= 1);
        assert!(telemetry.reserve_posting_hits >= 1);
    }

    #[test]
    fn alloc_node_prefers_reserve_node_offsets_under_pressure() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let payload_a = 111_u32.to_le_bytes();
        let payload_b = 222_u32.to_le_bytes();
        let succs = [NULL_OFFSET; MAX_HEIGHT];

        let posting_a = list
            .alloc_posting_entry(payload_a.len() as u16, &payload_a)
            .expect("alloc posting a failed");
        let tower_a = list.alloc_tower(1, &succs).expect("alloc tower a failed");
        let node_a = list
            .alloc_node(
                TestKey(10),
                1,
                tower_a.capacity as u8,
                tower_a.offset,
                posting_a,
            )
            .expect("alloc node a failed");
        list.push_reserve_node(header, node_a)
            .expect("reserve node push failed");

        let (_, _, hot_enter, _) = ShmSkipList::<TestKey>::pressure_thresholds(list.shm.len());
        while list.shm.chunked_arena().remaining_bytes() > hot_enter {
            let _ = list
                .shm
                .chunked_arena()
                .alloc_raw(4096, align_of::<u64>())
                .expect("failed to force pressure state into HOT");
        }

        let posting_b = list
            .alloc_posting_entry(payload_b.len() as u16, &payload_b)
            .expect("alloc posting b failed");
        let tower_b = list.alloc_tower(1, &succs).expect("alloc tower b failed");
        let node_b = list
            .alloc_node(
                TestKey(20),
                1,
                tower_b.capacity as u8,
                tower_b.offset,
                posting_b,
            )
            .expect("alloc node b failed");
        assert_eq!(node_b, node_a, "reserve node offset should be reused first");

        let telemetry = list.mutation_telemetry();
        assert!(telemetry.reserve_node_pushes >= 1);
        assert!(telemetry.reserve_node_hits >= 1);
    }

    #[test]
    fn alloc_tower_prefers_reserve_tower_and_can_reuse_taller_tower() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let succs = [NULL_OFFSET; MAX_HEIGHT];

        let reserved_height = 3_usize;
        let reused_for_height = 2_usize;
        let tower = list
            .alloc_tower(reserved_height, &succs)
            .expect("alloc reserve tower failed");
        list.push_reserve_tower(header, tower.offset, reserved_height)
            .expect("reserve tower push failed");

        let (_, _, hot_enter, _) = ShmSkipList::<TestKey>::pressure_thresholds(list.shm.len());
        while list.shm.chunked_arena().remaining_bytes() > hot_enter {
            let _ = list
                .shm
                .chunked_arena()
                .alloc_raw(4096, align_of::<u64>())
                .expect("failed to force pressure state into HOT");
        }

        let reused = list
            .alloc_tower(reused_for_height, &succs)
            .expect("alloc tower under pressure failed");
        assert_eq!(
            reused.offset, tower.offset,
            "tower allocator should reuse taller reserve tower when exact height missing"
        );
        assert_eq!(
            reused.capacity, reserved_height,
            "tower allocator should retain physical capacity metadata when reusing taller towers"
        );

        let telemetry = list.mutation_telemetry();
        assert!(telemetry.reserve_tower_pushes >= 1);
        assert!(telemetry.reserve_tower_hits >= 1);
    }

    #[test]
    fn reserve_tower_mask_tracks_nonempty_heights_and_bounds_miss_count() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let succs = [NULL_OFFSET; MAX_HEIGHT];
        let tower_height = 3_usize;
        let tower = list
            .alloc_tower(tower_height, &succs)
            .expect("alloc reserve tower failed");
        list.push_reserve_tower(header, tower.offset, tower_height)
            .expect("reserve tower push failed");

        let bit = 1_u64 << (tower_height - 1);
        assert_ne!(
            header
                .reserve_tower_nonempty_mask
                .load(AtomicOrdering::Acquire)
                & bit,
            0,
            "reserve tower nonempty mask should set the pushed tower height bit"
        );

        let before = list.mutation_telemetry();
        let reused = list
            .pop_reserve_tower_at_least(2)
            .expect("expected reserve tower candidate at-or-above requested height");
        assert_eq!(reused.offset, tower.offset);
        assert_eq!(reused.capacity, tower_height);
        let after_hit = list.mutation_telemetry();
        assert!(after_hit.reserve_tower_hits >= before.reserve_tower_hits + 1);
        assert_eq!(after_hit.reserve_tower_misses, before.reserve_tower_misses);

        let none = list.pop_reserve_tower_at_least(2);
        assert!(none.is_none(), "reserve stack should now be empty");
        let after_miss = list.mutation_telemetry();
        assert!(
            after_miss.reserve_tower_misses <= after_hit.reserve_tower_misses + 1,
            "at-least search should record at most one miss when the reserve tower pool is empty"
        );
        assert_eq!(
            header
                .reserve_tower_nonempty_mask
                .load(AtomicOrdering::Acquire)
                & bit,
            0,
            "reserve tower nonempty mask should clear once the stack is empty"
        );
    }

    #[test]
    fn pressure_state_forces_hot_when_reclaim_efficiency_collapses() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let (_, warm_exit, _, _) = ShmSkipList::<TestKey>::pressure_thresholds(list.shm.len());

        header
            .pressure_state
            .store(PRESSURE_STATE_NORMAL, AtomicOrdering::Release);
        header
            .alloc_failure_events
            .store(128, AtomicOrdering::Release);
        header.gc_assist_reclaimed.store(0, AtomicOrdering::Release);

        let state = list.update_pressure_state(header, warm_exit.saturating_add(1024));
        assert_eq!(
            state, PRESSURE_STATE_HOT,
            "efficiency collapse should force HOT even with generous remaining bytes"
        );

        let telemetry = list.mutation_telemetry();
        assert!(telemetry.pressure_to_hot >= 1);
    }

    #[test]
    fn pressure_state_hot_needs_healthy_windows_to_return_normal() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let (_, warm_exit, _, hot_exit) =
            ShmSkipList::<TestKey>::pressure_thresholds(list.shm.len());
        let remaining = warm_exit.saturating_add(1).max(hot_exit.saturating_add(1));

        header
            .pressure_state
            .store(PRESSURE_STATE_HOT, AtomicOrdering::Release);
        header
            .alloc_failure_events
            .store(PRESSURE_WINDOW_MIN_FAILURES * 2, AtomicOrdering::Release);
        header.gc_assist_reclaimed.store(0, AtomicOrdering::Release);
        header
            .gc_daemon_reclaimed
            .store(PRESSURE_WINDOW_MIN_FAILURES * 2, AtomicOrdering::Release);
        header
            .pressure_window_last_failures
            .store(PRESSURE_WINDOW_MIN_FAILURES, AtomicOrdering::Release);
        header
            .pressure_window_last_reclaimed
            .store(PRESSURE_WINDOW_MIN_FAILURES, AtomicOrdering::Release);
        header
            .pressure_window_consecutive_healthy
            .store(0, AtomicOrdering::Release);

        let warm_only = list.update_pressure_state(header, remaining);
        assert_eq!(
            warm_only, PRESSURE_STATE_WARM,
            "HOT should de-escalate only to WARM until enough healthy windows accumulate"
        );

        header
            .pressure_state
            .store(PRESSURE_STATE_HOT, AtomicOrdering::Release);
        header
            .pressure_window_consecutive_healthy
            .store(PRESSURE_HEALTHY_WINDOWS_REQUIRED, AtomicOrdering::Release);
        let normal = list.update_pressure_state(header, remaining);
        assert_eq!(
            normal, PRESSURE_STATE_NORMAL,
            "HOT should return to NORMAL when healthy efficiency has persisted long enough"
        );
    }

    #[test]
    fn maybe_collect_garbage_on_alloc_failure_updates_failure_and_gc_counters() {
        let list = make_list();
        let header = list.header_ref().expect("missing skiplist header");
        let before = list.mutation_telemetry();

        for _ in 0..GC_ASSIST_FAILURE_CADENCE_NORMAL {
            list.maybe_collect_garbage_on_alloc_failure(header);
        }

        let after = list.mutation_telemetry();
        assert!(
            after.alloc_failure_events
                >= before.alloc_failure_events + GC_ASSIST_FAILURE_CADENCE_NORMAL,
            "alloc failure counter should increase with failure handling invocations"
        );
        assert!(
            after.gc_assist_calls >= before.gc_assist_calls + 1,
            "gc assist should be triggered at least once at normal cadence"
        );
        assert!(
            after.gc_assist_reclaimed >= before.gc_assist_reclaimed,
            "reclaimed counter must remain monotonic"
        );
    }

    #[test]
    fn distinct_key_count_tracks_live_keys_with_duplicate_postings() {
        let list = make_list();
        let payload_a = 11_u32.to_le_bytes();
        let payload_b = 22_u32.to_le_bytes();

        list.insert_payload(TestKey(42), payload_a.len() as u16, &payload_a)
            .expect("first insert failed");
        assert_eq!(list.distinct_key_count(), 1);

        list.insert_payload(TestKey(42), payload_b.len() as u16, &payload_b)
            .expect("duplicate-key insert failed");
        assert_eq!(
            list.distinct_key_count(),
            1,
            "duplicate postings for a key must not increase distinct count"
        );

        list.insert_payload(TestKey(7), payload_a.len() as u16, &payload_a)
            .expect("second-key insert failed");
        assert_eq!(list.distinct_key_count(), 2);

        list.remove_payload(&TestKey(42), payload_a.len() as u16, &payload_a)
            .expect("first posting remove failed");
        assert_eq!(
            list.distinct_key_count(),
            2,
            "distinct count should remain while key still has a live posting"
        );

        list.remove_payload(&TestKey(42), payload_b.len() as u16, &payload_b)
            .expect("last posting remove failed");
        assert_eq!(
            list.distinct_key_count(),
            1,
            "distinct count should drop once last posting is removed"
        );

        list.remove_payload(&TestKey(7), payload_a.len() as u16, &payload_a)
            .expect("final key remove failed");
        assert_eq!(list.distinct_key_count(), 0);
    }

    #[test]
    fn distinct_key_count_remains_stable_across_churn_cycles() {
        const KEYS: i64 = 64;
        const CYCLES: usize = 6;
        let list = make_list();

        for cycle in 0..CYCLES {
            for key in 0..KEYS {
                let payload = ((cycle as u64) << 32 | key as u64).to_le_bytes();
                list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                    .expect("insert in churn cycle failed");
            }
            assert_eq!(
                list.distinct_key_count(),
                KEYS as usize,
                "distinct key count should match key cardinality after first insert phase"
            );

            for key in 0..KEYS {
                let payload = (((cycle + 1) as u64) << 32 | key as u64).to_le_bytes();
                list.insert_payload(TestKey(key), payload.len() as u16, &payload)
                    .expect("duplicate insert in churn cycle failed");
            }
            assert_eq!(
                list.distinct_key_count(),
                KEYS as usize,
                "adding duplicate postings should not increase distinct key count"
            );

            for key in 0..KEYS {
                let payload = ((cycle as u64) << 32 | key as u64).to_le_bytes();
                list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                    .expect("remove of first posting in churn cycle failed");
            }
            assert_eq!(
                list.distinct_key_count(),
                KEYS as usize,
                "removing only one posting per key should retain key visibility"
            );

            for key in 0..KEYS {
                let payload = (((cycle + 1) as u64) << 32 | key as u64).to_le_bytes();
                list.remove_payload(&TestKey(key), payload.len() as u16, &payload)
                    .expect("remove of last posting in churn cycle failed");
            }
            assert_eq!(
                list.distinct_key_count(),
                0,
                "distinct key count should return to zero after full cycle teardown"
            );
        }
    }

    #[test]
    fn count_payloads_matches_lookup_payloads() {
        let list = make_list();
        let a = 1_u32.to_le_bytes();
        let b = 2_u32.to_le_bytes();
        let c = 3_u32.to_le_bytes();

        list.insert_payload(TestKey(10), a.len() as u16, &a)
            .expect("insert a failed");
        list.insert_payload(TestKey(10), b.len() as u16, &b)
            .expect("insert b failed");
        list.insert_payload(TestKey(10), c.len() as u16, &c)
            .expect("insert c failed");
        list.remove_payload(&TestKey(10), b.len() as u16, &b)
            .expect("remove b failed");

        let mut via_lookup = 0_usize;
        list.lookup_payloads(&TestKey(10), |_, _| via_lookup += 1)
            .expect("lookup failed");
        let via_count = list.count_payloads(&TestKey(10)).expect("count failed");
        assert_eq!(via_count, via_lookup);
        assert_eq!(via_count, 2);

        let missing = list
            .count_payloads(&TestKey(999))
            .expect("count on missing key failed");
        assert_eq!(missing, 0);
    }

    #[test]
    fn spill_payloads_round_trip_and_remove_cleanly() {
        let list = make_list();
        let key = TestKey(77);
        let payload = vec![0xAB_u8; 96];

        list.insert_payload(key, payload.len() as u16, payload.as_slice())
            .expect("spill insert failed");

        let mut observed = Vec::new();
        list.lookup_payloads(&key, |len, bytes| {
            observed.push((len, bytes.to_vec()));
        })
        .expect("spill lookup failed");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0 as usize, payload.len());
        assert_eq!(observed[0].1, payload);

        list.remove_payload(&key, payload.len() as u16, payload.as_slice())
            .expect("spill remove failed");
        assert_eq!(
            list.count_payloads(&key)
                .expect("count after remove failed"),
            0
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn bounded_scan_matches_btreeset_model(
            keys in prop::collection::btree_set(-256_i16..256_i16, 1..96),
            lower in -300_i16..300_i16,
            upper in -300_i16..300_i16,
            include_lower in any::<bool>(),
            include_upper in any::<bool>(),
            has_lower in any::<bool>(),
            has_upper in any::<bool>(),
        ) {
            let list = make_list();
            for key in &keys {
                let payload = (*key as i32).to_le_bytes();
                list.insert_payload(TestKey(i64::from(*key)), payload.len() as u16, &payload)
                    .expect("insert failed");
            }

            let lower_key = TestKey(i64::from(lower));
            let upper_key = TestKey(i64::from(upper));
            let lower_bound = if has_lower {
                Some((&lower_key, if include_lower { ScanBound::Inclusive } else { ScanBound::Exclusive }))
            } else {
                None
            };
            let upper_bound = if has_upper {
                Some((&upper_key, if include_upper { ScanBound::Inclusive } else { ScanBound::Exclusive }))
            } else {
                None
            };

            let mut actual = Vec::new();
            list.scan_payloads_bounded(lower_bound, upper_bound, |key, _, _| actual.push(key.0))
                .expect("bounded scan failed");

            let expected: Vec<i64> = keys
                .iter()
                .map(|v| i64::from(*v))
                .filter(|key| {
                    let lower_ok = if has_lower {
                        if include_lower { *key >= i64::from(lower) } else { *key > i64::from(lower) }
                    } else {
                        true
                    };
                    let upper_ok = if has_upper {
                        if include_upper { *key <= i64::from(upper) } else { *key < i64::from(upper) }
                    } else {
                        true
                    };
                    lower_ok && upper_ok
                })
                .collect();
            prop_assert_eq!(actual, expected);
        }
    }
}
