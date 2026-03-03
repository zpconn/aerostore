use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use crate::procarray::{ProcArrayError, ProcArrayRegistration};
use crate::shm::{RelPtr, ShmAllocError, ShmArena};

const NULL_OFFSET: u32 = 0;
pub const MAX_HEIGHT: usize = 32;
pub const MAX_PAYLOAD_BYTES: usize = 192;
const GC_MIN_BATCH: usize = 1_024;
const GC_MAX_BATCH: usize = 65_536;
const GC_MAX_PASSES_PER_WAKE: usize = 8;

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
    current_height: AtomicU32,
    rng_state: AtomicU64,
    distinct_key_count: AtomicUsize,
    has_tombstones: AtomicU32,
    recycled_towers: [AtomicU64; MAX_HEIGHT],
    recycled_nodes: AtomicU64,
    recycled_postings: AtomicU64,
    retired_head: RelPtr<ShmSkipNode<K>>,
    retired_nodes: AtomicU64,
    reclaimed_nodes: AtomicU64,
    retry_insert_ops: AtomicU64,
    retry_remove_ops: AtomicU64,
    retry_loops: AtomicU64,
    retry_alloc: AtomicU64,
    retry_structural: AtomicU64,
    retry_epoch: AtomicU64,
    retry_max_insert_attempts: AtomicU64,
    retry_max_remove_attempts: AtomicU64,
    gc_daemon_pid: AtomicI32,
}

impl<K: ShmSkipKey> ShmSkipHeader<K> {
    #[inline]
    fn new(head_offset: u32, seed: u64) -> Self {
        Self {
            head: RelPtr::from_offset(head_offset),
            current_height: AtomicU32::new(1),
            rng_state: AtomicU64::new(seed.max(1)),
            distinct_key_count: AtomicUsize::new(0),
            has_tombstones: AtomicU32::new(0),
            recycled_towers: std::array::from_fn(|_| AtomicU64::new(0)),
            recycled_nodes: AtomicU64::new(0),
            recycled_postings: AtomicU64::new(0),
            retired_head: RelPtr::null(),
            retired_nodes: AtomicU64::new(0),
            reclaimed_nodes: AtomicU64::new(0),
            retry_insert_ops: AtomicU64::new(0),
            retry_remove_ops: AtomicU64::new(0),
            retry_loops: AtomicU64::new(0),
            retry_alloc: AtomicU64::new(0),
            retry_structural: AtomicU64::new(0),
            retry_epoch: AtomicU64::new(0),
            retry_max_insert_attempts: AtomicU64::new(0),
            retry_max_remove_attempts: AtomicU64::new(0),
            gc_daemon_pid: AtomicI32::new(0),
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
    _pad: [u8; 3],
    flags: AtomicU32,
    tower_offset: u32,
    postings_head: RelPtr<PostingEntry>,
    live_postings: AtomicU32,
    retire_txid: AtomicU64,
    retire_next: RelPtr<ShmSkipNode<K>>,
}

impl<K: ShmSkipKey> ShmSkipNode<K> {
    #[inline]
    fn new(key: K, height: u8, tower_offset: u32, postings_head: u32, live_postings: u32) -> Self {
        Self {
            key,
            height,
            _pad: [0_u8; 3],
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
    _pad: [u8; 2],
    deleted: AtomicU32,
    next: RelPtr<PostingEntry>,
    payload: [u8; MAX_PAYLOAD_BYTES],
}

impl PostingEntry {
    #[inline]
    fn new(payload_len: u16, payload: &[u8], next: u32) -> Self {
        let mut out = Self {
            len: payload_len,
            _pad: [0_u8; 2],
            deleted: AtomicU32::new(0),
            next: RelPtr::from_offset(next),
            payload: [0_u8; MAX_PAYLOAD_BYTES],
        };
        let len = payload_len as usize;
        out.payload[..len].copy_from_slice(&payload[..len]);
        out
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

impl<K: ShmSkipKey> ShmSkipList<K> {
    pub fn new_in_shared(shm: Arc<ShmArena>) -> Result<Self, ShmSkipListError> {
        let lane_bytes = size_of::<SkipLane<K>>()
            .checked_mul(MAX_HEIGHT)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let tower_offset = shm
            .chunked_arena()
            .alloc_raw(lane_bytes, align_of::<SkipLane<K>>())?;
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

        let head_offset = shm.chunked_arena().alloc(
            ShmSkipNode::new(
                K::sentinel(),
                MAX_HEIGHT as u8,
                tower_offset,
                NULL_OFFSET,
                0,
            )
            .with_flags(NODE_FLAG_FULLY_LINKED),
        )?;
        let head_offset = head_offset.load(AtomicOrdering::Acquire);

        let seed = shm.global_txid().load(AtomicOrdering::Acquire)
            ^ ((head_offset as u64) << 32)
            ^ 0x9E37_79B9_7F4A_7C15;
        let header_offset = shm
            .chunked_arena()
            .alloc(ShmSkipHeader::<K>::new(head_offset, seed))?;

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

    pub fn insert_payload(
        &self,
        key: K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        if payload_len as usize > MAX_PAYLOAD_BYTES || payload.len() < payload_len as usize {
            return Err(ShmSkipListError::PayloadTooLarge {
                len: payload_len as usize,
                max: MAX_PAYLOAD_BYTES,
            });
        }

        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        loop {
            let found = self.find(&key, &mut preds, &mut succs)?;
            if let Some(found_offset) = found {
                let node = self
                    .node_ref(found_offset)
                    .ok_or(ShmSkipListError::InvalidNode(found_offset))?;
                let flags = node.flags.load(AtomicOrdering::Acquire);
                if flags & NODE_FLAG_MARKED != 0 || flags & NODE_FLAG_FULLY_LINKED == 0 {
                    continue;
                }
                self.prepend_posting(node, payload_len, payload)?;
                return Ok(());
            }

            let posting_offset = self.alloc_posting_entry(payload_len, payload)?;

            let header = self
                .header_ref()
                .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
            let node_height = self.random_height(header) as usize;
            let head_offset = header.head.load(AtomicOrdering::Acquire);
            let observed_height = header
                .current_height
                .load(AtomicOrdering::Acquire)
                .clamp(1, MAX_HEIGHT as u32) as usize;
            for level in observed_height..node_height {
                preds[level] = head_offset;
                succs[level] = NULL_OFFSET;
            }

            let tower_offset = self.alloc_tower(node_height, &succs)?;
            let node_offset =
                self.alloc_node(key, node_height as u8, tower_offset, posting_offset)?;

            let pred_lane_0 = self.lane_ref_by_offset(preds[0], 0)?;
            if pred_lane_0
                .next
                .compare_exchange(
                    succs[0],
                    node_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_err()
            {
                self.recycle_unlinked_insert_allocations(
                    node_offset,
                    tower_offset,
                    posting_offset,
                    node_height,
                )?;
                continue;
            }
            header
                .distinct_key_count
                .fetch_add(1, AtomicOrdering::AcqRel);

            for level in 1..node_height {
                loop {
                    let pred_lane = self.lane_ref_by_offset(preds[level], level)?;
                    if pred_lane
                        .next
                        .compare_exchange(
                            succs[level],
                            node_offset,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }

                    let _ = self.find(&key, &mut preds, &mut succs)?;
                    if succs[level] == node_offset {
                        break;
                    }
                }
            }

            let node = self
                .node_ref(node_offset)
                .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
            node.flags
                .fetch_or(NODE_FLAG_FULLY_LINKED, AtomicOrdering::Release);
            self.maybe_raise_height(header, node_height as u32);
            return Ok(());
        }
    }

    pub fn remove_payload(
        &self,
        key: &K,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        if payload_len as usize > MAX_PAYLOAD_BYTES || payload.len() < payload_len as usize {
            return Ok(());
        }

        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        let found = self.find(key, &mut preds, &mut succs)?;
        let Some(node_offset) = found else {
            return Ok(());
        };
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;
        if node.live_postings.load(AtomicOrdering::Acquire) == 0 {
            return Ok(());
        }

        let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while post_offset != NULL_OFFSET {
            let post = self
                .posting_ref(post_offset)
                .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
            if post.deleted.load(AtomicOrdering::Acquire) == 0
                && post.len == payload_len
                && post.payload[..payload_len as usize] == payload[..payload_len as usize]
            {
                if post
                    .deleted
                    .compare_exchange(0, 1, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                    .is_ok()
                {
                    self.mark_tombstone_seen();
                    if self.decrement_live_postings(node) == 0 {
                        self.unlink_node(key, node_offset)?;
                    }
                    return Ok(());
                }
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }

        Ok(())
    }

    pub fn lookup_payloads<F>(&self, key: &K, mut visit: F) -> Result<(), ShmSkipListError>
    where
        F: FnMut(u16, &[u8]),
    {
        let _epoch_guard = ProcArrayEpochGuard::acquire(self.shm.as_ref())?;
        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        let found = self.find(key, &mut preds, &mut succs)?;
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
                let len = post.len as usize;
                if len > MAX_PAYLOAD_BYTES {
                    return Err(ShmSkipListError::InvalidPosting(post_offset));
                }
                visit(post.len, &post.payload[..len]);
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }

        Ok(())
    }

    pub fn count_payloads(&self, key: &K) -> Result<usize, ShmSkipListError> {
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
                        let len = post.len as usize;
                        if len > MAX_PAYLOAD_BYTES {
                            return Err(ShmSkipListError::InvalidPosting(post_offset));
                        }
                        visit(&node.key, post.len, &post.payload[..len]);
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

    pub fn collect_garbage_once(&self, max_nodes: usize) -> usize {
        let Some(header) = self.header_ref() else {
            return 0;
        };
        let snapshot = self.shm.create_snapshot();
        let horizon = snapshot.xmin;

        let detached_head = header
            .retired_head
            .swap(NULL_OFFSET, AtomicOrdering::AcqRel);
        if detached_head == NULL_OFFSET {
            return 0;
        }

        let mut reclaimed = 0_usize;
        let mut curr = detached_head;

        while curr != NULL_OFFSET {
            let Some(node) = self.node_ref(curr) else {
                break;
            };
            let next = node.retire_next.load(AtomicOrdering::Acquire);
            let retire_txid = node.retire_txid.load(AtomicOrdering::Acquire);

            if reclaimed < max_nodes && retire_txid != 0 && retire_txid < horizon {
                reclaimed += 1;
                header.reclaimed_nodes.fetch_add(1, AtomicOrdering::AcqRel);
                header.retired_nodes.fetch_sub(1, AtomicOrdering::AcqRel);
                let _ = self.recycle_retired_node(curr);
            } else {
                loop {
                    let old = header.retired_head.load(AtomicOrdering::Acquire);
                    node.retire_next.store(old, AtomicOrdering::Release);
                    if header
                        .retired_head
                        .compare_exchange(
                            old,
                            curr,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            }
            curr = next;
        }

        reclaimed
    }

    pub fn spawn_gc_daemon(
        &self,
        interval: Duration,
    ) -> Result<ShmSkipListGcDaemon, ShmSkipListError> {
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
                    let batch = backlog.clamp(GC_MIN_BATCH, GC_MAX_BATCH);
                    let mut reclaimed_any = false;
                    for _ in 0..GC_MAX_PASSES_PER_WAKE {
                        let reclaimed = list.collect_garbage_once(batch);
                        if reclaimed == 0 {
                            break;
                        }
                        reclaimed_any = true;
                        if reclaimed < batch {
                            break;
                        }
                    }
                    if reclaimed_any {
                        std::thread::yield_now();
                    } else {
                        std::thread::sleep(interval);
                    }
                }
            }
            rustix::runtime::Fork::Parent(pid) => {
                let pid_raw = pid.as_raw_nonzero().get();
                if let Some(header) = self.header_ref() {
                    header.gc_daemon_pid.store(pid_raw, AtomicOrdering::Release);
                }
                Ok(ShmSkipListGcDaemon { pid: pid_raw })
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
        }
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
                    let curr_lane = match self.lane_ref(curr_offset, curr, level) {
                        Ok(lane) => lane,
                        Err(ShmSkipListError::InvalidLane { .. }) => {
                            // Heal stale upper-lane links (for example, when a node offset is
                            // recycled with a lower height) by skipping this entry at `level`.
                            let fallback_next =
                                self.node_next_offset(curr_offset, 0).unwrap_or(NULL_OFFSET);
                            let pred_lane = self.lane_ref_by_offset(pred_offset, level)?;
                            if pred_lane
                                .next
                                .compare_exchange(
                                    curr_offset,
                                    fallback_next,
                                    AtomicOrdering::AcqRel,
                                    AtomicOrdering::Acquire,
                                )
                                .is_err()
                            {
                                continue 'retry;
                            }
                            curr_offset = fallback_next;
                            continue;
                        }
                        Err(err) => return Err(err),
                    };
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

    fn unlink_node(&self, key: &K, node_offset: u32) -> Result<(), ShmSkipListError> {
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
        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        loop {
            let _ = self.find(key, &mut preds, &mut succs)?;
            let mut detached_all = true;
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
        let retire_txid = self.shm.global_txid().fetch_add(1, AtomicOrdering::AcqRel);
        node.retire_txid.store(retire_txid, AtomicOrdering::Release);

        let Some(header) = self.header_ref() else {
            return;
        };
        header.retired_nodes.fetch_add(1, AtomicOrdering::AcqRel);
        loop {
            let old = header.retired_head.load(AtomicOrdering::Acquire);
            node.retire_next.store(old, AtomicOrdering::Release);
            if header
                .retired_head
                .compare_exchange(
                    old,
                    node_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    fn prepend_posting(
        &self,
        node: &ShmSkipNode<K>,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<(), ShmSkipListError> {
        let posting_offset = self.alloc_posting_entry(payload_len, payload)?;
        let posting = self
            .posting_ref(posting_offset)
            .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;

        loop {
            let old = node.postings_head.load(AtomicOrdering::Acquire);
            posting.next.store(old, AtomicOrdering::Release);
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

    fn alloc_posting_entry(
        &self,
        payload_len: u16,
        payload: &[u8],
    ) -> Result<u32, ShmSkipListError> {
        if let Some(offset) = self.pop_recycled_posting() {
            let ptr = self.posting_ptr(offset)?;
            // SAFETY:
            // `offset` was previously allocated as `PostingEntry` and was popped from the
            // skiplist-local recycled-posting stack, so this write reinitializes owned memory.
            unsafe { ptr.write(PostingEntry::new(payload_len, payload, NULL_OFFSET)) };
            return Ok(offset);
        }

        Ok(self
            .shm
            .chunked_arena()
            .alloc(PostingEntry::new(payload_len, payload, NULL_OFFSET))?
            .load(AtomicOrdering::Acquire))
    }

    fn alloc_node(
        &self,
        key: K,
        height: u8,
        tower_offset: u32,
        posting_offset: u32,
    ) -> Result<u32, ShmSkipListError> {
        if let Some(offset) = self.pop_recycled_node() {
            let ptr = self.node_ptr(offset)?;
            // SAFETY:
            // `offset` was previously allocated as `ShmSkipNode<K>` and was popped from the
            // skiplist-local recycled-node stack, so this write reinitializes owned memory.
            unsafe {
                ptr.write(ShmSkipNode::new(
                    key,
                    height,
                    tower_offset,
                    posting_offset,
                    1,
                ))
            };
            return Ok(offset);
        }

        Ok(self
            .shm
            .chunked_arena()
            .alloc(ShmSkipNode::new(
                key,
                height,
                tower_offset,
                posting_offset,
                1,
            ))?
            .load(AtomicOrdering::Acquire))
    }

    fn push_recycled_posting(&self, posting_offset: u32) -> Result<(), ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let posting = self
            .posting_ref(posting_offset)
            .ok_or(ShmSkipListError::InvalidPosting(posting_offset))?;

        loop {
            let old = header.recycled_postings.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            posting.next.store(old_head, AtomicOrdering::Release);
            let new = pack_stack_head(posting_offset, stack_head_tag(old).wrapping_add(1));
            if header
                .recycled_postings
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn pop_recycled_posting(&self) -> Option<u32> {
        let header = self.header_ref()?;
        loop {
            let old = header.recycled_postings.load(AtomicOrdering::Acquire);
            let head = stack_head_offset(old);
            if head == NULL_OFFSET {
                return None;
            }
            let posting = self.posting_ref(head)?;
            let next = posting.next.load(AtomicOrdering::Acquire);
            let new = pack_stack_head(next, stack_head_tag(old).wrapping_add(1));
            if header
                .recycled_postings
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(head);
            }
            std::hint::spin_loop();
        }
    }

    fn push_recycled_node(&self, node_offset: u32) -> Result<(), ShmSkipListError> {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let node = self
            .node_ref(node_offset)
            .ok_or(ShmSkipListError::InvalidNode(node_offset))?;

        loop {
            let old = header.recycled_nodes.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            node.retire_next.store(old_head, AtomicOrdering::Release);
            let new = pack_stack_head(node_offset, stack_head_tag(old).wrapping_add(1));
            if header
                .recycled_nodes
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn pop_recycled_node(&self) -> Option<u32> {
        let header = self.header_ref()?;
        loop {
            let old = header.recycled_nodes.load(AtomicOrdering::Acquire);
            let head = stack_head_offset(old);
            if head == NULL_OFFSET {
                return None;
            }
            let node = self.node_ref(head)?;
            let next = node.retire_next.load(AtomicOrdering::Acquire);
            let new = pack_stack_head(next, stack_head_tag(old).wrapping_add(1));
            if header
                .recycled_nodes
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(head);
            }
            std::hint::spin_loop();
        }
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
        let lane0_ptr = tower_ptr::<K>(self.shm.mmap_base(), tower_offset, 0, tower_height).ok_or(
            ShmSkipListError::InvalidLane {
                node_offset: NULL_OFFSET,
                level: 0,
            },
        )?;
        // SAFETY:
        // `tower_ptr` validated the pointer bounds and alignment for lane-0.
        let lane0 = unsafe { &*lane0_ptr };

        let head = &header.recycled_towers[tower_height - 1];
        loop {
            let old = head.load(AtomicOrdering::Acquire);
            let old_head = stack_head_offset(old);
            lane0.next.store(old_head, AtomicOrdering::Release);
            lane0.marked.store(0, AtomicOrdering::Release);
            let new = pack_stack_head(tower_offset, stack_head_tag(old).wrapping_add(1));
            if head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }

    fn pop_recycled_tower(&self, tower_height: usize) -> Option<u32> {
        if !(1..=MAX_HEIGHT).contains(&tower_height) {
            return None;
        }
        let header = self.header_ref()?;
        let head = &header.recycled_towers[tower_height - 1];
        loop {
            let old = head.load(AtomicOrdering::Acquire);
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
            if head
                .compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
                .is_ok()
            {
                return Some(tower_offset);
            }
            std::hint::spin_loop();
        }
    }

    fn pop_recycled_tower_at_least(&self, min_height: usize) -> Option<u32> {
        if min_height > MAX_HEIGHT {
            return None;
        }

        for tower_height in min_height..=MAX_HEIGHT {
            if let Some(offset) = self.pop_recycled_tower(tower_height) {
                return Some(offset);
            }
        }
        None
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

    fn recycle_unlinked_insert_allocations(
        &self,
        node_offset: u32,
        tower_offset: u32,
        posting_offset: u32,
        node_height: usize,
    ) -> Result<(), ShmSkipListError> {
        self.push_recycled_posting(posting_offset)?;
        self.push_recycled_tower(tower_offset, node_height)?;
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

        self.push_recycled_tower(node.tower_offset, node.height as usize)?;
        self.push_recycled_node(node_offset)?;
        Ok(())
    }

    fn alloc_tower(
        &self,
        height: usize,
        succs: &[u32; MAX_HEIGHT],
    ) -> Result<u32, ShmSkipListError> {
        let bytes = size_of::<SkipLane<K>>()
            .checked_mul(height)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let tower_offset = if let Some(offset) = self.pop_recycled_tower(height) {
            offset
        } else if let Some(offset) = self.pop_recycled_tower_at_least(height + 1) {
            offset
        } else {
            self.shm
                .chunked_arena()
                .alloc_raw(bytes, align_of::<SkipLane<K>>())?
        };
        let base = self.shm.mmap_base();
        for level in 0..height {
            let ptr = tower_ptr::<K>(base, tower_offset, level, height).ok_or(
                ShmSkipListError::InvalidLane {
                    node_offset: NULL_OFFSET,
                    level,
                },
            )?;
            // SAFETY:
            // `alloc_raw` reserved this memory and `tower_ptr` validated bounds/alignment.
            unsafe { ptr.write(SkipLane::new(succs[level])) };
        }
        Ok(tower_offset)
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
}

impl ShmSkipListGcDaemon {
    #[inline]
    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn terminate(&self, signal: i32) -> Result<(), ShmSkipListError> {
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
