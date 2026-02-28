use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use crate::shm::{RelPtr, ShmAllocError, ShmArena};

const NULL_OFFSET: u32 = 0;
pub const MAX_HEIGHT: usize = 32;
pub const MAX_PAYLOAD_BYTES: usize = 192;

const NODE_FLAG_MARKED: u32 = 1 << 0;
const NODE_FLAG_FULLY_LINKED: u32 = 1 << 1;

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

#[repr(C, align(64))]
struct ShmSkipHeader<K: ShmSkipKey> {
    head: RelPtr<ShmSkipNode<K>>,
    current_height: AtomicU32,
    rng_state: AtomicU64,
    distinct_key_count: AtomicUsize,
    retired_head: RelPtr<ShmSkipNode<K>>,
    retired_nodes: AtomicU64,
    reclaimed_nodes: AtomicU64,
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
            retired_head: RelPtr::null(),
            retired_nodes: AtomicU64::new(0),
            reclaimed_nodes: AtomicU64::new(0),
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
    retire_txid: AtomicU64,
    retire_next: RelPtr<ShmSkipNode<K>>,
}

impl<K: ShmSkipKey> ShmSkipNode<K> {
    #[inline]
    fn new(key: K, height: u8, tower_offset: u32, postings_head: u32) -> Self {
        Self {
            key,
            height,
            _pad: [0_u8; 3],
            flags: AtomicU32::new(0),
            tower_offset,
            postings_head: RelPtr::from_offset(postings_head),
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
            ShmSkipNode::new(K::sentinel(), MAX_HEIGHT as u8, tower_offset, NULL_OFFSET)
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

            let posting_offset = self
                .shm
                .chunked_arena()
                .alloc(PostingEntry::new(payload_len, payload, NULL_OFFSET))?
                .load(AtomicOrdering::Acquire);

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
            let node_offset = self
                .shm
                .chunked_arena()
                .alloc(ShmSkipNode::new(
                    key,
                    node_height as u8,
                    tower_offset,
                    posting_offset,
                ))?
                .load(AtomicOrdering::Acquire);

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
                    break;
                }
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }

        if !self.has_live_postings(node)? {
            self.unlink_node(key, node_offset)?;
        }

        Ok(())
    }

    pub fn lookup_payloads<F>(&self, key: &K, mut visit: F) -> Result<(), ShmSkipListError>
    where
        F: FnMut(u16, &[u8]),
    {
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

        let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while post_offset != NULL_OFFSET {
            let post = self
                .posting_ref(post_offset)
                .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
            if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                let len = post.len as usize;
                visit(post.len, &post.payload[..len]);
            }
            post_offset = post.next.load(AtomicOrdering::Acquire);
        }

        Ok(())
    }

    pub fn scan_payloads<P, F>(&self, predicate: P, mut visit: F) -> Result<(), ShmSkipListError>
    where
        P: Fn(&K) -> bool,
        F: FnMut(&K, u16, &[u8]),
    {
        let header = self
            .header_ref()
            .ok_or(ShmSkipListError::InvalidHeader(self.header_offset))?;
        let head_offset = header.head.load(AtomicOrdering::Acquire);
        let head_lane = self.lane_ref_by_offset(head_offset, 0)?;
        let mut curr_offset = head_lane.next.load(AtomicOrdering::Acquire);

        while curr_offset != NULL_OFFSET {
            let node = self
                .node_ref(curr_offset)
                .ok_or(ShmSkipListError::InvalidNode(curr_offset))?;
            let next = self.node_next_offset(curr_offset, 0)?;
            let flags = node.flags.load(AtomicOrdering::Acquire);
            if flags & NODE_FLAG_MARKED == 0
                && flags & NODE_FLAG_FULLY_LINKED != 0
                && predicate(&node.key)
            {
                let mut post_offset = node.postings_head.load(AtomicOrdering::Acquire);
                while post_offset != NULL_OFFSET {
                    let post = self
                        .posting_ref(post_offset)
                        .ok_or(ShmSkipListError::InvalidPosting(post_offset))?;
                    if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                        let len = post.len as usize;
                        visit(&node.key, post.len, &post.payload[..len]);
                    }
                    post_offset = post.next.load(AtomicOrdering::Acquire);
                }
            }
            curr_offset = next;
        }

        Ok(())
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
                    let _ = list.collect_garbage_once(1024);
                    std::thread::sleep(interval);
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
                    let curr_next = self.node_next_offset(curr_offset, level)?;

                    let node_marked =
                        curr.flags.load(AtomicOrdering::Acquire) & NODE_FLAG_MARKED != 0;
                    let lane_marked = self
                        .lane_ref(curr_offset, curr, level)?
                        .marked
                        .load(AtomicOrdering::Acquire)
                        != 0;
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
            self.decrement_distinct_key_count();
        }

        let height = node.height as usize;
        for level in (0..height).rev() {
            self.lane_ref(node_offset, node, level)?
                .marked
                .store(1, AtomicOrdering::Release);
        }

        let mut preds = [NULL_OFFSET; MAX_HEIGHT];
        let mut succs = [NULL_OFFSET; MAX_HEIGHT];
        loop {
            let _ = self.find(key, &mut preds, &mut succs)?;
            if succs[0] != node_offset {
                break;
            }
            for level in (0..height).rev() {
                let pred_lane = self.lane_ref_by_offset(preds[level], level)?;
                let next = self.node_next_offset(node_offset, level)?;
                let _ = pred_lane.next.compare_exchange(
                    node_offset,
                    next,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                );
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
        let posting_offset = self
            .shm
            .chunked_arena()
            .alloc(PostingEntry::new(payload_len, payload, NULL_OFFSET))?
            .load(AtomicOrdering::Acquire);
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
                return Ok(());
            }
        }
    }

    fn has_live_postings(&self, node: &ShmSkipNode<K>) -> Result<bool, ShmSkipListError> {
        let mut curr = node.postings_head.load(AtomicOrdering::Acquire);
        while curr != NULL_OFFSET {
            let post = self
                .posting_ref(curr)
                .ok_or(ShmSkipListError::InvalidPosting(curr))?;
            if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                return Ok(true);
            }
            curr = post.next.load(AtomicOrdering::Acquire);
        }
        Ok(false)
    }

    fn alloc_tower(
        &self,
        height: usize,
        succs: &[u32; MAX_HEIGHT],
    ) -> Result<u32, ShmSkipListError> {
        let bytes = size_of::<SkipLane<K>>()
            .checked_mul(height)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let tower_offset = self
            .shm
            .chunked_arena()
            .alloc_raw(bytes, align_of::<SkipLane<K>>())?;
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
        while h < MAX_HEIGHT as u8 && (bits & 0b11) == 0 {
            h += 1;
            bits >>= 2;
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
}
