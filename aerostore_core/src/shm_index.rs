use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::index::{IndexCompare, IndexValue};
use crate::shm::{RelPtr, ShmArena};

const KEY_INLINE_BYTES: usize = 128;
const ROWID_INLINE_BYTES: usize = 192;
const NULL_OFFSET: u32 = 0;
const DEFAULT_INDEX_ARENA_BYTES: usize = 64 << 20;

const KEY_TAG_I64: u8 = 1;
const KEY_TAG_U64: u8 = 2;
const KEY_TAG_STRING: u8 = 3;
const KEY_TAG_SENTINEL: u8 = 255;

#[derive(Debug)]
pub enum ShmIndexError {
    InvalidHeader(u32),
    InvalidNode(u32),
    InvalidPosting(u32),
    KeyTooLong { len: usize, max: usize },
    RowIdTooLarge { len: usize, max: usize },
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
            ShmIndexError::Fork(err) => write!(f, "fork failed: {}", err),
            ShmIndexError::Wait(err) => write!(f, "wait failed: {}", err),
            ShmIndexError::Signal(err) => write!(f, "signal failed: {}", err),
        }
    }
}

impl std::error::Error for ShmIndexError {}

#[repr(C, align(64))]
struct ShmSkipHeader {
    head: AtomicU32,
    retired_head: AtomicU32,
    retired_nodes: AtomicU64,
    reclaimed_nodes: AtomicU64,
    gc_daemon_pid: AtomicI32,
}

impl ShmSkipHeader {
    #[inline]
    fn new(head: u32) -> Self {
        Self {
            head: AtomicU32::new(head),
            retired_head: AtomicU32::new(NULL_OFFSET),
            retired_nodes: AtomicU64::new(0),
            reclaimed_nodes: AtomicU64::new(0),
            gc_daemon_pid: AtomicI32::new(0),
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
        let mut out = Self {
            tag: 0,
            len: 0,
            _pad: 0,
            data: [0_u8; KEY_INLINE_BYTES],
        };

        match value {
            IndexValue::I64(v) => {
                out.tag = KEY_TAG_I64;
                out.len = 8;
                out.data[..8].copy_from_slice(&v.to_le_bytes());
            }
            IndexValue::U64(v) => {
                out.tag = KEY_TAG_U64;
                out.len = 8;
                out.data[..8].copy_from_slice(&v.to_le_bytes());
            }
            IndexValue::String(v) => {
                let bytes = v.as_bytes();
                if bytes.len() > KEY_INLINE_BYTES {
                    return Err(ShmIndexError::KeyTooLong {
                        len: bytes.len(),
                        max: KEY_INLINE_BYTES,
                    });
                }
                out.tag = KEY_TAG_STRING;
                out.len = bytes.len() as u16;
                out.data[..bytes.len()].copy_from_slice(bytes);
            }
        }

        Ok(out)
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
            KEY_TAG_I64 => {
                let mut lhs = [0_u8; 8];
                let mut rhs = [0_u8; 8];
                lhs.copy_from_slice(&self.data[..8]);
                rhs.copy_from_slice(&other.data[..8]);
                i64::from_le_bytes(lhs).cmp(&i64::from_le_bytes(rhs))
            }
            KEY_TAG_U64 => {
                let mut lhs = [0_u8; 8];
                let mut rhs = [0_u8; 8];
                lhs.copy_from_slice(&self.data[..8]);
                rhs.copy_from_slice(&other.data[..8]);
                u64::from_le_bytes(lhs).cmp(&u64::from_le_bytes(rhs))
            }
            KEY_TAG_STRING => {
                let lhs_len = self.len as usize;
                let rhs_len = other.len as usize;
                self.data[..lhs_len].cmp(&other.data[..rhs_len])
            }
            _ => Ordering::Equal,
        }
    }
}

#[repr(C)]
struct ShmSkipNode {
    key: EncodedKey,
    marked: AtomicU32,
    next: AtomicU32,
    postings_head: AtomicU32,
    retire_txid: AtomicU64,
    retire_next: AtomicU32,
}

impl ShmSkipNode {
    #[inline]
    fn sentinel() -> Self {
        Self {
            key: EncodedKey::sentinel(),
            marked: AtomicU32::new(0),
            next: AtomicU32::new(NULL_OFFSET),
            postings_head: AtomicU32::new(NULL_OFFSET),
            retire_txid: AtomicU64::new(0),
            retire_next: AtomicU32::new(NULL_OFFSET),
        }
    }

    #[inline]
    fn new(key: EncodedKey, next: u32, postings_head: u32) -> Self {
        Self {
            key,
            marked: AtomicU32::new(0),
            next: AtomicU32::new(next),
            postings_head: AtomicU32::new(postings_head),
            retire_txid: AtomicU64::new(0),
            retire_next: AtomicU32::new(NULL_OFFSET),
        }
    }
}

#[repr(C)]
struct PostingEntry {
    len: u16,
    _pad: [u8; 2],
    deleted: AtomicU32,
    next: AtomicU32,
    payload: [u8; ROWID_INLINE_BYTES],
}

impl PostingEntry {
    #[inline]
    fn new(payload_len: u16, payload: &[u8; ROWID_INLINE_BYTES], next: u32) -> Self {
        Self {
            len: payload_len,
            _pad: [0_u8; 2],
            deleted: AtomicU32::new(0),
            next: AtomicU32::new(next),
            payload: *payload,
        }
    }
}

#[derive(Clone)]
pub struct SecondaryIndex<RowId>
where
    RowId: Ord + Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    field: &'static str,
    shm: Arc<ShmArena>,
    header_offset: u32,
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
        let arena = shm.chunked_arena();
        let head_offset = arena
            .alloc(ShmSkipNode::sentinel())
            .expect("failed to allocate shared index head")
            .load(AtomicOrdering::Acquire);
        let header_offset = arena
            .alloc(ShmSkipHeader::new(head_offset))
            .expect("failed to allocate shared index header")
            .load(AtomicOrdering::Acquire);

        Self {
            field,
            shm,
            header_offset,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn field(&self) -> &'static str {
        self.field
    }

    #[inline]
    pub fn shared_arena(&self) -> &Arc<ShmArena> {
        &self.shm
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

        loop {
            let (pred_offset, curr_offset, equal) = self.find_position(&key)?;

            if equal {
                let Some(node) = self.node_ref(curr_offset) else {
                    return Err(ShmIndexError::InvalidNode(curr_offset));
                };
                if node.marked.load(AtomicOrdering::Acquire) != 0 {
                    continue;
                }
                self.prepend_posting(node, payload_len, &payload);
                return Ok(());
            }

            let posting_offset = match self.shm.chunked_arena().alloc(PostingEntry::new(
                payload_len,
                &payload,
                NULL_OFFSET,
            )) {
                Ok(ptr) => ptr.load(AtomicOrdering::Acquire),
                Err(_) => return Ok(()),
            };
            let node_offset = match self.shm.chunked_arena().alloc(ShmSkipNode::new(
                key,
                curr_offset,
                posting_offset,
            )) {
                Ok(ptr) => ptr.load(AtomicOrdering::Acquire),
                Err(_) => return Ok(()),
            };

            let Some(pred) = self.node_ref(pred_offset) else {
                return Err(ShmIndexError::InvalidNode(pred_offset));
            };
            if pred
                .next
                .compare_exchange(
                    curr_offset,
                    node_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
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

        let (_, node_offset, equal) = self.find_position(&key)?;
        if !equal {
            return Ok(());
        }

        let Some(node) = self.node_ref(node_offset) else {
            return Err(ShmIndexError::InvalidNode(node_offset));
        };
        let mut posting_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while posting_offset != NULL_OFFSET {
            let Some(post) = self.posting_ref(posting_offset) else {
                return Err(ShmIndexError::InvalidPosting(posting_offset));
            };
            if post.deleted.load(AtomicOrdering::Acquire) == 0
                && post.len == payload_len
                && post.payload[..payload_len as usize] == payload[..payload_len as usize]
            {
                let _ = post.deleted.compare_exchange(
                    0,
                    1,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                );
                break;
            }
            posting_offset = post.next.load(AtomicOrdering::Acquire);
        }

        if !self.has_live_postings(node) {
            self.retire_node(&key, node_offset);
        }
        Ok(())
    }

    pub fn lookup(&self, predicate: &IndexCompare) -> Vec<RowId> {
        let mut out = BTreeSet::new();
        match predicate {
            IndexCompare::Eq(v) => {
                let Ok(key) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                let Ok((_, node_offset, equal)) = self.find_position(&key) else {
                    return Vec::new();
                };
                if equal {
                    if let Some(node) = self.node_ref(node_offset) {
                        self.collect_postings(node, &mut out);
                    }
                }
            }
            IndexCompare::Gt(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                self.scan_range(|key| key.cmp(&bound) == Ordering::Greater, &mut out);
            }
            IndexCompare::Lt(v) => {
                let Ok(bound) = EncodedKey::from_index_value(v) else {
                    return Vec::new();
                };
                self.scan_range(|key| key.cmp(&bound) == Ordering::Less, &mut out);
            }
            IndexCompare::In(values) => {
                for value in values {
                    let rows = self.lookup(&IndexCompare::Eq(value.clone()));
                    out.extend(rows);
                }
            }
        }

        out.into_iter().collect()
    }

    pub fn traverse(&self) -> Vec<(IndexValue, Vec<RowId>)> {
        let mut out = Vec::new();
        let Some(header) = self.header_ref() else {
            return out;
        };
        let Some(head) = self.node_ref(header.head.load(AtomicOrdering::Acquire)) else {
            return out;
        };

        let mut curr_offset = head.next.load(AtomicOrdering::Acquire);
        while curr_offset != NULL_OFFSET {
            let Some(node) = self.node_ref(curr_offset) else {
                break;
            };
            if node.marked.load(AtomicOrdering::Acquire) == 0 {
                if let Some(value) = node.key.as_index_value() {
                    let mut rows = BTreeSet::new();
                    self.collect_postings(node, &mut rows);
                    out.push((value, rows.into_iter().collect()));
                }
            }
            curr_offset = node.next.load(AtomicOrdering::Acquire);
        }

        out
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

        let mut keep = Vec::new();
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
                keep.push(curr);
            }

            curr = next;
        }

        for node_offset in keep {
            let Some(node) = self.node_ref(node_offset) else {
                continue;
            };
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

        reclaimed
    }

    pub fn spawn_gc_daemon(&self, interval: Duration) -> Result<ShmIndexGcDaemon, ShmIndexError> {
        let parent_pid = unsafe { libc::getpid() };
        let index = self.clone();

        // SAFETY:
        // A dedicated process is used for cross-process GC coordination.
        let fork_result = unsafe { rustix::runtime::fork() }.map_err(|err| {
            ShmIndexError::Fork(std::io::Error::from_raw_os_error(err.raw_os_error()))
        })?;

        match fork_result {
            rustix::runtime::Fork::Child(_) => {
                let _ = arm_parent_death_signal(parent_pid);
                loop {
                    let _ = index.collect_garbage_once(1024);
                    std::thread::sleep(interval);
                }
            }
            rustix::runtime::Fork::Parent(pid) => {
                let pid_raw = pid.as_raw_nonzero().get();
                if let Some(header) = self.header_ref() {
                    header.gc_daemon_pid.store(pid_raw, AtomicOrdering::Release);
                }
                Ok(ShmIndexGcDaemon { pid: pid_raw })
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
    pub fn reclaimed_nodes(&self) -> u64 {
        self.header_ref()
            .map(|header| header.reclaimed_nodes.load(AtomicOrdering::Acquire))
            .unwrap_or(0)
    }

    fn find_position(&self, key: &EncodedKey) -> Result<(u32, u32, bool), ShmIndexError> {
        'retry: loop {
            let header = self
                .header_ref()
                .ok_or(ShmIndexError::InvalidHeader(self.header_offset))?;
            let head_offset = header.head.load(AtomicOrdering::Acquire);
            let head = self
                .node_ref(head_offset)
                .ok_or(ShmIndexError::InvalidNode(head_offset))?;

            let mut pred_offset = head_offset;
            let mut pred = head;
            let mut curr_offset = pred.next.load(AtomicOrdering::Acquire);

            while curr_offset != NULL_OFFSET {
                let curr = self
                    .node_ref(curr_offset)
                    .ok_or(ShmIndexError::InvalidNode(curr_offset))?;
                let next_offset = curr.next.load(AtomicOrdering::Acquire);

                if curr.marked.load(AtomicOrdering::Acquire) != 0 {
                    if pred
                        .next
                        .compare_exchange(
                            curr_offset,
                            next_offset,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Acquire,
                        )
                        .is_ok()
                    {
                        curr_offset = next_offset;
                        continue;
                    }
                    continue 'retry;
                }

                match curr.key.cmp(key) {
                    Ordering::Less => {
                        pred_offset = curr_offset;
                        pred = curr;
                        curr_offset = next_offset;
                    }
                    Ordering::Equal => return Ok((pred_offset, curr_offset, true)),
                    Ordering::Greater => return Ok((pred_offset, curr_offset, false)),
                }
            }

            return Ok((pred_offset, NULL_OFFSET, false));
        }
    }

    fn scan_range<F>(&self, predicate: F, out: &mut BTreeSet<RowId>)
    where
        F: Fn(&EncodedKey) -> bool,
    {
        let Some(header) = self.header_ref() else {
            return;
        };
        let Some(head) = self.node_ref(header.head.load(AtomicOrdering::Acquire)) else {
            return;
        };

        let mut curr_offset = head.next.load(AtomicOrdering::Acquire);
        while curr_offset != NULL_OFFSET {
            let Some(node) = self.node_ref(curr_offset) else {
                break;
            };
            if node.marked.load(AtomicOrdering::Acquire) == 0 && predicate(&node.key) {
                self.collect_postings(node, out);
            }
            curr_offset = node.next.load(AtomicOrdering::Acquire);
        }
    }

    fn prepend_posting(
        &self,
        node: &ShmSkipNode,
        payload_len: u16,
        payload: &[u8; ROWID_INLINE_BYTES],
    ) {
        let posting_offset = match self.shm.chunked_arena().alloc(PostingEntry::new(
            payload_len,
            payload,
            NULL_OFFSET,
        )) {
            Ok(ptr) => ptr.load(AtomicOrdering::Acquire),
            Err(_) => return,
        };

        let Some(posting) = self.posting_ref(posting_offset) else {
            return;
        };
        loop {
            let old_head = node.postings_head.load(AtomicOrdering::Acquire);
            posting.next.store(old_head, AtomicOrdering::Release);
            if node
                .postings_head
                .compare_exchange(
                    old_head,
                    posting_offset,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    fn collect_postings(&self, node: &ShmSkipNode, out: &mut BTreeSet<RowId>) {
        let mut curr_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while curr_offset != NULL_OFFSET {
            let Some(post) = self.posting_ref(curr_offset) else {
                break;
            };
            if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                if let Some(row_id) = Self::decode_row_id(post) {
                    out.insert(row_id);
                }
            }
            curr_offset = post.next.load(AtomicOrdering::Acquire);
        }
    }

    fn has_live_postings(&self, node: &ShmSkipNode) -> bool {
        let mut curr_offset = node.postings_head.load(AtomicOrdering::Acquire);
        while curr_offset != NULL_OFFSET {
            let Some(post) = self.posting_ref(curr_offset) else {
                return false;
            };
            if post.deleted.load(AtomicOrdering::Acquire) == 0 {
                return true;
            }
            curr_offset = post.next.load(AtomicOrdering::Acquire);
        }
        false
    }

    fn retire_node(&self, key: &EncodedKey, node_offset: u32) {
        let Some(node) = self.node_ref(node_offset) else {
            return;
        };
        if node
            .marked
            .compare_exchange(0, 1, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return;
        }

        if let Ok((pred_offset, found_offset, found)) = self.find_position(key) {
            if found && found_offset == node_offset {
                if let Some(pred) = self.node_ref(pred_offset) {
                    let next_offset = node.next.load(AtomicOrdering::Acquire);
                    let _ = pred.next.compare_exchange(
                        node_offset,
                        next_offset,
                        AtomicOrdering::AcqRel,
                        AtomicOrdering::Acquire,
                    );
                }
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

    #[inline]
    fn header_ref(&self) -> Option<&ShmSkipHeader> {
        RelPtr::<ShmSkipHeader>::from_offset(self.header_offset).as_ref(self.shm.mmap_base())
    }

    #[inline]
    fn node_ref(&self, offset: u32) -> Option<&ShmSkipNode> {
        RelPtr::<ShmSkipNode>::from_offset(offset).as_ref(self.shm.mmap_base())
    }

    #[inline]
    fn posting_ref(&self, offset: u32) -> Option<&PostingEntry> {
        RelPtr::<PostingEntry>::from_offset(offset).as_ref(self.shm.mmap_base())
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

    fn decode_row_id(entry: &PostingEntry) -> Option<RowId> {
        let len = entry.len as usize;
        bincode::deserialize::<RowId>(&entry.payload[..len]).ok()
    }
}

pub struct ShmIndexGcDaemon {
    pid: i32,
}

impl ShmIndexGcDaemon {
    #[inline]
    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn terminate(&self, signal: i32) -> Result<(), ShmIndexError> {
        // SAFETY:
        // `pid` came from a successful fork call and belongs to this process group.
        let rc = unsafe { libc::kill(self.pid, signal) };
        if rc == 0 {
            Ok(())
        } else {
            Err(ShmIndexError::Signal(std::io::Error::last_os_error()))
        }
    }

    pub fn join(&self) -> Result<(), ShmIndexError> {
        let pid = rustix::process::Pid::from_raw(self.pid).ok_or_else(|| {
            ShmIndexError::Wait(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid pid for gc daemon",
            ))
        })?;
        let status = rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::empty())
            .map_err(|err| {
                ShmIndexError::Wait(std::io::Error::from_raw_os_error(err.raw_os_error()))
            })?;
        let Some(status) = status else {
            return Err(ShmIndexError::Wait(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "waitpid returned no status",
            )));
        };

        if status.exited() {
            if status.exit_status() == Some(0) {
                Ok(())
            } else {
                Err(ShmIndexError::Wait(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("gc daemon exited with status {:?}", status.exit_status()),
                )))
            }
        } else if status.signaled() {
            Ok(())
        } else {
            Err(ShmIndexError::Wait(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("gc daemon exited unexpectedly: {:?}", status),
            )))
        }
    }
}

#[cfg(target_os = "linux")]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> Result<(), ShmIndexError> {
    // SAFETY:
    // called immediately in the fork child before creating new threads.
    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if rc != 0 {
        return Err(ShmIndexError::Fork(std::io::Error::last_os_error()));
    }

    // SAFETY:
    // getppid is async-signal-safe and used for race-checking parent liveness.
    let observed_parent = unsafe { libc::getppid() };
    if observed_parent != expected_parent {
        return Err(ShmIndexError::Fork(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "parent exited before gc daemon armed PDEATHSIG",
        )));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn arm_parent_death_signal(_expected_parent: libc::pid_t) -> Result<(), ShmIndexError> {
    Ok(())
}
