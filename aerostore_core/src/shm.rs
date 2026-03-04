use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};

use crate::procarray::{
    ProcArray, ProcArrayError, ProcArrayRegistration, ProcSnapshot, PROCARRAY_SLOTS,
};

const SHM_HEADER_MAGIC: u32 = 0xAEB0_B007;
const SHM_LAYOUT_VERSION: u32 = 2;
const SHM_HEADER_ALIGN: u32 = 64;
pub(crate) const OCC_PARTITION_LOCKS: usize = 1024;
const FREE_LIST_NODE_MAGIC: u32 = 0xAEB0_F1E5;
const LOCAL_RECYCLE_CACHE_CAPACITY: usize = 64;
const LOCAL_RECYCLE_CACHE_REFILL_BATCH: usize = 16;
const LOCAL_RECYCLE_CACHE_FLUSH_BATCH: usize = 32;

pub const ARENA_CLASS_COUNT: usize = 9;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArenaClass {
    General = 0,
    RowVersion = 1,
    SkipNode = 2,
    SkipPosting = 3,
    SkipTower = 4,
    Spill32 = 5,
    Spill64 = 6,
    Spill128 = 7,
    Spill256 = 8,
}

impl ArenaClass {
    #[inline]
    pub const fn as_index(self) -> usize {
        self as usize
    }

    #[inline]
    const fn from_extended_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::General),
            1 => Some(Self::RowVersion),
            2 => Some(Self::SkipNode),
            3 => Some(Self::SkipPosting),
            4 => Some(Self::SkipTower),
            5 => Some(Self::Spill32),
            6 => Some(Self::Spill64),
            7 => Some(Self::Spill128),
            8 => Some(Self::Spill256),
            _ => None,
        }
    }

    #[inline]
    const fn supports_local_cache(self) -> bool {
        matches!(
            self,
            Self::RowVersion
                | Self::SkipNode
                | Self::SkipPosting
                | Self::Spill32
                | Self::Spill64
                | Self::Spill128
                | Self::Spill256
        )
    }

    #[inline]
    const fn extended_index(self) -> Option<usize> {
        let idx = self.as_index();
        if idx == 0 {
            None
        } else {
            Some(idx - 1)
        }
    }
}

const EXTENDED_ARENA_CLASS_COUNT: usize = ARENA_CLASS_COUNT - 1;

struct LocalRecycleCache {
    bins: [Vec<u32>; ARENA_CLASS_COUNT],
}

impl LocalRecycleCache {
    fn new() -> Self {
        Self {
            bins: std::array::from_fn(|_| Vec::new()),
        }
    }
}

thread_local! {
    static LOCAL_RECYCLE_CACHE: RefCell<LocalRecycleCache> = RefCell::new(LocalRecycleCache::new());
}

#[repr(C, align(64))]
pub(crate) struct OccPartitionLock {
    state: AtomicBool,
}

impl OccPartitionLock {
    #[inline]
    pub fn new() -> Self {
        Self {
            state: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn try_lock(&self) -> bool {
        self.state
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn unlock(&self) {
        self.state.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
pub enum ShmError {
    InvalidSize(usize),
    SizeExceedsRelPtrLimit(usize),
    MmapFailed(std::io::Error),
}

impl fmt::Display for ShmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmError::InvalidSize(size) => write!(f, "shared memory size {} is invalid", size),
            ShmError::SizeExceedsRelPtrLimit(size) => write!(
                f,
                "shared memory size {} exceeds RelPtr<u32> addressable range",
                size
            ),
            ShmError::MmapFailed(err) => write!(f, "mmap failed: {}", err),
        }
    }
}

impl std::error::Error for ShmError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShmAllocError {
    ZeroSizedType,
    SizeOverflow,
    OutOfMemory { requested: usize, remaining: usize },
}

impl fmt::Display for ShmAllocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmAllocError::ZeroSizedType => {
                write!(f, "zero-sized types are not supported in ShmArena")
            }
            ShmAllocError::SizeOverflow => write!(f, "allocation size overflow"),
            ShmAllocError::OutOfMemory {
                requested,
                remaining,
            } => write!(
                f,
                "shared arena out of memory (requested {}, remaining {})",
                requested, remaining
            ),
        }
    }
}

impl std::error::Error for ShmAllocError {}

#[derive(Clone, Copy)]
pub struct MmapBase<'a> {
    ptr: NonNull<u8>,
    len: usize,
    _marker: PhantomData<&'a [u8]>,
}

impl<'a> MmapBase<'a> {
    #[inline]
    pub fn as_ptr(self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn len(self) -> usize {
        self.len
    }
}

#[repr(transparent)]
pub struct RelPtr<T> {
    offset: AtomicU32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> RelPtr<T> {
    #[inline]
    pub const fn null() -> Self {
        Self {
            offset: AtomicU32::new(0),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_offset(offset: u32) -> Self {
        Self {
            offset: AtomicU32::new(offset),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_null(&self, order: Ordering) -> bool {
        self.offset.load(order) == 0
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> u32 {
        self.offset.load(order)
    }

    #[inline]
    pub fn store(&self, offset: u32, order: Ordering) {
        self.offset.store(offset, order);
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        self.offset.compare_exchange(current, new, success, failure)
    }

    #[inline]
    pub fn swap(&self, offset: u32, order: Ordering) -> u32 {
        self.offset.swap(offset, order)
    }

    #[inline]
    pub fn as_ref<'a>(&self, mmap_base: MmapBase<'a>) -> Option<&'a T> {
        let ptr = self.resolve_ptr(mmap_base, Ordering::Acquire)?;
        // SAFETY:
        // 1) `resolve_ptr` validates offset bounds and alignment against this mapped region.
        // 2) `mmap_base` lifetime ties the returned reference to the mapped segment lifetime.
        Some(unsafe { ptr.as_ref() })
    }

    fn resolve_ptr<'a>(&self, mmap_base: MmapBase<'a>, order: Ordering) -> Option<NonNull<T>> {
        let offset = self.offset.load(order);
        if offset == 0 {
            return None;
        }

        let size = size_of::<T>();
        if size == 0 {
            return None;
        }

        let offset_usize = offset as usize;
        let end = offset_usize.checked_add(size)?;
        if end > mmap_base.len {
            return None;
        }

        let addr = (mmap_base.ptr.as_ptr() as usize).checked_add(offset_usize)?;
        if addr % align_of::<T>() != 0 {
            return None;
        }

        NonNull::new(addr as *mut T)
    }
}

impl<T> Clone for RelPtr<T> {
    fn clone(&self) -> Self {
        Self::from_offset(self.load(Ordering::Acquire))
    }
}

impl<T> Default for RelPtr<T> {
    fn default() -> Self {
        Self::null()
    }
}

impl<T> fmt::Debug for RelPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelPtr")
            .field("offset", &self.load(Ordering::Relaxed))
            .finish()
    }
}

#[repr(C)]
struct SharedFreeListNode {
    next: AtomicU32,
    block_size: AtomicU32,
    block_align: AtomicU32,
    class_id: AtomicU32,
    magic: AtomicU32,
}

impl SharedFreeListNode {
    #[inline]
    fn initialize(&self, next: u32, block_size: u32, block_align: u32, class_id: u32) {
        self.next.store(next, Ordering::Release);
        self.block_size.store(block_size, Ordering::Release);
        self.block_align.store(block_align, Ordering::Release);
        self.class_id.store(class_id, Ordering::Release);
        self.magic.store(FREE_LIST_NODE_MAGIC, Ordering::Release);
    }
}

#[repr(C)]
struct ShmHeader {
    magic: u32,
    layout_version: u32,
    capacity: u32,
    data_start: u32,
    clean_shutdown: AtomicBool,
    boot_layout_offset: AtomicU32,
    next_txid: AtomicU64,
    proc_array: ProcArray,
    occ_partition_locks: [OccPartitionLock; OCC_PARTITION_LOCKS],
    free_list_head: AtomicU32,
    free_list_class_heads: [AtomicU32; EXTENDED_ARENA_CLASS_COUNT],
    free_list_pushes: AtomicU64,
    free_list_pops: AtomicU64,
    free_list_pop_misses: AtomicU64,
    free_list_class_pushes: [AtomicU64; EXTENDED_ARENA_CLASS_COUNT],
    free_list_class_pops: [AtomicU64; EXTENDED_ARENA_CLASS_COUNT],
    free_list_class_pop_misses: [AtomicU64; EXTENDED_ARENA_CLASS_COUNT],
    local_cache_hits: [AtomicU64; ARENA_CLASS_COUNT],
    local_cache_misses: [AtomicU64; ARENA_CLASS_COUNT],
    local_cache_flushes: [AtomicU64; ARENA_CLASS_COUNT],
    vacuum_daemon_pid: AtomicI32,
    head: AtomicU32,
}

pub struct ShmArena {
    base: NonNull<u8>,
    len: usize,
    header: NonNull<ShmHeader>,
}

unsafe impl Send for ShmArena {}
unsafe impl Sync for ShmArena {}

impl ShmArena {
    pub fn new(byte_len: usize) -> Result<Self, ShmError> {
        Self::validate_size(byte_len)?;

        // SAFETY:
        // - `mmap` is called with MAP_SHARED to ensure visibility across forked processes.
        // - `MAP_ANONYMOUS` gives us a process-shared anonymous segment without filesystem state.
        let map_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                byte_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if map_ptr == libc::MAP_FAILED {
            return Err(ShmError::MmapFailed(std::io::Error::last_os_error()));
        }

        let base = NonNull::new(map_ptr.cast::<u8>()).ok_or(ShmError::MmapFailed(
            std::io::Error::new(std::io::ErrorKind::Other, "mmap returned null"),
        ))?;
        // SAFETY:
        // `mmap` returned a writable region of length `byte_len`.
        unsafe { Self::from_mapped_region(base, byte_len, true) }
    }

    /// # Safety
    /// Caller must guarantee `base..base+byte_len` points to a valid writable mapping.
    pub(crate) unsafe fn from_mapped_region(
        base: NonNull<u8>,
        byte_len: usize,
        initialize_header: bool,
    ) -> Result<Self, ShmError> {
        Self::validate_size(byte_len)?;
        let arena = Self {
            base,
            len: byte_len,
            header: base.cast::<ShmHeader>(),
        };
        if initialize_header {
            arena.reinitialize_header()?;
        }
        Ok(arena)
    }

    pub fn reinitialize_header(&self) -> Result<(), ShmError> {
        let data_start = Self::validate_size(self.len)?;
        // SAFETY:
        // This writes allocator metadata at offset 0 of the mapped region.
        unsafe {
            self.header.as_ptr().write(ShmHeader {
                magic: SHM_HEADER_MAGIC,
                layout_version: SHM_LAYOUT_VERSION,
                capacity: self.len as u32,
                data_start,
                clean_shutdown: AtomicBool::new(true),
                boot_layout_offset: AtomicU32::new(0),
                next_txid: AtomicU64::new(1),
                proc_array: ProcArray::new(),
                occ_partition_locks: std::array::from_fn(|_| OccPartitionLock::new()),
                free_list_head: AtomicU32::new(0),
                free_list_class_heads: std::array::from_fn(|_| AtomicU32::new(0)),
                free_list_pushes: AtomicU64::new(0),
                free_list_pops: AtomicU64::new(0),
                free_list_pop_misses: AtomicU64::new(0),
                free_list_class_pushes: std::array::from_fn(|_| AtomicU64::new(0)),
                free_list_class_pops: std::array::from_fn(|_| AtomicU64::new(0)),
                free_list_class_pop_misses: std::array::from_fn(|_| AtomicU64::new(0)),
                local_cache_hits: std::array::from_fn(|_| AtomicU64::new(0)),
                local_cache_misses: std::array::from_fn(|_| AtomicU64::new(0)),
                local_cache_flushes: std::array::from_fn(|_| AtomicU64::new(0)),
                vacuum_daemon_pid: AtomicI32::new(0),
                head: AtomicU32::new(data_start),
            });
        }
        Ok(())
    }

    fn validate_size(byte_len: usize) -> Result<u32, ShmError> {
        if byte_len == 0 {
            return Err(ShmError::InvalidSize(byte_len));
        }
        if byte_len > u32::MAX as usize {
            return Err(ShmError::SizeExceedsRelPtrLimit(byte_len));
        }
        let data_start = align_up(size_of::<ShmHeader>() as u32, SHM_HEADER_ALIGN)
            .ok_or(ShmError::InvalidSize(byte_len))?;
        if data_start as usize >= byte_len {
            return Err(ShmError::InvalidSize(byte_len));
        }
        Ok(data_start)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn mmap_base(&self) -> MmapBase<'_> {
        MmapBase {
            ptr: self.base,
            len: self.len,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn chunked_arena(&self) -> ChunkedArena<'_> {
        let header = self
            .header_ref()
            .expect("shared memory header was unexpectedly invalid");
        ChunkedArena {
            mmap_base: self.mmap_base(),
            header,
        }
    }

    #[inline]
    pub fn flush_local_recycle_caches(&self) -> Result<(), ShmAllocError> {
        self.chunked_arena().flush_local_recycle_caches()
    }

    #[inline]
    fn header_ref(&self) -> Option<&ShmHeader> {
        // SAFETY:
        // `header` points into the current mapping. We verify the magic before use.
        let header = unsafe { self.header.as_ref() };
        if header.magic != SHM_HEADER_MAGIC {
            None
        } else if header.layout_version != SHM_LAYOUT_VERSION {
            None
        } else if header.capacity as usize != self.len {
            None
        } else if header.data_start
            != align_up(size_of::<ShmHeader>() as u32, SHM_HEADER_ALIGN).unwrap_or(0)
        {
            None
        } else if header.data_start as usize >= self.len {
            None
        } else {
            Some(header)
        }
    }

    #[inline]
    pub fn is_header_valid(&self) -> bool {
        self.header_ref().is_some()
    }

    #[inline]
    pub fn proc_array(&self) -> &ProcArray {
        &self
            .header_ref()
            .expect("shared memory header was unexpectedly invalid")
            .proc_array
    }

    #[inline]
    pub fn global_txid(&self) -> &AtomicU64 {
        &self
            .header_ref()
            .expect("shared memory header was unexpectedly invalid")
            .next_txid
    }

    #[inline]
    pub(crate) fn occ_partition_locks(&self) -> &[OccPartitionLock; OCC_PARTITION_LOCKS] {
        &self
            .header_ref()
            .expect("shared memory header was unexpectedly invalid")
            .occ_partition_locks
    }

    #[inline]
    pub fn begin_transaction(&self) -> Result<ProcArrayRegistration, ProcArrayError> {
        self.proc_array().begin_transaction(self.global_txid())
    }

    #[inline]
    pub fn end_transaction(
        &self,
        registration: ProcArrayRegistration,
    ) -> Result<(), ProcArrayError> {
        self.proc_array().end_transaction(registration)
    }

    #[inline]
    pub fn create_snapshot(&self) -> ProcSnapshot {
        self.proc_array().create_snapshot(self.global_txid())
    }

    #[inline]
    pub fn max_workers(&self) -> usize {
        PROCARRAY_SLOTS
    }

    #[inline]
    pub fn set_clean_shutdown_flag(&self, value: bool) {
        if let Some(header) = self.header_ref() {
            header.clean_shutdown.store(value, Ordering::Release);
        }
    }

    #[inline]
    pub fn clean_shutdown_flag(&self) -> bool {
        self.header_ref()
            .map(|header| header.clean_shutdown.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    #[inline]
    pub fn boot_layout_offset(&self) -> u32 {
        self.header_ref()
            .map(|header| header.boot_layout_offset.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn set_boot_layout_offset(&self, offset: u32) {
        if let Some(header) = self.header_ref() {
            header.boot_layout_offset.store(offset, Ordering::Release);
        }
    }

    #[inline]
    pub fn free_list_pushes(&self) -> u64 {
        self.header_ref()
            .map(|header| header.free_list_pushes.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn free_list_pops(&self) -> u64 {
        self.header_ref()
            .map(|header| header.free_list_pops.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn free_list_pop_misses(&self) -> u64 {
        self.header_ref()
            .map(|header| header.free_list_pop_misses.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn free_list_head_offset(&self) -> u32 {
        self.header_ref()
            .map(|header| header.free_list_head.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Best-effort lock-free depth estimate of the shared free-list.
    ///
    /// The estimate is bounded by `max_nodes` to keep sampling predictable.
    /// Returns `(depth, truncated)` where `truncated=true` means traversal hit
    /// `max_nodes` before reaching the end of the list.
    #[inline]
    pub fn free_list_depth_estimate(&self, max_nodes: usize) -> (u64, bool) {
        let Some(header) = self.header_ref() else {
            return (0, false);
        };
        if max_nodes == 0 {
            return (0, header.free_list_head.load(Ordering::Acquire) != 0);
        }

        let mut depth = 0_u64;
        let mut current = header.free_list_head.load(Ordering::Acquire);
        let limit = max_nodes as u64;

        while current != 0 && depth < limit {
            let Some(node) = self.free_list_node_ref(current) else {
                break;
            };
            if node.magic.load(Ordering::Acquire) != FREE_LIST_NODE_MAGIC {
                break;
            }
            current = node.next.load(Ordering::Acquire);
            depth = depth.saturating_add(1);
        }

        (depth, current != 0 && depth >= limit)
    }

    #[inline]
    pub fn vacuum_daemon_pid(&self) -> i32 {
        self.header_ref()
            .map(|header| header.vacuum_daemon_pid.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[inline]
    pub fn compare_exchange_vacuum_daemon_pid(&self, current: i32, new: i32) -> Result<i32, i32> {
        let Some(header) = self.header_ref() else {
            return Err(current);
        };
        header
            .vacuum_daemon_pid
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }

    #[inline]
    pub fn set_vacuum_daemon_pid(&self, pid: i32) {
        if let Some(header) = self.header_ref() {
            header.vacuum_daemon_pid.store(pid, Ordering::Release);
        }
    }

    fn free_list_node_ref(&self, offset: u32) -> Option<&SharedFreeListNode> {
        let start = offset as usize;
        let end = start.checked_add(size_of::<SharedFreeListNode>())?;
        if end > self.len {
            return None;
        }
        let addr = (self.base.as_ptr() as usize).checked_add(start)?;
        if addr % align_of::<SharedFreeListNode>() != 0 {
            return None;
        }
        // SAFETY:
        // Bounds and alignment are validated above against this mapped region.
        Some(unsafe { &*(addr as *const SharedFreeListNode) })
    }
}

impl Drop for ShmArena {
    fn drop(&mut self) {
        if let Some(header) = self.header_ref() {
            header.clean_shutdown.store(true, Ordering::Release);
        }
        // SAFETY:
        // `self.base` and `self.len` originate from successful `mmap`.
        let rc = unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
        debug_assert_eq!(rc, 0, "munmap failed: {}", std::io::Error::last_os_error());
    }
}

pub struct ChunkedArena<'a> {
    mmap_base: MmapBase<'a>,
    header: &'a ShmHeader,
}

impl<'a> ChunkedArena<'a> {
    #[inline]
    pub fn head_offset(&self) -> u32 {
        self.header.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn remaining_bytes(&self) -> usize {
        self.header.capacity.saturating_sub(self.head_offset()) as usize
    }

    pub fn alloc_raw(&self, size: usize, align: usize) -> Result<u32, ShmAllocError> {
        self.alloc_raw_in_class(size, align, ArenaClass::General)
    }

    pub fn alloc_raw_in_class(
        &self,
        size: usize,
        align: usize,
        class: ArenaClass,
    ) -> Result<u32, ShmAllocError> {
        if size == 0 {
            return Err(ShmAllocError::ZeroSizedType);
        }
        if align == 0 || !align.is_power_of_two() {
            return Err(ShmAllocError::SizeOverflow);
        }

        let size_u32 = u32::try_from(size).map_err(|_| ShmAllocError::SizeOverflow)?;
        let align_u32 = u32::try_from(align).map_err(|_| ShmAllocError::SizeOverflow)?;

        if class.supports_local_cache() {
            if let Some(offset) = self.try_pop_local_cache(class, size_u32, align_u32)? {
                return Ok(offset);
            }
        }

        if let Some(offset) = self.try_pop_recycled_in_class(size, align, class)? {
            if class.supports_local_cache() {
                self.refill_local_cache(
                    class,
                    size_u32,
                    align_u32,
                    LOCAL_RECYCLE_CACHE_REFILL_BATCH,
                );
            }
            return Ok(offset);
        }

        let size = size_u32;
        let align = align_u32;
        let mut spins = 0_u32;

        loop {
            let head = self.header.head.load(Ordering::Acquire);
            let start = align_up(head, align).ok_or(ShmAllocError::SizeOverflow)?;
            let end = start.checked_add(size).ok_or(ShmAllocError::SizeOverflow)?;

            if end > self.header.capacity {
                let remaining = self.header.capacity.saturating_sub(head) as usize;
                return Err(ShmAllocError::OutOfMemory {
                    requested: size as usize,
                    remaining,
                });
            }

            if self
                .header
                .head
                .compare_exchange(head, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(start);
            }

            spins = spins.wrapping_add(1);
            if spins & 0x3f == 0 {
                std::thread::yield_now();
            }
            if spins & 0x3ff == 0 {
                std::thread::sleep(std::time::Duration::from_micros(25));
            }
            std::hint::spin_loop();
        }
    }

    pub fn alloc<T>(&self, value: T) -> Result<RelPtr<T>, ShmAllocError> {
        self.alloc_in_class(value, ArenaClass::General)
    }

    pub fn alloc_in_class<T>(
        &self,
        value: T,
        class: ArenaClass,
    ) -> Result<RelPtr<T>, ShmAllocError> {
        if size_of::<T>() == 0 {
            return Err(ShmAllocError::ZeroSizedType);
        }

        let start = self.alloc_raw_in_class(size_of::<T>(), align_of::<T>(), class)?;
        let addr = (self.mmap_base.as_ptr() as usize)
            .checked_add(start as usize)
            .ok_or(ShmAllocError::SizeOverflow)?;
        let ptr = addr as *mut T;

        // SAFETY:
        // `ptr` points into a unique range reserved by `alloc_raw`.
        unsafe {
            ptr.write(value);
        }

        Ok(RelPtr::from_offset(start))
    }

    pub fn recycle_raw(&self, offset: u32, size: usize, align: usize) -> Result<(), ShmAllocError> {
        self.recycle_raw_in_class(offset, size, align, ArenaClass::General)
    }

    pub fn recycle_raw_in_class(
        &self,
        offset: u32,
        size: usize,
        align: usize,
        class: ArenaClass,
    ) -> Result<(), ShmAllocError> {
        if offset == 0 || size == 0 {
            return Ok(());
        }
        if align == 0 || !align.is_power_of_two() {
            return Err(ShmAllocError::SizeOverflow);
        }

        let size_u32 = u32::try_from(size).map_err(|_| ShmAllocError::SizeOverflow)?;
        let align_u32 = u32::try_from(align).map_err(|_| ShmAllocError::SizeOverflow)?;
        let end = (offset as usize)
            .checked_add(size)
            .ok_or(ShmAllocError::SizeOverflow)?;
        if end > self.header.capacity as usize {
            return Err(ShmAllocError::SizeOverflow);
        }
        if size < size_of::<SharedFreeListNode>()
            || (offset as usize) % align_of::<SharedFreeListNode>() != 0
        {
            return Ok(());
        }

        if class.supports_local_cache() {
            let node = self
                .free_list_node_ref(offset)
                .ok_or(ShmAllocError::SizeOverflow)?;
            node.initialize(0, size_u32, align_u32, class.as_index() as u32);
            let drained = self.push_local_cache(class, offset);
            if drained.is_empty() {
                return Ok(());
            }
            self.bump_local_cache_flushes(class, drained.len() as u64);
            for recycled_offset in drained {
                self.push_shared_recycled(recycled_offset, size_u32, align_u32, class)?;
            }
            return Ok(());
        }

        self.push_shared_recycled(offset, size_u32, align_u32, class)
    }

    pub fn flush_local_recycle_caches(&self) -> Result<(), ShmAllocError> {
        for class_idx in 0..ARENA_CLASS_COUNT {
            let Some(class) = ArenaClass::from_extended_index(class_idx) else {
                continue;
            };
            if !class.supports_local_cache() {
                continue;
            }

            let drained = LOCAL_RECYCLE_CACHE.with(|cache_cell| {
                let mut cache = cache_cell.borrow_mut();
                cache.bins[class_idx].drain(..).collect::<Vec<_>>()
            });
            if drained.is_empty() {
                continue;
            }
            self.bump_local_cache_flushes(class, drained.len() as u64);
            for offset in drained {
                let Some(node) = self.free_list_node_ref(offset) else {
                    continue;
                };
                let size = node.block_size.load(Ordering::Acquire);
                let align = node.block_align.load(Ordering::Acquire);
                self.push_shared_recycled(offset, size, align, class)?;
            }
        }
        Ok(())
    }

    fn push_shared_recycled(
        &self,
        offset: u32,
        size_u32: u32,
        align_u32: u32,
        class: ArenaClass,
    ) -> Result<(), ShmAllocError> {
        let head_slot = self.class_head_slot(class);
        let node = self
            .free_list_node_ref(offset)
            .ok_or(ShmAllocError::SizeOverflow)?;

        let mut spins = 0_u32;
        loop {
            let old_head = head_slot.load(Ordering::Acquire);
            node.initialize(old_head, size_u32, align_u32, class.as_index() as u32);
            if head_slot
                .compare_exchange(old_head, offset, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.bump_shared_push(class);
                return Ok(());
            }
            spins = spins.wrapping_add(1);
            if spins & 0x3f == 0 {
                std::thread::yield_now();
            }
            if spins & 0x3ff == 0 {
                std::thread::sleep(std::time::Duration::from_micros(25));
            }
            std::hint::spin_loop();
        }
    }

    fn try_pop_recycled_in_class(
        &self,
        size: usize,
        align: usize,
        class: ArenaClass,
    ) -> Result<Option<u32>, ShmAllocError> {
        let size_u32 = u32::try_from(size).map_err(|_| ShmAllocError::SizeOverflow)?;
        let align_u32 = u32::try_from(align).map_err(|_| ShmAllocError::SizeOverflow)?;
        let class_u32 = class.as_index() as u32;
        let head_slot = self.class_head_slot(class);
        let mut spins = 0_u32;

        loop {
            let head = head_slot.load(Ordering::Acquire);
            if head == 0 {
                self.bump_shared_pop_miss(class);
                return Ok(None);
            }

            let Some(node) = self.free_list_node_ref(head) else {
                self.bump_shared_pop_miss(class);
                return Ok(None);
            };

            if node.magic.load(Ordering::Acquire) != FREE_LIST_NODE_MAGIC
                || node.block_size.load(Ordering::Acquire) != size_u32
                || node.block_align.load(Ordering::Acquire) != align_u32
                || node.class_id.load(Ordering::Acquire) != class_u32
            {
                self.bump_shared_pop_miss(class);
                return Ok(None);
            }

            let next = node.next.load(Ordering::Acquire);
            if head_slot
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.bump_shared_pop(class);
                return Ok(Some(head));
            }

            spins = spins.wrapping_add(1);
            if spins & 0x3f == 0 {
                std::thread::yield_now();
            }
            if spins & 0x3ff == 0 {
                return Ok(None);
            }
            std::hint::spin_loop();
        }
    }

    fn try_pop_local_cache(
        &self,
        class: ArenaClass,
        size_u32: u32,
        align_u32: u32,
    ) -> Result<Option<u32>, ShmAllocError> {
        let class_idx = class.as_index();
        loop {
            let candidate = LOCAL_RECYCLE_CACHE.with(|cache_cell| {
                let mut cache = cache_cell.borrow_mut();
                cache.bins[class_idx].pop()
            });
            let Some(offset) = candidate else {
                self.bump_local_cache_miss(class);
                return Ok(None);
            };
            let Some(node) = self.free_list_node_ref(offset) else {
                continue;
            };
            if node.magic.load(Ordering::Acquire) != FREE_LIST_NODE_MAGIC {
                continue;
            }
            if node.class_id.load(Ordering::Acquire) != class.as_index() as u32 {
                continue;
            }
            if node.block_size.load(Ordering::Acquire) != size_u32
                || node.block_align.load(Ordering::Acquire) != align_u32
            {
                continue;
            }
            self.bump_local_cache_hit(class);
            return Ok(Some(offset));
        }
    }

    fn refill_local_cache(&self, class: ArenaClass, size_u32: u32, align_u32: u32, max: usize) {
        if max == 0 {
            return;
        }

        let mut refilled = Vec::new();
        for _ in 0..max {
            let popped = match self.try_pop_recycled_in_class(
                size_u32 as usize,
                align_u32 as usize,
                class,
            ) {
                Ok(Some(offset)) => Some(offset),
                _ => None,
            };
            let Some(offset) = popped else {
                break;
            };
            refilled.push(offset);
        }
        if refilled.is_empty() {
            return;
        }

        LOCAL_RECYCLE_CACHE.with(|cache_cell| {
            let mut cache = cache_cell.borrow_mut();
            cache.bins[class.as_index()].extend(refilled);
        });
    }

    fn push_local_cache(&self, class: ArenaClass, offset: u32) -> Vec<u32> {
        LOCAL_RECYCLE_CACHE.with(|cache_cell| {
            let mut cache = cache_cell.borrow_mut();
            let bin = &mut cache.bins[class.as_index()];
            bin.push(offset);
            if bin.len() <= LOCAL_RECYCLE_CACHE_CAPACITY {
                return Vec::new();
            }
            let flush_count = bin.len().saturating_sub(LOCAL_RECYCLE_CACHE_CAPACITY);
            let flush_count = flush_count.max(LOCAL_RECYCLE_CACHE_FLUSH_BATCH.min(bin.len()));
            bin.drain(..flush_count).collect::<Vec<_>>()
        })
    }

    #[inline]
    fn class_head_slot(&self, class: ArenaClass) -> &AtomicU32 {
        if let Some(idx) = class.extended_index() {
            &self.header.free_list_class_heads[idx]
        } else {
            &self.header.free_list_head
        }
    }

    #[inline]
    fn bump_shared_push(&self, class: ArenaClass) {
        self.header.free_list_pushes.fetch_add(1, Ordering::AcqRel);
        if let Some(idx) = class.extended_index() {
            self.header.free_list_class_pushes[idx].fetch_add(1, Ordering::AcqRel);
        }
    }

    #[inline]
    fn bump_shared_pop(&self, class: ArenaClass) {
        self.header.free_list_pops.fetch_add(1, Ordering::AcqRel);
        if let Some(idx) = class.extended_index() {
            self.header.free_list_class_pops[idx].fetch_add(1, Ordering::AcqRel);
        }
    }

    #[inline]
    fn bump_shared_pop_miss(&self, class: ArenaClass) {
        self.header
            .free_list_pop_misses
            .fetch_add(1, Ordering::AcqRel);
        if let Some(idx) = class.extended_index() {
            self.header.free_list_class_pop_misses[idx].fetch_add(1, Ordering::AcqRel);
        }
    }

    #[inline]
    fn bump_local_cache_hit(&self, class: ArenaClass) {
        self.header.local_cache_hits[class.as_index()].fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    fn bump_local_cache_miss(&self, class: ArenaClass) {
        self.header.local_cache_misses[class.as_index()].fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    fn bump_local_cache_flushes(&self, class: ArenaClass, count: u64) {
        self.header.local_cache_flushes[class.as_index()].fetch_add(count, Ordering::AcqRel);
    }

    fn free_list_node_ref(&self, offset: u32) -> Option<&SharedFreeListNode> {
        let start = offset as usize;
        let end = start.checked_add(size_of::<SharedFreeListNode>())?;
        if end > self.mmap_base.len() {
            return None;
        }
        let addr = (self.mmap_base.as_ptr() as usize).checked_add(start)?;
        if addr % align_of::<SharedFreeListNode>() != 0 {
            return None;
        }
        // SAFETY:
        // Bounds and alignment are validated above against this mapped region.
        Some(unsafe { &*(addr as *const SharedFreeListNode) })
    }
}

#[inline]
fn align_up(value: u32, align: u32) -> Option<u32> {
    if align == 0 || !align.is_power_of_two() {
        return None;
    }
    let mask = align - 1;
    value.checked_add(mask).map(|v| v & !mask)
}

#[cfg(test)]
mod tests {
    use super::{ArenaClass, ShmArena};

    #[test]
    fn class_segregated_reuse_does_not_cross_allocate_between_bins() {
        let shm = ShmArena::new(8 << 20).expect("shm");
        let arena = shm.chunked_arena();

        let spill32 = arena
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("alloc spill32");
        arena
            .recycle_raw_in_class(spill32, 32, 8, ArenaClass::Spill32)
            .expect("recycle spill32");

        let spill64 = arena
            .alloc_raw_in_class(64, 8, ArenaClass::Spill64)
            .expect("alloc spill64");
        assert_ne!(
            spill64, spill32,
            "spill64 should not consume spill32 free-list entries"
        );

        let spill32_reused = arena
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("alloc spill32 reused");
        assert_eq!(
            spill32_reused, spill32,
            "spill32 class should recycle its own offsets"
        );
    }

    #[test]
    fn local_recycle_cache_round_trips_same_class_offsets() {
        let shm = ShmArena::new(8 << 20).expect("shm");
        let arena = shm.chunked_arena();

        let first = arena
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("alloc spill32");
        arena
            .recycle_raw_in_class(first, 32, 8, ArenaClass::Spill32)
            .expect("recycle spill32");

        let second = arena
            .alloc_raw_in_class(32, 8, ArenaClass::Spill32)
            .expect("realloc spill32");
        assert_eq!(second, first);

        shm.flush_local_recycle_caches()
            .expect("flush local recycle caches");
    }
}
