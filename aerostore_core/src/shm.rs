use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::procarray::{
    ProcArray, ProcArrayError, ProcArrayRegistration, ProcSnapshot, PROCARRAY_SLOTS,
};

const SHM_HEADER_MAGIC: u32 = 0xAEB0_B007;
const SHM_HEADER_ALIGN: u32 = 64;
pub(crate) const OCC_PARTITION_LOCKS: usize = 1024;

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
struct ShmHeader {
    magic: u32,
    capacity: u32,
    data_start: u32,
    clean_shutdown: AtomicBool,
    boot_layout_offset: AtomicU32,
    next_txid: AtomicU64,
    proc_array: ProcArray,
    occ_partition_locks: [OccPartitionLock; OCC_PARTITION_LOCKS],
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
                capacity: self.len as u32,
                data_start,
                clean_shutdown: AtomicBool::new(true),
                boot_layout_offset: AtomicU32::new(0),
                next_txid: AtomicU64::new(1),
                proc_array: ProcArray::new(),
                occ_partition_locks: std::array::from_fn(|_| OccPartitionLock::new()),
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
    fn header_ref(&self) -> Option<&ShmHeader> {
        // SAFETY:
        // `header` points into the current mapping. We verify the magic before use.
        let header = unsafe { self.header.as_ref() };
        if header.magic != SHM_HEADER_MAGIC {
            None
        } else if header.capacity as usize != self.len {
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
        if size == 0 {
            return Err(ShmAllocError::ZeroSizedType);
        }
        if align == 0 || !align.is_power_of_two() {
            return Err(ShmAllocError::SizeOverflow);
        }

        let size = u32::try_from(size).map_err(|_| ShmAllocError::SizeOverflow)?;
        let align = u32::try_from(align).map_err(|_| ShmAllocError::SizeOverflow)?;
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
        if size_of::<T>() == 0 {
            return Err(ShmAllocError::ZeroSizedType);
        }

        let start = self.alloc_raw(size_of::<T>(), align_of::<T>())?;
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
}

#[inline]
fn align_up(value: u32, align: u32) -> Option<u32> {
    if align == 0 || !align.is_power_of_two() {
        return None;
    }
    let mask = align - 1;
    value.checked_add(mask).map(|v| v & !mask)
}
