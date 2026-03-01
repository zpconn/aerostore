use std::fmt;
use std::path::Path;
use std::sync::Arc;

use crate::shm_tmpfs::{map_tmpfs_shared, TmpfsAttachMode, DEFAULT_TMPFS_PATH};
use crate::{RelPtr, ShmAllocError, ShmArena, ShmError};

pub const BOOT_LAYOUT_MAGIC: u32 = 0xAEB0_4C59;
pub const BOOT_LAYOUT_VERSION: u32 = 1;
pub const BOOT_LAYOUT_MAX_INDEXES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootMode {
    ColdReplay,
    WarmAttach,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootLayout {
    pub magic: u32,
    pub version: u32,
    pub row_capacity: u32,
    pub occ_shared_header_offset: u32,
    pub occ_slot_offsets_offset: u32,
    pub occ_slot_offsets_len: u32,
    pub pk_header_offset: u32,
    pub pk_bucket_offsets_offset: u32,
    pub pk_bucket_offsets_len: u32,
    pub wal_ring_offset: u32,
    pub index_offsets: [u32; BOOT_LAYOUT_MAX_INDEXES],
    pub index_count: u32,
    pub _reserved: u32,
}

impl BootLayout {
    pub fn new(row_capacity: usize) -> Result<Self, BootloaderError> {
        let row_capacity_u32 = u32::try_from(row_capacity).map_err(|_| {
            BootloaderError::LayoutCorrupt(format!(
                "row_capacity {} does not fit into u32",
                row_capacity
            ))
        })?;
        Ok(Self {
            magic: BOOT_LAYOUT_MAGIC,
            version: BOOT_LAYOUT_VERSION,
            row_capacity: row_capacity_u32,
            occ_shared_header_offset: 0,
            occ_slot_offsets_offset: 0,
            occ_slot_offsets_len: 0,
            pk_header_offset: 0,
            pk_bucket_offsets_offset: 0,
            pk_bucket_offsets_len: 0,
            wal_ring_offset: 0,
            index_offsets: [0_u32; BOOT_LAYOUT_MAX_INDEXES],
            index_count: 0,
            _reserved: 0,
        })
    }
}

pub struct BootContext {
    pub shm: Arc<ShmArena>,
    pub mode: BootMode,
    pub orphaned_proc_slots_cleared: usize,
}

#[derive(Debug)]
pub enum BootloaderError {
    Shm(ShmError),
    Alloc(ShmAllocError),
    LayoutMissing,
    LayoutCorrupt(String),
}

impl fmt::Display for BootloaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BootloaderError::Shm(err) => write!(f, "shared memory error: {}", err),
            BootloaderError::Alloc(err) => write!(f, "shared allocation error: {}", err),
            BootloaderError::LayoutMissing => write!(f, "shared boot layout is missing"),
            BootloaderError::LayoutCorrupt(msg) => {
                write!(f, "shared boot layout is corrupt: {}", msg)
            }
        }
    }
}

impl std::error::Error for BootloaderError {}

impl From<ShmError> for BootloaderError {
    fn from(value: ShmError) -> Self {
        BootloaderError::Shm(value)
    }
}

impl From<ShmAllocError> for BootloaderError {
    fn from(value: ShmAllocError) -> Self {
        BootloaderError::Alloc(value)
    }
}

pub fn open_boot_context(
    shm_path: Option<&Path>,
    shm_bytes: usize,
) -> Result<BootContext, BootloaderError> {
    let shm_path = shm_path.unwrap_or_else(|| Path::new(DEFAULT_TMPFS_PATH));
    let mapped = map_tmpfs_shared(shm_path, shm_bytes)?;
    let shm = mapped.arena;

    let mode = match mapped.mode {
        TmpfsAttachMode::ColdStart => BootMode::ColdReplay,
        TmpfsAttachMode::WarmStart => match load_boot_layout(&shm) {
            Ok(Some(_)) => BootMode::WarmAttach,
            Ok(None) | Err(_) => {
                shm.reinitialize_header()?;
                BootMode::ColdReplay
            }
        },
    };

    let orphaned_proc_slots_cleared = if mode == BootMode::WarmAttach {
        shm.proc_array().clear_orphaned_slots()
    } else {
        0
    };

    shm.set_clean_shutdown_flag(false);
    Ok(BootContext {
        shm: Arc::new(shm),
        mode,
        orphaned_proc_slots_cleared,
    })
}

pub fn persist_boot_layout(shm: &ShmArena, layout: &BootLayout) -> Result<u32, BootloaderError> {
    validate_layout(layout)?;

    let existing_offset = shm.boot_layout_offset();
    if existing_offset == 0 {
        let ptr = shm.chunked_arena().alloc(*layout)?;
        let offset = ptr.load(std::sync::atomic::Ordering::Acquire);
        shm.set_boot_layout_offset(offset);
        Ok(offset)
    } else {
        let Some(existing_ref) =
            RelPtr::<BootLayout>::from_offset(existing_offset).as_ref(shm.mmap_base())
        else {
            return Err(BootloaderError::LayoutCorrupt(format!(
                "invalid existing layout pointer offset {}",
                existing_offset
            )));
        };
        let existing_ptr = existing_ref as *const BootLayout as *mut BootLayout;
        // SAFETY:
        // `existing_ptr` points to validated in-range shared memory location for BootLayout.
        unsafe {
            existing_ptr.write(*layout);
        }
        Ok(existing_offset)
    }
}

pub fn load_boot_layout(shm: &ShmArena) -> Result<Option<BootLayout>, BootloaderError> {
    let layout_offset = shm.boot_layout_offset();
    if layout_offset == 0 {
        return Ok(None);
    }
    let Some(layout_ref) = RelPtr::<BootLayout>::from_offset(layout_offset).as_ref(shm.mmap_base())
    else {
        return Err(BootloaderError::LayoutCorrupt(format!(
            "invalid layout offset {}",
            layout_offset
        )));
    };
    validate_layout(layout_ref)?;
    Ok(Some(*layout_ref))
}

pub fn alloc_u32_array(shm: &ShmArena, values: &[u32]) -> Result<(u32, u32), BootloaderError> {
    if values.is_empty() {
        return Ok((0, 0));
    }
    let byte_len = values
        .len()
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| {
            BootloaderError::LayoutCorrupt("u32 array byte length overflow".to_string())
        })?;
    let offset = shm
        .chunked_arena()
        .alloc_raw(byte_len, std::mem::align_of::<u32>())?;
    let base = shm.mmap_base().as_ptr();
    let dst = (base as usize)
        .checked_add(offset as usize)
        .ok_or_else(|| BootloaderError::LayoutCorrupt("u32 array offset overflow".to_string()))?
        as *mut u8;
    // SAFETY:
    // `alloc_raw` reserved a unique writable span `[offset, offset+byte_len)`.
    unsafe {
        std::ptr::copy_nonoverlapping(values.as_ptr().cast::<u8>(), dst, byte_len);
    }
    Ok((offset, values.len() as u32))
}

pub fn read_u32_array(shm: &ShmArena, offset: u32, len: u32) -> Result<Vec<u32>, BootloaderError> {
    if offset == 0 || len == 0 {
        return Ok(Vec::new());
    }
    let total_bytes = (len as usize)
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| {
            BootloaderError::LayoutCorrupt("u32 array byte length overflow".to_string())
        })?;
    let start = offset as usize;
    let end = start
        .checked_add(total_bytes)
        .ok_or_else(|| BootloaderError::LayoutCorrupt("u32 array range overflow".to_string()))?;
    if end > shm.len() {
        return Err(BootloaderError::LayoutCorrupt(format!(
            "u32 array out of bounds offset={} len={} bytes={} arena_len={}",
            offset,
            len,
            total_bytes,
            shm.len()
        )));
    }
    let src = (shm.mmap_base().as_ptr() as usize + start) as *const u32;
    // SAFETY:
    // bounds above guarantee we can read `len` elements from `src`.
    let slice = unsafe { std::slice::from_raw_parts(src, len as usize) };
    Ok(slice.to_vec())
}

pub fn clear_persisted_boot_layout(shm: &ShmArena) {
    shm.set_boot_layout_offset(0);
}

fn validate_layout(layout: &BootLayout) -> Result<(), BootloaderError> {
    if layout.magic != BOOT_LAYOUT_MAGIC {
        return Err(BootloaderError::LayoutCorrupt(format!(
            "layout magic {} != expected {}",
            layout.magic, BOOT_LAYOUT_MAGIC
        )));
    }
    if layout.version != BOOT_LAYOUT_VERSION {
        return Err(BootloaderError::LayoutCorrupt(format!(
            "layout version {} != expected {}",
            layout.version, BOOT_LAYOUT_VERSION
        )));
    }
    if layout.row_capacity == 0 {
        return Err(BootloaderError::LayoutCorrupt(
            "row_capacity must be > 0".to_string(),
        ));
    }
    if layout.index_count as usize > BOOT_LAYOUT_MAX_INDEXES {
        return Err(BootloaderError::LayoutCorrupt(format!(
            "index_count {} exceeds max {}",
            layout.index_count, BOOT_LAYOUT_MAX_INDEXES
        )));
    }
    Ok(())
}
