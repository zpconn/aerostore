use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr;
use std::ptr::NonNull;

use rustix::fs::{open, Mode, OFlags};

use crate::{ShmArena, ShmError};

pub const DEFAULT_TMPFS_PATH: &str = "/dev/shm/aerostore.mmap";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TmpfsAttachMode {
    ColdStart,
    WarmStart,
}

pub struct TmpfsMappedArena {
    pub arena: ShmArena,
    pub mode: TmpfsAttachMode,
}

pub fn map_tmpfs_shared(
    path: impl AsRef<Path>,
    byte_len: usize,
) -> Result<TmpfsMappedArena, ShmError> {
    let path = path.as_ref();
    let pre_existing = path.exists();
    let fd = open(
        path,
        OFlags::RDWR | OFlags::CREATE,
        Mode::from_bits(0o600).unwrap_or(Mode::empty()),
    )
    .map_err(io_from_rustix)
    .map_err(ShmError::MmapFailed)?;

    if !pre_existing {
        truncate_fd(fd.as_raw_fd(), byte_len)?;
    } else {
        let current_len = std::fs::metadata(path).map_err(ShmError::MmapFailed)?.len() as usize;
        if current_len != byte_len {
            truncate_fd(fd.as_raw_fd(), byte_len)?;
        }
    }

    // SAFETY:
    // `fd` references a tmpfs file descriptor opened read/write with at least `byte_len` bytes.
    let map_ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            byte_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd.as_raw_fd(),
            0,
        )
    };
    if map_ptr == libc::MAP_FAILED {
        return Err(ShmError::MmapFailed(std::io::Error::last_os_error()));
    }

    let base = match NonNull::new(map_ptr.cast::<u8>()) {
        Some(ptr) => ptr,
        None => {
            // SAFETY:
            // mapping was created successfully above and must be released on constructor failure.
            unsafe {
                let _ = libc::munmap(map_ptr, byte_len);
            }
            return Err(ShmError::MmapFailed(std::io::Error::new(
                std::io::ErrorKind::Other,
                "mmap returned null",
            )));
        }
    };

    // SAFETY:
    // `base` points at a valid writable mapping of `byte_len` bytes.
    let arena = match unsafe { ShmArena::from_mapped_region(base, byte_len, false) } {
        Ok(arena) => arena,
        Err(err) => {
            // SAFETY:
            // mapping was created successfully above and must be released on constructor failure.
            unsafe {
                let _ = libc::munmap(map_ptr, byte_len);
            }
            return Err(err);
        }
    };

    let mode = if pre_existing && arena.is_header_valid() {
        TmpfsAttachMode::WarmStart
    } else {
        arena.reinitialize_header()?;
        TmpfsAttachMode::ColdStart
    };

    arena.set_clean_shutdown_flag(false);
    Ok(TmpfsMappedArena { arena, mode })
}

fn truncate_fd(fd: libc::c_int, byte_len: usize) -> Result<(), ShmError> {
    let len = libc::off_t::try_from(byte_len).map_err(|_| ShmError::InvalidSize(byte_len))?;
    // SAFETY:
    // `fd` is an open file descriptor and `len` was bounds-checked above.
    let rc = unsafe { libc::ftruncate(fd, len) };
    if rc == 0 {
        Ok(())
    } else {
        Err(ShmError::MmapFailed(std::io::Error::last_os_error()))
    }
}

fn io_from_rustix(err: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(err.raw_os_error())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{map_tmpfs_shared, TmpfsAttachMode};
    use crate::RelPtr;
    use std::sync::atomic::Ordering;

    fn unique_path() -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock moved backwards")
            .as_nanos();
        std::path::PathBuf::from(format!("/dev/shm/aerostore_shm_tmpfs_test.{}", nonce))
    }

    #[test]
    fn cold_then_warm_attach_reuses_existing_segment() {
        let path = unique_path();
        let _ = std::fs::remove_file(&path);

        let first = map_tmpfs_shared(path.as_path(), 8 << 20).expect("first map failed");
        assert_eq!(first.mode, TmpfsAttachMode::ColdStart);
        let value_ptr = first
            .arena
            .chunked_arena()
            .alloc(1234_u64)
            .expect("alloc failed");
        let offset = value_ptr.load(Ordering::Acquire);
        first.arena.set_boot_layout_offset(offset);
        drop(first);

        let second = map_tmpfs_shared(path.as_path(), 8 << 20).expect("second map failed");
        assert_eq!(second.mode, TmpfsAttachMode::WarmStart);
        let roundtrip_offset = second.arena.boot_layout_offset();
        assert_eq!(roundtrip_offset, offset);
        let observed = RelPtr::<u64>::from_offset(roundtrip_offset)
            .as_ref(second.arena.mmap_base())
            .copied()
            .expect("roundtrip pointer invalid");
        assert_eq!(observed, 1234_u64);

        let _ = std::fs::remove_file(path);
    }
}
