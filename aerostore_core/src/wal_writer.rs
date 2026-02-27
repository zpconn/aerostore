use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::occ::{Error as OccError, OccTable, OccTransaction};
use crate::wal_ring::{
    deserialize_commit_record, serialize_commit_record, SharedWalRing, SynchronousCommit,
    WalRingCommit, WalRingError,
};

#[derive(Debug)]
pub enum WalWriterError {
    Io(io::Error),
    Occ(OccError),
    Ring(WalRingError),
    InvalidMode(&'static str),
}

impl fmt::Display for WalWriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalWriterError::Io(err) => write!(f, "io error: {}", err),
            WalWriterError::Occ(err) => write!(f, "occ error: {}", err),
            WalWriterError::Ring(err) => write!(f, "ring error: {}", err),
            WalWriterError::InvalidMode(msg) => write!(f, "invalid wal writer mode: {}", msg),
        }
    }
}

impl std::error::Error for WalWriterError {}

impl From<io::Error> for WalWriterError {
    fn from(value: io::Error) -> Self {
        WalWriterError::Io(value)
    }
}

impl From<OccError> for WalWriterError {
    fn from(value: OccError) -> Self {
        WalWriterError::Occ(value)
    }
}

impl From<WalRingError> for WalWriterError {
    fn from(value: WalRingError) -> Self {
        WalWriterError::Ring(value)
    }
}

pub struct SyncWalWriter {
    file: File,
}

impl SyncWalWriter {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn append_commit(&mut self, commit: &WalRingCommit) -> Result<(), WalWriterError> {
        let payload = serialize_commit_record(commit)?;
        self.append_payload_sync(payload.as_slice())?;
        Ok(())
    }

    fn append_payload_sync(&mut self, payload: &[u8]) -> io::Result<()> {
        let len = payload.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(payload)?;
        self.file.flush()?;
        fdatasync(self.file.as_raw_fd())
    }
}

pub struct WalWriterDaemon {
    pid: libc::pid_t,
}

impl WalWriterDaemon {
    #[inline]
    pub fn pid(&self) -> libc::pid_t {
        self.pid
    }

    pub fn terminate(&self, signal: libc::c_int) -> io::Result<()> {
        // SAFETY:
        // sending signal to child process created by `fork`.
        let rc = unsafe { libc::kill(self.pid, signal) };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn join(self) -> io::Result<()> {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // waiting for child process created by `fork`.
        let waited = unsafe { libc::waitpid(self.pid, &mut status as *mut libc::c_int, 0) };
        if waited != self.pid {
            return Err(io::Error::last_os_error());
        }
        if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "wal writer daemon exited unexpectedly (status={})",
                    libc::WEXITSTATUS(status)
                ),
            ));
        }
        Ok(())
    }

    pub fn join_any_status(self) -> io::Result<libc::c_int> {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // waiting for child process created by `fork`.
        let waited = unsafe { libc::waitpid(self.pid, &mut status as *mut libc::c_int, 0) };
        if waited != self.pid {
            return Err(io::Error::last_os_error());
        }
        Ok(status)
    }
}

pub fn spawn_wal_writer_daemon<const SLOTS: usize, const SLOT_BYTES: usize>(
    ring: SharedWalRing<SLOTS, SLOT_BYTES>,
    wal_path: impl AsRef<Path>,
) -> io::Result<WalWriterDaemon> {
    let wal_path = wal_path.as_ref().to_path_buf();

    // SAFETY:
    // `fork` is used intentionally to emulate a dedicated WAL writer OS process.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }

    if pid == 0 {
        let code = match wal_writer_daemon_loop(ring, &wal_path) {
            Ok(_) => 0_i32,
            Err(_) => 1_i32,
        };
        // SAFETY:
        // child exits immediately without unwinding parent runtime state.
        unsafe { libc::_exit(code) };
    }

    Ok(WalWriterDaemon { pid })
}

fn wal_writer_daemon_loop<const SLOTS: usize, const SLOT_BYTES: usize>(
    ring: SharedWalRing<SLOTS, SLOT_BYTES>,
    wal_path: &Path,
) -> Result<(), WalWriterError> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(wal_path)?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);
    let mut last_sync = Instant::now();

    loop {
        match ring.pop_bytes()? {
            Some(payload) => {
                // Validate payload shape to fail fast on corruption.
                let _ = deserialize_commit_record(payload.as_slice())?;
                let len = payload.len() as u32;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(payload.as_slice())?;
            }
            None => {
                if ring.is_closed()? && ring.is_empty()? {
                    break;
                }
                std::thread::yield_now();
                std::thread::sleep(Duration::from_micros(50));
            }
        }

        if last_sync.elapsed() >= Duration::from_secs(10) {
            writer.flush()?;
            fdatasync(writer.get_ref().as_raw_fd())?;
            last_sync = Instant::now();
        }
    }

    writer.flush()?;
    fdatasync(writer.get_ref().as_raw_fd())?;
    Ok(())
}

#[derive(Clone)]
enum CommitSink<const SLOTS: usize, const SLOT_BYTES: usize> {
    Synchronous(PathBuf),
    Asynchronous(SharedWalRing<SLOTS, SLOT_BYTES>),
}

pub struct OccCommitter<const SLOTS: usize, const SLOT_BYTES: usize> {
    mode: SynchronousCommit,
    sink: CommitSink<SLOTS, SLOT_BYTES>,
    sync_writer: Option<SyncWalWriter>,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> OccCommitter<SLOTS, SLOT_BYTES> {
    pub fn new_synchronous(wal_path: impl AsRef<Path>) -> Result<Self, WalWriterError> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sync_writer = SyncWalWriter::open(&wal_path)?;
        Ok(Self {
            mode: SynchronousCommit::On,
            sink: CommitSink::Synchronous(wal_path),
            sync_writer: Some(sync_writer),
        })
    }

    pub fn new_asynchronous(ring: SharedWalRing<SLOTS, SLOT_BYTES>) -> Self {
        Self {
            mode: SynchronousCommit::Off,
            sink: CommitSink::Asynchronous(ring),
            sync_writer: None,
        }
    }

    pub fn from_mode(
        mode: SynchronousCommit,
        wal_path: impl AsRef<Path>,
        ring: Option<SharedWalRing<SLOTS, SLOT_BYTES>>,
    ) -> Result<Self, WalWriterError> {
        match mode {
            SynchronousCommit::On => Self::new_synchronous(wal_path),
            SynchronousCommit::Off => {
                let Some(ring) = ring else {
                    return Err(WalWriterError::InvalidMode(
                        "synchronous_commit=off requires a ring buffer",
                    ));
                };
                Ok(Self::new_asynchronous(ring))
            }
        }
    }

    #[inline]
    pub fn mode(&self) -> SynchronousCommit {
        self.mode
    }

    pub fn commit<T: Copy + Send + Sync + 'static>(
        &mut self,
        table: &OccTable<T>,
        tx: &mut OccTransaction<T>,
    ) -> Result<usize, WalWriterError> {
        let record = table.commit_with_record(tx)?;
        let wal_commit = WalRingCommit::from(&record);

        match &self.sink {
            CommitSink::Synchronous(_) => {
                let Some(sync_writer) = self.sync_writer.as_mut() else {
                    return Err(WalWriterError::InvalidMode(
                        "missing sync writer for synchronous mode",
                    ));
                };
                sync_writer.append_commit(&wal_commit)?;
            }
            CommitSink::Asynchronous(ring) => {
                ring.push_commit_record(&wal_commit)?;
            }
        }

        Ok(record.writes.len())
    }

    #[inline]
    pub fn wal_path(&self) -> Option<&Path> {
        match &self.sink {
            CommitSink::Synchronous(path) => Some(path.as_path()),
            CommitSink::Asynchronous(_) => None,
        }
    }
}

fn fdatasync(fd: libc::c_int) -> io::Result<()> {
    // SAFETY:
    // fd is owned and valid for this process while file handle is alive.
    let rc = unsafe { libc::fdatasync(fd) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
