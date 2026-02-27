use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;

use crate::occ::{Error as OccError, OccTable, OccTransaction};
use crate::wal_ring::{
    deserialize_commit_record, serialize_commit_record, wal_commit_from_occ_record, SharedWalRing,
    SynchronousCommit, WalRingCommit, WalRingError,
};

#[derive(Debug)]
pub enum WalWriterError {
    Io(io::Error),
    Occ(OccError),
    Ring(WalRingError),
    Codec(String),
    InvalidMode(&'static str),
}

impl fmt::Display for WalWriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalWriterError::Io(err) => write!(f, "io error: {}", err),
            WalWriterError::Occ(err) => write!(f, "occ error: {}", err),
            WalWriterError::Ring(err) => write!(f, "ring error: {}", err),
            WalWriterError::Codec(msg) => write!(f, "codec error: {}", msg),
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
    let parent_pid = unsafe { libc::getpid() };

    // SAFETY:
    // `fork` is used intentionally to emulate a dedicated WAL writer OS process.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }

    if pid == 0 {
        if arm_parent_death_signal(parent_pid).is_err() {
            // SAFETY:
            // child exits immediately without unwinding parent runtime state.
            unsafe { libc::_exit(1) };
        }
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

#[cfg(target_os = "linux")]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> io::Result<()> {
    // SAFETY:
    // called in the child immediately after `fork` to ensure the WAL daemon receives
    // SIGTERM if its parent process disappears (normal exit or crash), preventing
    // orphaned background daemons in tests and CLI/Tcl hosts.
    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    // Handle the fork-to-prctl race where parent may have exited before we armed it.
    let observed_parent = unsafe { libc::getppid() };
    if observed_parent != expected_parent {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "parent exited before wal daemon armed PDEATHSIG",
        ));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn arm_parent_death_signal(_expected_parent: libc::pid_t) -> io::Result<()> {
    Ok(())
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
    ) -> Result<usize, WalWriterError>
    where
        T: serde::Serialize,
    {
        let record = table.commit_with_record(tx)?;
        let wal_commit = wal_commit_from_occ_record(&record)?;

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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OccRecoveryState {
    pub wal_records: usize,
    pub applied_writes: usize,
    pub max_txid: u64,
}

pub fn read_wal_file(path: impl AsRef<Path>) -> Result<Vec<WalRingCommit>, WalWriterError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let bytes = std::fs::read(path)?;
    let mut cursor = 0_usize;
    let mut commits = Vec::new();

    while cursor < bytes.len() {
        if bytes.len() - cursor < 4 {
            return Err(WalWriterError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "truncated wal length prefix at byte offset {} (remaining={})",
                    cursor,
                    bytes.len() - cursor
                ),
            )));
        }
        let mut len_buf = [0_u8; 4];
        len_buf.copy_from_slice(&bytes[cursor..cursor + 4]);
        cursor += 4;
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if cursor + frame_len > bytes.len() {
            return Err(WalWriterError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "truncated wal frame at byte offset {} (declared_len={}, remaining={})",
                    cursor,
                    frame_len,
                    bytes.len() - cursor
                ),
            )));
        }

        let frame = &bytes[cursor..cursor + frame_len];
        cursor += frame_len;
        commits.push(deserialize_commit_record(frame)?);
    }

    Ok(commits)
}

pub fn recover_occ_table_from_wal<T: Copy + Send + Sync + 'static>(
    table: &OccTable<T>,
    wal_path: impl AsRef<Path>,
) -> Result<OccRecoveryState, WalWriterError>
where
    T: DeserializeOwned,
{
    let commits = read_wal_file(wal_path)?;
    let mut applied_writes = 0_usize;
    let mut max_txid = 0_u64;

    for commit in &commits {
        if commit.txid > max_txid {
            max_txid = commit.txid;
        }
        for write in &commit.writes {
            let value: T = bincode::deserialize(write.value_payload.as_slice())
                .map_err(|err| WalWriterError::Codec(err.to_string()))?;
            table.apply_recovered_write(write.row_id as usize, commit.txid, value)?;
            applied_writes += 1;
        }
    }

    if max_txid > 0 {
        table.advance_global_txid_floor(max_txid.saturating_add(1));
    }

    Ok(OccRecoveryState {
        wal_records: commits.len(),
        applied_writes,
        max_txid,
    })
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
