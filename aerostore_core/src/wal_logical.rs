use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::wal_ring::{SharedWalRing, WalRingError};
use crate::ShmArena;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    Upsert {
        txid: u64,
        table: String,
        pk: String,
        payload: Vec<u8>,
    },
    Delete {
        txid: u64,
        table: String,
        pk: String,
    },
    Commit {
        txid: u64,
    },
}

impl WalRecord {
    #[inline]
    pub fn txid(&self) -> u64 {
        match self {
            WalRecord::Upsert { txid, .. } => *txid,
            WalRecord::Delete { txid, .. } => *txid,
            WalRecord::Commit { txid } => *txid,
        }
    }
}

#[derive(Debug)]
pub enum LogicalWalError {
    Io(io::Error),
    Ring(WalRingError),
    Codec(String),
    CorruptFrame { offset: usize, reason: String },
}

impl fmt::Display for LogicalWalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalWalError::Io(err) => write!(f, "io error: {}", err),
            LogicalWalError::Ring(err) => write!(f, "ring error: {}", err),
            LogicalWalError::Codec(msg) => write!(f, "codec error: {}", msg),
            LogicalWalError::CorruptFrame { offset, reason } => {
                write!(f, "corrupt wal frame at offset {}: {}", offset, reason)
            }
        }
    }
}

impl std::error::Error for LogicalWalError {}

impl From<io::Error> for LogicalWalError {
    fn from(value: io::Error) -> Self {
        LogicalWalError::Io(value)
    }
}

impl From<WalRingError> for LogicalWalError {
    fn from(value: WalRingError) -> Self {
        LogicalWalError::Ring(value)
    }
}

pub fn serialize_wal_record(record: &WalRecord) -> Result<Vec<u8>, LogicalWalError> {
    bincode::serialize(record).map_err(|err| LogicalWalError::Codec(err.to_string()))
}

pub fn deserialize_wal_record(payload: &[u8]) -> Result<WalRecord, LogicalWalError> {
    bincode::deserialize(payload).map_err(|err| LogicalWalError::Codec(err.to_string()))
}

#[derive(Clone)]
pub struct LogicalWalRing<const SLOTS: usize, const SLOT_BYTES: usize> {
    inner: SharedWalRing<SLOTS, SLOT_BYTES>,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> LogicalWalRing<SLOTS, SLOT_BYTES> {
    pub fn create(shm: std::sync::Arc<ShmArena>) -> Result<Self, LogicalWalError> {
        let inner = SharedWalRing::<SLOTS, SLOT_BYTES>::create(shm)?;
        Ok(Self { inner })
    }

    #[inline]
    pub fn from_shared(inner: SharedWalRing<SLOTS, SLOT_BYTES>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn as_shared(&self) -> SharedWalRing<SLOTS, SLOT_BYTES> {
        self.inner.clone()
    }

    pub fn push_record(&self, record: &WalRecord) -> Result<(), LogicalWalError> {
        let payload = serialize_wal_record(record)?;
        self.inner.push_bytes_blocking(payload.as_slice())?;
        Ok(())
    }

    pub fn pop_record(&self) -> Result<Option<WalRecord>, LogicalWalError> {
        let Some(payload) = self.inner.pop_bytes()? else {
            return Ok(None);
        };
        Ok(Some(deserialize_wal_record(payload.as_slice())?))
    }

    #[inline]
    pub fn close(&self) -> Result<(), LogicalWalError> {
        self.inner.close()?;
        Ok(())
    }

    #[inline]
    pub fn is_closed(&self) -> Result<bool, LogicalWalError> {
        Ok(self.inner.is_closed()?)
    }

    #[inline]
    pub fn is_empty(&self) -> Result<bool, LogicalWalError> {
        Ok(self.inner.is_empty()?)
    }
}

pub struct LogicalWalWriterDaemon {
    pid: libc::pid_t,
    ack_read_fd: libc::c_int,
    last_acked_txid: u64,
}

impl LogicalWalWriterDaemon {
    #[inline]
    pub fn pid(&self) -> libc::pid_t {
        self.pid
    }

    pub fn wait_for_txid_ack(&mut self, target_txid: u64) -> io::Result<()> {
        if self.ack_read_fd < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "txid acknowledgements are disabled for asynchronous logical WAL mode",
            ));
        }

        while self.last_acked_txid < target_txid {
            let mut buf = [0_u8; 8];
            let mut read = 0_usize;
            while read < buf.len() {
                // SAFETY:
                // ack_read_fd is a valid read-end of the ack pipe held by this process.
                let rc = unsafe {
                    libc::read(
                        self.ack_read_fd,
                        buf[read..].as_mut_ptr() as *mut libc::c_void,
                        buf.len() - read,
                    )
                };
                if rc == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "logical wal ack pipe closed before requested txid ack",
                    ));
                }
                if rc < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err);
                }
                read += rc as usize;
            }
            self.last_acked_txid = u64::from_le_bytes(buf);
        }
        Ok(())
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

    pub fn join(mut self) -> io::Result<()> {
        let status = self.wait_for_status()?;
        if libc::WIFSIGNALED(status) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "logical wal writer daemon terminated by signal {}",
                    libc::WTERMSIG(status)
                ),
            ));
        }
        if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "logical wal writer daemon exited unexpectedly (status={})",
                    libc::WEXITSTATUS(status)
                ),
            ));
        }
        Ok(())
    }

    pub fn join_any_status(mut self) -> io::Result<libc::c_int> {
        self.wait_for_status()
    }

    fn wait_for_status(&mut self) -> io::Result<libc::c_int> {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // waiting for child process created by `fork`.
        let waited = unsafe { libc::waitpid(self.pid, &mut status as *mut libc::c_int, 0) };
        if waited != self.pid {
            return Err(io::Error::last_os_error());
        }
        if self.ack_read_fd >= 0 {
            // SAFETY:
            // closing fd owned by this process.
            unsafe {
                libc::close(self.ack_read_fd);
            }
            self.ack_read_fd = -1;
        }
        Ok(status)
    }
}

impl Drop for LogicalWalWriterDaemon {
    fn drop(&mut self) {
        if self.ack_read_fd >= 0 {
            // SAFETY:
            // closing fd owned by this process.
            unsafe {
                libc::close(self.ack_read_fd);
            }
            self.ack_read_fd = -1;
        }
    }
}

pub fn spawn_logical_wal_writer_daemon<const SLOTS: usize, const SLOT_BYTES: usize>(
    ring: LogicalWalRing<SLOTS, SLOT_BYTES>,
    wal_path: impl AsRef<Path>,
    synchronous_commit: bool,
) -> io::Result<LogicalWalWriterDaemon> {
    let wal_path = wal_path.as_ref().to_path_buf();
    let parent_pid = unsafe { libc::getpid() };
    let mut ack_pipe = [-1_i32; 2];

    if synchronous_commit {
        // SAFETY:
        // creating a local unidirectional pipe for durable commit acknowledgements.
        let pipe_rc = unsafe { libc::pipe(ack_pipe.as_mut_ptr()) };
        if pipe_rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    // SAFETY:
    // `fork` is intentional: WAL writer must run as a separate process.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        if synchronous_commit {
            // SAFETY:
            // cleanup local pipe descriptors on fork failure.
            unsafe {
                libc::close(ack_pipe[0]);
                libc::close(ack_pipe[1]);
            }
        }
        return Err(io::Error::last_os_error());
    }

    if pid == 0 {
        if synchronous_commit {
            // SAFETY:
            // child only writes ack notifications.
            unsafe {
                libc::close(ack_pipe[0]);
            }
        }
        if arm_parent_death_signal(parent_pid).is_err() {
            // SAFETY:
            // child exits without unwinding parent state.
            unsafe { libc::_exit(1) };
        }
        let code = match logical_wal_writer_loop(
            ring,
            &wal_path,
            if synchronous_commit { ack_pipe[1] } else { -1 },
            synchronous_commit,
        ) {
            Ok(_) => 0_i32,
            Err(_) => 1_i32,
        };
        if synchronous_commit {
            // SAFETY:
            // child closes its writer fd before exit.
            unsafe {
                libc::close(ack_pipe[1]);
            }
        }
        // SAFETY:
        // child exits without unwinding parent state.
        unsafe { libc::_exit(code) };
    }

    let mut ack_read_fd = -1_i32;
    if synchronous_commit {
        // SAFETY:
        // parent only reads ack notifications.
        unsafe {
            libc::close(ack_pipe[1]);
        }
        ack_read_fd = ack_pipe[0];
    }

    Ok(LogicalWalWriterDaemon {
        pid,
        ack_read_fd,
        last_acked_txid: 0,
    })
}

#[cfg(target_os = "linux")]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> io::Result<()> {
    // SAFETY:
    // called immediately after fork in child.
    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    let observed_parent = unsafe { libc::getppid() };
    if observed_parent != expected_parent {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "parent exited before logical wal daemon armed PDEATHSIG",
        ));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn arm_parent_death_signal(_expected_parent: libc::pid_t) -> io::Result<()> {
    Ok(())
}

fn logical_wal_writer_loop<const SLOTS: usize, const SLOT_BYTES: usize>(
    ring: LogicalWalRing<SLOTS, SLOT_BYTES>,
    wal_path: &Path,
    ack_write_fd: libc::c_int,
    synchronous_commit: bool,
) -> Result<(), LogicalWalError> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(wal_path)?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);
    let mut last_sync = Instant::now();

    loop {
        match ring.pop_record()? {
            Some(record) => {
                let payload = serialize_wal_record(&record)?;
                let len = payload.len() as u32;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(payload.as_slice())?;

                if let WalRecord::Commit { txid } = record {
                    if synchronous_commit {
                        writer.flush()?;
                        fdatasync(writer.get_ref().as_raw_fd())?;
                        if ack_write_fd >= 0 {
                            write_ack_txid(ack_write_fd, txid)?;
                        }
                        last_sync = Instant::now();
                    }
                }
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

pub fn read_logical_wal_records(path: impl AsRef<Path>) -> Result<Vec<WalRecord>, LogicalWalError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let bytes = std::fs::read(path)?;
    let mut cursor = 0_usize;
    let mut out = Vec::new();

    while cursor < bytes.len() {
        let frame_offset = cursor;
        if bytes.len() - cursor < 4 {
            return Err(LogicalWalError::CorruptFrame {
                offset: frame_offset,
                reason: format!(
                    "truncated length prefix (remaining={})",
                    bytes.len() - cursor
                ),
            });
        }
        let mut len_buf = [0_u8; 4];
        len_buf.copy_from_slice(&bytes[cursor..cursor + 4]);
        cursor += 4;
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if cursor + frame_len > bytes.len() {
            return Err(LogicalWalError::CorruptFrame {
                offset: frame_offset,
                reason: format!(
                    "truncated frame (declared_len={}, remaining={})",
                    frame_len,
                    bytes.len() - cursor
                ),
            });
        }
        let frame = &bytes[cursor..cursor + frame_len];
        cursor += frame_len;
        let decoded = deserialize_wal_record(frame).map_err(|err| match err {
            LogicalWalError::Codec(msg) => LogicalWalError::CorruptFrame {
                offset: frame_offset,
                reason: format!("decode failed: {}", msg),
            },
            other => other,
        })?;
        out.push(decoded);
    }

    Ok(out)
}

pub fn append_logical_wal_record(
    path: impl AsRef<Path>,
    record: &WalRecord,
) -> Result<(), LogicalWalError> {
    let payload = serialize_wal_record(record)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(payload.as_slice())?;
    Ok(())
}

pub fn truncate_logical_wal(path: impl AsRef<Path>) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.sync_all()
}

fn fdatasync(fd: libc::c_int) -> io::Result<()> {
    // SAFETY:
    // fd belongs to a live file descriptor in this process.
    let rc = unsafe { libc::fdatasync(fd) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn write_ack_txid(fd: libc::c_int, txid: u64) -> Result<(), LogicalWalError> {
    let bytes = txid.to_le_bytes();
    let mut written = 0_usize;
    while written < bytes.len() {
        // SAFETY:
        // fd is the write end of the parent ack pipe in the child process.
        let rc = unsafe {
            libc::write(
                fd,
                bytes[written..].as_ptr() as *const libc::c_void,
                bytes.len() - written,
            )
        };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(LogicalWalError::Io(err));
        }
        written += rc as usize;
    }
    Ok(())
}
