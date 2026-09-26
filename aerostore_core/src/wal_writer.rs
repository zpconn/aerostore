use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::occ::{Error as OccError, OccCommitRecord, OccTable, OccTransaction};
use crate::recovery_delta::{replay_update_record, replay_update_record_with_pk_map};
use crate::wal_delta::{deserialize_wal_record as deserialize_delta_wal_record, WalDeltaCodec};
use crate::wal_ring::{
    deserialize_commit_record, serialize_commit_record, wal_commit_from_occ_record,
    wal_commit_from_occ_record_with_policy, SharedWalRing, SynchronousCommit, WalEncodingPolicy,
    WalRingCommit, WalRingError, WalRingWrite,
};
use crate::ShmPrimaryKeyMap;

#[derive(Debug)]
pub enum WalWriterError {
    Io(io::Error),
    Occ(OccError),
    Ring(WalRingError),
    Codec(String),
    InvalidMode(&'static str),
    /// A failed append could not be durably removed. Further commits must stop
    /// until exclusive recovery determines the surviving transaction history.
    Indeterminate {
        append: io::Error,
        rollback: io::Error,
    },
    /// WAL accepted the transaction but volatile publication failed. Recovery
    /// may commit it, so the caller must not treat this as an ordinary retry.
    PublicationAfterWal(OccError),
}

impl fmt::Display for WalWriterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalWriterError::Io(err) => write!(f, "io error: {}", err),
            WalWriterError::Occ(err) => write!(f, "occ error: {}", err),
            WalWriterError::Ring(err) => write!(f, "ring error: {}", err),
            WalWriterError::Codec(msg) => write!(f, "codec error: {}", msg),
            WalWriterError::InvalidMode(msg) => write!(f, "invalid wal writer mode: {}", msg),
            WalWriterError::Indeterminate { append, rollback } => write!(
                f,
                "indeterminate WAL append ({append}); durable tail rollback failed ({rollback}); recovery required"
            ),
            WalWriterError::PublicationAfterWal(err) => write!(
                f,
                "indeterminate publication after WAL acceptance ({err}); recovery required"
            ),
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
    stream_identity: [u64; 4],
    path: PathBuf,
    opener_pid: libc::pid_t,
}

impl SyncWalWriter {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        sync_parent_directory(path)?;
        let stream_identity = wal_file_identity(&file)?;
        Ok(Self {
            file,
            stream_identity,
            path: path.to_path_buf(),
            opener_pid: unsafe { libc::getpid() },
        })
    }

    /// Append a complete frame without an OccTable health boundary. Raw users
    /// must stop and recover after an indeterminate error; this writer cannot
    /// propagate poison to other independent writers by itself.
    pub fn append_commit(&mut self, commit: &WalRingCommit) -> Result<(), WalWriterError> {
        let payload = serialize_commit_record(commit)?;
        self.append_payload_sync(payload.as_slice())?;
        Ok(())
    }

    fn append_payload_sync(&mut self, payload: &[u8]) -> Result<(), WalWriterError> {
        self.append_payload_sync_with_health(payload, || Ok(()), || {})
    }

    fn append_payload_sync_with_health<C, P>(
        &mut self,
        payload: &[u8],
        check_health: C,
        poison: P,
    ) -> Result<(), WalWriterError>
    where
        C: FnOnce() -> Result<(), WalWriterError>,
        P: FnOnce(),
    {
        let pid = unsafe { libc::getpid() };
        if pid != self.opener_pid {
            // flock ownership follows an open-file description, which fork
            // shares. Reopen once in each child so its lock excludes the parent.
            let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(&self.path)?;
            if wal_file_identity(&file)? != self.stream_identity {
                return Err(WalWriterError::InvalidMode(
                    "inherited WAL path now names a different file",
                ));
            }
            self.file = file;
            self.opener_pid = pid;
        }
        #[cfg(test)]
        poison_regressions::before_file_lock();
        let _lock = WalFileLock::exclusive(self.file.as_raw_fd())?;
        // A managed writer may have waited behind another writer whose failed
        // tail rollback poisoned this same table. Observe that state only after
        // owning the file lock, before extending potentially uncertain bytes.
        check_health()?;
        let original_len = self.file.metadata()?.len();
        let len = payload.len() as u32;
        let append = (|| -> io::Result<()> {
            self.file.write_all(&len.to_le_bytes())?;
            self.file.write_all(payload)?;
            fdatasync(self.file.as_raw_fd())
        })();
        match append {
            Ok(()) => Ok(()),
            Err(append) => match self
                .file
                .set_len(original_len)
                .and_then(|()| fdatasync(self.file.as_raw_fd()))
            {
                Ok(()) => Err(WalWriterError::Io(append)),
                Err(rollback) => {
                    // Publish the detected failure before another managed
                    // writer can acquire this file lock and check table health.
                    poison();
                    Err(WalWriterError::Indeterminate { append, rollback })
                }
            },
        }
    }
}

pub struct WalWriterDaemon {
    pid: libc::pid_t,
    release_writer: Option<Box<dyn FnOnce() + Send + Sync>>,
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

    pub fn join(mut self) -> io::Result<()> {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // waiting for child process created by `fork`.
        let waited = unsafe { libc::waitpid(self.pid, &mut status as *mut libc::c_int, 0) };
        if waited != self.pid {
            return Err(io::Error::last_os_error());
        }
        if let Some(release) = self.release_writer.take() {
            release();
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

    pub fn join_any_status(mut self) -> io::Result<libc::c_int> {
        let mut status: libc::c_int = 0;
        // SAFETY:
        // waiting for child process created by `fork`.
        let waited = unsafe { libc::waitpid(self.pid, &mut status as *mut libc::c_int, 0) };
        if waited != self.pid {
            return Err(io::Error::last_os_error());
        }
        if let Some(release) = self.release_writer.take() {
            release();
        }
        Ok(status)
    }
}

pub fn spawn_wal_writer_daemon<const SLOTS: usize, const SLOT_BYTES: usize>(
    ring: SharedWalRing<SLOTS, SLOT_BYTES>,
    wal_path: impl AsRef<Path>,
) -> io::Result<WalWriterDaemon> {
    let wal_path = wal_path.as_ref().to_path_buf();
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&wal_path)?;
    sync_parent_directory(&wal_path)?;
    let identity = wal_file_identity(&file)?;
    ring.claim_writer(identity[1], identity[2])
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    let parent_pid = unsafe { libc::getpid() };
    // Advance the writer epoch in the parent before forking so any immediately
    // following commits observe the new epoch and force full-row baselines.
    if let Err(err) = ring.bump_writer_epoch() {
        let _ = ring.release_writer_after_join();
        return Err(io::Error::new(io::ErrorKind::Other, err.to_string()));
    }

    // SAFETY:
    // `fork` is used intentionally to emulate a dedicated WAL writer OS process.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let error = io::Error::last_os_error();
        let _ = ring.release_writer_after_join();
        return Err(error);
    }

    if pid == 0 {
        if arm_parent_death_signal(parent_pid).is_err() {
            // SAFETY:
            // child exits immediately without unwinding parent runtime state.
            unsafe { libc::_exit(1) };
        }
        let code = match wal_writer_daemon_loop(ring, file) {
            Ok(_) => 0_i32,
            Err(_) => 1_i32,
        };
        // SAFETY:
        // child exits immediately without unwinding parent runtime state.
        unsafe { libc::_exit(code) };
    }

    Ok(WalWriterDaemon {
        pid,
        release_writer: Some(Box::new(move || {
            let _ = ring.release_writer_after_join();
        })),
    })
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
    mut file: File,
) -> Result<(), WalWriterError> {
    // Explicit complete-frame batches avoid BufWriter's implicit flush in the
    // middle of a frame. The file lock protects actual I/O, not each enqueue.
    let mut pending = Vec::with_capacity(1 << 20);
    let mut last_sync = Instant::now();

    loop {
        match ring.pop_bytes()? {
            Some(payload) => {
                // Validate payload shape to fail fast on corruption.
                let _ = deserialize_commit_record(payload.as_slice())?;
                let len = payload.len() as u32;
                pending.extend_from_slice(&len.to_le_bytes());
                pending.extend_from_slice(payload.as_slice());
                if pending.len() >= 1 << 20 {
                    flush_wal_batch(&mut file, &mut pending, false)?;
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
            flush_wal_batch(&mut file, &mut pending, true)?;
            last_sync = Instant::now();
        }
    }

    flush_wal_batch(&mut file, &mut pending, true)?;
    Ok(())
}

fn flush_wal_batch(file: &mut File, pending: &mut Vec<u8>, sync: bool) -> io::Result<()> {
    let _lock = WalFileLock::exclusive(file.as_raw_fd())?;
    file.write_all(pending)?;
    pending.clear();
    if sync {
        fdatasync(file.as_raw_fd())?;
    }
    Ok(())
}

#[derive(Clone)]
enum CommitSink<const SLOTS: usize, const SLOT_BYTES: usize> {
    Synchronous(PathBuf),
    Asynchronous(SharedWalRing<SLOTS, SLOT_BYTES>),
}

#[derive(Clone, Debug, Default)]
struct AsyncDeltaState {
    observed_writer_epoch: u64,
    rows_with_full_baseline_in_epoch: HashSet<u64>,
    pending_full_baselines: Vec<u64>,
}

pub struct OccCommitter<const SLOTS: usize, const SLOT_BYTES: usize> {
    mode: SynchronousCommit,
    sink: CommitSink<SLOTS, SLOT_BYTES>,
    sync_writer: Option<SyncWalWriter>,
    async_delta_state: Option<AsyncDeltaState>,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> OccCommitter<SLOTS, SLOT_BYTES> {
    pub fn new_synchronous(wal_path: impl AsRef<Path>) -> Result<Self, WalWriterError> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sync_writer = SyncWalWriter::open(&wal_path)?;
        Ok(Self {
            mode: SynchronousCommit::On,
            sink: CommitSink::Synchronous(wal_path),
            sync_writer: Some(sync_writer),
            async_delta_state: None,
        })
    }

    pub fn new_asynchronous(ring: SharedWalRing<SLOTS, SLOT_BYTES>) -> Self {
        Self {
            mode: SynchronousCommit::Off,
            sink: CommitSink::Asynchronous(ring),
            sync_writer: None,
            async_delta_state: Some(AsyncDeltaState::default()),
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

    fn stream_identity_for_table<T: Copy + Send + Sync + 'static>(
        &self,
        table: &OccTable<T>,
    ) -> Result<[u64; 4], WalWriterError> {
        match &self.sink {
            CommitSink::Synchronous(_) => self
                .sync_writer
                .as_ref()
                .map(|writer| writer.stream_identity)
                .ok_or(WalWriterError::InvalidMode(
                    "missing synchronous WAL writer",
                )),
            CommitSink::Asynchronous(ring) => {
                if !std::sync::Arc::ptr_eq(table.shared_arena(), ring.shared_arena()) {
                    return Err(WalWriterError::InvalidMode(
                        "asynchronous WAL ring and table must share one local arena handle",
                    ));
                }
                Ok([
                    2,
                    u64::from(ring.ring_ptr().load(std::sync::atomic::Ordering::Acquire)),
                    SLOTS as u64,
                    SLOT_BYTES as u64,
                ])
            }
        }
    }

    /// Check a proposed mode/stream without changing table state. Once bound,
    /// mode changes require draining and exclusive cold recovery into a table.
    pub fn validate_for_table<T: Copy + Send + Sync + 'static>(
        &self,
        table: &OccTable<T>,
    ) -> Result<(), WalWriterError> {
        table.check_wal_stream(self.stream_identity_for_table(table)?)?;
        Ok(())
    }

    pub fn commit<T: Copy + Send + Sync + 'static>(
        &mut self,
        table: &OccTable<T>,
        tx: &mut OccTransaction<T>,
    ) -> Result<usize, WalWriterError>
    where
        T: WalDeltaCodec,
    {
        let identity = self.stream_identity_for_table(table)?;
        table.bind_wal_stream(identity)?;
        let mut wal_accepted = false;
        let accepted = &mut wal_accepted;
        let committer = &mut *self;
        let result = table.commit_with_record_prepared(tx, move |record| {
            // Codec and outer frame serialization operate on an immutable copy
            // before acquiring row/index guards. Validation below must still
            // reject a stale record before accepting any of these bytes.
            let mut prepared_epoch = None;
            let wal_commit = match &committer.sink {
                CommitSink::Synchronous(_) => wal_commit_from_occ_record(record)?,
                CommitSink::Asynchronous(ring) => {
                    let Some(state) = committer.async_delta_state.as_mut() else {
                        return Err(WalWriterError::InvalidMode(
                            "missing async delta state for asynchronous mode",
                        ));
                    };
                    let writer_epoch = ring.writer_epoch()?;
                    prepared_epoch = Some(writer_epoch);
                    if writer_epoch != state.observed_writer_epoch {
                        state.observed_writer_epoch = writer_epoch;
                        state.rows_with_full_baseline_in_epoch.clear();
                    }
                    state.pending_full_baselines.clear();
                    // A failed codec or enqueue must not claim that a full
                    // baseline reached the ring. Record it only after success.
                    wal_commit_from_occ_record_with_policy(record, |row_id| {
                        if state
                            .rows_with_full_baseline_in_epoch
                            .contains(&(row_id as u64))
                        {
                            WalEncodingPolicy::DeltaAllowed
                        } else {
                            state.pending_full_baselines.push(row_id as u64);
                            WalEncodingPolicy::ForceFull
                        }
                    })?
                }
            };

            let payload = serialize_commit_record(&wal_commit)?;
            Ok::<_, WalWriterError>(move |_record: &OccCommitRecord<T>| {
                match &committer.sink {
                    CommitSink::Synchronous(_) => {
                        let Some(sync_writer) = committer.sync_writer.as_mut() else {
                            return Err(WalWriterError::InvalidMode(
                                "missing sync writer for synchronous mode",
                            ));
                        };
                        sync_writer.append_payload_sync_with_health(
                            payload.as_slice(),
                            || table.check_not_poisoned().map_err(WalWriterError::from),
                            || table.poison_after_wal_failure(),
                        )?;
                    }
                    CommitSink::Asynchronous(ring) => {
                        let writer_epoch = ring.writer_epoch()?;
                        if prepared_epoch != Some(writer_epoch) {
                            // Optimistic payload preparation became stale: no
                            // bytes were accepted, so normal serialization retry
                            // is safe after the driver aborts this transaction.
                            // This check does not synchronize destructive ring
                            // reset; reset still requires exclusive shutdown.
                            #[cfg(feature = "retry-diagnostics")]
                            crate::retry_diagnostics::record(crate::retry_diagnostics::Cause::WalWriterEpochChanged, None, None);
                            return Err(WalWriterError::Occ(OccError::SerializationFailure));
                        }
                        ring.push_bytes_blocking(payload.as_slice())?;
                    }
                }
                *accepted = true;
                Ok(())
            })
        });
        let record = match result {
            Err(WalWriterError::Occ(err)) if wal_accepted => {
                table.poison_after_wal_failure();
                return Err(WalWriterError::PublicationAfterWal(err));
            }
            result => result?,
        };

        // Cache membership can lag accepted full rows safely. Update it only
        // after successful publication, outside all row/index guards. The
        // exclusive &mut committer borrow prevents local reuse in between.
        if let Some(state) = self.async_delta_state.as_mut() {
            state
                .rows_with_full_baseline_in_epoch
                .extend(state.pending_full_baselines.drain(..));
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

#[derive(Debug, Serialize, Deserialize)]
struct OccCheckpointRow<T> {
    row_id: u64,
    value: T,
}

#[derive(Debug, Serialize, Deserialize)]
struct OccCheckpointFrame<T> {
    version: u32,
    max_txid: u64,
    // IDs allocated before the cut but not yet committed into its row image.
    // They must still replay if they commit after checkpoint completion.
    in_flight_txids: Vec<u64>,
    rows: Vec<OccCheckpointRow<T>>,
}

#[derive(Debug, Deserialize)]
struct OccCheckpointFrameV1<T> {
    version: u32,
    max_txid: u64,
    rows: Vec<OccCheckpointRow<T>>,
}

const OCC_CHECKPOINT_VERSION: u32 = 2;

pub fn write_occ_checkpoint_and_truncate_wal<T: Copy + Send + Sync + 'static>(
    table: &OccTable<T>,
    checkpoint_path: impl AsRef<Path>,
    wal_path: impl AsRef<Path>,
) -> Result<usize, WalWriterError>
where
    T: Serialize,
{
    let checkpoint_path = checkpoint_path.as_ref();
    let wal_path = wal_path.as_ref();
    table.with_checkpoint_snapshot(|snapshot_rows, snapshot| {
        let wal_file = OpenOptions::new().create(true).write(true).open(wal_path)?;
        // Async rings cannot be drained or associated with their daemon's file
        // through this API. Reject them rather than silently making a bad cut.
        table.check_wal_stream(wal_file_identity(&wal_file)?)?;
        let frame = OccCheckpointFrame {
            version: OCC_CHECKPOINT_VERSION,
            max_txid: snapshot.xmax.saturating_sub(1),
            in_flight_txids: snapshot.in_flight_txids().to_vec(),
            rows: snapshot_rows
                .into_iter()
                .map(|(row_id, value)| OccCheckpointRow {
                    row_id: row_id as u64,
                    value,
                })
                .collect(),
        };
        let bytes =
            bincode::serialize(&frame).map_err(|err| WalWriterError::Codec(err.to_string()))?;
        let tmp_path = checkpoint_path.with_extension("tmp");
        let mut checkpoint_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        checkpoint_file.write_all(&bytes)?;
        checkpoint_file.sync_all()?;
        fs::rename(&tmp_path, checkpoint_path)?;
        // The replacement name must survive a crash before its prerequisite WAL
        // is discarded. Syncing only the file does not persist the rename.
        sync_parent_directory(checkpoint_path)?;
        truncate_wal_file(&wal_file, wal_path)?;
        Ok(frame.rows.len())
    })
}

pub fn recover_occ_table_from_checkpoint_and_wal<T: Copy + Send + Sync + 'static>(
    table: &OccTable<T>,
    checkpoint_path: impl AsRef<Path>,
    wal_path: impl AsRef<Path>,
) -> Result<OccRecoveryState, WalWriterError>
where
    T: WalDeltaCodec,
{
    recover_occ_table_from_checkpoint_and_wal_with_pk_map(table, None, checkpoint_path, wal_path)
}

pub fn recover_occ_table_from_checkpoint_and_wal_with_pk_map<T: Copy + Send + Sync + 'static>(
    table: &OccTable<T>,
    pk_map: Option<&ShmPrimaryKeyMap>,
    checkpoint_path: impl AsRef<Path>,
    wal_path: impl AsRef<Path>,
) -> Result<OccRecoveryState, WalWriterError>
where
    T: WalDeltaCodec,
{
    let checkpoint_path = checkpoint_path.as_ref();
    let mut checkpoint_applied = 0_usize;
    let mut checkpoint_max_txid = 0_u64;
    let mut checkpoint_in_flight = HashSet::new();

    if checkpoint_path.exists() {
        let bytes = fs::read(checkpoint_path)?;
        let version = bytes
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| WalWriterError::Codec("truncated OCC checkpoint version".into()))?;
        let frame: OccCheckpointFrame<T> = match version {
            1 => {
                let legacy: OccCheckpointFrameV1<T> = bincode::deserialize(&bytes)
                    .map_err(|err| WalWriterError::Codec(err.to_string()))?;
                debug_assert_eq!(legacy.version, 1);
                OccCheckpointFrame { version: OCC_CHECKPOINT_VERSION, max_txid: legacy.max_txid,
                    in_flight_txids: Vec::new(), rows: legacy.rows }
            }
            OCC_CHECKPOINT_VERSION => bincode::deserialize(&bytes)
                .map_err(|err| WalWriterError::Codec(err.to_string()))?,
            version => return Err(WalWriterError::Codec(format!(
                "unsupported OCC checkpoint version {version} (expected 1 or {OCC_CHECKPOINT_VERSION})"
            ))),
        };
        checkpoint_max_txid = frame.max_txid;
        checkpoint_in_flight.extend(frame.in_flight_txids);
        for row in frame.rows {
            let row_id = row.row_id as usize;
            if let Some(pk_map) = pk_map {
                let pk = T::wal_primary_key(row_id, &row.value);
                if !pk.is_empty() {
                    let mapped = pk_map
                        .insert_existing(pk.as_str(), row_id)
                        .map_err(|err| WalWriterError::Codec(err.to_string()))?;
                    if mapped != row_id {
                        return Err(WalWriterError::Codec(format!(
                            "checkpoint pk '{}' row mismatch: expected {}, mapped {}",
                            pk, row_id, mapped
                        )));
                    }
                }
            }
            table.apply_recovered_write(row_id, checkpoint_max_txid.max(1), row.value)?;
            checkpoint_applied += 1;
        }
        table.advance_global_txid_floor(checkpoint_max_txid.saturating_add(1));
    }

    let commits = read_wal_file(wal_path)?;
    let mut wal_records = 0_usize;
    let mut wal_applied = 0_usize;
    let mut max_txid = checkpoint_max_txid;

    for commit in &commits {
        if commit.txid <= checkpoint_max_txid && !checkpoint_in_flight.contains(&commit.txid) {
            continue;
        }
        wal_records += 1;
        if commit.txid > max_txid {
            max_txid = commit.txid;
        }
        for write in &commit.writes {
            apply_recovered_wal_write(table, pk_map, commit.txid, write)?;
            wal_applied += 1;
        }
    }

    if max_txid > 0 {
        table.advance_global_txid_floor(max_txid.saturating_add(1));
    }

    Ok(OccRecoveryState {
        wal_records,
        applied_writes: checkpoint_applied + wal_applied,
        max_txid,
    })
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
    T: WalDeltaCodec,
{
    recover_occ_table_from_wal_with_pk_map(table, None, wal_path)
}

pub fn recover_occ_table_from_wal_with_pk_map<T: Copy + Send + Sync + 'static>(
    table: &OccTable<T>,
    pk_map: Option<&ShmPrimaryKeyMap>,
    wal_path: impl AsRef<Path>,
) -> Result<OccRecoveryState, WalWriterError>
where
    T: WalDeltaCodec,
{
    let commits = read_wal_file(wal_path)?;
    let mut applied_writes = 0_usize;
    let mut max_txid = 0_u64;

    for commit in &commits {
        if commit.txid > max_txid {
            max_txid = commit.txid;
        }
        for write in &commit.writes {
            apply_recovered_wal_write(table, pk_map, commit.txid, write)?;
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

fn apply_recovered_wal_write<T>(
    table: &OccTable<T>,
    pk_map: Option<&ShmPrimaryKeyMap>,
    txid: u64,
    write: &WalRingWrite,
) -> Result<(), WalWriterError>
where
    T: WalDeltaCodec + Copy + Send + Sync + 'static,
{
    if !write.wal_record_payload.is_empty() {
        let record = deserialize_delta_wal_record(write.wal_record_payload.as_slice())
            .map_err(|err| WalWriterError::Codec(err.to_string()))?;
        if let Some(pk_map) = pk_map {
            replay_update_record_with_pk_map(table, pk_map, txid, &record)
                .map_err(|err| WalWriterError::Codec(err.to_string()))?;
        } else {
            replay_update_record(table, txid, write.row_id as usize, &record)
                .map_err(|err| WalWriterError::Codec(err.to_string()))?;
        }
        return Ok(());
    }

    let value: T = bincode::deserialize(write.value_payload.as_slice())
        .map_err(|err| WalWriterError::Codec(err.to_string()))?;
    table.apply_recovered_write(write.row_id as usize, txid, value)?;
    Ok(())
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

fn truncate_wal_file(file: &File, path: &Path) -> io::Result<()> {
    // Acquire before truncation: OpenOptions::truncate would already destroy
    // bytes before it could exclude a daemon flushing a complete-frame batch.
    let _lock = WalFileLock::exclusive(file.as_raw_fd())?;
    file.set_len(0)?;
    file.sync_all()?;
    sync_parent_directory(path)?;
    Ok(())
}

fn wal_file_identity(file: &File) -> io::Result<[u64; 4]> {
    let metadata = file.metadata()?;
    Ok([1, metadata.dev(), metadata.ino(), 0])
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

/// Coordinates independent process/file handles. It protects WAL bytes only;
/// transactions still use their existing partition and predicate lock sets.
struct WalFileLock {
    fd: libc::c_int,
}

impl WalFileLock {
    fn exclusive(fd: libc::c_int) -> io::Result<Self> {
        loop {
            // SAFETY: callers keep the owning File open until this guard drops.
            if unsafe { libc::flock(fd, libc::LOCK_EX) } == 0 {
                return Ok(Self { fd });
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

impl Drop for WalFileLock {
    fn drop(&mut self) {
        #[cfg(test)]
        poison_regressions::before_file_unlock();
        // SAFETY: this guard never outlives the File passed by its local caller.
        unsafe { libc::flock(self.fd, libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod poison_regressions {
    use super::*;
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};

    thread_local! {
        static CODEC_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
        static BEFORE_FILE_LOCK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
        static BEFORE_FILE_UNLOCK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    pub(super) fn before_file_lock() {
        let hook = BEFORE_FILE_LOCK.with(|hook| hook.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(super) fn before_file_unlock() {
        let hook = BEFORE_FILE_UNLOCK.with(|hook| hook.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
    struct CodecRow(u64);

    impl Serialize for CodecRow {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let hook = CODEC_HOOK.with(|hook| hook.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
            serializer.serialize_u64(self.0)
        }
    }

    impl WalDeltaCodec for CodecRow {}

    #[test]
    fn native_commit_rejects_poison_observed_during_payload_preparation() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal");
        let arena = Arc::new(crate::ShmArena::new(8 << 20).unwrap());
        let table = Arc::new(OccTable::<CodecRow>::new(Arc::clone(&arena), 1).unwrap());
        table.seed_row(0, CodecRow(0)).unwrap();
        let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, CodecRow(1)).unwrap();
        CODEC_HOOK.with(|hook| {
            let table = Arc::clone(&table);
            *hook.borrow_mut() = Some(Box::new(move || table.poison_after_wal_failure()));
        });
        let result = committer.commit(&table, &mut tx);
        assert!(matches!(result, Err(WalWriterError::Occ(OccError::Index(_)))),
            "poison during preparation must reject before WAL/publication: {result:?}");
        assert_eq!(table.latest_value(0).unwrap(), Some(CodecRow(0)));
        assert!(read_wal_file(&wal).unwrap().is_empty());
        assert!(arena.proc_array().create_snapshot(arena.global_txid()).in_flight_txids().is_empty());
        assert!(matches!(table.read(&mut tx, 0), Err(OccError::Index(_)) | Err(OccError::TransactionClosed)));
    }

    #[test]
    fn managed_sync_append_rechecks_table_poison_after_waiting_for_file_lock() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal");
        let arena = Arc::new(crate::ShmArena::new(8 << 20).unwrap());
        let table = Arc::new(OccTable::<u64>::new(Arc::clone(&arena), 1).unwrap());
        table.seed_row(0, 0).unwrap();
        let mut committer = OccCommitter::<8, 1024>::new_synchronous(&wal).unwrap();
        let held_file = OpenOptions::new().append(true).read(true).open(&wal).unwrap();
        let held = WalFileLock::exclusive(held_file.as_raw_fd()).unwrap();
        let (admitted, admitted_rx) = mpsc::channel();
        let worker = {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                let mut tx = table.begin_transaction().unwrap();
                table.write(&mut tx, 0, 1).unwrap();
                BEFORE_FILE_LOCK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || admitted.send(()).unwrap()));
                });
                committer.commit(&table, &mut tx)
            })
        };
        // The writer has passed table validation and entered its acceptance
        // callback; our independent file description still owns the flock.
        admitted_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        table.poison_after_wal_failure();
        drop(held);
        let result = worker.join().unwrap();
        assert!(matches!(result, Err(WalWriterError::Occ(OccError::Index(_)))),
            "managed append must recheck health inside flock: {result:?}");
        assert_eq!(table.latest_value(0).unwrap(), Some(0));
        assert!(read_wal_file(&wal).unwrap().is_empty());
        assert!(arena.proc_array().create_snapshot(arena.global_txid()).in_flight_txids().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detected_indeterminate_managed_append_poisons_before_file_unlock() {
        // /dev/full supplies real ENOSPC on write and EINVAL on truncation,
        // without mocking either failure or losing the open file's flock.
        let arena = Arc::new(crate::ShmArena::new(8 << 20).unwrap());
        let table = Arc::new(OccTable::<u64>::new(Arc::clone(&arena), 1).unwrap());
        table.seed_row(0, 0).unwrap();
        let mut committer = OccCommitter::<8, 1024>::new_synchronous("/dev/full").unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 1).unwrap();
        let poisoned_before_unlock = Arc::new(AtomicBool::new(false));
        BEFORE_FILE_UNLOCK.with(|hook| {
            let table = Arc::clone(&table);
            let observed = Arc::clone(&poisoned_before_unlock);
            *hook.borrow_mut() = Some(Box::new(move || {
                match table.begin_transaction() {
                    Err(OccError::Index(_)) => observed.store(true, Ordering::Release),
                    Ok(mut unexpected) => table.abort(&mut unexpected).unwrap(),
                    Err(error) => panic!("unexpected admission error: {error}"),
                }
            }));
        });
        let result = committer.commit(&table, &mut tx);
        assert!(matches!(result, Err(WalWriterError::Indeterminate { .. })), "{result:?}");
        assert!(poisoned_before_unlock.load(Ordering::Acquire),
            "detected uncertain tail must publish poison before releasing flock");
        assert_eq!(table.latest_value(0).unwrap(), Some(0));
        assert!(arena.proc_array().create_snapshot(arena.global_txid()).in_flight_txids().is_empty());
    }
}
