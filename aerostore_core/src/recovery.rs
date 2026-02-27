use std::collections::HashMap;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};

use crate::occ::{Error as OccError, OccTable};
use crate::wal_logical::{
    read_logical_wal_records, spawn_logical_wal_writer_daemon, truncate_logical_wal,
    LogicalWalError, LogicalWalRing, LogicalWalWriterDaemon, WalRecord,
};
use crate::ShmArena;

pub const SNAPSHOT_FILE_NAME: &str = "snapshot.dat";
pub const LOGICAL_WAL_FILE_NAME: &str = "aerostore_logical.wal";
pub const SNAPSHOT_VERSION: u32 = 1;
pub const LOGICAL_MAX_PK_BYTES: usize = 64;
pub const LOGICAL_MAX_PAYLOAD_BYTES: usize = 512;

#[derive(Clone, Debug)]
pub struct LogicalDatabaseConfig {
    pub data_dir: PathBuf,
    pub table_name: String,
    pub row_capacity: usize,
    pub shm_bytes: usize,
    pub synchronous_commit: bool,
}

impl LogicalDatabaseConfig {
    #[inline]
    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join(LOGICAL_WAL_FILE_NAME)
    }

    #[inline]
    pub fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join(SNAPSHOT_FILE_NAME)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalRow {
    pub exists: u8,
    pk_len: u16,
    pk: [u8; LOGICAL_MAX_PK_BYTES],
    payload_len: u16,
    payload: [u8; LOGICAL_MAX_PAYLOAD_BYTES],
}

impl LogicalRow {
    #[inline]
    pub fn empty() -> Self {
        Self {
            exists: 0,
            pk_len: 0,
            pk: [0_u8; LOGICAL_MAX_PK_BYTES],
            payload_len: 0,
            payload: [0_u8; LOGICAL_MAX_PAYLOAD_BYTES],
        }
    }

    pub fn from_parts(pk: &str, payload: &[u8]) -> Result<Self, RecoveryError> {
        if pk.is_empty() {
            return Err(RecoveryError::InvalidRow(
                "primary key cannot be empty".to_string(),
            ));
        }
        if pk.len() > LOGICAL_MAX_PK_BYTES {
            return Err(RecoveryError::InvalidRow(format!(
                "primary key exceeds max bytes (len={}, max={})",
                pk.len(),
                LOGICAL_MAX_PK_BYTES
            )));
        }
        if payload.len() > LOGICAL_MAX_PAYLOAD_BYTES {
            return Err(RecoveryError::InvalidRow(format!(
                "payload exceeds max bytes (len={}, max={})",
                payload.len(),
                LOGICAL_MAX_PAYLOAD_BYTES
            )));
        }

        let mut out = Self::empty();
        out.exists = 1;
        out.pk_len = pk.len() as u16;
        out.pk[..pk.len()].copy_from_slice(pk.as_bytes());
        out.payload_len = payload.len() as u16;
        out.payload[..payload.len()].copy_from_slice(payload);
        Ok(out)
    }

    #[inline]
    pub fn pk_string(&self) -> String {
        let len = self.pk_len as usize;
        String::from_utf8_lossy(&self.pk[..len]).to_string()
    }

    #[inline]
    pub fn payload_bytes(&self) -> Vec<u8> {
        let len = self.payload_len as usize;
        self.payload[..len].to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub checkpoint_txid: u64,
    pub row_count: usize,
}

#[derive(Debug)]
pub enum RecoveryError {
    Io(io::Error),
    Occ(OccError),
    Wal(LogicalWalError),
    Codec(String),
    InvalidRow(String),
    RowCapacityExceeded { capacity: usize, key: String },
    SnapshotMismatch(String),
    RuntimeUnavailable,
    LockPoisoned(&'static str),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryError::Io(err) => write!(f, "io error: {}", err),
            RecoveryError::Occ(err) => write!(f, "occ error: {}", err),
            RecoveryError::Wal(err) => write!(f, "logical wal error: {}", err),
            RecoveryError::Codec(msg) => write!(f, "codec error: {}", msg),
            RecoveryError::InvalidRow(msg) => write!(f, "invalid row: {}", msg),
            RecoveryError::RowCapacityExceeded { capacity, key } => write!(
                f,
                "row capacity exhausted (capacity={}, key='{}')",
                capacity, key
            ),
            RecoveryError::SnapshotMismatch(msg) => write!(f, "snapshot mismatch: {}", msg),
            RecoveryError::RuntimeUnavailable => write!(f, "logical wal runtime unavailable"),
            RecoveryError::LockPoisoned(name) => write!(f, "poisoned lock: {}", name),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<io::Error> for RecoveryError {
    fn from(value: io::Error) -> Self {
        RecoveryError::Io(value)
    }
}

impl From<OccError> for RecoveryError {
    fn from(value: OccError) -> Self {
        RecoveryError::Occ(value)
    }
}

impl From<LogicalWalError> for RecoveryError {
    fn from(value: LogicalWalError) -> Self {
        RecoveryError::Wal(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotRow {
    pk: String,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFile {
    version: u32,
    table_name: String,
    checkpoint_txid: u64,
    rows: Vec<SnapshotRow>,
}

struct LogicalWalRuntime<const SLOTS: usize, const SLOT_BYTES: usize> {
    ring: LogicalWalRing<SLOTS, SLOT_BYTES>,
    daemon: Option<LogicalWalWriterDaemon>,
    wal_path: PathBuf,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> LogicalWalRuntime<SLOTS, SLOT_BYTES> {
    fn start(
        shm: Arc<ShmArena>,
        wal_path: PathBuf,
        synchronous_commit: bool,
    ) -> Result<Self, RecoveryError> {
        let ring = LogicalWalRing::<SLOTS, SLOT_BYTES>::create(shm)?;
        let daemon = spawn_logical_wal_writer_daemon::<SLOTS, SLOT_BYTES>(
            ring.clone(),
            &wal_path,
            synchronous_commit,
        )?;
        Ok(Self {
            ring,
            daemon: Some(daemon),
            wal_path,
        })
    }

    fn append_records(
        &mut self,
        records: &[WalRecord],
        synchronous_commit: bool,
    ) -> Result<(), RecoveryError> {
        let mut max_commit_txid = None::<u64>;
        for record in records {
            self.ring.push_record(record)?;
            if let WalRecord::Commit { txid } = record {
                max_commit_txid = Some(max_commit_txid.map_or(*txid, |current| current.max(*txid)));
            }
        }

        if synchronous_commit {
            let Some(target_txid) = max_commit_txid else {
                return Ok(());
            };
            let Some(daemon) = self.daemon.as_mut() else {
                return Err(RecoveryError::RuntimeUnavailable);
            };
            daemon.wait_for_txid_ack(target_txid)?;
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), RecoveryError> {
        self.ring.close()?;
        if let Some(daemon) = self.daemon.take() {
            daemon.join()?;
        }
        Ok(())
    }
}

pub struct LogicalDatabase<const SLOTS: usize, const SLOT_BYTES: usize> {
    config: LogicalDatabaseConfig,
    shm: Arc<ShmArena>,
    table: Arc<OccTable<LogicalRow>>,
    key_index: SkipMap<String, usize>,
    next_row_id: AtomicUsize,
    wal_runtime: Mutex<Option<LogicalWalRuntime<SLOTS, SLOT_BYTES>>>,
    op_lock: Mutex<()>,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> LogicalDatabase<SLOTS, SLOT_BYTES> {
    pub fn boot_from_disk(config: LogicalDatabaseConfig) -> Result<Self, RecoveryError> {
        fs::create_dir_all(&config.data_dir)?;

        let shm = Arc::new(ShmArena::new(config.shm_bytes).map_err(|err| {
            RecoveryError::InvalidRow(format!("failed to create shared memory arena: {}", err))
        })?);
        let table = Arc::new(OccTable::<LogicalRow>::new(
            Arc::clone(&shm),
            config.row_capacity,
        )?);
        for row_id in 0..config.row_capacity {
            table.seed_row(row_id, LogicalRow::empty())?;
        }

        let db = Self {
            config: config.clone(),
            shm,
            table,
            key_index: SkipMap::new(),
            next_row_id: AtomicUsize::new(0),
            wal_runtime: Mutex::new(None),
            op_lock: Mutex::new(()),
        };

        let checkpoint_txid = db.load_snapshot()?;
        db.replay_wal_after_txid(checkpoint_txid)?;
        db.start_runtime()?;
        Ok(db)
    }

    pub fn upsert(&self, pk: &str, payload: &[u8]) -> Result<u64, RecoveryError> {
        let _op_guard = self
            .op_lock
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("op_lock"))?;

        let row = LogicalRow::from_parts(pk, payload)?;
        let row_id = self.resolve_or_allocate_row_id(pk)?;

        loop {
            let mut tx = self.table.begin_transaction()?;
            let txid = tx.txid();
            self.table.write(&mut tx, row_id, row)?;
            match self.table.commit(&mut tx) {
                Ok(_) => {
                    self.append_wal_records(&[
                        WalRecord::Upsert {
                            txid,
                            table: self.config.table_name.clone(),
                            pk: pk.to_string(),
                            payload: payload.to_vec(),
                        },
                        WalRecord::Commit { txid },
                    ])?;
                    return Ok(txid);
                }
                Err(OccError::SerializationFailure) => continue,
                Err(err) => return Err(RecoveryError::Occ(err)),
            }
        }
    }

    pub fn delete(&self, pk: &str) -> Result<u64, RecoveryError> {
        let _op_guard = self
            .op_lock
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("op_lock"))?;

        let Some(entry) = self.key_index.get(pk) else {
            return Ok(0);
        };
        let row_id = *entry.value();

        loop {
            let mut tx = self.table.begin_transaction()?;
            let txid = tx.txid();
            self.table.write(&mut tx, row_id, LogicalRow::empty())?;
            match self.table.commit(&mut tx) {
                Ok(_) => {
                    self.append_wal_records(&[
                        WalRecord::Delete {
                            txid,
                            table: self.config.table_name.clone(),
                            pk: pk.to_string(),
                        },
                        WalRecord::Commit { txid },
                    ])?;
                    return Ok(txid);
                }
                Err(OccError::SerializationFailure) => continue,
                Err(err) => return Err(RecoveryError::Occ(err)),
            }
        }
    }

    pub fn checkpoint_now(&self) -> Result<SnapshotMeta, RecoveryError> {
        let _op_guard = self
            .op_lock
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("op_lock"))?;
        self.checkpoint_now_inner()
    }

    pub fn row_count(&self) -> Result<usize, RecoveryError> {
        let rows = self.table.snapshot_latest_rows()?;
        Ok(rows.into_iter().filter(|(_, row)| row.exists != 0).count())
    }

    #[inline]
    pub fn wal_path(&self) -> PathBuf {
        self.config.wal_path()
    }

    pub fn payload_for_key(&self, key: &str) -> Result<Option<Vec<u8>>, RecoveryError> {
        let Some(entry) = self.key_index.get(key) else {
            return Ok(None);
        };
        let row_id = *entry.value();
        let Some(row) = self.table.latest_value(row_id)? else {
            return Ok(None);
        };
        if row.exists == 0 {
            return Ok(None);
        }
        Ok(Some(row.payload_bytes()))
    }

    pub fn head_offset_for_key(&self, key: &str) -> Result<Option<u32>, RecoveryError> {
        let Some(entry) = self.key_index.get(key) else {
            return Ok(None);
        };
        let row_id = *entry.value();
        let offset = self.table.row_head_offset(row_id)?;
        Ok(Some(offset))
    }

    pub fn snapshot_offsets(&self) -> HashMap<String, u32> {
        let mut offsets = HashMap::new();
        for entry in self.key_index.iter() {
            let key = entry.key().clone();
            let row_id = *entry.value();
            if let Ok(offset) = self.table.row_head_offset(row_id) {
                offsets.insert(key, offset);
            }
        }
        offsets
    }

    pub fn shutdown(&self) -> Result<(), RecoveryError> {
        let mut guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        if let Some(runtime) = guard.as_mut() {
            runtime.shutdown()?;
        }
        *guard = None;
        Ok(())
    }

    pub fn terminate_wal_daemon(&self, signal: libc::c_int) -> Result<(), RecoveryError> {
        let guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        let Some(runtime) = guard.as_ref() else {
            return Err(RecoveryError::RuntimeUnavailable);
        };
        let Some(daemon) = runtime.daemon.as_ref() else {
            return Err(RecoveryError::RuntimeUnavailable);
        };
        daemon.terminate(signal)?;
        Ok(())
    }

    pub fn restart_wal_daemon(&self) -> Result<(), RecoveryError> {
        let mut guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        let Some(runtime) = guard.as_mut() else {
            return Err(RecoveryError::RuntimeUnavailable);
        };
        if let Some(daemon) = runtime.daemon.take() {
            let _ = daemon.join_any_status();
        }
        let daemon = spawn_logical_wal_writer_daemon::<SLOTS, SLOT_BYTES>(
            runtime.ring.clone(),
            &runtime.wal_path,
            self.config.synchronous_commit,
        )?;
        runtime.daemon = Some(daemon);
        Ok(())
    }

    fn start_runtime(&self) -> Result<(), RecoveryError> {
        let mut guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        let runtime = LogicalWalRuntime::<SLOTS, SLOT_BYTES>::start(
            Arc::clone(&self.shm),
            self.config.wal_path(),
            self.config.synchronous_commit,
        )?;
        *guard = Some(runtime);
        Ok(())
    }

    fn append_wal_records(&self, records: &[WalRecord]) -> Result<(), RecoveryError> {
        let mut guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        let Some(runtime) = guard.as_mut() else {
            return Err(RecoveryError::RuntimeUnavailable);
        };
        runtime.append_records(records, self.config.synchronous_commit)
    }

    fn checkpoint_now_inner(&self) -> Result<SnapshotMeta, RecoveryError> {
        let mut guard = self
            .wal_runtime
            .lock()
            .map_err(|_| RecoveryError::LockPoisoned("wal_runtime"))?;
        let Some(runtime) = guard.as_mut() else {
            return Err(RecoveryError::RuntimeUnavailable);
        };
        runtime.ring.close()?;
        if let Some(daemon) = runtime.daemon.take() {
            daemon.join()?;
        }

        let rows = self.table.snapshot_latest_rows()?;
        let checkpoint_txid = self.table.current_global_txid().saturating_sub(1);

        let snapshot_rows = rows
            .into_iter()
            .filter_map(|(_, row)| {
                if row.exists == 0 {
                    None
                } else {
                    Some(SnapshotRow {
                        pk: row.pk_string(),
                        payload: row.payload_bytes(),
                    })
                }
            })
            .collect::<Vec<_>>();

        let snapshot = SnapshotFile {
            version: SNAPSHOT_VERSION,
            table_name: self.config.table_name.clone(),
            checkpoint_txid,
            rows: snapshot_rows,
        };
        write_snapshot_file(self.config.snapshot_path(), &snapshot)?;
        truncate_logical_wal(self.config.wal_path())?;

        let ring = LogicalWalRing::<SLOTS, SLOT_BYTES>::create(Arc::clone(&self.shm))?;
        let daemon = spawn_logical_wal_writer_daemon::<SLOTS, SLOT_BYTES>(
            ring.clone(),
            &runtime.wal_path,
            self.config.synchronous_commit,
        )?;
        runtime.ring = ring;
        runtime.daemon = Some(daemon);
        Ok(SnapshotMeta {
            checkpoint_txid,
            row_count: snapshot.rows.len(),
        })
    }

    fn load_snapshot(&self) -> Result<u64, RecoveryError> {
        let snapshot_path = self.config.snapshot_path();
        if !snapshot_path.exists() {
            return Ok(0);
        }

        let snapshot = read_snapshot_file(snapshot_path)?;
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(RecoveryError::SnapshotMismatch(format!(
                "unsupported snapshot version {} (expected {})",
                snapshot.version, SNAPSHOT_VERSION
            )));
        }
        if snapshot.table_name != self.config.table_name {
            return Err(RecoveryError::SnapshotMismatch(format!(
                "snapshot table '{}' does not match runtime table '{}'",
                snapshot.table_name, self.config.table_name
            )));
        }

        for row in &snapshot.rows {
            self.apply_upsert_no_wal(row.pk.as_str(), row.payload.as_slice())?;
        }
        Ok(snapshot.checkpoint_txid)
    }

    fn replay_wal_after_txid(&self, checkpoint_txid: u64) -> Result<(), RecoveryError> {
        let wal_records = read_logical_wal_records(self.config.wal_path())?;
        let mut staged = HashMap::<u64, Vec<WalRecord>>::new();

        for record in wal_records {
            match record {
                WalRecord::Upsert { txid, .. } | WalRecord::Delete { txid, .. } => {
                    if txid > checkpoint_txid {
                        staged.entry(txid).or_default().push(record);
                    }
                }
                WalRecord::Commit { txid } => {
                    if txid <= checkpoint_txid {
                        staged.remove(&txid);
                        continue;
                    }
                    let Some(ops) = staged.remove(&txid) else {
                        continue;
                    };
                    for op in ops {
                        match op {
                            WalRecord::Upsert {
                                table, pk, payload, ..
                            } => {
                                if table == self.config.table_name {
                                    self.apply_upsert_no_wal(pk.as_str(), payload.as_slice())?;
                                }
                            }
                            WalRecord::Delete { table, pk, .. } => {
                                if table == self.config.table_name {
                                    self.apply_delete_no_wal(pk.as_str())?;
                                }
                            }
                            WalRecord::Commit { .. } => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_upsert_no_wal(&self, pk: &str, payload: &[u8]) -> Result<(), RecoveryError> {
        let row_id = self.resolve_or_allocate_row_id(pk)?;
        let row = LogicalRow::from_parts(pk, payload)?;

        loop {
            let mut tx = self.table.begin_transaction()?;
            self.table.write(&mut tx, row_id, row)?;
            match self.table.commit(&mut tx) {
                Ok(_) => return Ok(()),
                Err(OccError::SerializationFailure) => continue,
                Err(err) => return Err(RecoveryError::Occ(err)),
            }
        }
    }

    fn apply_delete_no_wal(&self, pk: &str) -> Result<(), RecoveryError> {
        let Some(entry) = self.key_index.get(pk) else {
            return Ok(());
        };
        let row_id = *entry.value();

        loop {
            let mut tx = self.table.begin_transaction()?;
            self.table.write(&mut tx, row_id, LogicalRow::empty())?;
            match self.table.commit(&mut tx) {
                Ok(_) => return Ok(()),
                Err(OccError::SerializationFailure) => continue,
                Err(err) => return Err(RecoveryError::Occ(err)),
            }
        }
    }

    fn resolve_or_allocate_row_id(&self, key: &str) -> Result<usize, RecoveryError> {
        if let Some(entry) = self.key_index.get(key) {
            return Ok(*entry.value());
        }

        let candidate = self.next_row_id.fetch_add(1, Ordering::AcqRel);
        if candidate >= self.config.row_capacity {
            return Err(RecoveryError::RowCapacityExceeded {
                capacity: self.config.row_capacity,
                key: key.to_string(),
            });
        }

        let entry = self.key_index.get_or_insert(key.to_string(), candidate);
        let row_id = *entry.value();
        if row_id >= self.config.row_capacity {
            return Err(RecoveryError::RowCapacityExceeded {
                capacity: self.config.row_capacity,
                key: key.to_string(),
            });
        }

        Ok(row_id)
    }
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> Drop for LogicalDatabase<SLOTS, SLOT_BYTES> {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.wal_runtime.lock() {
            if let Some(runtime) = guard.as_mut() {
                let _ = runtime.shutdown();
            }
            *guard = None;
        }
    }
}

pub struct LogicalCheckpointerHandle {
    shutdown_tx: mpsc::Sender<()>,
    join: Option<JoinHandle<()>>,
}

impl LogicalCheckpointerHandle {
    pub fn stop(mut self) {
        let _ = self.shutdown_tx.send(());
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_logical_checkpointer<const SLOTS: usize, const SLOT_BYTES: usize>(
    db: Arc<LogicalDatabase<SLOTS, SLOT_BYTES>>,
    interval: Duration,
) -> LogicalCheckpointerHandle {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let join = thread::spawn(move || loop {
        match shutdown_rx.recv_timeout(interval) {
            Ok(_) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = db.checkpoint_now();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    });

    LogicalCheckpointerHandle {
        shutdown_tx,
        join: Some(join),
    }
}

fn write_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &SnapshotFile,
) -> Result<(), RecoveryError> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("tmp");
    let bytes =
        bincode::serialize(snapshot).map_err(|err| RecoveryError::Codec(err.to_string()))?;
    fs::write(&tmp_path, bytes)?;
    fs::rename(&tmp_path, path)?;
    let file = OpenOptions::new().read(true).open(path)?;
    file.sync_all()?;
    Ok(())
}

fn read_snapshot_file(path: impl AsRef<Path>) -> Result<SnapshotFile, RecoveryError> {
    let bytes = fs::read(path)?;
    bincode::deserialize(bytes.as_slice()).map_err(|err| RecoveryError::Codec(err.to_string()))
}
