use std::fs;
use std::hash::Hash;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

use crate::index::{IndexValue, IntoIndexValue};
use crate::mvcc::MvccError;
use crate::query::QueryEngine;
use crate::txn::{Transaction, TransactionManager, TxId};

const WAL_FILE_NAME: &str = "wal.log";
const CHECKPOINT_FILE_NAME: &str = "checkpoint.dat";
const WAL_CHANNEL_CAPACITY: usize = 16_384;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalOperation<K, V> {
    Insert { key: K, value: V },
    Update { key: K, value: V },
    Delete { key: K },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord<K, V> {
    pub txid: TxId,
    pub writes: Vec<WalOperation<K, V>>,
}

#[derive(Clone)]
pub struct IndexDefinition<V> {
    field: &'static str,
    extractor: Arc<dyn Fn(&V) -> IndexValue + Send + Sync>,
}

impl<V: 'static> IndexDefinition<V> {
    pub fn new<F>(field: &'static str, extractor: fn(&V) -> F) -> Self
    where
        F: IntoIndexValue + Send + Sync + 'static,
    {
        Self {
            field,
            extractor: Arc::new(move |row| extractor(row).into_index_value()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryStage {
    Initialize,
    LoadingCheckpoint,
    ReplayingWal,
    Complete,
}

#[derive(Clone, Debug)]
pub struct RecoveryStateMachine {
    pub stage: RecoveryStage,
    pub checkpoint_rows: usize,
    pub wal_records: usize,
    pub applied_writes: usize,
}

impl RecoveryStateMachine {
    fn new() -> Self {
        Self {
            stage: RecoveryStage::Initialize,
            checkpoint_rows: 0,
            wal_records: 0,
            applied_writes: 0,
        }
    }
}

#[derive(Debug)]
pub struct DurableTransaction<K, V> {
    inner: Transaction,
    writes: Vec<WalOperation<K, V>>,
}

impl<K, V> DurableTransaction<K, V> {
    pub fn txid(&self) -> TxId {
        self.inner.txid
    }

    pub fn tx(&self) -> &Transaction {
        &self.inner
    }
}

enum WalCommand {
    Commit {
        frame: Vec<u8>,
        ack: oneshot::Sender<io::Result<()>>,
    },
    Truncate {
        ack: oneshot::Sender<io::Result<()>>,
    },
}

#[derive(Clone)]
struct WalWriter {
    sender: mpsc::Sender<WalCommand>,
}

impl WalWriter {
    async fn open(wal_path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(wal_path)
            .await?;

        let (sender, receiver) = mpsc::channel(WAL_CHANNEL_CAPACITY);
        tokio::spawn(run_wal_writer(file, receiver));

        Ok(Self { sender })
    }

    async fn append<K, V>(&self, record: &WalRecord<K, V>) -> io::Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        let payload = bincode::serialize(record)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))?;
        let frame = encode_frame(&payload);

        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(WalCommand::Commit { frame, ack: ack_tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "WAL worker has stopped"))?;

        ack_rx
            .await
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "WAL worker dropped ack"))?
    }

    async fn truncate(&self) -> io::Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(WalCommand::Truncate { ack: ack_tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "WAL worker has stopped"))?;

        ack_rx
            .await
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "WAL worker dropped ack"))?
    }
}

async fn run_wal_writer(
    mut file: tokio::fs::File,
    mut receiver: mpsc::Receiver<WalCommand>,
) {
    let mut pending: Option<WalCommand> = None;

    loop {
        let command = match pending.take() {
            Some(cmd) => cmd,
            None => {
                let Some(cmd) = receiver.recv().await else {
                    break;
                };
                cmd
            }
        };

        match command {
            WalCommand::Commit { frame, ack } => {
                let mut batch = vec![(frame, ack)];

                loop {
                    match receiver.try_recv() {
                        Ok(WalCommand::Commit { frame, ack }) => batch.push((frame, ack)),
                        Ok(other) => {
                            pending = Some(other);
                            break;
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                let mut result = Ok(());
                for (frame, _) in &batch {
                    if let Err(err) = file.write_all(frame).await {
                        result = Err(err);
                        break;
                    }
                }
                if result.is_ok() {
                    result = file.sync_data().await;
                }

                let failure = result.err().map(|err| (err.kind(), err.to_string()));
                for (_, ack) in batch {
                    let response = match &failure {
                        Some((kind, message)) => Err(io::Error::new(*kind, message.clone())),
                        None => Ok(()),
                    };
                    let _ = ack.send(response);
                }
            }
            WalCommand::Truncate { ack } => {
                let result = async {
                    file.sync_data().await?;
                    file.set_len(0).await?;
                    file.sync_data().await
                }
                .await;
                let _ = ack.send(result);
            }
        }
    }
}

fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn decode_frames(bytes: &[u8]) -> Vec<&[u8]> {
    let mut frames = Vec::new();
    let mut cursor = 0_usize;

    while cursor + 4 <= bytes.len() {
        let mut len_buf = [0_u8; 4];
        len_buf.copy_from_slice(&bytes[cursor..cursor + 4]);
        let payload_len = u32::from_le_bytes(len_buf) as usize;
        cursor += 4;

        if cursor + payload_len > bytes.len() {
            break;
        }

        frames.push(&bytes[cursor..cursor + payload_len]);
        cursor += payload_len;
    }

    frames
}

fn read_wal_records<K, V>(wal_path: &Path) -> io::Result<Vec<WalRecord<K, V>>>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    if !wal_path.exists() {
        return Ok(Vec::new());
    }

    let bytes = fs::read(wal_path)?;
    let mut out = Vec::new();

    for frame in decode_frames(&bytes) {
        let record = bincode::deserialize::<WalRecord<K, V>>(frame)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))?;
        out.push(record);
    }

    Ok(out)
}

fn write_checkpoint<K, V>(checkpoint_path: &Path, rows: &[(K, V)]) -> io::Result<()>
where
    K: Serialize,
    V: Serialize,
{
    let payload = bincode::serialize(rows)
        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))?;
    let tmp_path = checkpoint_path.with_extension("tmp");
    fs::write(&tmp_path, payload)?;
    fs::rename(tmp_path, checkpoint_path)?;
    Ok(())
}

fn read_checkpoint<K, V>(checkpoint_path: &Path) -> io::Result<Vec<(K, V)>>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    if !checkpoint_path.exists() {
        return Ok(Vec::new());
    }

    let bytes = fs::read(checkpoint_path)?;
    bincode::deserialize::<Vec<(K, V)>>(&bytes)
        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err.to_string()))
}

fn map_mvcc_error(err: MvccError) -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        format!("MVCC apply failure during recovery: {:?}", err),
    )
}

pub struct DurableDatabase<K, V>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    tx_manager: Arc<TransactionManager>,
    engine: Arc<QueryEngine<K, V>>,
    wal: WalWriter,
    wal_path: PathBuf,
    checkpoint_path: PathBuf,
    commit_barrier: Arc<RwLock<()>>,
    checkpointer_shutdown: Option<oneshot::Sender<()>>,
    checkpointer_task: Option<JoinHandle<()>>,
}

impl<K, V> DurableDatabase<K, V>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub async fn open_with_recovery<P: AsRef<Path>>(
        data_dir: P,
        bucket_count: usize,
        checkpoint_interval: Duration,
        indexes: Vec<IndexDefinition<V>>,
    ) -> io::Result<(Self, RecoveryStateMachine)> {
        fs::create_dir_all(data_dir.as_ref())?;

        let wal_path = data_dir.as_ref().join(WAL_FILE_NAME);
        let checkpoint_path = data_dir.as_ref().join(CHECKPOINT_FILE_NAME);

        let tx_manager = Arc::new(TransactionManager::new());
        let table = Arc::new(crate::mvcc::MvccTable::<K, V>::new(bucket_count));

        let mut engine = QueryEngine::new(table);
        for index in indexes {
            engine.create_index_with_extractor(index.field, Arc::clone(&index.extractor));
        }

        let wal = WalWriter::open(&wal_path).await?;
        let mut db = Self {
            tx_manager,
            engine: Arc::new(engine),
            wal,
            wal_path,
            checkpoint_path,
            commit_barrier: Arc::new(RwLock::new(())),
            checkpointer_shutdown: None,
            checkpointer_task: None,
        };

        let recovery = db.recover_startup_state().await?;
        db.spawn_checkpointer(checkpoint_interval);
        Ok((db, recovery))
    }

    pub fn begin(&self) -> DurableTransaction<K, V> {
        DurableTransaction {
            inner: self.tx_manager.begin(),
            writes: Vec::new(),
        }
    }

    pub fn abort(&self, tx: DurableTransaction<K, V>) {
        self.tx_manager.abort(&tx.inner);
    }

    pub fn insert(&self, tx: &mut DurableTransaction<K, V>, key: K, value: V) -> Result<(), MvccError> {
        self.engine.insert(key.clone(), value.clone(), &tx.inner)?;
        tx.writes.push(WalOperation::Insert { key, value });
        Ok(())
    }

    pub fn update(
        &self,
        tx: &mut DurableTransaction<K, V>,
        key: &K,
        value: V,
    ) -> Result<(), MvccError> {
        self.engine.update(key, value.clone(), &tx.inner)?;
        tx.writes.push(WalOperation::Update {
            key: key.clone(),
            value,
        });
        Ok(())
    }

    pub fn delete(&self, tx: &mut DurableTransaction<K, V>, key: &K) -> Result<(), MvccError> {
        self.engine.delete(key, &tx.inner)?;
        tx.writes.push(WalOperation::Delete { key: key.clone() });
        Ok(())
    }

    pub async fn commit(&self, tx: DurableTransaction<K, V>) -> io::Result<usize> {
        let _read_guard = self.commit_barrier.read().await;
        let record = WalRecord {
            txid: tx.inner.txid,
            writes: tx.writes,
        };
        self.wal.append(&record).await?;
        Ok(self.engine.commit(&self.tx_manager, &tx.inner))
    }

    pub fn read_visible(&self, key: &K, tx: &DurableTransaction<K, V>) -> Option<V> {
        self.engine.read_visible(key, &tx.inner)
    }

    pub fn engine(&self) -> &Arc<QueryEngine<K, V>> {
        &self.engine
    }

    pub async fn checkpoint_now(&self) -> io::Result<usize> {
        let _write_guard = self.commit_barrier.write().await;

        let snapshot_tx = self.tx_manager.begin();
        let rows = self.engine.snapshot_rows(&snapshot_tx);
        self.tx_manager.abort(&snapshot_tx);

        write_checkpoint(&self.checkpoint_path, &rows)?;
        self.wal.truncate().await?;

        Ok(rows.len())
    }

    async fn recover_startup_state(&self) -> io::Result<RecoveryStateMachine> {
        let mut state = RecoveryStateMachine::new();

        state.stage = RecoveryStage::LoadingCheckpoint;
        let checkpoint_rows = read_checkpoint::<K, V>(&self.checkpoint_path)?;
        state.checkpoint_rows = checkpoint_rows.len();

        if !checkpoint_rows.is_empty() {
            let tx = self.tx_manager.begin();
            for (key, value) in checkpoint_rows {
                self.engine
                    .insert(key, value, &tx)
                    .map_err(map_mvcc_error)?;
                state.applied_writes += 1;
            }
            let _ = self.engine.commit(&self.tx_manager, &tx);
        }

        state.stage = RecoveryStage::ReplayingWal;
        let wal_records = read_wal_records::<K, V>(&self.wal_path)?;
        state.wal_records = wal_records.len();

        for record in wal_records {
            let tx = self.tx_manager.begin_with_txid(record.txid);
            for op in record.writes {
                match op {
                    WalOperation::Insert { key, value } => {
                        self.engine
                            .insert(key, value, &tx)
                            .map_err(map_mvcc_error)?;
                    }
                    WalOperation::Update { key, value } => {
                        self.engine
                            .update(&key, value, &tx)
                            .map_err(map_mvcc_error)?;
                    }
                    WalOperation::Delete { key } => {
                        self.engine.delete(&key, &tx).map_err(map_mvcc_error)?;
                    }
                }
                state.applied_writes += 1;
            }
            let _ = self.engine.commit(&self.tx_manager, &tx);
        }

        state.stage = RecoveryStage::Complete;
        Ok(state)
    }

    fn spawn_checkpointer(&mut self, interval: Duration) {
        if interval.is_zero() {
            return;
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let tx_manager = Arc::clone(&self.tx_manager);
        let engine = Arc::clone(&self.engine);
        let wal = self.wal.clone();
        let checkpoint_path = self.checkpoint_path.clone();
        let commit_barrier = Arc::clone(&self.commit_barrier);

        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _write_guard = commit_barrier.write().await;
                        let snapshot_tx = tx_manager.begin();
                        let rows = engine.snapshot_rows(&snapshot_tx);
                        tx_manager.abort(&snapshot_tx);

                        if write_checkpoint(&checkpoint_path, &rows).is_ok() {
                            let _ = wal.truncate().await;
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        self.checkpointer_shutdown = Some(shutdown_tx);
        self.checkpointer_task = Some(handle);
    }
}

impl<K, V> Drop for DurableDatabase<K, V>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    fn drop(&mut self) {
        if let Some(shutdown) = self.checkpointer_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.checkpointer_task.take() {
            task.abort();
        }
    }
}
