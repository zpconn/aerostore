use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

use crate::wal::DurableDatabase;
use crate::TxId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug)]
pub struct RowChange<K, V> {
    pub txid: TxId,
    pub key: K,
    pub kind: ChangeKind,
    pub value: Option<V>,
}

#[derive(Clone)]
pub struct TableWatch<K, V> {
    sender: broadcast::Sender<RowChange<K, V>>,
}

impl<K, V> TableWatch<K, V>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    pub fn publish(&self, change: RowChange<K, V>) {
        let _ = self.sender.send(change);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RowChange<K, V>> {
        self.sender.subscribe()
    }

    pub fn subscribe_filtered<F>(&self, filter: F) -> FilteredSubscription<K, V>
    where
        F: Fn(&RowChange<K, V>) -> bool + Send + Sync + 'static,
    {
        FilteredSubscription {
            receiver: self.sender.subscribe(),
            filter: Arc::new(filter),
        }
    }

    pub fn watch_key(&self, key: K) -> FilteredSubscription<K, V>
    where
        K: Eq,
    {
        self.subscribe_filtered(move |change| change.key == key)
    }
}

pub struct FilteredSubscription<K, V> {
    receiver: broadcast::Receiver<RowChange<K, V>>,
    filter: Arc<dyn Fn(&RowChange<K, V>) -> bool + Send + Sync>,
}

impl<K, V> FilteredSubscription<K, V>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub async fn recv(&mut self) -> Result<RowChange<K, V>, broadcast::error::RecvError> {
        loop {
            let change = self.receiver.recv().await?;
            if (self.filter)(&change) {
                return Ok(change);
            }
        }
    }
}

pub struct TtlSweeper {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for TtlSweeper {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

pub fn spawn_ttl_sweeper<K, V, F>(
    db: Arc<DurableDatabase<K, V>>,
    interval: Duration,
    ttl: Duration,
    updated_at_unix_secs: F,
) -> TtlSweeper
where
    K: Eq + Hash + Clone + Ord + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    F: Fn(&V) -> u64 + Send + Sync + 'static,
{
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let updated_at_unix_secs: Arc<dyn Fn(&V) -> u64 + Send + Sync> =
        Arc::new(updated_at_unix_secs);

    let task = tokio::spawn(async move {
        let mut ticker = time::interval(interval.max(Duration::from_millis(10)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now_unix = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let _ = db.prune_expired(ttl, now_unix, updated_at_unix_secs.as_ref()).await;
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

    TtlSweeper {
        shutdown: Some(shutdown_tx),
        task,
    }
}
