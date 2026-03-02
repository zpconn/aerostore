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
    let updated_at_unix_secs: Arc<dyn Fn(&V) -> u64 + Send + Sync> = Arc::new(updated_at_unix_secs);

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

#[cfg(test)]
mod tests {
    use super::{spawn_ttl_sweeper, ChangeKind, RowChange, TableWatch};
    use crate::wal::DurableDatabase;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tempfile::tempdir;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestValue {
        tag: String,
        payload: u8,
    }

    fn logical_row(tag: &str, payload: u8) -> TestValue {
        TestValue {
            tag: tag.to_string(),
            payload,
        }
    }

    #[tokio::test]
    async fn table_watch_watch_key_filters_only_target_key() {
        let watch = TableWatch::<String, TestValue>::with_capacity(4);
        let mut sub = watch.watch_key("K1".to_string());

        watch.publish(RowChange {
            txid: 1,
            key: "K2".to_string(),
            kind: ChangeKind::Insert,
            value: Some(logical_row("K2", 1)),
        });
        watch.publish(RowChange {
            txid: 2,
            key: "K1".to_string(),
            kind: ChangeKind::Update,
            value: Some(logical_row("K1", 2)),
        });

        let next = sub.recv().await.expect("recv");
        assert_eq!(next.txid, 2);
        assert_eq!(next.key, "K1");
        assert_eq!(next.kind, ChangeKind::Update);
    }

    #[tokio::test]
    async fn subscribe_filtered_skips_nonmatching_events_in_order() {
        let watch = TableWatch::<String, TestValue>::with_capacity(8);
        let mut sub = watch.subscribe_filtered(|change| change.txid % 2 == 0);

        for txid in 1..=4_u64 {
            watch.publish(RowChange {
                txid,
                key: format!("K{txid}"),
                kind: ChangeKind::Insert,
                value: Some(logical_row("K", txid as u8)),
            });
        }

        let first = sub.recv().await.expect("first");
        let second = sub.recv().await.expect("second");
        assert_eq!(first.txid, 2);
        assert_eq!(second.txid, 4);
    }

    #[test]
    fn with_capacity_zero_clamps_to_minimum_channel_capacity() {
        let watch = TableWatch::<u64, TestValue>::with_capacity(0);
        let mut sub = watch.subscribe();
        watch.publish(RowChange {
            txid: 1,
            key: 9,
            kind: ChangeKind::Insert,
            value: Some(logical_row("N", 9)),
        });
        let received = sub.try_recv().expect("should have at least one slot");
        assert_eq!(received.key, 9);
    }

    #[tokio::test]
    async fn ttl_sweeper_drop_stops_background_task() {
        let temp = tempdir().expect("tempdir");
        let (db, _) = DurableDatabase::<String, TestValue>::open_with_recovery(
            temp.path(),
            128,
            Duration::from_secs(30),
            Vec::new(),
        )
        .await
        .expect("open db");
        let db = std::sync::Arc::new(db);

        let sweeper = spawn_ttl_sweeper(
            db,
            Duration::from_millis(10),
            Duration::from_secs(60),
            |_| 0,
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(sweeper);
    }
}
