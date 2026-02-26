use std::sync::Arc;

use aerostore_core::{MvccTable, TransactionManager};
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
struct MarketRow {
    bid: u64,
    ask: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fifty_readers_never_observe_partial_updates_from_fifty_writers() {
    const WORKERS: usize = 50;
    const WRITE_ROUNDS: u64 = 20;
    const READ_TXNS_PER_WORKER: usize = 100;

    let txn_manager = Arc::new(TransactionManager::new());
    let table = Arc::new(MvccTable::<u64, MarketRow>::new(2048));

    let bootstrap_tx = txn_manager.begin();
    for key in 0..WORKERS as u64 {
        table
            .insert(key, MarketRow { bid: 0, ask: 0 }, &bootstrap_tx)
            .expect("bootstrap insert must succeed");
    }
    table.commit(&txn_manager, &bootstrap_tx);

    let start_barrier = Arc::new(Barrier::new(WORKERS * 2));
    let mut handles = Vec::with_capacity(WORKERS * 2);

    for writer_idx in 0..WORKERS {
        let txn_manager = Arc::clone(&txn_manager);
        let table = Arc::clone(&table);
        let start_barrier = Arc::clone(&start_barrier);

        handles.push(tokio::spawn(async move {
            start_barrier.wait().await;
            let key = writer_idx as u64;

            for round in 1..=WRITE_ROUNDS {
                let tx = txn_manager.begin();
                let bid = round * 10_000 + key;
                let ask = bid * 2;

                table
                    .update(&key, MarketRow { bid, ask }, &tx)
                    .expect("writer update must succeed");

                if round % 4 == 0 {
                    sleep(Duration::from_millis(1)).await;
                }

                let _ = table.commit(&txn_manager, &tx);
            }
        }));
    }

    for _ in 0..WORKERS {
        let txn_manager = Arc::clone(&txn_manager);
        let table = Arc::clone(&table);
        let start_barrier = Arc::clone(&start_barrier);

        handles.push(tokio::spawn(async move {
            start_barrier.wait().await;

            for _ in 0..READ_TXNS_PER_WORKER {
                let tx = txn_manager.begin();

                for key in 0..WORKERS as u64 {
                    let row = table
                        .read_visible(&key, &tx)
                        .expect("row must always be visible to snapshot");

                    assert_eq!(
                        row.ask,
                        row.bid * 2,
                        "reader observed a partial row state for key {}",
                        key
                    );
                }

                txn_manager.commit(&tx);
                tokio::task::yield_now().await;
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("worker task panicked during MVCC concurrency test");
    }

    let final_tx = txn_manager.begin();
    for key in 0..WORKERS as u64 {
        let row = table
            .read_visible(&key, &final_tx)
            .expect("final row should be present");
        let expected_bid = WRITE_ROUNDS * 10_000 + key;
        assert_eq!(row.bid, expected_bid);
        assert_eq!(row.ask, expected_bid * 2);
    }
    txn_manager.commit(&final_tx);
}
