use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam::epoch;
use serde::{Deserialize, Serialize};

use aerostore_core::{
    DurableDatabase, Field, IndexDefinition, RecoveryStage, SortDirection,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FlightRow {
    altitude: i32,
    groundspeed: u16,
}

fn altitude(row: &FlightRow) -> i32 {
    row.altitude
}

fn altitude_field() -> Field<FlightRow, i32> {
    Field::new("altitude", altitude)
}

fn tmp_data_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("aerostore_wal_recovery_{nonce}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_restores_checkpoint_wal_and_indexes() {
    let data_dir = tmp_data_dir();
    std::fs::create_dir_all(&data_dir).expect("failed to create temp data dir");

    let index_defs = vec![IndexDefinition::new("altitude", altitude)];

    let (db, first_recovery) = DurableDatabase::<u64, FlightRow>::open_with_recovery(
        &data_dir,
        2048,
        Duration::from_secs(300),
        index_defs.clone(),
    )
    .await
    .expect("failed to open durable database");

    assert_eq!(first_recovery.stage, RecoveryStage::Complete);
    assert_eq!(first_recovery.applied_writes, 0);

    let mut tx1 = db.begin();
    db.insert(
        &mut tx1,
        1001,
        FlightRow {
            altitude: 12_000,
            groundspeed: 440,
        },
    )
    .expect("insert should succeed");
    db.insert(
        &mut tx1,
        1002,
        FlightRow {
            altitude: 18_500,
            groundspeed: 452,
        },
    )
    .expect("insert should succeed");
    db.commit(tx1).await.expect("commit 1 must succeed");

    let mut tx2 = db.begin();
    db.update(
        &mut tx2,
        &1001,
        FlightRow {
            altitude: 13_250,
            groundspeed: 448,
        },
    )
    .expect("update should succeed");
    db.delete(&mut tx2, &1002).expect("delete should succeed");
    db.insert(
        &mut tx2,
        1003,
        FlightRow {
            altitude: 25_100,
            groundspeed: 470,
        },
    )
    .expect("insert should succeed");
    db.commit(tx2).await.expect("commit 2 must succeed");

    let checkpoint_rows = db
        .checkpoint_now()
        .await
        .expect("checkpoint should complete");
    assert!(checkpoint_rows >= 2);

    let mut tx3 = db.begin();
    db.update(
        &mut tx3,
        &1003,
        FlightRow {
            altitude: 26_000,
            groundspeed: 475,
        },
    )
    .expect("update should succeed");
    db.insert(
        &mut tx3,
        1004,
        FlightRow {
            altitude: 10_400,
            groundspeed: 399,
        },
    )
    .expect("insert should succeed");
    db.commit(tx3).await.expect("commit 3 must succeed");

    drop(db);

    let (recovered, recovery) = DurableDatabase::<u64, FlightRow>::open_with_recovery(
        &data_dir,
        2048,
        Duration::from_secs(300),
        index_defs,
    )
    .await
    .expect("failed to reopen durable database");

    assert_eq!(recovery.stage, RecoveryStage::Complete);
    assert!(recovery.checkpoint_rows >= 2);
    assert!(recovery.wal_records >= 1);
    assert!(recovery.applied_writes >= 4);

    let read_tx = recovered.begin();
    let row_1001 = recovered
        .read_visible(&1001, &read_tx)
        .expect("row 1001 missing after recovery");
    let row_1002 = recovered.read_visible(&1002, &read_tx);
    let row_1003 = recovered
        .read_visible(&1003, &read_tx)
        .expect("row 1003 missing after recovery");
    let row_1004 = recovered
        .read_visible(&1004, &read_tx)
        .expect("row 1004 missing after recovery");

    assert_eq!(row_1001.altitude, 13_250);
    assert!(row_1002.is_none(), "deleted row 1002 should stay deleted");
    assert_eq!(row_1003.altitude, 26_000);
    assert_eq!(row_1004.altitude, 10_400);

    let guard = epoch::pin();
    let indexed_rows = recovered
        .engine()
        .query()
        .gt(altitude_field(), 12_000_i32)
        .sort_by(altitude_field(), SortDirection::Desc)
        .limit(2)
        .execute(read_tx.tx(), &guard);

    assert_eq!(indexed_rows.len(), 2);
    assert!(indexed_rows[0].altitude >= indexed_rows[1].altitude);
    assert!(indexed_rows.iter().all(|row| row.altitude > 12_000));

    recovered.abort(read_tx);

    drop(recovered);
    let _ = std::fs::remove_dir_all(data_dir);
}
