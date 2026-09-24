use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use aerostore_core::{
    bulk_upsert_tsv, ChangeKind, DurableDatabase, IndexDefinition, TsvColumns, TsvDecodeError,
    TsvDecoder,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct FlightRow {
    lat: f64,
    lon: f64,
    altitude: i32,
    groundspeed: u16,
    updated_at: u64,
}

struct FlightTsvDecoder;

impl TsvDecoder<String, FlightRow> for FlightTsvDecoder {
    fn decode(&self, cols: &mut TsvColumns<'_>) -> Result<(String, FlightRow), TsvDecodeError> {
        let key = cols.expect_str()?.to_string();
        let row = FlightRow {
            lat: cols.expect_f64()?,
            lon: cols.expect_f64()?,
            altitude: cols.expect_i32()?,
            groundspeed: cols.expect_u16()?,
            updated_at: cols.expect_u64()?,
        };
        Ok((key, row))
    }
}

fn temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("aerostore_ingest_watch_ttl_{nonce}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_tsv_upsert_watch_stream_and_ttl_pruning() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time moved backwards")
        .as_secs();
    let old = now.saturating_sub(10_000);

    let dir = temp_dir();
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");

    let (db, _) = DurableDatabase::<String, FlightRow>::open_with_recovery(
        &dir,
        2048,
        Duration::from_secs(300),
        vec![IndexDefinition::new("altitude", |row: &FlightRow| {
            row.altitude
        })],
    )
    .await
    .expect("failed to open durable database");
    let db = Arc::new(db);

    let mut ual_watch = db.watch_key("UAL123".to_string());

    let first_batch = format!(
        "UAL123\t37.61\t-122.38\t12000\t450\t{}\nDAL456\t33.94\t-118.40\t9000\t420\t{}\n",
        now, old
    );
    let stats = bulk_upsert_tsv(&db, first_batch.as_bytes(), 2, &FlightTsvDecoder)
        .await
        .expect("first batch upsert should succeed");
    assert_eq!(stats.rows_inserted, 2);
    assert_eq!(stats.rows_updated, 0);

    let inserted = timeout(Duration::from_secs(1), ual_watch.recv())
        .await
        .expect("watch timeout for insert")
        .expect("broadcast closed");
    assert_eq!(inserted.kind, ChangeKind::Insert);
    assert_eq!(inserted.key, "UAL123");
    assert_eq!(
        inserted.value.expect("insert value missing").altitude,
        12_000
    );

    let second_batch = format!("UAL123\t37.62\t-122.39\t13250\t455\t{}\n", now);
    let stats = bulk_upsert_tsv(&db, second_batch.as_bytes(), 1, &FlightTsvDecoder)
        .await
        .expect("second batch upsert should succeed");
    assert_eq!(stats.rows_inserted, 0);
    assert_eq!(stats.rows_updated, 1);

    let updated = timeout(Duration::from_secs(1), ual_watch.recv())
        .await
        .expect("watch timeout for update")
        .expect("broadcast closed");
    assert_eq!(updated.kind, ChangeKind::Update);
    assert_eq!(updated.key, "UAL123");
    assert_eq!(
        updated.value.expect("update value missing").altitude,
        13_250
    );

    let _ttl = db.start_ttl_sweeper(
        Duration::from_millis(25),
        Duration::from_secs(600),
        |row: &FlightRow| row.updated_at,
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    let read_tx = db.begin();
    let ual = db
        .read_visible(&"UAL123".to_string(), &read_tx)
        .expect("UAL123 should remain after TTL sweep");
    let dal = db.read_visible(&"DAL456".to_string(), &read_tx);
    assert_eq!(ual.altitude, 13_250);
    assert!(dal.is_none(), "DAL456 should be pruned by TTL sweeper");
    db.abort(read_tx);

    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}
