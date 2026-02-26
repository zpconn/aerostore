use std::sync::Arc;
use std::time::Instant;

use crossbeam::epoch;

use aerostore_core::{Field, MvccTable, QueryEngine, SortDirection, TransactionManager};

#[derive(Clone, Debug)]
struct FlightPosition {
    lat: f64,
    lon: f64,
    altitude: i32,
    groundspeed: u16,
}

fn altitude(row: &FlightPosition) -> i32 {
    row.altitude
}

fn altitude_field() -> Field<FlightPosition, i32> {
    Field::new("altitude", altitude)
}

#[test]
fn benchmark_skiplist_indexed_range_scan_with_sort_and_limit() {
    const ROWS: usize = 100_000;
    const READERS: usize = 8;
    const SCANS_PER_READER: usize = 64;
    const LIMIT: usize = 20;

    let tx_manager = Arc::new(TransactionManager::new());
    let table = Arc::new(MvccTable::<u64, FlightPosition>::new(1 << 14));

    let mut engine = QueryEngine::new(Arc::clone(&table));
    engine.create_index("altitude", altitude);
    let engine = Arc::new(engine);

    let ingest_start = Instant::now();
    let ingest_tx = tx_manager.begin();
    for i in 0..ROWS {
        let row = FlightPosition {
            lat: 37.0 + (i as f64 * 0.000_01),
            lon: -122.0 - (i as f64 * 0.000_01),
            altitude: ((i % 45_000) as i32) + 500,
            groundspeed: 250 + ((i % 250) as u16),
        };
        engine
            .insert(i as u64, row, &ingest_tx)
            .expect("ingest insert must succeed");
    }
    let _ = engine.commit(&tx_manager, &ingest_tx);
    let ingest_elapsed = ingest_start.elapsed();

    let query_start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..READERS {
            let tx_manager = Arc::clone(&tx_manager);
            let engine = Arc::clone(&engine);

            scope.spawn(move || {
                for _ in 0..SCANS_PER_READER {
                    let tx = tx_manager.begin();
                    let guard = epoch::pin();

                    let rows = engine
                        .query()
                        .gt(altitude_field(), 10_000_i32)
                        .sort_by(altitude_field(), SortDirection::Asc)
                        .limit(LIMIT)
                        .execute(&tx, &guard);

                    assert_eq!(rows.len(), LIMIT);
                    assert!(rows.iter().all(|row| row.altitude > 10_000));
                    assert!(rows.iter().all(|row| row.groundspeed >= 250));
                    assert!(rows
                        .iter()
                        .all(|row| row.lat.is_finite() && row.lon.is_finite()));
                    assert!(rows.windows(2).all(|w| w[0].altitude <= w[1].altitude));

                    tx_manager.commit(&tx);
                }
            });
        }
    });
    let query_elapsed = query_start.elapsed();

    eprintln!(
        "ingest={} rows in {:?}; concurrent indexed scans={} in {:?}",
        ROWS,
        ingest_elapsed,
        READERS * SCANS_PER_READER,
        query_elapsed
    );
}
