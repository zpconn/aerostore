use std::sync::Arc;
use std::time::Instant;

use crossbeam::epoch;

use aerostore_core::{
    Field, IndexCatalog, IndexValue, MvccTable, OccTable, QueryEngine, QueryPlanner, ShmArena,
    SortDirection, StapiRow, StapiValue, TransactionManager, SecondaryIndex,
};

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StapiFlightRow {
    alt: i64,
    flight: [u8; 8],
    typ: [u8; 4],
}

impl StapiFlightRow {
    fn new(alt: i64, flight: &str, typ: &str) -> Self {
        Self {
            alt,
            flight: fixed_ascii::<8>(flight),
            typ: fixed_ascii::<4>(typ),
        }
    }
}

impl StapiRow for StapiFlightRow {
    fn has_field(field: &str) -> bool {
        matches!(field, "alt" | "altitude" | "flight" | "ident" | "typ" | "type")
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        match field {
            "alt" | "altitude" => Some(StapiValue::Int(self.alt)),
            "flight" | "ident" => Some(StapiValue::Text(decode_ascii(&self.flight))),
            "typ" | "type" => Some(StapiValue::Text(decode_ascii(&self.typ))),
            _ => None,
        }
    }
}

fn fixed_ascii<const N: usize>(value: &str) -> [u8; N] {
    let mut out = [0_u8; N];
    let bytes = value.as_bytes();
    let len = bytes.len().min(N);
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

fn decode_ascii(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|v| *v == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).to_string()
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

#[test]
fn benchmark_stapi_parse_compile_execute_vs_typed_query_path() {
    const ROWS: usize = 50_000;
    const PASSES: usize = 32;
    const LIMIT: usize = 20;

    // Baseline typed query path (existing MVCC query engine).
    let tx_manager = Arc::new(TransactionManager::new());
    let table = Arc::new(MvccTable::<u64, FlightPosition>::new(1 << 14));

    let mut engine = QueryEngine::new(Arc::clone(&table));
    engine.create_index("altitude", altitude);
    let engine = Arc::new(engine);

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
            .expect("typed path ingest insert must succeed");
    }
    let _ = engine.commit(&tx_manager, &ingest_tx);

    let typed_start = Instant::now();
    for _ in 0..PASSES {
        let tx = tx_manager.begin();
        let guard = epoch::pin();

        let rows = engine
            .query()
            .gt(altitude_field(), 10_000_i32)
            .sort_by(altitude_field(), SortDirection::Asc)
            .limit(LIMIT)
            .execute(&tx, &guard);

        assert_eq!(rows.len(), LIMIT);
        assert!(rows.windows(2).all(|w| w[0].altitude <= w[1].altitude));
        tx_manager.abort(&tx);
    }
    let typed_elapsed = typed_start.elapsed();

    // STAPI parser + planner path over OCC table/indexes.
    let shm = Arc::new(ShmArena::new(64 << 20).expect("failed to create shared arena"));
    let occ_table =
        OccTable::<StapiFlightRow>::new(Arc::clone(&shm), ROWS).expect("failed to create OCC table");
    let alt_index = Arc::new(SecondaryIndex::<usize>::new("alt"));

    for row_id in 0..ROWS {
        let alt = ((row_id % 45_000) as i64) + 500;
        let flight = if row_id % 2 == 0 {
            format!("UAL{:03}", row_id % 1_000)
        } else {
            format!("DAL{:03}", row_id % 1_000)
        };
        let typ = match row_id % 3 {
            0 => "B738",
            1 => "A320",
            _ => "B77W",
        };

        let row = StapiFlightRow::new(alt, flight.as_str(), typ);
        occ_table
            .seed_row(row_id, row)
            .expect("failed to seed OCC row for STAPI benchmark");
        alt_index.insert(IndexValue::I64(alt), row_id);
    }

    let catalog = IndexCatalog::new().with_index("alt", alt_index);
    let planner = QueryPlanner::<StapiFlightRow>::new(catalog);
    let stapi = "-compare {{match flight UAL*} {> alt 44000} {in typ {B738 A320}}} -sort alt -limit 20";

    let stapi_start = Instant::now();
    for _ in 0..PASSES {
        let plan = planner
            .compile_from_stapi(stapi)
            .expect("failed to compile STAPI query");
        let mut tx = occ_table
            .begin_transaction()
            .expect("begin_transaction failed for STAPI benchmark");
        let rows = plan
            .execute(&occ_table, &mut tx)
            .expect("STAPI plan execution failed");
        occ_table
            .abort(&mut tx)
            .expect("abort failed for STAPI benchmark");

        assert!(!rows.is_empty(), "expected STAPI query to return rows");
        assert!(rows.len() <= LIMIT);
        assert!(rows.iter().all(|row| row.alt > 44_000));
        assert!(rows.iter().all(|row| decode_ascii(&row.flight).starts_with("UAL")));
        assert!(rows.windows(2).all(|w| w[0].alt <= w[1].alt));
    }
    let stapi_elapsed = stapi_start.elapsed();

    eprintln!(
        "typed_query_elapsed={:?} stapi_parse_compile_execute_elapsed={:?} passes={} rows={}",
        typed_elapsed, stapi_elapsed, PASSES, ROWS
    );
}

#[test]
fn benchmark_tcl_style_alias_match_desc_offset_limit_path() {
    const ROWS: usize = 75_000;
    const PASSES: usize = 32;
    const LIMIT: usize = 15;
    const OFFSET: usize = 5;
    const FETCH_LIMIT: usize = LIMIT + OFFSET;

    let shm = Arc::new(ShmArena::new(96 << 20).expect("failed to create shared arena"));
    let occ_table = OccTable::<StapiFlightRow>::new(Arc::clone(&shm), ROWS)
        .expect("failed to create OCC table");
    let alt_index = Arc::new(SecondaryIndex::<usize>::new("altitude"));

    for row_id in 0..ROWS {
        let alt = ((row_id % 45_000) as i64) + 500;
        let flight = if row_id % 2 == 0 {
            format!("UAL{:03}", row_id % 1_000)
        } else {
            format!("DAL{:03}", row_id % 1_000)
        };
        let typ = match row_id % 3 {
            0 => "B738",
            1 => "A320",
            _ => "B77W",
        };
        let row = StapiFlightRow::new(alt, flight.as_str(), typ);
        occ_table
            .seed_row(row_id, row)
            .expect("failed to seed OCC row for alias benchmark");
        alt_index.insert(IndexValue::I64(alt), row_id);
    }

    let catalog = IndexCatalog::new()
        .with_index("alt", Arc::clone(&alt_index))
        .with_index("altitude", alt_index);
    let planner = QueryPlanner::<StapiFlightRow>::new(catalog);
    let stapi =
        "-compare {{match ident UAL*} {> altitude 10000} {in typ {B738 A320}}} -sort altitude";

    let start = Instant::now();
    for _ in 0..PASSES {
        let plan = planner
            .compile_from_stapi(stapi)
            .expect("failed to compile alias STAPI query");
        let mut tx = occ_table
            .begin_transaction()
            .expect("begin_transaction failed for alias benchmark");
        let mut rows = plan
            .execute(&occ_table, &mut tx)
            .expect("alias STAPI plan execution failed");
        occ_table
            .abort(&mut tx)
            .expect("abort failed for alias benchmark");

        rows.reverse(); // Tcl `-desc`.
        if rows.len() > FETCH_LIMIT {
            rows.truncate(FETCH_LIMIT);
        }
        let start_idx = OFFSET.min(rows.len()); // Tcl `-offset`.
        let end_idx = (start_idx + LIMIT).min(rows.len()); // Tcl `-limit`.
        let window = &rows[start_idx..end_idx];

        assert_eq!(window.len(), LIMIT);
        assert!(window.iter().all(|row| row.alt > 10_000));
        assert!(window.iter().all(|row| decode_ascii(&row.flight).starts_with("UAL")));
        assert!(window
            .iter()
            .all(|row| matches!(decode_ascii(&row.typ).as_str(), "B738" | "A320")));
        assert!(window.windows(2).all(|w| w[0].alt >= w[1].alt));
    }
    let elapsed = start.elapsed();

    eprintln!(
        "stapi_alias_match_desc_offset_limit_elapsed={:?} passes={} rows={}",
        elapsed, PASSES, ROWS
    );
}
