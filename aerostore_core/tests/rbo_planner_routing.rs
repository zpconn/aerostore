use std::sync::Arc;

use aerostore_core::{
    AccessPath, IndexCompare, IndexValue, OccTable, RouteKind, RuleBasedOptimizer, SchemaCatalog,
    SecondaryIndex, ShmArena, ShmPrimaryKeyMap, StapiRow, StapiValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FlightRow {
    flight_id: [u8; 8],
    dest: [u8; 4],
    geohash: [u8; 8],
    typ: [u8; 4],
    altitude: i64,
}

impl FlightRow {
    fn new(flight_id: &str, dest: &str, geohash: &str, typ: &str, altitude: i64) -> Self {
        Self {
            flight_id: fixed_ascii::<8>(flight_id),
            dest: fixed_ascii::<4>(dest),
            geohash: fixed_ascii::<8>(geohash),
            typ: fixed_ascii::<4>(typ),
            altitude,
        }
    }
}

impl StapiRow for FlightRow {
    fn has_field(field: &str) -> bool {
        matches!(
            field,
            "flight_id"
                | "flight"
                | "ident"
                | "dest"
                | "geohash"
                | "typ"
                | "type"
                | "alt"
                | "altitude"
        )
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        match field {
            "flight_id" | "flight" | "ident" => {
                Some(StapiValue::Text(decode_ascii(&self.flight_id)))
            }
            "dest" => Some(StapiValue::Text(decode_ascii(&self.dest))),
            "geohash" => Some(StapiValue::Text(decode_ascii(&self.geohash))),
            "typ" | "type" => Some(StapiValue::Text(decode_ascii(&self.typ))),
            "alt" | "altitude" => Some(StapiValue::Int(self.altitude)),
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

fn seed_optimizer() -> (OccTable<FlightRow>, RuleBasedOptimizer<FlightRow>) {
    let shm = Arc::new(ShmArena::new(32 << 20).expect("failed to allocate shared arena"));
    let table =
        OccTable::<FlightRow>::new(Arc::clone(&shm), 8).expect("failed to create OCC table");

    let rows = [
        FlightRow::new("UAL123", "KORD", "dp3wxyz", "B738", 11_000),
        FlightRow::new("UAL124", "KORD", "dp3wxya", "B739", 12_500),
        FlightRow::new("DAL456", "KATL", "dn5abc1", "A320", 9_500),
        FlightRow::new("AAL789", "KLAX", "9q5def2", "B77W", 35_000),
        FlightRow::new("UAL999", "KDEN", "9x1ghi3", "B738", 14_500),
        FlightRow::new("SWA111", "KDAL", "9x1ghi4", "B737", 10_000),
        FlightRow::new("DAL999", "KORD", "dp3wxyb", "A321", 13_000),
        FlightRow::new("JBU222", "KBOS", "drt2klm", "E190", 8_500),
    ];

    let pk_map = Arc::new(
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 256, 8)
            .expect("failed to create shared pk map"),
    );
    let flight_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "flight_id",
        Arc::clone(&shm),
    ));
    let dest_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "dest",
        Arc::clone(&shm),
    ));
    let geohash_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "geohash",
        Arc::clone(&shm),
    ));
    let alt_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "altitude",
        Arc::clone(&shm),
    ));

    for (row_id, row) in rows.into_iter().enumerate() {
        table
            .seed_row(row_id, row)
            .expect("failed to seed test row");
        pk_map
            .insert_existing(decode_ascii(&row.flight_id).as_str(), row_id)
            .expect("failed to seed pk map");
        flight_idx.insert(IndexValue::String(decode_ascii(&row.flight_id)), row_id);
        dest_idx.insert(IndexValue::String(decode_ascii(&row.dest)), row_id);
        geohash_idx.insert(IndexValue::String(decode_ascii(&row.geohash)), row_id);
        alt_idx.insert(IndexValue::I64(row.altitude), row_id);
    }

    let mut catalog = SchemaCatalog::new("flight_id").with_primary_key_map(pk_map);
    catalog.insert_index("flight_id", flight_idx);
    catalog.insert_index("dest", dest_idx);
    catalog.insert_index("geohash", geohash_idx);
    catalog.insert_index("altitude", Arc::clone(&alt_idx));
    catalog.insert_index("alt", alt_idx);
    catalog.set_cardinality_rank("flight_id", 0);
    catalog.set_cardinality_rank("geohash", 1);
    catalog.set_cardinality_rank("dest", 2);
    catalog.set_cardinality_rank("altitude", 3);
    catalog.set_cardinality_rank("alt", 3);

    let optimizer = RuleBasedOptimizer::<FlightRow>::new(catalog);
    (table, optimizer)
}

#[test]
fn rbo_routes_point_lookup_to_shared_primary_key_map() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{= flight_id UAL123}}")
        .expect("failed to compile pk query");

    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    assert_eq!(plan.driver_field(), Some("flight_id"));
    assert!(matches!(
        plan.access_path(),
        AccessPath::PrimaryKeyEq { .. }
    ));
    assert!(plan.residual_filter_fields().is_empty());

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("pk plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(decode_ascii(&rows[0].flight_id), "UAL123");
}

#[test]
fn rbo_prefers_rule1_primary_key_over_any_other_route() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{= flight_id UAL124} {= dest KORD} {> altitude 10000}}")
        .expect("failed to compile mixed pk query");

    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    assert_eq!(plan.driver_field(), Some("flight_id"));
    let residual = plan.residual_filter_fields();
    assert!(residual.contains(&"dest"));
    assert!(residual.contains(&"altitude"));

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("pk-precedence execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(decode_ascii(&rows[0].flight_id), "UAL124");
}

#[test]
fn rbo_selects_dest_index_exact_match_over_altitude_range_and_defers_match_filter() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi(
            "-compare {{> altitude 10000} {= dest KORD} {match typ B73*}} -limit 50",
        )
        .expect("failed to compile dest/alt/match query");

    assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
    assert_eq!(plan.driver_field(), Some("dest"));
    match plan.access_path() {
        AccessPath::Indexed { compare, .. } => {
            assert!(matches!(compare, IndexCompare::Eq(_)));
        }
        _ => panic!("expected indexed access path"),
    }
    let residual = plan.residual_filter_fields();
    assert!(residual.contains(&"altitude"));
    assert!(residual.contains(&"typ"));

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("dest-index plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| decode_ascii(&row.dest) == "KORD"));
    assert!(rows.iter().all(|row| row.altitude > 10_000));
    assert!(rows
        .iter()
        .all(|row| decode_ascii(&row.typ).starts_with("B73")));
}

#[test]
fn rbo_uses_index_range_scan_for_greater_than() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{> altitude 10000}}")
        .expect("failed to compile gt query");

    assert_eq!(plan.route_kind(), RouteKind::IndexRangeScan);
    assert_eq!(plan.driver_field(), Some("altitude"));
    match plan.access_path() {
        AccessPath::Indexed { compare, .. } => assert!(matches!(compare, IndexCompare::Gt(_))),
        _ => panic!("expected indexed access path"),
    }

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("gt-index plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert!(rows.iter().all(|row| row.altitude > 10_000));
}

#[test]
fn rbo_uses_index_range_scan_for_greater_or_equal() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{>= altitude 10000}}")
        .expect("failed to compile gte query");

    assert_eq!(plan.route_kind(), RouteKind::IndexRangeScan);
    assert_eq!(plan.driver_field(), Some("altitude"));
    match plan.access_path() {
        AccessPath::Indexed { compare, .. } => assert!(matches!(compare, IndexCompare::Gte(_))),
        _ => panic!("expected indexed access path"),
    }

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("gte-index plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert!(rows.iter().all(|row| row.altitude >= 10_000));
}

#[test]
fn rbo_tie_breaker_prefers_dest_eq_over_altitude_eq() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{= altitude 11000} {= dest KORD}}")
        .expect("failed to compile eq tie-break query");

    assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
    assert_eq!(plan.driver_field(), Some("dest"));

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("eq tie-break execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].altitude, 11_000);
    assert_eq!(decode_ascii(&rows[0].dest), "KORD");
}

#[test]
fn rbo_tie_breaker_prefers_geohash_range_over_altitude_range() {
    let (_table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{> altitude 10000} {> geohash 9}}")
        .expect("failed to compile range tie-break query");

    assert_eq!(plan.route_kind(), RouteKind::IndexRangeScan);
    assert_eq!(plan.driver_field(), Some("geohash"));
}

#[test]
fn rbo_falls_back_to_full_scan_for_unindexed_predicates() {
    let (table, optimizer) = seed_optimizer();
    let plan = optimizer
        .compile_from_stapi("-compare {{match typ B7*}}")
        .expect("failed to compile full-scan query");

    assert_eq!(plan.route_kind(), RouteKind::FullScan);
    assert_eq!(plan.driver_field(), None);
    assert!(matches!(plan.access_path(), AccessPath::FullScan));

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("full-scan plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|row| decode_ascii(&row.typ).starts_with("B7")));
}

#[test]
fn rbo_rejects_malformed_stapi_without_panic() {
    let (_table, optimizer) = seed_optimizer();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        optimizer
            .compile_from_stapi("-compare {{> altitude 10000}")
            .map_err(|err| err.tcl_error_message())
    }));

    assert!(result.is_ok(), "planner boundary must not panic");
    let err = result.expect("boundary should not panic");
    assert!(err.is_err(), "malformed query must return an error");
    let message = match err {
        Ok(_) => panic!("malformed query should fail"),
        Err(message) => message,
    };
    assert!(message.starts_with("TCL_ERROR:"));
}

#[test]
fn rbo_rejects_unknown_field_with_explicit_error() {
    let (_table, optimizer) = seed_optimizer();
    let err = match optimizer.compile_from_stapi("-compare {{= unknown 1}}") {
        Ok(_) => panic!("unknown field should fail planning"),
        Err(err) => err,
    };
    let message = err.tcl_error_message();
    assert!(message.starts_with("TCL_ERROR:"));
    assert!(message.contains("unknown field"));
}
