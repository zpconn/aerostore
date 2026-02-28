use std::sync::Arc;

use aerostore_core::{
    AccessPath, IndexCompare, IndexValue, OccTable, RouteKind, RuleBasedOptimizer, SchemaCatalog,
    SecondaryIndex, ShmArena, ShmPrimaryKeyMap, StapiRow, StapiValue,
};

const AIRCRAFT_TYPES: [&str; 5] = ["B738", "A320", "E190", "B77W", "CRJ9"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FlightRow {
    flight_id: [u8; 8],
    aircraft_type: [u8; 8],
}

impl FlightRow {
    fn new(flight_id: &str, aircraft_type: &str) -> Self {
        Self {
            flight_id: fixed_ascii::<8>(flight_id),
            aircraft_type: fixed_ascii::<8>(aircraft_type),
        }
    }
}

impl StapiRow for FlightRow {
    fn has_field(field: &str) -> bool {
        matches!(field, "flight_id" | "aircraft_type")
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        match field {
            "flight_id" => Some(StapiValue::Text(decode_ascii(&self.flight_id))),
            "aircraft_type" => Some(StapiValue::Text(decode_ascii(&self.aircraft_type))),
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
fn planner_selects_highest_cardinality_index_and_demotes_other_eq_predicates() {
    const ROWS: usize = 10_000;
    const TARGET_ROW_ID: usize = 4_240;

    let shm = Arc::new(ShmArena::new(128 << 20).expect("failed to create shared arena"));
    let table =
        OccTable::<FlightRow>::new(Arc::clone(&shm), ROWS).expect("failed to create OCC table");
    let flight_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "flight_id",
        Arc::clone(&shm),
    ));
    let aircraft_type_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "aircraft_type",
        Arc::clone(&shm),
    ));

    for row_id in 0..ROWS {
        let flight_id = format!("U{:07}", row_id);
        let aircraft_type = AIRCRAFT_TYPES[row_id % AIRCRAFT_TYPES.len()];
        table
            .seed_row(row_id, FlightRow::new(flight_id.as_str(), aircraft_type))
            .expect("failed to seed row");
        flight_idx.insert(IndexValue::String(flight_id), row_id);
        aircraft_type_idx.insert(IndexValue::String(aircraft_type.to_string()), row_id);
    }

    assert_eq!(flight_idx.distinct_key_count(), ROWS);
    assert_eq!(aircraft_type_idx.distinct_key_count(), AIRCRAFT_TYPES.len());

    let catalog = SchemaCatalog::new("pk_unused")
        .with_index("flight_id", Arc::clone(&flight_idx))
        .with_index("aircraft_type", Arc::clone(&aircraft_type_idx));
    let optimizer = RuleBasedOptimizer::<FlightRow>::new(catalog);

    let target_flight_id = format!("U{:07}", TARGET_ROW_ID);
    let target_aircraft_type = AIRCRAFT_TYPES[TARGET_ROW_ID % AIRCRAFT_TYPES.len()];
    let stapi_variants = [
        format!(
            "-compare {{{{= flight_id {}}} {{= aircraft_type {}}}}}",
            target_flight_id, target_aircraft_type
        ),
        format!(
            "-compare {{{{= aircraft_type {}}} {{= flight_id {}}}}}",
            target_aircraft_type, target_flight_id
        ),
    ];

    for stapi in stapi_variants {
        let plan = optimizer
            .compile_from_stapi(stapi.as_str())
            .expect("failed to compile cardinality query");

        assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
        assert_eq!(plan.driver_field(), Some("flight_id"));
        assert_eq!(plan.residual_filter_fields(), vec!["aircraft_type"]);

        match plan.access_path() {
            AccessPath::Indexed { field, compare } => {
                assert_eq!(field, "flight_id");
                assert_eq!(
                    compare,
                    &IndexCompare::Eq(IndexValue::String(target_flight_id.clone()))
                );
            }
            _ => panic!("expected indexed equality access path"),
        }

        let mut tx = table.begin_transaction().expect("begin_transaction failed");
        let rows = plan
            .execute(&table, &mut tx)
            .expect("plan execution should succeed");
        table.abort(&mut tx).expect("abort failed");

        assert_eq!(rows.len(), 1);
        assert_eq!(decode_ascii(&rows[0].flight_id), target_flight_id);
        assert_eq!(decode_ascii(&rows[0].aircraft_type), target_aircraft_type);
    }
}

#[test]
fn planner_prefers_primary_key_when_cardinality_ties_secondary_index() {
    const ROWS: usize = 4_096;
    const TARGET_ROW_ID: usize = 1_337;

    let shm = Arc::new(ShmArena::new(128 << 20).expect("failed to create shared arena"));
    let table =
        OccTable::<FlightRow>::new(Arc::clone(&shm), ROWS).expect("failed to create OCC table");
    let pk_map = Arc::new(
        ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 2_048, ROWS)
            .expect("failed to create shared primary key map"),
    );
    let aircraft_type_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "aircraft_type",
        Arc::clone(&shm),
    ));

    for row_id in 0..ROWS {
        let flight_id = format!("U{:07}", row_id);
        let aircraft_type = format!("T{:07}", row_id);
        table
            .seed_row(
                row_id,
                FlightRow::new(flight_id.as_str(), aircraft_type.as_str()),
            )
            .expect("failed to seed row");
        pk_map
            .insert_existing(flight_id.as_str(), row_id)
            .expect("failed to seed primary key map");
        aircraft_type_idx.insert(IndexValue::String(aircraft_type), row_id);
    }

    assert_eq!(pk_map.distinct_key_count(), ROWS);
    assert_eq!(aircraft_type_idx.distinct_key_count(), ROWS);

    let catalog = SchemaCatalog::new("flight_id")
        .with_primary_key_map(Arc::clone(&pk_map))
        .with_index("aircraft_type", Arc::clone(&aircraft_type_idx));
    let optimizer = RuleBasedOptimizer::<FlightRow>::new(catalog);

    let target_flight_id = format!("U{:07}", TARGET_ROW_ID);
    let target_aircraft_type = format!("T{:07}", TARGET_ROW_ID);
    let stapi = format!(
        "-compare {{{{= flight_id {}}} {{= aircraft_type {}}}}}",
        target_flight_id, target_aircraft_type
    );
    let plan = optimizer
        .compile_from_stapi(stapi.as_str())
        .expect("failed to compile tie-cardinality query");

    assert_eq!(plan.route_kind(), RouteKind::PrimaryKeyLookup);
    assert_eq!(plan.driver_field(), Some("flight_id"));
    assert_eq!(plan.residual_filter_fields(), vec!["aircraft_type"]);
    match plan.access_path() {
        AccessPath::PrimaryKeyEq { field, key } => {
            assert_eq!(field, "flight_id");
            assert_eq!(key, &target_flight_id);
        }
        _ => panic!("expected primary-key equality access path"),
    }

    let mut tx = table.begin_transaction().expect("begin_transaction failed");
    let rows = plan
        .execute(&table, &mut tx)
        .expect("plan execution should succeed");
    table.abort(&mut tx).expect("abort failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(decode_ascii(&rows[0].flight_id), target_flight_id);
    assert_eq!(decode_ascii(&rows[0].aircraft_type), target_aircraft_type);
}

#[test]
fn planner_driver_is_stable_with_reordered_mixed_predicates() {
    const ROWS: usize = 10_000;
    const TARGET_ROW_ID: usize = 4_240;

    let shm = Arc::new(ShmArena::new(128 << 20).expect("failed to create shared arena"));
    let table =
        OccTable::<FlightRow>::new(Arc::clone(&shm), ROWS).expect("failed to create OCC table");
    let flight_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "flight_id",
        Arc::clone(&shm),
    ));
    let aircraft_type_idx = Arc::new(SecondaryIndex::<usize>::new_in_shared(
        "aircraft_type",
        Arc::clone(&shm),
    ));

    for row_id in 0..ROWS {
        let flight_id = format!("U{:07}", row_id);
        let aircraft_type = AIRCRAFT_TYPES[row_id % AIRCRAFT_TYPES.len()];
        table
            .seed_row(row_id, FlightRow::new(flight_id.as_str(), aircraft_type))
            .expect("failed to seed row");
        flight_idx.insert(IndexValue::String(flight_id), row_id);
        aircraft_type_idx.insert(IndexValue::String(aircraft_type.to_string()), row_id);
    }

    let catalog = SchemaCatalog::new("pk_unused")
        .with_index("flight_id", Arc::clone(&flight_idx))
        .with_index("aircraft_type", Arc::clone(&aircraft_type_idx));
    let optimizer = RuleBasedOptimizer::<FlightRow>::new(catalog);

    let target_flight_id = format!("U{:07}", TARGET_ROW_ID);
    let target_aircraft_type = AIRCRAFT_TYPES[TARGET_ROW_ID % AIRCRAFT_TYPES.len()];
    let stapi_variants = [
        format!(
            "-compare {{{{= flight_id {}}} {{= aircraft_type {}}} {{notmatch aircraft_type Z*}}}}",
            target_flight_id, target_aircraft_type
        ),
        format!(
            "-compare {{{{notmatch aircraft_type Z*}} {{= aircraft_type {}}} {{= flight_id {}}}}}",
            target_aircraft_type, target_flight_id
        ),
    ];

    for stapi in stapi_variants {
        let plan = optimizer
            .compile_from_stapi(stapi.as_str())
            .expect("failed to compile mixed predicate query");

        assert_eq!(plan.route_kind(), RouteKind::IndexExactMatch);
        assert_eq!(plan.driver_field(), Some("flight_id"));

        let residual = plan.residual_filter_fields();
        assert_eq!(residual.len(), 2);
        assert!(
            residual.iter().all(|field| *field == "aircraft_type"),
            "both non-driver predicates should remain residual aircraft_type filters"
        );

        let mut tx = table.begin_transaction().expect("begin_transaction failed");
        let rows = plan
            .execute(&table, &mut tx)
            .expect("plan execution should succeed");
        table.abort(&mut tx).expect("abort failed");

        assert_eq!(rows.len(), 1);
        assert_eq!(decode_ascii(&rows[0].flight_id), target_flight_id);
        assert_eq!(decode_ascii(&rows[0].aircraft_type), target_aircraft_type);
    }
}
