use std::collections::BTreeSet;
use std::sync::Arc;

use aerostore_core::{IndexCompare, IndexValue, SecondaryIndex, ShmArena, ShmIndexError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct BigRowId {
    bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
enum RangeMode {
    Gt,
    Gte,
    Lt,
    Lte,
}

fn matches_range_mode(value: i64, mode: RangeMode, threshold: i64) -> bool {
    match mode {
        RangeMode::Gt => value > threshold,
        RangeMode::Gte => value >= threshold,
        RangeMode::Lt => value < threshold,
        RangeMode::Lte => value <= threshold,
    }
}

fn lookup_range_set(index: &SecondaryIndex<u32>, mode: RangeMode, threshold: i64) -> BTreeSet<u32> {
    let compare = match mode {
        RangeMode::Gt => IndexCompare::Gt(IndexValue::I64(threshold)),
        RangeMode::Gte => IndexCompare::Gte(IndexValue::I64(threshold)),
        RangeMode::Lt => IndexCompare::Lt(IndexValue::I64(threshold)),
        RangeMode::Lte => IndexCompare::Lte(IndexValue::I64(threshold)),
    };
    index.lookup(&compare).into_iter().collect()
}

fn scan_reference_set(fixtures: &[(i64, u32)], mode: RangeMode, threshold: i64) -> BTreeSet<u32> {
    fixtures
        .iter()
        .filter_map(|(altitude, row_id)| {
            matches_range_mode(*altitude, mode, threshold).then_some(*row_id)
        })
        .collect()
}

#[test]
fn oversized_string_key_is_rejected_without_mutating_index() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u64>::new_in_shared("flight", Arc::clone(&shm));
    let oversized = "X".repeat(129);

    let err = index
        .try_insert(IndexValue::String(oversized), 17)
        .expect_err("oversized key should return KeyTooLong");
    match err {
        ShmIndexError::KeyTooLong { len, max } => {
            assert_eq!(len, 129);
            assert_eq!(max, 128);
        }
        other => panic!("unexpected error variant for oversized key: {:?}", other),
    }

    let hits = index.lookup(&IndexCompare::Eq(IndexValue::String("UAL123".to_string())));
    assert!(
        hits.is_empty(),
        "failed insert must not leave indexed entries"
    );
    assert!(
        index.traverse().is_empty(),
        "failed insert must not create traversable nodes"
    );
}

#[test]
fn oversized_row_id_payload_is_rejected_without_mutating_index() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<BigRowId>::new_in_shared("altitude", Arc::clone(&shm));
    let huge_row = BigRowId {
        bytes: vec![42_u8; 256],
    };

    let err = index
        .try_insert(IndexValue::I64(12_000), huge_row)
        .expect_err("oversized row payload should return RowIdTooLarge");
    match err {
        ShmIndexError::RowIdTooLarge { len, max } => {
            assert!(
                len > max,
                "reported payload length must exceed inline row-id capacity"
            );
            assert_eq!(max, 192);
        }
        other => panic!("unexpected error variant for oversized row-id: {:?}", other),
    }

    let hits = index.lookup(&IndexCompare::Eq(IndexValue::I64(12_000)));
    assert!(
        hits.is_empty(),
        "failed insert must not leave indexed entries"
    );
    assert!(
        index.traverse().is_empty(),
        "failed insert must not create traversable nodes"
    );
}

#[test]
fn range_operators_respect_inclusive_exclusive_boundaries_with_duplicates() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));

    let fixtures = [
        (10_i64, 101_u32),
        (10, 102),
        (20, 201),
        (20, 202),
        (30, 301),
    ];
    for (altitude, row_id) in fixtures {
        index.insert(IndexValue::I64(altitude), row_id);
    }

    assert_eq!(
        lookup_range_set(&index, RangeMode::Gt, 20),
        BTreeSet::from([301_u32]),
        "GT should exclude duplicate boundary key and return strict tail"
    );
    assert_eq!(
        lookup_range_set(&index, RangeMode::Gte, 20),
        BTreeSet::from([201_u32, 202, 301]),
        "GTE should include all duplicate boundary rows"
    );
    assert_eq!(
        lookup_range_set(&index, RangeMode::Lt, 20),
        BTreeSet::from([101_u32, 102]),
        "LT should exclude duplicate boundary key"
    );
    assert_eq!(
        lookup_range_set(&index, RangeMode::Lte, 20),
        BTreeSet::from([101_u32, 102, 201, 202]),
        "LTE should include all duplicate boundary rows"
    );

    assert!(
        lookup_range_set(&index, RangeMode::Gt, 30).is_empty(),
        "GT above max key should be empty"
    );
    assert!(
        lookup_range_set(&index, RangeMode::Lt, 10).is_empty(),
        "LT below min key should be empty"
    );
    assert_eq!(
        lookup_range_set(&index, RangeMode::Gt, 25),
        BTreeSet::from([301_u32]),
        "GT on interior gap should start at successor key"
    );
    assert_eq!(
        lookup_range_set(&index, RangeMode::Lte, 9),
        BTreeSet::new(),
        "LTE below min key should be empty"
    );
}

#[test]
fn range_operators_match_scan_reference_across_threshold_grid() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared arena"));
    let index = SecondaryIndex::<u32>::new_in_shared("altitude", Arc::clone(&shm));

    let fixtures = [
        (-5_i64, 1_u32),
        (0, 2),
        (0, 3),
        (7, 4),
        (10, 5),
        (10, 6),
        (10, 7),
        (15, 8),
        (22, 9),
        (40, 10),
    ];
    for (altitude, row_id) in fixtures {
        index.insert(IndexValue::I64(altitude), row_id);
    }

    let thresholds = [-10_i64, -5, -1, 0, 6, 10, 11, 22, 23, 100];
    let modes = [RangeMode::Gt, RangeMode::Gte, RangeMode::Lt, RangeMode::Lte];
    for mode in modes {
        for threshold in thresholds {
            let indexed = lookup_range_set(&index, mode, threshold);
            let scanned = scan_reference_set(&fixtures, mode, threshold);
            assert_eq!(
                indexed,
                scanned,
                "range parity mismatch for mode={} threshold={}",
                match mode {
                    RangeMode::Gt => "gt",
                    RangeMode::Gte => "gte",
                    RangeMode::Lt => "lt",
                    RangeMode::Lte => "lte",
                },
                threshold
            );
        }
    }
}
