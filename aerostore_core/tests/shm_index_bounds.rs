use std::sync::Arc;

use aerostore_core::{IndexCompare, IndexValue, SecondaryIndex, ShmArena, ShmIndexError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct BigRowId {
    bytes: Vec<u8>,
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
