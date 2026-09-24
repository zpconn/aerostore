use super::*;

#[test]
fn canonical_sets_cover_empty_duplicates_collisions_and_boundaries() {
    for count in [0, 1, 2, 8, 4096] {
        for input in [
            vec![],
            vec![0],
            vec![0, 0],
            vec![7, 0, 3, 7],
            vec![4095, 0, 4095],
        ] {
            let expected = if let Some(invalid) = input.iter().find(|id| **id >= count) {
                Err(*invalid)
            } else {
                let mut sorted = input.clone();
                sorted.sort_unstable();
                sorted.dedup();
                Ok(sorted)
            };
            assert_eq!(canonical_buckets_sort(&input, count), expected);
            assert_eq!(canonical_buckets_bitmap(&input, count), expected);
        }
    }
}

#[test]
fn all_short_inputs_match_standard_sort_and_reject_late_invalid_values() {
    for encoding in 0..4096usize {
        let input = [
            encoding % 8,
            (encoding / 8) % 8,
            (encoding / 64) % 8,
            (encoding / 512) % 8,
        ];
        for count in 0..=8 {
            let expected = if let Some(invalid) = input.iter().find(|id| **id >= count) {
                Err(*invalid)
            } else {
                let mut sorted = input.to_vec();
                sorted.sort_unstable();
                sorted.dedup();
                Ok(sorted)
            };
            assert_eq!(canonical_buckets_sort(&input, count), expected);
            assert_eq!(canonical_buckets_bitmap(&input, count), expected);
        }
    }
}

#[test]
fn stamp_decision_covers_equality_zero_and_maximum() {
    assert!(!stamp_precedes_snapshot(0, 0));
    assert!(stamp_precedes_snapshot(0, 1));
    assert!(!stamp_precedes_snapshot(1, 1));
    assert!(!stamp_precedes_snapshot(u64::MAX, 1));
    assert!(stamp_precedes_snapshot(u64::MAX - 1, u64::MAX));
    assert!(!stamp_precedes_snapshot(u64::MAX, u64::MAX));
}
