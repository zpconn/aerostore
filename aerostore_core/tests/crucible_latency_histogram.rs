#[path = "../benches/support/latency_histogram.rs"]
mod latency_histogram;

use latency_histogram::{
    bucket_bounds, latency_bucket, merge_histograms, percentile_bounds, LatencyBounds,
    HIST_BUCKETS, SUBDIVISIONS,
};

fn histogram(samples: &[u64]) -> [u64; HIST_BUCKETS] {
    let mut counts = [0; HIST_BUCKETS];
    for sample in samples {
        counts[latency_bucket(*sample)] += 1;
    }
    counts
}

#[test]
fn every_bucket_covers_exactly_its_inclusive_interval() {
    assert_eq!(HIST_BUCKETS, 3_776);
    assert_eq!(
        bucket_bounds(0),
        LatencyBounds {
            lower_ns: 0,
            upper_ns: 0
        }
    );
    assert_eq!(bucket_bounds(HIST_BUCKETS - 1).upper_ns, u64::MAX);
    for bucket in 0..HIST_BUCKETS {
        let bounds = bucket_bounds(bucket);
        assert!(bounds.lower_ns <= bounds.upper_ns);
        assert_eq!(latency_bucket(bounds.lower_ns), bucket);
        assert_eq!(latency_bucket(bounds.upper_ns), bucket);
        let midpoint = bounds.lower_ns + (bounds.upper_ns - bounds.lower_ns) / 2;
        assert_eq!(latency_bucket(midpoint), bucket);
        if bucket > 0 {
            assert_eq!(latency_bucket(bounds.lower_ns - 1), bucket - 1);
            assert_eq!(bucket_bounds(bucket - 1).upper_ns + 1, bounds.lower_ns);
        }
        if bucket + 1 < HIST_BUCKETS {
            assert_eq!(latency_bucket(bounds.upper_ns + 1), bucket + 1);
        }
        assert!(
            u128::from(bounds.upper_ns - bounds.lower_ns) * SUBDIVISIONS as u128
                <= u128::from(bounds.lower_ns)
        );
    }
}

#[test]
fn small_values_and_power_of_two_edges_are_not_rounded_outside_their_bucket() {
    for value in 0..128 {
        assert_eq!(
            bucket_bounds(latency_bucket(value)),
            LatencyBounds {
                lower_ns: value,
                upper_ns: value,
            }
        );
    }
    for exponent in 0..64 {
        let power = 1_u64 << exponent;
        for value in [power - 1, power, power + 1] {
            let bounds = bucket_bounds(latency_bucket(value));
            assert!(bounds.lower_ns <= value && value <= bounds.upper_ns);
        }
        assert_eq!(bucket_bounds(latency_bucket(power)).lower_ns, power);
    }
    assert_eq!(latency_bucket(u64::MAX), HIST_BUCKETS - 1);
}

#[test]
fn nearest_rank_handles_empty_and_small_sample_count_boundaries() {
    assert_eq!(
        percentile_bounds(&[0; HIST_BUCKETS], 0, 99),
        LatencyBounds::default()
    );
    for count in [1, 2, 3, 99, 100, 101, 199, 200, 201] {
        let samples: Vec<u64> = (0..count).collect();
        let counts = histogram(&samples);
        for percentile in [1, 50, 90, 99, 100] {
            let rank = (count * u64::from(percentile)).div_ceil(100);
            let exact = samples[(rank - 1) as usize];
            let bounds = percentile_bounds(&counts, count, percentile);
            assert!(
                bounds.lower_ns <= exact && exact <= bounds.upper_ns,
                "count={count}, percentile={percentile}, exact={exact}, bounds={bounds:?}"
            );
        }
    }
}

#[test]
fn nearest_rank_remains_exact_above_float_precision_and_at_u64_max() {
    for total in [(1_u64 << 53) + 1, u64::MAX] {
        for percentile in [1, 50, 90, 99, 100] {
            let rank = (u128::from(total) * u128::from(percentile)).div_ceil(100) as u64;
            let mut counts = [0; HIST_BUCKETS];
            counts[10] = rank - 1;
            counts[20] = total - (rank - 1);
            assert_eq!(
                percentile_bounds(&counts, total, percentile),
                LatencyBounds {
                    lower_ns: 20,
                    upper_ns: 20,
                }
            );
            counts[10] += 1;
            counts[20] -= 1;
            assert_eq!(
                percentile_bounds(&counts, total, percentile),
                LatencyBounds {
                    lower_ns: 10,
                    upper_ns: 10,
                }
            );
        }
    }
}

#[test]
fn merged_quantiles_contain_sorted_oracle_for_samples_across_every_bucket() {
    let mut samples = vec![0, 0, u64::MAX, u64::MAX];
    for bucket in 0..HIST_BUCKETS {
        let bounds = bucket_bounds(bucket);
        samples.extend([bounds.lower_ns, bounds.upper_ns]);
        if bounds.lower_ns > 0 {
            samples.push(bounds.lower_ns - 1);
        }
        if bounds.upper_ns < u64::MAX {
            samples.push(bounds.upper_ns + 1);
        }
    }
    let mut shards = [[0; HIST_BUCKETS]; 3];
    for (idx, sample) in samples.iter().enumerate() {
        shards[idx % 3][latency_bucket(*sample)] += 1;
    }
    let mut merged = [0; HIST_BUCKETS];
    for shard in shards {
        merge_histograms(&mut merged, &shard);
    }
    assert_eq!(merged, histogram(&samples));
    samples.sort_unstable();
    for percentile in 1..=100 {
        let rank = (samples.len() * percentile as usize).div_ceil(100);
        let exact = samples[rank - 1];
        let bounds = percentile_bounds(&merged, samples.len() as u64, percentile);
        assert!(
            bounds.lower_ns <= exact && exact <= bounds.upper_ns,
            "percentile={percentile}, exact={exact}, bounds={bounds:?}"
        );
    }
}

#[test]
fn former_same_bucket_near_doubling_is_now_distinguishable() {
    let baseline = percentile_bounds(&histogram(&[524_288; 100]), 100, 99);
    let candidate = percentile_bounds(&histogram(&[1_000_000; 100]), 100, 99);
    // Even the most favorable endpoints prove a regression beyond 10%.
    assert!(u128::from(candidate.lower_ns) * 10 > u128::from(baseline.upper_ns) * 11);
}

#[test]
#[should_panic(expected = "inconsistent latency histogram")]
fn rejects_inconsistent_sample_total_instead_of_returning_a_fake_tail() {
    percentile_bounds(&histogram(&[1, 2, 3]), 4, 99);
}

#[test]
#[should_panic(expected = "latency sample count overflow")]
fn rejects_counter_overflow_instead_of_saturating_silently() {
    let mut destination = [0; HIST_BUCKETS];
    let mut source = [0; HIST_BUCKETS];
    destination[0] = u64::MAX;
    source[0] = 1;
    merge_histograms(&mut destination, &source);
}
