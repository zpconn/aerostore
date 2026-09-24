//! Bounded latency measurement shared by the Crucible and its ordinary tests.
//!
//! There are 64 subdivisions per power of two, with exact buckets below 128 ns.
//! Every reported interval is inclusive and contains its nearest-rank quantile;
//! its upper endpoint is at most 1 + 1/64 times its positive lower endpoint.
//! This module measures observations; it does not verify the database engine.

pub const SUBDIVISIONS: usize = 64;
const SUBDIVISION_BITS: u32 = 6;
pub const HIST_BUCKETS: usize = SUBDIVISIONS * (65 - SUBDIVISION_BITS as usize);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatencyBounds {
    pub lower_ns: u64,
    pub upper_ns: u64,
}

#[inline]
pub fn latency_bucket(latency_ns: u64) -> usize {
    if latency_ns < SUBDIVISIONS as u64 {
        return latency_ns as usize;
    }
    let exponent = 63 - latency_ns.leading_zeros();
    let shift = exponent - SUBDIVISION_BITS;
    let subdivision = (latency_ns >> shift) as usize - SUBDIVISIONS;
    SUBDIVISIONS + shift as usize * SUBDIVISIONS + subdivision
}

pub fn bucket_bounds(bucket: usize) -> LatencyBounds {
    assert!(bucket < HIST_BUCKETS, "latency bucket out of range");
    if bucket < SUBDIVISIONS {
        return LatencyBounds {
            lower_ns: bucket as u64,
            upper_ns: bucket as u64,
        };
    }
    let relative = bucket - SUBDIVISIONS;
    let shift = relative / SUBDIVISIONS;
    let subdivision = relative % SUBDIVISIONS;
    let lower_ns = (SUBDIVISIONS as u64 + subdivision as u64) << shift;
    // The final bucket ends at u64::MAX without overflowing this addition.
    let upper_ns = lower_ns + ((1_u64 << shift) - 1);
    LatencyBounds { lower_ns, upper_ns }
}

pub fn merge_histograms(dst: &mut [u64; HIST_BUCKETS], src: &[u64; HIST_BUCKETS]) {
    for (dst, src) in dst.iter_mut().zip(src) {
        *dst = dst
            .checked_add(*src)
            .expect("latency sample count overflow");
    }
}

/// Inclusive bounds for ceil(total_samples * percentile / 100), without float
/// rounding or intermediate u64 overflow. Call only after writers have stopped.
pub fn percentile_bounds(
    hist: &[u64; HIST_BUCKETS],
    total_samples: u64,
    percentile: u32,
) -> LatencyBounds {
    assert!((1..=100).contains(&percentile));
    let observed: u128 = hist.iter().map(|count| u128::from(*count)).sum();
    assert_eq!(
        observed,
        u128::from(total_samples),
        "inconsistent latency histogram"
    );
    if total_samples == 0 {
        return LatencyBounds::default();
    }
    let rank = (u128::from(total_samples) * u128::from(percentile)).div_ceil(100);
    let mut cumulative = 0_u128;
    for (idx, count) in hist.iter().enumerate() {
        cumulative += u128::from(*count);
        if cumulative >= rank {
            return bucket_bounds(idx);
        }
    }
    unreachable!("validated histogram contains the requested rank")
}
