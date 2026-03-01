use std::hint::black_box;
use std::time::{Duration, Instant};

use aerostore_core::{build_update_record, serialize_delta_wal_record};
use criterion::{criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};

const UPDATES: usize = 100_000;
const REQUIRED_REDUCTION_ONE_COL: f64 = 0.90;
const REQUIRED_REDUCTION_TWO_COL: f64 = 0.75;
const REQUIRED_REDUCTION_FOUR_COL: f64 = 0.60;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct FlightStateBench {
    flight_id: u64,
    altitude: f64,
    lat: f64,
    lon: f64,
    heading: f64,
    groundspeed: f64,
    // Dense payload approximating a high-column flight state row.
    aux_a: [f64; 24],
    aux_b: [f64; 24],
    aux_c: [f64; 24],
    aux_d: [f64; 24],
}

impl aerostore_core::WalDeltaCodec for FlightStateBench {}

#[derive(Clone, Copy, Debug)]
enum MutationProfile {
    OneColumn,
    TwoColumns,
    FourColumns,
}

impl MutationProfile {
    fn label(self) -> &'static str {
        match self {
            MutationProfile::OneColumn => "one_col",
            MutationProfile::TwoColumns => "two_col",
            MutationProfile::FourColumns => "four_col",
        }
    }

    fn required_reduction(self) -> f64 {
        match self {
            MutationProfile::OneColumn => REQUIRED_REDUCTION_ONE_COL,
            MutationProfile::TwoColumns => REQUIRED_REDUCTION_TWO_COL,
            MutationProfile::FourColumns => REQUIRED_REDUCTION_FOUR_COL,
        }
    }

    fn apply(self, row: &mut FlightStateBench, step: usize) {
        let stepf = step as f64;
        row.altitude = 31_000.0 + stepf;
        match self {
            MutationProfile::OneColumn => {}
            MutationProfile::TwoColumns => {
                row.groundspeed = 430.0 + (step % 80) as f64;
            }
            MutationProfile::FourColumns => {
                row.groundspeed = 430.0 + (step % 80) as f64;
                row.lat = 37.618_805_6 + (stepf * 1e-6);
                row.lon = -122.375_416_7 - (stepf * 1e-6);
                row.heading = (90.0 + stepf) % 360.0;
            }
        }
    }
}

impl FlightStateBench {
    fn seed() -> Self {
        Self {
            flight_id: 9_876_543,
            altitude: 31_000.0,
            lat: 37.618_805_6,
            lon: -122.375_416_7,
            heading: 270.0,
            groundspeed: 451.0,
            aux_a: [0.0; 24],
            aux_b: [0.0; 24],
            aux_c: [0.0; 24],
            aux_d: [0.0; 24],
        }
    }
}

fn run_full_serialization(updates: usize, profile: MutationProfile) -> (usize, Duration) {
    let mut row = FlightStateBench::seed();
    let mut total_bytes = 0_usize;
    let start = Instant::now();

    for step in 0..updates {
        profile.apply(&mut row, step);
        let payload = bincode::serialize(&row).expect("full-row serialize failed");
        total_bytes += payload.len();
    }

    (total_bytes, start.elapsed())
}

fn run_delta_serialization(updates: usize, profile: MutationProfile) -> (usize, Duration) {
    let mut base = FlightStateBench::seed();
    let mut total_bytes = 0_usize;
    let start = Instant::now();

    for step in 0..updates {
        let mut updated = base;
        profile.apply(&mut updated, step);

        let record = build_update_record(base.flight_id.to_string(), &base, &updated, 0)
            .expect("build delta record failed");
        let payload = serialize_delta_wal_record(&record).expect("serialize delta record failed");
        total_bytes += payload.len();
        base = updated;
    }

    (total_bytes, start.elapsed())
}

fn check_profile_assertions(profile: MutationProfile) {
    let (full_bytes, full_elapsed) = run_full_serialization(UPDATES, profile);
    let (delta_bytes, delta_elapsed) = run_delta_serialization(UPDATES, profile);

    let reduction = 1.0 - (delta_bytes as f64 / full_bytes.max(1) as f64);
    let full_throughput = full_bytes as f64 / full_elapsed.as_secs_f64().max(f64::EPSILON);
    let delta_throughput = delta_bytes as f64 / delta_elapsed.as_secs_f64().max(f64::EPSILON);
    let required = profile.required_reduction();

    println!(
        "wal_delta_throughput_{}: updates={} full_bytes={} delta_bytes={} reduction={:.2}% full_bps={:.2} delta_bps={:.2}",
        profile.label(),
        UPDATES,
        full_bytes,
        delta_bytes,
        reduction * 100.0,
        full_throughput,
        delta_throughput
    );

    assert!(
        reduction >= required,
        "expected {} delta WAL byte reduction >= {:.0}% (observed {:.2}%)",
        profile.label(),
        required * 100.0,
        reduction * 100.0
    );
}

fn bench_wal_delta_throughput(c: &mut Criterion) {
    let profiles = [
        MutationProfile::OneColumn,
        MutationProfile::TwoColumns,
        MutationProfile::FourColumns,
    ];

    for profile in profiles {
        check_profile_assertions(profile);
    }

    let mut group = c.benchmark_group("wal_delta_throughput");
    group.sample_size(10);
    for profile in profiles {
        let full_label = format!("full_row_100k_{}", profile.label());
        let delta_label = format!("delta_row_100k_{}", profile.label());
        group.bench_function(full_label, |b| {
            b.iter(|| black_box(run_full_serialization(UPDATES, profile)))
        });
        group.bench_function(delta_label, |b| {
            b.iter(|| black_box(run_delta_serialization(UPDATES, profile)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_wal_delta_throughput);
criterion_main!(benches);
