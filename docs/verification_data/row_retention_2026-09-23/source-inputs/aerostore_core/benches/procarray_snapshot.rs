use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Instant;

use aerostore_core::ShmArena;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_procarray_snapshot(c: &mut Criterion) {
    let ns_txid_10 = snapshot_avg_ns(10);
    let ns_txid_10m = snapshot_avg_ns(10_000_000);
    let delta = (ns_txid_10 - ns_txid_10m).abs();

    println!(
        "procarray_snapshot_proof txid_10={:.2}ns txid_10_000_000={:.2}ns delta={:.2}ns",
        ns_txid_10, ns_txid_10m, delta
    );

    assert!(
        ns_txid_10 < 50.0 && ns_txid_10m < 50.0,
        "snapshot budget exceeded: txid_10={:.2}ns txid_10_000_000={:.2}ns (target < 50ns)",
        ns_txid_10,
        ns_txid_10m
    );
    assert!(
        delta <= 5.0,
        "snapshot time should be constant regardless of global txid (delta={:.2}ns)",
        delta
    );

    let mut group = c.benchmark_group("procarray_snapshot");
    group.sample_size(120);
    group.measurement_time(std::time::Duration::from_secs(3));

    for &start_txid in &[10_u64, 10_000_000_u64] {
        let shm = ShmArena::new(8 << 20).expect("failed to allocate benchmark shared arena");
        shm.global_txid().store(start_txid, Ordering::Release);
        group.bench_with_input(
            BenchmarkId::from_parameter(start_txid),
            &shm,
            |b, local_shm| {
                b.iter(|| {
                    let snapshot = local_shm.create_snapshot();
                    black_box(snapshot.xmax);
                });
            },
        );
    }

    group.finish();
}

fn snapshot_avg_ns(start_txid: u64) -> f64 {
    const WARMUP: usize = 100_000;
    const ITERATIONS: usize = 2_000_000;

    let shm = ShmArena::new(8 << 20).expect("failed to allocate proof shared arena");
    shm.global_txid().store(start_txid, Ordering::Release);

    for _ in 0..WARMUP {
        let snapshot = shm.create_snapshot();
        black_box(snapshot.xmax);
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let snapshot = shm.create_snapshot();
        black_box(snapshot.xmax);
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / ITERATIONS as f64
}

criterion_group!(benches, bench_procarray_snapshot);
criterion_main!(benches);
