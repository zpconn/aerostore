#![cfg(unix)]

use std::cmp::Ordering;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::shm_skiplist::{ScanBound, ShmSkipKey, ShmSkipList};
use aerostore_core::ShmArena;
use criterion::{criterion_group, criterion_main, Criterion};

const TOTAL_KEYS: i64 = 1_000_000;
const LOWER_BOUND: i64 = 999_990;
const EXPECTED_TAIL_HITS: usize = 9;
const TAIL_QUERY_BUDGET_NS: f64 = 5_000.0;
const BENCH_ARENA_BYTES: usize = 1 << 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BenchKey(i64);

impl ShmSkipKey for BenchKey {
    fn sentinel() -> Self {
        BenchKey(i64::MIN)
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

fn build_fixture() -> ShmSkipList<BenchKey> {
    let shm = Arc::new(ShmArena::new(BENCH_ARENA_BYTES).expect("failed to allocate benchmark shm"));
    let list = ShmSkipList::<BenchKey>::new_in_shared(shm).expect("failed to build skiplist");

    for key in 0..TOTAL_KEYS {
        let payload = (key as u32).to_le_bytes();
        list.insert_payload(BenchKey(key), payload.len() as u16, &payload)
            .expect("failed to insert sequential benchmark key");
    }

    list
}

#[inline]
fn tail_query_gt(list: &ShmSkipList<BenchKey>, lower: &BenchKey) -> usize {
    let mut hits = 0_usize;
    list.scan_payloads_bounded(Some((lower, ScanBound::Exclusive)), None, |_, _, _| {
        hits = hits.wrapping_add(1);
    })
    .expect("tail range query failed");
    hits
}

fn bench_shm_skiplist_seek_bounds(c: &mut Criterion) {
    let list = build_fixture();
    let lower = BenchKey(LOWER_BOUND);

    let warmup_hits = tail_query_gt(&list, &lower);
    assert_eq!(
        warmup_hits, EXPECTED_TAIL_HITS,
        "tail query cardinality mismatch for > {}",
        LOWER_BOUND
    );

    let mut total_iters = 0_u128;
    let mut total_ns = 0_u128;

    let mut group = c.benchmark_group("shm_skiplist_seek_bounds");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("gt_999_990", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut checksum = 0_usize;
            for _ in 0..iters {
                checksum = checksum.wrapping_add(tail_query_gt(&list, &lower));
            }
            black_box(checksum);
            let elapsed = start.elapsed();
            total_iters = total_iters.saturating_add(iters as u128);
            total_ns = total_ns.saturating_add(elapsed.as_nanos());
            elapsed
        })
    });
    group.finish();

    assert!(total_iters > 0, "criterion produced zero iterations");
    let avg_ns = total_ns as f64 / total_iters as f64;
    println!(
        "shm_skiplist_seek_bounds: keys={} query=>{} avg_ns={:.2} budget_ns={:.2}",
        TOTAL_KEYS, LOWER_BOUND, avg_ns, TAIL_QUERY_BUDGET_NS
    );
    assert!(
        avg_ns < TAIL_QUERY_BUDGET_NS,
        "tail query exceeded budget: avg_ns={:.2} budget_ns={:.2}",
        avg_ns,
        TAIL_QUERY_BUDGET_NS
    );
}

criterion_group!(benches, bench_shm_skiplist_seek_bounds);
criterion_main!(benches);
