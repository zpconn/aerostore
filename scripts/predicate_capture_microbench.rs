//! Standalone native lookup harness: link these identical bytes against each
//! source-bound default-feature library. This is not a HyperFeed capacity test.
use aerostore_core::{
    IndexCompare, IndexPublicationPolicy, IndexValue, OccTable, SecondaryIndex, ShmArena,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const SCENARIOS: [&str; 7] = [
    "empty_fresh_broad",
    "full_fresh_broad",
    "empty_repeated_broad",
    "full_repeated_broad",
    "equality_heavy",
    "equality_then_broad",
    "overlapping_ranges",
];

#[derive(Clone, Copy)]
struct Row {
    key: i64,
    active: bool,
}

fn key(row: &Row) -> Option<IndexValue> {
    row.active.then_some(IndexValue::I64(row.key))
}

fn queries(scenario: &str) -> Vec<IndexCompare> {
    let broad = || IndexCompare::Gte(IndexValue::I64(i64::MIN));
    match scenario {
        "empty_fresh_broad" | "full_fresh_broad" => vec![broad()],
        "empty_repeated_broad" | "full_repeated_broad" => vec![broad(), broad()],
        "equality_heavy" => (0..64)
            .map(|n| IndexCompare::Eq(IndexValue::I64((n % 16) * 10)))
            .collect(),
        "equality_then_broad" => vec![IndexCompare::Eq(IndexValue::I64(30)), broad()],
        "overlapping_ranges" => vec![
            IndexCompare::Lte(IndexValue::I64(630)),
            IndexCompare::Gte(IndexValue::I64(320)),
        ],
        _ => unreachable!(),
    }
}

fn expected_rows(query: &IndexCompare, population: usize) -> Vec<usize> {
    (0..population)
        .filter(|id| {
            let key = (*id as i64) * 10;
            match query {
                IndexCompare::Eq(IndexValue::I64(bound)) => key == *bound,
                IndexCompare::Gte(IndexValue::I64(bound)) => key >= *bound,
                IndexCompare::Lte(IndexValue::I64(bound)) => key <= *bound,
                _ => unreachable!(),
            }
        })
        .collect()
}

fn run(scenario: &str, policy_name: &str, block: usize, iterations: usize, warmups: usize) {
    let policy = match policy_name {
        "hashed" => IndexPublicationPolicy::Hashed,
        "ordered" => IndexPublicationPolicy::OrderedI64 {
            origin: 0,
            width: 10,
        },
        _ => unreachable!(),
    };
    let population = if scenario.starts_with("empty_") {
        0
    } else {
        128
    };
    let arena = Arc::new(ShmArena::new(32 << 20).expect("create private arena"));
    let mut table = OccTable::new(arena.clone(), 128).expect("create table");
    let index = SecondaryIndex::new_in_shared_with_publication_policy("key", arena.clone(), policy)
        .expect("create index");
    for id in 0..128 {
        let row = Row {
            key: (id as i64) * 10,
            active: id < population,
        };
        table.seed_row(id, row).expect("seed row");
        if let Some(key) = key(&row) {
            index.try_insert(key, id).expect("seed index");
        }
    }
    table.bind_index(index.clone(), key).expect("bind index");
    let queries = queries(scenario);
    let expected: Vec<_> = queries
        .iter()
        .map(|query| expected_rows(query, population))
        .collect();
    for sample in 0..warmups + iterations {
        // Measurement storage and fixture setup are outside the transaction
        // timer. Begin, result assertions and commit are included in that timer;
        // each lookup timer includes only the actual native lookup call.
        let mut lookup_ns = Vec::with_capacity(queries.len());
        let started = Instant::now();
        let mut tx = table.begin_transaction().expect("begin");
        for (query, expected) in queries.iter().zip(&expected) {
            let lookup_start = Instant::now();
            let rows = table
                .index_lookup(black_box(&mut tx), black_box(&index), black_box(query))
                .expect("uncontended lookup");
            let elapsed = lookup_start.elapsed().as_nanos();
            assert_eq!(
                &rows, expected,
                "native lookup must return the complete expected result"
            );
            lookup_ns.push(elapsed);
            black_box(rows);
        }
        let commit_start = Instant::now();
        assert_eq!(table.commit(&mut tx).expect("commit"), 0);
        let commit_ns = commit_start.elapsed().as_nanos();
        let transaction_ns = started.elapsed().as_nanos();
        let total_lookup_ns: u128 = lookup_ns.iter().sum();
        let returned_rows: Vec<_> = expected.iter().map(Vec::len).collect();
        println!("{{\"kind\":\"sample\",\"scenario\":\"{scenario}\",\"policy\":\"{policy_name}\",\"block\":{block},\"sample\":{sample},\"warmup\":{},\"lookup_ns\":{lookup_ns:?},\"total_lookup_ns\":{total_lookup_ns},\"commit_ns\":{commit_ns},\"transaction_ns\":{transaction_ns},\"returned_rows\":{returned_rows:?}}}", sample < warmups);
    }
    assert!(
        arena.create_snapshot().is_empty(),
        "all registrations drained"
    );
}

fn main() {
    let mut policy = String::from("hashed");
    let mut scenario = String::from("all");
    let mut block = 0;
    let mut iterations = 64;
    let mut warmups = 4;
    let mut describe = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--policy" => policy = args.next().expect("policy value"),
            "--scenario" => scenario = args.next().expect("scenario value"),
            "--block" => {
                block = args
                    .next()
                    .expect("block value")
                    .parse()
                    .expect("numeric block")
            }
            "--iterations" => {
                iterations = args
                    .next()
                    .expect("iteration value")
                    .parse()
                    .expect("numeric iterations")
            }
            "--warmups" => {
                warmups = args
                    .next()
                    .expect("warmup value")
                    .parse()
                    .expect("numeric warmups")
            }
            "--describe" => describe = true,
            _ => panic!("unsupported argument: {arg}"),
        }
    }
    assert!(matches!(policy.as_str(), "hashed" | "ordered"));
    assert!(scenario == "all" || SCENARIOS.contains(&scenario.as_str()));
    assert!((1..=10_000).contains(&iterations) && warmups <= 100 && block <= 1_000_000);
    println!("{{\"kind\":\"configuration\",\"policy\":\"{policy}\",\"scenario\":\"{scenario}\",\"block\":{block},\"iterations_per_scenario\":{iterations},\"warmups_per_scenario\":{warmups},\"scope\":\"Single-thread default-feature native lookup/transaction timing; fresh transaction each sample; fixtures remain fixed; warmups retained but excluded from comparisons. No HyperFeed throughput, concurrency or capacity claim.\",\"analytical_search_comparisons\":{{\"fresh_4096_baseline\":8386560,\"fresh_4096_candidate\":0,\"second_identical_4096_both\":8390656,\"scope\":\"Source-level find predicate comparisons for an empty prior read set and ascending unique requested buckets; not a measured CPU count or timing attribution.\"}}}}");
    if describe {
        return;
    }
    for name in SCENARIOS {
        if scenario == "all" || scenario == name {
            run(name, &policy, block, iterations, warmups);
        }
    }
}
