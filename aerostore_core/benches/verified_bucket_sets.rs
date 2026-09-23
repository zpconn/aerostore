//! Controlled microbenchmark of the exact production bucket-set kernels.
//! This is not an end-to-end transaction benchmark or an automatic promotion.
use std::hint::black_box;
use std::time::Instant;

use aerostore_verified::{canonical_buckets_bitmap, canonical_buckets_sort};
use serde_json::json;

const BUCKETS: usize = 4096;
type Kernel = fn(&[usize], usize) -> Result<Vec<usize>, usize>;

fn standard(input: &[usize], buckets: usize) -> Result<Vec<usize>, usize> {
    for &id in input {
        if id >= buckets {
            return Err(id);
        }
    }
    let mut output = input.to_vec();
    output.sort_unstable();
    output.dedup();
    Ok(output)
}

fn sample(kernel: Kernel, input: &[usize], iterations: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(kernel(black_box(input), black_box(BUCKETS))).unwrap();
    }
    start.elapsed().as_nanos() as f64 / iterations as f64
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let output_path = args.windows(2).find(|a| a[0] == "--output").map(|a| &a[1]);
    let kernels: [(&str, Kernel); 3] = [
        ("standard", standard),
        ("verified_sort", canonical_buckets_sort),
        ("verified_bitmap", canonical_buckets_bitmap),
    ];
    let mut results = Vec::new();
    for (case, length, key_domain) in [
        ("empty", 0, 1),
        ("equality", 1, BUCKETS),
        ("short_in", 8, BUCKETS),
        ("medium_in", 64, BUCKETS),
        ("duplicate_in", 1024, 32),
        ("long_in", 1024, BUCKETS),
        ("all_buckets", BUCKETS, BUCKETS),
    ] {
        let mut seed = 0x6a09_e667_f3bc_c909u64;
        let input: Vec<_> = (0..length)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if case == "all_buckets" {
                    i
                } else {
                    (seed >> 32) as usize % key_domain
                }
            })
            .collect();
        let expected = standard(&input, BUCKETS).unwrap();
        for (_, kernel) in kernels {
            assert_eq!(kernel(&input, BUCKETS).unwrap(), expected);
        }
        // Calibrate each algorithm independently, then hold its iteration count
        // fixed. Rotate order each round to reduce drift and warming bias.
        let counts: Vec<_> = kernels
            .iter()
            .map(|(_, kernel)| {
                let estimate = sample(*kernel, &input, 64).max(1.0);
                (5_000_000.0 / estimate).clamp(8.0, 100_000.0) as usize
            })
            .collect();
        let mut samples = [Vec::new(), Vec::new(), Vec::new()];
        for round in 0..9 {
            for step in 0..kernels.len() {
                let index = (round + step) % kernels.len();
                samples[index].push(sample(kernels[index].1, &input, counts[index]));
            }
        }
        for (index, (algorithm, _)) in kernels.iter().enumerate() {
            let mut ordered = samples[index].clone();
            ordered.sort_by(f64::total_cmp);
            results.push(json!({
                "case": case, "input_length": length, "unique_buckets": expected.len(),
                "algorithm": algorithm, "iterations_per_sample": counts[index],
                "median_ns_per_call": ordered[ordered.len()/2],
                "samples_ns_per_call": samples[index],
            }));
        }
    }
    let report = json!({
        "format_version": 1, "bucket_count": BUCKETS,
        "scope": "Microbenchmark of exact safe functions; excludes hashing, locking, transactions, and durability. Does not promote a candidate.",
        "passed_output_comparisons": true, "results": results,
    });
    let text = serde_json::to_string_pretty(&report).unwrap();
    if let Some(path) = output_path {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).unwrap();
            }
        }
        std::fs::write(&path, text + "\n").unwrap();
        println!("Bucket microbenchmark report: {}", path.display());
    } else {
        println!("{text}");
    }
}
