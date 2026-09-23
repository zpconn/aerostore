//! Untimed allocation measurements of the exact bucket kernels.
//! The separate timing benchmark never installs this instrumented allocator.
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use aerostore_verified::{canonical_buckets_bitmap, canonical_buckets_sort};
use serde_json::json;

const BUCKETS: usize = 4096;
type Kernel = fn(&[usize], usize) -> Result<Vec<usize>, usize>;

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// This executable is single threaded. Inputs, expected values, JSON and output
// are allocated while counting is disabled. Atomics make allocator callbacks
// nonallocating; they do not turn this into a concurrent attribution profiler.
static ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);
static ACCOUNTING_ERROR: AtomicBool = AtomicBool::new(false);

fn add_live(bytes: usize) {
    let previous = LIVE.fetch_add(bytes, Ordering::SeqCst);
    if let Some(current) = previous.checked_add(bytes) {
        PEAK.fetch_max(current, Ordering::SeqCst);
    } else {
        ACCOUNTING_ERROR.store(true, Ordering::SeqCst);
    }
}

fn remove_live(bytes: usize) {
    if LIVE
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_sub(bytes)
        })
        .is_err()
    {
        ACCOUNTING_ERROR.store(true, Ordering::SeqCst);
    }
}

// SAFETY: every operation forwards the unchanged pointer/layout arguments to
// System. Instrumentation touches only atomics and never allocates or unwinds.
// Allocation failures preserve the original allocation for realloc, matching
// GlobalAlloc's contract. This unsafe adapter is benchmark-only code.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let active = ACTIVE.load(Ordering::SeqCst);
        let pointer = unsafe { System.alloc(layout) };
        if active {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            REQUESTED.fetch_add(layout.size(), Ordering::SeqCst);
            if pointer.is_null() {
                FAILED.fetch_add(1, Ordering::SeqCst);
            } else {
                add_live(layout.size());
            }
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let active = ACTIVE.load(Ordering::SeqCst);
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if active {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            REQUESTED.fetch_add(layout.size(), Ordering::SeqCst);
            if pointer.is_null() {
                FAILED.fetch_add(1, Ordering::SeqCst);
            } else {
                add_live(layout.size());
            }
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let active = ACTIVE.load(Ordering::SeqCst);
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if active {
            REALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            REQUESTED.fetch_add(new_size, Ordering::SeqCst);
            if resized.is_null() {
                FAILED.fetch_add(1, Ordering::SeqCst);
            } else {
                remove_live(layout.size());
                add_live(new_size);
            }
        }
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if ACTIVE.load(Ordering::SeqCst) {
            DEALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            remove_live(layout.size());
        }
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[derive(Clone, Copy)]
struct Counts {
    allocations: usize,
    reallocations: usize,
    deallocations: usize,
    requested: usize,
    live: usize,
    peak: usize,
    failed: usize,
    accounting_error: bool,
}

fn counts() -> Counts {
    Counts {
        allocations: ALLOCATIONS.load(Ordering::SeqCst),
        reallocations: REALLOCATIONS.load(Ordering::SeqCst),
        deallocations: DEALLOCATIONS.load(Ordering::SeqCst),
        requested: REQUESTED.load(Ordering::SeqCst),
        live: LIVE.load(Ordering::SeqCst),
        peak: PEAK.load(Ordering::SeqCst),
        failed: FAILED.load(Ordering::SeqCst),
        accounting_error: ACCOUNTING_ERROR.load(Ordering::SeqCst),
    }
}

fn reset() {
    assert!(!ACTIVE.load(Ordering::SeqCst));
    for counter in [
        &ALLOCATIONS,
        &REALLOCATIONS,
        &DEALLOCATIONS,
        &REQUESTED,
        &LIVE,
        &PEAK,
        &FAILED,
    ] {
        counter.store(0, Ordering::SeqCst);
    }
    ACCOUNTING_ERROR.store(false, Ordering::SeqCst);
}

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

fn measure(kernel: Kernel, input: &[usize], expected: &[usize]) -> serde_json::Value {
    reset();
    // Exactly one kernel invocation is attributed. No assertions, formatting,
    // input construction, timing calls, or JSON allocation occur in this span.
    ACTIVE.store(true, Ordering::SeqCst);
    let result = black_box(kernel)(black_box(input), black_box(BUCKETS));
    ACTIVE.store(false, Ordering::SeqCst);
    let call = counts();

    let output = result.expect("all benchmark bucket IDs are valid");
    assert_eq!(output, expected);
    let retained_output_bytes = output.capacity() * std::mem::size_of::<usize>();
    let output_payload_bytes = output.len() * std::mem::size_of::<usize>();

    // A second, separately reported span attributes only destruction of the
    // returned vector. Kernel-call allocation counters above are immutable.
    ACTIVE.store(true, Ordering::SeqCst);
    drop(output);
    ACTIVE.store(false, Ordering::SeqCst);
    let after_drop = counts();
    let leak_check_passed = call.live == retained_output_bytes
        && after_drop.live == 0
        && after_drop.allocations == call.allocations
        && after_drop.reallocations == call.reallocations
        && after_drop.failed == 0
        && !after_drop.accounting_error;
    assert!(
        leak_check_passed,
        "allocation accounting or output-drop leak check failed"
    );

    json!({
        "allocation_calls": call.allocations,
        "reallocation_calls": call.reallocations,
        "deallocation_calls_during_call": call.deallocations,
        "total_bytes_requested": call.requested,
        "peak_live_bytes": call.peak,
        "live_bytes_at_return": call.live,
        "retained_output_bytes": retained_output_bytes,
        "output_payload_bytes": output_payload_bytes,
        "output_drop_deallocation_calls": after_drop.deallocations - call.deallocations,
        "total_deallocation_calls_including_output_drop": after_drop.deallocations,
        "live_bytes_after_output_drop": after_drop.live,
        "failed_allocation_calls": after_drop.failed,
        "leak_check_passed": leak_check_passed,
    })
}

fn output_path() -> Option<PathBuf> {
    let mut arguments = std::env::args().skip(1);
    let mut output = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            // Cargo may pass this to a harness=false benchmark executable.
            "--bench" => {}
            "--output" => {
                assert!(output.is_none(), "--output may only be supplied once");
                let path = PathBuf::from(arguments.next().expect("--output requires a path"));
                assert!(path.is_absolute(), "--output must be an absolute path");
                output = Some(path);
            }
            _ => panic!("unrecognized argument: {argument}"),
        }
    }
    output
}

fn main() {
    let output_path = output_path();
    let kernels: [(&str, Kernel); 3] = [
        ("standard", standard),
        ("verified_sort", canonical_buckets_sort),
        ("verified_bitmap", canonical_buckets_bitmap),
    ];
    let mut results = Vec::new();
    // Identical seven cases, LCG seed and generation rule to the timing
    // benchmark; retained separately to avoid instrumenting timed runs.
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
        for (algorithm, kernel) in kernels {
            let measured = measure(kernel, &input, &expected);
            results.push(json!({
                "case": case,
                "input_length": length,
                "unique_buckets": expected.len(),
                "algorithm": algorithm,
                "allocations": measured,
            }));
        }
    }
    let report = json!({
        "format_version": 1,
        "bucket_count": BUCKETS,
        "usize_bytes": std::mem::size_of::<usize>(),
        "measurement": "Untimed single-call GlobalAlloc instrumentation; a separate span measures returned Vec destruction.",
        "scope": "Exact safe bucket kernels only; excludes input/JSON allocations, hashing, locks, transactions, allocator metadata, RSS, and shared-memory reclamation. No promotion decision.",
        "byte_accounting": "Requested layout bytes; realloc contributes its full new size to total requested bytes and replaces old size in live bytes. Peak excludes allocator-internal temporary old/new overlap and retained allocator arenas.",
        "allocation_calls_definition": "alloc and alloc_zeroed calls combined; realloc calls reported separately",
        "passed_output_comparisons": true,
        "passed_no_leaks": true,
        "results": results,
    });
    let text = serde_json::to_string_pretty(&report).unwrap();
    if let Some(path) = output_path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, text + "\n").unwrap();
        println!("Bucket allocation report: {}", path.display());
    } else {
        println!("{text}");
    }
}
