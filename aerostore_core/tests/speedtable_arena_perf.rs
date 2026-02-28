use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{ChunkedArena, Table};
use aerostore_macros::speedtable;
use crossbeam::epoch;

#[speedtable]
#[repr(C)]
struct FlightState {
    lat: f64,
    lon: f64,
    alt: i32,
    gs: u16,
}

#[test]
fn speedtable_macro_adds_hidden_system_columns() {
    let mut row = FlightState::new(37.618_805_6, -122.375_416_7, 30_000, 450);

    assert_eq!(row.__null_bitmask(), 0);
    assert_eq!(row._xmin, 0);
    assert_eq!(row._xmax.load(Ordering::Relaxed), 0);

    row.__set_null_by_index(FlightState::__NULLBIT_ALT, true);
    assert!(row.__is_null_by_index(FlightState::__NULLBIT_ALT));

    row.__set_null_by_index(FlightState::__NULLBIT_ALT, false);
    assert!(!row.__is_null_by_index(FlightState::__NULLBIT_ALT));

    assert_eq!(
        FlightState::__null_index_for_field("lat"),
        Some(FlightState::__NULLBIT_LAT)
    );
    assert_eq!(
        FlightState::__null_index_for_field("lon"),
        Some(FlightState::__NULLBIT_LON)
    );
    assert_eq!(
        FlightState::__null_index_for_field("alt"),
        Some(FlightState::__NULLBIT_ALT)
    );
    assert_eq!(
        FlightState::__null_index_for_field("gs"),
        Some(FlightState::__NULLBIT_GS)
    );
    assert_eq!(FlightState::__null_index_for_field("missing"), None);
    assert!(!row.__is_null_field("missing"));
}

#[test]
fn speedtable_null_bitmask_masks_garbage_column_bytes_without_option_wrappers() {
    let mut row = FlightState::new(37.618_805_6, -122.375_416_7, 30_000, 450);
    row.alt = i32::MIN;
    row.__set_null_by_index(FlightState::__NULLBIT_ALT, true);

    assert!(row.__is_null_field("alt"));
    assert_eq!(
        row.alt,
        i32::MIN,
        "physical payload remains garbage while logical null bit is authoritative"
    );

    row.__set_null_by_index(FlightState::__NULLBIT_ALT, false);
    assert!(!row.__is_null_field("alt"));
}

#[test]
fn table_maintains_version_chain_per_primary_key() {
    let table: Table<u64, FlightState> = Table::new(1024, 8192);

    let v1 = table.upsert(1001, FlightState::new(37.0, -122.0, 10_000, 260), 1);
    let v2 = table.upsert(1001, FlightState::new(37.1, -122.1, 11_000, 265), 2);

    let guard = &epoch::pin();
    let head = table.head(&1001, guard).expect("head must exist");
    let head_ref = unsafe { head.as_ref().expect("head unexpectedly null") };

    assert_eq!(head_ref.xmin, 2);
    assert_eq!(head_ref.value.alt, 11_000);

    let next = head_ref.next.load(Ordering::Acquire, guard);
    let next_ref = unsafe { next.as_ref().expect("next unexpectedly null") };

    assert_eq!(next_ref.xmin, 1);
    assert_eq!(next_ref.value.alt, 10_000);

    let raw_head = head.as_raw() as *mut _;
    assert_eq!(raw_head, v2);
    assert_ne!(v1, std::ptr::null_mut());
}

#[test]
fn allocates_one_million_rows_lock_free_under_50ms() {
    const ROWS: usize = 1_000_000;

    let arena = Arc::new(ChunkedArena::<FlightState>::new(131_072));
    let workers = std::env::var("AEROSTORE_BENCH_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(10)
        });

    let per_worker = ROWS / workers;
    let remainder = ROWS % workers;

    let start = Instant::now();
    std::thread::scope(|scope| {
        for worker in 0..workers {
            let arena = Arc::clone(&arena);
            let count = per_worker + usize::from(worker == 0) * remainder;

            scope.spawn(move || {
                for i in 0..count {
                    let row = FlightState::new(
                        37.618_805_6,
                        -122.375_416_7,
                        20_000 + i as i32,
                        300_u16.wrapping_add(i as u16),
                    );

                    let ptr = arena.alloc(row);
                    assert!(!ptr.is_null());
                }
            });
        }
    });
    let elapsed = start.elapsed();
    eprintln!("alloc benchmark elapsed = {:?}", elapsed);

    assert_eq!(arena.len(), ROWS);

    if cfg!(debug_assertions) {
        eprintln!(
            "debug profile elapsed = {:?}; strict <50ms assert is only enforced in release",
            elapsed
        );
    } else {
        assert!(
            elapsed <= Duration::from_millis(50),
            "expected < 50ms, got {:?}",
            elapsed
        );
    }
}
