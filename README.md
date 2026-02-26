# Aerostore

A lock-free, memory-resident Rust database prototype built for high-concurrency market/flight workloads.

Aerostore combines:

- MVCC snapshot isolation inspired by PostgreSQL visibility semantics
- Skip-list secondary indexes for fast range/equality lookups
- WAL + group commit + checkpoint + replay recovery
- Zero-copy-ish TSV ingestion path for bulk upserts
- Broadcast-style change streaming and TTL pruning
- A Tcl `cdylib` bridge so Tcl interpreters can query the same in-process Rust state

## Workspace Layout

- `aerostore_core/`: core database engine
- `aerostore_macros/`: `#[speedtable]` proc macro
- `aerostore_tcl/`: Tcl extension (`cdylib`) exposing database APIs

## Core Concepts

### 1) Lock-Free Arena + Version Chains

- `ChunkedArena<T>` provides chunked allocation with atomic cursors.
- `Table<K, V>` maintains per-key version chains (`VersionNode`) without global mutexes.
- New versions are linked by CAS; prior versions are kept for readers until safe reclamation.

Relevant files:

- `aerostore_core/src/arena.rs`
- `aerostore_core/tests/speedtable_arena_perf.rs`

### 2) Transaction Manager + Snapshot Isolation

- `TransactionManager` dispenses monotonic `TxId` values from a global atomic counter.
- Active transactions are tracked in a lock-free linked structure.
- Each transaction gets a `Snapshot { xmin, xmax, active }`.

Relevant files:

- `aerostore_core/src/txn.rs`
- `aerostore_core/src/mvcc.rs`

### 3) MVCC Visibility + Non-Blocking Readers

- Row versions store `_xmin` and atomic `_xmax`.
- Updates create a new row version and CAS-mark old row `_xmax`.
- Reads evaluate visibility with `is_visible(...)`, so readers can proceed without blocking writers.
- Garbage collection uses `crossbeam-epoch` deferred destruction after versions become safely obsolete.

Relevant files:

- `aerostore_core/src/mvcc.rs`
- `aerostore_core/tests/mvcc_tokio_concurrency.rs`

### 4) Skip-List Secondary Indexes + Typed Query Builder

- Secondary indexes use `crossbeam_skiplist::SkipMap<IndexValue, SkipSet<RowId>>`.
- `QueryBuilder` supports:
  - `eq`
  - `gt`
  - `lt`
  - `in_values`
  - `sort_by`
  - `limit`
  - `offset`
- Optimizer behavior:
  - uses index candidates when a filter maps to an indexed field
  - otherwise parallel sequential scan
- Both paths apply MVCC visibility checks before yielding rows.

Relevant files:

- `aerostore_core/src/index.rs`
- `aerostore_core/src/query.rs`
- `aerostore_core/tests/query_index_benchmark.rs`

### 5) Durability: WAL, Group Commit, Checkpoint, Recovery

- `DurableDatabase` wraps query/MVCC with WAL-backed commit.
- Commit path:
  1. Serialize transaction write-set (`bincode`)
  2. Append framed WAL record
  3. Group flush with background WAL writer (`sync_data`)
  4. Acknowledge commit only after flush
  5. Apply MVCC commit + publish change events
- Checkpointer periodically snapshots visible rows into `checkpoint.dat` and truncates WAL.
- Startup recovery:
  1. Load checkpoint
  2. Replay remaining WAL records in txid order
  3. Rebuild indexes/MVCC state

Relevant files:

- `aerostore_core/src/wal.rs`
- `aerostore_core/tests/wal_crash_recovery.rs`

### 6) Streaming + TTL + High-Speed TSV Ingestion

- `bulk_upsert_tsv(...)` parses TSV line/column slices with `memchr`.
- Rows are inserted/updated in batches within durable transactions.
- `TableWatch` provides broadcast subscriptions for row changes.
- TTL sweeper periodically prunes rows based on `updated_at` extractor.

Relevant files:

- `aerostore_core/src/ingest.rs`
- `aerostore_core/src/watch.rs`
- `aerostore_core/tests/ingest_watch_ttl.rs`

### 7) Tcl Extension Bridge

- `aerostore_tcl` builds as a `cdylib`.
- Exposes:
  - `Aerostore_Init` / `Aerostore_SafeInit`
  - `aerostore::init ?data_dir?`
  - `FlightState ingest_tsv <tsv_data> ?batch_size?`
  - `FlightState search -compare ... -sort ... -limit ... -offset ...`
- Uses process-global `OnceLock` database/runtime so multiple Tcl interpreters in one process share the same DB.
- Parses integer/double Tcl arguments through Tcl numeric APIs (`Tcl_GetWideIntFromObj`, `Tcl_GetDoubleFromObj`) to avoid string conversion on hot paths.

Relevant files:

- `aerostore_tcl/src/lib.rs`
- `aerostore_tcl/test.tcl`

## Build Prerequisites

### Rust

- Rust stable toolchain (`cargo`, `rustc`)

### Tcl bridge (`aerostore_tcl`)

- Tcl 8.6 development/runtime libraries
- `clang` + `libclang` (used by `clib`/bindgen during build)

On Debian/Ubuntu, this is typically:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

```bash
cargo build
```

Build just the Tcl extension:

```bash
cargo build -p aerostore_tcl
```

## Tests

Run all tests:

```bash
cargo test
```

Run in release mode (recommended for performance-sensitive tests):

```bash
cargo test --release
```

Run specific key tests:

```bash
cargo test --release --test mvcc_tokio_concurrency -- --nocapture
cargo test --release --test wal_crash_recovery -- --nocapture
cargo test --release --test ingest_watch_ttl -- --nocapture
```

## Benchmarks (test-based)

These are implemented as Rust test targets that print timing info and assert behavior.

```bash
cargo test --release --test speedtable_arena_perf -- --nocapture
cargo test --release --test query_index_benchmark -- --nocapture
```

Optional worker control for arena benchmark:

```bash
AEROSTORE_BENCH_WORKERS=8 cargo test --release --test speedtable_arena_perf -- --nocapture
```

## Quick Rust Usage Example

```rust
use std::sync::Arc;
use crossbeam::epoch;
use aerostore_core::{Field, MvccTable, QueryEngine, SortDirection, TransactionManager};

#[derive(Clone)]
struct Flight {
    altitude: i32,
    gs: u16,
}

fn altitude(row: &Flight) -> i32 { row.altitude }
fn altitude_field() -> Field<Flight, i32> { Field::new("altitude", altitude) }

let txm = Arc::new(TransactionManager::new());
let table = Arc::new(MvccTable::<u64, Flight>::new(2048));
let mut engine = QueryEngine::new(table);
engine.create_index("altitude", altitude);

let tx = txm.begin();
engine.insert(1, Flight { altitude: 12000, gs: 430 }, &tx).unwrap();
engine.insert(2, Flight { altitude: 9000, gs: 390 }, &tx).unwrap();
engine.commit(&txm, &tx);

let read_tx = txm.begin();
let guard = epoch::pin();
let rows = engine
    .query()
    .gt(altitude_field(), 10_000)
    .sort_by(altitude_field(), SortDirection::Asc)
    .limit(10)
    .execute(&read_tx, &guard);
assert_eq!(rows.len(), 1);
```

## Tcl Usage

Build the extension:

```bash
cargo build -p aerostore_tcl
```

Run the bundled demo:

```bash
tclsh aerostore_tcl/test.tcl
```

The demo script will:

1. Try `package require aerostore`
2. Fallback to loading `target/debug/libaerostore_tcl.so`
3. Initialize the DB
4. Ingest sample TSV rows
5. Run a filtered/sorted/limited `FlightState search`

You can also load directly:

```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore
puts [aerostore::init ./tmp/aerostore_tcl_demo]
set count [FlightState search -compare {{> altitude 10000}} -sort lat -limit 50]
puts $count
```

## Data Files

For `DurableDatabase` instances:

- `wal.log`: append-only framed WAL records
- `checkpoint.dat`: periodic compact snapshot

During normal operation, recovery reads checkpoint first, then replays remaining WAL.

## Current Status / Caveats

- Prototype quality with heavy concurrency focus; API may evolve.
- Some benchmarks are test-target benchmarks (not criterion harnesses).
- Tcl package loading may require explicit package name in `load ... Aerostore` depending on filename conventions.
- Durability currently uses `bincode` serialization for WAL/checkpoints.

## License

No explicit license file is currently present in this repository.
