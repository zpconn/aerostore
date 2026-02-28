# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, shared-memory database prototype for high-ingest flight/state workloads.
It is built around lock-free shared-memory data structures, serializable OCC/SSI transaction rules,
STAPI-style query planning, and WAL/checkpoint durability paths.

## What Is New In V4

- Shared-memory secondary indexing is backed by a lock-free `ShmSkipList`.
- Dynamic cardinality routing is enabled for equality predicates.
- Both the shared primary-key map and shared skiplist track `distinct_key_count`.
- STAPI parser and filter engine support full operator set used in this repo, including:
  - `null`, `notnull`
  - `<=`, `>=`, `!=`
  - `notmatch`
- Speedtable null semantics are driven by hidden `_null_bitmask` system column injection,
  without `Option<T>` layout inflation.

## Repository Layout

- `aerostore_core`: engine, parser/planner/executor, shared-memory primitives, durability, tests, benches.
- `aerostore_tcl`: Tcl extension bridge (`cdylib`) and integration tests.
- `aerostore_macros`: `#[speedtable]` proc macro crate.
- `docs/nightly_perf.md`: nightly/isolated performance runbook.

## Architecture Overview

### Shared Memory Foundation

- POSIX shared mapping (`MAP_SHARED`) via `ShmArena`.
- `RelPtr<T>` relative pointers instead of process-local absolute pointers.
- Lock-free shared bump allocation (`ChunkedArena`) using CAS.

### Transaction Model

- OCC with snapshot-based reads and serializable conflict checks.
- Partitioned lock striping in commit path to reduce hotspot contention.
- Savepoint and rollback-to-savepoint support.

### Indexing

- `ShmPrimaryKeyMap` in shared memory for fast PK lookup.
- `SecondaryIndex` backed by `ShmSkipList` in shared memory.
- Skiplist is lock-free and CAS-linked across levels.
- Non-unique index semantics are supported through posting chains (duplicate keys map to multiple row IDs).
- `distinct_key_count` is exposed on both PK and secondary index structures.

### STAPI Parser and Filters

- Full operator set:
  - `=` / `==`, `!=` / `<>`
  - `>`, `>=`, `<`, `<=`
  - `in`, `match`, `notmatch`
  - `null`, `notnull`
- Sort and limit support:
  - `-sort <field>`
  - `-limit <n>`
- Tcl bridge controls:
  - `-offset <n>`, `-asc`, `-desc`

### Nullability Without `Option<T>`

`#[speedtable]` injects hidden system fields into row layout:

- `_null_bitmask: u64`
- `_xmin: u64`
- `_xmax: AtomicU64`

A set bit in `_null_bitmask` means the logical value is SQL `NULL`, regardless of the bytes in the physical field slot.

### Planner and Execution

Planner logic (`rbo_planner.rs`) does the following:

1. Build equality-route candidates from:
   - PK equality predicate (if PK map configured)
   - Indexed equality predicates
2. Choose equality driver by highest `distinct_key_count`.
3. Tie-break equality candidates by:
   - PK over secondary index
   - then earlier filter index in query
4. If no equality candidate exists, choose best indexed range route by configured range rank.
5. Otherwise fall back to full scan.

All non-driver predicates are compiled as residual filters and applied after candidate row ID narrowing.

### Durability and Recovery

- Shared ring WAL path in `wal_ring.rs` with synchronous or asynchronous commit behavior.
- WAL writer daemon path in `wal_writer.rs`.
- Logical WAL + snapshot recovery in `wal_logical.rs` and `recovery.rs`.
- Legacy durability path in `wal.rs` remains for compatibility/reference.

## Query Example

```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore
set _ [aerostore::init ./aerostore_tcl_data]

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1

set n [FlightState search -compare {{= flight_id UAL123} {>= altitude 10000} {notmatch typ C17*}} -sort altitude -limit 50]
puts $n
```

## Tcl Bridge Commands

- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState search ...`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`

Config keys:

- `aerostore.synchronous_commit` (`on` or `off`)
- `aerostore.checkpoint_interval_secs`

## Build Requirements

Rust:

- stable toolchain (`cargo`, `rustc`)

For Tcl bridge builds/tests:

- Tcl 8.6 runtime + dev headers
- clang + libclang

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

Workspace:

```bash
cargo build --workspace
```

Core only:

```bash
cargo build -p aerostore_core
```

Tcl bridge only:

```bash
cargo build -p aerostore_tcl
```

## Test Commands

### Fast compile sanity

```bash
cargo test --workspace --no-run
cargo test -p aerostore_core --tests --no-run
cargo test -p aerostore_tcl --tests --no-run
cargo test -p aerostore_macros --test ui
```

### Cardinality and index routing gates

```bash
cargo test -p aerostore_core --test planner_cardinality -- --nocapture
cargo test -p aerostore_core --test rbo_planner_routing -- --nocapture
cargo test -p aerostore_core --test shm_shared_memory forked_primary_key_map_updates_are_cross_process_visible -- --nocapture
cargo test -p aerostore_core --test shm_index_contention -- --nocapture
cargo test -p aerostore_core distinct_key_count_ -- --nocapture
```

### Parser/filter/bridge reliability gates

```bash
cargo test -p aerostore_core stapi_parser -- --nocapture
cargo test -p aerostore_core filters:: -- --nocapture
cargo test -p aerostore_core --test speedtable_arena_perf -- --nocapture
cargo test -p aerostore_tcl --test search_stapi_bridge -- --nocapture
cargo test -p aerostore_tcl --test config_checkpoint_integration -- --nocapture
```

### Recovery and concurrency suites

```bash
cargo test -p aerostore_core --test occ_write_skew -- --nocapture
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1
```

## Benchmark Commands

### Core benchmark suites

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
```

### Ignored heavy contention/model suites

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### Criterion benches

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
```

Nightly/isolated runbook:

- `docs/nightly_perf.md`

## Reliability Coverage Highlights

- `aerostore_core/tests/planner_cardinality.rs`
  - dynamic cardinality route selection, PK-vs-secondary tie behavior, residual demotion checks.
- `aerostore_core/tests/rbo_planner_routing.rs`
  - route choice, residual negative/null handling, malformed query safety.
- `aerostore_core/tests/shm_index_contention.rs`
  - cross-process contention and distinct-count/index-consistency stress.
- `aerostore_core/tests/shm_shared_memory.rs`
  - fork visibility and shared PK map behavior under contention.
- `aerostore_core/src/shm_skiplist.rs` unit tests
  - ordering invariants, mark/unlink behavior, GC horizon, distinct-key churn checks.
- `aerostore_core/tests/query_index_benchmark.rs`
  - STAPI planner/execute benchmarks, cardinality trap benchmark, residual filter benchmarks.
- `aerostore_tcl/tests/search_stapi_bridge.rs`
  - list/raw STAPI bridge parity and operator boundary checks.

## Current Status

- Shared-memory skiplist index backend is active and cross-process validated.
- Dynamic equality route selection uses live distinct-cardinality counters.
- STAPI negative and null operators are implemented and exercised by parser/filter/bridge tests.
- Speedtable null semantics are bitmask-driven and ABI/layout friendly.
- Async and sync durability paths are implemented and tested.
- Legacy modules remain in tree for compatibility/reference and comparative testing.

## License

No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
