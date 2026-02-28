# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype designed for high-ingest, low-latency flight/state workloads.

It combines:

- lock-free shared-memory data structures (`RelPtr`, shared arena, CAS-linked structures),
- serializable OCC/SSI-style transaction behavior,
- STAPI-style parsing, planning, and execution,
- shared-memory primary and secondary indexes,
- WAL/checkpoint durability and recovery paths,
- Tcl bridge integration (`cdylib`) for low-overhead embedding.

## Table of Contents

- [Project Goals](#project-goals)
- [Current Status](#current-status)
- [Repository Layout](#repository-layout)
- [Core Concepts](#core-concepts)
- [Data Structures and Internals](#data-structures-and-internals)
- [Query Model](#query-model)
- [STAPI Grammar Reference](#stapi-grammar-reference)
- [Planner and Cardinality Routing](#planner-and-cardinality-routing)
- [Nullability Model](#nullability-model)
- [Durability and Recovery](#durability-and-recovery)
- [Tcl Bridge](#tcl-bridge)
- [Build and Prerequisites](#build-and-prerequisites)
- [Testing and Benchmarks](#testing-and-benchmarks)
- [Nightly and Stress Runbook](#nightly-and-stress-runbook)
- [Troubleshooting](#troubleshooting)
- [Known Constraints](#known-constraints)
- [License](#license)

## Project Goals

Aerostore focuses on these practical goals:

- Fast point and selective range queries over mutable flight-state data.
- Cross-process shared-memory operation without copying index state per process.
- Deterministic planner behavior that stays explainable.
- Strong concurrency correctness via serializable validation semantics.
- Recoverability from persisted artifacts (WAL/checkpoint/snapshot paths).

Non-goals right now:

- SQL surface compatibility.
- Full distributed operation.
- Multi-node replication.
- User-facing product hardening (authentication, tenancy, RBAC, etc).

## Current Status

The current implementation includes:

- Shared-memory primary-key map (`ShmPrimaryKeyMap`) for PK equality lookups.
- Shared-memory lock-free skiplist-backed secondary indexes (`ShmSkipList` via `SecondaryIndex`).
- Dynamic equality routing based on live index cardinality (`distinct_key_count`).
- STAPI parser support for `null`, `notnull`, `<=`, `>=`, `!=`, `notmatch`, and other core operators.
- Null semantics enforced by hidden row bitmask fields, without relying on `Option<T>` field layout.
- WAL writer daemon and logical recovery paths with broad regression/benchmark coverage.

## Repository Layout

- `aerostore_core/`
  - Shared memory primitives, OCC engine, parser/planner/executor, WAL/recovery, tests, benchmarks.
- `aerostore_tcl/`
  - Tcl bridge and integration tests.
- `aerostore_macros/`
  - `#[speedtable]` proc macro crate (row layout/system fields/null bitmask helpers).
- `docs/nightly_perf.md`
  - nightly and isolated stress/perf runbook.

## Core Concepts

### Shared Memory First

Aerostore uses a shared memory mapping as the authoritative runtime state for key data structures. Processes created by `fork()` can observe and mutate the same shared index/table structures, coordinated through atomics and CAS.

### Relative Pointers (`RelPtr<T>`)

Pointers inside shared memory are stored as relative offsets from mapping base, not absolute process-local addresses. This allows multiple processes with different virtual mapping base addresses to resolve the same logical object safely.

### OCC + Serializable Validation

Transactions gather read/write intents and are validated at commit for serialization conflicts. This enables high read/write concurrency while rejecting unsafe interleavings.

### Candidate Narrowing Before Residuals

Planner-driven candidate row ID generation (PK/index/full scan) runs first. Residual predicates are evaluated only on candidate rows, minimizing expensive predicate work.

## Data Structures and Internals

### Shared Arena and Allocation

Key files:

- `aerostore_core/src/shm.rs`
- `aerostore_core/src/arena.rs`

Behavior:

- Shared chunked bump allocation with atomic head movement.
- Alignment-aware raw and typed allocation.
- Intended for stable in-shared-memory object identity via offsets.

### Primary Key Map (`ShmPrimaryKeyMap`)

Key file:

- `aerostore_core/src/execution.rs`

Highlights:

- Shared hash-bucket map with CAS insertion.
- Handles contended `get_or_insert`/`insert_existing` across processes.
- Exposes `distinct_key_count()` (live/novel key count tracked in shared header).
- Used as planner route driver for PK equality path when configured.

### Secondary Index (`SecondaryIndex<RowId>`) and Skiplist

Key files:

- `aerostore_core/src/shm_index.rs`
- `aerostore_core/src/shm_skiplist.rs`

Highlights:

- Shared-memory skiplist lanes (`MAX_HEIGHT`), lock-free CAS insertion/removal.
- Duplicate-key support through posting chains per key node.
- Range and exact lookup APIs.
- Cross-process behavior validated with `fork()` tests.
- Exposes `distinct_key_count()` for dynamic planner selectivity decisions.

Skiplist node lifecycle:

- insert path links new tower and marks node fully linked,
- delete path marks postings deleted,
- node unlink path marks node and splices lanes,
- retired/reclaimed node metrics support deferred GC behavior.

### Transaction Visibility and ProcArray

Key files:

- `aerostore_core/src/occ_partitioned.rs`
- `aerostore_core/src/procarray.rs`

Highlights:

- Bounded process slot registration for active transaction tracking.
- Snapshot creation from active transaction metadata.
- Conflict detection for serializable outcomes.

## Query Model

Aerostore currently supports STAPI-style query strings and list forms through parser/planner/execution.

Pipeline:

1. parse STAPI string into AST (`stapi_parser.rs`),
2. compile filters and choose access path (`rbo_planner.rs`),
3. produce candidate row IDs from driver route (`execution.rs`),
4. apply residual predicates (`filters.rs`),
5. apply sort and limit.

## STAPI Grammar Reference

### Top-level options

- `-compare <compare_expr>`
- `-sort <field>`
- `-limit <n>`

### Compare operators

- `null <field>`
- `notnull <field>`
- `= <field> <value>`
- `== <field> <value>`
- `!= <field> <value>`
- `<> <field> <value>`
- `> <field> <value>`
- `>= <field> <value>`
- `< <field> <value>`
- `<= <field> <value>`
- `in <field> {v1 v2 ...}`
- `match <field> <glob>`
- `notmatch <field> <glob>`

### Value typing

Atoms are parsed in this order:

- `i64` if integer parse succeeds,
- `f64` if decimal/scientific format parse succeeds,
- text otherwise.

### Examples

Single compare clause:

```tcl
-compare {{= flight_id UAL123}}
```

Multiple compare clauses:

```tcl
-compare {{= flight_id UAL123} {>= altitude 10000} {notmatch typ C17*}}
```

Null checks:

```tcl
-compare {{null destination} {<= altitude 12000}}
```

## Planner and Cardinality Routing

Key files:

- `aerostore_core/src/rbo_planner.rs`
- `aerostore_core/src/planner_cardinality.rs`

### Route kinds

- `PrimaryKeyLookup`
- `IndexExactMatch`
- `IndexRangeScan`
- `FullScan`

### Equality route selection

When multiple equality-indexable predicates are present, candidates are compared by:

1. higher `distinct_key_count` first,
2. PK candidate over secondary candidate on ties,
3. earlier filter index on ties.

This prevents low-cardinality index traps when a more selective equality route is available.

### Range route selection

If no equality route is available, indexed range candidates use catalog range rank ordering (`SchemaCatalog::rank_for`).

### Residual behavior

All non-driver predicates are compiled as residuals and applied in-memory after driver candidate expansion.

Negative and null-style predicates are intentionally residual in route planning:

- `!=`
- `notmatch`
- `null`
- `notnull`

## Nullability Model

Key files:

- `aerostore_macros/src/lib.rs`
- `aerostore_core/src/filters.rs`

`#[speedtable]` injects hidden row fields:

- `_null_bitmask: u64`
- `_xmin: u64`
- `_xmax: AtomicU64`

Behavior:

- bit `i` set means column `i` is logically SQL `NULL`.
- filter evaluation checks null bitmask first.
- non-null operators short-circuit false for null fields.
- this avoids `Option<T>` field layout overhead and preserves packed row semantics.

## Durability and Recovery

### Paths

- Shared WAL ring and writer daemon (`wal_ring.rs`, `wal_writer.rs`).
- Logical WAL and snapshot replay (`wal_logical.rs`, `recovery.rs`).
- Compatibility path in `wal.rs` retained for legacy/reference.

### Artifacts

Common file names used by current paths:

- `aerostore.wal`
- `occ_checkpoint.dat`
- `aerostore_logical.wal`
- `snapshot.dat`

### Tcl config knobs

- `aerostore.synchronous_commit` (`on` or `off`)
- `aerostore.checkpoint_interval_secs`

## Tcl Bridge

Key file:

- `aerostore_tcl/src/lib.rs`

Registered commands:

- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState search ...`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`

`FlightState search` supports:

- `-compare <stapi compare expr or Tcl list>`
- `-sort <field>`
- `-limit <n>`
- `-offset <n>`
- `-asc`
- `-desc`

Example session:

```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore
set _ [aerostore::init ./aerostore_tcl_data]

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1

set count [FlightState search -compare {{= flight_id UAL123} {>= altitude 10000}} -sort altitude -limit 50]
puts $count
```

## Build and Prerequisites

### Rust

- Stable Rust toolchain (`rustc`, `cargo`).

### Tcl bridge prerequisites

- Tcl 8.6 runtime and headers.
- clang and libclang.

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build Commands

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

## Testing and Benchmarks

### Fast sanity and compile gates

```bash
cargo test --workspace --no-run
cargo test -p aerostore_core --tests --no-run
cargo test -p aerostore_tcl --tests --no-run
cargo test -p aerostore_macros --test ui
```

### Planner/index reliability gates

```bash
cargo test -p aerostore_core --test planner_cardinality -- --nocapture
cargo test -p aerostore_core --test rbo_planner_routing -- --nocapture
cargo test -p aerostore_core --test shm_shared_memory forked_primary_key_map_updates_are_cross_process_visible -- --nocapture
cargo test -p aerostore_core --test shm_index_contention -- --nocapture
cargo test -p aerostore_core distinct_key_count_ -- --nocapture
```

### Parser/filter/bridge gates

```bash
cargo test -p aerostore_core stapi_parser -- --nocapture
cargo test -p aerostore_core filters:: -- --nocapture
cargo test -p aerostore_core --test speedtable_arena_perf -- --nocapture
cargo test -p aerostore_tcl --test search_stapi_bridge -- --nocapture
cargo test -p aerostore_tcl --test config_checkpoint_integration -- --nocapture
```

### Durability/concurrency gates

```bash
cargo test -p aerostore_core --test occ_write_skew -- --nocapture
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1
```

### Benchmark suites

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
```

### Ignored heavy suites

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### Criterion benches

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
```

## Nightly and Stress Runbook

Use:

- `docs/nightly_perf.md`

It includes:

- release build sanity,
- WAL/recovery/throughput suites,
- V4 shared-index validation gate,
- ignored stress suites,
- criterion commands.

## Reliability Coverage Highlights

### Planner and query behavior

- `aerostore_core/tests/planner_cardinality.rs`
  - high-cardinality route selection,
  - PK-vs-secondary tie handling,
  - predicate-order stability with residuals.
- `aerostore_core/tests/rbo_planner_routing.rs`
  - route kind behavior,
  - residual negative/null guarantees,
  - malformed/unknown handling.

### Shared-memory index correctness

- `aerostore_core/tests/shm_index_fork.rs`
- `aerostore_core/tests/shm_index_contention.rs`
- `aerostore_core/tests/shm_index_gc_horizon.rs`
- `aerostore_core/tests/shm_index_bounds.rs`
- `aerostore_core/src/shm_skiplist.rs` unit tests

### Shared-memory primitives and PK behavior

- `aerostore_core/tests/shm_shared_memory.rs`
- `aerostore_core/tests/shm_fork.rs`

### Durability and recovery

- `aerostore_core/tests/wal_crash_recovery.rs`
- `aerostore_core/tests/logical_recovery.rs`
- `aerostore_core/tests/wal_writer_lifecycle.rs`
- `aerostore_core/tests/wal_ring_benchmark.rs`

### Tcl integration

- `aerostore_tcl/tests/search_stapi_bridge.rs`
- `aerostore_tcl/tests/config_checkpoint_integration.rs`

## Troubleshooting

### `invalid Tcl-style list syntax`

Cause:

- malformed braces or quote/list structure in `-compare` expression.

Action:

- test expression with a minimal single clause first,
- then grow clause set incrementally.

### `unknown STAPI option` or `unsupported compare operator`

Cause:

- option typo or unsupported operator spelling.

Action:

- verify operator names exactly as documented in [STAPI Grammar Reference](#stapi-grammar-reference).

### Parse or planner errors returned as `TCL_ERROR:`

Cause:

- syntax issues, field mismatch, or invalid indexed value for route.

Action:

- verify field names are valid for current row type,
- ensure values are type-compatible with indexed route where expected.

### Cross-process tests fail on non-Unix systems

Cause:

- several suites rely on `fork()` and are guarded with `#[cfg(unix)]`.

Action:

- run on Linux/macOS for full shared-memory fork coverage.

### Benchmark variability

Cause:

- shared CI hosts or background load.

Action:

- use isolated hosts,
- run `--release`,
- prefer single-threaded harness for long suites (`--test-threads=1`).

## Known Constraints

- No official SQL surface in this prototype.
- Some modules are retained as compatibility/reference paths and are not the primary execution path.
- Performance benchmark thresholds may still vary by machine class.

## License

No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
