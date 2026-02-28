# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, shared-memory database prototype for high-ingest flight/state workloads.
It combines:

- lock-free shared-memory data structures (`RelPtr`, shared arena, CAS-based updates),
- PostgreSQL-inspired snapshot/OCC/SSI behavior,
- shared-memory secondary indexing,
- WAL + checkpoint/snapshot durability paths,
- in-process Tcl integration (`cdylib`) for low-overhead execution.

## How Aerostore Works (Plain Language)

1. Data lives in a shared memory segment so forked worker processes see the same state.
2. Transactions read a snapshot and build local read/write sets.
3. Writes create new row versions and publish them atomically at commit time.
4. Reads do not block writes; SSI validation catches conflicting commit interleavings.
5. Commits are persisted through WAL paths (sync or async ring/daemon, depending on mode).
6. Checkpoints/snapshots compact current state to disk.
7. On startup, Aerostore rebuilds live state into a fresh shared memory mapping from persisted artifacts.

## What This Repository Contains

- `aerostore_core`: engine, concurrency control, shared memory primitives, query planner/executor, durability, tests/benchmarks.
- `aerostore_tcl`: Tcl extension bridge exposing search/ingest/config/checkpoint commands.
- `aerostore_macros`: schema macro crate.
- `docs/nightly_perf.md`: nightly/long-running benchmark runbook.

The repo currently includes multiple durability/concurrency paths (legacy + newer shared-memory paths) for compatibility and validation.

## Query Support

### STAPI-style filters

- `=` exact match
- `>` greater than
- `<` less than
- `>=` greater than or equal
- `in field {a b c}` membership
- `match field glob*` glob matching

### Search controls

- `-sort <field>`
- `-limit <n>`
- Tcl bridge options: `-offset <n>`, `-asc`, `-desc`

## Rule-Based Optimizer (RBO)

Aerostore uses a zero-allocation heuristic planner in `rbo_planner.rs` + `execution.rs`.

Routing precedence:

1. Primary-key equality route (`RouteKind::PrimaryKeyLookup`) through shared `ShmPrimaryKeyMap`.
2. Indexed equality route (`RouteKind::IndexExactMatch`).
3. Indexed range route (`RouteKind::IndexRangeScan`) for `>`, `<`, `>=`.
4. Full scan (`RouteKind::FullScan`) fallback.

Tie-breaks are deterministic via cardinality ranking (default: `flight_id` -> `geohash` -> `dest` -> `altitude`).

Execution pipeline:

1. produce candidate row IDs from the chosen route,
2. read rows under transaction snapshot visibility,
3. apply residual predicates,
4. apply sort,
5. apply limit.

## Why Queries Are Fast

- Shared-memory indexes avoid IPC/network overhead between worker processes.
- Candidate narrowing happens early (PK/index route before residual filtering).
- Residual predicates run only on narrowed candidates.
- Snapshot-safe reads do not block writers.
- Shared-memory PK lookup path provides O(1) route for exact key queries.

## Architecture Summary

### 1) Shared Memory Foundation (`shm.rs`)

- POSIX shared memory mapping (`MAP_SHARED`).
- Relative pointers (`RelPtr<T>`) instead of process-local absolute pointers.
- Lock-free shared bump allocator (`ChunkedArena`) with CAS head movement.

### 2) Active Transaction Tracking (`procarray.rs`)

- Bounded, cache-line-aligned ProcArray (`PROCARRAY_SLOTS = 256`).
- CAS slot claim/release for begin/end transaction.
- Bounded snapshot creation cost.

### 3) OCC/SSI Core (`occ_partitioned.rs` via `occ.rs`)

- Serializable OCC with read-set and write-set tracking.
- Partitioned lock striping (`1024` lock buckets) to reduce commit bottlenecks.
- Savepoints and rollback-to-savepoint write-intent truncation.
- Conflict outcome: `SerializationFailure`.

### 4) Shared-Memory Indexing

- Shared secondary index implementation in `shm_index.rs` (`SecondaryIndex`).
- CAS-based insertion/removal, range scans, and deferred reclamation horizon checks.
- Shared primary key map in `execution.rs` (`ShmPrimaryKeyMap`) for PK route.

### 5) Query Layer

- `stapi_parser.rs`: Tcl list syntax parsing to typed AST.
- `rbo_planner.rs`: route selection and residual predicate compilation.
- `execution.rs`: candidate-to-result execution path.
- `query.rs`: typed Rust query API (`QueryBuilder`, `QueryEngine`) also retained.

### 6) Ingestion and Streaming

- `ingest.rs`: high-speed TSV ingestion helpers.
- `watch.rs`: subscriptions + TTL sweeper support.

### 7) Durability and Recovery

- `wal_ring.rs` + `wal_writer.rs`: shared ring commit path and WAL writer daemon.
- `wal_logical.rs` + `recovery.rs`: logical WAL framing and snapshot/WAL replay recovery.
- `wal.rs`: legacy durability path retained.

## Durability Paths and Artifacts

On-disk files used by different paths:

- `aerostore.wal`
- `occ_checkpoint.dat`
- `aerostore_logical.wal`
- `snapshot.dat`
- (legacy path) `wal.log`, `checkpoint.dat`

Config knobs exposed through Tcl:

- `aerostore.synchronous_commit` (`on` / `off`)
- `aerostore.checkpoint_interval_secs`

## Tcl Bridge (`aerostore_tcl`)

The Tcl extension exports:

- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState search ...`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`

Example:

```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore
set _ [aerostore::init ./aerostore_tcl_data]

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1
set n [FlightState search -compare {{= flight_id UAL123} {>= altitude 10000}} -sort altitude -limit 50]
puts $n
```

## Build Prerequisites

Rust:

- stable Rust toolchain (`cargo`, `rustc`)

For Tcl bridge builds/tests:

- Tcl 8.6 runtime and development headers
- clang/libclang

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

## Test and Benchmark Runbook

### Fast sanity checks

```bash
cargo test --workspace --no-run
cargo test -p aerostore_core --tests --no-run
cargo test -p aerostore_tcl --tests --no-run
```

### Targeted reliability suites

```bash
cargo test -p aerostore_core --test rbo_planner_routing -- --nocapture
cargo test -p aerostore_core --test occ_write_skew -- --nocapture
cargo test -p aerostore_core --test shm_shared_memory -- --nocapture
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_tcl --test search_stapi_bridge -- --nocapture
cargo test -p aerostore_tcl --test config_checkpoint_integration -- --nocapture
```

### Benchmark / long-running suites

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
```

Ignored heavy contention/model suites:

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

Criterion:

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

Nightly runbook:

- `docs/nightly_perf.md`

## Reliability Coverage Matrix

### Concurrency, MVCC, OCC, savepoints

- `aerostore_core/tests/test_concurrency.rs`
  - loom CAS model checks and long-running GC stress/leak accounting.
- `aerostore_core/tests/mvcc_tokio_concurrency.rs`
  - 50 readers + 50 writers snapshot isolation behavior.
- `aerostore_core/tests/procarray_concurrency.rs`
  - thread/fork ProcArray stress and slot release safety.
- `aerostore_core/tests/occ_write_skew.rs`
  - write-skew rejection across baseline/planner/keyed/savepoint/PK-route scenarios.
- `aerostore_core/tests/occ_savepoint_reclaim.rs`
  - savepoint rollback truncation/reuse and replay correctness.

### Shared memory and index correctness

- `aerostore_core/tests/shm_fork.rs`
  - parent/child `RelPtr` mutation sanity.
- `aerostore_core/tests/shm_shared_memory.rs`
  - CAS contention, ring integrity, PK map fork visibility, OCC recycle stress.
- `aerostore_core/tests/shm_index_fork.rs`
  - cross-process index visibility.
- `aerostore_core/tests/shm_index_contention.rs`
  - same-key contention exactness.
- `aerostore_core/tests/shm_index_gc_horizon.rs`
  - deferred reclamation horizon enforcement.
- `aerostore_core/tests/shm_index_bounds.rs`
  - oversized key/payload rejection.

### Planning and query behavior

- `aerostore_core/tests/rbo_planner_routing.rs`
  - route precedence, tie-breaks, residual filtering, malformed syntax handling.
- `aerostore_core/tests/query_index_benchmark.rs`
  - typed query vs STAPI path, Tcl-style path, PK route vs full scan, tie-break benchmark.

### WAL, checkpoint, crash recovery

- `aerostore_core/tests/wal_crash_recovery.rs`
  - replay, checkpoint tail recovery, corruption handling, idempotency, high-cardinality checks.
- `aerostore_core/tests/logical_recovery.rs`
  - logical snapshot/WAL crash recovery and strict malformed-log rejection.
- `aerostore_core/tests/wal_writer_lifecycle.rs`
  - writer daemon lifecycle guarantees.
- `aerostore_core/tests/wal_ring_benchmark.rs`
  - sync vs async throughput and backpressure gates.
- `aerostore_core/tests/occ_checkpoint_benchmark.rs`
  - checkpoint/replay cardinality performance.

### Ingest, watch, TTL, Tcl integration

- `aerostore_core/tests/ingest_watch_ttl.rs`
  - ingest + watch + TTL behavior.
- `aerostore_tcl/tests/search_stapi_bridge.rs`
  - list/raw STAPI parity, `>=`, PK equality routing, error boundary checks.
- `aerostore_tcl/tests/config_checkpoint_integration.rs`
  - Tcl config toggles, checkpoint behavior, recovery (including PK lookup checks).

## Current Status

- Primary execution path is shared-memory OCC/SSI + partitioned lock striping.
- Query path is RBO (`rbo_planner.rs` + `execution.rs`) with shared PK map support.
- Shared-memory secondary indexes are cross-process visible under `fork()`.
- Async WAL ring path and logical WAL/snapshot recovery path are both implemented and tested.
- Legacy modules (`mvcc.rs`, `wal.rs`, `occ_legacy.rs`) remain for compatibility/reference and test coverage.

## Workspace Layout

- `aerostore_core/` core engine, durability, parser/planner/execution, tests, benchmarks.
- `aerostore_tcl/` Tcl extension bridge and integration tests.
- `aerostore_macros/` schema macro crate.
- `docs/` operational docs (nightly benchmark runbook).

## License

No license file is currently present. Treat this repository as proprietary/internal unless a license is explicitly added.
