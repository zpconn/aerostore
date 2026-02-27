# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, lock-free shared-memory database prototype designed for high-ingest, high-concurrency market/flight data workloads.

As of February 27, 2026, this repository contains a working hybrid architecture:

- V1 in-process MVCC engine with durable WAL/checkpoint/recovery.
- V2 shared-memory (`mmap`) foundation for `fork()`-heavy deployments.
- PostgreSQL-style bounded `ProcArray` for active transaction tracking.
- Serializable OCC/SSI with savepoints and rollback-to-savepoint.
- Eager savepoint-write reclamation via lock-free recycled-row free list.
- SkipList secondary indexes and typed query execution.
- STAPI parser/compiler modules for Speedtables-style query syntax.
- Decoupled WAL ring + WAL writer daemon for `synchronous_commit=off` behavior.
- Tcl `cdylib` bridge for in-process ingest/query commands.

## Workspace Layout

- `aerostore_core/`: core engine, durability, parser/planner, tests, benches.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl extension crate (`cdylib`) and demo script.

## Current Module Inventory

`aerostore_core/src` currently exports:

- `arena.rs`: lock-free arena and version-chain table primitives.
- `txn.rs`: transaction manager and snapshot model (V1 path).
- `mvcc.rs`: MVCC visibility and row-version operations.
- `index.rs`: SkipList secondary indexes.
- `query.rs`: typed `QueryBuilder` planner/executor.
- `stapi_parser.rs`: `nom` combinator parser for STAPI/Tcl-style query strings.
- `planner.rs`: STAPI AST compiler and dynamic execution plan with index routing.
- `shm.rs`: shared memory arena (`mmap`) + `RelPtr<T>`.
- `procarray.rs`: bounded 256-slot active tx array.
- `occ.rs`: shared-memory OCC/SSI with savepoints.
- `wal.rs`: WAL, group commit, checkpointing, recovery.
- `wal_ring.rs`: shared-memory MPSC ring + `rkyv` commit payload codec.
- `wal_writer.rs`: forked WAL writer daemon + sync/async OCC commit sink.
- `ingest.rs`: TSV bulk upsert pipeline.
- `watch.rs`: pub/sub change streaming + TTL sweeper.

## Architecture Summary

### 1. Schema + Arena Foundation

- `#[speedtable]` injects hidden system columns (`_nullmask`, `_xmin`, `_xmax`).
- `ChunkedArena<T>` and `Table<K, V>` provide lock-free allocation + version chains.

Primary code:

- `aerostore_macros/src/lib.rs`
- `aerostore_core/src/arena.rs`

### 2. MVCC Visibility (PostgreSQL-inspired)

- Monotonic txids and active transaction tracking.
- Version-chain visibility checks via `is_visible(...)`.
- Deferred reclamation via `crossbeam-epoch`.

Primary code:

- `aerostore_core/src/txn.rs`
- `aerostore_core/src/mvcc.rs`

### 3. Shared Memory + Relative Pointer Model

- `ShmArena` maps shared memory with `MAP_SHARED`.
- `RelPtr<T>` stores offsets, not absolute pointers, so forked processes can safely dereference at different virtual base addresses.

Primary code:

- `aerostore_core/src/shm.rs`

### 4. Bounded ProcArray

- Fixed `PROCARRAY_SLOTS = 256`.
- Cache-line aligned slots to reduce false sharing.
- CAS claim/release on begin/end transaction.
- Snapshot creation has bounded constant work (scan 256 slots).

Primary code:

- `aerostore_core/src/procarray.rs`
- `aerostore_core/benches/procarray_snapshot.rs`

### 5. OCC/SSI + Savepoints

- Process-local read/write sets.
- `savepoint(name)` and `rollback_to(name)` support nested partial rollback.
- Commit phase validates read set and returns `SerializationFailure` on conflicts.
- Rolled-back write intents are recycled immediately into a lock-free free list.
- Superseded same-row writes within one transaction are recycled after commit publish.

Primary code:

- `aerostore_core/src/occ.rs`

### 6. Typed Query + STAPI Query Planning

- Typed path: `QueryBuilder` + SkipList candidate routing.
- STAPI path:
  - `stapi_parser.rs` parses strings like
    - `-compare {{match flight UAL*} {> alt 10000} {in typ {B738 A320}}} -sort alt -limit 50`
  - AST: `Query { filters, sort, limit }`
  - `planner.rs` compiles AST into executable filter plan.
  - Indexed `=`/`>` filters route to SkipList first; remaining clauses run as closures.
  - OCC execution path uses `OccTable::read(...)`, so rows are tracked in read-set for SSI validation.

Primary code:

- `aerostore_core/src/query.rs`
- `aerostore_core/src/stapi_parser.rs`
- `aerostore_core/src/planner.rs`

### 7. Durability

Two durability stacks currently exist:

- Integrated `DurableDatabase` stack (`wal.rs`):
  - framed `wal.log` (`bincode`)
  - background group commit
  - periodic `checkpoint.dat`
  - startup checkpoint + WAL replay
- Shared-memory OCC durability primitives (`wal_ring.rs` + `wal_writer.rs`):
  - lock-free MPSC ring
  - daemonized WAL sink with periodic `fdatasync`
  - `OccCommitter` supports sync and async commit modes

Primary code:

- `aerostore_core/src/wal.rs`
- `aerostore_core/src/wal_ring.rs`
- `aerostore_core/src/wal_writer.rs`

### 8. Ingest / Streaming / TTL

- High-speed TSV decode + batch upsert.
- Broadcast watch stream on commits.
- Background TTL sweeper.

Primary code:

- `aerostore_core/src/ingest.rs`
- `aerostore_core/src/watch.rs`

### 9. Tcl Bridge

- `aerostore_tcl` exports `Aerostore_Init` / `Aerostore_SafeInit`.
- Shared-memory OCC table + WAL ring writer are initialized once via `OnceLock` and shared by all interpreters in-process.
- `FlightState search` is executed against the OCC snapshot path (planner + index routing + SSI read tracking).
- `FlightState ingest_tsv` performs batched OCC upserts and commits through the WAL ring committer path.
- FFI errors are normalized with `TCL_ERROR:` prefix.

Primary code:

- `aerostore_tcl/src/lib.rs`
- `aerostore_tcl/test.tcl`

## Build Prerequisites

### Rust

- Stable Rust (`cargo`, `rustc`).

### Tcl extension (optional)

- Tcl 8.6 runtime/dev headers.
- `clang` + `libclang`.

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

Tcl extension only:

```bash
cargo build -p aerostore_tcl
```

## Test + Benchmark Runbook

### Full release suite

```bash
cargo test --workspace --release -- --test-threads=1
```

### Long-running GC stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### ProcArray constant-time benchmark

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

Assertions in benchmark:

- snapshot latency under `50ns` at txid=10 and txid=10,000,000
- near-constant delta between low/high txid runs

### WAL sync vs async throughput benchmark

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes -- --exact --nocapture --test-threads=1
```

Target:

- async mode (`synchronous_commit=off`) at least `10x` throughput vs sync mode

Additional WAL benchmark variants:

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts -- --exact --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_savepoint_churn -- --exact --nocapture --test-threads=1
```

### Query/index benchmarks

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

Includes:

- typed query benchmark (existing)
- STAPI parse+compile+execute benchmark (new)
- Tcl-style alias/match + `-desc`/`-offset`/`-limit` benchmark path (new)

### Targeted reliability suites

```bash
cargo test -p aerostore_core --release --test occ_write_skew --test occ_savepoint_reclaim --test procarray_concurrency --test wal_crash_recovery --test shm_shared_memory -- --test-threads=1
```

### Savepoint Reclamation Validation

```bash
cargo test -p aerostore_core --release --test occ_savepoint_reclaim -- --test-threads=1
```

## Reliability and Concurrency Coverage

- `tests/test_concurrency.rs`
  - loom CAS publication model check
  - 100,000-update GC stress + drop-accounting leak proof
- `tests/mvcc_tokio_concurrency.rs`
  - 50-reader/50-writer partial-update visibility guard
- `tests/procarray_concurrency.rs`
  - threaded/forked transaction slot stress
  - OCC conflict + ring backpressure slot-release check
  - concurrent savepoint/rollback churn with bounded shared-arena head growth assertion
- `tests/occ_write_skew.rs`
  - baseline write-skew serializable failure proof
  - planner-driven read variant proving SSI read-set integration
  - Tcl-like keyed-upsert write-skew variant (key map + planner reads)
  - savepoint-heavy write-skew variant proving SSI behavior with reclaimed interim writes
- `tests/occ_savepoint_reclaim.rs`
  - savepoint rollback reclamation loop proving abandoned writes are immediately reusable
  - nested savepoint rollback correctness for pending-write visibility
- `tests/wal_crash_recovery.rs`
  - checkpoint + WAL replay recovery
  - WAL daemon crash/restart replayability
  - Tcl-like keyed upsert crash/restart replay + secondary-index consistency checks
  - savepoint-rollback WAL test proving rolled-back write intents never become durable frames
- `tests/wal_ring_benchmark.rs`
  - sync-vs-async throughput gate
  - Tcl-like keyed upsert throughput gate (read/write + index maintenance workload)
  - savepoint-churn throughput gate (many rolled-back writes per committed transaction)
  - ring backpressure integrity checks
- `tests/query_index_benchmark.rs`
  - indexed query scan benchmark
  - STAPI parse/compile/execute benchmark extension
  - Tcl-style alias/match + paging/sort benchmark extension
- `tests/shm_shared_memory.rs`
  - forked CAS contention correctness
  - forked ring producer integrity checks
  - forked OCC recycle free-list CAS stress with bounded growth and no slot leakage

## Tcl Demo

Run:

```bash
tclsh aerostore_tcl/test.tcl
```

The demo now validates:

- basic ingest and search
- complex STAPI-style search input
- Tcl alias handling (`ident`, `alt`) with `match` glob filtering
- `-desc` + `-offset` + `-limit` paging semantics on search counts
- second ingest pass proving keyed upsert behavior (update existing + insert new)
- malformed search rejection with explicit `TCL_ERROR:` message

Example query syntax:

```tcl
set count [FlightState search -compare {{> altitude 10000} {< altitude 36000} {in flight {UAL123 AAL456 SWA321}}} -sort altitude -limit 10]
```

## Durable On-Disk Files

Current disk artifacts:

- V1 `DurableDatabase` path:
  - `wal.log`: framed append-only write-ahead log
  - `checkpoint.dat`: checkpoint image
- Tcl OCC + WAL ring path:
  - `aerostore.wal`: daemon-appended WAL stream from shared-memory ring commits

V1 recovery order:

1. load checkpoint
2. replay WAL frames
3. serve traffic

## Current Integration Boundaries

- `DurableDatabase` remains the V1 MVCC durability path used by core recovery tests.
- Tcl-facing query/ingest execution is now wired to shared-memory OCC + WAL ring commits (the previous README caveat has been resolved).
- Tcl search option parsing still uses Tcl list-object decoding (`Tcl_ListObjGetElements`) before mapping into planner filters; direct raw STAPI string parsing is implemented in `aerostore_core::stapi_parser` and validated in unit tests.
- `rollback_to(savepoint)` now eagerly recycles abandoned write allocations into a lock-free per-table free list, and subsequent OCC writes reuse those slots immediately.

## License

No license file is present yet. Treat this repository as proprietary/internal unless a license is explicitly added.
