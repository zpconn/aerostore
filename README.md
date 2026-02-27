# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, lock-free shared-memory database prototype for high-ingest, high-concurrency flight/state workloads.

As of February 27, 2026, this repository contains:

- V1 in-process MVCC engine with durable WAL/checkpoint/recovery (`wal.rs` path).
- V2 shared-memory (`mmap`) architecture for `fork()`-heavy deployments.
- PostgreSQL-style bounded `ProcArray` active transaction tracking.
- Serializable OCC/SSI with nested savepoints and rollback-to-savepoint.
- Lock-free savepoint write-slot reclamation/reuse.
- SkipList secondary indexes and typed + STAPI query planning.
- Shared-memory WAL ring + WAL writer daemon for async commit mode.
- OCC WAL replay recovery into fresh shared-memory tables.
- Tcl `cdylib` bridge for in-process ingest/query execution.

## Workspace Layout

- `aerostore_core/`: core engine, durability, parser/planner, tests, benchmarks.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl extension crate (`cdylib`) and demo script.

## Module Inventory

`aerostore_core/src` exports:

- `arena.rs`: lock-free arena + version-chain table primitives (V1).
- `txn.rs`: V1 transaction manager/snapshot model.
- `mvcc.rs`: V1 MVCC visibility/version operations.
- `index.rs`: SkipList secondary indexes.
- `query.rs`: typed `QueryBuilder` execution path.
- `stapi_parser.rs`: `nom` parser for STAPI/Tcl query syntax.
- `planner.rs`: STAPI AST compiler with index routing.
- `shm.rs`: shared memory arena (`mmap`) + `RelPtr<T>`.
- `procarray.rs`: bounded 256-slot active tx array.
- `occ.rs`: shared-memory OCC/SSI + savepoints + recycled free list + replay apply helpers.
- `wal.rs`: V1 durable WAL/checkpoint/group-commit/recovery path.
- `wal_ring.rs`: shared-memory MPSC ring + `rkyv` framing + per-write value payload carriage.
- `wal_writer.rs`: WAL daemon, sync/async commit sink, strict WAL parser, OCC replay helpers.
- `ingest.rs`: TSV bulk upsert pipeline.
- `watch.rs`: pub/sub streaming + TTL sweeper.

## Architecture Summary

### 1) Shared-Memory Foundation

- `ShmArena` maps process-shared memory with `MAP_SHARED`.
- `RelPtr<T>` stores offsets instead of virtual addresses.
- Child processes can dereference safely even with different mapping bases.

Primary code:

- `aerostore_core/src/shm.rs`

### 2) Bounded ProcArray

- `PROCARRAY_SLOTS = 256`.
- Cache-line aligned slots reduce false sharing.
- CAS claim/release in `begin_transaction`/`end_transaction`.
- Snapshot is bounded constant work (scan 256 slots).

Primary code:

- `aerostore_core/src/procarray.rs`
- `aerostore_core/benches/procarray_snapshot.rs`

### 3) OCC/SSI + Savepoints

- Process-local read/write sets.
- Nested savepoints with `rollback_to(name)`.
- Serializable validation returns `SerializationFailure` on conflicts.
- Abandoned write intents are eagerly recycled into lock-free free list.
- Superseded same-row intents are also recycled at commit.

Primary code:

- `aerostore_core/src/occ.rs`

### 4) Query Paths

- Typed path: `QueryBuilder` + index candidate routing.
- STAPI path:
  - parse (`stapi_parser.rs`) -> AST
  - compile (`planner.rs`) -> executable plan
  - route indexed filters (`=`/`>` etc.) to SkipList first
  - apply remaining filters as closures
- OCC reads are tracked for SSI validation.

Primary code:

- `aerostore_core/src/query.rs`
- `aerostore_core/src/stapi_parser.rs`
- `aerostore_core/src/planner.rs`

### 5) Durability Stacks

Two durability stacks exist:

- V1 durable stack (`wal.rs`):
  - framed append-only `wal.log` (`bincode`)
  - periodic `checkpoint.dat`
  - startup checkpoint + WAL replay

- Shared-memory OCC durable stack (`wal_ring.rs` + `wal_writer.rs`):
  - lock-free MPSC shared ring
  - forked WAL writer daemon with periodic `fdatasync`
  - WAL frames include commit metadata and serialized row-value payloads
  - strict WAL parser detects truncated/corrupt frames with explicit errors
  - replay API `recover_occ_table_from_wal(...)` rebuilds fresh OCC tables

Primary code:

- `aerostore_core/src/wal.rs`
- `aerostore_core/src/wal_ring.rs`
- `aerostore_core/src/wal_writer.rs`

### 6) Tcl Bridge

- Exports `Aerostore_Init` / `Aerostore_SafeInit`.
- Global shared DB initialized via `OnceLock`.
- `FlightState search` runs through STAPI planner path.
- `FlightState ingest_tsv` performs batched OCC upserts.
- On init, Tcl bridge replays durable WAL into OCC table, then rebuilds key/secondary indexes from recovered rows.
- FFI errors normalized with `TCL_ERROR:` prefix.

Primary code:

- `aerostore_tcl/src/lib.rs`
- `aerostore_tcl/test.tcl`

## Build Prerequisites

### Rust

- Stable Rust toolchain (`cargo`, `rustc`).

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

### Tcl bridge integration

```bash
cargo test -p aerostore_tcl --release --test search_stapi_bridge -- --test-threads=1
```

### Long-running MVCC GC stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### ProcArray constant-time benchmark

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

Benchmark assertion target:

- snapshot latency under `50ns` at txid=10 and txid=10,000,000

### WAL sync vs async throughput benchmarks

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes -- --exact --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts -- --exact --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_savepoint_churn -- --exact --nocapture --test-threads=1
```

Target:

- async mode (`synchronous_commit=off`) at least `10x` throughput vs sync mode

### Query/index benchmarks

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

Includes:

- indexed query scan benchmark
- STAPI parse/compile/execute benchmark
- Tcl-style alias/match + paging/sort benchmark path
- Tcl-bridge-style STAPI assembly + compile + execute benchmark path

### OCC/WAL replay reliability + replay throughput

```bash
cargo test -p aerostore_core --release --test wal_crash_recovery -- --nocapture --test-threads=1
```

Includes:

- crash/restart WAL daemon replay checks
- fresh-table OCC replay recovery checks
- truncated/corrupt WAL rejection checks
- idempotent replay checks (same WAL into fresh tables)
- startup replay throughput benchmark (`benchmark_occ_wal_replay_startup_throughput`)

### Targeted reliability suite

```bash
cargo test -p aerostore_core --release --test occ_write_skew --test occ_savepoint_reclaim --test procarray_concurrency --test wal_crash_recovery --test wal_writer_lifecycle --test shm_shared_memory -- --test-threads=1
```

## Reliability Coverage Matrix

- `tests/test_concurrency.rs`
  - loom CAS model checks
  - 100,000-update GC stress + drop-accounting leak proof
- `tests/mvcc_tokio_concurrency.rs`
  - 50-reader/50-writer partial-update visibility guard
- `tests/procarray_concurrency.rs`
  - threaded/forked slot stress
  - OCC conflict + ring backpressure slot-release checks
  - savepoint/rollback churn with bounded shared-arena growth checks
- `tests/occ_write_skew.rs`
  - write-skew serializable failure proof
  - planner-read SSI coverage
  - Tcl-like keyed-upsert SSI coverage
  - savepoint-heavy write-path SSI coverage
- `tests/occ_savepoint_reclaim.rs`
  - reclaimed write-slot reuse proof
  - nested savepoint rollback correctness
- `tests/wal_crash_recovery.rs`
  - OCC replay recovery into fresh shared-memory tables
  - WAL daemon crash/restart replayability
  - keyed-upsert + index consistency after replay
  - savepoint rollback durability correctness
  - malformed/truncated WAL rejection
  - replay idempotency across fresh tables
  - replay startup throughput benchmark
- `tests/wal_writer_lifecycle.rs`
  - parent-death lifecycle guard (daemon exits when parent exits)
- `tests/wal_ring_benchmark.rs`
  - sync-vs-async throughput gate
  - keyed-upsert throughput gate
  - savepoint-churn throughput gate
  - ring backpressure integrity (txid + value payload)
- `tests/shm_shared_memory.rs`
  - forked CAS contention correctness
  - forked ring producer integrity (txid + value payload checksums)
  - forked OCC recycle free-list CAS stress

## Tcl Demo

Run:

```bash
tclsh aerostore_tcl/test.tcl
```

Demo covers:

- TSV ingest + search
- complex STAPI-style filters
- alias handling (`ident`, `alt`)
- `-desc` + `-offset` + `-limit` paging
- keyed upsert behavior on repeated ingest
- malformed search rejection with explicit `TCL_ERROR:`

Example query:

```tcl
set count [FlightState search -compare {{> altitude 10000} {< altitude 36000} {in flight {UAL123 AAL456 SWA321}}} -sort altitude -limit 10]
```

## Durable On-Disk Files

- V1 path (`wal.rs`):
  - `wal.log`
  - `checkpoint.dat`
- OCC/Tcl path:
  - `aerostore.wal` (framed WAL stream written by daemon)

## Current Integration Boundaries

- Shared-memory OCC path is WAL-replay durable, but does not yet implement OCC-native checkpoint compaction/truncation.
- `synchronous_commit=off` intentionally acknowledges commit before disk flush; latest buffered interval can be lost on hard crash by design.
- `DurableDatabase` remains available for legacy V1 API coverage.
- Tcl config hooks for runtime toggles (`set_config/get_config`) are still not exported.

## License

No license file is present yet. Treat this repository as proprietary/internal unless a license is explicitly added.
