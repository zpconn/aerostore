# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first database prototype for high-ingest, high-concurrency flight/state workloads.  
It combines lock-free shared-memory data structures with PostgreSQL-inspired MVCC/OCC semantics and Tcl in-process integration.

## How It Works in Plain English

1. All workers share one big memory area, so they are looking at the same database state.
2. When a transaction changes a row, it writes a new version instead of overwriting the old one.
3. Readers see a stable snapshot, so reads do not block writers and do not see half-finished updates.
4. Before commit, Aerostore checks whether another transaction changed something you depended on; if yes, it aborts with a serialization failure so callers can retry safely.
5. Committed changes are written to WAL for crash recovery, with configurable sync behavior (`on` for durability-first, `off` for throughput-first).
6. Periodic checkpoints save a compact current state so restart is fast: load checkpoint, replay remaining WAL, rebuild indexes.
7. Tcl scripts call into this engine directly in-process (`FlightState search`, `FlightState ingest_tsv`) without network or IPC overhead.

As of February 27, 2026, this repo includes:

- V1 in-process MVCC + durable WAL/checkpoint/recovery path.
- V2 shared-memory (`mmap`) architecture designed for `fork()`-heavy process models.
- Bounded ProcArray (256 slots) for O(1)-bounded snapshot construction.
- Serializable OCC/SSI with nested savepoints and rollback-to-savepoint.
- Lock-free write-intent reclamation and reuse after rollback/supersession.
- SkipList secondary indexing and typed + STAPI planner execution.
- Shared-memory WAL ring and dedicated WAL writer daemon for async commit mode.
- OCC durability via checkpoint compaction + WAL tail replay.
- Tcl `cdylib` bridge with global `OnceLock` database initialization.

## Workspace Layout

- `aerostore_core/`: storage engines, transaction logic, parser/planner, durability, tests, benchmarks.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl extension bridge (`cdylib`) and Tcl-focused integration tests.

## Core Module Inventory

`aerostore_core/src` exports:

- `arena.rs`: V1 lock-free arena + version-chain primitives.
- `txn.rs`: V1 transaction manager and snapshot model.
- `mvcc.rs`: V1 MVCC visibility/version operations.
- `index.rs`: SkipList secondary indexes.
- `query.rs`: typed `QueryBuilder` API and execution.
- `stapi_parser.rs`: parser-combinator STAPI parser (`nom`).
- `planner.rs`: STAPI AST compiler + index-routing execution plan.
- `shm.rs`: shared memory mapping and `RelPtr<T>` relative pointers.
- `procarray.rs`: bounded 256-slot active transaction array.
- `occ.rs`: shared-memory OCC/SSI + savepoints + recyclable write-intent free-list.
- `wal.rs`: V1 durable stack (group commit + checkpoint + recovery).
- `wal_ring.rs`: shared-memory MPSC WAL ring and commit framing.
- `wal_writer.rs`: WAL writer daemon, commit sink modes, checkpoint+WAL recovery helpers.
- `ingest.rs`: zero-copy-style TSV ingest and bulk upsert primitives.
- `watch.rs`: pub/sub change feed and TTL sweeper.

## Architecture Summary

### 1) Shared Memory (`shm.rs`)

- Uses `MAP_SHARED` mappings for process-shared data.
- `RelPtr<T>` stores offsets rather than process-local virtual addresses.
- Child processes can safely dereference shared rows despite different mapping bases.

### 2) ProcArray (`procarray.rs`)

- Fixed-size `PROCARRAY_SLOTS = 256`.
- Cache-line aligned slot cells reduce false sharing.
- `begin_transaction` claims a slot via CAS; `end_transaction` releases with `0`.
- Snapshot creation scans a bounded array, avoiding unbounded active-list growth.

### 3) OCC/SSI + Savepoints (`occ.rs`)

- Each transaction tracks process-local read/write sets.
- Nested `savepoint` and `rollback_to` supported.
- SSI validation detects conflicting post-snapshot updates and returns `SerializationFailure`.
- Abandoned write intents are eagerly recycled into a lock-free free-list.
- Replay helpers exist for recovery and checkpoint application.

### 4) Query and Planner Paths (`query.rs`, `stapi_parser.rs`, `planner.rs`)

- Typed query path via `QueryBuilder`.
- STAPI path parses Tcl-style filter syntax into AST, then compiles to executable plans.
- Indexed predicates route to SkipList candidates first; residual filters apply as closures.
- Reads flow through visibility and OCC read tracking for serializable validation.

### 5) Dual Durability Stacks

V1 path (`wal.rs`):

- append-only `wal.log` (`bincode`),
- periodic `checkpoint.dat`,
- startup checkpoint + WAL replay.

V2 OCC path (`wal_ring.rs` + `wal_writer.rs`):

- shared-memory MPSC ring for async producer fast-path,
- dedicated WAL writer daemon with periodic `fdatasync`,
- strict WAL frame parsing with explicit truncation/corruption errors,
- checkpoint compaction (`occ_checkpoint.dat`) + WAL tail replay recovery.

### 6) Tcl Bridge (`aerostore_tcl/src/lib.rs`)

- Exports `Aerostore_Init` / `Aerostore_SafeInit`.
- Global DB initialized once via `OnceLock`.
- `FlightState search` executes through STAPI planner path.
- `FlightState ingest_tsv` performs batched OCC upserts with index maintenance.
- Startup recovery loads checkpoint + WAL and rebuilds in-memory indexes.
- All FFI errors are normalized with `TCL_ERROR: ...`.

## Tcl API Surface

Namespace commands:

- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`

Supported config keys:

- `aerostore.synchronous_commit` = `on|off`
- `aerostore.checkpoint_interval_secs` = non-negative integer seconds (`0` disables periodic checkpointing)

Table command:

- `FlightState search ?options?`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`

Example STAPI query:

```tcl
set count [FlightState search -compare {{> altitude 10000} {< altitude 36000} {in flight {UAL123 AAL456 SWA321}}} -sort altitude -limit 10]
```

## Durability + Commit Semantics

- Default Tcl mode is `synchronous_commit=on` (commit waits for durable append path).
- `synchronous_commit=off` uses WAL ring buffering and acknowledges before disk flush.
- In `off` mode, latest buffered interval can be lost on hard crash by design.
- `aerostore::checkpoint_now` is rejected while `synchronous_commit=off`.
- Background checkpointer thread runs in Tcl/OCC path and uses `aerostore.checkpoint_interval_secs` (default `300` seconds).

## Build Prerequisites

Rust:

- stable Rust toolchain (`cargo`, `rustc`).

Tcl extension (optional):

- Tcl 8.6 runtime + dev headers,
- `clang` and `libclang`.

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

## Runbook: Tests and Benchmarks

Full release test suite:

```bash
cargo test --workspace --release -- --test-threads=1
```

### Tcl integration

```bash
cargo test -p aerostore_tcl --release --test search_stapi_bridge -- --test-threads=1
cargo test -p aerostore_tcl --release --test config_checkpoint_integration -- --test-threads=1
tclsh aerostore_tcl/test.tcl
```

### Long-running MVCC GC stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### ProcArray benchmark

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

Target:

- snapshot latency under `50ns` at txid=10 and txid=10,000,000.

### WAL sync vs async throughput gates

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes -- --exact --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts -- --exact --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes_savepoint_churn -- --exact --nocapture --test-threads=1
```

Target:

- async (`synchronous_commit=off`) throughput at least `10x` sync mode.

### Query/index benchmark suite

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

### OCC replay reliability + replay throughput

```bash
cargo test -p aerostore_core --release --test wal_crash_recovery -- --nocapture --test-threads=1
```

Includes crash/restart replay checks, truncation/corruption handling, idempotency checks, checkpoint+tail recovery, and replay throughput benchmark.

### OCC checkpoint latency benchmark

```bash
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --nocapture --test-threads=1
```

Profiles checkpoint latency at:

- `10,000` rows,
- `100,000` rows,
- `1,000,000` rows.

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
  - classic write-skew serializable-failure proof
  - planner-read SSI coverage
  - Tcl-like keyed-upsert SSI coverage
  - savepoint-heavy write-path SSI coverage
- `tests/occ_savepoint_reclaim.rs`
  - reclaimed write-slot reuse proof
  - nested savepoint rollback correctness
- `tests/wal_crash_recovery.rs`
  - OCC replay into fresh shared-memory tables
  - checkpoint compaction + WAL tail replay correctness
  - checkpoint-under-write-load recoverability
  - WAL daemon crash/restart replayability
  - keyed-upsert/index consistency after replay
  - savepoint rollback durability correctness
  - malformed/truncated WAL rejection
  - replay idempotency across fresh tables
  - replay throughput benchmark
- `tests/wal_writer_lifecycle.rs`
  - parent-death lifecycle guard (daemon exits with parent)
- `tests/wal_ring_benchmark.rs`
  - sync-vs-async throughput gates
  - keyed-upsert throughput gate
  - savepoint-churn throughput gate
  - backpressure integrity checks (txid + payload correctness)
- `tests/occ_checkpoint_benchmark.rs`
  - checkpoint latency profiling at 10k/100k/1M row cardinalities
- `tests/shm_shared_memory.rs`
  - forked CAS contention correctness
  - forked ring-producer integrity
  - forked OCC recycle free-list CAS stress
- `aerostore_tcl/tests/search_stapi_bridge.rs`
  - STAPI parse/plan Tcl bridge parity and malformed input handling
- `aerostore_tcl/tests/config_checkpoint_integration.rs`
  - `off -> on -> checkpoint -> recover` durability transition
  - periodic checkpointer file/truncation behavior
  - Tcl-path sync-vs-async throughput benchmark

## On-Disk Artifacts

V1 path (`wal.rs`):

- `wal.log`
- `checkpoint.dat`

OCC/Tcl path:

- `aerostore.wal`
- `occ_checkpoint.dat`

## Current Status

- Shared-memory OCC durability is integrated with checkpoint compaction and startup recovery (`checkpoint + WAL tail`).
- Tcl runtime config hooks are exported and tested.
- Tcl-facing query execution, STAPI parsing/planning, OCC tracking, and durability flow are wired together.
- `DurableDatabase` remains in-tree for V1 compatibility and legacy-path coverage.

## License

No license file is present yet. Treat this repository as proprietary/internal unless a license is explicitly added.
