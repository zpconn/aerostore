# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, shared-memory database prototype for high-ingest flight/state workloads.
It combines lock-free data structures, PostgreSQL-style concurrency control ideas, and Tcl in-process integration.

As of February 27, 2026, this repository contains:

- V1 in-process MVCC + WAL/checkpoint recovery path.
- V2 shared-memory OCC/SSI engine with ProcArray, savepoints, shared-memory lock-free secondary indexing, WAL ring, and WAL writer daemon.
- V3 logical WAL + snapshot recovery path with strict corruption detection and crash-recovery validation.

The project now includes a production-oriented solution to a hard systems problem: lock-free, cross-process shared-memory secondary indexes for a fork-heavy in-memory database.

## How Shared-Memory Secondary Indexing Works

1. The index lives in POSIX shared memory (`MAP_SHARED`), not the Rust process heap.
2. Every index link is a relative offset (`RelPtr`/`AtomicU32`), so parent and forked children can map at different virtual addresses without breaking pointers.
3. Inserts never take a global lock. Writers allocate node/posting structs in the shared arena and use CAS to splice them into the index.
4. Deletes are logical first: posting entries are marked deleted, and empty key nodes are unlinked with CAS.
5. Unlinked nodes are deferred for reclamation. They are not immediately freed.
6. Reclamation uses ProcArray snapshot horizons: a retired node is reclaimed only after all older active transactions are gone.
7. Because all metadata and links are in shared memory, updates from one process are immediately visible to all sibling worker processes after `fork()`.

## How It Works (Plain Language)

1. Aerostore keeps most state in shared memory so multiple worker processes can see the same database.
2. Writers do not overwrite rows in place. They publish new row versions and mark old versions obsolete.
3. Readers run on snapshots, so they do not block writers and never see half-written updates.
4. At commit, OCC/SSI validation checks for serializable conflicts. Conflicting transactions return `SerializationFailure` so callers can retry.
5. Committed changes are persisted through WAL and checkpoints.
6. On restart, Aerostore rebuilds memory state by loading a checkpoint and replaying WAL records.
7. Tcl scripts run queries and ingestion in-process, without network or IPC round-trips.

## Query Support and Why It Is Fast

Supported filter operators:

- `=` exact match
- `>` and `<` range predicates
- `in field {a b c}` set membership
- `match field pattern` glob-style matching

Execution controls:

- `-sort <field>`
- `-limit <n>`
- `-offset <n>`
- ascending and descending order

Why queries are fast:

- Indexed predicates route to shared-memory lock-free `SecondaryIndex` scans first.
- Unindexed predicates use sequential scan fallback.
- Every candidate is filtered through MVCC/OCC visibility, so results are snapshot-correct.
- Residual filters are applied after candidate narrowing.
- `-limit` enables early stop when possible.
- Tcl path runs in-process through Rust planner/executor.

## Workspace Layout

- `aerostore_core/`: core storage engines, durability, parser/planner, tests, benchmarks.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl `cdylib` bridge and Tcl integration tests.

## Core Module Inventory

`aerostore_core/src` exports:

- `arena.rs`: V1 arena and version-chain primitives.
- `txn.rs`: V1 transaction manager and snapshot model.
- `mvcc.rs`: V1 MVCC visibility rules and table helpers.
- `index.rs`: index value/compare abstraction and `SecondaryIndex` export.
- `shm_index.rs`: shared-memory lock-free secondary index (`AtomicU32` + `RelPtr` links) with deferred cross-process GC.
- `query.rs`: strongly typed query builder/executor.
- `stapi_parser.rs`: STAPI parser (nom combinators).
- `planner.rs`: AST-to-plan compiler with index routing.
- `shm.rs`: shared memory allocator + `RelPtr<T>`.
- `procarray.rs`: bounded 256-slot process transaction array.
- `occ.rs`: OCC/SSI transactions, read/write sets, savepoints, rollback, recyclable write intents.
- `wal.rs`: V1 durable database WAL/checkpoint/recovery path.
- `wal_ring.rs`: shared-memory MPSC WAL ring buffer.
- `wal_writer.rs`: WAL writer daemon, replay helpers, checkpoint recovery.
- `wal_logical.rs`: V3 logical WAL records, daemon, strict frame parsing, corruption errors.
- `recovery.rs`: V3 logical snapshot + logical WAL replay database (`LogicalDatabase`).
- `ingest.rs`: high-speed TSV ingestion primitives.
- `watch.rs`: pub/sub watch streams and TTL sweeper.

## Architecture Summary

### 1) Shared Memory Foundation (`shm.rs`)

- Uses shared memory mappings for process-shared state.
- `RelPtr<T>` stores offsets instead of process-local pointers.
- Supports `fork()` process models where virtual addresses can differ per process.

### 2) Transaction Tracking (`procarray.rs`)

- Fixed `PROCARRAY_SLOTS = 256`.
- CAS slot claim/release for active transaction IDs.
- Snapshot creation scans a bounded array to avoid unbounded active-list growth.

### 3) Serializable OCC (`occ.rs`)

- Per-transaction process-local read/write sets.
- Nested savepoints and `rollback_to`.
- SSI conflict validation at commit.
- Abandoned/superseded write intents are recycled in shared arena free-list.

### 4) Planner and Index Routing (`stapi_parser.rs`, `planner.rs`, `query.rs`, `shm_index.rs`)

- STAPI strings compile into typed execution plans.
- Indexable predicates (`=`/`>`/`<`) route to shared-memory index scans.
- Unindexed predicates run as residual filters.
- Reads are tracked for SSI validation.

### 5) Ingestion and Streaming (`ingest.rs`, `watch.rs`)

- Zero-allocation-oriented TSV parsing for bulk upserts.
- Table watch subscriptions emit row-change events.
- TTL sweeper prunes stale rows for bounded memory usage.

### 6) Durability Stacks

V1 durability path (`wal.rs`):

- Append-only `wal.log`
- Periodic `checkpoint.dat`
- Recovery by checkpoint + WAL replay

OCC ring path (`wal_ring.rs` + `wal_writer.rs`):

- Shared MPSC ring for producer fast-path
- WAL writer daemon flush + `fdatasync` cadence
- `occ_checkpoint.dat` + `aerostore.wal` replay

Logical durability path (`wal_logical.rs` + `recovery.rs`):

- Logical `WalRecord` enum (`Upsert`, `Delete`, `Commit`)
- Frame format: `u32 length + bincode payload`
- Strict fail-fast parsing on corrupt/truncated frames
- `snapshot.dat` + `aerostore_logical.wal` replay into fresh shared memory
- `LogicalDatabaseConfig { synchronous_commit: bool }`
  - `true`: commit waits for WAL daemon durability ack on `Commit`
  - `false`: commit returns after enqueue; daemon syncs on interval

### 7) Tcl Bridge (`aerostore_tcl/src/lib.rs`)

- Exposes `Aerostore_Init` / `Aerostore_SafeInit`.
- Global in-process database via `OnceLock`.
- Commands:
  - `aerostore::init ?data_dir?`
  - `aerostore::set_config <key> <value>`
  - `aerostore::get_config <key>`
  - `aerostore::checkpoint_now`
  - `FlightState search ?options?`
  - `FlightState ingest_tsv <tsv_data> ?batch_size?`

Current Tcl durability path uses OCC ring + checkpoint flow (`aerostore.wal`, `occ_checkpoint.dat`).

## On-Disk Artifacts

V1 path:

- `wal.log`
- `checkpoint.dat`

OCC ring path:

- `aerostore.wal`
- `occ_checkpoint.dat`

Logical path:

- `aerostore_logical.wal`
- `snapshot.dat`

## Build Prerequisites

Rust:

- stable Rust toolchain (`cargo`, `rustc`)

Tcl extension (optional):

- Tcl 8.6 runtime + development headers
- `clang` and `libclang`

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

Build workspace:

```bash
cargo build --workspace
```

Build Tcl extension only:

```bash
cargo build -p aerostore_tcl
```

## Test and Benchmark Runbook

Full release compile check:

```bash
cargo test --workspace --release --no-run
```

Core release suite:

```bash
cargo test -p aerostore_core --release -- --test-threads=1
```

Tcl release suite:

```bash
cargo test -p aerostore_tcl --release -- --test-threads=1
tclsh aerostore_tcl/test.tcl
```

### Targeted Reliability Suites

Logical crash recovery and WAL robustness:

```bash
cargo test -p aerostore_core --release --test logical_recovery -- --nocapture --test-threads=1
```

Savepoint reclamation and replay correctness:

```bash
cargo test -p aerostore_core --release --test occ_savepoint_reclaim -- --test-threads=1
```

OCC crash/replay and daemon lifecycle path:

```bash
cargo test -p aerostore_core --release --test wal_crash_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_writer_lifecycle -- --test-threads=1
```

Long-running MVCC GC stress (ignored by default):

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### Benchmarks and Performance Gates

ProcArray criterion benchmark:

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

WAL ring throughput/backpressure gates:

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1
```

Includes:

- sync vs async throughput gate (`>= 10x`)
- keyed upsert throughput gate (`>= 10x`)
- savepoint churn throughput gate (`>= 10x`)
- logical async vs sync throughput gate (`>= 10x`)
- backpressure integrity checks

Checkpoint and logical replay benchmarks:

```bash
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --nocapture --test-threads=1
```

Includes:

- OCC checkpoint latency at `10k/100k/1M` rows
- logical snapshot + replay recovery benchmark at `10k/100k/1M` rows

Query/index planner benchmark suite:

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

Shared-memory secondary index benchmark suite:

```bash
cargo test -p aerostore_core --release --test shm_index_benchmark -- --nocapture --test-threads=1
```

Includes hard performance gates:

- `Eq` lookup vs table scan (`>= 3x`)
- range lookup vs table scan (`>= 2x`)
- forked multi-process contention throughput (`>= 20%` of single-process baseline)

## Reliability Coverage Matrix

- `tests/test_concurrency.rs`
  - Loom CAS model checks
  - 100,000-update GC stress with drop-tracker leak accounting
- `tests/mvcc_tokio_concurrency.rs`
  - 50-reader/50-writer visibility guard
- `tests/procarray_concurrency.rs`
  - threaded and forked slot stress
  - OCC conflict and ring backpressure slot release checks
  - savepoint rollback churn under contention
- `tests/occ_write_skew.rs`
  - classic write skew serializable-failure proof
  - planner-read SSI coverage
- `tests/occ_savepoint_reclaim.rs`
  - reclaimed write-intent reuse
  - nested savepoint correctness
  - rolled-back intents excluded from logical replay durability
- `tests/wal_crash_recovery.rs`
  - OCC replay into fresh shared-memory tables
  - checkpoint + WAL tail recovery
  - daemon crash/restart replayability
  - malformed/truncated WAL handling
  - replay idempotency and throughput checks
  - large-cardinality index parity (`Eq`/`Gt`/`Lt`) vs table scan after recovery
- `tests/logical_recovery.rs`
  - hard-exit crash recovery from `snapshot.dat` + `aerostore_logical.wal`
  - mixed upsert/update/delete replay correctness
  - strict malformed logical WAL rejection
  - commit-ack durability stress
  - logical WAL daemon kill/restart recovery
  - forked writer-process crash and parent recovery
- `tests/wal_writer_lifecycle.rs`
  - parent-death daemon lifecycle guard
- `tests/wal_ring_benchmark.rs`
  - all sync-vs-async throughput gates and backpressure integrity
- `tests/occ_checkpoint_benchmark.rs`
  - checkpoint and logical replay cardinality benchmarks (10k/100k/1M)
- `tests/shm_shared_memory.rs`
  - forked CAS correctness
  - forked ring producer integrity
  - shared free-list stress
- `tests/shm_fork.rs`
  - parent/child `RelPtr` CAS mutation sanity
- `tests/shm_index_fork.rs`
  - parent-owned shared secondary index with concurrent child inserts and parent traversal visibility proof
- `tests/shm_index_contention.rs`
  - cross-process same-key duplicate insert/remove contention with exact posting-set verification
- `tests/shm_index_gc_horizon.rs`
  - retired-node reclamation blocked by active snapshot horizon and released after transaction end
- `tests/shm_index_bounds.rs`
  - oversized key and oversized row-id payload rejection without index mutation
- `tests/shm_index_benchmark.rs`
  - strict shared-index performance gates for Eq/range/forked contention paths
- `tests/shm_benchmark.rs`
  - forked range scan timing
- `tests/query_index_benchmark.rs`
  - typed query vs STAPI planner performance paths (shared secondary index route)
- `tests/ingest_watch_ttl.rs`
  - TSV ingest, watch notifications, TTL sweeper behavior
- `tests/speedtable_arena_perf.rs`
  - arena-oriented performance path checks
- `aerostore_tcl/tests/search_stapi_bridge.rs`
  - Tcl search option bridge and STAPI planner integration
- `aerostore_tcl/tests/config_checkpoint_integration.rs`
  - Tcl config toggles, checkpointing, and recovery transitions

## Current Status

- Shared-memory OCC + ProcArray + savepoints are integrated and tested.
- SecondaryIndex is now fully shared-memory-backed and cross-process visible under `fork()`.
- Logical WAL + snapshot recovery is integrated at the core level with strict corruption detection.
- Tcl bridge is integrated with OCC ring durability path and planner execution.
- Legacy V1 durable path remains available for compatibility/testing.

## License

No license file is present yet. Treat this repository as proprietary/internal unless a license is explicitly added.
