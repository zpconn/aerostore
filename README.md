# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first, shared-memory database prototype for high-ingest flight/state workloads.
It combines lock-free shared-memory data structures, PostgreSQL-inspired MVCC/OCC semantics, and Tcl in-process integration.

## What This Repository Contains

- V1 in-process MVCC + WAL/checkpoint durability path.
- V2 shared-memory OCC/SSI engine with ProcArray snapshots, savepoints, and lock striping.
- V2 shared-memory, cross-process secondary indexes that survive `fork()` visibility requirements.
- V2 shared-memory WAL ring + WAL writer daemon (`synchronous_commit` on/off behavior).
- V3 logical WAL + snapshot durability and deterministic crash recovery.
- Tcl extension bridge (`cdylib`) that maps STAPI-style search options to Rust planner execution.

<<<<<<< HEAD
## System Overview (Plain Language)

Aerostore now at its core includes a lock-free cross-process shared-memory secondary index implementation supporting fast queries across forks.

1. Most mutable state is in shared memory so sibling worker processes can see the same database state.
2. Writers publish new row versions and mark old versions obsolete instead of mutating in place.
3. Readers run against snapshots and do not block writers.
4. Commit uses serializable OCC/SSI validation; conflicts return `SerializationFailure` for retry loops.
5. Durability uses WAL + checkpoints/snapshots.
6. Startup recovery rebuilds active state by replaying persisted data into fresh shared memory.
7. Tcl commands execute in-process against the same shared engine to avoid network/IPC overhead.

## Query Support and Why It Is Fast

Supported filtering operators:

- `=` exact match
- `>` and `<` range predicates
- `in field {a b c}` membership
- `match field pattern` glob-style matching

Execution controls:

- `-sort <field>`
- `-limit <n>`
- `-offset <n>`
- ascending/descending order

Why it is fast:

- Indexed predicates route to shared-memory secondary-index scans first.
- Unindexed predicates fall back to sequential scans.
- Candidate rows are filtered through MVCC/OCC visibility before return.
- Residual filters are applied after candidate narrowing.
- `-limit` allows early stop behavior.
- Tcl path stays in-process and uses compiled Rust query plans.

## Shared-Memory Indexing Model

Aerostore includes a shared-memory secondary indexing design for fork-heavy runtimes:

1. Index structures live in POSIX shared memory, not per-process heap memory.
2. Links are relative offsets (`RelPtr` / `AtomicU32`) rather than process-local absolute pointers.
3. Writers use CAS to insert/unlink nodes and posting entries.
4. Deletes are logical first; physical reclamation is deferred.
5. Reclamation waits on ProcArray snapshot horizon safety.
6. Updates by one process are visible to sibling processes mapped to the same segment.

## Workspace Layout

- `aerostore_core/`: core engine, durability, parser/planner, tests, benchmarks.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl `cdylib` bridge and Tcl integration tests.
- `docs/`: operational documentation (including nightly performance runbook).

## Core Module Inventory

`aerostore_core/src` currently exports:

- `arena.rs`: V1 arena and version-chain primitives.
- `txn.rs`: V1 transaction manager and snapshot model.
- `mvcc.rs`: V1 MVCC visibility helpers.
- `shm.rs`: shared-memory mapping + lock-free bump allocator + `RelPtr<T>`.
- `procarray.rs`: bounded 256-slot active-transaction array.
- `occ_partitioned.rs`: default OCC/SSI implementation with partitioned lock striping.
- `occ.rs`: default OCC re-export facade.
- `occ_legacy.rs`: legacy global-commit-lock OCC implementation kept for compatibility.
- `index.rs`: index compare/value abstraction and `SecondaryIndex` export.
- `shm_index.rs`: shared-memory lock-free secondary index and deferred GC horizon integration.
- `query.rs`: typed query builder/executor.
- `stapi_parser.rs`: STAPI parser (nom combinators).
- `planner.rs`: AST-to-plan compiler and index routing.
- `wal.rs`: V1 WAL/checkpoint/recovery path.
- `wal_ring.rs`: shared-memory MPSC WAL ring structures and encoding.
- `wal_writer.rs`: WAL writer daemon and OCC replay/checkpoint utilities.
- `wal_logical.rs`: logical WAL records and logical WAL daemon helpers.
- `recovery.rs`: logical snapshot + logical WAL replay database boot path.
- `ingest.rs`: high-speed TSV ingestion primitives.
- `watch.rs`: watch subscriptions and TTL sweeper.

## Architecture Summary

### 1) Shared Memory Foundation (`shm.rs`)

- POSIX shared mappings for cross-process state sharing.
- Relative pointers (`RelPtr<T>`) for mapping-base-independent addressing.
- Lock-free shared arena allocation via CAS bump pointer.

### 2) Transaction Tracking (`procarray.rs`)

- Fixed-size `PROCARRAY_SLOTS = 256` active TxID table.
- CAS claim/release and bounded snapshot scans.
- Snapshot work is bounded to slot count, not historical TxID magnitude.

### 3) Default OCC/SSI (`occ_partitioned.rs`)

- Process-local read/write sets and nested savepoints.
- Partitioned commit lock striping over 1024 shared lock buckets.
- Required buckets are derived from read/write row IDs, deduped, sorted, and acquired in ascending order.
- SSI validation checks read-set stability before publish.
- Conflicts return `SerializationFailure`.

### 4) Planner and Index Routing (`stapi_parser.rs`, `planner.rs`, `query.rs`, `shm_index.rs`)

- STAPI syntax compiles to typed execution plans.
- Indexable predicates (`=`, `>`, `<`) route to index-first scans.
- Residual predicates apply after candidate narrowing.
- Read tracking integrates with SSI validation.

### 5) Ingestion and Streaming (`ingest.rs`, `watch.rs`)

- TSV ingest path oriented for low allocation pressure.
- Watch subscriptions stream row-change events.
- TTL sweeper removes stale rows.

### 6) Durability Stacks

V1 durability (`wal.rs`):

- `wal.log` append path
- `checkpoint.dat` snapshots
- replay by checkpoint + WAL

OCC ring durability (`wal_ring.rs` + `wal_writer.rs`):

- shared MPSC ring producer path
- dedicated WAL writer daemon + fsync cadence
- `aerostore.wal` + `occ_checkpoint.dat` replay

Logical durability (`wal_logical.rs` + `recovery.rs`):

- logical `WalRecord` (`Upsert`, `Delete`, `Commit`)
- strict framed decode (`u32 len + payload`)
- corruption/truncation fail-fast parsing
- `snapshot.dat` + `aerostore_logical.wal` recovery into fresh shared memory

### 7) Tcl Bridge (`aerostore_tcl/src/lib.rs`)

- Exposes `Aerostore_Init` / `Aerostore_SafeInit`.
- Global in-process engine via `OnceLock`.
- Supports search, ingest, config toggles, and checkpoint entry points.
- Tcl search options map into Rust planner execution.

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

- Tcl 8.6 runtime and development headers
- `clang` and `libclang`

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

Build full workspace:

```bash
cargo build --workspace
```

Build Tcl bridge only:

```bash
cargo build -p aerostore_tcl
```

## Test and Benchmark Runbook

Release compile check:

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

WAL ring throughput/backpressure benchmarks:

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
```

Current WAL ring benchmark gates include:

- sync vs async throughput (`>= 10x`)
- keyed upsert throughput (`>= 10x`)
- savepoint churn throughput (`>= 10x`)
- logical async vs sync throughput (`>= 10x`)
- parallel disjoint-key producer throughput (`>= 3x`)
- backpressure integrity assertions

Partitioned OCC lock-striping contention benchmark (ignored by default, run explicitly):

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
```

This benchmark validates 16-process disjoint vs hot-key scaling with a `>= 12x` gate.

Checkpoint and replay benchmark suite:

```bash
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --nocapture --test-threads=1
```

Query/planner benchmark suite:

```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

Shared-memory index benchmark suite:

```bash
cargo test -p aerostore_core --release --test shm_index_benchmark -- --nocapture --test-threads=1
```

Additional nightly command set is documented in `docs/nightly_perf.md`.

## Reliability Coverage Matrix

- `tests/test_concurrency.rs`
  - Loom CAS model checks.
  - 100,000-update GC stress with drop-tracker leak accounting.
- `tests/mvcc_tokio_concurrency.rs`
  - 50-reader / 50-writer visibility safety.
- `tests/procarray_concurrency.rs`
  - thread/fork slot stress.
  - OCC conflict + ring backpressure slot release.
  - savepoint rollback churn.
  - disjoint-write vs hot-row conflict profile comparison.
- `tests/occ_write_skew.rs`
  - classic write-skew serializable-failure proof.
  - planner-read SSI coverage.
  - keyed upsert SSI coverage.
  - savepoint-heavy SSI coverage.
  - distinct lock-bucket write-skew serializable proof.
- `tests/occ_savepoint_reclaim.rs`
  - reclaimed write-intent reuse.
  - nested savepoint correctness.
  - rolled-back intent exclusion from logical replay durability.
- `tests/wal_crash_recovery.rs`
  - OCC replay into fresh shared-memory tables.
  - checkpoint + WAL tail recovery.
  - daemon crash/restart replayability.
  - malformed/truncated WAL handling.
  - replay idempotency.
  - large-cardinality index parity checks.
  - high-cardinality multi-round latest-value recovery verification.
- `tests/logical_recovery.rs`
  - hard-exit crash recovery from `snapshot.dat` + `aerostore_logical.wal`.
  - mixed upsert/update/delete replay correctness.
  - strict malformed logical WAL rejection.
  - commit-ack durability stress.
  - logical WAL daemon kill/restart recovery.
  - forked writer-process crash and parent recovery.
- `tests/wal_writer_lifecycle.rs`
  - parent-death daemon lifecycle guard.
- `tests/wal_ring_benchmark.rs`
  - sync/async durability throughput and backpressure gates.
  - parallel disjoint-key producer throughput gate.
- `tests/occ_partitioned_lock_striping_benchmark.rs`
  - 16-process hot-key vs disjoint-key lock striping gate (`>= 12x`).
- `tests/occ_checkpoint_benchmark.rs`
  - checkpoint and logical replay cardinality benchmarks.
- `tests/shm_shared_memory.rs`
  - forked CAS correctness.
  - forked ring producer integrity.
  - shared free-list stress.
- `tests/shm_fork.rs`
  - parent/child `RelPtr` CAS mutation sanity.
- `tests/shm_index_fork.rs`
  - cross-process shared index insertion visibility under `fork()`.
- `tests/shm_index_contention.rs`
  - cross-process same-key duplicate insert/remove contention exactness.
- `tests/shm_index_gc_horizon.rs`
  - retired-node reclamation horizon enforcement.
- `tests/shm_index_bounds.rs`
  - oversized key / row-id payload rejection.
- `tests/shm_index_benchmark.rs`
  - shared-index Eq/range/forked-contention performance gates.
- `tests/shm_benchmark.rs`
  - shared-memory/fork range scan performance checks.
- `tests/query_index_benchmark.rs`
  - typed query vs STAPI planner performance paths.
- `tests/ingest_watch_ttl.rs`
  - TSV ingest + watch + TTL sweeper behavior.
- `tests/speedtable_arena_perf.rs`
  - arena-oriented performance checks.
- `aerostore_tcl/tests/search_stapi_bridge.rs`
  - Tcl option bridge and STAPI planner integration.
- `aerostore_tcl/tests/config_checkpoint_integration.rs`
  - Tcl config toggles, checkpointing, and recovery transitions.

## Current Status

- Default OCC path uses partitioned lock striping in shared memory.
- Legacy global-lock OCC path remains available for compatibility testing (`occ_legacy.rs`).
- Shared-memory secondary index path is cross-process visible under `fork()`.
- Logical WAL + snapshot durability path is integrated with strict decode/validation.
- Tcl bridge uses the shared engine and planner path in-process.

## License

No license file is present yet. Treat this repository as proprietary/internal unless a license is explicitly added.
