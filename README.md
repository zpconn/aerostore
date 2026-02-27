# Aerostore

Aerostore is a Rust-first, lock-free database prototype aimed at replacing Hyperfeed's legacy PostgreSQL state-store for high-ingest, high-concurrency flight data workloads.

As of February 27, 2026, the repository contains a working hybrid architecture:

- V1 in-process MVCC + durable WAL/checkpoint/recovery path.
- V2 shared-memory (`mmap`) foundation for fork-heavy deployments.
- PostgreSQL-style bounded `ProcArray` snapshot tracking.
- Serializable OCC/SSI with savepoints and rollback-to-savepoint.
- Lock-free SkipList secondary indexes + typed query planner.
- Decoupled WAL ring primitives with optional async commit (`synchronous_commit=off` behavior).
- Tcl `cdylib` bridge for in-process query and ingest calls.

## Workspace Layout

- `aerostore_core/`: core engine, durability layers, tests, and benches.
- `aerostore_macros/`: `#[speedtable]` schema macro crate.
- `aerostore_tcl/`: Tcl extension crate (`cdylib`) and integration demo script.

## Current Module Inventory

`aerostore_core/src` currently exports:

- `arena.rs`: lock-free arena and version-chain table primitives.
- `txn.rs`: transaction manager and snapshot data model (V1 MVCC path).
- `mvcc.rs`: MVCC row visibility + version-chain operations.
- `index.rs`: secondary index abstraction backed by `crossbeam_skiplist::SkipMap`.
- `query.rs`: typed query builder, planner, and executor.
- `wal.rs`: append-only WAL, group commit worker, checkpointing, recovery state machine.
- `ingest.rs`: low-allocation TSV decode + bulk upsert.
- `watch.rs`: commit-stream pub/sub and TTL sweeper.
- `shm.rs`: shared memory arena (`mmap`) + relative pointers (`RelPtr<T>`).
- `procarray.rs`: bounded 256-slot active transaction array.
- `occ.rs`: shared-memory optimistic concurrency control (SSI validation + savepoints).
- `wal_ring.rs`: lock-free shared-memory MPSC WAL ring + `rkyv` serialization helpers.
- `wal_writer.rs`: forked WAL writer daemon + sync/async commit dispatcher.

## Architecture Overview

### 1. Schema + Arena Primitives

- `#[speedtable]` macro injects hidden system columns (`_nullmask`, `_xmin`, `_xmax`) into rows.
- `ChunkedArena<T>` and `Table<K, V>` provide lock-free allocation and version chaining for the baseline path.

Primary code:

- `aerostore_macros/src/lib.rs`
- `aerostore_core/src/arena.rs`

### 2. MVCC and Visibility (PostgreSQL-Inspired)

- `TransactionManager` issues monotonic txids and tracks active transactions.
- `MvccTable` keeps version chains with atomic `_xmax`.
- `is_visible(...)` enforces snapshot visibility.
- Old versions are reclaimed via `crossbeam-epoch`.

Primary code:

- `aerostore_core/src/txn.rs`
- `aerostore_core/src/mvcc.rs`

### 3. Shared Memory Foundation for `fork()`

- `ShmArena` uses `mmap(MAP_SHARED | MAP_ANONYMOUS)`.
- `RelPtr<T>` stores 32-bit offsets rather than absolute pointers.
- Shared bump allocator returns `RelPtr<T>` so parent/child mappings remain valid even with different virtual base addresses.

Primary code:

- `aerostore_core/src/shm.rs`

### 4. Bounded ProcArray (O(1) Snapshot Cost)

- Fixed active transaction table: `PROCARRAY_SLOTS = 256`.
- Each slot is cache-line aligned (`#[repr(align(64))]`) to reduce false sharing.
- Begin/end transaction use CAS claim/release over fixed slots.
- Snapshot creation scans only 256 slots (bounded constant cost, no unbounded linked list).

Primary code:

- `aerostore_core/src/procarray.rs`
- `aerostore_core/benches/procarray_snapshot.rs`

### 5. Serializable OCC/SSI with Savepoints

- `OccTransaction` keeps process-local `ReadSet` and `WriteSet`.
- `savepoint(name)` records current write-set length.
- `rollback_to(name)` truncates pending writes to that savepoint boundary.
- `commit()` performs SSI validation under a short shared-memory spinlock.
- Validation conflicts return `Error::SerializationFailure`.
- Successful commits atomically publish staged versions into shared indexes.

Primary code:

- `aerostore_core/src/occ.rs`

### 6. Indexing and Query Planner

- Secondary indexes use lock-free SkipLists.
- `QueryBuilder<T>` supports `eq`, `gt`, `lt`, `in_values`, sort, limit, and offset.
- Planner selects indexed candidate path when possible, otherwise falls back to sequential scan.
- All query paths pass rows through MVCC visibility checks before returning results.

Primary code:

- `aerostore_core/src/index.rs`
- `aerostore_core/src/query.rs`

### 7. Durability Paths

Aerostore currently has two durability-related stacks:

- `wal.rs` path (integrated with `DurableDatabase`):
  - framed append-only WAL (`wal.log`) via `bincode`
  - group-commit writer task and commit acks
  - periodic checkpoints (`checkpoint.dat`)
  - restart recovery: checkpoint load then WAL replay
- `wal_ring.rs` + `wal_writer.rs` path (shared-memory OCC-oriented primitives):
  - lock-free MPSC ring in shared memory
  - `rkyv` commit frame serialization
  - dedicated daemon process drains ring and `fdatasync`s periodically
  - `OccCommitter` supports synchronous and asynchronous commit modes

Primary code:

- `aerostore_core/src/wal.rs`
- `aerostore_core/src/wal_ring.rs`
- `aerostore_core/src/wal_writer.rs`

### 8. Ingestion, Event Streaming, and TTL

- TSV parser is optimized with `memchr` and typed decoders.
- Bulk upsert writes are batched.
- `TableWatch` emits row changes to subscribers through `tokio::sync::broadcast`.
- TTL sweeper prunes rows older than a configured threshold.

Primary code:

- `aerostore_core/src/ingest.rs`
- `aerostore_core/src/watch.rs`

### 9. Tcl Extension Bridge

- `aerostore_tcl` builds as a `cdylib` and exports `Aerostore_Init`/`Aerostore_SafeInit`.
- Database/runtime are shared across interpreters via `OnceLock`.
- Tcl command surface currently includes:
  - `aerostore::init ?data_dir?`
  - `FlightState ingest_tsv ...`
  - `FlightState search ...`

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

## Test and Benchmark Runbook

### Full release regression

```bash
cargo test --workspace --release -- --test-threads=1
```

### Long-running lock-free GC stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

### ProcArray constant-time proof benchmark

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```

This benchmark asserts:

- snapshot creation remains under `50ns` at low and high txid baselines
- snapshot time delta remains near-constant (`txid=10` vs `txid=10_000_000`)

### WAL ring throughput benchmark (`sync` vs `async`)

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark benchmark_async_synchronous_commit_modes -- --exact --nocapture --test-threads=1
```

Benchmark target:

- asynchronous mode (`synchronous_commit=off`) must be at least `10x` faster than synchronous mode

### Additional performance-oriented suites

```bash
cargo test -p aerostore_core --release --test speedtable_arena_perf -- --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --nocapture
```

## Reliability and Concurrency Test Coverage

- `tests/test_concurrency.rs`
  - loom model-check for CAS ordering and pointer publication
  - Hyperfeed GC stress: 100,000 updates + slow readers + drop-accounting leak proof
- `tests/mvcc_tokio_concurrency.rs`
  - 50 reader transactions vs 50 writer transactions, no partial-update visibility
- `tests/procarray_concurrency.rs`
  - threaded/forked begin-end stress
  - OCC conflict pressure with WAL ring backpressure while ensuring slots release
- `tests/occ_write_skew.rs`
  - classic serializable write-skew anomaly
  - verifies one commit and one `SerializationFailure`
  - runs in both synchronous and asynchronous commit modes
- `tests/wal_crash_recovery.rs`
  - checkpoint + WAL replay restoration
  - async WAL daemon crash/restart replayability test
- `tests/wal_ring_benchmark.rs`
  - sync vs async commit throughput gate
  - producer backpressure correctness and message integrity checks
- `tests/shm_shared_memory.rs`
  - forked CAS correctness
  - forked multi-producer ring integrity (checksum/xor/count validation)

## Tcl Demo and Query Syntax

Run demo:

```bash
tclsh aerostore_tcl/test.tcl
```

The demo attempts `package require aerostore`, falls back to loading the built shared object, initializes a DB, ingests TSV rows, and runs a filtered/sorted search.

Example search:

```tcl
set count [FlightState search -compare {{> altitude 10000}} -sort lat -limit 50]
```

Supported search filters:

- `=`, `==`, `>`, `<`, `in`

Supported options:

- `-compare`
- `-sort`
- `-asc` or `-desc`
- `-limit`
- `-offset`

Field aliases:

- `flight_id`, `flight`, `ident`
- `altitude`, `alt`
- `lat`
- `lon`, `long`, `longitude`
- `gs`, `groundspeed`
- `updated_at`, `updated`, `ts`

## Durable Files on Disk

For `DurableDatabase` data directories:

- `wal.log`: framed append-only WAL segments.
- `checkpoint.dat`: compact checkpoint state.

Recovery order:

1. load `checkpoint.dat`
2. replay remaining WAL frames
3. resume normal service

## Status and Known Integration Boundaries

- `DurableDatabase` currently runs on the V1 in-process MVCC stack.
- Shared-memory OCC + WAL ring components are implemented and tested, but not yet fully wired into the Tcl-facing durable path.
- `rollback_to(savepoint)` truncates local pending writes; abandoned shared-memory allocations are logically discarded (not immediately compacted).
- `aerostore_tcl/test.tcl` includes optional hooks for future config/retry-loop commands and safely skips them if not exported yet.

## License

No license file has been added yet. Treat this repository as proprietary/internal unless a license is explicitly published.
