# Aerostore

<img width="500" height="500" alt="ChatGPT Image Feb 26, 2026, 06_15_42 PM" src="https://github.com/user-attachments/assets/3573ab8f-45b9-481c-bdcf-cb1186d73df0" />

Aerostore is a lock-free, memory-resident Rust database prototype for high-concurrency flight/market workloads.

It combines:

- MVCC row versioning with snapshot visibility rules inspired by PostgreSQL.
- Skip-list secondary indexes for fast point/range lookup.
- Durability primitives: append-only WAL, group commit, periodic checkpoint, and crash recovery.
- High-throughput TSV bulk upsert ingestion.
- Change streaming (pub-sub) and TTL pruning.
- A Tcl `cdylib` bridge so Hyperfeed-style Tcl interpreters can query Rust state in-process.
- POSIX shared-memory primitives (`mmap`, relative pointers, fork-safe CAS) for Aerostore V2.

## Current Repo State (February 27, 2026)

What is implemented and passing today:

- `aerostore_core` engine modules (`arena`, `mvcc`, `txn`, `index`, `query`, `wal`, `ingest`, `watch`, `shm`, `procarray`).
- `aerostore_macros` `#[speedtable]` proc macro.
- `aerostore_tcl` shared library bridge (`cdylib`) with Tcl commands.
- Release-mode workspace test suite passing.
- Long-running ignored GC stress test passing.
- Criterion ProcArray snapshot benchmark passing strict `<50ns` guard on this machine.

## Workspace Layout

- `aerostore_core/`: core engine, durability, shared memory, tests, benchmarks.
- `aerostore_macros/`: `#[speedtable]` proc macro.
- `aerostore_tcl/`: Tcl extension crate (`cdylib`) and demo script.

## Architecture Overview

### 1) Speedtable Macro + Lock-Free Arena

- `#[speedtable]` injects hidden system columns (`_nullmask`, `_xmin`, `_xmax`) and null-bit helpers.
- `ChunkedArena<T>` and `Table<K, V>` provide lock-free allocation and per-key version chains.

Key files:

- `aerostore_macros/src/lib.rs`
- `aerostore_core/src/arena.rs`
- `aerostore_core/tests/speedtable_arena_perf.rs`

### 2) MVCC Engine (V1 Path)

- `TransactionManager` currently provides monotonic txids and in-flight tracking for the in-process durable path.
- `MvccTable` keeps row version chains with `_xmin`/`_xmax` and compare-and-swap update/delete semantics.
- `is_visible(...)` enforces snapshot visibility during reads and query execution.
- Reclamation uses `crossbeam-epoch` deferred destruction.

Key files:

- `aerostore_core/src/txn.rs`
- `aerostore_core/src/mvcc.rs`
- `aerostore_core/tests/mvcc_tokio_concurrency.rs`

### 3) Shared-Memory Foundation (V2 Path)

- `ShmArena` allocates process-shared memory via `mmap(MAP_SHARED | MAP_ANONYMOUS)`.
- `RelPtr<T>` stores 32-bit relative offsets instead of absolute pointers.
- Shared-memory `ChunkedArena` is a lock-free bump allocator returning only `RelPtr<T>`.
- `ShmHeader` contains global txid and a bounded ProcArray.

Key files:

- `aerostore_core/src/shm.rs`
- `aerostore_core/tests/shm_fork.rs`
- `aerostore_core/tests/shm_shared_memory.rs`

### 4) Bounded ProcArray (PostgreSQL-Style Snapshot Tracking)

- Fixed-size `ProcArray` of 256 slots (`PROCARRAY_SLOTS = 256`).
- Each slot is cache-line aligned (`#[repr(align(64))]`) to reduce false sharing.
- `begin_transaction`: linear CAS claim of a `0` slot with a unique txid.
- `end_transaction`: release slot back to `0` with release ordering.
- `create_snapshot`: bounded scan over 256 slots, zero heap allocation, returns `(xmin, xmax, in_flight[])`.

Key files:

- `aerostore_core/src/procarray.rs`
- `aerostore_core/tests/procarray_concurrency.rs`
- `aerostore_core/benches/procarray_snapshot.rs`

### 5) Secondary Index + Query Optimizer

- Secondary indexes use `crossbeam_skiplist::SkipMap<IndexValue, SkipSet<RowId>>`.
- Query filters support `eq`, `gt`, `lt`, `in_values`, plus `sort_by`, `limit`, and `offset`.
- Optimizer uses an index when a filter maps to an indexed field; otherwise parallel-style sequential candidate collection.
- Results always flow through MVCC visibility before returning rows.

Key files:

- `aerostore_core/src/index.rs`
- `aerostore_core/src/query.rs`
- `aerostore_core/tests/query_index_benchmark.rs`

### 6) Durability: WAL + Group Commit + Checkpoint + Recovery

- WAL records are framed and serialized via `bincode`.
- Group commit is handled by a background WAL writer task that batches commits and calls `sync_data`.
- Commits return success only after WAL fsync-like durability step completes.
- Checkpointer periodically snapshots visible rows to `checkpoint.dat` and truncates WAL.
- Startup recovery loads checkpoint then replays remaining WAL records.

Key files:

- `aerostore_core/src/wal.rs`
- `aerostore_core/tests/wal_crash_recovery.rs`

### 7) TSV Ingest + Change Streaming + TTL

- TSV parser uses `memchr` for delimiter scanning and typed decode helpers.
- `bulk_upsert_tsv(...)` performs batched insert/update in durable transactions.
- `TableWatch` publishes row-level change events via `tokio::sync::broadcast`.
- TTL sweeper prunes expired rows based on an `updated_at` extractor.

Key files:

- `aerostore_core/src/ingest.rs`
- `aerostore_core/src/watch.rs`
- `aerostore_core/tests/ingest_watch_ttl.rs`

### 8) Tcl FFI Bridge

- `aerostore_tcl` builds as a `cdylib` and exports `Aerostore_Init` / `Aerostore_SafeInit`.
- Uses process-global `OnceLock` DB/runtime so multiple interpreters share one memory-resident engine.
- Tcl numeric argument conversion uses Tcl integer/double APIs (`Tcl_GetWideIntFromObj`, `Tcl_GetDoubleFromObj`) to avoid string-only paths.
- `FlightState search` returns the number of matching rows.

Key files:

- `aerostore_tcl/src/lib.rs`
- `aerostore_tcl/test.tcl`

## Prerequisites

### Rust

- Stable Rust toolchain (`cargo`, `rustc`).

### Tcl Extension Build (optional)

- Tcl 8.6 runtime/dev packages.
- `clang` and `libclang` (needed by the Tcl C binding toolchain).

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

Build everything:

```bash
cargo build --workspace
```

Build Tcl extension only:

```bash
cargo build -p aerostore_tcl
```

## Test and Benchmark Runbook

### Fast Full Regression (release)

```bash
cargo test --workspace --release
```

### Long GC Stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --nocapture
```

### ProcArray Criterion Benchmark (strict)

```bash
cargo bench -p aerostore_core --bench procarray_snapshot -- --nocapture
```

This benchmark asserts:

- snapshot average `< 50ns` for `txid=10` and `txid=10_000_000`
- near-constant-time behavior between low/high txid ranges

Note: workspace bench profile is tuned for reproducible perf (`lto = "thin"`, `codegen-units = 1`).

### Other Performance-Focused Targets

```bash
cargo test -p aerostore_core --release --test speedtable_arena_perf -- --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --nocapture
```

Optional worker control for arena allocation test:

```bash
AEROSTORE_BENCH_WORKERS=8 cargo test -p aerostore_core --release --test speedtable_arena_perf -- --nocapture
```

## Concurrency and Reliability Tests

`aerostore_core/tests/test_concurrency.rs`:

- `loom_validates_cas_update_and_index_pointer_swaps`
  - Model-checks interleavings around CAS update/index publication.
  - Guards against memory-ordering regressions and ABA/cycle issues in modeled chains.
- `hyperfeed_gc_stress_reclaims_dead_versions_without_memory_leaks` (ignored by default)
  - Writer updates one row 100,000 times.
  - Slow readers hold snapshots concurrently.
  - Asserts exactly `99,999` dead versions reclaimed.
  - Uses custom drop tracking to prove no leak in reclaimed generations.

`aerostore_core/tests/procarray_concurrency.rs`:

- Multi-thread begin/end/snapshot stress over bounded 256-slot ProcArray.
- Forked-process stress ensures slots are released correctly across child processes.

## Tcl Usage

Run the demo script:

```bash
tclsh aerostore_tcl/test.tcl
```

The script:

- loads the extension (`package require aerostore`, with fallback direct `.so` load)
- initializes DB state directory
- ingests TSV rows
- runs a filtered search

### Tcl Commands

Initialize:

```tcl
aerostore::init ?data_dir?
```

Ingest TSV:

```tcl
FlightState ingest_tsv <tsv_data> ?batch_size?
```

Search:

```tcl
FlightState search \
  -compare {{> altitude 10000} {in flight {UAL123 DAL789}}} \
  -sort lat \
  -asc \
  -limit 50 \
  -offset 0
```

Supported search options:

- `-compare` with operators `=`, `==`, `>`, `<`, `in`
- `-sort <field>`
- `-asc` or `-desc`
- `-limit <n>`
- `-offset <n>`

Fields: `flight_id|flight|ident`, `altitude|alt`, `lat`, `lon|long|longitude`, `gs|groundspeed`, `updated_at|updated|ts`.

## Durable On-Disk Artifacts

For `DurableDatabase` data directories:

- `wal.log`: append-only framed transaction records.
- `checkpoint.dat`: checkpoint snapshot written by checkpointer/manual checkpoint.

Recovery order on startup:

1. Load checkpoint.
2. Replay remaining WAL records.
3. Resume normal operation.

## Known Caveats

- The legacy `TransactionManager` path used by `DurableDatabase` is still the V1 in-process tracker; ProcArray currently lives in the shared-memory V2 path (`shm`/`procarray`) and is tested/benchmarked there.
- Tcl demo fallback currently loads `target/debug/libaerostore_tcl.so`; adjust path for non-Linux or release artifacts.
