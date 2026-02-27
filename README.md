# Aerostore

<img width="500" height="500" alt="ChatGPT Image Feb 26, 2026, 06_15_42 PM" src="https://github.com/user-attachments/assets/3573ab8f-45b9-481c-bdcf-cb1186d73df0" />

Aerostore is a Rust prototype database for high-concurrency flight/market workloads.

It currently includes:

- Lock-free row version storage and query paths.
- MVCC snapshot visibility semantics (PostgreSQL-inspired).
- Shared-memory foundations for fork-heavy deployments (`mmap`, relative pointers).
- Bounded ProcArray transaction tracking (256 slots, cache-line aligned).
- A standalone optimistic concurrency (OCC/SSI) layer with savepoints.
- Skip-list secondary indexing and typed query builder.
- WAL + group commit + checkpoint + recovery.
- TSV bulk upsert ingestion, pub-sub change streams, and TTL sweeper.
- Tcl `cdylib` bridge for in-process Tcl access.

## Current Status (as of February 27, 2026)

Implemented and passing in release mode:

- `aerostore_core` modules: `arena`, `mvcc`, `txn`, `occ`, `procarray`, `index`, `query`, `shm`, `wal`, `ingest`, `watch`.
- `aerostore_macros` `#[speedtable]` macro.
- `aerostore_tcl` shared library bridge.
- Full `aerostore_core` release test suite (including OCC write-skew test).
- ProcArray Criterion benchmark with strict `<50ns` proof check.

## Workspace Layout

- `aerostore_core/`: core engine + tests/benches.
- `aerostore_macros/`: proc-macro crate.
- `aerostore_tcl/`: Tcl extension crate (`cdylib`) and `test.tcl` demo.

## Architecture

### 1) Speedtable Macro + Arena Primitives

- `#[speedtable]` injects hidden `_nullmask`, `_xmin`, `_xmax` fields.
- `ChunkedArena<T>` and `Table<K, V>` provide lock-free allocation/chaining primitives.

Key files:

- `aerostore_macros/src/lib.rs`
- `aerostore_core/src/arena.rs`
- `aerostore_core/tests/speedtable_arena_perf.rs`

### 2) MVCC (V1 In-Process Path)

- `TransactionManager` provides txid assignment and active-transaction tracking.
- `MvccTable` stores row version chains with `_xmin` and atomic `_xmax`.
- `is_visible(...)` controls snapshot visibility.
- Old versions are reclaimed with `crossbeam-epoch`.

Key files:

- `aerostore_core/src/txn.rs`
- `aerostore_core/src/mvcc.rs`
- `aerostore_core/tests/mvcc_tokio_concurrency.rs`

### 3) Shared Memory + Relative Pointers (V2 Foundation)

- `ShmArena` uses `mmap(MAP_SHARED | MAP_ANONYMOUS)`.
- `RelPtr<T>` stores 32-bit offsets (not absolute pointers).
- Shared-memory bump allocator returns `RelPtr<T>` for fork-safe access.

Key files:

- `aerostore_core/src/shm.rs`
- `aerostore_core/tests/shm_fork.rs`
- `aerostore_core/tests/shm_shared_memory.rs`

### 4) Bounded ProcArray (PostgreSQL-Style)

- Fixed `PROCARRAY_SLOTS = 256`.
- `#[repr(align(64))]` slots to reduce false sharing.
- CAS claim/release for begin/end transaction.
- Snapshot creation scans fixed 256 slots with no heap allocation.

Key files:

- `aerostore_core/src/procarray.rs`
- `aerostore_core/tests/procarray_concurrency.rs`
- `aerostore_core/benches/procarray_snapshot.rs`

### 5) OCC / SSI Layer with Savepoints

- New `OccTable<T>` + `OccTransaction<T>` in shared memory.
- Process-local transaction state:
  - `ReadSet`: `RelPtr<OccRow<T>>` + observed `xmin`.
  - `WriteSet`: pending row mutations allocated in shared arena but not globally linked until commit.
- Nested savepoints:
  - `savepoint(name)` stores write-set length.
  - `rollback_to(name)` truncates pending writes back to that point.
- Commit does SSI-style validation under a shared-memory spinlock:
  - Validates read-set rows for dangerous updates/deletes after snapshot start.
  - On conflict: aborts and returns `Error::SerializationFailure`.
  - On success: atomically publishes write pointers into global row heads.

Key files:

- `aerostore_core/src/occ.rs`
- `aerostore_core/tests/occ_write_skew.rs`

### 6) Index + Query Engine

- Secondary indexes via `crossbeam_skiplist::SkipMap`.
- Typed `QueryBuilder` supports `eq`, `gt`, `lt`, `in_values`, sorting, limits, offsets.
- Uses indexed candidate selection when possible, otherwise sequential candidate scan.
- Final visibility/filtering performed before returning rows.

Key files:

- `aerostore_core/src/index.rs`
- `aerostore_core/src/query.rs`
- `aerostore_core/tests/query_index_benchmark.rs`

### 7) Durability (WAL + Group Commit + Checkpoint + Recovery)

- WAL records serialized with `bincode`.
- Group commit via background async WAL writer and batched fsync-like `sync_data`.
- Checkpointer writes `checkpoint.dat` and truncates WAL.
- Recovery loads checkpoint then replays remaining WAL records.

Key files:

- `aerostore_core/src/wal.rs`
- `aerostore_core/tests/wal_crash_recovery.rs`

### 8) Ingest / Streaming / TTL

- TSV parser uses `memchr` and typed column decoders.
- Bulk upsert commits batched durable transactions.
- `TableWatch` publishes commit changes via `tokio::sync::broadcast`.
- Background TTL sweeper prunes expired rows.

Key files:

- `aerostore_core/src/ingest.rs`
- `aerostore_core/src/watch.rs`
- `aerostore_core/tests/ingest_watch_ttl.rs`

### 9) Tcl Bridge

- `aerostore_tcl` exports `Aerostore_Init` / `Aerostore_SafeInit`.
- Global `OnceLock` state shares a single DB/runtime across interpreters.
- Tcl numeric values are parsed using Tcl native numeric APIs.

Key files:

- `aerostore_tcl/src/lib.rs`
- `aerostore_tcl/test.tcl`

## Build Prerequisites

### Rust

- Stable Rust toolchain (`cargo`, `rustc`).

### Tcl bridge (optional)

- Tcl 8.6 runtime/dev packages.
- `clang` and `libclang`.

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Build

```bash
cargo build --workspace
```

Tcl extension only:

```bash
cargo build -p aerostore_tcl
```

## Test + Benchmark Runbook

### Full release regression

```bash
cargo test --workspace --release
```

### Long GC stress (ignored by default)

```bash
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --nocapture
```

### OCC write-skew serialization test

```bash
cargo test -p aerostore_core --release --test occ_write_skew -- --nocapture
```

### ProcArray benchmark (strict)

```bash
cargo bench -p aerostore_core --bench procarray_snapshot -- --nocapture
```

This benchmark enforces:

- `< 50ns` snapshot budget for low and high txid baselines.
- Near-constant-time behavior (`txid=10` vs `txid=10_000_000`).

Bench profile is tuned in workspace config (`[profile.bench] lto="thin", codegen-units=1`).

### Other perf-oriented targets

```bash
cargo test -p aerostore_core --release --test speedtable_arena_perf -- --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --nocapture
```

Optional worker override for arena benchmark:

```bash
AEROSTORE_BENCH_WORKERS=8 cargo test -p aerostore_core --release --test speedtable_arena_perf -- --nocapture
```

## Reliability/Concurrency Test Inventory

- `aerostore_core/tests/test_concurrency.rs`
  - Loom model check for CAS ordering/pointer publication.
  - 100k-update GC stress test with drop-accounting proof.
- `aerostore_core/tests/procarray_concurrency.rs`
  - Threaded + forked begin/end/snapshot stress for bounded ProcArray.
- `aerostore_core/tests/occ_write_skew.rs`
  - Classic write-skew simulation; verifies one commit and one `SerializationFailure`.

## Tcl Usage

Run demo:

```bash
tclsh aerostore_tcl/test.tcl
```

The demo:

1. Attempts `package require aerostore`.
2. Falls back to loading `target/debug/libaerostore_tcl.so`.
3. Initializes DB.
4. Ingests sample TSV.
5. Runs a filtered search.

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

Supported options:

- `-compare` operators: `=`, `==`, `>`, `<`, `in`
- `-sort <field>`
- `-asc` / `-desc`
- `-limit <n>`
- `-offset <n>`

Supported field aliases:

- `flight_id|flight|ident`
- `altitude|alt`
- `lat`
- `lon|long|longitude`
- `gs|groundspeed`
- `updated_at|updated|ts`

## Durable Files

For `DurableDatabase` directories:

- `wal.log`: framed append-only transaction log.
- `checkpoint.dat`: compact checkpoint snapshot.

Startup recovery order:

1. Load `checkpoint.dat`.
2. Replay remaining WAL records.
3. Continue normal service.

## Caveats / Integration Notes

- OCC (`occ.rs`) is currently a standalone shared-memory concurrency layer and is not yet wired into the WAL/recovery/query/Tcl durable path.
- `rollback_to` truncates local pending writes; shared-memory bump allocations are abandoned logically (not physically reclaimed immediately).
- `DurableDatabase` still uses the V1 in-process `TransactionManager`/MVCC path.
- Tcl demo fallback currently assumes Linux `.so` path (`target/debug/libaerostore_tcl.so`).
