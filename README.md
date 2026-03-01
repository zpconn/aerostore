# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype for high-ingest, update-heavy flight/market workloads.

The system is optimized around:
- shared-memory data structures with relative pointers,
- serializable OCC transaction validation,
- STAPI-style query parsing and rule-based planning,
- shared-memory primary and secondary indexes,
- WAL + checkpoint durability and crash recovery,
- a Tcl bridge for embedding in scripting workflows.

## Table of Contents
- [Status](#status)
- [Key Features](#key-features)
- [Delta-Encoded WAL](#delta-encoded-wal)
- [Architecture Overview](#architecture-overview)
- [Repository Layout](#repository-layout)
- [Build and Prerequisites](#build-and-prerequisites)
- [Quick Start](#quick-start)
- [Tcl Bridge Usage](#tcl-bridge-usage)
- [STAPI Query Model](#stapi-query-model)
- [Durability and Recovery Artifacts](#durability-and-recovery-artifacts)
- [Testing Runbook](#testing-runbook)
- [Benchmark Runbook](#benchmark-runbook)
- [Known Constraints](#known-constraints)
- [License](#license)

## Status
Aerostore is an actively evolving prototype with broad regression/benchmark coverage in `aerostore_core/tests` and `aerostore_tcl/tests`.

Current implementation focus:
- correctness and concurrency safety in shared memory,
- deterministic planner routing behavior,
- durability/recovery behavior under crash/restart scenarios,
- WAL write-amplification reduction for update-centric workloads.

Non-goals right now:
- SQL compatibility,
- distributed replication,
- production hardening features (auth, tenancy, RBAC).

## Key Features
- Shared-memory primitives:
  - chunked arena allocation,
  - relative pointers (`RelPtr<T>`),
  - cross-process/fork-safe pointer resolution.
- OCC engine:
  - snapshot-based reads,
  - serializable conflict checks,
  - savepoint/rollback support,
  - partitioned lock striping.
- Indexing:
  - `ShmPrimaryKeyMap` for PK routing,
  - lock-free skiplist-backed `SecondaryIndex`,
  - distinct-key cardinality tracking for planner route decisions.
- Query stack:
  - Tcl/STAPI parser,
  - rule-based optimizer,
  - candidate driver route + residual filtering.
- Durability:
  - shared WAL ring + writer daemon,
  - OCC checkpointing,
  - crash recovery from checkpoint + WAL tail.

## Delta-Encoded WAL
Aerostore now supports delta WAL records for update-heavy workloads.

### Record shape
- `WalRecord::UpdateDelta { pk: String, dirty_mask: u64, delta_bytes: Vec<u8> }`
- `WalRecord::UpdateFull { pk: String, payload: Vec<u8> }`

Key files:
- `aerostore_core/src/wal_delta.rs`
- `aerostore_core/src/recovery_delta.rs`
- `aerostore_core/src/wal_writer.rs`
- `aerostore_core/src/wal_ring.rs`
- `aerostore_core/src/occ_partitioned.rs`

### Write path behavior
- OCC tracks per-write `dirty_columns_bitmask: u64`.
- WAL build path computes semantic dirty mask from codec logic.
- Delta payload packs only changed fields for custom codecs.
- Safety guard: if delta reconstruction from base cannot exactly recreate target row, WAL falls back to `UpdateFull`.

### Recovery path behavior
- On `UpdateDelta` replay:
  - resolve base row via PK mapping,
  - load current base value,
  - overlay changed fields from `delta_bytes` according to `dirty_mask`,
  - allocate a new row version,
  - CAS-link new head (`apply_recovered_write_cas`).

### FlightState field-level codec
`aerostore_tcl/src/lib.rs` defines a custom `WalDeltaCodec` for `FlightState` that packs only changed semantic fields (`exists`, `flight_id`, `lat_scaled`, `lon_scaled`, `altitude`, `gs`, `updated_at`).

## Architecture Overview
### Shared memory and pointers
- Core shared memory implementation: `aerostore_core/src/shm.rs`.
- `RelPtr<T>` stores offsets instead of process-local addresses, so structures remain valid across processes.

### OCC and transactions
- OCC table engine: `aerostore_core/src/occ_partitioned.rs`.
- Transaction metadata and snapshots: `aerostore_core/src/procarray.rs`.
- Serializable behavior enforced via read/write conflict checks during commit.

### Planner and execution
- Parser: `aerostore_core/src/stapi_parser.rs`.
- Planner: `aerostore_core/src/rbo_planner.rs`, `aerostore_core/src/planner_cardinality.rs`.
- Execution + index routing: `aerostore_core/src/execution.rs`.

## Repository Layout
- `aerostore_core/`
  - shared-memory primitives, OCC, parser/planner/execution, WAL/recovery, tests, benchmarks.
- `aerostore_tcl/`
  - Tcl `cdylib` bridge and integration tests.
- `aerostore_macros/`
  - `#[speedtable]` proc macro crate.
- `docs/`
  - additional runbooks (including nightly/stress references).

## Build and Prerequisites
### Rust
- Stable Rust toolchain (`cargo`, `rustc`).

### Tcl bridge prerequisites (Linux example)
```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Quick Start
### Build workspace
```bash
cargo build --workspace
```

### Compile test targets
```bash
cargo test --workspace --no-run
```

### Run core and Tcl suites
```bash
cargo test -p aerostore_core
cargo test -p aerostore_tcl
```

## Tcl Bridge Usage
The Tcl package is registered as `aerostore`.

Important commands:
- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState search ...`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`

Config keys:
- `aerostore.synchronous_commit` (`on` or `off`)
- `aerostore.checkpoint_interval_secs`

Example:
```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore

set _ [aerostore::init ./aerostore_tcl_data]
aerostore::set_config aerostore.synchronous_commit on

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1
set n [FlightState search -compare {{= flight_id UAL123} {>= altitude 10000}} -limit 10]
puts $n
```

## STAPI Query Model
Top-level options:
- `-compare <compare_expr>`
- `-sort <field>`
- `-limit <n>`
- `-offset <n>`
- `-asc`
- `-desc`

Supported compare operators:
- `null`, `notnull`
- `=`, `==`, `!=`, `<>`
- `>`, `>=`, `<`, `<=`
- `in`
- `match`, `notmatch`

Planner route kinds:
- `PrimaryKeyLookup`
- `IndexExactMatch`
- `IndexRangeScan`
- `FullScan`

## Durability and Recovery Artifacts
Common file names:
- `aerostore.wal`
- `occ_checkpoint.dat`
- `aerostore_logical.wal`
- `snapshot.dat`

Recovery entry points are in `aerostore_core/src/wal_writer.rs` and support replay from:
- WAL only,
- checkpoint + WAL tail,
- PK-map-aware replay variants.

## Testing Runbook
### Core correctness
```bash
cargo test -p aerostore_core
```

### Tcl integration
```bash
cargo test -p aerostore_tcl
```

### Delta WAL focused
```bash
cargo test -p aerostore_core wal_delta::tests:: -- --nocapture
cargo test -p aerostore_core --test wal_delta_codec -- --nocapture
cargo test -p aerostore_core --test wal_delta_recovery_pk_map -- --nocapture
```

### Recovery and crash behavior
```bash
cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
```

### Ignored heavy suites
```bash
cargo test -p aerostore_core -- --ignored
```

## Benchmark Runbook
### Delta WAL throughput benchmark
```bash
cargo bench -p aerostore_core --bench wal_delta_throughput
```

Current benchmark assertions enforce byte-volume reductions for 100,000 updates:
- one-column mutation: `>= 90%`
- two-column mutation: `>= 75%`
- four-column mutation: `>= 60%`

### Other criterion benches
```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
```

### Test-harness benchmark suites
```bash
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
```

## Known Constraints
- Prototype-stage project, not a production distribution.
- Full test coverage is strongest on Unix-like systems (many fork/cross-process tests use `#[cfg(unix)]`).
- Performance thresholds are machine-sensitive.
- Some legacy/compatibility modules are retained alongside the current primary paths.

## License
No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
