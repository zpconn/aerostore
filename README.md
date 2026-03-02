# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype for high-ingest, update-heavy workloads.

It is optimized around:
- process-shared memory structures,
- optimistic concurrency control (OCC) with serializable validation,
- WAL durability with checkpoint/replay,
- delta-encoded updates for write amplification reduction,
- tmpfs warm restarts,
- Tcl FFI ingestion/search paths.

## Table of Contents
- [Current Scope](#current-scope)
- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Hybrid OCC Livelock Mitigation](#hybrid-occ-livelock-mitigation)
- [Delta-Encoded WAL](#delta-encoded-wal)
- [Tmpfs Warm Restart](#tmpfs-warm-restart)
- [Tcl Bridge](#tcl-bridge)
- [Build and Prerequisites](#build-and-prerequisites)
- [Quick Start](#quick-start)
- [Testing Runbook](#testing-runbook)
- [Benchmark Runbook](#benchmark-runbook)
- [Latest Validated Results (2026-03-02)](#latest-validated-results-2026-03-02)
- [Operational Notes](#operational-notes)
- [Known Constraints](#known-constraints)
- [License](#license)

## Current Scope
Aerostore is under rapid active development. The project currently focuses on:
- shared-memory data structures with relative pointers (`RelPtr<T>`),
- serializable OCC with savepoints,
- STAPI parser + rule-based planner + shared index execution,
- synchronous and asynchronous WAL commit modes,
- delta WAL update records,
- tmpfs-backed warm attach.

Current non-goals:
- SQL compatibility,
- distributed replication,
- production auth/tenancy/RBAC layers.

## Repository Layout
- `aerostore_core`: core storage engine, OCC, WAL/recovery, planner, tests/benches.
- `aerostore_tcl`: Tcl `cdylib` bridge exposing `FlightState` commands.
- `aerostore_macros`: proc-macro support crate.
- `docs`: project runbooks and notes.

## Architecture
Core modules in `aerostore_core/src`:
- Shared memory: `shm.rs`, `shm_index.rs`, `shm_skiplist.rs`, `shm_tmpfs.rs`.
- OCC: `occ_partitioned.rs` (active), `occ.rs`, `occ_legacy.rs`.
- Retry/backoff primitives: `retry.rs`.
- Query/planner: `stapi_parser.rs`, `rbo_planner.rs`, `execution.rs`.
- WAL and recovery: `wal_ring.rs`, `wal_writer.rs`, `wal_delta.rs`, `recovery_delta.rs`, `recovery.rs`.
- Warm restart bootstrap: `bootloader.rs`.

## Hybrid OCC Livelock Mitigation
Aerostore now includes a hybrid optimistic/pessimistic contention path for hot keys.

### What was added
- Truncated exponential backoff with jitter:
  - `RetryPolicy` and `RetryBackoff` in `aerostore_core/src/retry.rs`.
  - Defaults: base 1ms, truncation ceiling 16ms, escalation threshold at 3 consecutive failures.
- Row-level pessimistic latch metadata:
  - `OccRow` now includes `is_locked: AtomicBool` and lock owner txid in `occ_partitioned.rs`.
- Pessimistic fallback API:
  - `OccTable::lock_for_update(...)` returning `RowLockGuard`.
  - Competing transactions on a locked row fail fast with `SerializationFailure`.
- Tcl retry loop integration:
  - The repo's Tcl FFI bridge is implemented in `aerostore_tcl/src/lib.rs`.
  - On serialization failures, ingest retries use jittered backoff and escalate to `lock_for_update` after repeated row-local conflicts.

### Why it matters
Under hot-key contention, this breaks retry synchronization waves and converts wasted spin conflict storms into orderly progress.

## Delta-Encoded WAL
Delta WAL support is implemented in:
- `aerostore_core/src/wal_delta.rs`
- `aerostore_core/src/recovery_delta.rs`
- `aerostore_core/src/occ_partitioned.rs`

Record variants:
- `WalRecord::UpdateFull { pk, payload }`
- `WalRecord::UpdateDelta { pk, dirty_mask, delta_bytes }`

Write path:
- OCC write intents track `dirty_columns_bitmask: u64`.
- Changed fields are serialized into `delta_bytes` using `WalDeltaCodec`.
- Guardrails fall back to full-row WAL when delta reconstruction checks fail.

Recovery path:
- Resolve row by PK map.
- Reconstruct value via full or delta payload.
- CAS-link recovered row version into the version chain.

## Tmpfs Warm Restart
Warm restart is implemented with a real shared file mapping (`MAP_SHARED`) on tmpfs.

Key pieces:
- Default mapping file: `/dev/shm/aerostore.mmap`.
- Override with `AEROSTORE_SHM_PATH`.
- Shared header magic: `0xAEB0_B007` and `clean_shutdown` flag.
- Warm attach boot mode skips snapshot load and WAL replay when mapping/layout is valid.
- Orphaned ProcArray slots from dispatcher crashes are cleaned during warm attach.

## Tcl Bridge
The Tcl package is `aerostore`.

Main commands:
- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`
- `FlightState search ...`

Supported config keys:
- `aerostore.synchronous_commit` (`on` or `off`)
- `aerostore.checkpoint_interval_secs`

## Build and Prerequisites
Rust:
- stable toolchain (`cargo`, `rustc`).

Linux Tcl dependencies (example):
```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

Build workspace:
```bash
cargo build --workspace
```

## Quick Start
Compile tests without running:
```bash
cargo test --workspace --no-run
```

Minimal Tcl smoke snippet (debug build):
```tcl
load ./target/debug/libaerostore_tcl.so Aerostore
package require aerostore

set _ [aerostore::init ./aerostore_tcl_data]
aerostore::set_config aerostore.synchronous_commit on

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1
set n [FlightState search -compare {{= flight_id UAL123}} -limit 10]
puts $n
```

## Testing Runbook
General note:
- Many fork/process-heavy suites are most stable with `--test-threads=1`.

Full core suite:
```bash
cargo test -p aerostore_core -- --nocapture
```

Full Tcl suite:
```bash
cargo test -p aerostore_tcl -- --nocapture
```

### Hybrid OCC mission-critical checks
```bash
cargo test -p aerostore_core --test occ_row_lock_semantics -- --nocapture
cargo test -p aerostore_core --test occ_partitioned_lock_striping_benchmark benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput -- --nocapture
cargo test -p aerostore_core --test occ_partitioned_lock_striping_benchmark benchmark_hybrid_hot_row_outperforms_pure_occ_baseline -- --nocapture
cargo test -p aerostore_core --test occ_partitioned_lock_striping_benchmark benchmark_partitioned_occ_lock_striping_multi_process_scaling -- --ignored --nocapture
cargo test -p aerostore_core --test procarray_concurrency procarray_disjoint_writes_have_lower_conflicts_than_hot_row_contention -- --nocapture
cargo test -p aerostore_core --test occ_write_skew -- --nocapture
```

### Durability and restart checks
```bash
cargo test -p aerostore_core --test wal_delta_codec -- --nocapture
cargo test -p aerostore_core --test wal_delta_recovery_pk_map -- --nocapture
cargo test -p aerostore_core --test wal_ring_benchmark -- --nocapture
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test tmpfs_warm_restart_chaos -- --nocapture --test-threads=1
cargo test -p aerostore_core --test tmpfs_warm_restart_expansions -- --nocapture --test-threads=1
```

### Tcl integration checks
```bash
cargo test -p aerostore_tcl --test config_checkpoint_integration -- --nocapture
cargo test -p aerostore_tcl --test search_stapi_bridge -- --nocapture
```

## Benchmark Runbook
Criterion benches:
```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench wal_delta_throughput
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
cargo bench -p aerostore_core --bench tmpfs_warm_restart
```

Benchmark-style test harness suites:
```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_crash_recovery benchmark_occ_wal_replay_startup_throughput -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_tcl --release --test config_checkpoint_integration benchmark_tcl_synchronous_commit_modes -- --test-threads=1 --nocapture
```

## Latest Validated Results (2026-03-02)
This section reflects the latest targeted verification run for the hybrid OCC mission and its regression checks.

### Hybrid OCC + lock semantics
- `benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput`:
  - 16 workers x 1000 updates each on one row.
  - Throughput observed: `103,120.65 TPS`.
  - Gate: `> 10,000 TPS`.
  - Status: pass.
- `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline`:
  - Median hybrid TPS: `108,421.12`.
  - Median pure OCC TPS: `57,138.48`.
  - Ratio: `1.90x`.
  - Gate in test: `>= 1.5x`.
  - Status: pass.
- `occ_row_lock_semantics`:
  - competing tx fast-abort under lock: pass.
  - lock release after abort/drop: pass.

### Contention and isolation regressions
- `benchmark_partitioned_occ_lock_striping_multi_process_scaling` (ignored benchmark run explicitly):
  - contended TPS: `53,673.94`
  - disjoint TPS: `348,296.14`
  - ratio: `6.49x`
  - gate: `>= 3.0x`
  - status: pass.
- `procarray_disjoint_writes_have_lower_conflicts_than_hot_row_contention`: pass.
- `occ_write_skew` suite: 6/6 pass.

### Durability and bridge regressions
- `wal_ring_benchmark` suite: 6/6 pass.
- `config_checkpoint_integration`: 3/3 pass.
- `search_stapi_bridge`: pass.

## Operational Notes
Durability artifacts under the configured data dir include:
- `aerostore.wal`
- `occ_checkpoint.dat`
- logical paths may also use `aerostore_logical.wal` and `snapshot.dat`.

Shared memory segment defaults:
- `/dev/shm/aerostore.mmap`
- override with `AEROSTORE_SHM_PATH`.

Force cold restart:
```bash
rm -f /dev/shm/aerostore.mmap
```

For parallel integration jobs:
- use unique `AEROSTORE_SHM_PATH` values per job/process.

## Known Constraints
- Prototype codebase; internals can change rapidly.
- Many strongest tests are Unix/Linux specific (`fork`, `/dev/shm`, signals).
- Throughput thresholds are host-sensitive.
- Tcl bridge uses process-global initialization (`OnceLock`), so one DB instance per process.

## License
No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
