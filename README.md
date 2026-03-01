# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype for high-ingest, update-heavy flight/market workloads.

The repository contains:
- `aerostore_core`: shared-memory primitives, OCC engine, parser/planner, WAL/recovery, benchmarks/tests.
- `aerostore_tcl`: Tcl `cdylib` bridge with `FlightState` ingest/search commands.
- `aerostore_macros`: proc-macro crate used by core tests/examples.

## Table of Contents
- [Current Scope](#current-scope)
- [Architecture Overview](#architecture-overview)
- [Delta-Encoded WAL](#delta-encoded-wal)
- [Tmpfs Warm Restart](#tmpfs-warm-restart)
- [Tcl Bridge](#tcl-bridge)
- [Build and Prerequisites](#build-and-prerequisites)
- [Quick Start](#quick-start)
- [Testing Runbook](#testing-runbook)
- [Benchmark Runbook](#benchmark-runbook)
- [Operational Notes](#operational-notes)
- [Repository Map](#repository-map)
- [Known Constraints](#known-constraints)
- [License](#license)

## Current Scope
Aerostore currently focuses on:
- shared-memory data structures with relative pointers (`RelPtr<T>`),
- serializable OCC validation with savepoints,
- shared primary/secondary index paths for STAPI-style search,
- WAL durability with checkpoint + replay,
- delta-encoded WAL for update-centric writes,
- warm restart from tmpfs-backed shared memory.

Non-goals right now:
- SQL compatibility,
- distributed replication,
- production hardening features (auth, tenancy, RBAC).

## Architecture Overview
Core source modules live under `aerostore_core/src`:
- shared memory: `shm.rs`, `shm_index.rs`, `shm_skiplist.rs`,
- OCC engine: `occ_partitioned.rs` (active), `occ.rs`, `occ_legacy.rs`,
- query/planner: `stapi_parser.rs`, `rbo_planner.rs`, `planner_cardinality.rs`, `execution.rs`,
- durability: `wal_ring.rs`, `wal_writer.rs`, `wal_delta.rs`, `recovery_delta.rs`, `recovery.rs`,
- warm restart bootstrap: `shm_tmpfs.rs`, `bootloader.rs`.

The Tcl bridge lives in `aerostore_tcl/src/lib.rs` and exposes:
- `aerostore::init ?data_dir?`
- `aerostore::set_config <key> <value>`
- `aerostore::get_config <key>`
- `aerostore::checkpoint_now`
- `FlightState ingest_tsv <tsv_data> ?batch_size?`
- `FlightState search ...`

## Delta-Encoded WAL
Delta WAL support is implemented in:
- `aerostore_core/src/wal_delta.rs`
- `aerostore_core/src/recovery_delta.rs`
- `aerostore_core/src/occ_partitioned.rs`
- `aerostore_core/src/wal_ring.rs`
- `aerostore_core/src/wal_writer.rs`

Record variants:
- `WalRecord::UpdateFull { pk, payload }`
- `WalRecord::UpdateDelta { pk, dirty_mask, delta_bytes }`

Write-path behavior:
- OCC tracked writes include `dirty_columns_bitmask: u64`.
- Delta mask is computed from `WalDeltaCodec::compute_dirty_mask`.
- Changed fields are tightly packed via `WalDeltaCodec::encode_changed_fields`.
- Safety guard: if delta application cannot exactly reconstruct the target row, encoding falls back to `UpdateFull`.

Recovery behavior:
- `replay_update_record_with_pk_map` resolves row ID from PK map, reads base head offset, reconstructs row value (delta/full), then applies CAS-linked recovered write.
- `replay_update_record` supports numeric PK/fallback row-ID replay without PK map.

`FlightState` in `aerostore_tcl` implements a custom `WalDeltaCodec` over semantic fields:
- `exists`, `flight_id`, `lat_scaled`, `lon_scaled`, `altitude`, `gs`, `updated_at`.

## Tmpfs Warm Restart
Warm restart is implemented with a shared tmpfs mapping:
- default shm path: `/dev/shm/aerostore.mmap`,
- override path: environment variable `AEROSTORE_SHM_PATH`,
- mapping path: `aerostore_core/src/shm_tmpfs.rs` (`MAP_SHARED` on a real file descriptor),
- boot path: `aerostore_core/src/bootloader.rs`.

Shared memory metadata:
- shared header magic: `0xAEB0_B007` (in `shm.rs`),
- shared header `clean_shutdown: AtomicBool`,
- boot layout magic/version: `BOOT_LAYOUT_MAGIC` / `BOOT_LAYOUT_VERSION`.

Boot modes:
- `BootMode::WarmAttach`: existing valid mapping + valid persisted boot layout,
- `BootMode::ColdReplay`: new/invalid layout path; initializes fresh shared state and uses checkpoint/WAL recovery.

Warm attach behavior:
- attaches existing OCC table/index/WAL ring structures from persisted layout,
- clears orphaned ProcArray slots (`clear_orphaned_slots`) left by a crashed dispatcher,
- skips checkpoint load and WAL replay in the warm path.

Cold path behavior:
- rebuilds state from checkpoint + WAL (when available),
- used when shm file is missing/corrupt or layout validation fails.

## Tcl Bridge
The Tcl package name is `aerostore`.

Config keys:
- `aerostore.synchronous_commit` (`on` or `off`)
- `aerostore.checkpoint_interval_secs`

Default runtime values in `aerostore_tcl/src/lib.rs`:
- default data dir: `./aerostore_tcl_data`
- default row capacity: `32768`
- default checkpoint interval: `300`
- WAL filename: `aerostore.wal`
- checkpoint filename: `occ_checkpoint.dat`

STAPI search options accepted by `FlightState search`:
- `-compare`
- `-sort`
- `-limit`
- `-offset`
- `-asc`
- `-desc`

## Build and Prerequisites
Rust:
- stable toolchain (`cargo`, `rustc`).

Linux Tcl bridge dependencies (example):
```bash
sudo apt-get update
sudo apt-get install -y tcl tcl-dev clang libclang-dev
```

## Quick Start
Build:
```bash
cargo build --workspace
```

Compile tests without running:
```bash
cargo test --workspace --no-run
```

Minimal Tcl usage (Linux debug build):
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
- many process/fork suites are most stable with `--test-threads=1`.

Core suites:
```bash
cargo test -p aerostore_core
```

Tcl suites:
```bash
cargo test -p aerostore_tcl
```

Delta WAL focused:
```bash
cargo test -p aerostore_core --test wal_delta_codec -- --nocapture
cargo test -p aerostore_core --test wal_delta_recovery_pk_map -- --nocapture
```

Recovery and crash behavior:
```bash
cargo test -p aerostore_core --test logical_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --test wal_writer_lifecycle -- --nocapture --test-threads=1
```

ProcArray and shared-memory concurrency:
```bash
cargo test -p aerostore_core --test procarray_concurrency -- --nocapture --test-threads=1
cargo test -p aerostore_core --test shm_shared_memory -- --nocapture --test-threads=1
```

Warm-restart tests:
```bash
cargo test -p aerostore_core --test tmpfs_warm_restart_chaos -- --nocapture --test-threads=1
cargo test -p aerostore_core --test tmpfs_warm_restart_expansions -- --nocapture --test-threads=1
```

Tcl integration tests:
```bash
cargo test -p aerostore_tcl --test search_stapi_bridge -- --test-threads=1
cargo test -p aerostore_tcl --test config_checkpoint_integration -- --test-threads=1
```

Nightly/perf-focused runbook:
- see `docs/nightly_perf.md`.

## Benchmark Runbook
Criterion benchmarks:
```bash
cargo bench -p aerostore_core --bench wal_delta_throughput
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
cargo bench -p aerostore_core --bench tmpfs_warm_restart
```

`wal_delta_throughput` includes byte-reduction assertions for 100,000 updates:
- one-column mutation: `>= 90%`
- two-column mutation: `>= 75%`
- four-column mutation: `>= 60%`

Test-harness benchmark suites:
```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
```

## Operational Notes
Durability artifacts (under data dir):
- `aerostore.wal`
- `occ_checkpoint.dat`
- logical durability paths also use `aerostore_logical.wal` and `snapshot.dat`.

Shared memory segment:
- default: `/dev/shm/aerostore.mmap`
- override: `AEROSTORE_SHM_PATH=/dev/shm/<your_path>.mmap`

Force a cold restart from the Tcl path:
```bash
rm -f /dev/shm/aerostore.mmap
```
or remove the file pointed to by `AEROSTORE_SHM_PATH`.

Test isolation tip:
- when running multiple integration jobs concurrently, use distinct `AEROSTORE_SHM_PATH` values per process.

## Repository Map
- `aerostore_core/src`: shared memory, OCC, parser/planner, WAL/recovery.
- `aerostore_core/tests`: correctness, crash/recovery, contention, throughput-style tests.
- `aerostore_core/benches`: Criterion benchmarks.
- `aerostore_tcl/src/lib.rs`: Tcl package entrypoint and command wiring.
- `aerostore_tcl/tests`: Tcl integration and config/recovery tests.
- `docs/nightly_perf.md`: nightly/stress/perf runbook.

## Known Constraints
- Prototype project; APIs and internals may change quickly.
- Many strongest suites target Unix/Linux (`fork`, `/dev/shm`, process signaling).
- Performance thresholds are machine-sensitive.
- Tcl bridge uses process-global initialization (`OnceLock`), so each process hosts one shared DB instance at a time.

## License
No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
