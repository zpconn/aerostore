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
cargo test -p aerostore_core --release --test shm_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_crash_recovery benchmark_occ_wal_replay_startup_throughput -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_tcl --release --test config_checkpoint_integration benchmark_tcl_synchronous_commit_modes -- --test-threads=1 --nocapture
```

### Latest Full Benchmark Sweep
Run timestamp: `2026-03-01 23:50:21 UTC`.

All values below are host-specific and should be used primarily for trend/regression comparison on the same class of machine.

#### Criterion Benches
| Benchmark | Result | Status | What it means |
|---|---:|---|---|
| `procarray_snapshot/10` | `40.98 ns` | Pass | Snapshot creation is extremely fast at low txid values. |
| `procarray_snapshot/10000000` | `40.93 ns` | Pass | Snapshot creation remains constant-time even at large txid values. |
| `procarray_snapshot_proof` | `delta=0.17 ns` | Pass | Confirms txid growth does not materially change snapshot latency. |
| `wal_delta_throughput` one-column | `reduction=94.12%` | Pass | Delta WAL cuts byte volume heavily for single-field updates (well above the 90% gate). |
| `wal_delta_throughput` two-column | `reduction=92.52%` | Pass | Delta WAL still saves most bytes when mutating two fields (above 75% gate). |
| `wal_delta_throughput` four-column | `reduction=90.93%` | Pass | Delta WAL keeps strong volume reduction for wider updates (above 60% gate). |
| `wal_delta_throughput/full_row_100k_one_col` | `[4.2999, 4.3229] ms` | Pass | Baseline full-row serialization latency for 100k single-column updates. |
| `wal_delta_throughput/delta_row_100k_one_col` | `[69.027, 69.094] ms` | Pass | Delta encoding is CPU-heavier but writes far fewer bytes; byte savings are the primary objective. |
| `wal_delta_throughput/full_row_100k_two_col` | `[4.3231, 4.3416] ms` | Pass | Full-row baseline for two-column update profile. |
| `wal_delta_throughput/delta_row_100k_two_col` | `[71.924, 72.260] ms` | Pass | Delta CPU cost for two-column updates; still meets byte-reduction targets. |
| `wal_delta_throughput/full_row_100k_four_col` | `[4.8055, 4.8096] ms` | Pass | Full-row baseline for four-column profile. |
| `wal_delta_throughput/delta_row_100k_four_col` | `[74.973, 75.532] ms` | Pass | Delta CPU cost for wider updates; byte volume remains significantly reduced. |
| `tmpfs_warm_restart/warm_attach_boot` | `[57.440, 57.776] us` | Pass | Warm attach startup path is sub-0.1ms and effectively O(1). |
| `tmpfs_warm_restart/cold_boot_fixture_build` | `[1.8466, 1.8596] ms` | Pass | Cold fixture bootstrap is materially slower than warm attach as expected. |
| `tmpfs_warm_restart/post_restart_update_throughput` | `[1.6412, 1.6658] ms` | Pass | Immediate post-restart write path remains fast and stable. |
| `shm_skiplist_adversarial` | `large_p99_ns=5200` | **Fail** | Tail lookup latency exceeded the strict `<5000ns` budget in this run; benchmark failed with assertion. |

#### Core Benchmark-Style Tests
| Benchmark | Result | Status | What it means |
|---|---:|---|---|
| `benchmark_occ_checkpoint_latency_by_cardinality` | `10k: 2.989 ms`, `100k: 4.918 ms`, `1M: 24.674 ms` | Pass | Checkpointing scales well with row count; per-row cost drops at larger batches. |
| `benchmark_logical_snapshot_and_replay_recovery_by_cardinality` | `10k: 22.261 ms`, `100k: 173.999 ms`, `1M: 1.775 s` | Pass | Logical snapshot+WAL replay remains linear and stable across cardinalities. |
| `benchmark_shared_index_indexed_range_scan_with_sort_and_limit` | `ingest 100k in 50.581 ms; 512 scans in 828.002 ms` | Pass | Shared index supports high-ingest plus concurrent indexed range scanning. |
| `benchmark_stapi_null_notnull_residual_filters` | `1.129 s (40 passes, 40k rows)` | Pass | Measures cost of null/notnull residual predicate execution. |
| `benchmark_stapi_parse_compile_execute_vs_typed_query_path` | `typed=104.819 ms`, `stapi=27.025 ms` | Pass | In this scenario, STAPI parse+compile+execute path outperformed typed path. |
| `benchmark_stapi_rbo_cardinality_trap_flight_id_over_aircraft_type` | `30.707 us` | Pass | Planner cardinality heuristics route to the selective path efficiently. |
| `benchmark_stapi_rbo_pk_point_lookup_vs_full_scan` | `pk=14.906 us`, `scan=16.196 s` | Pass | Primary-key routing avoids catastrophic full-scan cost. |
| `benchmark_stapi_rbo_tiebreak_dest_over_altitude` | `1.441 s` | Pass | Evaluates route tie-break behavior under competing predicates. |
| `benchmark_stapi_residual_negative_filters_with_index_driver` | `1.797 s` | Pass | Measures negative/residual filtering overhead after index-driven candidate selection. |
| `benchmark_tcl_bridge_style_stapi_assembly_compile_execute` | `8.919 s` | Pass | End-to-end Tcl-style STAPI assembly+compile+execute overhead benchmark. |
| `benchmark_tcl_style_alias_match_desc_offset_limit_path` | `10.966 s` | Pass | Complex alias/match/sort/offset/limit Tcl path throughput measurement. |
| `benchmark_shm_allocation_throughput` | `106,099,805 rows/s` | Pass | Shared arena allocator supports very high allocation throughput. |
| `benchmark_forked_range_scan_latency` | `258,687 ns`, `463,881,061 rows/s` | Pass | Cross-process shared-memory range scans remain very fast. |
| `benchmark_shm_index_eq_lookup_vs_scan` | `speedup=675.82x` | Pass | Exact-match index lookups massively outperform table scans. |
| `benchmark_shm_index_range_lookup_vs_scan` | `speedup=2.22x` | Pass | Range index lookups outperform scan baseline for tested workload. |
| `benchmark_shm_index_forked_contention_throughput` | `single=7.77M tps`, `forked=6.06M tps`, `ratio=0.78` | Pass | Multi-process contention reduces throughput, but shared index remains high-throughput. |
| `benchmark_async_synchronous_commit_modes` | `sync=1248.76 tps`, `async=1,474,792.26 tps`, `1181.00x` | Pass | Async WAL commit path provides massive throughput uplift vs fsync-heavy sync mode. |
| `benchmark_async_synchronous_commit_modes_parallel_disjoint_keys` | `sync=1218.10 tps`, `async=2,017,026.73 tps`, `1655.88x` | Pass | Async mode scales strongly on parallel disjoint-key workload. |
| `benchmark_async_synchronous_commit_modes_savepoint_churn` | `sync=1330.95 tps`, `async=1,090,306.37 tps`, `819.20x` | Pass | Savepoint-heavy churn remains far faster in async mode. |
| `benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts` | `sync=1235.70 tps`, `async=706,446.53 tps`, `571.70x` | Pass | Tcl-like keyed upsert pattern strongly benefits from async commit mode. |
| `benchmark_logical_async_vs_sync_commit_modes` | `sync=1052.27 tps`, `async=927,556.11 tps`, `881.48x` | Pass | Logical WAL path also shows large async advantage. |
| `benchmark_occ_wal_replay_startup_throughput` | `4,235,384.31 writes/s` | Pass | OCC WAL replay startup path can apply writes at multi-million writes/sec. |
| `benchmark_partitioned_occ_lock_striping_multi_process_scaling` | `contended=3.00 tps`, `disjoint=523,476.17 tps`, `174,528.96x` | Pass | Demonstrates lock-striping impact: hot-key contention collapses throughput; disjoint keys scale. |
| `benchmark_tcl_synchronous_commit_modes` | `on=1123.60 tps`, `off=300,000.00 tps`, `267.0x` | Pass | Tcl bridge sees large throughput gain when synchronous commit is disabled. |

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
