# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype focused on high-ingest, update-heavy workloads.

The project is optimized around:
- process-shared memory structures (`RelPtr<T>`, arena allocation),
- serializable OCC with hybrid optimistic/pessimistic hot-row handling,
- WAL durability with checkpoint and recovery,
- delta-encoded WAL updates to reduce write amplification,
- tmpfs warm restarts to avoid routine cold-start replay,
- Tcl FFI ingestion/search integration.

## Table of Contents
- [Current Scope](#current-scope)
- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Bound-Seeking SkipList Range Scans](#bound-seeking-skiplist-range-scans)
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
Aerostore is under active development. Current focus:
- shared-memory storage/index structures,
- serializable transactions with savepoints,
- STAPI parser + rule-based planner + index-aware execution,
- synchronous/asynchronous WAL commit modes,
- delta WAL update records + delta replay,
- tmpfs-backed warm attach boot path.

Current non-goals:
- SQL compatibility,
- distributed replication,
- production auth/tenancy/RBAC.

## Repository Layout
- `aerostore_core`: storage engine, OCC, WAL/recovery, planner, shared-memory primitives, tests/benches.
- `aerostore_tcl`: Tcl `cdylib` bridge for `FlightState` ingest/search commands.
- `aerostore_macros`: proc-macro support crate.
- `docs`: notes and runbooks.

## Architecture
Core modules in `aerostore_core/src`:
- Shared memory and indexes: `shm.rs`, `shm_index.rs`, `shm_skiplist.rs`, `shm_tmpfs.rs`.
- OCC and retry: `occ_partitioned.rs` (active path), `occ.rs`, `occ_legacy.rs`, `retry.rs`.
- Query/planner: `stapi_parser.rs`, `rbo_planner.rs`, `execution.rs`, `query.rs`.
- WAL and recovery: `wal_ring.rs`, `wal_writer.rs`, `wal_delta.rs`, `recovery_delta.rs`, `recovery.rs`.
- Warm restart bootstrap: `bootloader.rs`.

## Bound-Seeking SkipList Range Scans
Aerostore now uses true bound-seeking traversals for skiplist-backed range predicates.

### Problem solved
Previous range behavior could degenerate into level-0 scans from head for predicates like `>` and `<`, creating O(N) behavior on large keyspaces.

### What was implemented
- `seek_ge` traversal that starts at top height and descends lanes to locate the first level-0 candidate where `key >= bound`.
- `scan_payloads_bounded(lower, upper, ...)` API that:
  - starts at `seek_ge(lower)` for lower-bounded scans,
  - enforces strict exclusivity/inclusivity for bounds,
  - performs immediate early termination on upper-bound breach.
- Secondary index range lookups (`Gt`, `Gte`, `Lt`, `Lte`) route through bounded scans.

### Correctness and performance coverage added
- Duplicate-key and boundary semantics tests (`Gt/Gte/Lt/Lte`) against scan-reference parity.
- Tombstone-heavy seek/scan correctness tests.
- 1,000,000-key criterion benchmark (`> 999,990`) with hard latency assertion `< 5us`.

## Hybrid OCC Livelock Mitigation
Aerostore includes a hybrid optimistic/pessimistic contention path for hot rows.

Implemented components:
- Truncated exponential backoff with jitter:
  - `RetryPolicy` and `RetryBackoff` in `aerostore_core/src/retry.rs`.
  - defaults: base 1ms, truncation ceiling 16ms, escalation after repeated failures.
- Row latch metadata in OCC row header:
  - row lock state in `occ_partitioned.rs`.
- Pessimistic fallback API:
  - `OccTable::lock_for_update(...)` returning a guard for orderly progress under hot-key contention.
- Tcl retry integration:
  - serialization failures use jittered backoff and escalate to pessimistic path after repeated row-local aborts.

## Delta-Encoded WAL
Delta WAL support is implemented in:
- `aerostore_core/src/wal_delta.rs`
- `aerostore_core/src/recovery_delta.rs`
- OCC integration in `aerostore_core/src/occ_partitioned.rs`

Record variants:
- `WalRecord::UpdateFull { pk, payload }`
- `WalRecord::UpdateDelta { pk, dirty_mask, delta_bytes }`

Write path:
- OCC write intents track dirty-column bitmasks.
- Only dirty fields are encoded into compact delta payloads.
- Full-row fallback remains available for safety/guardrails.

Recovery path:
- locate base row by PK map,
- reconstruct next row version from base + delta overlay,
- CAS-link recovered version into MVCC chain.

## Tmpfs Warm Restart
Warm restart uses a file-backed shared mapping (`MAP_SHARED`) on tmpfs.

Key behavior:
- default mapping file: `/dev/shm/aerostore.mmap`,
- override path: `AEROSTORE_SHM_PATH`,
- shared header magic: `0xAEB0_B007`,
- clean shutdown flag in shared header,
- warm attach path skips snapshot load and WAL replay when shared layout is valid,
- orphaned ProcArray slots are cleaned on warm attach after crash.

## Tcl Bridge
Tcl package: `aerostore`.

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
- fork/process-heavy suites are usually most stable with `--test-threads=1`.

Core suite:
```bash
cargo test -p aerostore_core -- --nocapture
```

Tcl suite:
```bash
cargo test -p aerostore_tcl -- --nocapture
```

### SkipList bound-seeking and range checks
```bash
cargo test -p aerostore_core seek_ge_returns_exact_gap_and_end_candidates -- --nocapture
cargo test -p aerostore_core bounded_scan_gt_starts_at_seek_ge_and_collects_tail_only -- --nocapture
cargo test -p aerostore_core bounded_scan_lt_and_lte_apply_strict_early_termination -- --nocapture
cargo test -p aerostore_core seek_ge_and_bounded_scan_stay_correct_with_tombstone_heavy_keys -- --nocapture
cargo test -p aerostore_core --test shm_index_bounds range_operators_ -- --nocapture
cargo test -p aerostore_core --test wal_crash_recovery recovery_large_cardinality_index_parity_matches_table_scan -- --nocapture
```

### Hybrid OCC checks
```bash
cargo test -p aerostore_core --test occ_row_lock_semantics -- --nocapture
cargo test -p aerostore_core --test occ_partitioned_lock_striping_benchmark benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput -- --nocapture
cargo test -p aerostore_core --test occ_partitioned_lock_striping_benchmark benchmark_hybrid_hot_row_outperforms_pure_occ_baseline -- --nocapture
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

## Benchmark Runbook
Criterion benches:
```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench wal_delta_throughput
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
cargo bench -p aerostore_core --bench shm_skiplist_seek_bounds
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
This section reports a full benchmark sweep across the repository.

Run scope:
- Criterion benches executed: 5
- Benchmark-style test suites executed: 8
- Benchmark-style functions executed: 27
- Total benchmark entrypoints executed: 32
- Overall status: completed, with 2 benchmark gate failures:
  - `benchmark_shm_index_range_modes_selectivity_vs_scan`
  - `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline`

### Criterion Benchmarks (`aerostore_core/benches`)
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `procarray_snapshot` | `procarray_snapshot/10` | `[40.842 ns 40.872 ns 40.905 ns]`; proof line `txid_10=41.28ns` | pass | Snapshot acquisition at low txid stays in tens-of-ns range. |
| `procarray_snapshot` | `procarray_snapshot/10000000` | `[40.827 ns 40.863 ns 40.906 ns]`; proof line `txid_10_000_000=41.18ns` | pass | Snapshot acquisition is flat vs txid scale (no txid-growth penalty). |
| `wal_delta_throughput` | `full_row_100k_one_col` | `[4.3262 ms 4.3349 ms 4.3417 ms]`; paired full bytes `81,600,000` | pass/info | Full-row baseline for one-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_one_col` | `[69.771 ms 69.849 ms 69.934 ms]`; delta bytes `4,799,987`; reduction `94.12%` | pass/info | Delta path massively cuts WAL volume for one-column updates. |
| `wal_delta_throughput` | `full_row_100k_two_col` | `[4.3200 ms 4.3230 ms 4.3279 ms]`; paired full bytes `81,600,000` | pass/info | Full-row baseline for two-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_two_col` | `[72.749 ms 72.801 ms 72.885 ms]`; delta bytes `6,099,987`; reduction `92.52%` | pass/info | Delta still saves most WAL bytes with two dirty columns. |
| `wal_delta_throughput` | `full_row_100k_four_col` | `[4.8149 ms 4.8178 ms 4.8212 ms]`; paired full bytes `81,600,000` | pass/info | Full-row baseline for four-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_four_col` | `[76.237 ms 76.357 ms 76.457 ms]`; delta bytes `7,399,987`; reduction `90.93%` | pass/info | Delta retains high byte savings as dirty-column count increases. |
| `shm_skiplist_adversarial` | `p99_ns` | `small=2200ns`, `medium=2800ns`, `large=3500ns`, slopes `0.410/0.388`, criterion `[187.68 ps 188.11 ps 188.69 ps]` | pass (gate `<5000ns`) | Skiplist lookup p99 remains within strict budget under adversarial writer/reader pressure. |
| `shm_skiplist_seek_bounds` | `gt_999_990` | criterion `[77.630 ns 77.730 ns 77.844 ns]`; computed avg `77.85ns`; budget `5000ns` | pass | Bound-seeking traversal bypasses head scan for tail predicate (`> 999,990`). |
| `tmpfs_warm_restart` | `warm_attach_boot` | `[55.210 us 55.335 us 55.437 us]` | pass | Warm attach is very fast (microsecond-scale attach path). |
| `tmpfs_warm_restart` | `cold_boot_fixture_build` | `[2.0275 ms 2.0322 ms 2.0360 ms]` | pass/info | Cold fixture construction is millisecond-scale and much slower than warm attach. |
| `tmpfs_warm_restart` | `post_restart_update_throughput` | `[1.7442 ms 1.7500 ms 1.7532 ms]` | pass/info | Post-restart writes are immediately high-throughput after warm attach. |

Notes:
- `tmpfs_warm_restart` requires access to `/dev/shm`; it was run with elevated execution permissions.

### Benchmark-Style Test Suites
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes` | `sync_tps=1228.10`, `async_tps=1360532.37`, ratio `1107.83x` | pass | Async WAL commit massively outperforms sync commit in this host run. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_parallel_disjoint_keys` | `sync_tps=1262.24`, `async_tps=1280267.42`, ratio `1014.28x` | pass | Async advantage remains under parallel disjoint-key ingest. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_savepoint_churn` | `sync_tps=1282.31`, `async_tps=940115.58`, ratio `733.14x` | pass | Savepoint-heavy workloads still favor async commit heavily. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts` | `sync_tps=1136.55`, `async_tps=683117.92`, ratio `601.04x` | pass | Tcl-style keyed upsert ingest is strongly commit-mode sensitive. |
| `wal_ring_benchmark` | `benchmark_logical_async_vs_sync_commit_modes` | `sync_tps=1011.45`, `async_tps=922315.32`, ratio `911.88x` | pass | Logical WAL path also shows very large async-vs-sync gap. |
| `occ_checkpoint_benchmark` | `benchmark_occ_checkpoint_latency_by_cardinality` | `10k: 3.445576ms (344.56ns/row)`, `100k: 6.623267ms (66.23ns/row)`, `1M: 33.283742ms (33.28ns/row)` | pass | Checkpoint latency scales smoothly with strong per-row amortization at larger cardinalities. |
| `occ_checkpoint_benchmark` | `benchmark_logical_snapshot_and_replay_recovery_by_cardinality` | `10k: 21.920646ms (490,405 rows/s)`, `100k: 195.629607ms (515,004 rows/s)`, `1M: 1.816991139s (550,773 rows/s)` | pass | Snapshot+replay recovery throughput remains stable across data sizes. |
| `query_index_benchmark` | `benchmark_shared_index_indexed_range_scan_with_sort_and_limit` | ingest `100k` rows in `50.836373ms`; `512` scans in `787.330252ms` | pass | Indexed range+sort+limit path sustains high concurrent read throughput. |
| `query_index_benchmark` | `benchmark_stapi_parse_compile_execute_vs_typed_query_path` | typed `91.627101ms`; STAPI `8.428373ms` | pass | STAPI parse/compile/execute path was faster than typed path in this scenario. |
| `query_index_benchmark` | `benchmark_tcl_style_alias_match_desc_offset_limit_path` | `10.765663204s` | pass | End-to-end alias/match/offset/limit query stack cost at configured workload. |
| `query_index_benchmark` | `benchmark_tcl_bridge_style_stapi_assembly_compile_execute` | `8.685921493s` | pass | Measures full Tcl-bridge-style STAPI assembly+compile+execute overhead. |
| `query_index_benchmark` | `benchmark_stapi_rbo_pk_point_lookup_vs_full_scan` | pk `14.574us`; scan `16.089316278s` | pass | Planner PK route avoids catastrophic full-scan cost (orders-of-magnitude win). |
| `query_index_benchmark` | `benchmark_stapi_rbo_tiebreak_dest_over_altitude` | `1.458485781s` | pass | Verifies/quantifies planner tie-break routing behavior under competing indexes. |
| `query_index_benchmark` | `benchmark_stapi_rbo_cardinality_trap_flight_id_over_aircraft_type` | `54.913us` | pass | Planner avoids cardinality trap and picks the more selective access path. |
| `query_index_benchmark` | `benchmark_stapi_residual_negative_filters_with_index_driver` | `1.75223821s` | pass | Shows cost when index driver must apply residual negative predicates. |
| `query_index_benchmark` | `benchmark_stapi_null_notnull_residual_filters` | `1.017999583s` | pass | Captures null/not-null residual filtering overhead in STAPI path. |
| `shm_index_benchmark` | `benchmark_shm_index_eq_lookup_vs_scan` | speedup `486.27x` (`224.817us` vs `109.321917ms`) | pass | EQ index lookup massively outperforms full scan. |
| `shm_index_benchmark` | `benchmark_shm_index_range_lookup_vs_scan` | speedup `2.93x` (`218.570092ms` vs `641.127582ms`) | pass (gate `>=2.0x`) | Core range index path clears its regression target. |
| `shm_index_benchmark` | `benchmark_shm_index_range_modes_selectivity_vs_scan` | overall speedup `1.69x` in full run (`1.65x` isolated rerun) | **failed** (gate `>=2.0x`) | Mixed-selectivity mode mix currently misses configured overall speedup gate. |
| `shm_index_benchmark` | `benchmark_shm_index_forked_contention_throughput` | `single_tps=7162178.40`, `forked_tps=5610793.82`, ratio `0.78` | pass (gate `>=0.20`) | Cross-process contention retains most single-process throughput. |
| `shm_benchmark` | `benchmark_shm_allocation_throughput` | `107,825,593 rows/s` | pass | Shared arena allocator throughput is very high on this host. |
| `shm_benchmark` | `benchmark_forked_range_scan_latency` | `660,182,210 rows/s`; `rows=120000`; `threshold=10000` | pass | Forked-process scan over shared memory is bandwidth-efficient. |
| `wal_crash_recovery` | `benchmark_occ_wal_replay_startup_throughput` | `3,891,039 writes/s` (`12000` applied writes) | pass | WAL replay startup can apply writes at multi-million/s scale. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput` | `throughput_tps=940549.29`, `commits=16000/16000`, `conflicts=44`, `escalations=12` | pass | Hybrid path sustains high TPS on hot-row contention workload. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline` | hybrid samples: `482878.40/768711.85/826885.26 TPS`; pure OCC: `43.60 TPS`, `commits=872/16000`, `timed_out_workers=16` | **failed** (timeout assertion in pure OCC run) | Demonstrates extreme pure-OCC collapse under hot-row contention on this host run. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_partitioned_occ_lock_striping_multi_process_scaling` (ignored) | `contended_tps=1790.10`, `disjoint_tps=349069.76`, ratio `195.00x`; contended timed-out workers `16` | pass (ignored run) | Disjoint-key throughput is dramatically higher than contended hot-row workload. |
| `aerostore_tcl/config_checkpoint_integration` | `benchmark_tcl_synchronous_commit_modes` | `on_tps=987.65`, `off_tps=240000.0`, ratio `243.0x` | pass | Tcl ingest path is highly sensitive to synchronous commit setting. |

## Operational Notes
Durability artifacts under the configured data directory include:
- `aerostore.wal`
- `occ_checkpoint.dat`
- logical paths may also use `aerostore_logical.wal` and `snapshot.dat`.

Shared memory defaults:
- `/dev/shm/aerostore.mmap`
- override with `AEROSTORE_SHM_PATH`.

Force a cold restart:
```bash
rm -f /dev/shm/aerostore.mmap
```

For parallel integration jobs:
- use unique `AEROSTORE_SHM_PATH` values per job/process.

## Known Constraints
- Prototype codebase; internals may change quickly.
- Many strongest tests are Unix/Linux specific (`fork`, `/dev/shm`, signals).
- Throughput thresholds are host-sensitive.
- Tcl bridge uses process-global initialization (`OnceLock`), so one DB instance per process.

## License
No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
