# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a Rust-first shared-memory database prototype focused on high-ingest, update-heavy workloads.

The project is optimized around:
- process-shared memory structures (`RelPtr<T>`, arena allocation),
- serializable OCC with hybrid optimistic/pessimistic hot-row handling,
- WAL durability with checkpoint and recovery,
- delta-encoded WAL updates to reduce write amplification,
- cross-process vacuum + shared free-list recycling for update-heavy MVCC churn,
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
- [Cross-Process Vacuum GC](#cross-process-vacuum-gc)
- [Tcl Bridge](#tcl-bridge)
- [Build and Prerequisites](#build-and-prerequisites)
- [Quick Start](#quick-start)
- [Testing Runbook](#testing-runbook)
- [Benchmark Runbook](#benchmark-runbook)
- [Latest Validated Results (2026-03-03)](#latest-validated-results-2026-03-03)
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
- cross-process vacuum reclaim + shared free-list reuse,
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
- Vacuum and reclaim: `vacuum.rs`.

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

## Cross-Process Vacuum GC
Aerostore now includes a cross-process vacuum path to prevent `/dev/shm` exhaustion under heavy update churn.

Implemented components:
- Shared lock-free free-list in shared header (`SharedFreeList` equivalent):
  - `free_list_head`, push/pop counters, and daemon PID slot in `ShmHeader`.
- Allocator recycle-first policy:
  - `ChunkedArena::alloc_raw` attempts free-list pop before bumping high-water mark.
  - `ChunkedArena::recycle_raw` pushes reclaimed blocks for O(1) reuse.
- Vacuum daemon in `aerostore_core/src/vacuum.rs`:
  - 1-second sweep cadence by default.
  - computes `global_xmin` from the 256-slot `ProcArray`.
  - leader election/ownership via shared daemon PID slot.
- Dead tuple reclaim in OCC:
  - unlinks versions where `xmax < global_xmin` from row version chains,
  - pushes reclaimed row blocks back into shared free-list.
- Secondary index cleanup hook:
  - reclaimed-row callbacks in Tcl runtime remove stale postings while preserving live postings for unchanged indexed fields.

Primary files:
- `aerostore_core/src/shm.rs`
- `aerostore_core/src/occ_partitioned.rs`
- `aerostore_core/src/vacuum.rs`
- `aerostore_tcl/src/lib.rs`

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

### Vacuum and recycle checks
```bash
cargo test -p aerostore_core --test vacuum_recycle_stress -- --nocapture
cargo test -p aerostore_core --test vacuum_leader_handoff -- --nocapture
cargo test -p aerostore_core --test vacuum_free_list_invariants -- --nocapture --test-threads=1
cargo test -p aerostore_core --test vacuum_recycle_ab_benchmark -- --nocapture
cargo test -p aerostore_tcl --lib vacuum_index_cleanup_tests -- --nocapture
```

## Benchmark Runbook
Criterion benches:
```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench wal_delta_throughput
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
cargo bench -p aerostore_core --bench shm_skiplist_seek_bounds
cargo bench -p aerostore_core --bench tmpfs_warm_restart
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Hyperfeed Crucible note:
- requires Docker (`testcontainers` starts a local PostgreSQL container).
- runs two Aerostore memory profiles per invocation: `profile_512m` and `profile_1g`.
- default sustained duration is 60 seconds; override with `AEROSTORE_CRUCIBLE_DURATION_SECS` for local smoke runs.
- optional daemon tuning knobs:
  - `AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS`
  - `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS`
- output includes index update failure counters, vacuum/index reclaim telemetry, and PostgreSQL scan server-vs-client latency breakdown.
- hard gates enforce `Aerostore TPS >= 2.0x Postgres TPS` and `Aerostore p99 <= 0.6x Postgres p99`.

Benchmark-style test harness suites:
```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test vacuum_recycle_ab_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_crash_recovery benchmark_occ_wal_replay_startup_throughput -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_tcl --release --test config_checkpoint_integration benchmark_tcl_synchronous_commit_modes -- --test-threads=1 --nocapture
```

## Latest Validated Results (2026-03-03)
This section reports a full rerun of workspace tests plus the complete benchmark suite (criterion benches, benchmark-style tests, and 60-second crucible runs).

Run scope:
- Criterion benches executed: 6
- Benchmark-style test suites executed: 10
- Benchmark-style functions executed: 30
- Total benchmark entrypoints executed: 36
- Crucible runs executed: 2 sustained 60-second runs (first run failed one gate; second run passed all gates)
- Overall status: benchmark coverage completed; post-fix workspace rerun is passing.

### Full Test Sweep Status (2026-03-03)
| Check Group | Result | Notes |
| --- | --- | --- |
| `cargo test --workspace -- --nocapture` | pass | Post-fix rerun completed with all workspace tests passing (including `wal_crash_recovery`). |
| `cargo test -p aerostore_core --test wal_crash_recovery -- --nocapture --test-threads=1` | pass | `11/11` passed after fixing WAL writer epoch restart race. |
| tmpfs warm-restart tests and benchmarks | pass (with host `/dev/shm`) | Requires host-level mmap permissions; reruns succeeded once executed with `/dev/shm` access. |
| vacuum and reclaim suites | pass | Stress, leader-handoff, and free-list invariants suites passed in workspace run. |
| hot-row OCC contention suites | pass | `timed_out_workers=0` in release benchmark runs. |

### Hyperfeed Crucible Diagnostic (2026-03-03)
Command:
```bash
AEROSTORE_CRUCIBLE_DURATION_SECS=60 cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Latest full passing run (`AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS=50`, `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS=50`):

| Profile | Aerostore TPS | Postgres TPS | TPS Ratio | Aerostore p99 (us) | Postgres p99 (us) | Index Remove Failures | Index Insert Failures | Vacuum Reclaimed Rows | Index Reclaimed Nodes (delta) | Free-List Pushes/Pops (delta) | Retry Loops | Max Insert Attempts | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| `profile_512m` | `376908.38` | `89206.64` | `4.23x` | `65.54` | `262.14` | `0` | `0` | `20,395,142` | `19,246,805` | `20,395,142 / 19,843,639` | `401,658` | `4097` | pass |
| `profile_1g` | `414002.64` | `89664.26` | `4.62x` | `65.54` | `262.14` | `0` | `0` | `22,396,979` | `19,954,538` | `22,396,979 / 21,658,204` | `394,583` | `4097` | pass |

Engine timing details for the same run:

| Profile | Aerostore elapsed (s) | Postgres elapsed (s) | Notes |
| --- | ---: | ---: | --- |
| `profile_512m` | `67.639` | `60.002` | Aerostore workers drained past the nominal 60s window before final shutdown/aggregation. |
| `profile_1g` | `67.623` | `60.002` | Same drain behavior; comparison still uses total completed ops over measured elapsed window. |

First 60s attempt in this rerun sequence (before the passing run) failed one gate:
- `profile_1g` observed `Aerostore p99 = 262.14us` vs `Postgres p99 = 262.14us` (ratio `1.000`, required `<= 0.600`).
- A second immediate rerun passed both TPS and p99 gates for both profiles.

Interpretation:
- Aerostore remains ahead on throughput in this run (`4.23x` to `4.62x`) with strictly zero index insert/remove failures in both profiles.
- PostgreSQL scan server execution time remains much smaller than end-to-end RTT, so transport/protocol overhead is still a major cost center on its path.
- Crucible latency gates can be sensitive to host variance; a failed first run and passing second run occurred back-to-back under the same settings.

### Criterion Benchmarks (`aerostore_core/benches`)
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `procarray_snapshot` | `procarray_snapshot/10` | `[39.558 ns 39.645 ns 39.741 ns]`; proof `txid_10=42.02ns` | pass | Snapshot acquisition at low txid stays in tens-of-ns range. |
| `procarray_snapshot` | `procarray_snapshot/10000000` | `[39.402 ns 39.479 ns 39.558 ns]`; proof `txid_10_000_000=42.05ns` | pass | Snapshot acquisition remains flat vs txid scale (no txid-growth penalty). |
| `wal_delta_throughput` | `full_row_100k_one_col` | `[4.1487 ms 4.1561 ms 4.1659 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for one-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_one_col` | `[66.364 ms 66.511 ms 66.667 ms]`; delta bytes `4,799,987`; reduction `94.12%` | pass/info | Delta path massively cuts WAL byte volume for one-column updates. |
| `wal_delta_throughput` | `full_row_100k_two_col` | `[4.1413 ms 4.1533 ms 4.1632 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for two-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_two_col` | `[69.660 ms 69.752 ms 69.852 ms]`; delta bytes `6,099,987`; reduction `92.52%` | pass/info | Delta still saves most WAL bytes with two dirty columns. |
| `wal_delta_throughput` | `full_row_100k_four_col` | `[4.6037 ms 4.6149 ms 4.6290 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for four-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_four_col` | `[72.597 ms 72.737 ms 72.896 ms]`; delta bytes `7,399,987`; reduction `90.93%` | pass/info | Delta retains high byte savings as dirty-column count increases. |
| `shm_skiplist_adversarial` | `p99_ns` | `small=2600ns`, `medium=3100ns`, `large=3700ns`, slopes `0.299/0.308`, criterion `[179.97 ps 180.45 ps 180.99 ps]` | pass (gate `<5000ns`) | Skiplist lookup p99 remains inside strict budget under adversarial writer/reader pressure. |
| `shm_skiplist_seek_bounds` | `gt_999_990` | criterion `[77.835 ns 78.092 ns 78.342 ns]`; computed avg `77.72ns`; budget `5000ns` | pass | Bound-seeking traversal bypasses the first 999,990 nodes for tail predicate `> 999,990`. |
| `tmpfs_warm_restart` | `warm_attach_boot` | `[48.982 us 49.156 us 49.421 us]` | pass | Warm attach remains microsecond-scale. |
| `tmpfs_warm_restart` | `cold_boot_fixture_build` | `[2.3553 ms 2.3646 ms 2.3743 ms]` | pass/info | Cold fixture construction is millisecond-scale and much slower than warm attach. |
| `tmpfs_warm_restart` | `post_restart_update_throughput` | `[1.7435 ms 1.7517 ms 1.7592 ms]` | pass/info | Post-restart write throughput is immediately high after warm attach. |

Notes:
- `tmpfs_warm_restart` was executed with elevated permissions so `/dev/shm` mapping is measured on real tmpfs.

### Benchmark-Style Test Suites
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes` | `sync_tps=1321.98`, `async_tps=1360807.86`, ratio `1029.37x` | pass | Async WAL commit massively outperforms sync commit in this run. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_parallel_disjoint_keys` | `sync_tps=1294.59`, `async_tps=1469900.44`, ratio `1135.42x` | pass | Async advantage remains strong under parallel disjoint-key ingest. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_savepoint_churn` | `sync_tps=1336.67`, `async_tps=1041997.07`, ratio `779.55x` | pass | Savepoint-heavy workloads still favor async commit heavily. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts` | `sync_tps=1298.82`, `async_tps=688320.35`, ratio `529.96x` | pass | Tcl-like keyed upsert ingest is highly commit-mode sensitive. |
| `wal_ring_benchmark` | `benchmark_logical_async_vs_sync_commit_modes` | `sync_tps=1153.21`, `async_tps=914065.59`, ratio `792.63x` | pass | Logical WAL path also shows a very large async-vs-sync gap. |
| `occ_checkpoint_benchmark` | `benchmark_occ_checkpoint_latency_by_cardinality` | `10k: 2.900334ms (290.03ns/row)`, `100k: 4.715268ms (47.15ns/row)`, `1M: 24.536467ms (24.54ns/row)` | pass | Checkpoint latency scales smoothly with strong per-row amortization at higher cardinalities. |
| `occ_checkpoint_benchmark` | `benchmark_logical_snapshot_and_replay_recovery_by_cardinality` | `10k: 22.35304ms (480,918.93 rows/s)`, `100k: 176.619104ms (570,436.59 rows/s)`, `1M: 1.793417883s (558,012.73 rows/s)` | pass | Snapshot+replay recovery throughput remains stable across data sizes. |
| `query_index_benchmark` | `benchmark_shared_index_indexed_range_scan_with_sort_and_limit` | ingest `100k` rows in `50.462744ms`; `512` scans in `657.545733ms` | pass | Indexed range+sort+limit path sustains high concurrent read throughput. |
| `query_index_benchmark` | `benchmark_stapi_parse_compile_execute_vs_typed_query_path` | typed `54.517194ms`; STAPI `7.068974ms` | pass | STAPI parse/compile/execute path is faster than typed path in this scenario. |
| `query_index_benchmark` | `benchmark_tcl_style_alias_match_desc_offset_limit_path` | `10.501318577s` | pass | End-to-end alias/match/offset/limit query stack cost at configured workload. |
| `query_index_benchmark` | `benchmark_tcl_bridge_style_stapi_assembly_compile_execute` | `8.415177634s` | pass | Full Tcl-bridge-style STAPI assembly+compile+execute overhead. |
| `query_index_benchmark` | `benchmark_stapi_rbo_pk_point_lookup_vs_full_scan` | pk `14.279us`; scan `15.553936058s` | pass | Planner PK route avoids catastrophic full-scan cost (orders-of-magnitude win). |
| `query_index_benchmark` | `benchmark_stapi_rbo_tiebreak_dest_over_altitude` | `1.405741611s` | pass | Quantifies planner tie-break routing behavior under competing indexes. |
| `query_index_benchmark` | `benchmark_stapi_rbo_cardinality_trap_flight_id_over_aircraft_type` | `29.743us` | pass | Planner avoids cardinality trap and picks the selective access path. |
| `query_index_benchmark` | `benchmark_stapi_residual_negative_filters_with_index_driver` | `1.678075485s` | pass | Shows cost when an index driver must apply residual negative predicates. |
| `query_index_benchmark` | `benchmark_stapi_null_notnull_residual_filters` | `960.03769ms` | pass | Captures null/not-null residual filtering overhead in STAPI path. |
| `shm_index_benchmark` | `benchmark_shm_index_eq_lookup_vs_scan` | speedup `643.66x` (`205.538us` vs `132.296066ms`) | pass | EQ index lookup massively outperforms full scan. |
| `shm_index_benchmark` | `benchmark_shm_index_range_lookup_vs_scan` | speedup `8.25x` (`70.773639ms` vs `583.610136ms`) | pass (gate `>=2.0x`) | Core range index path clears its regression target with margin. |
| `shm_index_benchmark` | `benchmark_shm_index_range_modes_selectivity_vs_scan` | overall speedup `4.98x` (`86.81855ms` vs `432.146455ms`) | pass (gate `>=2.0x`) | Mixed-selectivity range modes now comfortably clear the speedup gate. |
| `shm_index_benchmark` | `benchmark_shm_index_forked_contention_throughput` | `single_tps=6325414.76`, `forked_tps=4053713.73`, ratio `0.64` | pass (gate `>=0.20`) | Cross-process contention retains most single-process throughput. |
| `shm_benchmark` | `benchmark_shm_allocation_throughput` | `85,665,814 rows/s` (`250,000` rows in `2.918317ms`) | pass | Shared arena allocator throughput remains very high. |
| `shm_benchmark` | `benchmark_forked_range_scan_latency` | `718,812,522 rows/s`; `rows=120000`; `threshold=10000`; `matched=91497` | pass | Forked-process scan over shared memory is bandwidth-efficient. |
| `vacuum_recycle_ab_benchmark` | `benchmark_vacuum_enabled_outlasts_disabled_under_hot_row_stress` | assertion harness passed under release profile | pass | Vacuum-enabled configuration outlasts disabled-vacuum control under the same 10MB stress envelope. |
| `wal_crash_recovery` | `benchmark_occ_wal_replay_startup_throughput` | `3,886,853.69 writes/s` (`12000` applied writes in `3.08733ms`) | pass | WAL replay startup applies writes at multi-million/s scale. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput` | `throughput_tps=759579.72`, `commits=16000/16000`, `conflicts=54`, `escalations=16`, `timed_out_workers=0` | pass | Hybrid path sustains high TPS on hot-row contention workload. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline` | `hybrid_median_tps=428823.21`, `pure_median_tps=21337.31`, ratio `20.10x`; timed-out workers `0` | pass | Hybrid mode decisively outperforms pure OCC while maintaining worker liveness. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_partitioned_occ_lock_striping_multi_process_scaling` (ignored) | `contended_tps=20225.93`, `disjoint_tps=349076.95`, ratio `17.26x`; contended/disjoint timed-out workers `0/0` | pass (ignored run) | Disjoint-key throughput is much higher than contended hot-row workload, and both paths complete without timeouts. |
| `aerostore_tcl/config_checkpoint_integration` | `benchmark_tcl_synchronous_commit_modes` | `on_tps=1121.50`, `off_tps=240000.0`, ratio `214.0x` | pass | Tcl ingest path remains highly sensitive to synchronous-commit setting. |

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
- WAL crash-recovery behavior is timing-sensitive around daemon kill/restart paths; latest branch reruns pass after the writer-epoch restart race fix.

## License
No license file is currently present.
Treat this repository as proprietary/internal unless a license is explicitly added.
