# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

Aerostore is a prototype Rust-first shared-memory database prototype focused on high-ingest, update-heavy workloads and clean durability / restart support (roughly the same durability class as PostgreSQL with async commit).

The project is optimized around:
- process-shared memory structures (`RelPtr<T>`, arena allocation),
- serializable OCC with hybrid optimistic/pessimistic hot-row handling,
- WAL durability with checkpoint and recovery,
- delta-encoded WAL updates to reduce write amplification,
- cross-process vacuum + shared free-list recycling for update-heavy MVCC churn,
- tmpfs warm restarts to avoid routine cold-start replay,
- Tcl FFI ingestion/search integration.

Under benchmark conditions ("The Crucible" benchmark here), aerostore has achieved an order-of-magnitude performance improvement over PostgreSQL. However, performance and memory behavior over long periods of sustained pressure remains an area of active research. As of now, aerostore is not recommended yet for serious production use cases.

## Table of Contents
- [Current Scope](#current-scope)
- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Why Aerostore Can Beat Postgres in Crucible](#why-aerostore-can-beat-postgres-in-crucible)
- [Bound-Seeking SkipList Range Scans](#bound-seeking-skiplist-range-scans)
- [Hybrid OCC Livelock Mitigation](#hybrid-occ-livelock-mitigation)
- [Delta-Encoded WAL](#delta-encoded-wal)
- [Tmpfs Warm Restart](#tmpfs-warm-restart)
- [Cross-Process Vacuum GC](#cross-process-vacuum-gc)
- [Memory and Sustained-Performance Evolution](#memory-and-sustained-performance-evolution)
- [Tcl Bridge](#tcl-bridge)
- [Build and Prerequisites](#build-and-prerequisites)
- [Quick Start](#quick-start)
- [Testing Runbook](#testing-runbook)
- [Benchmark Runbook](#benchmark-runbook)
- [Latest Validated Results (2026-03-04)](#latest-validated-results-2026-03-04)
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

## Why Aerostore Can Beat Postgres in Crucible
The observed `~5x` to `~12x` TPS gains in Crucible are real for this benchmark shape, and come from systems-path differences more than any single micro-optimization:

1. Lower IPC/protocol overhead:
   Aerostore runs in-process/shared-memory; it avoids the client/server SQL protocol and much of the network round-trip + executor framing cost that appears in end-to-end Postgres client latency.
2. Narrow, workload-specific hot path:
   Crucible is mostly keyed upserts and bounded scans on a fixed schema (`FlightState`), which maps well to Aerostore’s direct shared-memory row/index paths.
3. Reduced write amplification:
   Delta-encoded WAL records persist only changed columns for updates, reducing commit-side serialization and WAL byte volume.
4. Hot-key contention controls:
   Hybrid OCC (backoff + pessimistic fallback) prevents collapse under concentrated key collisions.
5. Sustained-churn reclaim work:
   Vacuum/index-GC/recycle improvements reduce allocator retry storms and keep throughput from collapsing as churn accumulates.

Important caveat:
- This is not a universal claim that Aerostore is always faster than Postgres.
- The measured edge is workload- and configuration-dependent.
- In async durability modes, both systems can acknowledge writes that are later lost on hard crash; compare durability settings apples-to-apples.

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

## Memory and Sustained-Performance Evolution
This section summarizes the major memory/performance changes made after the prior qualitative phase (see [docs/crucible_10x_attempt_log_2026-03-03.md](/home/zpconn/code/aerostore/docs/crucible_10x_attempt_log_2026-03-03.md) and [docs/crucible_allocator_telemetry_2026-03-03.md](/home/zpconn/code/aerostore/docs/crucible_allocator_telemetry_2026-03-03.md)).

### What the old bottleneck looked like
In sustained churn runs, Aerostore previously showed:
- large worker drain beyond the nominal 60s window,
- allocator retry storms (`retry_alloc` spikes, `max_insert_attempts` pinned at `4097`),
- and throughput collapse at smaller memory profiles.

The allocator telemetry study showed this was mainly a reuse/pressure problem (high pop misses and poor reuse fit under churn), not just "live bytes only go up forever."

### Engine changes made since that phase
1. Class-segregated recycle lists + local recycle cache in `aerostore_core/src/shm.rs`.
   - Added class-keyed free-list heads and counters (`free_list_class_heads`, class push/pop/miss telemetry).
   - Added per-class local recycle caches to reduce cross-process CAS pressure.
   - `alloc_raw_in_class` now prefers same-class recycled blocks before bumping high-water.
2. OCC row-version recycle locality improvements in `aerostore_core/src/occ_partitioned.rs`.
   - Added primary-shard + bounded probe-shard recycle pop strategy.
   - Added "starved stash" fallback path and telemetry (`alloc_from_starved/primary/probe`, `stash_starved`) to avoid pathological misses.
3. Direct index move/relink fast path in `aerostore_core/src/shm_index.rs` and `aerostore_core/src/shm_skiplist.rs`.
   - `try_move_payload` now attempts `try_move_relink_fast` / `move_payload_relink` before falling back to remove+insert.
   - This reduces node/posting churn for key updates and lowers allocator pressure in update-heavy workloads.
4. Pressure-aware skiplist allocator behavior in `aerostore_core/src/shm_skiplist.rs`.
   - Added pressure state machine (`normal/warm/hot`) with hysteresis.
   - Added reserve pools for nodes/postings/towers and pressure-aware pop/push behavior.
   - Added retry/GC telemetry (`retry_loops`, `gc_assist_calls`, `gc_assist_reclaimed`, `pressure_to_*`, reserve hit/miss counters).
5. Retired-node reclaim and GC assist hardening in `aerostore_core/src/shm_skiplist.rs`.
   - FIFO retired queue accounting and reclaim counters are surfaced (`retired_backlog`, reclaimed deltas).
   - GC assist now reacts during pressure/retry loops to keep churn bounded.
6. Crucible observability expansion in `aerostore_core/benches/hyperfeed_crucible.rs`.
   - Profile matrix now includes `profile_2g` and `profile_3584m` in the standard run.
   - Output now includes richer allocator/index/epoch telemetry so bottlenecks are diagnosable from benchmark output directly.

### Measured impact (sustained 60s crucible)
Baseline values below are from the prior qualitative phase log; current values are from the latest validated run in this README.

| Profile | Prior Phase TPS Ratio (Aerostore/Postgres) | Latest TPS Ratio | Prior Aerostore Elapsed | Latest Aerostore Elapsed | Index Failures (Latest) | What Changed Qualitatively |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `profile_512m` | `2.52x` | `5.20x` | `69.206s` | `61.224s` | `0/0` | Major improvement, but still the highest-pressure regime (`retry_loops` and pressure transitions remain non-zero). |
| `profile_1g` | `6.30x` | `10.57x` | `73.073s` | `60.118s` | `0/0` | Moved from sub-10x to sustained 10x+ with drain largely eliminated. |
| `profile_2g` | `11.46x` (later addendum run) | `11.90x` | `60.143s` | `60.124s` | `0/0` | Sustained >10x remains stable with low retry pressure. |
| `profile_3584m` | n/a in prior qualitative baseline | `12.44x` | n/a | `60.102s` | `0/0` | Highest throughput in current matrix, with clean allocator/index telemetry. |

### Current state
- The previous sustained-churn collapse mode is materially reduced.
- Index insert/remove failures are `0` across the latest 60s matrix.
- `1g+` profiles are operating in a much healthier retry/pressure envelope than before.
- `512m` still shows pressure transitions and higher retry activity, so this is improved but not "no-pressure" at the smallest memory tier.

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

### Recycle, allocator, and index-delta invariant expansions
```bash
cargo test -p aerostore_core flush_local_recycle_cache_exports_offsets_cross_thread -- --nocapture
cargo test -p aerostore_core free_list_depth_estimate_reports_truncation_when_scan_limit_hit -- --nocapture
cargo test -p aerostore_core recycle_raw_in_class_invalid_block_does_not_mutate_free_list_counters -- --nocapture
cargo test -p aerostore_core general_class_path_bypasses_local_cache_behavior -- --nocapture
cargo test -p aerostore_core starved_slot_roundtrip_isolated_by_recycle_key -- --nocapture
cargo test -p aerostore_core allocate_row_prefers_starved_before_other_sources -- --nocapture
cargo test -p aerostore_core allocate_row_uses_primary_recycle_head_before_probe -- --nocapture
cargo test -p aerostore_core allocate_row_uses_probe_when_primary_empty -- --nocapture
cargo test -p aerostore_core recycle_telemetry_invariant_holds_in_single_recycle_cycle -- --nocapture
cargo test -p aerostore_core alloc_node_prefers_reserve_node_offsets_under_pressure -- --nocapture
cargo test -p aerostore_core alloc_tower_prefers_reserve_tower_and_can_reuse_taller_tower -- --nocapture
cargo test -p aerostore_core pressure_state_forces_hot_when_reclaim_efficiency_collapses -- --nocapture
cargo test -p aerostore_core maybe_collect_garbage_on_alloc_failure_updates_failure_and_gc_counters -- --nocapture
cargo test -p aerostore_core --test shm_recycle_visibility -- --nocapture
cargo test -p aerostore_core --test index_relink_gc_consistency -- --nocapture
cargo test -p aerostore_core --test occ_recycle_invariants_single_thread -- --nocapture
cargo test -p aerostore_tcl resolve_scan_mode_invalid_chunk_rows_falls_back_to_defaults -- --nocapture
cargo test -p aerostore_tcl apply_row_delta_updates_only_changed_indexes -- --nocapture
cargo test -p aerostore_tcl apply_row_delta_delete_then_reinsert_restores_full_index_state -- --nocapture
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

"The Crucible" note:
- runs Aerostore against Postgres on a simulated workload, both with async commit, with 16 concurrent workers. 80% of operations are keyed upserts, mixing disjoint keys and 5% heavily contended hot-keys. 20% of operations are time-based range scans utilizing indexes.
- requires Docker (`testcontainers` starts a local PostgreSQL container).
- runs four Aerostore memory profiles per invocation: `profile_512m`, `profile_1g`, `profile_2g`, and `profile_3584m`.
- default sustained duration is 60 seconds; override with `AEROSTORE_CRUCIBLE_DURATION_SECS` for local smoke runs.
- optional profile filter: `AEROSTORE_CRUCIBLE_PROFILE_FILTER=profile_1g` (or another profile name) to run a subset.
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

## Latest Validated Results (2026-03-04)
This section reports a full rerun of workspace tests plus the full benchmark suite (all Criterion benches, release benchmark-style suites, and 60-second crucible profiles).

Run scope:
- Workspace test sweep executed: `cargo test --workspace -- --nocapture` (pass).
- Added correctness-expansion suites executed for recycle visibility, recycle ordering invariants, skiplist reserve/pressure behavior, and Tcl index-delta application paths.
- Criterion bench suites executed: 6 (`procarray_snapshot`, `wal_delta_throughput`, `shm_skiplist_adversarial`, `shm_skiplist_seek_bounds`, `tmpfs_warm_restart`, `hyperfeed_crucible`).
- Criterion benchmark entrypoints executed: 21.
- Release benchmark-style suites executed: 10 suites / 30 benchmark functions.
- Crucible profiles executed in one invocation: `profile_512m`, `profile_1g`, `profile_2g`, `profile_3584m`.
- Overall status: full test + benchmark coverage completed; all benchmark gates in this run passed.

### Full Test Sweep Status (2026-03-04)
| Check Group | Result | Notes |
| --- | --- | --- |
| `cargo test --workspace -- --nocapture` | pass | Full workspace sweep passed after rerunning with host-level permissions for `/dev/shm` tmpfs tests. |
| tmpfs warm-restart tests and benches | pass | `/dev/shm` mapping paths passed under host permissions (`warm_restart_chaos`, `warm_restart_expansions`, `tmpfs_warm_restart`). |
| WAL crash recovery suites | pass | `11/11` in `wal_crash_recovery`; startup replay throughput benchmark passed. |
| vacuum and recycle suites | pass | stress, leader handoff, free-list invariants, and A/B benchmark all passed. |
| recycle/index-delta expansion suites | pass | new allocator/recycle invariants, OCC recycle-order tests, skiplist pressure/reserve checks, and Tcl index-delta tests all passed. |
| hot-row OCC contention suites | pass | release benchmark runs reported `timed_out_workers=0` across hybrid, pure, and scaling checks. |

### "The Crucible" Diagnostic (2026-03-04)
Command:
```bash
AEROSTORE_CRUCIBLE_DURATION_SECS=60 cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Latest full passing run (`AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS=50`, `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS=50`):

| Profile | Aerostore TPS | Postgres TPS | TPS Ratio | Aerostore p99 (us) | Postgres p99 (us) | Index Remove Failures | Index Insert Failures | Vacuum Reclaimed Rows | Index Reclaimed Nodes (delta) | Retry Loops | Max Insert Attempts | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `profile_512m` | `458579.35` | `88130.21` | `5.20x` | `32.77` | `262.14` | `0` | `0` | `22,460,939` | `20,366,241` | `633,779` | `4097` | pass |
| `profile_1g` | `925142.73` | `87540.69` | `10.57x` | `65.54` | `262.14` | `0` | `0` | `44,494,428` | `40,773,454` | `0` | `1` | pass |
| `profile_2g` | `1048833.70` | `88141.42` | `11.90x` | `32.77` | `262.14` | `0` | `0` | `50,448,350` | `46,358,569` | `0` | `1` | pass |
| `profile_3584m` | `1096988.71` | `88202.20` | `12.44x` | `32.77` | `262.14` | `0` | `0` | `52,745,075` | `49,440,419` | `0` | `1` | pass |

Engine timing details for the same run:

| Profile | Aerostore elapsed (s) | Postgres elapsed (s) | Notes |
| --- | ---: | ---: | --- |
| `profile_512m` | `61.224` | `60.002` | Slight worker drain beyond nominal 60s window; no index insert/remove failures. |
| `profile_1g` | `60.118` | `60.002` | Stable 10x+ throughput regime; no index failures. |
| `profile_2g` | `60.124` | `60.002` | 11.9x throughput with zero index failures and clean p99 gate. |
| `profile_3584m` | `60.102` | `60.002` | Best TPS in this run while preserving zero index failures. |

Interpretation:
- Aerostore beat tuned Postgres in all four profiles (`5.20x` to `12.44x` TPS) with zero index insert/remove failures.
- The 512 MiB profile still entered allocator pressure (higher retry/alloc-failure telemetry), while 1 GiB+ profiles remained in the stable low-retry regime.
- Postgres server execution for scans stayed low relative to client RTT, indicating IPC/protocol overhead remains a major part of its end-to-end path.

### Criterion Benchmarks (`aerostore_core/benches`)
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `procarray_snapshot` | `procarray_snapshot/10` | `[42.378 ns 42.436 ns 42.495 ns]`; proof `txid_10=40.08ns` | pass | Snapshot read latency remains in low tens-of-ns for small txid values. |
| `procarray_snapshot` | `procarray_snapshot/10000000` | `[42.234 ns 42.275 ns 42.322 ns]`; proof `txid_10_000_000=40.16ns` | pass | Snapshot latency remains flat at large txid values (no txid-growth cliff). |
| `wal_delta_throughput` | `full_row_100k_one_col` | `[4.1453 ms 4.1491 ms 4.1567 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for one-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_one_col` | `[66.422 ms 66.520 ms 66.620 ms]`; delta bytes `4,799,987`; reduction `94.12%` | pass/info | Delta encoding dramatically reduces WAL byte volume for sparse updates. |
| `wal_delta_throughput` | `full_row_100k_two_col` | `[4.1541 ms 4.1609 ms 4.1719 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for two-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_two_col` | `[69.274 ms 69.543 ms 69.750 ms]`; delta bytes `6,099,987`; reduction `92.52%` | pass/info | Delta path preserves >90% byte savings with two dirty fields. |
| `wal_delta_throughput` | `full_row_100k_four_col` | `[4.6586 ms 4.6770 ms 4.6899 ms]`; full bytes `81,600,000` | pass/info | Full-row baseline for four-column mutation workload. |
| `wal_delta_throughput` | `delta_row_100k_four_col` | `[72.160 ms 72.280 ms 72.466 ms]`; delta bytes `7,399,987`; reduction `90.93%` | pass/info | Delta remains strongly space-efficient as dirty-column count increases. |
| `shm_skiplist_adversarial` | `p99_ns` | `small=2700ns`, `medium=3200ns`, `large=4100ns`, slopes `0.289/0.431`, criterion `[183.02 ps 183.19 ps 183.38 ps]` | pass (gate `<5000ns`) | Adversarial skiplist lookup p99 remains within strict latency budget. |
| `shm_skiplist_seek_bounds` | `gt_999_990` | criterion `[80.293 ns 80.436 ns 80.613 ns]`; computed avg `80.87ns`; budget `5000ns` | pass | Bound-seeking traversal bypasses nearly the entire keyspace for high lower bounds. |
| `tmpfs_warm_restart` | `warm_attach_boot` | `[51.989 us 52.327 us 52.629 us]` | pass | Warm attach remains microsecond-scale. |
| `tmpfs_warm_restart` | `cold_boot_fixture_build` | `[4.4296 ms 4.4589 ms 4.4771 ms]` | pass/info | Cold fixture rebuild remains millisecond-scale and much slower than warm attach. |
| `tmpfs_warm_restart` | `post_restart_update_throughput` | `[2.0125 ms 2.0201 ms 2.0255 ms]` | pass/info | Post-restart write throughput remains immediate after warm attach. |

Notes:
- `tmpfs_warm_restart` and tmpfs-backed tests/benchmarks were run with host permissions so `/dev/shm` paths are exercised on real tmpfs mappings.

### Benchmark-Style Test Suites
| Suite | Benchmark | Key Results | Status | What It Means |
| --- | --- | --- | --- | --- |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes` | `sync_tps=1149.62`, `async_tps=1182507.64`, ratio `1028.60x` | pass | Async WAL commit remains orders of magnitude faster than sync commit in this workload. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_parallel_disjoint_keys` | `sync_tps=1193.44`, `async_tps=1855239.60`, ratio `1554.53x` | pass | Async advantage increases further under parallel disjoint-key ingest. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_savepoint_churn` | `sync_tps=1145.49`, `async_tps=889357.68`, ratio `776.40x` | pass | Savepoint-heavy write churn still heavily favors async commit mode. |
| `wal_ring_benchmark` | `benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts` | `sync_tps=1217.30`, `async_tps=604560.71`, ratio `496.64x` | pass | Tcl-style keyed-upsert path remains highly commit-mode sensitive. |
| `wal_ring_benchmark` | `benchmark_logical_async_vs_sync_commit_modes` | `sync_tps=1057.42`, `async_tps=805658.88`, ratio `761.91x` | pass | Logical WAL path keeps a very large async-vs-sync throughput gap. |
| `occ_checkpoint_benchmark` | `benchmark_occ_checkpoint_latency_by_cardinality` | `10k: 2.406956ms (240.70ns/row)`, `100k: 5.103431ms (51.03ns/row)`, `1M: 25.854745ms (25.85ns/row)` | pass | Checkpoint latency scales smoothly and amortizes well with larger row counts. |
| `occ_checkpoint_benchmark` | `benchmark_logical_snapshot_and_replay_recovery_by_cardinality` | `10k: 24.682396ms (435,533.08 rows/s)`, `100k: 204.769569ms (492,016.47 rows/s)`, `1M: 1.9754387s (506,596.33 rows/s)` | pass | Snapshot+replay recovery throughput remains stable across cardinality growth. |
| `query_index_benchmark` | `benchmark_shared_index_indexed_range_scan_with_sort_and_limit` | ingest `100k` rows in `52.808069ms`; `512` scans in `598.780568ms` | pass | Indexed range+sort+limit path sustains high concurrent scan throughput. |
| `query_index_benchmark` | `benchmark_stapi_parse_compile_execute_vs_typed_query_path` | typed `57.120493ms`; STAPI `6.896766ms` | pass | STAPI parse/compile/execute route is faster than typed path in this scenario. |
| `query_index_benchmark` | `benchmark_tcl_style_alias_match_desc_offset_limit_path` | `10.432720671s` | pass | End-to-end alias/match/offset/limit query path cost at current workload size. |
| `query_index_benchmark` | `benchmark_tcl_bridge_style_stapi_assembly_compile_execute` | `8.419170493s` | pass | Full Tcl-bridge-style STAPI assembly+compile+execute overhead profile. |
| `query_index_benchmark` | `benchmark_stapi_rbo_pk_point_lookup_vs_full_scan` | pk `24.086us`; scan `15.644328701s` | pass | Planner PK route still avoids catastrophic full scan cost. |
| `query_index_benchmark` | `benchmark_stapi_rbo_tiebreak_dest_over_altitude` | `1.409988079s` | pass | Captures planner tie-break behavior under competing candidate indexes. |
| `query_index_benchmark` | `benchmark_stapi_rbo_cardinality_trap_flight_id_over_aircraft_type` | `40.508us` | pass | Planner avoids the cardinality trap and chooses a selective route. |
| `query_index_benchmark` | `benchmark_stapi_residual_negative_filters_with_index_driver` | `1.689679607s` | pass | Quantifies residual negative-filter cost on index-driven execution. |
| `query_index_benchmark` | `benchmark_stapi_null_notnull_residual_filters` | `988.466507ms` | pass | Captures null/not-null residual filtering overhead in STAPI path. |
| `shm_index_benchmark` | `benchmark_shm_index_eq_lookup_vs_scan` | speedup `498.80x` (`208.436us` vs `103.968775ms`) | pass | EQ index lookup strongly outperforms full scan baseline. |
| `shm_index_benchmark` | `benchmark_shm_index_range_lookup_vs_scan` | speedup `10.67x` (`54.642966ms` vs `582.937023ms`) | pass (gate `>=2.0x`) | Core range index path clears speedup gate with margin. |
| `shm_index_benchmark` | `benchmark_shm_index_range_modes_selectivity_vs_scan` | overall speedup `6.62x` (`64.751745ms` vs `428.52998ms`) | pass (gate `>=2.0x`) | Mixed selectivity range modes remain materially faster than scan fallback. |
| `shm_index_benchmark` | `benchmark_shm_index_forked_contention_throughput` | `single_tps=29368.47`, `forked_tps=109811.85`, ratio `3.74` | pass (gate `>=0.20`) | Cross-process indexing scales above single-process baseline in this run. |
| `shm_benchmark` | `benchmark_shm_allocation_throughput` | `74,810,654 rows/s` (`250,000` rows in `3.34177ms`) | pass | Shared arena allocation throughput remains high. |
| `shm_benchmark` | `benchmark_forked_range_scan_latency` | `502,874,767 rows/s`; `rows=120000`; `threshold=10000`; `matched=91497` | pass | Forked-process scan over shared memory remains bandwidth-efficient. |
| `vacuum_recycle_ab_benchmark` | `benchmark_vacuum_enabled_outlasts_disabled_under_hot_row_stress` | assertion harness passed under release profile | pass | Vacuum-enabled mode still outlasts disabled vacuum in the fixed 10MB stress envelope. |
| `wal_crash_recovery` | `benchmark_occ_wal_replay_startup_throughput` | `3,436,778.46 writes/s` (`12000` applied writes in `3.491642ms`) | pass | WAL replay startup still applies writes at multi-million/s scale. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput` | `throughput_tps=678011.92`, `commits=16000/16000`, `conflicts=51`, `escalations=17`, `timed_out_workers=0` | pass | Hybrid OCC path sustains high hot-row throughput while keeping worker liveness. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline` | `hybrid_median_tps=389520.60`, `pure_median_tps=23744.89`, ratio `16.40x`; timed-out workers `0` | pass | Hybrid mode remains decisively faster than pure OCC under hot-key contention. |
| `occ_partitioned_lock_striping_benchmark` | `benchmark_partitioned_occ_lock_striping_multi_process_scaling` (ignored) | `contended_tps=17660.14`, `disjoint_tps=349051.91`, ratio `19.76x`; contended/disjoint timed-out workers `0/0` | pass (ignored run) | Disjoint-key mode remains much faster while both modes complete without timeouts. |
| `aerostore_tcl/config_checkpoint_integration` | `benchmark_tcl_synchronous_commit_modes` | `on_tps=1102.94`, `off_tps=30769.23`, ratio `27.90x` | pass | Tcl ingest path continues to show clear throughput sensitivity to sync-commit mode. |

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
