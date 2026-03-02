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

1. `procarray_snapshot`
- Result:
  - `procarray_snapshot/10`: `[40.842 ns 40.872 ns 40.905 ns]`
  - `procarray_snapshot/10000000`: `[40.827 ns 40.863 ns 40.906 ns]`
  - proof line: `txid_10=41.28ns txid_10_000_000=41.18ns delta=0.10ns`
- Meaning:
  - snapshot lookup latency is effectively constant with respect to txid scale (no growth from 10 to 10M).

2. `wal_delta_throughput`
- Result:
  - one-column update workload:
    - full bytes: `81,600,000`
    - delta bytes: `4,799,987`
    - byte reduction: `94.12%`
    - full encode time: `[4.3262 ms 4.3349 ms 4.3417 ms]`
    - delta encode time: `[69.771 ms 69.849 ms 69.934 ms]`
  - two-column update workload:
    - byte reduction: `92.52%`
    - full encode time: `[4.3200 ms 4.3230 ms 4.3279 ms]`
    - delta encode time: `[72.749 ms 72.801 ms 72.885 ms]`
  - four-column update workload:
    - byte reduction: `90.93%`
    - full encode time: `[4.8149 ms 4.8178 ms 4.8212 ms]`
    - delta encode time: `[76.237 ms 76.357 ms 76.457 ms]`
- Meaning:
  - delta encoding dramatically reduces WAL byte volume, while this benchmark harness shows higher CPU encode cost for per-field delta packing.

3. `shm_skiplist_adversarial`
- Result:
  - `small_p99_ns=2200`, `medium_p99_ns=2800`, `large_p99_ns=3500`
  - slopes: `small->medium=0.410`, `medium->large=0.388`
  - criterion line: `[187.68 ps 188.11 ps 188.69 ps]`
  - gate status: pass (`large p99 < 5000ns`)
- Meaning:
  - shared skiplist lookup latency remains under strict p99 budget and scales sublinearly under adversarial load.

4. `shm_skiplist_seek_bounds`
- Result:
  - `keys=1,000,000`, query `> 999,990`
  - criterion line: `[77.630 ns 77.730 ns 77.844 ns]`
  - computed average: `77.85 ns`
  - gate status: pass (`< 5000 ns`)
- Meaning:
  - bound-seeking starts near tail and bypasses the first `999,990` keys, avoiding O(N) head scan behavior.

5. `tmpfs_warm_restart`
- Result:
  - `warm_attach_boot`: `[55.210 us 55.335 us 55.437 us]`
  - `cold_boot_fixture_build`: `[2.0275 ms 2.0322 ms 2.0360 ms]`
  - `post_restart_update_throughput`: `[1.7442 ms 1.7500 ms 1.7532 ms]`
  - note: this benchmark needed `/dev/shm` access and was run outside sandbox restrictions.
- Meaning:
  - warm attach path is much faster than rebuilding a cold fixture and supports immediate post-restart update throughput.

### Benchmark-Style Test Suites

1. `wal_ring_benchmark` (5 benchmark functions)
- Result:
  - `benchmark_async_synchronous_commit_modes`: `sync_tps=1228.10`, `async_tps=1360532.37`, ratio `1107.83x`
  - `benchmark_async_synchronous_commit_modes_parallel_disjoint_keys`: ratio `1014.28x`
  - `benchmark_async_synchronous_commit_modes_savepoint_churn`: ratio `733.14x`
  - `benchmark_async_synchronous_commit_modes_tcl_like_keyed_upserts`: ratio `601.04x`
  - `benchmark_logical_async_vs_sync_commit_modes`: ratio `911.88x`
- Meaning:
  - asynchronous WAL commit mode is vastly higher-throughput than synchronous mode in this benchmark environment.

2. `occ_checkpoint_benchmark` (2 benchmark functions)
- Result:
  - `benchmark_occ_checkpoint_latency_by_cardinality`:
    - `10k rows: 3.445576ms (344.56 ns/row)`
    - `100k rows: 6.623267ms (66.23 ns/row)`
    - `1M rows: 33.283742ms (33.28 ns/row)`
  - `benchmark_logical_snapshot_and_replay_recovery_by_cardinality`:
    - `10k rows: 21.920646ms (490,405 rows/s)`
    - `100k rows: 195.629607ms (515,004 rows/s)`
    - `1M rows: 1.816991139s (550,773 rows/s)`
- Meaning:
  - checkpoint and logical replay remain bounded and scale to high cardinalities with sustained per-row throughput.

3. `query_index_benchmark` (9 benchmark functions)
- Result:
  - `benchmark_shared_index_indexed_range_scan_with_sort_and_limit`: `100k` ingest in `50.836ms`, `512` scans in `787.330ms`
  - `benchmark_stapi_parse_compile_execute_vs_typed_query_path`: typed `91.627ms`, STAPI `8.428ms`
  - `benchmark_tcl_style_alias_match_desc_offset_limit_path`: `10.765663s`
  - `benchmark_tcl_bridge_style_stapi_assembly_compile_execute`: `8.685921s`
  - `benchmark_stapi_rbo_pk_point_lookup_vs_full_scan`: pk `14.574us`, scan `16.089316s`
  - `benchmark_stapi_rbo_tiebreak_dest_over_altitude`: `1.458486s`
  - `benchmark_stapi_rbo_cardinality_trap_flight_id_over_aircraft_type`: `54.913us`
  - `benchmark_stapi_residual_negative_filters_with_index_driver`: `1.752238s`
  - `benchmark_stapi_null_notnull_residual_filters`: `1.017999s`
- Meaning:
  - planner/index routing and STAPI execution paths strongly outperform equivalent scan-heavy paths in targeted scenarios.

4. `shm_index_benchmark` (4 benchmark functions)
- Result:
  - `benchmark_shm_index_eq_lookup_vs_scan`: speedup `486.27x` (pass)
  - `benchmark_shm_index_forked_contention_throughput`: ratio `0.78` (pass, gate `>=0.20`)
  - `benchmark_shm_index_range_lookup_vs_scan`: speedup `2.93x` (pass, gate `>=2.0`)
  - `benchmark_shm_index_range_modes_selectivity_vs_scan`: failed
    - overall speedup `1.69x` in full-suite run (`1.65x` on isolated rerun)
    - failure against gate `>=2.0x`
- Meaning:
  - equality and simple range lookups are healthy, but mixed selectivity range-mode benchmark currently underperforms its configured gate.

5. `shm_benchmark` (2 benchmark functions)
- Result:
  - `benchmark_shm_allocation_throughput`: `107,825,593 rows/s`
  - `benchmark_forked_range_scan_latency`: `660,182,210 rows/s` (`120k` rows, threshold `10000`)
- Meaning:
  - shared arena allocation and forked scan memory access are both very high-throughput on this host.

6. `wal_crash_recovery` benchmark function
- Result:
  - `benchmark_occ_wal_replay_startup_throughput`: `3,891,039 writes/s` (`12000` txns)
- Meaning:
  - OCC WAL replay startup path can apply recovered writes at multi-million-writes/second scale in this run.

7. `occ_partitioned_lock_striping_benchmark` (3 benchmark functions)
- Result:
  - `benchmark_hybrid_hot_row_backoff_and_pessimistic_throughput`:
    - `throughput_tps=940549.29`, `commits=16000/16000`, `conflicts=44`, `escalations=12` (pass)
  - `benchmark_hybrid_hot_row_outperforms_pure_occ_baseline`:
    - hybrid run samples: `482878.40 TPS`, `768711.85 TPS`, `826885.26 TPS`
    - pure OCC run: `43.60 TPS`, `commits=872/16000`, `timed_out_workers=16`
    - status: failed (`pure_occ` timed out workers assertion)
  - `benchmark_partitioned_occ_lock_striping_multi_process_scaling` (ignored run):
    - `contended_tps=1790.10`, `disjoint_tps=349069.76`, ratio `195.00x`
    - `contended_timed_out_workers=16`, `disjoint_timed_out_workers=0` (pass)
- Meaning:
  - hybrid mode can sustain very high hot-row throughput, while pure OCC collapses under extreme contention in this host run; disjoint-key workloads remain dramatically faster than contended ones.

8. `aerostore_tcl/config_checkpoint_integration` benchmark function
- Result:
  - `benchmark_tcl_synchronous_commit_modes`: `on_tps=987.65`, `off_tps=240000.0`, ratio `243.0x`
- Meaning:
  - Tcl ingest throughput is highly sensitive to synchronous durability mode; async mode is much faster in this benchmark.

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
