# Nightly Performance Runbook

This runbook is for isolated nightly/performance jobs. It intentionally includes suites that are ignored in default `cargo test` runs.

## Release Build Sanity

```bash
cargo test --workspace --release --no-run
```

## Criterion Benches

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
cargo bench -p aerostore_core --bench wal_delta_throughput
cargo bench -p aerostore_core --bench shm_skiplist_adversarial
cargo bench -p aerostore_core --bench shm_skiplist_seek_bounds
cargo bench -p aerostore_core --bench tmpfs_warm_restart
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

`hyperfeed_crucible` notes:
- requires Docker daemon access (PostgreSQL is launched via `testcontainers`).
- runs two Aerostore profiles per execution (`profile_512m`, `profile_1g`).
- defaults to a 60-second sustained workload; set `AEROSTORE_CRUCIBLE_DURATION_SECS` for shorter smoke runs.
- optional daemon cadence controls:
  - `AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS`
  - `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS`
- output includes:
  - Aerostore/Postgres TPS and latency ratios,
  - PostgreSQL server-exec vs client-RTT scan breakdown,
  - Aerostore index update failure counters and reclaim telemetry deltas.
- exits with a clear error when Docker is unavailable.

## Benchmark-Style Test Suites (Release)

Run with single-threaded harness scheduling for stability:

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test query_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_index_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test shm_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test vacuum_recycle_ab_benchmark -- --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test wal_crash_recovery benchmark_occ_wal_replay_startup_throughput -- --test-threads=1 --nocapture
cargo test -p aerostore_tcl --release --test config_checkpoint_integration benchmark_tcl_synchronous_commit_modes -- --test-threads=1 --nocapture
```

## V4 Shared-Index Validation Gate

All commands below are required to pass for V4 shared-index changes:

```bash
cargo test -p aerostore_core --release --test shm_index_bounds -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test shm_index_fork -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test shm_index_contention -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test shm_index_gc_horizon -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test shm_index_benchmark -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
```

## Expanded Stability and Correctness Gates

Nightly jobs should also run these suites that protect recent architecture changes:

```bash
cargo test -p aerostore_core --test wal_delta_codec -- --nocapture
cargo test -p aerostore_core --test wal_delta_recovery_pk_map -- --nocapture
cargo test -p aerostore_core --test tmpfs_warm_restart_chaos -- --nocapture --test-threads=1
cargo test -p aerostore_core --test tmpfs_warm_restart_expansions -- --nocapture --test-threads=1
cargo test -p aerostore_core --test occ_row_lock_semantics -- --nocapture
cargo test -p aerostore_core --test procarray_concurrency procarray_disjoint_writes_have_lower_conflicts_than_hot_row_contention -- --nocapture
cargo test -p aerostore_core --test vacuum_recycle_stress -- --nocapture
cargo test -p aerostore_core --test vacuum_leader_handoff -- --nocapture
cargo test -p aerostore_core --test vacuum_free_list_invariants -- --nocapture --test-threads=1
cargo test -p aerostore_core --test vacuum_recycle_ab_benchmark -- --nocapture
cargo test -p aerostore_tcl --lib vacuum_index_cleanup_tests -- --nocapture
```

## Explicit Ignored Stress Suites

Run these in isolated jobs with single-threaded test harness scheduling:

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

## Known Intermittent Flake

`wal_crash_recovery::async_wal_daemon_restart_does_not_persist_rolled_back_savepoint_intents` may fail sporadically under host contention. Nightly automation should rerun that suite once before flagging a hard regression.
