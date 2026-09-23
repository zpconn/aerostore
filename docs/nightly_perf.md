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

The separate [Extended HyperFeed Crucible](extended_crucible.md) exercises complete
synthetic message transactions and native isolation contracts:

```bash
cargo bench -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine both --mode all --families 32 --cycles 2 --workers 4 \
  --output target/extended-crucible.json
```

Its full gate currently fails three Aerostore index/isolation contracts. Preserve
that failure in automation; a bounded replay pass is not a substitute. This
finite, phased workload complements the sustained churn checks below.

`hyperfeed_crucible` notes:
- comparison mode requires Docker daemon access (PostgreSQL is launched via `testcontainers`).
- runs four profiles by default (`profile_512m`, `profile_1g`, `profile_2g`, `profile_3584m`); select profiles with `AEROSTORE_CRUCIBLE_PROFILE_FILTER`.
- defaults to a 60-second sustained workload; set `AEROSTORE_CRUCIBLE_DURATION_SECS` for shorter smoke runs.
- optional daemon cadence controls:
  - `AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS`
  - `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS`
- output includes:
  - Aerostore/Postgres TPS and latency ratios,
  - PostgreSQL server-exec vs client-RTT scan breakdown,
  - Aerostore index update failure counters and reclaim telemetry deltas.
- stops starting transactions at the deadline and completes index maintenance for every committed row before counting the operation. Worker failures, scan failures, maintenance failures, and daemon shutdown errors fail the run.
- compares the final raw index traversal with all 50,000 committed table rows, including ordering and duplicate detection; drains retired node/posting queues and requires zero GC recycle errors. The final allocation census requires every index node, posting, tower, and physical tower lane to be reachable or reusable, with no duplicate ownership or unaccounted structural storage.
- prints interval TPS, arena high-water growth, successful fresh allocation bytes by class, and reclamation/backlog counters every five seconds (override with `AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS`). `AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=/tmp/alloc_{profile}.csv` also writes cumulative allocator counters.
- runs lasting at least 30 seconds require second-half fresh arena growth to stay within one seeded working-set footprint and second-half interval TPS to retain at least 50% of first-half TPS. The generous throughput bound catches a cliff without imposing a machine-specific minimum. These finite-run gates do not prove unlimited-duration memory stability.
- exits with a clear error when Docker is unavailable in comparison mode.

Run the identical Aerostore workload and correctness gates without PostgreSQL:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_PROFILE_FILTER=profile_2g \
AEROSTORE_CRUCIBLE_DURATION_SECS=120 \
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

A smaller arena exposes memory pressure sooner. Arena overrides are restricted to diagnostic mode so comparison profiles remain reproducible:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_DURATION_SECS=240 \
AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=/tmp/crucible_128m.csv \
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Short runs (under 30 seconds) execute the correctness gates and report `status=short_run`; they do not establish sustained performance. Aerostore-only runs do not claim a PostgreSQL performance ratio.

Long-run parity gate (nightly):
```bash
./scripts/check_crucible_2g_120_vs_240.sh
```

This gate runs `profile_2g` back-to-back at 120s and 240s and fails if:
- 240s Aerostore TPS < 90% of 120s Aerostore TPS,
- 240s TPS ratio (Aerostore/Postgres) < 90% of 120s ratio,
- either run fails exact table/index agreement, clean GC drain, or exact structural allocation ownership,
- either run has index insert/remove failures,
- either run has `max_insert_attempts > 128` or end `pressure_state == HOT`,
- either run fails the interval throughput or bounded fresh-growth gate,
- or either run’s measured workload and worker drain exceeds its duration by more than one second (WAL/daemon shutdown and the final audits are outside this timing).

The same 120s/240s stability gate runs without Docker when `AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1` is set. Only PostgreSQL comparison checks are omitted:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 ./scripts/check_crucible_2g_120_vs_240.sh
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 AEROSTORE_CRUCIBLE_SHM_MIB=128 \
  AEROSTORE_CRUCIBLE_LOG_DIR=/tmp/crucible_128m_compare \
  ./scripts/check_crucible_2g_120_vs_240.sh
```

## Focused Concurrency Models

Use the production lock with Loom atomics in a separate target directory:

```bash
RUSTFLAGS='--cfg aerostore_loom' \
CARGO_TARGET_DIR=/tmp/aerostore-loom-target \
cargo test -p aerostore_core --test shm_mutation_model --release
```

The five-model suite includes negative controls for the detached predecessor race and missing row guard. It has a preemption bound of 2 and a maximum of 10,000 branches, without time/permutation cutoffs. Use the `aerostore_loom` flag exactly; generic `loom` also changes dependency configurations. See [sustained churn correctness](sustained_churn_correctness.md) for invariants, regression commands, model limits, and shared-memory compatibility.

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
