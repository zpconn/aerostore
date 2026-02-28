# Nightly Performance Runbook

This runbook is for isolated nightly/performance jobs. It intentionally includes suites that are ignored in default `cargo test` runs.

## Release Build Sanity

```bash
cargo test --workspace --release --no-run
```

## Core Throughput and Recovery Suites

```bash
cargo test -p aerostore_core --release --test wal_ring_benchmark -- --test-threads=1
cargo test -p aerostore_core --release --test occ_checkpoint_benchmark -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test wal_crash_recovery -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test query_index_benchmark -- --nocapture --test-threads=1
cargo test -p aerostore_core --release --test shm_index_benchmark -- --nocapture --test-threads=1
```

## Explicit Ignored Contention/Stress Suites

Run these in isolated jobs with single-threaded test harness scheduling:

```bash
cargo test -p aerostore_core --release --test occ_partitioned_lock_striping_benchmark -- --ignored --test-threads=1 --nocapture
cargo test -p aerostore_core --release --test test_concurrency -- --ignored --test-threads=1
```

## Criterion

```bash
cargo bench -p aerostore_core --bench procarray_snapshot
```
