# Sustained repair validation — 2026-09-22

Environment: Intel Core Ultra 9 285K, 24 visible CPUs, x86_64 WSL2
(`6.6.87.2-microsoft-standard-WSL2`), Rust 1.93.1, Docker Engine 29.8.0,
PostgreSQL image `postgres:16`. Measurements are specific to this host.

The tested code is the working tree based on
`c96fd38ad048ab3ade095d22151341bf5e7ab0bd`, including the repairs and existing
uncommitted work. `source-sha256.txt` identifies the relevant source contents.
Validation was performed before committing; the source fingerprints identify the
tested working-tree contents.

`workspace-tests.log` records the successful release workspace suite;
`loom-tests.log` records all five bounded models, including both negative controls.
The model scope and remaining limitations are described in
[sustained churn correctness](../../sustained_churn_correctness.md).

Run commands, with no concurrent test or benchmark workloads:

```bash
AEROSTORE_CRUCIBLE_LOG_DIR="$PWD/docs/bench_data/crucible_fixed_2026-09-22/2g" \
./scripts/check_crucible_2g_120_vs_240.sh

AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_LOG_DIR="$PWD/docs/bench_data/crucible_fixed_2026-09-22/128m" \
./scripts/check_crucible_2g_120_vs_240.sh
```

Each directory contains the full 120-second and 240-second benchmark logs and
their allocation telemetry CSVs. The reduced-arena run retains the script's
historical `profile_2g` filenames; its log reports `profile=diagnostic` and
`aerostore_shm_bytes=134217728`.

Use the operation tables and named `hyperfeed_crucible_*` fields for workload
results. The trailing Criterion picosecond timings measure access to already
computed ratio values, not database operations; they are not performance evidence.

All four sustained runs passed. `results.json` contains the measurements, and
each directory's `gate-result.txt` records its script verdict. The complete
results are recorded in the linked correctness document. Each run passed table/index parity, a complete structural
allocation census, reclamation drain, zero operation failures, and the stated
memory and throughput gates. PostgreSQL comparison thresholds remain unchanged.
