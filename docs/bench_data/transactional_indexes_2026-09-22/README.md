# Transactional index validation — 2026-09-22–23

**The complete Extended Crucible gate passes on both Aerostore and PostgreSQL.** Every native contract passes, including the three that failed in the [preserved baseline](../extended_crucible_2026-09-22/README.md). Full physical state and emitted output match the reference after every replay phase. The directory name records the start date of this validation.

## Source and environment

The repairs are based on commit `3a30560ab3651bcac0b38c442b441a2d9ce30b4c`, which committed the original extended workload and its failing contracts. [source-sha256.txt](source-sha256.txt) fingerprints the final changed/new source files, manifests, and lockfile used by the focused tests, Tcl tests, and final benchmarks; unchanged files come from that base commit. The workspace compilation detail is recorded below. Verify from the repository root:

```sh
sha256sum -c docs/bench_data/transactional_indexes_2026-09-22/source-sha256.txt
```

Host: Intel Core Ultra 9 285K, 24 visible CPUs, WSL2 Linux `6.6.87.2-microsoft-standard-WSL2`, Rust `1.93.1`. Docker was `29.8.0`; the extended comparison used PostgreSQL `16.13 (Debian 16.13-1.pgdg13+1)`. Its settings are recorded in the report: serializable transactions, `fsync=on`, asynchronous commit, and a ten-second WAL writer delay. An initial sandbox invocation could not start its Docker container; the retained successful comparison was rerun with Docker access.

The new [native index protocol](../../transactional_indexes.md) uses shared predicate/publication metadata and explicit retries for incompatible older indexed snapshots. The bounded replay still coordinates declared family write slots. Both facts are part of the measured configuration. The rates in its JSON are not production HyperFeed speed claims or equal-durability comparisons.

## Extended workload and regressions

| Check | Result | Evidence |
| --- | --- | --- |
| Both engines, 32 families, four cycles, eight workers | PASS: 3,840 deliveries and 108 phase comparisons per engine; all six native contracts pass on each engine; exit 0 | [JSON](both_32f_4c_8w.json), [log](both_32f_4c_8w.log) |
| Aerostore, 64 families, 16 cycles, eight workers, 128 MiB arena | PASS: 30,720 deliveries, 432 phase comparisons, all six native contracts; exit 0; arena high-water mark 5,063,784 bytes | [JSON](aerostore_64f_16c_8w_128m.json), [log](aerostore_64f_16c_8w_128m.log) |
| Release workspace suite | PASS: 384 tests, two existing ignored tests, no failures | [log](workspace-tests.log), [command/exit](workspace-tests.json) |
| Final core and focused regressions | PASS: 234 tests, including 182 core unit tests, 20 native index tests, eight query tests, 21 extended model/adapter tests, and existing row-lock/vacuum regressions | [log](focused-tests.log), [command/exit](focused-tests.json) |
| Final Tcl integration tests | PASS: 16 tests, including warm attachment and native index maintenance | [log](tcl-tests.log), [command/exit](tcl-tests.json) |
| Undersized arena | Correctly fails: exit 2, failed replay report, stale success removed | [JSON](undersized_arena_failure.json), [log](undersized_arena_failure.log) |
| Killed worker | Correctly fails promptly: exit 2, failed replay report, stale success removed | [JSON](killed_worker_failure.json), [log](killed_worker_failure.log) |

The full workspace run includes every runtime protocol and performance change, plus the Tcl bridge, recovery, query benchmarks, and process tests. Its compilation preceded one final compatibility change: incrementing the private index-header version to reject the smaller publication-bucket layout used during development. The final focused and Tcl runs rebuild with that change and its new rejection test. The final benchmarks also use the new header version.

Each engine's shared replay committed 3,712 messages and deliberately aborted 128. The larger run committed 29,696 and deliberately aborted 1,024. Aerostore's final exact index and allocation ownership audits passed in both runs. The shared replay recorded 757 Aerostore retries and 9,917 PostgreSQL retries; the larger Aerostore replay recorded 7,395. Retry counters and latency include both predicate conflicts and exhausted contention waits.

New regressions also reproduce and check the snapshot-retention dependency discovered during the repair: an older writer can commit after a newer reader's snapshot, and vacuum must retain the old version needed by that reader. A separate progress test verifies that later readers do not perpetually inherit obsolete retention horizons. Another controlled test exhausts the real shared arena between two prepared index destinations and checks allocation-free rollback to the exact old state.

The failure-path checks first seeded fake successful reports, then verified nonzero exits, `passed: false`, removal of the stale marker, and a concrete replay error. [failure-path-validation.json](failure-path-validation.json) records their arguments. The worker check killed only a descendant `--internal-worker` of its own invocation. These checks validate failure reporting, not recovery of an arena after a worker dies while holding a lock.

## Sustained original Crucible

All four final 120/240-second runs pass the unchanged correctness, allocation ownership, reclamation, memory-growth, throughput-retention, and drain gates. The normal 2 GiB runs also pass the unchanged PostgreSQL thresholds: at least 2× throughput and at most 0.6× overall p99 latency. Across the four Aerostore runs, **225,213,445 operations** completed successfully.

| Arena | Duration | Aerostore ops/s | Overall p99, µs | Arena high-water, MiB | Second-half fresh bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 GiB | 120 s | 315,881.18 | 131.07 | 32.983 | 272,712 |
| 2 GiB | 240 s | 309,568.33 | 131.07 | 33.964 | 1,597,112 |
| 128 MiB | 120 s | 314,615.04 | 131.07 | 33.022 | 27,544 |
| 128 MiB | 240 s | 313,533.69 | 131.07 | 33.735 | 382,768 |

| Duration | PostgreSQL ops/s | Aerostore/PostgreSQL throughput | Overall p99 ratio |
| --- | ---: | ---: | ---: |
| 120 s | 51,098.01 | 6.18× | 0.500 |
| 240 s | 50,591.95 | 6.12× | 0.250 |

The longer run retains 98.00% of aggregate throughput at 2 GiB and 99.66% at 128 MiB. No index mutation failures, allocation failures, or unaccounted structural allocations were reported, and retired queues fully drained.

The latency tradeoff remains visible: **Aerostore update-only p99 is 524.29 µs versus PostgreSQL's 262.14 µs** in both comparisons. Passing the overall workload gate does not mean every operation has lower latency. The original benchmark's range probes count raw postings; Extended Crucible separately checks transactional indexed-query semantics. These measurements also do not establish equal crash durability or production HyperFeed performance.

The [machine-readable summary](sustained-summary.json), [2 GiB command and exit](churn_2g.json), [2 GiB log](churn_2g.log), [128 MiB command and exit](churn_128m.json), and [128 MiB log](churn_128m.log) retain the results. Individual logs and five-second allocation CSVs are in `churn_2g/` and `churn_128m/`. The script's historical `profile_2g` filenames also name the 128 MiB diagnostic runs; the actual arena size is recorded in each log.

Reproduce with Docker running for the PostgreSQL comparison:

```sh
CARGO_NET_OFFLINE=true \
AEROSTORE_CRUCIBLE_LOG_DIR=/tmp/transactional-crucible-2g \
bash scripts/check_crucible_2g_120_vs_240.sh

CARGO_NET_OFFLINE=true AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_LOG_DIR=/tmp/transactional-crucible-128m \
bash scripts/check_crucible_2g_120_vs_240.sh
```

## Contention investigation

The first native publication implementation passed correctness and sustained memory gates, but its 256 publication buckets and single-try acquisition caused frequent transaction retries and approximately 1,049 microsecond p99 latency. The [initial 120/240-second trial](investigation_single_try/churn_128m.log) and [exploratory probe summary](investigation_single_try/probe-summary.json) preserve that regression.

The retained implementation uses 4,096 publication buckets per index, bounded spin/yield acquisition for index and row partition locks, and removes sleeps from bounded partition acquisition. The larger fixed table reduces unrelated-key collisions at the cost of more metadata and more work for broad range predicates. Benchmark thresholds and application retry backoff are unchanged. Some exploratory probes used temporary conflict tracing; the final validation uses the retained source without that instrumentation.

## Reproduce the extended gate

Exact recorded commands and exits are in [validation.json](validation.json). Equivalent commands using disposable output paths:

```sh
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine both --mode all --families 32 --cycles 4 --workers 8 --seed 20260922 \
  --output target/extended-both-fixed.json

cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine aerostore --mode all --families 64 --cycles 16 --workers 8 \
  --shm-mib 128 --seed 8675309 --output target/extended-aerostore-fixed.json

cargo test --offline --workspace --release -- --test-threads=1
```

These results establish the implemented synthetic workload and tested storage contracts. They do not establish compatibility with proprietary HyperFeed code, arbitrary process-death recovery, or unbounded runtime stability.
