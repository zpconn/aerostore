# Single-flight turnover retention stress, September 25, 2026

All three runs completed their fixed 12,000-message corpus at 100 offered messages/second for 120 seconds. Every original full-history check found a valid serial witness; all final invariants passed. Recomputing counts, outcomes, retries and latency percentiles from the streamed histories reproduces each report exactly.

These are **single-flight turnover stress runs, not an active-fleet or capacity qualification**. The historical workload name is `lifecycle`. It supplies each flight generation's sixteen messages consecutively: creation, source growth, positions, projection/rescheduling, arrivals and expiry. Its 16 configured identities do not mean 16 simultaneously active flights. A separate fleet workload is required to measure that behavior.

All messages completed before the next scheduled arrival, ten milliseconds later. Consequently, the four configured workers did not produce overlapping message execution in these runs. Zero retries here demonstrates neither contention tolerance nor a capacity limit. No physical multi-machine workers were measured.

| Engine/transport | Overall p50 | Overall p99 | First 10s p99 | Last 10s p99 | Retries |
| --- | ---: | ---: | ---: | ---: | ---: |
| AeroStore direct mapping | 0.449 ms | 3.160 ms | 3.357 ms | 2.828 ms | 0 |
| AeroStore Unix service | 1.492 ms | 4.279 ms | 4.418 ms | 3.956 ms | 0 |
| PostgreSQL native Unix socket | 1.102 ms | 2.119 ms | 2.391 ms | 1.952 ms | 0 |

Ten-second p99s fluctuate within 2.800–3.379 ms, 3.956–4.611 ms and 1.835–2.391 ms respectively. This finite, lightly loaded test shows no persistent worsening of p99 over its two minutes. It does not establish long-run stability, a 10× advantage, or replacement readiness. These are three individual runs with full-history instrumentation, not repeated capacity estimates.

**Known timing limitation:** the raw `completed_messages_per_second_including_drain` field from this preserved build omits the interval between stopping the workload timer and starting the post-worker-shutdown drain timer. It is not complete shutdown-inclusive throughput. The per-message latency timestamps, ten-second cohorts and correctness observations remain valid; corrected throughput requires separate new-build evidence.

Each engine produced the same aggregate outcomes: 5,250 created views, 19,500 updated views, 22,500 outputs, 3,000 claimed events, 5,250 rescheduled events, 750 expired families and 33,750 expired records. Counts can overlap: they are separate measures, not additive counts of distinct useful messages. Exactly 9,000 transcripts contain writes and 3,000 contain none. The read-only visits are 750 empty cancellations, 1,500 empty housekeeping searches and 750 expiry searches that expired no family. They are legitimate maintenance work; the historical report's `no_op_fraction: 0` only counts selected no-op outcomes and must not be read as “every message changed data.”

## Allocation and retention observations

| Measurement | Direct AeroStore | Unix service | PostgreSQL |
| --- | ---: | ---: | ---: |
| Initial arena allocation high-water | 2,381,440 B | 2,381,440 B | Not measured |
| After-drain arena allocation high-water | 2,578,680 B | 2,576,792 B | Not measured |
| Fresh row allocations, initial → after drain | 4,096 → 4,977 | 4,096 → 4,968 | Not applicable |
| Row reuse allocations after drain | 116,869 | 116,878 | Not applicable |
| Maximum sampled retired postings, summed across indexes | 110 | 108 | Not applicable |
| Maximum sampled retired nodes, summed across indexes | 15 | 14 | Not applicable |
| Retired postings/nodes after drain | 0 / 0 | 0 / 0 | Not applicable |
| Record relation plus indexes, initial → after drain | Not measured | Not measured | 1,024,000 → 3,817,472 B |

Native allocation high-water grows by 197,240 bytes and 195,352 bytes respectively. Growth slows but continues during the last window; **a plateau or a bound has not been demonstrated**. Sampled index allocation failures and GC recycling errors are zero. Both after-drain snapshots show 117,750 row recycling operations and 114,000 reclaimed index postings. These counters describe allocation and reclamation within the fixture; they are neither live-memory byte counts nor physical RSS. The arena capacity was 256 MiB, substantially greater than its allocated high-water.

PostgreSQL's after-drain relation size comprises 2,138,112 table bytes and 1,679,360 index bytes. Two autovacuums occurred; estimated dead tuples reached a sampled maximum of 56,131 and were 9,833 after drain. Tuple estimates and cumulative statistics can lag activity. Relation file sizes measure storage, **not PostgreSQL process memory**, and are not comparable to AeroStore's arena high-water. No process-tree RSS or equal total-memory-budget claim follows from this table.

The sampled gauges are instantaneous observations, not maxima over every operation. For example, `active_transactions` is zero at every native sample although transactions ran between samples. The native WAL file was 10,267,592 bytes after drain in both runs; arena counters exclude that file and the benchmark's history files. PostgreSQL's database/cluster counters in the raw reports have broader scope than the individual record relation.

## Contract and method

The fixture uses one native host: direct shared mapping, a Unix-domain service, or PostgreSQL 16.13 over its native Unix socket. PostgreSQL uses `SERIALIZABLE`, buffered read-your-writes updates, discovered write locks in ascending row-ID order at commit, composite indexes, `fsync=on`, `full_page_writes=on`, `synchronous_commit=off`, `wal_writer_delay=10s`, `deadlock_timeout=10ms` and autovacuum enabled. See the [PostgreSQL build/settings evidence](../postgres-and-network/README.md).

Business commits acknowledge asynchronously for both engines. Explicit post-work drains are outside message latency and are reported separately: approximately 30.1 ms direct, 40.2 ms service and 4.6 ms PostgreSQL. PostgreSQL uses a synchronous WAL flush fence on the owned fixture; native closes/drains its WAL ring. These operations do not establish equivalent crash-recovery contracts or durable acknowledgement for preceding business messages.

[windows.json](windows.json) contains all 36 ten-second cohorts, each with exactly 1,000 messages. Cohorts use scheduled arrival time relative to the first arrival, with half-open boundaries `[0,10)`, `[10,20)`, and so on. A delayed completion stays in its arrival cohort, preserving queueing costs. E2E latency runs from scheduled arrival through coordinator receipt, including queues, Store RPC, retries/backoff and result IPC. Service latency and arrival queue delay are separate diagnostics; their percentiles are not additive.

Percentiles use the runner's nearest-rank definition, `sorted[ceil(N*p/100)-1]`, computed from individual messages. Storage observations are assigned by the midpoint of each sample's monotonic start/end timestamps relative to the same admission epoch. The initial pre-admission sample is retained separately. There is no interpolation. Raw reports retain the full sample series and index-by-index counters; window summaries preserve first, last, minimum and maximum observed values. Timing uses clocks on this single host only.

## Reproduce and inspect

Run from anywhere with Python 3, without a database or external dependencies:

```sh
python3 docs/bench_data/architecture_2026-09-25/stress-retention/analyze.py --verify
```

The script validates archive hashes, then reproduces every window and reconciles all 36,000 messages with the original overall/per-kind latency, count, retry and outcome reports. Each compressed CSV retains the message ID, worker, kind, original nanosecond timing, retries, write-operation count and outcome counters. It omits complete query/read/write payloads: **it supports aggregate reproduction, not a fresh serial-oracle replay**. Original full histories remain under `target/architecture-qualification-2026-09-25/retention-final/`; [inputs.json](inputs.json) records their byte counts and SHA-256 digests, plus original initial/final state and witness digests. The original unmodified reports, trials, witnesses and campaign metadata are archived here.

[build-provenance.json](build-provenance.json) pins binary `af39521c16db6244b7a8772a314d8df9a8554128b33a555f336b45f11b784e1f` and the exact pre-fleet source inventory. All 478 files in [stress-source.tar.gz](../stress-source.tar.gz) were checked against that inventory while creating this archive. Every trial's before/after binary and source hashes match it. The current working-tree model may differ; it is not the source of these measurements.
