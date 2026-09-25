# Staggered-fleet retention checks, September 25, 2026

All three 120-second, 100-message/second runs completed the same 12,000-message corpus with valid full-history serial witnesses and passing final invariants. The seeded fleet starts with 12 live families and ends with 13, independently recounted from the archived row snapshots. This improves population and background-work coverage over the [single-flight turnover stress](../stress-retention/README.md). It remains a synthetic, uncalibrated HyperFeed workload.

**Actual message execution did not overlap in these paced runs.** A sweep of the original successful-transaction intervals, message-service intervals and scheduled-arrival-to-receipt intervals finds maximum concurrency 1 and zero overlapping pairs for all three engines. Every message completed before the next arrival, ten milliseconds later. Four configured workers and a seeded active fleet do not make these particular runs contention tests. They establish neither sustainable capacity nor a 10× improvement.

| Engine/transport | Overall p50 | Overall p99 | First 10s p99 | Last 10s p99 | Retries |
| --- | ---: | ---: | ---: | ---: | ---: |
| AeroStore direct mapping | 0.432 ms | 3.285 ms | 3.145 ms | 3.443 ms | 0 |
| AeroStore Unix service | 1.512 ms | 5.295 ms | 5.177 ms | 5.113 ms | 0 |
| Native PostgreSQL Unix socket | 1.166 ms | 2.193 ms | 2.165 ms | 2.380 ms | 0 |

Ten-second p99s range from 2.832–3.443 ms, 4.875–5.616 ms and 1.902–2.480 ms respectively. These are individual, instrumented, single-seed runs. Their finite latency fluctuations are not a long-run stability guarantee or a repeated capacity comparison. Native/direct, native/service and PostgreSQL all execute on one WSL host; no physical MMHF network is involved.

**Known timing limitation:** this preserved build's `completed_messages_per_second_including_drain` omits the interval between stopping the workload timer and starting the post-worker-shutdown drain timer. It is not complete shutdown-inclusive throughput and must not be used as a corrected capacity result. Per-message timestamps, these latency cohorts, histories and state checks are unaffected. Corrected timing requires separate new-build evidence.

## Population and useful work

All three engines have identical initial/final population snapshots:

| Population | Initial | Final |
| --- | ---: | ---: |
| Live families | 12 | 13 |
| Active records | 234 | 251 |
| Flight views | 64 | 71 |
| Retained positions | 27 | 28 |
| Scheduled events | 32 | 33 |

The configuration uses 16 identities and `hot-percent=0`. Snapshot counts are not a continuously measured minimum active population. Each engine creates 5,250 views, updates 19,500 views, emits 21,963 outputs, expires 749 families and expires 34,839 records overall. Outcomes can overlap and must not be added as distinct useful-message counts.

All four background operations now perform real mutations, with exactly matching effects across engines:

| Background operation | Messages | Messages containing writes | Observed effects |
| --- | ---: | ---: | --- |
| Global projection | 750 | 750 | 3,000 claims, 2,463 outputs, 3,000 event reschedules |
| Global cancellation | 750 | 750 | 3,000 event cancellations |
| Global rescheduling | 750 | 750 | 3,000 event reschedules |
| Global housekeeping | 1,500 | 750 | 20,832 expired records |

Of 12,000 messages, 10,499 contain writes and 1,501 are read-only: 750 housekeeping visits and 751 family-expiry visits. Read-only maintenance is legitimate work, but the original `no_op_fraction: 0` is a narrower missing/deferred-outcome metric. It does not mean every message mutated records. The histories contain 122,052 write operations per engine; these are operation counts, not distinct rows or messages.

## Retention observations

| Measurement | Direct AeroStore | Unix service | PostgreSQL |
| --- | ---: | ---: | ---: |
| Initial arena allocation high-water | 2,432,768 B | 2,432,768 B | Not measured |
| After-drain arena allocation high-water | 2,632,944 B | 2,632,808 B | Not measured |
| Fresh row allocations, initial → after drain | 4,096 → 4,965 | 4,096 → 4,964 | Not applicable |
| Row reuse allocations after drain | 121,183 | 121,184 | Not applicable |
| Maximum sampled retired postings, all indexes | 72 | 68 | Not applicable |
| Maximum sampled retired nodes, all indexes | 9 | 9 | Not applicable |
| Retired postings/nodes after drain | 0 / 0 | 0 / 0 | Not applicable |
| Record relation plus indexes, initial → after drain | Not measured | Not measured | 1,089,536 → 3,825,664 B |

Native high-water grows by 200,176 bytes and 200,040 bytes respectively. Growth slows but continues late in the run; **neither a plateau nor bounded memory has been demonstrated**. Both engines recycle 122,052 rows and reclaim 117,618 index postings. Index allocation failures and GC recycling errors are zero at every sample and after drain. These counters measure arena allocation/reuse, not live-memory bytes, physical RSS or whole-process memory. The configured arena capacity is 256 MiB. Instantaneous samples can miss transient activity: every native `active_transactions` sample is zero despite the transactions between samples.

PostgreSQL ends with 2,056,192 table bytes plus 1,769,472 index bytes. Two autovacuums occurred. Estimated dead tuples reached a sampled maximum of 57,616 and were 50,449 after drain; estimates and statistics can lag activity. Relation sizes are storage measurements, not PostgreSQL RSS, and must not be compared numerically with the native arena gauge as equal memory footprints. Cluster/database counters in the raw reports also have broader scope than this fixture's record relation.

Both native WAL files contain 11,625,672 bytes after drain, outside the arena measurement. History files are also excluded. The successful explicit post-work drains took approximately 27.0 ms direct, 44.3 ms service and 11.1 ms PostgreSQL. Business acknowledgements remain asynchronous; the PostgreSQL synchronous fence and native WAL-ring drain are outside per-message latency and do not establish equivalent crash recovery or durable business acknowledgements. PostgreSQL's build, configuration and adapter contract are documented in the [baseline archive](../postgres-and-network/README.md).

## Reproduce the analysis

```sh
python3 docs/bench_data/architecture_2026-09-25/fleet-retention/analyze.py --verify
```

[windows.json](windows.json) contains 36 ten-second arrival cohorts of exactly 1,000 messages each. Cohorts are half-open `[0,10)`, `[10,20)`, etc., relative to the minimum scheduled-arrival timestamp. Delayed completions remain in their original cohort. E2E latency includes arrival queues, Store RPC, retries/backoff and coordinator receipt; service and queue percentiles are separate, non-additive diagnostics. Nearest-rank percentiles use `sorted[ceil(N*p/100)-1]` on individual nanosecond measurements.

The interval sweep counts overlap using half-open intervals; one interval ending exactly when another begins does not overlap. Successful-attempt timestamps omit failed attempts; message-service intervals include retry/backoff time. These runs have zero retries. All clocks are on the same host. Storage samples use their measured midpoint relative to the arrival epoch without interpolation; baseline pre-arrival samples and after-drain state are retained separately. Sample maxima are not continuous maxima.

Compact CSV projections reproduce all latency, overlap, count, write and outcome calculations; initial/final compressed snapshots reproduce population counts. The unmodified reports, trials, serial witnesses and campaign metadata are also retained. A fresh serial-oracle replay requires the full original histories, whose paths, byte counts and SHA-256 digests are recorded in [inputs.json](inputs.json). The projection omits the complete observations needed for that replay; original histories remain under `target/architecture-qualification-2026-09-25/fleet-retention/`.

[build-provenance.json](build-provenance.json) pins binary `f37b288b38be0b86a29f1db32079f5819c4fe28220937219f41260c9c8549033` and source inventory `7b02ad0fd7c2e1a04db8603c4321015c10c4d82ee3bfaa2cfd6adda457558d87`. Every trial's before/after identities match. All 478 files in [fleet-source.tar.gz](../fleet-source.tar.gz) were verified against that inventory. This archive is separate from the older stress workload/build and the [short fleet functional checks](../fleet-functional/README.md).
