# HyperFeed capacity with an explicit memory-backed arena

**Checkpoint: September 29, 2026.** At 6,400 offered messages/s, the first
905-second native trial completed **6,399.96 incoming messages/s** during
admission but **failed the 50 ms foreground p99 requirement at 50.92 ms**.
All three jobs from each maintenance class finished on time with positive work.
The first 4,032/s sustained trial passed operational requirements at 12.95 ms
p99, but lacks a completed full-history guard and repeat. Neither rate is a new
accepted capacity baseline.

The accepted historical comparison remains native **3,072/s** versus PostgreSQL
**640/s**, or **4.8×** under that campaign's stated synthetic contracts. A short
6,400/s screen also passed here, but it ran for only two minutes and exercised
no maintenance. It does not establish the 10× goal.

## Configuration and acceptance

This follows the [arena placement experiment](hyperfeed_commit_phases.md).
Backing is now a recorded `--arena-backing memfd` configuration, with descriptor
observations confirming tmpfs backing. The default remains `file`. The
[backing contract](hyperfeed_qualification.md#arena-backing) records the lifetime
difference: new memfd attachments require the owner's open descriptor, while
existing mappings survive owner exit. Native WAL remains in the case directory;
asynchronous acknowledgment and final drain are unchanged. Equal crash
durability, persistent-arena recovery and physical MMHF performance remain
unestablished.

All six completed cells use the same captured default-feature executable and
fixed local Unix-service workload: 1,024 identities, the seven-fork profile,
16 foreground workers plus two maintenance workers, temporary signature affinity
with a 600 ms TTL, ordered due/expiry indexes, housekeeping-only expiry
eligibility, and prefix-selected maintenance sweeps. Projection commits batches
of four and housekeeping batches of 32, with terminal empty-query transactions.
Only offered rate, duration, the declared short maintenance stress cadence, and
nonbinding message caps differ between the cells below.

The sustained policy requires 905 seconds with 300-second maintenance timers,
foreground p99 at most 50 ms, at least 99% completion during admission and in its
final third, bounded sampled queue growth, and three completed maintenance jobs
per class with at least two doing positive work. Maintenance must finish before
its next deadline. Two distinct seeds and a matching full-history guard are
required before accepting a repeated passing rate. Retry exhaustion and
maintenance starvation are configuration failures; resource, generator and
oracle limits are assessed separately. See the unchanged
[capacity policy](../scripts/assess_hyperfeed_capacity.py).

The enforced budget is 24 logical CPUs, a 2 GiB native arena, a 36 GiB owned-memory
ceiling, and a 4 GiB host available-memory reserve. The 4 GiB swap containment
limit is not an allowance for accepted results: any owned swap invalidates a
measurement. Builds and trials run one at a time. The original 20 GiB development
and screen growth budget remains in force. The sustained trials used a
qualification-only 40 GiB addendum; a later, unused
[64 GiB qualification ceiling](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/qualification-budget64.json)
allows subsequent stages to preserve all earlier evidence. Both count all
growth from the same original baseline, including retained histories, and
require fresh admission checks. No trial used the 64 GiB tier in this campaign.
Both Linux and the Windows volume hosting WSL retain a 30 GiB disk reserve.
These declarations and stage admissions are in the
[plan](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/plan.json) and
[qualification budget](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/qualification-budget.json).

## Completed observations

Throughput counts each logical foreground input once when its completion reaches
the coordinator **while arrivals continue**, divided by the admission duration
measured with `CLOCK_MONOTONIC`. Retries, transactions, fork updates and background jobs cannot inflate
that numerator. P99 includes arrival queueing and retries; completion latency
retains messages that finish in the final drain. All rows use seed 20260929.

| Cell | Admission / timer cadence | Offered/s | Completed/s during admission | Foreground p99 | Status |
| --- | --- | ---: | ---: | ---: | --- |
| [Burst 4,032](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/burst4032-s29/cell.json) | 120 s / 300 s | 4,032 | 4,031.80 | 12.80 ms | Screen passed; no timer jobs |
| [Burst 6,400](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/burst6400-s29/cell.json) | 120 s / 300 s | 6,400 | 6,399.63 | 19.37 ms | Screen passed; no timer jobs |
| [Maintenance stress](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/maintenance4032-s29/cell.json) | 40 s / 5 s | 4,032 | 4,031.33 | 14.28 ms | Seven jobs per class completed on time |
| [First sustained trial](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/long4032-s29/cell.json) | 905 s / 300 s | 4,032 | 4,031.97 | 12.95 ms | Operational pass; guard and repeat pending |
| [Maintenance stress 6,400](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/maintenance6400-s29/cell.json) | 40 s / 5 s | 6,400 | 6,399.00 | 54.28 ms | Failed foreground p99; seven jobs per class completed on time |
| [Sustained 6,400](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/long6400-s29/cell.json) | 905 s / 300 s | 6,400 | 6,399.96 | 50.92 ms | Failed foreground p99; three positive jobs per class completed on time |

The 4,032/s sustained trial received 3,648,934 foreground completions during admission
and drained all 3,648,960 offered messages. Each maintenance class completed all
three jobs on time and with positive work. After the 60-second queue warmup,
the maximum sampled backlog was 87 messages, oldest sampled outstanding age
26.04 ms, and late-minus-early mean backlog growth 0.84 messages. All passed
the declared policy. Sampling is once per second, not a continuous maximum.

Retries remain substantial: that trial recorded 2,372,595 commit-stage
serialization failures, 1,438 candidate-lookup failures, 28 global-expiry-lookup
failures and nine history-lookup failures. Default instrumentation identifies
stages, not whether each rejection was logically necessary. Successful retries
remain included in message latency and excluded from useful throughput.

Native arena allocation high-water rose from 66,007,648 to 72,634,136 bytes;
roughly 100.7 million row reuses were recorded, and retired index postings drained
to zero. These counters describe native arena allocation, not database RSS or
proof of indefinitely flat retention. The whole owned cgroup peaked at **14.61 GiB**,
including workers, coordinator receipts, file cache, setup, teardown and checking.
Metrics mode still retains evidence proportional to the offered corpus. Its
memory growth must not be attributed wholesale to native arena retention, nor
discarded when budgeting the campaign. At 6,400/s, arena high-water reached
74,441,872 bytes while the owned cgroup peaked at 22.88 GiB. Both sustained
resource assessments passed.

The [latency-window analysis](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/latency-window-analysis.json)
explains why the short 6,400/s pass was insufficient. The 905-second run had
40 one-second arrival cohorts with p99 above 50 ms, versus only the startup
second in its 120-second screen and none in the 4,032/s sustained run. The worst
cohort, at 253–254 seconds, reached 266.06 ms; the largest sampled backlog was
1,189 messages during the nearby 251–261-second burst. Bad cohorts numbered
13, 20 and seven across the first 300 seconds, next 300, and final 305 seconds,
respectively. This is intermittent behavior, not a demonstrated monotonic
degradation with runtime.

None of those 40 cohorts overlapped, or lay within two seconds of, a maintenance
job. The strongest observed bursts therefore do not support attributing the
miss primarily to maintenance. Foreground queue p99 was 48.34 ms, compared with
6.09 ms service p99; the corresponding short-screen values were 16.53 and
6.06 ms. These separately calculated quantiles cannot be added or subtracted.
Likewise, window p99 values cannot be combined into global p99, and a cohort
below 50 ms can still contain individual slower inputs. The run-level 1.658-second
maximum includes maintenance and equals a housekeeping job's latency; it is not
a measured foreground maximum.

The analysis preserves [threshold windows](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/latency-over-50ms.csv),
[30-second summaries](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/latency-timeline-30s.csv)
and [maintenance intervals](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/latency-maintenance-jobs.csv).
It reads no raw operation history. Resource receipts show no major faults,
reclaim scans or owned swap in either sustained run. They lack an absolute
monotonic controller anchor, and realtime-minus-relative-monotonic offsets
changed during execution, so this evidence cannot precisely align file writeback
or minor faults with the latency cohorts. A separate
[clock-step analysis](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/resource-clock-step-analysis.json)
finds discrete positive offset changes approximately every 30 seconds, not
smooth drift: 32 changes above 1 ms in the 6,400/s envelope, totaling 6.719 seconds;
the largest adjacent change was 228.267 ms. The 4,032/s envelope had 31 such
changes, totaling 4.605 seconds. These observations neither correct throughput
nor automatically invalidate internally monotonic workload records. Calibration
against independent physical elapsed time remains a WSL limitation. Future
profiling should record bracketed monotonic, raw-monotonic and realtime readings
with admission anchors before attributing stalls to clock adjustments.

## Evidence boundary and next decision

The [capture](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/candidate-02/capture/capture.json)
binds source `a4e898c32e0b0722…` and executable `62a98297d18e68f5…`.
The [historical source comparison](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/source-comparison.json)
finds unchanged hashes for production engine code, PostgreSQL/native/service
adapters, workload/model, calibrated scheduling, workers/accounting, Cargo inputs
and verification sources. Changes are in arena fixture/configuration, metadata,
harness and tests. The executable and harness differ from the historical
PostgreSQL measurement, so a ratio to its 640/s result is a historical reference,
pending a fresh matched PostgreSQL comparison.

[Focused Rust checks](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/focused-checks/tests/receipt.json.gz)
passed 66 test executions. The
[final checks](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/final-checks-02.json)
passed 198 Python tests and the existing default-source verification guard,
covering attachment lifetime, metadata rejection, accounting and configuration
matching. The earlier final-check invocation's import error remains preserved.
The source guard checks the reviewed source relationship; this work adds no
whole-engine proof. Metrics runs passed structural, ordering and useful-work
checks, but their unrecorded transaction histories are not exhaustively verified.

The [4,032/s full-history checker](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/full4032/guard.json)
was deliberately interrupted to prioritize investigating 6,400/s; its incomplete
evidence is preserved and supplies no correctness pass. Both 6,400/s policy
failures remain evidence. All campaign jobs are stopped. A roughly five-minute
diagnostic profiling the whole owned process group is planned, but its launcher
has not been implemented or run. It should target the demonstrated queue bursts
before another implementation change. Any accepted point still needs
bound source, configuration, a completed guard, repeated sustained measurements
and resource receipts. A fresh PostgreSQL comparison remains outstanding.
Population turnover, production message distributions, larger worker counts and
unavailable HyperFeed code remain workload limitations.

The [final evidence audit](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/final-audit.json) checked **1,687 referenced files** and confirmed all ten owned resource envelopes stopped. The [compact archive](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/artifact-manifest.json) preserves 1.57 MiB of receipts, helpers, summaries and tables; full histories, captured sources and executables remain at their original local paths. No new conditional capacity point was accepted.

Campaign growth was 23.58 GiB against the 40 GiB ceiling. Closeout left approximately 478.9 GiB free inside Linux, 214.2 GiB on Windows C:, and 45.4 GiB available RAM. Builds reused the normal development cache; no isolated compiler trees were created or removed. No evidence was deleted, and zero Linux or Windows space reclaimed is claimed.

The [saved checkpoint](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/checkpoint.json) records evidence paths, resource checks and capture-validation commands. The [next diagnostic specification](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_arena_capacity_2026-09-29/evidence/next-diagnostic-plan.json) calls for 300 seconds at 6,400/s with a 1M per-worker message cap and profiling from seconds 5–285. Its new launcher and analyzer still need implementation and validation; the saved plan is not an executable profiling result.
