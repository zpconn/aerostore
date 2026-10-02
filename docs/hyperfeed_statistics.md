# PostgreSQL statistics and the next load step

The PostgreSQL statistics treatment is now a reproducible benchmark option, and
both engines complete a fresh 256-input/second rolling workload. AeroStore's
central service also completes the 512-input/second workload with a verified
full-history run and a matching measurement run. PostgreSQL exhausts its retry
budget at that higher load despite the statistics update succeeding. These
results identify the next investigation; they do not establish a capacity ratio
or the project's 10× target.

This follows the [external statistics control](hyperfeed_expiry_resume.md).
The new [evidence archive](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/pg_statistics_2026-09-28/artifact-manifest.json)
retains successful and failed attempts, source snapshots, logs, histories,
resource accounting, and independent checks. No production engine algorithm,
default index policy, or formal proof changed in this checkpoint.

## Reproducible treatment

The contention benchmark and qualification runner accept
`--pg-analyze-after-seconds N`. Zero preserves initial-only explicit analysis;
PostgreSQL's automatic maintenance remains enabled. A positive delay schedules
one additional `ANALYZE` during fixed-arrival admission. A preconnected observer
runs independently of the coordinator's receipt collection. It verifies the
scratch schema's ownership marker and typed schema/table/backend identities,
records monotonic command times and statistics observations, and joins before
schema cleanup. A failed, missing, excessively late, or incomplete action cannot
qualify a successful run. Cancellation and statement timeouts bound cleanup.

The requested treatment participates in exact correctness-companion matching.
Native and service runs record the same option as inapplicable. Statistics
counters are observations that can lag; they are not substitutes for successful
command execution or evidence of which query plan ran. See the
[qualification runbook](hyperfeed_qualification.md) for configuration details.

The new source snapshot is
`455e2bac53283806cb54b9cb7ddb17f51c823043b6f90b9338acfdf75f6f96f4`;
the measured executable is
`c8b96bccd3ca553f83208531f999b99db511abcb939afd1e970456495673f2d1`.
The 199 focused Rust/Python checks passed, including live PostgreSQL failure
tests for relation replacement, missing ownership, terminated backends,
cancellation, invalid schedules, and exact companion matching. The build uses
the ordinary Cargo target with an unused campaign-specific cfg to avoid
overwriting historical executable paths. This is an exploratory benchmark build,
not a newly accepted formal-verification build.

## Matched 256-input/second point

All four 185-second runs use seed 20260929, 32 configured identities, 16 foreground
workers plus two background workers, temporary signature affinity, 40-second
retention, and a 640-input rolling cycle. Projection and housekeeping run every
five seconds with batch sizes four and 32. PostgreSQL uses prepared statements,
buffered writes, SERIALIZABLE transactions, and an explicit statistics refresh
five seconds after admission. The service uses optional ordered expiry
publication, hashed due publication, and all-active expiry eligibility.

Each run completes 47,288 useful business messages, 72 retirement controls, and
36 jobs of each background class. The two full-history runs have valid serial
witnesses. Each lighter measurement run has an exact full-history companion;
its own history is not independently verified.

| Engine | Evidence | Foreground p99 | Whole housekeeping job p99 | Total retries |
| --- | --- | ---: | ---: | ---: |
| PostgreSQL | Full | 3.344 ms | 437.045 ms | 499 |
| AeroStore service | Full | 2.903 ms | 371.558 ms | 65 |
| AeroStore service | Metrics | 2.885 ms | 343.024 ms | 72 |
| PostgreSQL | Metrics | 3.256 ms | 352.190 ms | 655 |

Foreground p99 includes queueing, all retries, and retirement controls. With only
36 maintenance jobs, nearest-rank maintenance p99 is the observed maximum. These
are one-seed operating-point observations, not a stable speed ratio. Both
PostgreSQL refreshes dispatch at approximately five seconds and finish in about
11.5 ms; the recorded explicit analyze count changes from one to two.

## The 512-input/second step

The rolling cycle increases to 1,280 inputs to preserve approximately sixty-second
generation turnover. This adds position-update density; it does not preserve a
constant message mix across the two rates.

PostgreSQL fails around 39 seconds when a foreground message exhausts the budget
of **128 retries, permitting 129 attempts**. Its five-second statistics refresh
had succeeded. The received successful transactions already report 52,445
retries. Those partial counts exclude work not received by the coordinator and
cannot provide a completed-run throughput denominator. Both background workers
had completed their seven jobs through 35 seconds.

The separate service campaign completes both 185-second runs:

| Evidence | Useful business messages | Foreground p99 | Whole housekeeping job p99 | Total retries |
| --- | ---: | ---: | ---: | ---: |
| Full | 94,648 | 3.984 ms | 371.831 ms | 2,453 |
| Metrics | 94,648 | 2.688 ms | 261.175 ms | 1,289 |

Each service run completes 36 projection jobs and 36 housekeeping jobs, with
repeated useful expiration and generation reuse. The full run passes complete
history checking; the metrics run uses it as an exact companion. No PostgreSQL
metrics companion was run after its full run failed.

## Executed plans and serialization dependencies

A separate instrumented PostgreSQL run captures actual executed plans with
`auto_explain` and samples serialization read locks once per second. Its first
observer attempt stops safely because it also sees an older owned scratch
schema. The corrected observer binds the exact fresh coordinator schema and
records the excluded historical identities. Both attempts remain in the archive;
historical schemas and the native PostgreSQL installation remain intact. The
diagnostic module is compiled privately against the retained PostgreSQL 16.13
headers and loaded only through the diagnostic connection options.

The corrected diagnostic captures a candidate-search plan using `callsign_idx`
with this index condition:

```text
(scheduled >= $3) AND (scheduled <= $4)
```

The callsign/registration condition is applied afterward as a filter:

```text
(callsign = $1) OR (($2 <> 0) AND (tail = $2))
```

This is an executed prepared plan, not the initialization-time literal
`EXPLAIN`. It can inspect flight rows outside the matching alias group because
the workload's flights share a schedule window. The lock samples also contain
table-wide `SIReadLock` entries on `records`. These observations support an
avoidable access-path contribution to broad conflicts, but do not prove which
statement acquired each lock or explain every serialization rejection. The
workload also deliberately shares callsigns across four identities, so some
cross-flight read/write dependencies are part of its semantics.

The instrumented workload fails on a housekeeping job around 101 seconds,
whereas the original uninstrumented attempt fails on a foreground position
message around 39 seconds. Sampling and logging perturb timing. The diagnostic
capture passes its own provenance, resource, and cleanup checks; **the workload
still fails**, and its timings are excluded from performance comparison.
Sampled completed plans taking at least one millisecond omit fast and failing
executions. One-second lock snapshots also miss short-lived locks.
Only ten completed plans meet the sampling criteria: one candidate search,
eight expiration searches using `event_time_idx`, and one ordered row-locking
query using `records_pkey`. No batch-update or family-query plan is captured;
their access paths remain unestablished by this diagnostic. Server logs record
72,816 read/write-dependency serialization errors and five concurrent-update
serialization errors. Those are server error counts, not a complete message
history or proof that all dependency conflicts are avoidable.

The next controlled experiment should preserve candidate-query results while
making the callsign and registration access paths separately selective. Test
equivalent SQL forms against the partial indexes, capture the actual prepared
plans, and require complete-history checks before treating this as a baseline
repair. Keep genuine overlapping-candidate dependencies and SERIALIZABLE
semantics. A statistics refresh alone is insufficient at the higher load.

## Resources and evidence retention

Builds, tests, workers, and owned PostgreSQL processes run under the retained
36 GiB memory limit with a 4 GiB swap allowance, no CPU quota, and no
`memory.high` throttling. The matched 256-input campaign peaks at 4.302 GiB;
the service 512-input pair peaks at 4.872 GiB. Final accounting records zero
swap use and zero limit/OOM events. These figures include each campaign's
processes and charged file cache; they are not per-engine memory comparisons.

The campaign keeps its original 20 GiB additional-storage budget and reserves
30 GiB on both Linux and the Windows C: volume hosting the VHDX. After all jobs
exit, audited cleanup removes 1,391 newly generated compiler-cache files and
one private module object, reclaiming 391.828 MiB of allocated intermediates.
All 2,136 retained executable, source, tool, and runtime references rehash
successfully, with no newly missing or changed artifacts. The recorded module
and benchmark/test executables remain available at their original paths.

Linux free space rises by approximately 389.9 MiB over the cleanup interval;
Windows free space falls by about 1.45 MiB. No Windows VHDX reclamation is
claimed. Before archival, headroom is approximately 649.02 GiB in Linux and
407.90 GiB on Windows. Archival and Git storage have a separate 4 GiB allowance
within the original budget. The archive's standalone validator checks stored
and reconstructed bytes, source bindings, and retained-artifact references;
it does not rerun database histories or require their original `target/` paths.

## Scope and next decisions

All measurements are on one WSL2 host and one boot, using PostgreSQL 16.13 and
asynchronous acknowledgement followed by a final WAL drain. That does not equate
the engines' crash durability. The five-second maintenance cadence deliberately
stresses overlap; real HyperFeed sweeps were described as roughly five to ten
minutes apart. The small synthetic population, rate-dependent lifecycle, and
16 foreground workers do not represent the historical 100–300-worker deployments.
Calibrated capacity qualification remains disabled.

The service's family queries examine roughly 16.7 candidates per returned row at
256 inputs/second. That is a measurable selectivity cost, but its recorded query
time alone does not establish the dominant end-to-end bottleneck. Native
`StoreMetrics.reads` includes internal index materialization and must not be
reported as RPC counts. Focused request/byte counts and time by operation would
make a later service optimization more informative.

The ordered expiry policy still has a fixed approximately 68-minute precision
window without automatic rotation. A long-lived window strategy, realistic
population/cadence, surviving-worker availability, and physical two-host
measurements remain required. Preserve the current correctness checks while
using measured conflict and service costs to choose the next changes.
