# Sustained message capacity

This campaign makes completed incoming messages per wall-clock second the
primary measurement. It preserves the
[population experiments](hyperfeed_population_scaling.md) and postpones selective
family indexes and RPC batching until a capacity comparison identifies the next
bottleneck. No engine optimization is part of the measurement change.

## Declared experiment

The initial local comparison fixes 1,024 configured identities, seven forks per
identity, 16 foreground processes plus two maintenance processes, the existing
fixed-population calibrated message mix, and temporary signature affinity with a
600 ms TTL. One quarter of identities are quiet. Projection and housekeeping each
start every 300 wall-clock seconds and commit batches of four and 32 rows,
respectively, ending each sweep with a committed empty query. Each sustained cell
admits inputs for 905 seconds, exposing three occurrences of each timer. Arrivals
continue independently of worker progress; every input is counted once.

PostgreSQL uses the existing native installation, a local Unix socket,
`SERIALIZABLE`, prepared statements, buffered writes, the split candidate query,
prefix maintenance queries, initial `ANALYZE`, and one additional `ANALYZE` at
five seconds. AeroStore uses the existing local database-owned Unix-socket
service, prefix maintenance queries, ordered due and expiry publication, and
housekeeping-only expiry eligibility. These are existing configurations. The
engines execute the same application handlers and discover writes through reads
and queries. Every foreground message is one transaction; a complete maintenance
sweep spans several independent transactions.

Each cell has the same 24-logical-CPU affinity and 36 GiB total cgroup memory
ceiling, including clients and measurement overhead. Trials run sequentially.
The envelope permits at most 4 GiB swap for containment, but any observed swap or
memory-limit event excludes a clean performance result. At least 4 GiB host
available memory and 30 GiB free on both the Linux filesystem and Windows VHDX
host volume must remain. The native arena reserves 2 GiB; PostgreSQL retains its
existing 128 MiB shared-buffer setting. These are different internal allocations
under a common total envelope, not an assertion of equal resident memory or
optimal tuning. Windows and other host activity are not isolated.
PostgreSQL retains serialization-error logging, including failing SQL statements,
and nine indexes on its records table (including the primary key). The native
fixture has five secondary indexes and separate direct row-ID access. Logging
and index maintenance remain configuration differences, and the comparison does
not establish an optimally tuned PostgreSQL limit. For example, the first
PostgreSQL 640/s trial retained a 2.77 GB server log.

Both engines acknowledge asynchronous WAL. PostgreSQL has `fsync` and
`full_page_writes` on, `synchronous_commit` off, and a 10-second WAL-writer delay.
AeroStore's writer appends complete frames in 1 MiB batches and requests
`fdatasync` every ten seconds and on final drain. Successful trials include a
final drain. These intervals do not equate the engines' crash-loss windows.
Equal acknowledged crash durability,
dependent-transaction recovery, and durable exactly-once delivery are not
established. This comparison therefore applies to the stated asynchronous
contracts, not to a synchronous durable PostgreSQL replacement claim.

## Acceptance and search

The predeclared foreground budget is 50 ms end-to-end p99, from scheduled arrival
through coordinator receipt, including queuing, retries and result delivery.
Completions received strictly before admission ends must reach at least 99% of
offered inputs, both over the entire admission interval and its final third.
Drained throughput is retained as a secondary metric and cannot substitute for
completions during arrivals.

After a 60-second warmup, sampled aggregate foreground backlog must not exceed
one second of offered traffic. The final-third mean queue may exceed the
early-third mean by at most 50 ms of offered traffic. One-second queue samples
and their slope are reported as samples, not an exact continuous queue maximum.
The sampled oldest outstanding foreground message must be at most one second
old after warmup.
Every maintenance job must finish before the following timer deadline; at least
three jobs and two jobs with positive effects must complete for each class.
Retry exhaustion, excessive backlog, and maintenance starvation fail the tested
configuration. Failed or unfinished inputs never count as completed throughput.

Memory acceptance requires the common envelope, no allocation/reclamation
errors, and successful final audits. Cgroup anonymous/file/shared-memory trends,
native arena and retirement counters, and PostgreSQL relation/dead-tuple trends
are reported separately. Growing retained measurement receipts make a flat
whole-cgroup memory requirement inappropriate; storage bytes are not RSS.

Increase input rate, retain failed attempts, refine the transition, and repeat
the highest passing and nearby failing rates. Two distinct seeds must pass a
905-second endpoint to call it repeatably passing. Seeds vary message data,
not the deterministic arrival pattern; this is limited repetition. Short probes
guide the search but cannot establish a sustained passing rate. Failures need
not be monotonic, and an observed failure is not a universal upper bound for an
engine or for other worker counts.

Full-history tests remain correctness guardrails. Bounded full-history companion
runs check the measured build and endpoint handlers, including accelerated
maintenance. The initial coverage amendment sized them to
`max(40, min(90, floor(100000 / rate)))` seconds with five-second timers.
The first PostgreSQL 640/s companion completed admission and drain, but its
history checker hit the benchmark's internal `seconds + 300` watchdog. The
larger outer timeout did not extend that deadline; the timeout evidence remains
preserved and does not count as a correctness pass or database saturation.

The resumed plan uses 40-second companions for both engines. A separate bounded
supervisor invokes the exact measured executable's existing internal coordinator
and permits 1,800 seconds for execution and checking. The reference oracle's
2,000,000-candidate search budget is unchanged. The 40-second duration includes
the quiet population's second positive projection at 35 seconds; each companion
must complete at least three sweeps and two positive jobs of each class.
The supervisor records the original coordinator result separately from its
assembled outer report, and checks resource, configuration, and cleanup bindings.
Long metrics runs retain structural audits, ordering, useful-work
and accounting checks but do not record complete operation histories. A short
serial witness does not prove a different long execution. The existing formal
verification and qualification gates remain intact; this campaign adds a
separate conditional synthetic capacity assessment.

Generator caps, resource interruptions, and an exhausted correctness oracle are
reported separately from database operational failures. The first build allowed
at most 100,000 messages per worker. After a 3,072/s short probe outgrew that
limit for a 905-second run, a separately preserved build added an explicit
calibrated-metrics allowance of one million messages per worker and eight
million inputs overall. Defaults and the 100,000-message full-history limit
remain unchanged. Exact routing counts must fit each worker's allowance before
execution; the harness never truncates the corpus. Short full-history guardrails
may use a smaller allowance only when their exact counts independently fit it.
This changes evidence-storage admission, not database configuration.

## Scope

The real HyperFeed source and simultaneous flight population are unavailable.
This fixture has a bounded working set, no population turnover, a fixed quiet
quarter and source mix, and synthetic identifier overlap. At this population,
foreground circular slots alias; a 905-second run does not cover the one-hour
foreground retention horizon. The initial old-position cohorts provide finite
housekeeping work. Sixteen foreground workers do not reproduce the historical
100–300 worker deployment. Local sockets do not measure physical MMHF network
cost. These limitations stay attached to every capacity number and ratio.

## Accepted synthetic capacity baseline

The 905-second measurements and all four full-history companion guards have
finished. The capacity assessor accepts the repeated endpoints below under the
declared conditional correctness coverage.
Each row contains two seeds, with the same workload and configuration across
both databases except for the stated adapter and durability differences.
Throughput excludes completions received after admission ends; p99 covers all
offered foreground inputs through final drain, so late completions remain in
the latency distribution.

| Database | Offered messages/s | Completed messages/s during arrivals, seeds 29 / 30 | End-to-end p99, seeds 29 / 30 | Operational outcome |
| --- | ---: | ---: | ---: | --- |
| AeroStore Unix service | 3,072 | 3,071.983 / 3,071.981 | 26.194 / 23.824 ms | Both pass |
| AeroStore Unix service | 3,584 | 3,583.975 / 3,583.973 | 30.313 / 53.157 ms | Mixed |
| AeroStore Unix service | 4,032 | 4,031.970 / 4,031.969 | 131.708 / 177.693 ms | Both fail p99 |
| PostgreSQL | 640 | 639.991 / 639.993 | 42.467 / 49.551 ms | Both pass |
| PostgreSQL | 704 | 703.985 / 703.989 | 54.304 / 52.784 ms | Both fail p99 |

The highest tested repeated passes are 3,072/s and 640/s, a **4.8×**
ratio on this synthetic fixture. This is neither an exact maximum-capacity ratio
nor a demonstrated 10× production HyperFeed result. A higher input rate can
still complete almost all messages yet fail the required latency budget.
The final assessment includes both seeds at 640, 704, 3,072 and 3,584/s, each
paired with its same-rate full-history guard. The 4,032/s failures are additional
operational evidence; that rate has no same-rate full-history companion and is
not included in the accepted companion-bound assessment.

Both PostgreSQL companions and both AeroStore companions found valid serial
histories. Their reference checks took about 186, 208, 905 and 999 seconds,
respectively, after 40-second workload admissions. The 3,072/s outer launcher
completion note was lost during a tool-session interruption; the original
coordinator, controller and resource-envelope results completed successfully.
A separately recorded host audit confirmed all owned processes and cgroups had
exited. The missing launcher note remains absent and its exit status unknown;
the accepted inner execution is bound separately rather than inventing a note.

The saved 90-second PostgreSQL 640/s history was also replayed independently
using the measured source's unchanged reference modules and oracle budget.
It found a valid serial history in 323.8 seconds of oracle time (344.1 seconds
including the replay wrapper), within a 12 GiB envelope and 600-second limit.
The original watchdog-timeout report remains unchanged. This supplemental
witness does not reconstruct any missing shutdown or storage audits.

All of these completed long trials finished three useful projection and three
useful housekeeping jobs before their next deadlines. Endpoint queue trends
were bounded. The earlier PostgreSQL 1,024/s attempt exhausted its 128-retry
limit after about 72.7 seconds; that remains a failed configuration. Short
screens, checker limits and resource interruptions stay separate from sustained
operational failures.

At the passing endpoints, the maximum sampled outstanding queue was 181 / 132
messages for AeroStore and 79 / 85 for PostgreSQL, in seed order. The largest
sampled ages of the oldest outstanding message were 90 / 72 ms and 344 / 522 ms,
respectively. Late-third minus early-third mean queue growth was 1.65 / 0.29
messages for AeroStore and −0.55 / −0.35 for PostgreSQL. All passed the declared
queue requirements; these are one-second observations, not continuous maxima.

At the passing endpoints, all-transaction retries per incoming message were
approximately 0.88–0.89 for AeroStore and 3.78–4.20 for PostgreSQL. These ratios
include maintenance retries; retries never count as additional messages.
Recorded native failures were predominantly commit serialization conflicts;
PostgreSQL failures were predominantly SQLSTATE `40001` during buffered writes.

Native arena usage grew from about 66 MB to 77–78 MB and drained retired index
postings to zero. PostgreSQL relation storage grew from about 61 MB to 130 MB.
These are storage/allocator measurements, not equivalent RSS figures. The whole
envelope, which also includes clients, retained receipts and file cache, peaked
around 11.2–11.4 GB for AeroStore and 5.7–6.1 GB for PostgreSQL. Neither the
duration nor this fixture establishes an indefinite memory plateau.

## Evidence guiding the next experiment

Separate diagnostic profiles used the unchanged default executable. AeroStore's
4,032/s capture averaged 13.20 CPU cores; worker and service-session CPU was
about 70.85% system time, with approximately 86.9 million voluntary context
switches over the 120-second observation window. Its latency spikes occurred
without a maintenance tick. PostgreSQL's 704/s profile averaged 6.05 cores;
the foreground cohort around the 300-second maintenance tick reached about
421 ms p99. These are diagnostic observations, not additional capacity passes.

The next candidate is reducing per-message communication overhead in the native
worker/service path. First distinguish baseline request costs from traffic
amplified by retries, then test a small change such as writing a frame header
and payload together while preserving deadlines and partial-I/O behavior.
The profile does not yet prove that broader RPC batching or a new index design
will increase capacity. Judge any candidate by repeated sustained completed
message throughput under the same correctness, latency, queue and maintenance
requirements.

The subsequent [frame-writing experiment](hyperfeed_frame_write_experiment.md)
tested two small implementations with short paired screens. Neither showed a
consistent gain; both remain preserved as experiments and the working baseline
is unchanged. Review of the existing time-window profiles makes service-session
CPU bursts the next attribution target.

The subsequent [stack profile](hyperfeed_burst_profile.md) reproduced those
bursts and narrowed elevated commit samples to index predicate-lock acquisition.
The working baseline and accepted capacity endpoints remain unchanged. Identify
the contended buckets and owner hold times before choosing a lock or index change.

Raw results, original timeout evidence, source and executable receipts, profile
analyses, and resumed checks remain under `target/sustained-capacity-20260928`.
The [archive manifest](bench_data/sustained_capacity_2026-09-28-retry2/artifact-manifest.json)
and [validation receipt](bench_data/sustained_capacity_2026-09-28-retry2/archive-validation.json)
cover 2,236 logical files: 87.2 GB of original evidence stored in 5.1 GB of
bounded compressed chunks. Executables and runtime dependencies remain at their
original local paths; the archive records their hashes rather than embedding
their bytes. The final input manifest and assessment are at
`resume-20260929/assessment/final-capacity-{input,assessment}.json` inside the
archive and original evidence directory.

The first archive stopped at its archive/Git allowance. Its partial bytes remain
locally at `docs/bench_data/sustained_capacity_2026-09-28/`, excluded from Git;
its failure record and retained-file inventory are included in the complete
retry archive. The retry increased the archive/Git allowance within the unchanged
120 GiB campaign ceiling and 30 GiB free-space reserves. This storage-limit stop
does not change any database verdict.

Audited cleanup reclaimed 0.79 GiB of compiler intermediates without newly
missing or changed retained artifacts. This is Linux filesystem space, not a
claim of space returned to Windows. The pre-measurement test receipt records
514 successful executions across 19 suites. After archival, a small assessor
fix added report-level error classification: 34 focused policy tests passed,
and reassessing the eight saved trials left their metrics and verdicts unchanged.
No engine optimization or major formal-proof expansion was required.

The stored archive can be checked without the original `target/` tree:

```sh
python3 docs/bench_data/sustained_capacity_2026-09-28-retry2/resume-20260929/resources/verify_stored_archive_v6.py \
  --archive docs/bench_data/sustained_capacity_2026-09-28-retry2
```
