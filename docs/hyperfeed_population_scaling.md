# HyperFeed population and rate scaling

This checkpoint measures how the existing engines behave with larger synthetic
flight populations, before changing AeroStore's native index implementation.
It follows the [ordered maintenance experiment](hyperfeed_maintenance_prefix.md),
which established a completed PostgreSQL comparison under an explicit prefix
query contract. The new campaign reuses that exact source and executable.

Both engines complete the first 256-identity, 512-input/second comparison and
pass the declared foreground latency policy in both evidence modes. AeroStore
has lower foreground p99 at this operating point; PostgreSQL finishes the
background jobs faster. At 1,024 inputs/second the service completes both runs,
while PostgreSQL's first full-history run exhausts its foreground retry budget.
PostgreSQL also completes both 1,024-identity runs at 512 inputs/second. The
corresponding service configuration exhausts projection retries twice, but the
existing optional ordered-due policy completes both full-history and metrics
runs. That isolates a useful configuration choice without replacing the native
query algorithm. A subsequent housekeeping-eligibility treatment also completes,
but does not close the housekeeping latency gap. The next proposed experiment
targets excess family-query materialization using an additional selective index.
These results do not establish a capacity ratio or a 10× HyperFeed replacement
claim. No production engine code or default policy changes in this checkpoint.

## Workload and controls

The workload uses the existing calibrated **fixed-population** profile, with
rolling generation reuse disabled. Both engines start with the same seeded
population and offered message corpus. Each population has a synthetic quiet
quarter; incoming messages cycle through the remaining identities and three
source bits. Seven synthetic forks exist for each identity.

| Configured identities | Active identity lanes | Quiet identities | Flight forks | Reserved physical record slots |
| ---: | ---: | ---: | ---: | ---: |
| 256 | 192 | 64 | 1,792 | 65,536 |
| 1,024 | 768 | 256 | 7,168 | 262,144 |

The reserved slots include two 128-record physical family pools per configured
identity. Fixed-population runs use one pool; they do not exercise alternating
generation reuse. The two populations are separate experimental configurations,
not interchangeable correctness companions.

The initial probes use 90 seconds of open-loop admission, 16 foreground workers
and two maintenance workers, seed `20260929`, and five-second projection and
housekeeping intervals. The common transaction treatment is prefix selection,
with projection batches of four and housekeeping batches of 32. Each sweep uses
successive transactions at a fixed cutoff and ends with a committed empty query.
PostgreSQL uses split candidate lookup, prepared buffered writes, SERIALIZABLE
transactions, and an explicit ANALYZE five seconds into admission. AeroStore's
service uses Unix-domain transport, ordered expiry publication, hashed due
publication, and the existing complete-read prefix fallback.

The declared policy is foreground end-to-end p99 at or below **50ms**, and drained
foreground throughput at least **95%** of the offered rate. This is a synthetic
test budget, not a supplied HyperFeed SLA. Latency includes arrival queueing,
transaction work, retries/backoff, and delivery of the result to the coordinator.
Whole-job projection and housekeeping latency is reported separately; their
values are not judged against the foreground 50ms threshold.

The driver permits a correctness-valid full-history run that misses the latency
policy to supply its exact metrics companion. Such a pair can be accepted as
valid evidence while preserving a failed performance verdict. A failed workload,
invalid history, missing useful effects, incomplete sweep, or failed resource
check stops the pair. It is not silently converted into a completed saturation
measurement.

## Completed baseline results

The following rows have passed independent metadata, artifact, companion, and
resource review. The full run has a valid complete serial history; the metrics
run has an exact source/executable/configuration/corpus full-history companion.
The companion does not verify the metrics run's unrecorded history.

| Engine | Identities | Offered inputs/s | Evidence | Foreground p99 | Projection job p99 | Housekeeping job p99 | Total retries | Drained foreground inputs/s |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL | 256 | 512 | Full | 9.551ms | 254.512ms | 30.878ms | 31,424 | 511.441 |
| PostgreSQL | 256 | 512 | Metrics | 11.465ms | 234.291ms | 25.808ms | 31,077 | 511.447 |
| AeroStore service | 256 | 512 | Full | 2.556ms | 437.511ms | 96.398ms | 936 | 511.211 |
| AeroStore service | 256 | 512 | Metrics | 2.873ms | 421.808ms | 85.826ms | 1,286 | 511.253 |
| AeroStore service | 256 | 1,024 | Full | 6.298ms | 1,578.854ms | 98.395ms | 93,424 | 1,022.492 |
| AeroStore service | 256 | 1,024 | Metrics | 6.787ms | 1,297.073ms | 86.350ms | 93,225 | 1,022.215 |
| PostgreSQL | 1,024 | 512 | Full | 6.768ms | 1,453.447ms | 113.654ms | 30,085 | 511.420 |
| PostgreSQL | 1,024 | 512 | Metrics | 3.849ms | 604.209ms | 113.022ms | 15,287 | 511.440 |

At 512 inputs/s, all four runs complete 46,080 useful foreground inputs, all 17 projection jobs and
all 17 housekeeping jobs. Three jobs of each maintenance class perform positive
work. Projection claims and reschedules 1,344 events and produces 1,344 outputs;
housekeeping expires 3,136 records. There are 353 committed projection batch
transactions and 116 housekeeping transactions, including each job's terminal
empty query. With only 17 jobs per maintenance class, nearest-rank job p99 is
the observed maximum.

PostgreSQL's metrics run records 31,008 foreground retries, 69 projection retries,
and no housekeeping retries. Its aggregate retry causes are 29,832 serialization
failures during buffered batch writes and 1,245 at commit. These counters show
substantial retry work despite passing the foreground latency policy at this
offered rate. They do not prove where a later saturation point lies.

Full and metrics runs execute sequentially. Their differences do not isolate the
cost of history recording from run-to-run variation. A successful 512-input/s
point is a completed offered-load measurement, not a maximum capacity estimate.

The service metrics run at 512 inputs/s records 765 foreground retries, 517 projection retries,
and four housekeeping retries. Its global due queries report 189,960 candidate
IDs and 3,176 returned prefix rows, including retry work. Global expiry queries
report 55,352 candidates and 3,264 prefix rows. Those gaps reflect broad native
reads before prefix selection; neither count measures physical pages read.

At 1,024 inputs/s, the service completes 92,160 useful foreground messages per
run with the same maintenance effects and batch counts. Its metrics run records
91,461 foreground retries and 1,764 projection retries; housekeeping has none.
Global due lookups report 479,716 candidate IDs and 7,632 returned prefix rows.
Foreground p99 still passes the 50ms policy, but retries grow sharply and the
slowest projection job takes 1.297 seconds. Passing this point does not imply
that either foreground or maintenance cost scales linearly with offered load.

The retries are also distributed unevenly. At 1,024 inputs/s, odd-numbered
workers account for 99.28% of full-mode foreground retries and 99.12% in metrics
mode. Every completed 256-identity service run reports a maximum sampled worker
backlog of one; the counter is not a continuous queue maximum. Four adjacent
identities share a callsign, and round-robin assignment to
16 workers repeatedly aligns each worker with the same position in that group.
The adjacent arrival spacing falls from about 1.953ms at 512/s to 0.977ms at
1,024/s. Complete candidate reads can overlap even when those transactions
update distinct flight families.

This is evidence for investigating repeated arrival-order bias, not proof of a
database defect or a hardware worker-parity effect. Retry classifications with
detailed tracing disabled do not separate row validation from exact predicate
conflicts. The shared deterministic retry delay may contribute, but its causal
effect has not been isolated. Changing the random seed alone does not change
this profile's identity traversal, arrival offsets, or initial routing.

At 1,024 identities, PostgreSQL completes all 46,080 foreground inputs and 34
maintenance jobs in each run. Three projection jobs process 5,376 events;
three housekeeping jobs expire 12,544 records. Completing those sweeps requires
1,361 committed projection transactions and 410 housekeeping transactions.
The full run has substantially more retries and slower projection than its
metrics companion. Sequential measurements do not isolate recording overhead
from timing variation; the difference is retained rather than averaged away.

The full run's serial-history oracle takes 265.335 seconds, outside the measured
90.102-second admission-to-drain interval. The qualifier trial takes 386.201
seconds overall, including setup and checking. This is a correctness-checking
cost, not a database latency measurement. Metrics mode does not replay a complete
history and finishes its qualifier trial in 122.013 seconds.

## Failed runs

| Engine | Identities | Offered inputs/s | Evidence attempted | Outcome |
| --- | ---: | ---: | --- | --- |
| PostgreSQL | 256 | 1,024 | Full | Foreground worker 9 exhausts 128 retries on message `1018233`; metrics is not started |
| AeroStore service | 1,024 | 512 | Full | Projection worker 16 exhausts 128 retries on message `8000000012`, after 12 committed batches in its first job; metrics is not started |
| AeroStore service, unchanged repeat | 1,024 | 512 | Full | The same projection worker exhausts retries on message `8000000037`, after 37 committed batches in its first job; metrics is not started |

The PostgreSQL failure concerns foreground work, rather than the housekeeping failure in
the earlier complete-query investigation. The failing worker's cumulative
snapshot records 1,139 commits and 5,506 aborts: 5,260 serialization failures
during buffered batch writes and 246 at commit. Those counters include earlier
transactions and failed attempts; they do not isolate the final message.

The independent failure-evidence audit passes while retaining the failed
benchmark verdict. The benchmark exits 2, the qualifier exits 1, and the outer
memory envelope exits 1. Cleanup completes with zero OOM and zero swap. Partial
histories remain retained, but no complete-history witness or whole-run latency
and throughput measurement is available. Retry exhaustion is not a completed
PostgreSQL saturation upper bound and cannot be used as the denominator of a
speedup claim.

The larger-population service failure provides a concrete native maintenance
case to investigate. The failed projection worker's cumulative snapshot records
434 transaction starts, 12 commits, and 422 aborts: 398 failures at commit, 22
during due lookup, and two during history lookup. Its due queries report 726,024
candidate IDs and 1,648 returned prefix rows across successful and failed
attempts. These are cumulative counters through the acknowledged service
operation, not the final transaction alone. They expose broad read work but do
not identify which commit dependency rejected each attempt.

An unchanged repeat reproduces the projection failure, at a different batch.
Its worker records 1,279 starts, 37 commits, and 1,242 aborts: 1,185 at commit,
54 during due lookup, and three during history lookup. Due queries accumulate
2,083,996 candidate IDs and 4,900 returned prefix rows. Thus two attempts fail
the first sweep; they do not establish a universal failure threshold or isolate
a dependency-level cause.

Both service attempts retain failed benchmark/qualifier/envelope verdicts,
with no timeout, OOM, or swap and confirmed cleanup. Neither first projection job
completes, so no whole-job latency or complete-run performance comparison can be
reported. The failed partial histories do not establish a capacity bound.

## Ordered due publication and housekeeping eligibility

The separately bound treatment changes only native due publication from hashed
to ordered. It uses the same executable, offered corpus, expiry policy, prefix
contract, resource bounds, and independent full/metrics workflow. Its exact
correctness companion key differs from the hashed control; an earlier hashed
full run cannot supply this treatment's metrics evidence.

Both 1,024-identity, 512-input/s runs complete with valid evidence and pass the
foreground policy. Their useful foreground count, 5,376 projection effects,
12,544 expired records, 1,361 projection transactions, and 410 housekeeping
transactions match the completed PostgreSQL configuration. The table also shows
the subsequent housekeeping-eligibility pair, which completes the same work.

| Expiry eligibility | Evidence | Foreground p99 | Projection job p99 | Housekeeping job p99 | Total retries | Projection retries | Housekeeping retries | Drained foreground inputs/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All active | Full | 3.897ms | 773.485ms | 1,413.912ms | 2,882 | 3 | 185 | 511.096 |
| All active | Metrics | 3.870ms | 753.116ms | 1,275.739ms | 3,106 | 7 | 163 | 511.063 |
| Housekeeping | Full | 3.178ms | 793.437ms | 1,419.244ms | 1,289 | 6 | 182 | 511.103 |
| Housekeeping | Metrics | 3.126ms | 748.402ms | 1,178.370ms | 1,397 | 2 | 152 | 511.106 |

This result supports narrower publication dependencies as a useful treatment for
the repeated projection failure. It does not identify every failed dependency
or establish a general win outside this finite publication window. The adapter
still captures and materializes the complete native predicate before selecting
each prefix. Its metrics run records 1,214,424 due candidate IDs and 5,404
returned prefix rows, including retries. This is not an optimized bounded native
read implementation.

Housekeeping remains a concrete gap: metrics job p99 is 1.276 seconds versus
PostgreSQL's 113ms at the same configured population and rate. The native run
reports 1,360,606 expiry candidate IDs, 17,536 returned prefix rows, and 163
housekeeping retries. Its expiry index currently admits every active record;
PostgreSQL's partial index includes only positions, outbox and deduplication
records. The separate eligibility treatment selects the already implemented native
`housekeeping` eligibility policy while keeping both publication policies
ordered. It tests whether excluding flight and scheduled-event membership helps
this workload; it does not implement a narrower prefix query.

That treatment does not fix the observed housekeeping gap. Its full run records
1,410,846 expiry candidates and 1.419-second housekeeping p99, versus 1,387,269
candidates and 1.414 seconds with all-active eligibility. Its metrics run records
1,310,218 candidates, 16,896 returned prefix rows, 152 housekeeping retries, and
1.178-second housekeeping p99. The latter is about 7.6% below the earlier native
metrics result, but remains well above PostgreSQL's 113ms. Each maintenance p99
is the maximum of only 17 jobs.

Foreground p99 is about 19% lower in both evidence modes with housekeeping
eligibility. These sequential pairs do not isolate a repeatable causal speedup,
so the option is not promoted as a demonstrated performance improvement. The
negative housekeeping result is specific to this profile; it does not establish
that selective eligibility is useless elsewhere. Substantial complete-read
work remains in both variants.

The full treatment's oracle takes 256.850 seconds outside the 90.159-second
admission-to-drain interval. Checking time again remains separate from engine
latency.

## What this profile measures

The larger population increases indexed records and background sweep sizes.
Quiet-flight projection alone can select 448 scheduled events at 256 identities,
or 1,792 at 1,024 identities, in batches of four. PostgreSQL's ordered SQL prefix
can avoid returning the whole eligible set for every batch. AeroStore currently
captures and materializes the complete indexed predicate before selecting the
prefix. Repeating that read as a sweep advances can grow much faster than the
number of identities. Candidate/materialization diagnostics, rather than the
smaller returned-prefix row count, are needed to assess that work.

Ninety seconds provides positive quiet-flight projection at the 5-, 35-, and
65-second cutoffs. The finite seeded old-position cohorts expire at 5, 10, and
15 seconds. Later housekeeping jobs usually test empty completion. This does
not represent sustained retention pressure: the profile's retention cutoff is
one hour, there is no flight creation/retirement turnover, and newly written
records do not age through that cutoff during these runs. The earlier rolling
workload remains a separate retention and reuse guardrail.

The original fixed-profile message IDs also alias some bounded rings. A flight's
successive IDs advance by 192 or 768, both divisible by the eight-position and
32-output ring sizes. Its incoming updates therefore reuse the same slots rather
than populating a realistic retained history. The rolling profile uses a
different ID stride specifically to avoid this aliasing. The current experiment
isolates population and offered-load scaling within the unchanged fixed profile;
it does not repair or conceal that limitation.

Temporary signature affinity uses the same 600ms TTL throughout. At 512 inputs/s,
the nominal interval between messages for an active identity is 375ms with 256
configured identities and 1.5 seconds with 1,024. Population and rate therefore
change the frequency of affinity expiry, even when the policy is unchanged.
Reports must retain dispatch hits/misses, expiry, worker assignment, and actual
same-flight overlap/order observations. TTL expiry alone does not establish that
ownership changed; these regular identity counts can preserve the same owners.
Neither capacity monotonicity nor a causal explanation should be inferred from
rate alone.

In the completed 256-identity runs, the reported routing counts agree across
engines and evidence modes at the same rate. At 512 inputs/s there are 45,888
affinity hits and 192 new-signature misses. At 1,024 inputs/s the service reports
91,968 hits and 192 new-signature misses. Neither configuration records expiry
misses, planned owner changes, or flights assigned to multiple workers. The
completed runs observe zero same-flight overlaps and zero out-of-order
completions. The retry increase between these two service rates therefore does
not coincide with an observed change in flight ownership or message order.

The completed 1,024-identity PostgreSQL and ordered-due service runs instead record zero affinity hits,
768 new-signature misses, and 45,312 expiry misses. They still record zero owner
changes, zero flights assigned to multiple workers, and zero same-flight overlap
or completion-order violations. The regular round-robin pattern retains the
same owners after expiry in these runs; this population comparison has not
tested realistic migration among workers.

Ordered native expiry publication uses the fixed event-time origin
`1700000000000000000` and a one-second bucket width. A 90- or 185-second run stays
inside its 4,093 ordinary positive-time intervals. The one-hour-old housekeeping
cutoff and seeded pre-epoch history occupy the lower overflow bucket. This
differs from rolling tests whose expiry cutoff advances through active-time
buckets. The current run does not test publication-window rotation or long-lived
production behavior.

## Next decisions

The repeated larger-population service failure redirected this checkpoint from
higher offered rates to existing index controls. The
[ordered due publication](hyperfeed_ordered_range.md) treatment completes; the
additional expiry-eligibility treatment also completes but leaves the main
housekeeping gap. Both retain complete native predicate reads and the
fixed-window limitations. The original hashed controls remain unchanged.

The next proposed implementation experiment is an opt-in selective flight-family
equality index using the existing complete OCC lookup. The ordered-due full run
records 5,080,856 family candidate IDs versus 675,850 returned records, about
7.5 candidates per returned row. PostgreSQL already has a `(family, kind)` index;
the native family index includes all active record types and filters afterward.
These counts identify discarded materialization, not its share of CPU time,
RPC time, or retries. A partial flight-family index can test that specific cost
while keeping the broad fallback for other queries. Extra index publication,
allocation, memory, and write costs must be measured too.

This experiment needs unchanged complete results and independent history checks,
plus tests for empty predicates, competing inserts, family/kind/eligibility
changes, private writes, savepoints, and historical complete-or-retry reads.
Fixture attachment and recovery/bootstrap must use the same extractor set.
The existing correctness and source-bound verification checks remain required.

A subsequent, separate transport experiment can buffer native writes into fewer
RPCs while preserving the worker's message body and retry loop. PostgreSQL
already buffers writes. Request counts, batch sizes, flush reasons, retry work,
and end-to-end latency should establish whether batching helps; its error,
savepoint, and lost-commit-reply behavior must remain explicit. Neither this nor
the family index is implemented or has a measured speedup in this checkpoint.

Reserve a new ordered-prefix core algorithm for residual maintenance costs that
these smaller experiments cannot address. The current fallback still reads the
full predicate for every batch. Any narrower algorithm must preserve exact
prefix results, tie ordering, private writes, empty-search dependencies,
historical completeness or retry, and safe reclamation. Returning fewer rows
without establishing those properties would weaken the transaction contract.

Repeated PostgreSQL 1,024/s and intermediate-rate probes remain useful for
separating unfavorable schedules from repeatable limits. If retry alignment
persists, a 15-worker service pair at 1,024/s can test whether concentration
follows worker identity or position in a shared-callsign group. This also changes
concurrency and is therefore a diagnostic comparison. A later arrival-order
treatment should preserve per-flight order, bind its changed corpus, and run
against both engines while retaining the original sequential stress profile.
Randomized retry delays would be a separate experiment. These workload checks
remain necessary before interpreting failures as sustainable capacity limits.

## Reuse, resources, and evidence

The unchanged 503-file source fingerprint is
`608028c5fe176bcefcbe6415dce68b018a6a7994805e5208516cafe4c834ea8f`.
The retained benchmark executable is
`cdf5fa277a719b088f42197a7a8123b418ec84b7862fb734da0659184cb90a6c`.
Its original build receipt and source archive remain the provenance authority;
this checkpoint does not rebuild it or rerun the previous 433 regression
executions. Fresh helper checks comprise 18 mocked driver/admission tests, nine
reviewer tests, seven admission arithmetic fixtures, and six archive/provenance
tests. All 40 pass. The separate ordered-due harness adds 11 passing pure
configuration, companion, and review tests; the housekeeping harness adds six.
All 57 fresh helper checks pass. There are 19 benchmark trial attempts: 16
complete, including four fresh smoke cells, and three retain retry-exhaustion
failures. The later planned rate and worker-count probes are deferred rather
than presented as executed measurements.

The native arena capacity is 2,048MiB at both populations, with the same setting
in each full/metrics pair. The PostgreSQL fixture retains 128MiB shared buffers
and its existing observed settings. Arena capacity is not resident memory;
neither these settings nor a common cgroup ceiling establish equal memory use.
Both engines acknowledge WAL asynchronously, with distinct flush/recovery
implementations; crash durability is not equated.

Each stage requires a fresh, source/build/stage-bound disk admission receipt.
The campaign budgets 60GiB of additional storage while retaining a 30GiB reserve
on both Linux and the Windows host volume. Memory envelopes cap the owned
process group at 36GiB, retain a 4GiB host-available reserve, and reject OOM or
swap activity as clean timing evidence. The completed PostgreSQL pair records
zero OOM/swap and a 3.04GiB peak across the owned group; the service pair records
zero OOM/swap and a 2.48GiB peak. The service's 1,024-input/s pair also has zero
OOM/swap, with a 4.75GiB group peak. These figures include more than the database
process. The 1,024-identity PostgreSQL pair peaks at 2.95GiB with zero OOM/swap;
the ordered-due service pair and the additional housekeeping-eligibility pair
each peak at about 2.90GiB with zero OOM/swap.

Per-second storage samples distinguish native arena high-water and retirement
counters from PostgreSQL relation sizes and estimated dead tuples. They are not
interchangeable RSS measurements. Native prefix `returned_rows` counts the
visible prefix, not the complete read capture. Initial PostgreSQL literal
EXPLAIN samples do not establish the prepared plans executed during the run.

Local evidence is retained under `target/population-saturation-20260928`,
including `pg-f256-r512-review.json`, `service-f256-r512-review.json`,
`service-f256-r1024-review.json`, and the failure-preserving
`pg-f256-r1024-review.json`, plus `pg-f1024-r512-review.json` and the failure-preserving
`service-f1024-r512-review.json` and `service-f1024-r512-repeat-review.json`.
The separate treatments are reviewed in
`service-f1024-r512-ordered-due-review.json` and
`service-f1024-r512-housekeeping-review.json`. Independent reviews
reassess evidence bindings and verdicts; they do not rerun the history oracle.
Calibrated capacity qualification, architecture promotion, and the real-world
10× claim remain closed. The 1,024-identity and 32-foreground-worker implementation
ceilings also remain below a demonstrated production HyperFeed population and
the reported 100–300 workers across single- and multi-machine deployments.

The [portable evidence archive](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/population_saturation_2026-09-28/artifact-manifest.json)
preserves 496 logical files in 660 stored members, including the failed attempts.
Its [independent check](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/population_saturation_2026-09-28/archive-validation-independent.json)
and [completion receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/population_saturation_2026-09-28/archive-validation-completion.json)
record successful source, provenance, and reference-closure verification.
The local retention audit separately checked the original bytes of 2,304 retained
artifacts and five aliases, with no missing or changed files. No compiler build
was performed, no compiler-cache cleanup was needed, and no files were deleted
or space reclaimed. After archival, Linux had 619.64GiB free and the Windows C:
host volume had 378.36GiB free, each above the 30GiB reserve. Accounting for
archive and Git copies uses 12.82GiB of the campaign's 60GiB growth budget.

The archived [implementation experiment notes](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/population_saturation_2026-09-28/ordered_prefix_next_step.md)
describe the selective-index, batching, and bounded-prefix requirements. The
separate [arrival-order design](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/population_saturation_2026-09-28/arrival_permutation_design.md)
preserves the existing stress profile while proposing a way to test its regular
collision pattern. These are proposed follow-ups, not implemented changes.
