# Rolling workload findings — 2026-09-27

The follow-up [expiry-range experiment](hyperfeed_expiry_range.md) implements
the bounded investigation proposed below and retains its results separately.

The next bounded engine experiment should test narrower expiry-range
dependencies. The existing expiry-eligibility filter did not resolve contention
in the new rolling controls. Keep the current database-owned service as a
control: its longer full-history run exhausted housekeeping retries, while the
direct and PostgreSQL runs completed. These findings do not establish a reason
to replace the ownership design or a result against the project's 10× target.

The [evidence archive](bench_data/rolling_2026-09-27/README.md) retains the
successful controls, the earlier failed process run, diagnostics, and the longer
pilot. The [workload guide](hyperfeed_rolling.md) defines the synthetic lifecycle;
the [resource review](hyperfeed_capacity_resources.md) explains the remaining
population, worker, service-frame, and memory-accounting limits.

The short controls used 32 foreground workers, 64 configured identities, 256
offered foreground inputs/second for ten seconds, 16 inputs per generation,
one-second retention and maintenance timers, complete sweeps, and temporary
signature affinity. Each completed 2,560 foreground inputs and 18 maintenance
jobs in 2,882 transactions. Full histories were valid. Each observed 96 family
retirements and 96 generation reuses, with 9,664 records expired by housekeeping.

| Engine, diagnostics disabled | Foreground p99, ms | Whole-housekeeping-job p99, ms | Housekeeping retries |
| --- | ---: | ---: | ---: |
| AeroStore direct | 3.21 | 120.84 | 79 |
| AeroStore Unix service | 3.00 | 526.94 | 443 |
| PostgreSQL | 14.70 | 143.57 | 65 |

These are individual exploratory runs, not repeated capacity estimates. Latency
includes retries and scheduled-arrival queueing. Foreground latency includes
retirement controls; business-message throughput excludes them. There are only
nine housekeeping jobs per run, so their p99 is the slowest observed job. All
engines completed roughly the same offered rate. A latency ratio at that fixed
rate cannot be converted into a capacity ratio. Projection did no useful work
within this ten-second window because its physical horizon remains 30 seconds.

A separate instrumented service run completed with 494 retries: 448 rejected an
`event_time` predicate stamp at commit, 20 encountered a busy `event_time`
bucket, 12 found a post-snapshot `event_time` stamp during lookup, and 14 involved
the family index. Thus expiry stamp validation accounts for 90.7% of the observed
rejections. Instrumented timing is not used in the table or treated as a
measurement of instrumentation overhead. A stamp rejection identifies the
native decision branch; it does not identify the writer or prove that every
rejection was avoidable. A busy bucket likewise does not identify its holder.

The initial service process run exhausted 128 retries on the first batch of its
ninth housekeeping job while formal checks were also running. Its worker had
649 commit-time rejections and 13 lookup-time rejections, with all 61,407 protocol
operations acknowledged and no recorded transport failure. The later quiet run
completed. The subsequent quiet 185-second service full-history run also
exhausted 128 retries, on the housekeeping job admitted at 160 seconds. Formal
interference therefore cannot explain away the observed liveness problem.

The longer pilot used 16 workers, 32 configured identities, 256 inputs/second,
640 inputs per generation, 40-second retention, and five-second timers. Its 24
active identity lanes have two pools each; a generation lasts 60 seconds at this
particular rate. Direct and PostgreSQL completed full and metrics runs with
matching full-history companions. Each completed 47,288 business messages,
observed 48 retirements and 48 reuses, and produced 144 outputs across six late
projection jobs and 5,600 expirations across nine late housekeeping jobs.
Metrics foreground p99 was 1.00 ms direct and 2.98 ms PostgreSQL; both completed
about 255 useful business messages/second including drain. This is one seed and
one offered rate, with reversed engine order between evidence modes, not a
saturation comparison or a production-cadence result.

The service metrics run completed with useful lifecycle coverage, but its
matching full-history run failed. Its own operation history was not recorded.
It is excluded from numeric performance comparisons, even though its aggregate
business effects match the successful runs. Successful companions validate
separate executions; they do not retrospectively verify omitted metrics
histories.

There are two interacting mechanisms worth separating. Hashed publication maps
the expiry range to all 4,096 index buckets. The adapter also materializes the
complete candidate result before the handler selects a batch. The service then
issues an individual read and write for each selected row: approximately 67
synchronous operations for a full 32-row attempt. Candidate materialization
happens inside the server-side query; the million-scale read counter in the
failed run is not a million RPCs. Extra time between query and commit plausibly
increases exposure to index changes. The observed direct/service difference
supports testing this interaction, without establishing its causal share.

Four additional ten-second service controls enabled native diagnostics and
counterbalanced the eligibility policies across two seeds. All four passed
their full histories with stable source and binary identities:

| Seed | Housekeeping retries, all-active → filtered | Whole-job p99/max, ms, all-active → filtered | Expiry commit-stamp rejections, all-active → filtered |
| --- | ---: | ---: | ---: |
| 20260927 | 505 → 532 | 745.87 → 816.67 | 493 → 520 |
| 20260928 | 532 → 573 | 682.76 → 925.47 | 521 → 562 |

Filtering removed the unwanted record kinds from materialization: candidate and
returned-row counts agree in both filtered cells. It did not remove the broad
predicate dependency, and neither observed pair improved housekeeping latency
or retries. Two seeds do not establish a universal regression. One all-active
run expired 9,663 records; the others expired 9,664. Keep that valid
transaction-order difference rather than assuming identical maintenance effects.

The next experiment should change one dependency policy at a time:

1. Add an explicit experimental expiry-publication option using the existing
   `OrderedI64` mapping. Keep hashed publication as the baseline, expiry
   eligibility as `all-active`, the due policy, batch sizes, query semantics,
   protocol, and retry budget fixed. Combining filtering with the mapping would
   obscure which change helped.
2. Compare direct/service × hashed/ordered expiry with matched inputs and balanced
   repeated ordering. Use short diagnostic probes to check the rejection mix,
   followed by useful rolling projection/housekeeping runs with exact
   full-history companions. Record all failures, useful completions, whole-job
   latency, foreground latency, backlog, bucket contention, and retention.
3. Predict fewer expiry-stamp rejections and shorter housekeeping jobs in both
   paths. If those counts remain unchanged, or lock contention and latency rise,
   the candidate has not demonstrated the intended benefit. If dependency
   rejections fall but service housekeeping remains slow, test a bounded
   server-side maintenance transaction or command batch next, preserving the
   same reads, writes, atomicity, complete terminal probe, and outcome recovery.

Before timing a new mapping, retain regressions for empty queries, inserts,
deletions, movement across the cutoff, own writes, old snapshots, and complete
terminal probes. The existing proof and source-binding checks remain guardrails;
changing the fixture does not enlarge their claims.

The ordered mapping is a finite-window experiment. At one second per interval,
its 4,093 interior intervals cover about 68 minutes; earlier and later signed
keys accumulate in saturated endpoint buckets. It never wraps or silently
rotates. A short run entirely inside that window cannot establish sustainable
precision. Test shifted origins and saturation explicitly. Grouping contemporary
writes into one time interval can also increase writer contention. Any eventual
long-lived range design must preserve completeness and historical-reader safety
while addressing those limits.

Accelerated five-second maintenance remains distinct from HyperFeed's reported
five-to-ten-minute cadence. Changing the offered rate with a fixed cycle length
also changes flight lifetime; a rate sweep must account for that coupling.
Equal crash durability, resource equivalence,
physical MMHF, and survivor availability remain separate requirements; neither
these diagnostics nor a lower p99 qualifies a PostgreSQL replacement.
