# Query dependency capture experiment

A transactional index query records the publication stamp of every bucket it
reads. Commit validation uses those stamps to detect a competing change. The
previous capture loop searched the entire growing dependency vector for every
requested bucket, including entries just appended by the same query.

The candidate records the vector's length before capture and searches only that
initial prefix. A query's bucket IDs are unique after canonicalization, so an
entry appended for an earlier bucket cannot match a later bucket of that query.
Dependencies from earlier queries remain searchable. Index identity remains
part of the comparison, and a changed stamp still makes the transaction fail.

The implementation uses the original whole-vector iterator while the vector's
length equals the saved prefix length. Once an entry is appended, it searches
the bounded slice instead. Both branches search exactly the same logical
prefix. This hybrid spelling was selected after the simpler slice expression
produced a compiler-related repeated-query slowdown in the microbenchmark.

For a fresh query over 4096 distinct buckets, the old search examines
`4096 * 4095 / 2 = 8,386,560` newly appended entries. The candidate examines no
entries in the initially empty prefix. It still locks every required bucket,
reads every stamp, records every dependency, and validates the same predicate
at commit. For `k` requested buckets and `m` prior dependencies, its searches
can still examine up to `k * m` entries. Repeated broad queries can therefore
remain expensive.

This is a narrow change to capture cost. It does not change the index's conflict
partitions, historical read behavior, retry policy, expiry eligibility, WAL
ordering or durability contract. Hashed publication remains the default; the
optional [ordered due policy](hyperfeed_ordered_range.md) remains a separate
experimental setting. The shared index header remains version 3.

## Correctness boundary

The native selector's successful paths must return unique bucket IDs. The
capture adapter checks the selector's wiring and represents the frozen length
and bounded search explicitly. The proof tracks the unchanged prior prefix and
the origin of appended entries, proving that the skipped suffix cannot match
the current bucket. Verified sort and bitmap kernels supply uniqueness in their
feature configurations; the default standard-library sort/dedup remains an
explicit trust assumption.

The [capture proof](../verification/predicate_capture/README.md) and its
compositions retain their conditional scope. The
[diagnostic baseline review](../verification/retry_diagnostics/capture_prefix_review.md)
records the semantic source change explicitly; the diagnostic normalizer does
not erase it. Native tests cover repeated and overlapping queries, separate
indexes, savepoint retention, and capture failure after partial progress.
These checks do not establish whole-engine serializability, universal ordered
mapping correctness, or worker-death availability.

## Measurement design

The comparison preserves the previous default-feature release binary and its
exact source bundle, then builds the candidate with the same compiler and
features. Each binary runs its qualification helper from the matching source
root. Full-history companions stay within the same source, binary, policy,
workload and seed. Metrics runs do not independently verify their own histories.

First, a standalone native microbenchmark compares fresh empty/populated range
queries, equality queries, repeated range queries, and equality followed by a
range. A median control slowdown of at least 10% triggers investigation before
acceptance; raw block timings remain available to assess noise.

The concurrent comparison crosses old/new capture with hashed/ordered due
publication. Expiry publication and all-active eligibility stay fixed. It uses
three seeds at 512, 1024 and 2048 offered foreground messages per second, with
four foreground workers, two maintenance workers, 16 configured identities,
and seven seconds of admission. Projection and housekeeping run every one/two
seconds through complete batched sweeps. These are accelerated stress settings,
not the operator's five-to-ten-minute cadence or realistic fleet scale.

Each block rotates the four native variants, alternates PostgreSQL's position,
and reverses order for metrics after the full-history runs. PostgreSQL uses the
unchanged prepared/buffered SERIALIZABLE adapter. Both native builds omit retry
diagnostics. Asynchronous WAL contracts are recorded; crash equivalence is not
claimed. Failed and stale-work trials remain visible and are excluded from
numeric paired improvements. Fixed-rate latency improvements must not be
reported as throughput speedups or evidence of the 10× goal.

## Native microbenchmark result

The final six-block comparison completed all 168 processes, with 10,752 timed
transactions and 672 retained warmups. Independent recomputation agreed with
the recorded results. Across empty/populated fixtures and hashed/ordered
policies, fresh broad lookup time fell 96.59–96.65%; the complete fresh
transaction fell 88.39–88.56%. The second repeated broad lookup changed by
−0.19% to +0.27%, while the two-query transaction fell 45.88–46.09%.
Equality-heavy total lookup time increased 1.05–1.23%, and transaction time
increased 1.09–1.85%. These are medians of paired block changes, not confidence
intervals or whole-workload throughput ratios.

The first slice-only candidate failed the repeated-query control: its second
lookup was about 39% slower. Several isolated spellings were compared before
selecting the hybrid expression. Generated assembly suggests repeated loop-bound
loads explain that regression; no hardware-counter attribution was available.
The rejected candidate and exploratory measurements remain part of the evidence.
The final hybrid passes the predeclared 10% control threshold.

## Concurrent result and next decision

All 36 candidate cells completed with useful work and valid full-history or
exact-companion evidence. Of 36 baseline cells, 27 completed and 22 qualified
for numeric comparison. PostgreSQL completed 12 of 18 cells; six qualified.
All 15 execution failures occurred at 2048 offered messages/second: nine native
baseline retry-limit failures and six PostgreSQL backlog-limit failures.
Eleven additional completed cells were excluded for stale-work or companion
failures. The full archive retains those exclusions.

In qualified metrics pairs at 512 messages/second, median paired foreground
p99 fell 80.6% with hashed publication and 77.3% with ordered publication.
Hashed projection and housekeeping p99 fell 96.1% each. The candidate also
completed all 12 cells at 2048 messages/second, where no baseline cell qualified
for a numeric comparison. That is evidence of improved behavior under this
specific stress load, not a sustainable-capacity ratio.

With ordered publication, the capture change increased projection p99 at
1024 messages/second in all three metrics pairs by 0.035–0.406 ms (3.1–37.4%, median
13.9%). These tails contain only six projection jobs per run. Foreground and
housekeeping latency improved strongly, but the projection result remains
visible and the ordered policy remains experimental.

Only the 512-message/second PostgreSQL controls qualified for numerical
comparison. In metrics runs, their foreground p99 was a median paired 7.9 times the candidate's
with hashed publication and 8.2 times with ordered publication. These are
latency ratios at a fixed offered load, not throughput speedups. PostgreSQL's
1024-message/second trials completed but contained stale fork updates; they
are excluded. This experiment does not demonstrate the 10× goal.

The next experiment should establish useful-work capacity with longer runs,
replenished populations, worker-count sensitivity, and the operator's five-to-ten
minute maintenance cadence. Keep the current short stress corpus as a regression
control. Measure complete messages and per-fork effects, queue growth, retries,
end-to-end p99, and retention; use matched PostgreSQL settings and resources.
This short corpus has only one positive-effect projection sweep; later empty
sweeps do not demonstrate sustained maintenance capacity. Active population,
arrival rate, turnover and message mix still need calibration.
Daily flight counts must not be substituted for simultaneous active flights.
Worker-death availability and physical two-host MMHF remain separate unresolved
qualification requirements. Further index changes should follow these results.
