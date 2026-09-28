# PostgreSQL candidate-query investigation

Splitting flight matching into two indexable branches corrects a PostgreSQL
access-path problem, but does not resolve the higher-load housekeeping failure.
Both query forms exhaust the retry budget at the same housekeeping step in fresh
instrumented and uninstrumented runs. This checkpoint narrows the next experiment;
it does not establish AeroStore's 10× throughput target.

This follows the [statistics checkpoint](hyperfeed_statistics.md). The
[evidence archive](bench_data/pg_candidate_2026-09-28/artifact-manifest.json)
retains source snapshots, executable/runtime bindings, full and partial histories, executed
plans, failure reports, resource accounting, and independent reviews.

## Selectable query shape

The contention benchmark and qualification driver accept
`--pg-candidate-query or|split`. The default remains `or`. The split form uses
one SQL statement with disjoint `UNION ALL` branches:

* Match callsign and the schedule window.
* Match nonzero registration and the schedule window, excluding callsign matches
  already returned by the first branch.

The result has one global ordering by record ID. Both branches share a statement
snapshot and SERIALIZABLE transaction. Existing unsigned-window handling, signed
identity values, buffered read-your-writes, and savepoints retain their semantics.
The split form changes neither the selected business records nor the index schema.

Requested and effective treatment metadata participates in exact correctness
companion matching. AeroStore records the setting as inapplicable. Historical
reports that omit it mean the original query; modern explicit requests require
matching metadata. Initialization-time literal plan audits reflect the selected
form and are retained even when a later workload fails. They remain distinct
from executed prepared-plan evidence.

The frozen source fingerprint is
`aa0b93d19150546405ccb333a42b628293695e0ff2df74d38cd7d512fc930e5b`;
the benchmark executable is
`9ac86745503fed8bc83c12552614bad08b0faa8cd2d89cc684984b38a7fda761`.
All 216 focused Rust/Python regression checks passed, including live PostgreSQL
tests for competing creation after empty callsign or registration searches,
duplicate exclusion, signed extremes, ordering, buffered overlays, and savepoints.
Six full/metrics smoke cells passed with exact treatment-specific companions.
Production engine algorithms and formal proofs are unchanged.

## Executed plans

Separate diagnostics use the retained PostgreSQL 16.13 `auto_explain` module,
sampling 1% of completed statements with no minimum duration. Execution analysis
and per-node timing are disabled; row counts in plans are estimates. The observer
binds the exact fresh scratch schema and samples serialization read locks once
per second. Both diagnostic captures pass their provenance, resource, and cleanup
checks, while preserving the workloads' failed verdicts.

| Observation | Original OR | Split branches |
| --- | ---: | ---: |
| Sampled completed plans for the owned schema | 19,067 | 18,758 |
| Sampled candidate plans | 1,075 | 996 |
| Candidate plans with identity equality in index conditions | 0 | 996 |
| Snapshots with relation-level `SIReadLock` on `records` | 75 / 102 | 10 / 102 |
| Reported read/write dependency serialization errors | 56,668 | 55,357 |

Every sampled original candidate plan uses `callsign_idx` with only schedule
bounds in `Index Cond`, then applies callsign/registration matching as a filter.
Every sampled split candidate plan instead places callsign equality in
`callsign_idx` and registration equality in `tail_idx`, alongside schedule bounds.
The registration branch's callsign inequality remains a duplicate-exclusion
filter. All 996 split plans and 1,074 original plans visibly retain parameter
references in plan expressions.

Sampled batch updates use nested loops with primary-key access, and sampled
ordered row-locking statements use the primary key. Family searches almost
always use `family_idx`; one original-query sample uses `callsign_idx` and filters
by family. Each diagnostic captures one global expiration query using
`event_time_idx`. These completed-plan samples do not establish the access paths
of failing or unsampled statements.

The lower frequency of table-wide read locks is consistent with narrower access.
It does not attribute individual locks to statements or prove the cause of a
serialization failure. Snapshots repeat persistent locks and miss short-lived
ones. Diagnostic timings are excluded from performance comparison.

## Higher-load controls

All four PostgreSQL attempts use seed 20260929, 512 offered inputs/second,
32 configured identities, 16 foreground workers plus two background workers,
a planned 185-second admission, temporary signature affinity, 40-second retention,
and a 1,280-input rolling cycle. Both background jobs run every five seconds,
with projection batches of four and housekeeping batches of 32. PostgreSQL uses
prepared statements, buffered writes, SERIALIZABLE isolation, and an explicit
statistics refresh five seconds after admission.

Both instrumented forms and both uninstrumented forms fail on worker 17,
message `8000159744`: the first batch of the 100-second housekeeping job exhausts
128 retries (129 attempts). No PostgreSQL metrics companion runs after a failed
full-history attempt. Partial histories and successful transaction receipts are
retained, but do not establish a completed-run throughput or latency denominator.

The original uninstrumented housekeeping worker's lifetime counters record
170 predicate queries returning 282,112 rows, versus 4,800 buffered writes and
38 commits. These are cumulative counters, not measurements of only the failing
transaction. They expose substantial read amplification worth investigating.

The fresh AeroStore central-service control completes both 185-second runs using
ordered expiry publication, hashed due publication, and all-active expiry
eligibility. Each completes 94,648 useful business messages, 72 retirement
controls, and 36 jobs of each background class, with repeated expiration and
generation reuse.

| Evidence | Foreground p99 | Whole housekeeping job p99 | Total retries |
| --- | ---: | ---: | ---: |
| Full | 2.582 ms | 366.261 ms | 794 |
| Metrics | 2.963 ms | 265.392 ms | 1,532 |

Foreground latency includes queueing, retries, and retirement controls. With 36
maintenance jobs, nearest-rank maintenance p99 is the observed maximum. The full
run passes the complete-history oracle; the metrics run has its exact source,
binary, treatment, and corpus companion, rather than an independently verified
history. These are one-seed operating points. No engine optimization in this
checkpoint explains differences from earlier service timings, and the failed
PostgreSQL controls do not establish a capacity denominator.

The campaign used a 36 GiB memory envelope with 4 GiB swap allowance and a 4 GiB
host-availability reserve, without CPU quotas or `memory.high` throttling.
All completed controllers reported zero swap and zero OOM/limit events; the
service pair's peak accounted memory was about 4.87 GiB. The disk budget allowed
20 GiB additional allocation and preserved 30 GiB reserves on both Linux and the
Windows C: volume hosting the WSL disk.

After all jobs exited, audited cleanup reclaimed 391.81 MiB of compiler caches.
The final check preserved 2,211 referenced artifacts and five loader aliases,
with no new missing or changed files. The supplemental tool inventory includes
PostgreSQL's local `libpq` dependency. Windows free space did not increase during
cleanup; released Linux allocation is not a claim of VHDX space returned to
Windows. Binaries remain at their original referenced paths, and the portable
archive retains their bindings rather than bundling them.

## Next experiment

Current maintenance transactions query the complete eligible set, sort it, and
then select a bounded write batch. Bounded writes therefore do not imply bounded
reads. This behavior is explicit in the workload model and its complete-query
history oracle; silently adding a PostgreSQL-only `LIMIT` would change the
contract being compared.

Investigate an explicit ordered bounded-selection contract for both engines:
return the earliest eligible records with a deterministic ID tie-breaker,
preserve read-your-writes, and detect changes that could alter the selected
prefix, including empty results. The independent serial oracle must recompute
that prefix. Start with a conservative complete-read fallback in AeroStore and
an ordered SQL prefix query with suitable indexes in PostgreSQL. Buffered
overlays must refill the prefix when local writes remove leading rows. This
tests the opportunity before investing in a new native index primitive; the
existing limited-posting lookup does not establish ordered-prefix completeness.
Retain the current complete-query scenarios as controls, and
measure candidate rows, retries, complete-message latency, and maintenance
completion before adopting the change.

This small-population workload deliberately accelerates background cadence and
retention. It remains a contention stress test. Larger populations, the reported
five-to-ten-minute HyperFeed cadence, sustained capacity brackets, matched crash
contracts, and physical two-host MMHF measurements remain necessary for the
project's replacement and 10× claims.
