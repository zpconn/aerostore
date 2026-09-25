# Fleet campaign review

**Historical timing limitation:** these `f37b288b…` campaigns predate the continuous-drain correction. Their reported workload-plus-drain denominator omits receipt-file flushing and worker shutdown between those intervals. Their original capacity-policy output is retained for audit but withdrawn as current qualification evidence. The corrected gate rejects these reports for passing or failing capacity bounds. Message latency, retries, useful effects, retained failures, and recorded serial witnesses remain observations of these executions; fresh campaigns are needed for corrected throughput qualification.

The fleet workload exposes a substantial native concurrency problem under these synthetic settings. Adding workers at 1,000 offered messages/s causes much higher latency, many retries, and less useful application work. Disabling operation recording does not remove that behavior. The interactive service prototype has its own severe limits. These results identify investigations to run next; they do not select a replacement architecture or establish a 10× capacity comparison.

Both campaigns use the [frozen fleet build](fleet-build-provenance.json): source SHA-256 `7b02ad0fd7c2e1a04db8603c4321015c10c4d82ee3bfaa2cfd6adda457558d87`, binary SHA-256 `f37b288b38be0b86a29f1db32079f5819c4fe28220937219f41260c9c8549033`. Source and binary stayed stable. Each campaign covers three engines, two offered rates (100/1,000 per second), two worker counts (1/4), and three seeds (`20260924`–`20260926`), with five seconds of admissions, sixteen configured identities, a uniform fleet, and the declared 50ms p99 budget. Each cell gets a fresh benchmark process. The fixed corpus is independent of worker count; all inputs must drain without silently reducing arrivals.

The [full-history review](fleet-full-campaign-review.json) retains all 36 cells: **33 Valid histories and three service backlog failures**. There are no Invalid or Inconclusive verdicts. The historical gate counted 27 policy passes and six policy failures among those 33 histories; that capacity output is superseded as described above. The [metrics review](fleet-metrics-campaign-review.json) retains all 36 further cells: **31 completed, explicitly unverified executions and five failures**. Those 31 executions have exact full-history companions; they acquire no serial witnesses from those companions. There are 33 verified recorded histories across these two campaigns, not 64. All completed cells satisfy the fleet population, complete-cycle, and four mutating-background-kind checks, which do not independently qualify capacity.

## Higher offered-rate results

The table shows the range across the three declared seeds at 1,000 offered messages/s. Metrics rows with two completions explicitly exclude the failed third execution from the latency range, while retaining its failure in the completion column.

| Engine / workers | Full-history completions | Full p99 including queue/retries | Metrics completions | Metrics p99 including queue/retries | Metrics missing/deferred inputs |
| --- | --- | --- | --- | --- | --- |
| Direct native / 1 | 3/3 Valid | 5.22–6.20ms | 3/3 | 4.60–5.86ms | 0% |
| Direct native / 4 | 3/3 Valid | 1,460.94–1,888.69ms | 2/3 | 1,197.47–1,370.63ms | 30.18–33.34% |
| PostgreSQL / 1 | 3/3 Valid | 1.65–2.16ms | 3/3 | 1.76–1.87ms | 0% |
| PostgreSQL / 4 | 3/3 Valid | 6.01–6.91ms | 3/3 | 5.19–6.32ms | 0% |
| Interactive Unix service / 1 | 0/3; backlog failures | — | 0/3; backlog failures | — | — |
| Interactive Unix service / 4 | 3/3 Valid | 12,271.64–12,628.28ms | 2/3 | 12,117.85–12,794.58ms | 9.10–9.34% |

Native four-worker full runs have 17,366–17,691 retries per 5,000 inputs; their two completed metrics counterparts have 15,288–16,443. PostgreSQL four-worker runs have 984–1,231 retries in full mode and 689–955 in metrics mode. All one-worker native and PostgreSQL cells have zero retries. At 100 offered messages/s every engine/worker/seed cell completes and has zero retries and missing/deferred inputs. Its original policy pass does not satisfy the corrected timing gate.

The five metrics failures are three one-worker service backlog overflows, plus retry exhaustion for seed `20260926` in the four-worker service (message `1000984`) and direct native engine (message `1004015`). Each exhausted the 128-retry limit. These are retained progress failures, not Invalid-history verdicts or evidence establishing a throughput ceiling. The three full-mode failures are the corresponding one-worker service backlog overflows.

## Correctness and useful work

A valid transaction order can produce fewer useful effects in this model. Missing/deferred outcomes and stale observations are permitted by its matching, allocation, and retention rules; their occurrence does not establish lost committed data. They nevertheless prevent treating equal message counts as equal useful work.

The three native one-worker full runs claim 1,248 events each, cancel 1,252 each, expire 310–313 families, and produce 9,147–9,156 outputs. Native four-worker full runs claim only 380–406 events, cancel 458–502, expire 197–208 families, and produce 2,418–2,567 outputs. They also have 28.22–30.34% missing/deferred inputs and 1,170–1,239 stale observations. PostgreSQL four-worker full runs preserve the event-claim, cancellation, and family-expiry counts, with 8,977–9,016 outputs and at most two stale observations. Valid reordering affects projection and housekeeping even when the missing/deferred fraction is zero.

Initial populations are 12–13 live physical families. One-worker native and PostgreSQL final populations are 13–14; native four-worker full runs finish with 20–21. Two physical allocation banks permit simultaneous generations of a configured identity, so a population above sixteen is possible. Initial/final snapshots do not establish a minimum concurrent population or prove that all logical identities remained active throughout the run.

## What the query diagnostics establish

Native diagnostics for the three 1,000/s full-mode seeds, summed across seeds and workers, are:

| Query stage | Candidate/returned ratio, 1 worker | Ratio, 4 workers | Successful query time, 1 worker | Time, 4 workers |
| --- | --- | --- | --- | --- |
| Candidate matching | 1.23 | 3.26 | 0.058s | 0.175s |
| Family | 3.58 | 2.23 | 0.071s | 0.078s |
| Global due | 1.00 | 1.00 | 6.012s | 15.561s |
| Global expiry | 2.26 | 7.85 | 4.020s | 7.173s |
| Position history | 33.45 | 30.84 | 0.027s | 0.024s |

The measured successful-query time concentrates in global due and expiry queries. A large candidate ratio alone is not a dominant-cost diagnosis: position-history queries have the largest ratio but much less measured time. Calls include failed attempts; candidate counts follow completed index lookups but precede point reads; returned rows and successful-query time count only completed queries. These totals include attempts whose transactions later abort and are neither CPU-time profiles nor costs per committed message.

There is also a concrete adapter schema difference. Native's inherited `event_time` index contains every active record, while PostgreSQL's expiry index contains only active positions, outputs, and deduplication records. Native filters the other kinds after lookup. A matching narrow native expiry index is a useful small control before attributing expiry amplification to the engine. Both due indexes are already partial, and that expiry control alone cannot explain the measured global-due cost or retry counts.

The next investigation should pair that schema control with precise native abort-cause and historical-visibility profiling. A global query can have genuine cross-family conflicts. Retry-stage counts and earlier controlled false-positive examples do not show that unrelated invalidation dominates this workload. The interactive service measurements also combine ownership with per-operation transport and serialization overhead; they do not isolate the cost of database ownership itself.

## Scope of the decision

The original gate selected 1,000/s with one worker for direct native and PostgreSQL, and 100/s for the service. Those historical capacity selections are withdrawn because their denominator omitted the shutdown interval. No corrected lower or upper capacity bound follows from these reports, and no 10× capacity claim follows. The synthetic cadence, ordering freedom, expiry policy, SQL/Tcl compatibility, crash/recovery behavior, and worker-failure availability still require separate evidence. Longer retention runs are also separate from these five-second comparisons.

## Separate retention checks

The [compact retention review](fleet-retention-campaign-review.json) records three further full-history runs on the same source and binary: direct native, Unix service, and PostgreSQL, each with four workers, 100 offered messages/s, seed `20260925`, and 120 seconds of admissions. All three complete 12,000 messages with Valid histories, zero retries, and zero missing, deferred, or stale outcomes. Initial/final populations are 12/13 for each, and every outcome total matches across engines, including 749 expired families, 3,000 claimed events, 3,000 cancelled events, and 21,963 outputs. End-to-end p99 is 3.285ms native, 5.295ms service, and 2.193ms PostgreSQL.

The original gate reported three individual policy passes, but their capacity timing is superseded and this one-seed campaign also fails the three-seed capacity requirement. The [detailed interval analysis](../fleet-retention/README.md) confirms no overlapping messages, so these runs do not establish behavior under sustained contention. Native allocation high-water and PostgreSQL relation sizes are different measurements; neither is process RSS, and two minutes of observations cannot prove bounded memory or a steady state. These three additional Valid histories bring the fleet comparison-plus-retention total to 36 recorded witnesses; metrics executions remain unverified.
