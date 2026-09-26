# HyperFeed workload calibration

This ledger separates the architect's operating experience, the published design,
and assumptions in the executable workload. The figures below were supplied by
Zach Conn on 2026-09-25; they are approximate historical operating points, not a
production trace or a measurement of today's FlightAware systems.

## Operator guidance

| Dimension | Supplied information | Benchmark implication |
| --- | --- | --- |
| Single-machine workers | Started with about 100 HyperFeed workers | Include a 100-worker operating point once harness limits and resources support it. Workers are not equivalent to dedicated CPU cores. |
| MMHF workers | Grew to about 200–300 workers across machines, connecting to one PostgreSQL host; PostgreSQL limited further growth | Include 200- and 300-worker operating points and measure saturation. Local processes can test concurrency; physical remote-host performance still needs separate machines. |
| Dispatch | Round-robin with temporary affinity for the same `(callsign, registration)` signature; the architect confirmed MMHF retained this policy | Distinguish intended balancing from measured worker load. Reproduce temporary affinity and retain alias/affinity-expiry contention. |
| Flight volume | About 70,000–100,000 flights per day | This is daily flight volume, not simultaneous active flights, retained database population, or messages per second. |
| Forks | Roughly 3–8 forks per physical flight | Vary provenance views and their eligibility. The number of existing forks does not imply that every message updates all of them. |
| Position handling | Each fork eligible for an incoming position receives a distinct update | Check each eligible fork's state, position history and output separately; exclude ineligible forks. |
| Maintenance | Projection and housekeeping each normally run every 5–10 minutes | Complete scheduled jobs while foreground input continues. |
| Maintenance transactions | Recollection: batches of updates committed together, not necessarily an entire sweep in one transaction | Use multiple batch transactions per sweep as the working contract; exact batch sizes, grouping and differences between jobs remain unconfirmed. |
| PostgreSQL statement reuse | Heavy use of "stored statements," likely prepared statements; exact API not recalled | Reuse prepared workload statements across transactions and retries, and examine their actual query plans when tuning the PostgreSQL baseline. |
| Ordering | Messages for one flight normally arrive in order | Preserve ordinary ordering without assuming that every message resolving to that flight executes on one worker. |

The reported PostgreSQL bottleneck is useful operating evidence. It does not
identify its internal cause or establish either database's capacity in this
synthetic benchmark. The 10× goal remains useful completed input messages per
second under matched workload, resources and transaction/durability contracts.
Fork-update counts are a separate measure of equivalent business work.

## Published dispatcher and transaction boundary

The 2016 paper describes round-robin dispatch with expiring affinity keyed by
`(callsign, registration)`, implemented using a TTL map (§4.1). This allows messages
for one physical flight to reach different workers. It describes one transaction
and retry loop per input message, with fork creation using savepoints (§4.2.2).
That supports multiple distinct fork updates within one message transaction,
without implying independent commits for each fork. [Paper](https://www.tcl-lang.org/community/tcl2016/assets/talk37/hyperfeed-paper.pdf).

In a follow-up on 2026-09-25, Zach confirmed that MMHF retained temporary signature
affinity. This is now operator-confirmed dispatch behavior for both deployments.
The affinity lifetime and precise refresh/expiry behavior still need calibration;
the input-message transaction boundary above remains based on the paper.

Zach subsequently recalled that maintenance committed batches of updates together,
not necessarily an entire sweep atomically. Treat this as approximate operator
guidance supporting a sweep composed of batch transactions, not a measured batch
size or confirmation that projection and housekeeping used identical grouping.
Keep batch sizes configurable and label chosen values as experimental. This
maintenance guidance does not change the foreground message transaction boundary.

He also recalled extensive use of "stored statements." Prepared statements are
the likely interpretation; the exact API remains unconfirmed. The current
[PostgreSQL adapter](../aerostore_core/benches/contention_crucible/postgres.rs)
already prepares its workload reads, predicate queries, row locks and writes once
when connecting, retaining the statement handles across transactions and retries.
Its buffered mode commits discovered row changes with a prepared set-based update.
This groups row writes within one transaction. The optional [maintenance sweep
mode](hyperfeed_maintenance.md) now repeats whole batch transactions, retaining
prepared statement reuse, until a committed complete query is empty. Existing plan
reports use literal/custom examples and do not inspect the plans actually selected
for repeatedly executed prepared statements. Include that inspection in future
PostgreSQL tuning before drawing a capacity comparison.

## Current implementation and gaps

The [calibrated profile](hyperfeed_calibrated.md) retains permanent synthetic
identity routing as its default control. Its optional [signature-affinity mode](hyperfeed_affinity.md)
now implements round-robin assignment with temporary input-signature affinity,
an explicit experimental TTL and optional mixed identifier forms. Uniform cyclic
input and the alias distribution remain synthetic; the profile does not establish
the load balance of a variable-rate population of real flights.

The [message model](../aerostore_core/benches/contention_crucible/model.rs) already
performs distinct updates to eligible provenance views inside one transaction.
Three provenance bits generate seven nonempty combinations; calibrated warmup
always establishes all seven views. A normal single-source position therefore
updates four eligible views, each with its own flight state, position record,
scheduled event and output. The remaining three views must not incorporate that
source. This captures fan-out, but not the reported variation in fork counts or
the real distribution of source eligibility.

In that current fixture, a useful position normally writes 17 distinct logical
records: four records per eligible view plus one message deduplication record.
That count excludes index and WAL work. The fixed 128-slot family layout reserves
space for seven views and their associated histories; adding an eighth view
requires changing the layout and its checks, not just a loop bound.

The optional [complete maintenance mode](hyperfeed_maintenance.md) keeps each
timer job's original cutoff across bounded transactions and requires a committed
empty query to finish. The default batch control is retained. Sweep seeds use at
most three finite expiry cohorts, while the batch control preserves its older
population; this difference prevents treating mode comparisons as an isolated
transaction-loop benchmark. Full jobs are not atomic snapshots, and finite cohorts
do not supply sustained lifecycle turnover.

The harness currently accepts at most 32 foreground workers and 1,024 logical
families. The archived calibration runs used four foreground workers and 16
families. Those runs establish functional behavior under those limits; they do
not exercise the newly supplied operating scale. Raising an argument limit alone
would not establish support: transaction registrations, service sessions,
PostgreSQL connections, memory allocation, history capture and the oracle need
an explicit resource and scaling review.

Two concrete limits already need attention. The [service prototype](../aerostore_core/benches/contention_crucible/service.rs)
defaults to 128 sessions. The native [process array](../aerostore_core/src/procarray.rs)
has 256 transaction/epoch registrations, and an indexed lookup can temporarily
acquire an additional registration for its skiplist scan. These are simultaneous
registrations, not a count of connected workers; internal operations need headroom
beside application transactions. The current contention adapter treats exhausted
registrations as fatal. Supporting the requested worker scale therefore needs an
explicit admission/headroom policy and exhaustion tests, not only larger CLI
bounds or a blindly increased constant.

## Next implementation requirements

1. Use the new temporary-affinity option and retain permanent-identity routing as
   its control. Calibrate the affinity lifetime/refresh policy and identifier
   distribution; compare identical message corpora and inspect actual useful
   fork updates when valid processing orders differ. Routing uses visible input
   fields, not the oracle's already-resolved flight identity.
2. Parameterize provenance populations and eligibility while checking every
   distinct fork update and output. Preserve message-level commit/retry behavior.
   Confirm whether the approximate 3–8 count includes the all-provenance parent;
   do not equate a range of observed counts with a uniform random distribution.
3. Validate the implemented complete sweeps under sustained concurrent input,
   using the operator's recollection as the working transaction contract. Measure
   batch-size sensitivity, retries and whole-job completion; the default
   four-event/32-record sizes are synthetic. Keep timer cadence and seed cohorts
   fixed when comparing sizes. Then add flight creation, growth, terminal
   transitions and retirement. Treat daily
   flight volume, concurrently updating flights and retained families as separate
   inputs.
4. Expand the reviewed harness/resource limits to support 100/200/300-worker
   experiments. Measure load imbalance, useful input completions, eligible fork
   updates, retries, queueing-inclusive p99 and retention during maintenance.
   Choose an efficient AeroStore worker count from measurements; the historical
   PostgreSQL counts are comparison points, not a requirement to use identical
   concurrency to reach the target.

Still unknown: concurrent active and retained populations, input message rate and
bursts, fork-count/eligibility distributions, affinity lifetime/refresh behavior
and maintenance batch sizes/grouping. Exact PostgreSQL statement APIs and plan
behavior are also unconfirmed. Until those are calibrated,
parameter sweeps are sensitivity experiments with declared assumptions. Daily
flight counts alone cannot supply these missing quantities.

The [previous validation archive](bench_data/calibrated_2026-09-25/README.md)
remains unchanged. This additional calibration does not retroactively enlarge
its coverage or qualify a throughput claim.
