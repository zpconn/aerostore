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
the transaction-boundary description above remains based on the paper.

## Current implementation and gaps

The [calibrated profile](hyperfeed_calibrated.md) routes a resolved synthetic
identity permanently to one worker using its ordinal modulo the worker count.
This is an ordering control, not the paper's dispatcher. Its uniform cyclic input
also makes worker balance easier than a variable-rate population of real flights.

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

1. Retain permanent-identity routing as a control. Add the confirmed dispatcher
   with explicit affinity lifetime, callsign/registration variation and tests
   where different input signatures resolve to the same flight or affinity
   expires while earlier work is still in flight. A dispatcher
   must use information available in the incoming message, not the oracle's
   already-resolved flight identity.
2. Parameterize provenance populations and eligibility while checking every
   distinct fork update and output. Preserve message-level commit/retry behavior.
   Confirm whether the approximate 3–8 count includes the all-provenance parent;
   do not equate a range of observed counts with a uniform random distribution.
3. Complete each maintenance sweep through bounded transactions, then add flight
   creation, growth, terminal transitions and retirement. Treat daily flight
   volume, concurrently updating flights and retained families as separate inputs.
4. Expand the reviewed harness/resource limits to support 100/200/300-worker
   experiments. Measure load imbalance, useful input completions, eligible fork
   updates, retries, queueing-inclusive p99 and retention during maintenance.
   Choose an efficient AeroStore worker count from measurements; the historical
   PostgreSQL counts are comparison points, not a requirement to use identical
   concurrency to reach the target.

Still unknown: concurrent active and retained populations, input message rate and
bursts, fork-count/eligibility distributions, affinity lifetime/refresh behavior
and maintenance transaction granularity. Until those are calibrated,
parameter sweeps are sensitivity experiments with declared assumptions. Daily
flight counts alone cannot supply these missing quantities.

The [previous validation archive](bench_data/calibrated_2026-09-25/README.md)
remains unchanged. This additional calibration does not retroactively enlarge
its coverage or qualify a throughput claim.
