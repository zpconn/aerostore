# Resource limits for the next HyperFeed capacity experiment

The current harness can exercise 16 or 32 foreground workers without increasing
engine or service limits. Calibrated runs add two maintenance workers. These bounds
permit testing those configurations; they do not establish a sustainable rate. This milestone retains the 32-worker cap; **100 workers are not
supported by the harness**.

| Resource | Current bound and consequence |
| --- | --- |
| Foreground workers | Rust runner, calibrated scheduler and Python qualification checks accept at most 32. The scheduler also records worker ownership in a `u32` bit mask; merely increasing the validators would be incorrect. |
| Service sessions | 128 simultaneous connections. Each accepted session has its own executor thread with an 8 MiB stack reservation. A 32-worker calibrated run uses 34 sessions, reserving about 272 MiB of executor stack address space; this is not measured resident memory. |
| Native transaction registrations | The shared ProcArray has 256 slots. Each active OCC transaction uses one, and an index operation can temporarily use another. Do not equate 256 slots with 256 available application workers. |
| Population | At most 1,024 configured logical identities. Calibrated initialization reserves two physical families per identity and 128 record slots per physical family: 256 slots per identity, including inactive slots. This is a fixture layout, not a realistic population estimate. |
| Shared arena | CLI range 32–3,584 MiB. Rows, versions, indexes and the WAL ring share this budget. Successful initialization does not establish adequate headroom under sustained updates. |
| Service messages | Each encoded request/reply is limited to 8 MiB. Transactions are limited to 100,000 operations and 60 seconds. Maintenance batch size limits writes, **not** the complete query result transmitted first. |
| Offered corpus | Calibrated schedules admit at most 3.2 million foreground messages, with at most 100,000 scheduled jobs per worker and a maximum one-hour run. More duration or offered load can hit evidence limits before storage capacity. |

These limits come from the [runner](../aerostore_core/benches/contention_crucible/runner.rs),
[calibrated scheduler](../aerostore_core/benches/contention_crucible/calibrated.rs),
[service](../aerostore_core/benches/contention_crucible/service.rs),
[fixture](../aerostore_core/benches/contention_crucible/fixture.rs), and
[qualification driver](../scripts/qualify_hyperfeed.py).

The [OCC transaction](../aerostore_core/src/occ_partitioned.rs) and
[skip-list epoch guard](../aerostore_core/src/shm_skiplist.rs) share the same
[ProcArray](../aerostore_core/src/procarray.rs). For the current synchronous
worker paths, conservatively budget two registrations per active worker:
36 for 16 foreground plus two maintenance workers, or 68 for 32 plus two.
This leaves room for other activity; it is not an admission guarantee for
arbitrary additional callers. Five index collectors and a row-vacuum worker
also consume scheduling and locking resources. Current collection passes read
snapshots rather than permanently occupying a ProcArray slot. The runner also
starts a WAL writer process. These helpers belong in the database resource
budget.

The full-history [oracle](../aerostore_core/benches/contention_crucible/oracle.rs)
searches transaction orders consistent with observed intervals. More overlapping
transactions can widen that search; exhausting its two-million-step default
budget is `Inconclusive`, not success. Each reference query scans the reserved
record population in the [reference store](../aerostore_core/benches/contention_crucible/model.rs).
Increasing identities therefore also increases initialization and checking
costs; its retained undo journals also grow with the successful search prefix.
Oracle execution is outside measured throughput, but recording and
delivering full receipts occur during the run. Metrics mode omits operation
histories while still retaining per-transaction receipts, outcomes, latency
vectors and a history file. Its coordinator memory and output work grow with
the corpus. A smaller full-history run cannot act as the exact companion of a
larger metrics run; source, binary and workload configuration must match.

Existing storage counters are insufficient for an equal-memory claim. Native
[retention samples](../aerostore_core/benches/contention_crucible/aerostore.rs)
measure arena allocation high-water and retirement/reuse counters. PostgreSQL
[samples](../aerostore_core/benches/contention_crucible/postgres.rs) measure
relation sizes and approximate tuple/statistics counters. Neither measures
total resident memory. Service executors attach the same shared mapping, so
summing mapping sizes or RSS can double-count shared pages. A capacity campaign
needs a recorded process/cgroup boundary, CPU time and resident-memory accounting
for owner, workers, WAL/GC helpers and PostgreSQL children, with harness overhead
identified. The existing `--cpu-budget` is a declaration, not enforcement.

Start with a bounded central-service experiment: `service-unix` versus native
PostgreSQL over a local socket, 16 then 32 foreground workers, and one population
large enough to distribute work across them. For example, 128 identities gives
32,768 reserved slots and 96 foreground-active identities in the existing
quiet-quarter initialization. That is an explicit synthetic starting point,
not FlightAware calibration. Validate initialization, complete query frame
sizes, oracle completion and memory headroom at a low rate before exploring a
rate bracket. Preserve the direct-access engine as a diagnostic control, then
use loopback TCP to expose transport sensitivity; physical MMHF remains a
separate comparison.

Use replenished lifecycle work before calling the experiment sustained
capacity. First exercise its correctness at accelerated cadence, then observe
several actual five-to-ten-minute intervals. Require repeated useful foreground
and maintenance effects, completed terminal probes, acceptable retry-inclusive
p99, bounded backlog and retention, and exact full-history companions. Retain
every failed or inconclusive cell. Do not interpret an engine's last passing
offered rate as its maximum capacity or divide two such rates to claim 10×.

PostgreSQL needs enough usable connections for all foreground and maintenance
workers, the sampling connection, and administrative headroom after reserved
slots. The existing test-server setting of 100 connections cannot admit 100
foreground plus two maintenance workers even before coordinator and reserved
connections. Preflight and record actual `max_connections`, memory, predicate-lock,
WAL, autovacuum and timeout settings through the adapter. Keep prepared
statements, buffered writes and `SERIALIZABLE` enabled for the comparison.
Asynchronous acknowledgement plus a final drain still does not establish equal
crash durability. Resource limits and a declared latency budget must be agreed
before interpreting a saturation bracket.

A later 100-worker step must replace the ownership bit mask, review every
Rust/Python/remote admission and corpus bound, and test connection/registration
exhaustion and survivor behavior. Although 102 sessions fit the service's
present session limit, its stack reservations, temporary index registrations,
query materialization, oracle costs and PostgreSQL connection budget still need
measurement. At 200 foreground workers the current 128-session limit blocks
admission; at 300, even one simultaneous transaction per worker exceeds the
256-slot ProcArray. Queueing active transactions and changing the shared layout
are different options requiring their own evaluation. None is authorized by
simply lifting a command-line cap.
