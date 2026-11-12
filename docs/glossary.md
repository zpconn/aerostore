# Glossary

Terms used across Aerostore's code, reports and verification ledger, grouped
by topic. Where a report uses a term in a narrower sense, the report's own
definition wins.

## Engine

| Term | Meaning |
| --- | --- |
| **Arena** | A shared-memory region holding rows, index nodes and metadata. Every attached process maps the same arena. Allocation uses size classes and shared free lists. |
| **`RelPtr`** | A pointer stored as an offset from the arena base, so it stays valid in every process regardless of where the arena is mapped. |
| **memfd arena** | An arena backed by an anonymous memory file (`memfd_create`) rather than a file on tmpfs. It is volatile, and benchmark runs use it to avoid page-fault spikes. |
| **Warm attach / cold boot** | Warm attach reuses a valid existing mapping and skips snapshot load and WAL replay. Cold boot rebuilds a fresh mapping from durable inputs. A layout-version mismatch forces a cold boot. |
| **OCC** | Optimistic concurrency control. Transactions run without holding locks, then validate their reads and writes at commit and retry on conflict. `OccTable` (`occ_partitioned.rs`) is the active implementation. |
| **MVCC / row version** | Each row keeps a chain of versions, so readers see the snapshot they started with while writers publish newer versions. |
| **ProcArray** | A fixed 256-slot shared table of active transactions. Snapshots and the vacuum horizon are computed from it. The name follows PostgreSQL. |
| **Horizon (global xmin)** | The oldest transaction ID any active snapshot still needs. Vacuum may reclaim only versions that every active snapshot has moved past. |
| **Posting** | One entry in a secondary index mapping a key to a row ID. A key with several rows has several postings. |
| **Bound index** | A secondary index registered with `OccTable::bind_index`. Commit then maintains it atomically with the rows, and it accepts no raw mutations. |
| **Predicate bucket / stamp** | Each bound index has 4,096 hash buckets. A bucket holds a short-lived publication lock and a stamp recording the last commit that changed it. A lookup records the stamps it read, even for an empty result. Commit revalidates them. This is how phantoms are detected. |
| **Hashed / ordered publication** | Two policies for mapping index keys to buckets. *Hashed* is the default. *Ordered* maps time-like keys to ordered ranges, so updates far in the future do not conflict with near-term range queries. It is optional and experimental. |
| **Savepoint** | A named point inside a transaction that it can roll back to without aborting. Predicate dependencies survive the rollback, conservatively. |
| **Poison** | A table state entered after an unexpected failure partway through publication. Further transactional access fails until recovery, so a half-maintained index is never served as healthy. |
| **Vacuum / index GC** | Background reclamation. Vacuum unlinks row versions older than the horizon. Index GC retires deleted postings and nodes. Both return storage to shared free lists. |
| **WAL stream** | The single write-ahead log a table is bound to: a synchronous file or an asynchronous shared ring. Changing streams on a live table is rejected. |
| **Delta record** | A WAL update record carrying only the changed columns, as a dirty-column mask plus bytes, instead of the full row. |
| **Synchronous / asynchronous commit** | Synchronous commit acknowledges after `fdatasync`. Asynchronous commit acknowledges once the record reaches the shared ring; a background writer persists it later. |
| **Checkpoint cut** | The point a checkpoint captures. It records both an ID cutoff and the transactions active at capture, so a transaction that began early but committed late is still replayed. |
| **Direct mode** | Application processes map the arena and call the engine directly. |
| **Database-owned service** | An experimental mode in which one owner process executes all storage operations, and clients send begin/read/query/write/commit requests over a Unix socket (`service-unix`) or TCP (`service-tcp`). It isolates client failures from engine state. It currently lives in the benchmark harness. |
| **STAPI / Speedtables** | The Tcl table-query interface Aerostore's query parser accepts, from FlightAware's open-source Speedtables project. A rule-based planner compiles STAPI queries to index or scan routes. |

## Workloads and benchmarks

| Term | Meaning |
| --- | --- |
| **HyperFeed** | FlightAware's parallel flight-tracking engine, described publicly in a [2016 Tcl conference paper](https://www.tcl-lang.org/community/tcl2016/assets/talk37/hyperfeed-paper.pdf). Aerostore's workloads are synthetic models of its storage behavior; no HyperFeed code is included. |
| **MMHF** | Multi-machine HyperFeed: workers on several machines sharing one central database host. |
| **Crucible** | The original storage-churn benchmark (`hyperfeed_crucible`). Sixteen workers run keyed upserts and indexed range probes against Aerostore and PostgreSQL, with exact table/index agreement checks. |
| **Extended Crucible** | Complete message transactions replayed against Aerostore, PostgreSQL and a serial reference model, including native concurrency-contract probes. |
| **Contention Crucible** | Transactions that discover their writes through queries and compete for the same flights. A serial-witness oracle checks the complete history. The headline service benchmark runs on it. |
| **Workload profiles** | `legacy`, `lifecycle`, `fleet`, `calibrated` and the optional rolling population are increasingly realistic scenarios. `calibrated` adds per-flight ordering and maintenance timers. |
| **Identity / family / fork** | An *identity* is one logical flight. Its *family* is the set of rows representing it. A *fork* is one provenance view within the family, and a message updates every fork eligible for it. Some reports also use "family" for the physical slot group backing an identity. |
| **Projection / housekeeping** | The two scheduled maintenance jobs. Projection advances scheduled events; housekeeping expires old data. Each runs as a sweep of batch transactions while foreground messages continue. |
| **Signature affinity** | Round-robin dispatch that temporarily routes messages sharing a `(callsign, registration)` signature to the same worker. Because the affinity expires, one flight can still reach several workers. |
| **Qualification harness** | `scripts/qualify_hyperfeed.py`. It runs engines across rates, workers and seeds, with explicit gates for correctness, useful work and latency. |

## Measurement

| Term | Meaning |
| --- | --- |
| **Incoming message** | One foreground input, processed as one transaction (with retries). Throughput counts each incoming message once. Fork updates, retries and maintenance jobs are not counted. |
| **Offered rate** | Messages per second admitted on a fixed schedule, independent of how fast workers complete them. Backlog therefore shows up as latency instead of disappearing. |
| **Tested endpoint / lower bound** | The highest offered rate that passed repeatedly. It is a lower bound on capacity, not a measured maximum. |
| **p99 including retries** | End-to-end latency from admission to completion, including queueing and every retry. |
| **Full-history run / metrics run** | A full-history run records every transaction so the oracle can check serializability; the instrumentation slows it. A metrics run is uninstrumented and supplies the capacity numbers. |
| **Correctness companion** | The full-history run paired with a capacity result: same executable, configuration and offered rate. |
| **Serial witness** | A serial order of the committed transactions that reproduces every observation, effect and final state. Finding one shows that the recorded history is serializable. |
| **Paired screen** | A short alternating baseline/candidate comparison (B/C, C/B across two seeds) used to decide whether a change deserves a sustained trial. |
| **Sustained qualification** | Repeated 905-second trials with normal 300-second maintenance cadence, plus a correctness companion. Only these promote a capacity result. |
| **Inconclusive** | The comparison's noise exceeded its declared limit. It is neither a pass nor a fail, and it does not promote a change. |

## Verification and process

| Term | Meaning |
| --- | --- |
| **P0–P6** | Phases of the [verification plan](formal_verification_plan.md): P0 contracts, P1 complete-slice pilot, P2 transaction protocol, P3 storage implementation, P4 durability, P5 query composition, P6 resources and release. P0 is complete and P1 is in progress. |
| **Claim ledger** | [`verification/claims.toml`](../verification/claims.toml). Each claim is marked partial, in progress or open, with its scope and required checks. |
| **Assumption** | An explicitly trusted premise in [`verification/assumptions.toml`](../verification/assumptions.toml), such as compiler behavior, allocator behavior or the translators. |
| **Proof-input lock / frozen boundary** | [`verification/frozen_boundary.json`](../verification/frozen_boundary.json) hashes every input the proofs depend on. A stale lock fails CI. |
| **Anchored comparison** | A boundary check whose checker comes from an independent base commit, so a candidate cannot change the code and the checker together. |
| **Pilot / full profile** | `pilot` runs every implemented component check. `full` deliberately fails until whole-engine obligations are met. |
| **Negative control / mutant** | A deliberately broken variant that a check must reject. It shows the check can actually fail. |
| **Judge** | Any test, specification, oracle, threshold or negative control that decides whether a change is acceptable. Judges are never weakened to make a change pass. |
| **Gate / promotion** | A gate combines judges into one verdict. Promotion means a candidate passed its gates and became the new baseline. |
| **Campaign / receipt** | A campaign is a bounded set of runs with declared inputs and budgets. A receipt binds a result to the exact source, executable, configuration and logs that produced it. |
