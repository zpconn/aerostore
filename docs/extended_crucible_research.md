# Research basis for Extended Crucible

Research date: 2026-09-22. Scope: synthetic, single-machine HyperFeed workers sharing one database. Remote workers and the MMHF transport are outside this workload.

This document separates public descriptions of HyperFeed from an independently designed simulator. It is a research and coverage specification, not a claim to reproduce FlightAware's proprietary matching logic, production schema, traffic distribution, or current implementation. The executable's runbook and output determine which proposed cases have actually been implemented and exercised.

## Primary sources and what they establish

### 1. Transaction structure and concurrent actors

Zach Conn, **Hyperfeed: FlightAware's parallel flight tracking engine**, 23rd Annual Tcl/Tk Conference, paper dated 2016-10-25. [Paper](https://www.tcl-lang.org/community/tcl2016/assets/talk37/hyperfeed-paper.pdf).

Sections 4.2–4.6 describe candidate queries followed by application decisions inside an open transaction, whole-message retries, buffered output, and savepoints around fork creation. Other database clients perform projection and housekeeping. A database-backed virtual schedule permits a different worker to cancel or reschedule an event. The dispatcher uses round-robin assignment with temporary signature affinity rather than permanent flight ownership. Section 5 describes STAPI translation of existing Tcl queries to SQL. Reported PostgreSQL tuning kept `fsync` enabled, disabled synchronous commit, and used a ten-second WAL writer delay.

These are historical architectural facts. They motivate the transaction boundaries and concurrency cases below; they do not establish current source code, isolation settings for every handler, or the exact schema.

### 2. Earlier storage evolution and output consumers

Karl Lehenbauer, **Tcl at FlightAware**, 25th Annual Tcl/Tk Conference, 2018-10-15–19. [Paper](https://www.tcl-lang.org/community/tcl2018/assets/talk150/Paper.pdf), [conference attribution and dates](https://www.tcl-lang.org/community/tcl2018/abstracts.html).

The historical account explains why tracking required persistent state: identifiers were reused, messages needed association with flightplans, and missing events needed interpretation. The system retained original feed data before normalization, then combined feeds for interpretation. Shared PostgreSQL storage supported multiple child processes after partitioning approaches proved unattractive. Controlstream fed several downstream applications. This supports replayable inputs and complete output checks as useful benchmark artifacts. It does not make downstream API queries part of the database workload being replaced.

### 3. Flightplan families and information provenance

Garrett McGrath, **Flight Tracking Access Control**, 2022-12-05. [Article](https://flightaware.engineering/flight-tracking-access-control/).

An input's provenance denotes its distribution class; a flightplan's pedigree records contributing provenances. A family includes a parent encompassing all contributing classes and children representing subsets. The general construction grows family membership as `N → 2N + 1`; three distinct classes can produce seven members. A message updates only members whose pedigree contains its provenance. The article explicitly omits optimizations and special handling for the most permissive class. Customer display permissions are evaluated downstream; blocklist enforcement is separate.

For the simulator, provenance is therefore a cause of write amplification and divergent stored state, not merely a label copied into every row. A three-bit generic model is a deliberate simplification, not FlightAware's real access-control policy.

### 4. Existing simulation and regression practice

Yuki Saito, **Redeye: Cloud Regression Tests for HyperFeed**, 2021-08-09. [Article](https://flightaware.engineering/cloudregressiontestsforhyperfeed/).

The original single-machine regression runner accepted YAML flight descriptions and SQL assertions, generated restricted synthetic feeds, ran HyperFeed in simulation mode, and checked resulting PostgreSQL state. Tests used isolated schemas. The later cloud service retained that testing workflow while changing execution infrastructure.

This is direct evidence that synthetic flight scenarios are a legitimate way to exercise HyperFeed behavior. Extended Crucible follows that testing idea with newly authored scenarios and an independent expected-state model; it does not have access to those original fixtures or the real simulator.

### 5. Time semantics and catch-up

Garrett McGrath, **Unifying Air and Surface Coverage**, 2022-08-01. [Article](https://flightaware.engineering/engineering-blog-unified-feed/).

The article distinguishes timestamp ordering of feed ingestion from correctness of message content. Its feed-combining discussion requires consistent behavior during live processing and historical catch-up, and explains how producers running at different speeds can defeat a naive shared priority queue. Surface-to-flight association is itself nontrivial.

This concerns a related feed service, not HyperFeed's internal transaction implementation. It motivates separate arrival and observation timestamps, deterministic replay, and speed-independent virtual-time tests. It does not justify recreating that service inside this benchmark.

### 6. Aviation counterexamples that prevent an unrealistically easy schema

Ben Burwell, **Falsehoods Programmers Believe About Aviation**, 2025-06-02. [Article](https://flightaware.engineering/falsehoods-programmers-believe-about-aviation/).

Examples invalidate assumptions that a callsign uniquely identifies a flight, schedules always exist, delays are short, identifiers never change, reported departures or cancellations are conclusive, and ADS-B positions or identifiers are always accurate. The literal text `NULL` can occur as an identification value.

The simulator should consequently distinguish database identity from reported identity, absent values from literal strings, and arrival order from observation time. Its rejection and matching thresholds remain synthetic choices. A simplistic “one row per callsign, overwrite on every message” model would sidestep precisely these pressures.

### 7. Why output correctness matters

Jonathan Cone, **Firehose++ — The Evolution of a High-Performance Streaming API**, 2021-03-30. [Article](https://flightaware.engineering/firehose-evolution-of-a-high-performance-streaming-api/).

The article describes normalized source feeds entering HyperFeed and controlstream carrying its evaluated results into Firehose. It identifies controlstream files as a retained canonical record. This makes output multiplicity and content meaningful acceptance criteria alongside database rows. The simulator's output journal is an observation mechanism; it is not an assertion that HyperFeed uses a transactional outbox or guarantees exactly-once delivery across crashes.

### 8. Present-day scope and language evolution

Ben Burwell, **Building a Bridge from Tcl to Rust**, 2024-10-07. [Article](https://flightaware.engineering/building-a-bridge-from-tcl-to-rust/).

FlightAware describes incremental Rust integration into HyperFeed while preserving its accumulated tracking logic. This reinforces the distinction between testing a storage replacement and rewriting the domain engine. A Rust benchmark can simulate the storage contract, but passing it does not demonstrate Tcl/STAPI compatibility or production algorithm equivalence.

FlightAware, **Data Sources**, undated living page, consulted 2026-09-22. [Page](https://www.flightaware.com/about/datasources/).

The published categories include ANSP, datalink, terrestrial ADS-B, space-based ADS-B, and airline FLIFO. Airline inputs include out/off/on/in events, plans, times, and cancellations. These justify heterogeneous synthetic sources; marketing aggregate rates do not establish transaction mix, contention, or per-flight event distributions.

### 9. Public API and isolation references

FlightAware's [Speedtables repository](https://github.com/flightaware/speedtables) separates generated tables, a server interface, and STAPI's interchangeable-backend abstraction. This is useful future integration material; Extended Crucible does not need to implement that whole API to measure transaction behavior.

The [PostgreSQL 16 isolation documentation](https://www.postgresql.org/docs/16/transaction-iso.html) distinguishes statement snapshots under read committed from serializable execution, which can require retrying an entire transaction. Its [savepoint reference](https://www.postgresql.org/docs/16/sql-savepoint.html) describes retaining earlier work when rolling back later work. The simulator must state the isolation contract it tests. The conference paper's use of “linearizable/serializable” should not be converted into an unqualified claim that PostgreSQL's serializable mode proves all external real-time or output-delivery properties.

## Simulator design: explicit assumptions

The rest of this document proposes our own bounded workload. All field layouts, identifiers, thresholds, scoring rules, distributions, limits, and concurrency schedules below are design choices. They are not recovered production details.

### Boundary of the initial executable

The [initial model](../aerostore_core/benches/extended_crucible/model.rs) uses 128 preallocated physical slots per family, tagged logical records, three generic provenance bits, bounded position/output/deduplication history, integer identifier surrogates, and phased deterministic replay. A phase has at most one distinct message per family; one phase sends four identical deliveries to create contention. Different families can execute concurrently; a family's distinct messages retain their prescribed phase order.

The fixture supplies its possible write slots before a transaction begins so both database adapters can coordinate potential writes at transaction start. Aerostore takes row guards before its snapshot; PostgreSQL uses a locking query inside a serializable transaction. Candidate selection still uses queried identifiers, time, and route evidence. An allocation hint may choose empty storage for a new family; it may not rescue a missing candidate result for an existing family. This deliberately bounded construction does **not** establish arbitrary matching predicates or native serializable execution.

The initial workload includes multirow pedigree updates, savepoint failure, whole-message abort, duplicate/stale/invalid inputs, concurrent duplicate contention, projection, schedule replacement and cancellation, expiry of retained history, and atomic terminal-family expiry followed by storage reuse in the next cycle. Its `parent` field records a synthetic clone ancestor, not HyperFeed's all-provenance parent convention; candidate queries examine all views. It does not yet implement competition among event generations, real string/null semantics, fuzzy corroboration of status messages, or unrestricted simultaneous distinct messages for one family. The broader coverage table below remains a design map, not a completed-feature checklist.

The [native contract probes](../aerostore_core/benches/extended_crucible/contracts.rs) exercise storage properties independently of the fixture's declared write sets. Replay agreement and contract results must be reported separately; agreement under additional coordination cannot turn a failed native contract into a compatibility pass.

### Model and identity

Use stable surrogate row identifiers and a separate family identity. Give input messages callsign, optional registration, optional route/schedule hints, provenance, receive time, observation time, and a reproducible message ID. A generator can retain an intended family ID for its oracle, but the backend's matcher must not read that hidden answer.

Candidate searches should return rows and require application evaluation. Suggested search predicates combine reported identity, a schedule interval, route hints, and a root/child condition. Deterministic scoring and an explicit tie rule make tests reproducible. Include repeated callsigns whose routes or time windows differ. Nullable fields need an explicit representation; absent callsign and the literal `NULL` must remain distinct.

Suggested logical records:

| Record | Fields that drive the workload | Useful lookup shape |
| --- | --- | --- |
| Flight family/root | Stable ID, identifiers, route, scheduled interval, revision, lifecycle | Identifier plus time interval; active root filtering |
| Flightplan member | Family ID, pedigree, status, latest accepted real observation, projected state | Family members; pedigree uniqueness |
| Position | Family/member association, source, observation time, value, accepted/projected marker | Flight history ordered by observation time |
| Deferred event | Stable event ID, family ID, due time, generation, state | Due-time range; cancel/reschedule by event ID |
| Message receipt | Message ID, terminal disposition, application revision | Idempotency lookup |
| Output record | Message ID, member ID, ordinal, event type, resulting state | Exact committed output comparison |

These are logical distinctions. Physical implementation may use several tables or typed records in one table, provided transactions, candidate queries, and audits still exercise the storage engine. Packing an entire family into an opaque value would conceal multirow behavior and should be reported as reduced coverage.

### Transaction families and source-to-scenario mapping

The source column identifies motivation. The proposed action and assertion are our simulator contract.

| Scenario | Motivation | Proposed transaction and decisive assertion |
| --- | --- | --- |
| Match or create | Sources 1, 2, 6 | Query candidates, score returned values, create when none qualify. Simultaneous equivalent first messages yield one matching family after retries; unrelated repeated callsigns remain separate. |
| Late plan reconciliation | Sources 2, 6 | Apply a plan to a provisional flight using several candidate reads; revise route/schedule indexes while preserving stable identity and attached history. |
| Provenance expansion | Source 3 | Add missing pedigrees and update eligible members in one transaction. Audit member uniqueness, complete intended fan-out, and restricted-data separation. |
| Failed optional fork | Sources 1, 3 | Stage some child writes and outputs after a savepoint, inject failure, roll back that branch, then finish permitted outer work. No partial children or branch output survive. |
| Position acceptance | Sources 6, 9 | Query flight and recent observations, apply an explicit synthetic plausibility rule, append history, update eligible state, and generate output. Older or rejected observations cannot overwrite the selected latest real state. |
| Status correction | Sources 6, 8 | Use multiple pieces of synthetic evidence to accept or defer departure, arrival, cancellation, or diversion. A later correction obeys the specified state rule rather than arrival-order overwriting. |
| Deferred work | Sources 1, 5 | Schedule, replace, cancel, and claim events transactionally. Only the current generation may apply effects; a stale claimant must fail or observe cancellation. |
| Projection | Sources 1, 5 | Range-query eligible records, reread evidence, and write an explicitly estimated state. A racing accepted real position must not be replaced by a projection based on older evidence. |
| Housekeeping | Sources 1, 7 | Select expired families and remove dependent records/index entries. Late accepted activity must be ordered consistently with deletion; subsequent scans expose no orphaned members. |
| Whole-message retry | Sources 1, 9 | Force a conflict after reads and staged changes. Repeat the complete handler; successful output appears once and failed-attempt output never appears. |
| Input replay | Sources 4, 5, 7 | Replay the same generated inputs with changed execution speed. Compare semantic state and committed output using defined ordering rules. |

### Concrete invariants

1. **Message atomicity:** a successful transaction publishes all intended row changes; an aborted attempt publishes none. Reads through indexes must agree with visible table records.
2. **Predicate correctness:** an empty candidate result is still a dependency. A concurrent qualifying insertion must be ordered or detected; validating only rows already returned is insufficient.
3. **Family uniqueness:** one member exists per `(family, pedigree)`. Root identity is stable, and every live member references a live family.
4. **Provenance isolation:** each member's derived state depends only on input provenances admitted by that member. A fork created later cannot inherit forbidden history from an all-source aggregate.
5. **Savepoint rollback:** later row changes, temporary allocations, scheduled work, and buffered outputs are undone together; earlier intended work remains available.
6. **Time correctness:** synthetic rules distinguish observation age, arrival order, and virtual deadlines. Real and projected positions remain distinguishable.
7. **Event ownership:** a due event generation has at most one committed application. Cancellation or replacement invalidates a previously read generation.
8. **Output discipline:** outputs are checked by semantic key and content; retries do not multiply them. Crash delivery guarantees require separate tests and are not implied by buffering.
9. **Reclamation:** deleted or replaced records stop appearing in indexes, then become reclaimable after readers release snapshots. Live, retired, and reusable ownership remains accounted for.
10. **Progress:** every submitted operation has a reported disposition; retries are bounded; worker failures cannot turn into a successful aggregate report.

### Determinism without hiding concurrency bugs

Use a seeded generator, stable integer units, a fixed initial virtual epoch, and logical clocks. Keep wall-clock pacing separate from the input stream. Preserve enough trace information to reproduce any failure: seed, inputs, transaction/worker IDs, stage, attempts, selected candidates, disposition, and output keys.

A reference model should be much simpler than either storage backend, using ordinary ordered maps and explicit state transitions. Fixed small scenarios also need independently written expected results: two adapters calling the same erroneous domain function can agree with each other.

Sequential replay can compare exact state and output. Concurrent execution generally admits several correct serial orders. Either design rounds whose intended final results commute, replay a validated commit order, or check histories against allowed outcomes. Sorting output does not make conflicting order-dependent updates equivalent. Recording commit order is also insufficient if decisions were based on inconsistent earlier reads; targeted interleaving tests must check those dependencies.

Do not achieve correctness by placing an undisclosed harness mutex around every complete transaction. If a backend adapter requires a coarse lock or guard row to provide the requested semantics, name it, measure its contention, and report that configuration. Such a result tests the adapter plus engine and cannot establish native predicate isolation.

### Workload dimensions and reporting

Keep the original Crucible churn workload as a separate regression. Add short deterministic correctness scenarios before a longer mixed run. The extended workload should sweep family count, candidates per query, history length, provenance count, worker count, hot-family skew, delayed observations, event reschedules, fork-failure injection, and lifecycle turnover. Sweep these independently where practical; a single blended throughput figure can hide an expensive transaction family.

Report committed **messages** per second and complete-message latency, including retries. Also report attempt counts, abort categories, savepoint rollbacks, rows read/written, candidate counts, outputs, index mutations, reclaimed bytes, and time-series memory/throughput. A fork message changing many members must not be counted as many successfully processed input messages.

Aerostore and PostgreSQL should receive identical fixtures and perform equivalent logical reads, decisions, writes, rollback, and output checks. Keep result materialization equivalent. Publish connection mode and durability settings. An in-memory Aerostore run and a WAL-enabled PostgreSQL run are different configurations even if both complete the same logical test; neither is an equal-durability speed claim.

Useful larger cases include hot-flight bursts mixed with independent traffic, overlapping identity components, three provenance classes, long-lived readers during housekeeping, and bounded repeated flight lifecycles. Sustained memory measurements need a steady-state retention policy so legitimate accumulated history is not mistaken for a leak.

## Boundaries and remaining unknowns

Public sources do not reveal production tables and indexes, query frequencies, current isolation selection, full matching rules, all fork exceptions, transaction lengths, retry budgets, or present retention policies. No synthetic percentage should be presented as measured HyperFeed traffic. A passing scenario establishes the implemented synthetic contract and nothing stronger.

Full geographic reasoning, real source parsers, ML inference, all flight lifecycle exceptions, downstream access control, and Tcl integration are separate work. So are abrupt worker death, durable restart, and output delivery after a crash unless the executable specifically implements and tests those paths. Remote transport is deliberately outside the requested single-machine scope.

The useful outcome is an auditable database workload that makes realistic transaction dependencies explicit, exposes unsupported semantics rather than silently weakening them, and can be refined with the architect's knowledge while the proprietary codebase is unavailable.
