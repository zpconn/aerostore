# Finite TLA+ protocol pilot

This directory contains executable **abstract models**, not a proof of Aerostore's Rust implementation. The production-to-model refinement, weak-memory semantics, complete transaction-history theorem, and safe allocator/reclamation implementations remain open obligations in the [verification plan](../../docs/formal_verification_plan.md).

Run the declared campaign from the repository root:

```sh
python3 scripts/check_tla.py --download
```

`--download` only downloads a missing jar. The URL, reported version and SHA-256 are pinned in [toolchain.json](toolchain.json); any checksum mismatch fails. Java 21 was used. Once installed, omit `--download` for an offline run. Output defaults to `target/verification/tla/report.json` with each exact generated configuration and textual trace alongside it. `--list` lists cases; repeat `--case NAME` for a deliberately partial campaign. The report distinguishes partial selection from the complete declared campaign.

TLC runs in a fresh temporary working directory containing only checksum-matched copies of the declared model files and generated configurations. Standard modules come from the pinned jar; repository-root `.tla`/`.class` overrides are outside the search path. The runner rejects nonempty `TLC_LIBRARY`, `TLA_LIBRARY`, `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, `_JAVA_OPTIONS`, and `CLASSPATH` overrides before invoking Java. This closes a module-resolution/configuration gap; the Java runtime and pinned TLC implementation remain trusted.

`python3 scripts/test_tla_runner.py` exercises fail-closed evidence classification without Java or network access. It checks complete-search markers and an exactly empty queue, the pinned TLC invariant/temporal exit codes (12/13), named properties, real trace/lasso markers, timeout/signal handling, unrelated failures, and environment override rejection that clears stale success reports. It also reclassifies every retained campaign log. These are tests of the evidence runner, not proofs of TLC.

The [retained campaign report](evidence/report.json) records all 74 cases: twenty-nine completed finite searches (four also check conditional liveness), twenty-three intended safety counterexamples, three intended liveness counterexamples, and nineteen positive reachability witnesses. Safety counterexamples and witnesses stop when the named invariant fails; they are **not completed safety searches**. Liveness failures retain a cyclic or stuttering behavior violating the exact named temporal property. Individual logs retain the actual state traces. The runner rejects syntax errors, other properties, unexpected errors, timeouts, and stale tool/source identities. It also clears a stale success report before starting. Each case has a 120-second limit, 512 MiB Java heap, one worker, fixed fingerprint index and random seed. There are no depth/state constraints or symmetry reductions. TLC's finite-state fingerprinting remains part of the model-checking evidence, not a deductive proof.

## Publication and predicate model

[Publication.tla](Publication.tla) explores two one-shot transactions, two rows, two nonzero keys, one index, and either one or two hash buckets. The empty-creation scenario has two writers; the key-move scenario forces the writer to receive the older start identifier; the disjoint scenario permits independent successful work. The one-bucket configuration exercises collisions.

Separate actions cover:

1. Atomic identifier reservation/registration, then a separate lifecycle snapshot.
2. Lookup locking, stamp validation/dependency recording, candidate capture, guard release, and later row materialization.
3. Canonical bucket acquisition, predicate revalidation and write-base checking.
4. Destination preparation, source removal, physical row-head publication, deregistration, fresh publication stamps and unlock.

Visible rows are derived from physical heads plus the registered writer's old version. `logicalRows` independently records the intended committed state; equality with the derived visible rows is checked. Snapshot storage preserves the captured logical values throughout this finite execution. This abstracts actual MVCC traversal and assumes safe retention; it does not prove ProcArray locking, vacuum, pointers, or byte ownership. Each modeled writer changes at most one row; there are no allocation failures, savepoints, repeated transactions, ranges/In predicates, multi-index failures, owner death, or machine-counter wraparound in this initial model. Stamping all changed buckets is one model action while their guards remain held. The model checks equality predicates, complete-or-reject results, absent-creation exclusion, visible-state agreement, and final postings. It does not establish general serializability or liveness.

Positive witnesses demonstrate two disjoint commits, a rejected conflicting creator after both empty searches, an older snapshot's explicit rejection, and successful materialization **after** a key move using candidate IDs captured before that move. Witness checks run the safety invariants too, so an unrelated safety failure cannot count as a witness.

Four deliberate mutations must fail `Safety`:

- `OmitEmpty`: discard dependencies for an empty predicate.
- `SkipValidation`: skip predicate validation at commit.
- `BeginStamp`: stamp with the writer's original identifier instead of a fresh publication identifier.
- `EarlyUnlock`: release publication guards after physical row publication but before deregistration/stamping.

The finite state spaces arise from two transactions that each execute at most once. Their completion is not evidence about an unbounded number of transactions, arbitrary capacity, or long-running memory growth. Additional models and inductive proofs are required for those claims.

## Repeated predicates and local writes

[PredicatePublication.tla](PredicatePublication.tla) adds a separate data-bearing
slice with one older writer, one reader that performs two equality lookups, two
rows and one or two publication buckets. Six scenarios cover an empty predicate,
a key move, and staged own inserts, moves into/out of the predicate, and deletion.
The own-write overlay is used during materialization; these local changes are not
published by the reader in this model.

Candidate capture records its dependency even for an empty result. Each repeated
capture rechecks stamp equality and the snapshot boundary. Candidate IDs survive
guard release and are materialized against the pinned snapshot plus local writes.
Commit acquisition, validation and completion are separate actions, so the model
checks the guard lifetime after validation. The writer prepares the destination,
removes the source, publishes the row, deregisters, reserves a fresh stamp and
unlocks in distinct actions. Reader start remains enabled between deregistration
and stamping.

The added 25 cases comprise twelve complete safety searches, eight intended
`PredicateSafety` counterexamples, and five reachability witnesses. The invariant
checks exact returned row IDs, excludes successful stale predicate commits by
comparing snapshot membership to logical membership at commit, checks visible-row
agreement, and checks terminal postings. Counterexamples remove empty dependencies,
repeat validation, own-write candidates, old-key bucket coverage, fresh publication
time, or required guard lifetime. The own-candidate mutation is exercised for both
insertion and a move into the predicate. Witnesses require an own insert to be
returned, a successful repeated read, a rejected repeated read, a start between
deregistration and stamping, and materialization of captured IDs after a key move.
Witnesses also check `PredicateSafety`; an unrelated failure cannot count.

This is still a finite abstract protocol model. Snapshot reservation/registration,
exact raw candidate capture, pinned MVCC, atomic all-bucket writer acquisition and
per-bucket exclusion are primitive assumptions. It omits failed allocations,
rollback, row conflicts, repeated writer commits, broad predicates, multiple
indexes, weak memory, process failure and counter exhaustion. It supplies no
Rust refinement, general serializability or liveness theorem. The existing
`Publication.tla` remains the complementary conflicting-creator model with two
writing transactions and separate canonical bucket acquisitions.

## Durability design exploration

[Durability.tla](Durability.tla) separately explores two transactions where T2 observes T1, plus an optional checkpoint. It separates visible publication, durable log append, acknowledgement, snapshot, cutoff sampling, durable checkpoint publication, whole-log truncation and crash/recovery.

The `Dependent` unsafe variant permits publication before the WAL append. It produces a dependency-closure counterexample. A separate `AcknowledgedSafety` check produces a trace where T2 has **received success** but recovery lacks T1. The proposed safe variant makes the append durable before exposing the write.

The `Checkpoint` unsafe variant already uses write-ahead publication, isolating the checkpoint defect: it captures rows without global quiescence, later samples allocated start identifiers as the replay cutoff, persists the checkpoint and truncates the log. It can lose an acknowledged transaction. The proposed safe variant drains all started transactions and blocks new ones throughout checkpoint capture/cut/persistence/truncation. Both safe designs complete their finite safety searches, and witnesses require nonempty recovery, recovery of a dependent acknowledgement, and successful recovery after a nonempty checkpoint.

These historical traces remain **model counterexamples to candidate ordering rules**. Corresponding failure schedules were subsequently reproduced in the native Rust [WAL protocol regressions](../../aerostore_core/tests/wal_protocol_regressions.rs), which are separate implementation evidence. The old safe checkpoint configuration drains all transactions; that remains a **model-only design alternative**. The implemented repair instead uses the active-cut protocol below. Atomic durable append and durable checkpoint publication are trusted model primitives; torn records, separate file/directory persistence, process failure handling, ring/delta encoding and OS storage guarantees are omitted. No inference of production durability is justified from the safe configurations passing. Their purpose is to expose concrete design obligations before implementation verification assumes a persistence contract.

## Checkpoint exclusion with continuing transaction starts

[CheckpointCut.tla](CheckpointCut.tla) models the implemented alternative without requiring a transaction drain. Two one-shot writers use separate partitions; the checkpoint acquires both partitions in separate actions. Transaction starts remain enabled throughout acquisition, row capture, active-set/cut sampling, persistence, truncation, and release. Writers append durably, publish their row, and deregister while holding their partition. The checkpoint retains both partitions through durable image publication and whole-log truncation.

Rows and the `(allocation cut, active IDs)` are captured in separate steps while publication is excluded. Recovery includes a log record if its start ID exceeds the durable cut **or was active at that cut**. This preserves a transaction that began before checkpoint capture but can commit only after checkpoint exclusion ends. The safe finite search finishes with 4,630 distinct states and an empty queue; a crash is enabled after every protocol step.

Three mutations must fail the named `CheckpointSafety` acknowledgement-preservation invariant:

- `OmitActive` drops active IDs from the durable image/replay rule and loses an old-starting, later-committing transaction.
- `ReleaseAfterSnapshot` drops partition exclusion immediately after row capture, allowing a later acknowledgement to be absent from the persisted image and removed by truncation.
- `TruncateBeforePersist` discards the WAL before the replacement image is durable, permitting a crash to lose an acknowledged write.

Two positive witnesses demonstrate recovery of an old-starting transaction absent from the image and a transaction that starts while checkpointing is in progress. Witness traces also check `CheckpointSafety`. These are finite safety/feasibility results, with no fairness or progress claim.

Each writer has one distinct row and commits at most once. The model does not cover conflicting updates to one row, replay ordering of multiple deltas, multiple checkpoints, torn records, shared WAL-ring lifecycle/identity, failing codec callbacks, or OS persistence primitives. Identifier allocation plus active registration/sampling and durable append/image publication are trusted model operations. The source correspondence and these primitive contracts remain unproved. The native tests supply separate evidence for repaired schedules; they do not turn this model into a Rust refinement theorem.

## Reclamation and continuous-load progress

These separate resource pilots use **finite cyclic graphs** rather than a transaction-count cap. They check infinite behaviors of those finite scenario abstractions under the fairness assumptions below. There is no proved abstraction from unbounded Rust timestamps/allocations into these graphs, and the pilots have not been compositionally connected to `Publication.tla` or the production code. They do not prove a quantitative heap bound or a wall-clock latency guarantee.

[Reclamation.tla](Reclamation.tla) contains one retired version, one older writer, two reusable reader slots, and an optional surviving row guard. Reader 1 snapshots while the writer is active, so it actually needs the old version. After the writer commits, reader 2 begins before reader 1 ends. The slots then alternate forever, always leaving at least one reader active. True need and recorded horizon are separate state components.

- `ActiveOnly` wrongly reclaims after the writer ends, despite reader 1's captured need; `Safety` must fail.
- `IgnoreGuard` reclaims while the old row guard survives; `Safety` must fail.
- `InheritedHorizon` copies the prior reader's obsolete horizon into each new reader. Actual need ends, but the old horizon circulates forever. `ReclaimedEventually` must fail with a cycle.
- The correct variants retain the version until actual need and the guard end, then satisfy `ReclaimedEventually`, even under continuously overlapping readers. A positive witness reaches free storage while readers remain active.

Its specification assumes weak fairness for foreground reader steps, dropping an eligible guard, and collection when eligible. This expresses finite reader/guard lifetimes and eventual collector service. It **does not assume that collection becomes eligible**: the inherited-horizon mutation prevents eligibility forever and still respects those fairness assumptions. The Boolean old/new epoch distinction describes this particular schedule; it is not arbitrary integer wrapping or a general timestamp-renaming theorem.

[CollectorPriority.tla](CollectorPriority.tla) models one repeatedly active foreground actor, one requesting collector, and an abstract CAS mutex. Priority observation, foreground acquisition, priority recheck, foreground release, collector acquisition and completion are separate actions. There is weak fairness for each actor step, including the collector's CAS; **no strong fairness** is granted to winning an intermittently available mutex.

`NoPriority` permits an infinite fair foreground reacquisition cycle with a waiting collector. `CollectorEventuallyRuns` must fail. In the correct design, priority blocks new admissions, the finitely many already started operations drain, and collector acquisition becomes continuously enabled. `NoRecheck` separately violates `AdmissionSafety` by admitting work after a cached precheck races collector registration. That mutation is tested as an admission-policy failure, not incorrectly claimed to imply starvation by itself. Atomic mutex exclusion and finite work within each critical section are assumptions; one foreground actor is enough to exhibit starvation but does not prove arbitrary-worker progress.

[LiveKeyPosting.tla](LiveKeyPosting.tla) contains a permanent posting 0 keeping a key alive and two reusable posting blocks. It checks disjoint exhaustive ownership (`reachable`, `retired`, `free`) and `RetiredEventuallyReusable`. Its collector reclaims a full finite batch, with weak fairness only for eligible collection. `OnlyEmptyKeys` disables posting collection while the permanent key remains live; a counterexample reaches exhausted storage and retries forever. The correct model reclaims deleted postings under the live key. Foreground allocation/removal is not required to continue for this conditional property: **every posting that is retired** must eventually become free. A positive witness ensures retirement/reclamation actually occurs. The atomic batch is an abstraction; the model does not verify real spill/tower ownership, traversal, collector admission or size-class fragmentation.

## Primary-key insertion and the searched head

[PrimaryKeyInsert.tla](PrimaryKeyInsert.tla) models two one-shot callers inserting
into one append-only bucket. The callers either use the same key or use distinct
keys that collide in that bucket. Separate configurations assign row IDs through
an abstract reservation counter (`Allocate`) or provide distinct row IDs
(`Existing`). Each caller captures the bucket head, searches that immutable
chain, allocates its private candidate once, prepares its link, and performs CAS.
A failed CAS supplies the observed head for a fresh search before retry.

The safe protocol compares against the **head whose chain was searched**. A
publication between absence detection and CAS therefore causes a retry. The
`LateHead` mutation instead loads the current head after absence detection and
uses it as the expected value without searching that chain. Both callers can
then report success with different row IDs for the same key, even though neither
CAS fails. This is the race repaired in both `get_or_insert` and `insert_existing`
in [execution.rs](../../aerostore_core/src/execution.rs).

Four completed searches check unique published keys, reachability of all
published nodes, and each returned ID's agreement with that key's first
successful publication. Two negative cases reject `LateHead` for allocated and
provided IDs. A positive witness requires a losing CAS, rescan, and return of the
existing winner's ID; another requires both distinct colliding keys to publish.
Witness cases check the safety invariants too and are not completed searches.

The native deterministic tests
`pk_map_racing_get_or_insert_returns_existing_winner`,
`pk_map_racing_insert_existing_returns_existing_winner`,
`pk_map_racing_get_or_insert_observes_explicit_winner`, and
`pk_map_racing_insert_existing_observes_allocated_winner` execute the real Rust
methods, including mixed API races. They are separate implementation evidence;
there is **no Rust refinement proof** connecting them or the implementation to
this model.

The finite model contains exactly two possible entry nodes, so following a head
and its successor is the complete chain, not a traversal depth limit. Published
entries and links are immutable. Atomic search of a captured chain, fresh
nonreused node identities, and atomic CAS/reservation are assumptions. It omits
allocation failure, reclamation of losing private nodes, capacity/wraparound,
the provided-ID allocator-floor update, arbitrary mixed reservation inputs,
arbitrary-length chains, weak-memory semantics, and owner death. No fairness or
general termination claim is made.

## Evidence integrity

[campaign.json](campaign.json) is the complete configuration manifest, including constants and expected property for every case. [evidence/report.json](evidence/report.json) hashes all eight models, the campaign, tool metadata and runner and records exact Java invocations. Re-run with `--output verification/tla/evidence` only when intentionally refreshing the retained evidence. Routine verification should use the default disposable target directory.
