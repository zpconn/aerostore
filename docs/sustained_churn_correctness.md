# Sustained churn correctness and verification

The sustained Crucible workload exposed structural corruption that throughput alone did not detect. The original repair established process-shared serialization for index structure operations and stable per-row writer coordination. Subsequent Extended Crucible failures led to [native transactional index publication and predicate validation](transactional_indexes.md). Performance work must retain the correctness gates described below. The historical measurements in this report predate those transaction-layer changes.

## Failure mechanisms

1. **Detached predecessor insertion.** An insertion retained predecessor A, another operation unlinked A, and insertion successfully changed A's next pointer. The new node became unreachable despite a successful return. Epoch protection kept A allocated but did not keep it connected to the live index.
2. **Mutable published keys.** The in-place rekey path changed a removed node's key while readers or mutators could still retain its address, invalidating traversal ordering.
3. **Commit/index reordering.** Two writers could commit the same row in order A, B but publish their index changes in order B, A. The missing-source fallback could create permanently stale postings.
4. **Failed allocation ownership.** Posting, spill, and tower allocations could be abandoned when a later allocation failed before publication.
5. **Posting reclamation.** Deleted individual postings on keys that remained live needed their own retirement/reclamation path; reclaiming only empty key nodes was insufficient for general churn.
6. **Collector starvation during repair.** A first serialized baseline let the collector skip work when it could not acquire the mutation lock. Sustained foreground traffic could starve reclamation and exhaust the arena despite a structurally correct index. The collector now registers as a priority waiter on the same lock. Ordinary operations keep the inexpensive CAS path, but yield to pending collection; the foreground path rechecks priority after acquisition to close the registration race.

The historical investigation established real correctness defects and reproduced memory exhaustion. It did not measure each defect's contribution to the original March benchmark capture.

Full-suite validation also reproduced an OCC allocation-progress failure: a release-mode writer exhausted a 10 MiB arena before the default one-second vacuum interval elapsed. Recycled-row stacks additionally used untagged offset CAS operations, allowing an ABA pop and a stale pop's next-pointer read to race row reinitialization. Recycler shard locks now protect both pointer access and ownership transfer; the production recycler no longer abandons contended pushes into a thread-local stash.

The general arena allocator had independent ownership defects. Its thread-local offset cache mixed arenas, survived arena reinitialization, lost blocks on thread exit, and discarded mismatched block sizes. A controlled ABA schedule also made two live allocations receive the same shared free-list offset. Regression tests reproduced these failures before repair. Recycled blocks now remain in per-class shared pools protected by the process-shared lock, and size searches preserve nonmatching blocks. The cache-flush API remains compatible but is a no-op; recycling is immediately visible across threads and processes.

Error and telemetry checks also matter: exhausted deletion retries now return their error, and moves contribute to insertion/retry telemetry. Saturating every ProcArray slot verifies that a failed removal cannot claim success and that move retries are measured.

## Current protocol and invariants

The shared index lock covers predecessor search, publication, unlinking, moves, scans, and collection. It resides in shared memory and coordinates independently attached processes. The skiplist retains ordered links and epoch tracking, but its repaired mutation protocol is serialized. Published keys remain immutable; a move publishes its destination before removing its source, preserving the source when destination allocation fails. Partial allocations must return to reusable storage before their operation exits.

`OccTable::bind_index` now registers native index maintenance before transactions start. Every commit validates indexed predicate reads (including empty results), prepares all index destinations, publishes the rows and index changes under shared publication guards, and invalidates older incompatible indexed snapshots with a serialization error. Savepoint rollback and abort derive index state from the surviving row writes. Initial binding and independently attached handles validate the shared registry. Raw mutations of bound indexes are rejected.

`OccTable::lock_indexed_rows` remains available to bound contention before taking a snapshot, but is no longer the mechanism that establishes atomic index publication. The Tcl bridge and both Crucible writers use the native commit path. A stop request cannot split an already started native commit. `OccCommitter` publishes rows and indexes before WAL encoding/output; a later WAL error remains fatal and must not replay the committed transaction.

Index destinations are allocated before any source is removed. Preparation failure rolls back successful additions without requiring allocation. Unexpected failure during source removal, rollback, or publication poisons the shared table/indexes so inconsistent state cannot be queried as healthy. See the [transactional-index guide](transactional_indexes.md) for the complete protocol, snapshot horizon protection, regression tests, and limits.

The checked invariants are:

- Every successfully published insertion remains reachable until deletion.
- Live links remain ordered and acyclic; previously published keys do not change.
- An allocation is live, temporarily owned by an operation, retired, or reusable. Failure before publication returns temporary allocations.
- Native commits publish registered index changes with rows and validate positive and empty index predicate dependencies.
- After writers quiesce, the raw index traversal exactly equals the committed table, including cardinality, key/row association, ordering, and multiplicity.
- After workers and readers finish, retired index work drains without recycling errors.
- A quiescent allocation census requires `allocated = reachable + retired + reusable` for index nodes, postings, tower slots, and physical tower lanes. The census detects missing ownership, duplicate ownership, cyclic storage chains, and invalid upper links. After the final drain, every retired count is zero.

Raw index diagnostics do not form a transaction snapshot; application queries must use the registered transactional API. Abrupt process death while holding a shared lock requires arena recovery; ordinary shutdown requests GC termination outside its critical section. The benchmark fails its disposable process if a worker crashes or must be killed, avoiding a graceful join on a potentially stranded lock.

### OCC allocation pressure

Normal row allocation checks its own recycler shard and four nearby shards before allocating fresh storage. If fresh allocation fails, it searches every recycler shard before returning exhaustion. Fresh-allocation telemetry counts successful allocations only.

Transactional writes with a configured vacuum leader request an early vacuum pass on allocation failure and retry for at most one second, stopping sooner if the leader exits. The vacuum daemon checks the shared request during its interruptible sleep, normally within 50 ms. The transaction remains registered while waiting, so versions required by its snapshot remain protected. The retry occurs outside OCC partition locks, and reclamation still runs through the daemon and its callbacks. Recovery operations, which already hold a partition lock, do not enter this wait. If there is no configured vacuum leader, or live snapshots prevent reclaiming enough storage, allocation still returns an error; the engine does not reclaim live versions to force progress.

## Regression and model checks

Run the real implementation's regressions and multiprocess checks:

```bash
cargo test -p aerostore_core --release --lib shm_skiplist::tests -- --test-threads=1
cargo test -p aerostore_core --release --lib shm_index::tests -- --test-threads=1
cargo test -p aerostore_core --release --lib shm::tests -- --test-threads=1
cargo test -p aerostore_core --release --test occ_index_ordering -- --test-threads=1
cargo test -p aerostore_core --release --test shm_index_fork -- --test-threads=1
cargo test -p aerostore_core --release --test shm_index_contention -- --test-threads=1
cargo test -p aerostore_core --release --test shm_index_gc_horizon -- --test-threads=1
cargo test -p aerostore_core --release --test occ_recycle_progress --test vacuum_recycle_stress --test vacuum_recycle_ab_benchmark
cargo test -p aerostore_core --release --lib occ_partitioned::tests
```

The focused Loom suite uses the production shared-lock implementation with Loom atomics:

```bash
RUSTFLAGS='--cfg aerostore_loom' \
CARGO_TARGET_DIR=/tmp/aerostore-loom-target \
cargo test -p aerostore_core --test shm_mutation_model --release
```

The models cover a small live graph where two workers interleave insertion, deletion, observation, collection, and allocation ownership; two writers publishing table/index changes; and failures during partial allocation. Two negative controls check that the original unprotected predecessor protocol and omission of the row guard both have counterexamples. Together with the three repaired-protocol checks, the suite contains five Loom models. The production lock's acquisition/release protocol is shared with the models, while the graph, table, and allocator are abstractions.

Exploration uses a **preemption bound of 2** and **10,000 maximum branches**, with no elapsed-time or permutation cutoff. Exceeding the branch bound fails the test; it is not a successful partial run. These tests establish the older structural/row-coordination model properties within those bounds; they do not model the newer native predicate/publication protocol. They do not prove the entire skiplist, mmap behavior, arbitrary process failures, all allocator states, or unbounded execution. Real allocation-failure tests, controlled race regressions, process tests, and sustained workloads remain necessary.

Use `aerostore_loom`, not the generic `loom` configuration flag: the latter also changes dependency behavior, including Tokio, and does not select this suite correctly.

## Sustained validation

A diagnostic run needs no Docker:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_DURATION_SECS=30 \
AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=/tmp/crucible_128m.csv \
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Run 120 and 240 seconds at the normal 2 GiB profile and at 128 MiB:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_LOG_DIR=/tmp/crucible_2g_compare \
./scripts/check_crucible_2g_120_vs_240.sh

AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_LOG_DIR=/tmp/crucible_128m_compare \
./scripts/check_crucible_2g_120_vs_240.sh
```

Unset `AEROSTORE_CRUCIBLE_AEROSTORE_ONLY` for the PostgreSQL comparison. Normal comparison mode retains the existing minimum 2x TPS and maximum 0.6 p99 latency ratio gates. Diagnostic mode makes no PostgreSQL performance claim.

The benchmark checks exact final table/index agreement using a fallible raw traversal that preserves duplicates and order. It then performs the exact allocation census after GC drain, including the sentinel and the physical capacity of recycled towers. Each structural class reports allocated, reachable, retired, and reusable counts; missing or duplicate ownership fails the run. This census covers index structural storage, not payload spill blocks, arena padding, or OCC row versions. Separate allocation-failure tests and fresh-allocation telemetry cover those other paths. It requires the 80/20 upsert/scan mix, 5% hot upserts, zero operation/index failures, clean child exits, zero leftover worker epoch registrations, and a drained retired queue with no recycle errors. In-flight committed updates finish during drain and count as successful only after index maintenance succeeds.

Every five seconds, it reports interval TPS, arena high-water bytes, successful fresh allocation bytes by class, retired node/posting backlogs, reclaimed nodes/postings, and allocation failure events. Use `AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS=1000` for closer inspection. Class counters measure successful bump allocations and exclude reuse; arena head growth also includes alignment. The whole-node retirement count is derived from the change in queued plus reclaimed nodes; `retired_nodes()` itself reports queue depth. A final drained GC queue alone cannot establish absence of a leak: unreachable objects may never have entered that queue. The separate ownership census detects such missing structural allocations.

For runs of at least 30 seconds, second-half arena growth must be no more than one seeded working-set footprint, and second-half interval throughput must retain at least 50% of first-half throughput with no zero-progress full interval. This catches major degradation without a host-specific TPS floor. The 120s/240s script requires the successful allocation-audit marker and additionally requires the longer run to retain 90% of aggregate TPS, a worker drain within one second, no HOT ending pressure, and at most 128 insertion attempts. Comparison mode also retains 90% of the PostgreSQL TPS ratio. These are finite-run regression gates, not a proof that memory stays bounded forever.

## Shared-memory compatibility

The current shared arena layout is version **4** and boot metadata is version **6**. Layout 4 adds transactional index metadata and snapshot lifecycle/horizon state to the earlier lock/reclamation/accounting repairs. Old mmap files are incompatible and must be cold-rebuilt using the application's recovery path and durable input/WAL as appropriate. Do not attach an old mapping using guessed offsets or change version fields to bypass validation. Preserve required durable recovery inputs before replacing an existing mapping.

## Historical validation results (before transactional indexes)

Validated on 2026-09-22 using Rust 1.93.1 on WSL2, Intel Core Ultra 9 285K with 24 visible CPUs, Docker 29.8.0, and PostgreSQL 16. The tested working tree is based on `c96fd38ad048ab3ade095d22151341bf5e7ab0bd`. [Raw logs, CSVs, commands, source fingerprints, and machine-readable results](bench_data/crucible_fixed_2026-09-22/README.md) accompany this report.

The complete `cargo test --offline --workspace --release -- --test-threads=1` suite passed. All five Loom checks passed with the bounds above, including the two counterexample controls. Existing explicitly ignored tests remain outside the default workspace run.

Both sustained comparison scripts passed, with each workload run separately from the test suite and other benchmarks:

| Arena limit | Duration | Aerostore ops/s | Total operations | Arena high-water MiB | Second-half growth, bytes | Late/early interval throughput |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 GiB | 120 s | 358,954 | 43,076,086 | 20.724 | 80,104 | 1.0352 |
| 2 GiB | 240 s | 376,727 | 90,416,047 | 20.861 | 17,824 | 0.9342 |
| 128 MiB | 120 s | 369,716 | 44,367,587 | 20.798 | 96,344 | 1.0136 |
| 128 MiB | 240 s | 371,966 | 89,273,403 | 20.881 | 34,280 | 1.0046 |

Across 267,133,123 Aerostore operations, every run finished with exactly 50,000 committed rows and 50,000 matching index postings. Every structural ownership audit passed. All retired node/posting queues drained to zero, with zero operation/index failures, zero allocation failures, zero GC recycle errors, and zero leftover worker epoch registrations. Maximum insertion attempts, including moves, were 1; pressure ended NORMAL. Each workload plus worker drain took its requested duration plus approximately 4 ms.

The 240-second run retained **104.95%** of the 120-second aggregate throughput at 2 GiB, and **100.61%** at 128 MiB. The measured arena high-water marks were below 21 MiB in all four runs. Post-seed fresh allocation totals ranged from 1.33–1.41 MB for row versions, 1.56–1.62 MB for nodes, 0.495–0.516 MB for postings, and 0.159–0.165 MB for towers; general and spill classes allocated no additional bytes. These class figures use decimal MB; the high-water column uses binary MiB. Per-interval and cumulative class data are in the CSVs.

The normal 2 GiB profile also passed the unchanged PostgreSQL gates:

| Duration | PostgreSQL ops/s | Aerostore/PostgreSQL throughput | Aerostore/PostgreSQL p99 |
| --- | ---: | ---: | ---: |
| 120 s | 51,290 | 7.00× | 0.125 |
| 240 s | 49,161 | 7.66× | 0.062 |

PostgreSQL was intentionally omitted from the reduced-arena diagnostic runs. These measurements support the repaired behavior for the tested workloads and durations; they do not extend the bounded models into a proof of arbitrary executions or process-crash recovery.
