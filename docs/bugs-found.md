# Bugs found by the guardrails

Aerostore was built with AI coding agents, and its tests, models, proofs and
benchmark oracles are what make the result trustworthy. This page lists the
engine defects those guardrails caught, how each was found, and how it was
repaired. Every entry is described in a linked report, and every repair is a
commit in this repository.

**How to read "found by".** A finding is credited to the check that first
exposed it. Some models were written after a defect was already fixed, to keep
it from returning. Those models are credited as regression guards, not as the
finder. Commit hashes refer to this repository's rewritten history. The
[history map](../evidence/history-commit-map.tsv) translates them to the
archived originals.

## Summary

| Area | Defects | Found by | Repair |
| --- | ---: | --- | --- |
| Index and allocator structure under sustained churn | 9 | Sustained Crucible, then targeted regressions | [`f993cf2`](https://github.com/zpconn/aerostore/commit/f993cf2) |
| Transactional isolation of secondary indexes | 3 | Extended Crucible native-contract probes | [`6cb7254`](https://github.com/zpconn/aerostore/commit/6cb7254) |
| WAL, checkpoint and insertion races | 5 | TLA+ counterexamples, cross-process tests, review | [`5760a1a`](https://github.com/zpconn/aerostore/commit/5760a1a) |
| Public vacuum horizon | 1 | Closing a premise in the row-retention proof | [`b95d093`](https://github.com/zpconn/aerostore/commit/b95d093) |
| Performance regressions in repair candidates | 2 | Matched whole-engine comparison runner | Folded into `5760a1a`; [`f3584f8`](https://github.com/zpconn/aerostore/commit/f3584f8) |

## Structural corruption under sustained churn

The original sustained Crucible benchmark showed throughput decay and arena
exhaustion. Investigating that decay exposed structural defects that a
throughput number alone would never reveal. Regression tests reproduced each
defect before it was repaired. Details are in
[sustained churn correctness](sustained_churn_correctness.md).

| Defect | What went wrong | Consequence |
| --- | --- | --- |
| Detached predecessor insertion | An insert held predecessor A. A concurrent operation unlinked A, and the insert still swung A's next pointer. Epoch protection kept A allocated but not reachable. | An index entry silently vanished after a successful insert |
| Mutable published keys | An in-place rekey changed a removed node's key while other threads still held its address | Skiplist traversal order was corrupted |
| Commit/index reordering | Two writers committed a row in order A, B but published index changes in order B, A. A missing-source fallback then created postings. | Permanently stale index postings |
| Failed-allocation ownership | Posting, spill and tower allocations were abandoned when a later allocation in the same operation failed | Leaked shared memory |
| Posting reclamation only for empty keys | Deleted postings under a key that stayed live were never retired | Arena exhaustion under ordinary churn |
| Collector starvation | The collector skipped its pass whenever the mutation lock was busy, which under sustained load was always | Arena exhaustion despite a correct index |
| OCC allocation progress and recycled-row ABA | A writer exhausted a small arena before vacuum ran. Separately, untagged CAS on recycled-row stacks let a stale pop race row reinitialization. | Spurious exhaustion; row corruption |
| Arena allocator ownership | A thread-local cache mixed arenas, survived reinitialization and lost blocks on thread exit. A controlled ABA schedule gave two live allocations the same offset. | Two owners of one block of shared memory |
| Swallowed deletion-retry exhaustion | A failed removal reported success | Silent index inconsistency |

**Repair.** A process-shared lock now serializes skiplist structure changes.
Published keys are immutable, and a move publishes its destination before it
removes its source. Each row has stable writer coordination. Partial
allocations are returned before an operation exits. The collector registers as
a priority waiter on the same lock. Recycled blocks live in per-class shared
pools.

**Regression guards.** Seven bounded [Loom cases](../verification/contracts/lock_models.md)
run against the production lock. They include negative controls that must
rediscover the detached-predecessor race and the missing row guard. The TLA+
models `LiveKeyPosting` and `CollectorPriority` were written afterwards to pin
the reclamation and starvation fixes.

## Transactional isolation of secondary indexes

Extended Crucible replays complete message transactions against a serial
reference model. Its native-contract mode exposed three gaps in how secondary
indexes participated in transactions. See
[Extended Crucible](extended_crucible.md) and
[transactional indexes](transactional_indexes.md).

| Defect | What went wrong | Consequence |
| --- | --- | --- |
| Competing creation after empty searches | Two transactions each searched an index, found nothing, and created the same logical entity | Write skew: both committed |
| Row/index publication gap | A committed row and its index postings did not become visible atomically | Readers could see a row without its index entry, or the reverse |
| Old-key candidates for older snapshots | An index lookup from an older snapshot missed rows whose key had since moved | Incomplete query results |

**Repair.** Indexes became part of the table. Each index has 4,096 hash buckets
carrying publication locks and stamps. A lookup records its bucket stamps even
when it finds nothing, and commit revalidates them. Rows and postings publish
under the same guards. Old-snapshot lookups now fail with an explicit
`SerializationFailure` rather than return an incomplete set. That is a retry
contract, not a history-preserving index.

## WAL, checkpoint and insertion races

These are the clearest model-first findings. A small TLA+ specification of
publication, logging, checkpointing and crash recovery produced
counterexamples. Deterministic Rust regressions then reproduced the same
schedules in the real engine. See the [durability contract](../verification/contracts/durability.md)
and the [TLA+ campaign](../verification/tla/README.md).

| Defect | Found by | What went wrong | Consequence |
| --- | --- | --- | --- |
| Publish before WAL | `Durability.tla` (`Dependent`, `AcknowledgedSafety`), then `wal_protocol_regressions.rs` | Commit published rows, then encoded and submitted the WAL record | After a crash, a transaction that had read and acknowledged T1's write could survive while T1 was lost |
| Unsafe checkpoint cut | `Durability.tla` (`Checkpoint`), then native regressions | The checkpoint released exclusion before sampling its replay cutoff, and recovered by start ID alone | Truncation discarded a transaction that started early but committed late |
| Mixed-stream dependency | Reproduced in the native engine | A synchronous acknowledgement depended on a write still buffered in another WAL stream | Lost dependency after a crash |
| Poison admission | Final review, confirmed by three deterministic regressions | A transaction could resume after the table was poisoned during WAL preparation. The synchronous appender checked poison outside its file lock. | Work admitted after an indeterminate failure |
| Duplicate primary-key insertion | The cross-process test suite, then `PrimaryKeyInsert.tla` (`LateHead`) | CAS compared against a freshly loaded bucket head instead of the head whose chain was searched | Two callers both "inserted" the same key with different row IDs |

**Repair.** Commit now encodes its WAL payload before taking guards. It
validates under the guards and accepts the WAL before it removes source
postings or publishes rows. A checkpoint holds partition exclusion through
persistence and truncation, and it records the IDs active at the cut. Each
table is bound to exactly one WAL stream. Poison is rechecked under guards and
under the file lock. Primary-key insertion uses CAS against the searched head.
`CheckpointCut.tla` checks the implemented cut, including three mutants that
must fail.

## Public vacuum horizon

While closing the horizon premise in the [row-retention proof](../verification/storage_slice/README.md),
the proof work exposed a public API bug. `vacuum_reclaim_once` accepted a
caller-supplied horizon, and a horizon past a live reader's snapshot could
prune the version that reader needed. The reader's first read then reported
the row absent, and its transaction committed. The public method now clamps
requests to the arena's retained horizon. Proofs of the caller dispatch and
deterministic native checks cover the repair.

## Performance regressions caught by the comparison gate

Correctness repairs also go through matched whole-engine performance
comparisons (`scripts/compare_engine_performance.py`). Two repair candidates
did not pass at first.

- **WAL encoding under commit guards.** The first durability candidate
  lengthened the commit critical section. Extended workloads regressed
  repeatably, and the candidate was rejected. The revised design moves encoding
  ahead of guard acquisition. A regression test checks that a competing writer
  can finish during encoding and invalidate the prepared transaction without
  logging it.
- **7% sustained-Crucible throughput loss.** The revised durability candidate
  still failed the original Crucible throughput gate. Three lock and layout
  experiments were rejected. A cached skiplist removal window then recovered
  the throughput, with a median gain of 1.23% and a lower candidate p99 in
  every pair. The gate still classified the study as **inconclusive**, because
  the reference's own p99 spread exceeded the noise limit. The change was
  accepted on that recorded basis, not as a performance promotion.

The same work found a measurement defect. Crucible's latency histogram reported
bucket lower bounds as if they were exact values. It now reports exact integer
quantile intervals on both engines. See the
[verification plan](formal_verification_plan.md) for this sequence.

## Earlier fixes

The February–March 2026 history has three agent-authored repairs:
`df02e8e` (MVCC snapshot race), `c29001e` (WAL daemon lifecycle) and `de61be8`
(WAL daemon restart epoch race). The current documentation does not record
their mechanisms, so they are listed by commit only.

## Known open issues

These are stated limitations, not undiscovered bugs:

- **Worker death in direct mode.** A process killed while it holds a row lock,
  an index guard or a commit's predicate guards leaves those resources
  unusable to survivors until the engine is restarted from durable state. Its
  registration also pins vacuum. The database-owned service isolates client
  death at the tested cut points. See the
  [worker failure contract](worker_failure_contract.md).
- **Torn or corrupt WAL.** Recovery reports an error; it does not recover the
  valid prefix. WAL frames carry no checksums yet.
- **Counter exhaustion.** Transaction and stamp counters are unchecked `u64`
  values. The finite-clock no-wrap rule is not enforced natively.
- **Weaker query paths.** `ChunkedEventual` scans and the older in-process
  `MvccTable`/`QueryEngine` API are not serializable.
