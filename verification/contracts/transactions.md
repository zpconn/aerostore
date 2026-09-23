# Transaction and recovery contract, version 1

This file fixes the target contract. It does not assert that the entire engine
has been verified. `claims.toml` and the generated verification report identify
which evidence exists and which obligations remain open.

## Covered application shape

One `OccTable` on one Linux host, used by independently attached worker
processes. Bootstrap/recovery and physical seeding are exclusive. Runtime
logical creation/deletion updates preseeded physical rows. Every attachment
uses the complete registered index set and identical pure key extractors.
Raw index diagnostics, legacy table implementations, multitable transactions,
remote workers, and arbitrary Tcl/C runtime behavior are outside this target.

## Observable operations

Histories record begin, row reads, complete predicate query results, pending
writes, savepoints, rollback, abort, publication, commit response, durable
acknowledgement, crash, and recovery. Results include explicit serialization
retry, clean resource failure, poisoned storage, and failure after publication.

Successful committed transactions must admit a serial execution with the same
results and writes, preserving real-time precedence between nonoverlapping
transactions. An internal transaction ID is not assumed to be that serial order.
Reads see the transaction's pending writes. Savepoint rollback restores pending
values; conservative retained read dependencies may cause extra retries.

Indexed reads return all matching rows allowed by the snapshot, or explicitly
retry. An old snapshot cannot silently lose candidates because a current index
key moved. Empty queries establish dependencies. A successful commit validates
all relevant reads and predicates, and publishes the complete row/index change.
Guarded transient physical mismatches are allowed only when permitted observers
cannot successfully consume them as a consistent committed result.

Prepublication failure preserves the old committed database. Unexpected partial
failure poisons access until exclusive recovery. Admission checks must reject
observed poison; a commit rechecks after acquiring its guards. This does not
promise instantaneous cancellation of disjoint operations already admitted, or
forbid their later wall-clock responses. A WAL failure after publication
is not a clean abort and cannot authorize repeating an already applied input.
Precommit observations must not cause irreversible external effects. Opacity
for all aborted/speculative observations is an additional open obligation.

Primary-key insertion must publish at most one entry per key. Competing callers
for the same key return the winning entry's row ID, including callers supplying
an existing row ID. Distinct keys sharing a hash bucket remain independently
insertable. A successful head CAS must compare against the exact chain that was
searched; a failed CAS requires searching the newly observed chain before
retrying. Caller-supplied row IDs retain their bootstrap/ownership preconditions.
Never-published losing entries may be recycled; automatic ID reservations can
still leave holes, so a bound on physical row-ID consumption remains separate
from a bound on distinct keys.

## Storage and progress

Every allocation has exactly one live, retired/pinned, in-flight, or free owner.
No reader, cursor, or row guard may access a recycled version. Integer overflow,
offset bounds, sentinel values, slot exhaustion, and process attachment must be
handled explicitly. Ordinary attachment cannot clear another live worker's
registrations or create a second ownership authority for the same mapping.

Safety must hold without fairness. Progress and finite-space guarantees require
stated scheduling, worker-lifetime, resource, and live-set assumptions. Collector
admission must permit reclamation while foreground work continues. Readers
cannot inherit obsolete horizons forever. Removed postings must be reclaimable
even when their key remains live. No guarantee assumes away a permanently
pinned reader, a failed collector, or an exhausted arena.

The [bounded production-lock campaign](lock_models.md) checks protected-value
handoff, observed priority admission, and abstract index/row-guard schedules
using the actual lock source with Loom atomics. Its finite bounds and mandatory
weakened-acquire counterexample do not discharge native mmap, unbounded
fairness, or the memory-progress obligations above.

## Durability

Synchronous acknowledgement survives supported crashes with all dependencies.
Asynchronous acknowledgement may lose a recent suffix, but recovery preserves
whole transactions and a dependency-closed history. Log positions represent
commit/log order rather than transaction start order. A checkpoint contains a
consistent cut and permits truncation only of records it safely subsumes.
Replaying durable inputs deterministically reconstructs rows and derived indexes.

The [native durability boundary](durability.md) records the implemented repairs
and current mode/stream restrictions. Deterministic regressions now reproduce
the earlier visibility-before-WAL and checkpoint-cut failures. The finite
models remain evidence about modeled executions, with no automatic Rust
refinement or complete filesystem guarantee.

## Optimization boundary

Observable guarantees, error semantics, durability mode, resource budgets, and
environmental premises remain fixed within an experiment. Internal protocols
and representations may change if the candidate implementation re-establishes
the contract. A green old model, an omitted branch, or a stronger unproved
primitive assumption is insufficient evidence for a changed implementation.
