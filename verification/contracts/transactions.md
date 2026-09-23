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
failure poisons access until exclusive recovery. A WAL failure after publication
is not a clean abort and cannot authorize repeating an already applied input.
Precommit observations must not cause irreversible external effects. Opacity
for all aborted/speculative observations is an additional open obligation.

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

## Durability

Synchronous acknowledgement survives supported crashes with all dependencies.
Asynchronous acknowledgement may lose a recent suffix, but recovery preserves
whole transactions and a dependency-closed history. Log positions represent
commit/log order rather than transaction start order. A checkpoint contains a
consistent cut and permits truncation only of records it safely subsumes.
Replaying durable inputs deterministically reconstructs rows and derived indexes.

The current visibility-before-WAL and checkpoint-cut algorithms require separate
investigation. Model counterexamples are evidence about modeled executions,
not automatic demonstrations that an actual application permits that schedule.

## Optimization boundary

Observable guarantees, error semantics, durability mode, resource budgets, and
environmental premises remain fixed within an experiment. Internal protocols
and representations may change if the candidate implementation re-establishes
the contract. A green old model, an omitted branch, or a stronger unproved
primitive assumption is insufficient evidence for a changed implementation.
