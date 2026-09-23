# Transactional secondary indexes

`OccTable` now owns registered secondary-index maintenance and predicate validation. This closes the three gaps first exposed by Extended Crucible: competing creation after empty searches, a gap between row commit and index publication, and incomplete index results for older snapshots.

Use `bind_index` during quiescent initialization, before the table's first transaction. Seed or recover rows and build matching postings first. Bind the same indexes and deterministic extractors on every independently attached handle. The shared registry rejects incomplete attachments, and initial binding verifies that postings exactly match the seeded rows.

```rust,ignore
let mut table = OccTable::<Flight>::new(arena.clone(), capacity)?;
let index = SecondaryIndex::<usize>::new_in_shared("callsign", arena.clone());
// Seed/recover rows and matching raw postings here, before workers start.
table.bind_index(index.clone(), |row| {
    row.active.then_some(IndexValue::I64(row.callsign))
})?;

let mut tx = table.begin_transaction()?;
let candidates = table.index_lookup(
    &mut tx, &index, &IndexCompare::Eq(IndexValue::I64(callsign)),
)?;
// Evaluate candidates, read/write rows, and use savepoints as needed.
table.commit(&mut tx)?; // Includes every bound index; no postcommit maintenance.
```

Call `abort` on abandoned transactions, including failed attempts, and retry the complete application transaction on `SerializationFailure`. The same automatic index publication occurs through `commit_with_record` and `OccCommitter`. Bound indexes reject raw insert/remove/move calls. Direct table seeding and recovery writes are also rejected after binding. Raw index traversal remains available for quiescent auditing and explicitly nontransactional diagnostics; it is not a substitute for `index_lookup`.

## Predicate and snapshot rules

Each shared index has 4,096 fixed hash buckets containing publication locks and stamps, occupying 64 KiB plus its header. Equality predicates protect their key's bucket; `In` protects the union of relevant buckets. Inequalities conservatively protect all buckets and therefore cost more to acquire and validate. Hash collisions and broad ranges may cause additional retries. The metadata size is fixed, so this scheme does not accumulate historical postings or predicate records in shared memory.

An index lookup takes the relevant short-lived locks, checks their stamps against the transaction's start identifier, and records dependencies even for an empty result. It captures the complete candidate IDs, releases the locks, then reads the actual snapshot rows. It includes pending writes and rechecks the index extractor. Commit revalidates changes that occur after candidate capture. Rolling back to a savepoint restores pending row values and therefore their candidate keys. Predicate dependencies are conservatively retained across savepoint rollback.

Indexes store current postings. If a relevant key bucket changed after the transaction started, lookup returns `SerializationFailure` before presenting an incomplete candidate set. This is an explicit retry contract, not a guarantee that every old transaction can continue scanning historical index keys. Direct row MVCC still retains versions for active snapshots.

The query executor uses registered indexes and reapplies the complete query predicate to visible rows. A primary-key map alone cannot protect an absent result; the executor uses a bound equality index or falls back to reading all physical slots. Unregistered raw indexes likewise use that safe fallback. `StrictSnapshot` validates its transaction before returning and retries serialization failures within a finite budget. `ChunkedEventual` remains an explicitly weaker mode using independent chunk snapshots.

These guarantees apply to the process-shared `OccTable` path, including the Tcl bridge and Extended Crucible's Aerostore adapter. The original Crucible uses native row/index publication for writes, but its range probes deliberately count raw postings as a storage-churn diagnostic; those probes do not provide transactional predicate isolation. The older in-process `MvccTable`/`QueryEngine` prototype is a separate API and is not upgraded to serializable predicate isolation by this change.

## Commit protocol

1. Compute changes from the final pending value for each row and every registered index. Validate keys and payloads before modifying postings.
2. Acquire affected index buckets and recorded read buckets in canonical order, then the existing row partition locks. Each bounded acquisition tries up to 4,096 times, periodically yielding without sleeping. Unresolved contention returns a serialization retry after releasing acquired guards, before application backoff. No publication lock is retained across the application transaction.
3. Revalidate predicate stamps, concrete reads, and write bases. Insert every destination posting before removing any source. If a destination allocation fails, undo the successful additions without allocation, leaving the old rows and source postings intact.
4. Remove source postings and publish the complete row write set while the transaction remains registered. A concurrent transactional lookup of an affected bucket retries while publication is in progress.
5. Deregister the transaction, allocate a fresh publication stamp from the shared monotonic identifier allocator, and stamp every changed bucket before releasing locks.

The stamp is assigned after publication, rather than using the writer's original transaction identifier. A transaction that began earlier but committed later must still invalidate an overlapping reader. Assigning the stamp after deregistration also ensures that a reader whose start identifier follows the stamp can see the entire committed row transaction. A reader that began in the publication interval either finds a held bucket lock or observes a newer stamp and retries. Commit revalidation catches changes that occur after a successful earlier lookup.

Transaction registration, deregistration, and snapshot capture use a separate short lifecycle lock to make the active-transaction snapshot coherent. It covers metadata operations only, not user transaction execution or index maintenance.

Each active reader pins the snapshot horizon captured at its start, including an older writer that was still active. Vacuum uses those pinned horizons, so that writer's later commit cannot cause the reader's required old row to be recycled. New snapshots use current active transaction identifiers rather than inheriting other readers' retained horizons; reclamation therefore advances when the readers that actually need an old version finish. Vacuum also retains a version while a `RowLockGuard` still refers to it, preventing a stale guard from unlocking a different owner after storage reuse.

An unexpected source-removal, rollback, or partial-publication failure poisons the shared table and its indexes. Further transactional access fails explicitly until recovery; the engine does not present a partially maintained index as healthy. Successful row/index publication precedes WAL encoding/output in the existing `OccCommitter` design. A subsequent WAL error is fatal and must not replay an already committed input. This change establishes atomic row/index visibility, not a new crash-durability guarantee.

## Tests and compatibility

The regressions cover empty equality and range searches, insertion/removal/key movement across predicate boundaries, old snapshots whose first lookup follows a move, out-of-order writer transaction identifiers, distinct-key progress, own writes, nested savepoints, abort, real arena exhaustion during multi-index preparation, publication interrupted by a concurrent reader, independent process attachment, postcommit WAL errors, and snapshot/row-lock retention during vacuum. Query tests also preserve numeric primary-key types when using a registered index. Query and Tcl tests exercise the production integration paths.

```sh
cargo test --offline -p aerostore_core --release \
  --test occ_transactional_index --test transactional_query_execution -- \
  --test-threads=1
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine both --mode all --output target/extended-crucible.json
```

Shared arena layout **4** replaces layout 3; boot metadata remains version 6. The index wrapper is version **2**, rejecting the smaller bucket layout used during initial development of these repairs. Existing mappings require a cold rebuild from preserved recovery inputs. Do not alter version fields to force attachment. The index wrapper header and shared table registry are persistent layout changes.

Abrupt process death while holding a shared lock still requires arena recovery. Finite tests and the publication argument above do not constitute a formal proof of the full engine or production HyperFeed compatibility.
