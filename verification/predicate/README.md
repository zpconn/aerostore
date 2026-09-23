# Native indexed-transaction data contracts

This campaign extracts three existing production methods from
`aerostore_core/src/occ_partitioned.rs`: `index_lock_keys`,
`index_read_conflict`, and `publish_index_stamps`. It proves their data contracts
for arbitrary finite read/change collections, registry contents, key-to-bucket
mappings, and stamps, conditional on the explicitly named primitive contracts.
There is no second executable database and no added production bookkeeping.

```sh
python3 verification/predicate/generate.py
python3 verification/predicate/test_generate.py
python3 verification/predicate/run.py
```

The generated file is [predicate.verus.rs](predicate.verus.rs). The fixed
[contracts](contracts.rs), [adapter](generate.py), and [runner](run.py) belong to
the verification boundary. The runner uses the pinned Verus distribution with
`--no-cheating`, checks each required root separately, and records input,
generated-source, tool, and log hashes in
`target/verification/predicate/receipt.json`. Stale generated code, source changes
during the run, missing checks, and wrong-kind negative-control failures fail the
campaign.

## What is established

**Lock coverage.** A successful `index_lock_keys` returns the sorted, duplicate-free
union of every recorded read dependency's `(binding, bucket)` and every change's
old and new key buckets. All recorded index offsets resolve on success. The proof
requires change bindings to be within the registered-index count; successful
read resolution also establishes that bound. The proof
tracks bindings as well as buckets: equal bucket numbers in different registered
indexes cannot replace one another. It assumes neither collision-free hashing nor
distinct read/change inputs. Empty reads, absent old/new keys, repeated buckets,
and multiple bindings are included.

This proves what the native algorithm requests to lock. It does not prove that
native lock acquisition establishes ownership or that guards survive until every
required publication operation finishes.

**Read validation.** On success, `index_read_conflict` returns true exactly when
the sticky conflict flag is set or at least one captured dependency does not have
a present, unchanged stamp strictly below the transaction's snapshot identifier.
Stamp reads are interpreted under the held-bucket and stable-read primitive
contracts. Missing bindings or native read errors may return `Err`; they cannot
be treated as a successful validation. The strict comparison helper is freshly
extracted from `aerostore_verified/src/lib.rs` and verified in this campaign too.

**Publication.** With prior deregistration, the required affected guards, and a
nonexhausted allocator step, successful `publish_index_stamps` assigns the same
reserved stamp to exactly the union of the changed old/new buckets. It modifies
no other stamps in the operation's abstract projection. No allocation step or
stamp update occurs for an empty change vector. A nonempty vector reserves once,
even if its optional old/new keys are all absent. On an error, already published
stamps may remain; any altered stamp is an affected bucket with the reserved
value. The commit driver's separate poison/cleanup obligations still apply.

**Conditional composition.** `publication_invalidates_dependency` proves that a
captured dependency on any touched bucket cannot validate after a publication
whose stamp is at least the reader's snapshot identifier. The publishing writer's
own transaction identifier is irrelevant: an older writer can publish a newer
stamp. `changed_stamp_invalidates_dependency` handles any changed captured stamp,
independently of that snapshot inequality. The proof harness
`validate_after_late_publication` calls the extracted native validator and proves
that it returns conflict or an error under the first lemma's hypotheses.

These are parameterized theorems, not an enumeration of two-worker schedules.
However, they do not themselves prove that native lookup captured every necessary
dependency, or that a particular native concurrent execution satisfies the
publication-time inequality. Those are separate capture/history and allocator
refinement obligations. The [capture campaign](../predicate_capture/README.md)
and Lean/TLA work address complementary parts; their conclusions are not silently
imported into these Rust contracts.

## Exact source adaptation

The adapter checks each native signature and retains the selected executable body
with the following restricted transformations:

1. Native row-independent transaction/read/change fields become proof-side
   structures. Keys become opaque `usize` identities with an arbitrary fixed
   `(binding, key) -> bucket` map. No key-equality or hash-injectivity property is
   introduced. Unused fields such as change row identifiers are projected away.
2. The exact native registry `position`/`find` expressions become
   `find_binding`; the registry map must represent their actual first matching
   binding. Native indexed references become binding arguments. Explicit
   `changes_valid` preconditions require every change binding to be in range,
   and registry lookup guarantees an in-range result. The real `index_changes`
   producer's satisfaction of that precondition and the native pointer/index
   representation remain outside these proofs.
3. `BTreeSet` creation/insertion/collection use a generic `PairSet` interface with
   ordinary extensional insertion and lexicographic iteration contracts. Its
   implementation, allocation, and panic behavior are assumed. The unused native
   Boolean insertion result is omitted.
4. Read/change and set-iteration `for` loops become indexed loops with fixed
   proof invariants and decreasing measures. The exact two-element
   `[before.as_ref(), after.as_ref()].into_iter().flatten()` iteration is unrolled
   into `if let Some` branches in the same order. Insertions, arguments, early
   returns, `?` propagation, and conflict decisions remain source-derived.
5. Key encoding/hash, guarded stamp loads, the `AcqRel` allocator reservation,
   and stamp publication stores become declared primitive calls. The exact
   native ordering token is checked; changing it fails extraction. Their native
   atomic implementations are not proved here.
6. `&self` becomes a mutable proof-side driver for publication. Proof invariants
   and ghost snapshots cannot execute in AeroStore. That mutable receiver does
   not establish exclusive access to the live database.

Unknown receiver calls, changed loop shapes or selected signatures, unsupported
attributes, and proof bypasses fail extraction. Other unsupported syntax must
still pass Verus typechecking and verification; it is never silently erased.
This is a reviewed, restricted adapter, not an automatic whole-module Rust
refinement tool.

## Concurrency and primitive boundary

`State.stamps` is the operation's guarded stamp projection, **not a frozen view
of every stamp in the live database**. The frame theorem describes this
operation's updates. Other workers may publish disjoint buckets concurrently.
Connecting this projection to real lock ownership and admissible interference
remains unproved.

`State.clock` now records the previous represented clock observation. A reservation
may return a larger value because other allocators intervene; `reserved_stamp`
records that returned label and `reservations` appends it to the represented
history. The projected counter then becomes label plus one. This is not a claim
that the physical global counter advances by only one over the native method's
duration. The publication theorem names the actual returned label, including on
partial publication errors. The [shared-clock scenario](../lifecycle_scenario/README.md)
implements this primitive by calling the source-bound lifecycle reservation on
the same object used by reader registration, deriving freshness in that
controlled schedule. General native interleaving and memory-order correspondence
remain open. Wraparound is explicitly excluded, including intervening allocations;
this campaign does not repair or verify exhaustion handling.

The primitive trait contracts are assumptions. They cover registry/key-map
stability, correct binding resolution and key encoding, collection semantics,
guarded load/store behavior, and allocator linearization. The theorem does not
establish posting/MVCC correspondence, raw-index completeness, allocator or
reclamation safety, rollback ownership, process-shared mutex correctness, native
weak-memory/mmap refinement, or panic freedom. It does not complete P1 or prove
full transaction-history refinement. Receipts explicitly keep native
storage/atomic and whole-history refinement claims false.

## Negative controls

Thirteen independently checked source mutants omit or corrupt read/write lock
buckets, merge write bindings, ignore sticky conflicts, ignore changed stamps,
ignore the snapshot boundary, publish to the wrong bucket, publish a stale stamp,
omit publication, skip nonempty changes, corrupt touched bindings, or make the
snapshot comparison nonstrict. Every mutant must reach Verus and fail an intended
precondition, postcondition, loop invariant, or proof assertion. A syntax error
or adapter rejection does not count as a semantic negative control.

Ten adapter tests separately cover freshness, ordinary comments, duplicate
methods, signature drift, wrong registry predicates, atomic-order changes,
changed two-key iteration, unknown receiver calls, proof bypasses, and the
semantic mutants' ability to reach the verifier.
