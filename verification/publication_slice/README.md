# Checked lifecycle, guard, and predicate joins

This proof-only harness embeds and verifies the exact current generated native
predicate, guard, and ProcArray lifecycle modules. It does not implement another
database. Its three connections are:

1. Call native `index_lock_keys`, then native `acquire_index_locks`. The returned
   guard vector covers every read dependency and old/new publication bucket.
   Local binding ordinals must map to the same index header in the same physical
   arena. The inverse registry relation also proves distinct header identities.
2. Translate that actual guard coverage into the predicate validator's required
   permissions under the explicit native guarded-observation contract.
3. Derive a publication stamp newer than a prior reader reservation from the
   shared-clock history relation. Feed the actual publication's stamp relation
   into the actual native validator, proving that it rejects an affected read.
   No `stamp >= reader` numeric premise is supplied to this combined result.

The relational join still takes a represented shared reservation history. The
[lifecycle scenario](../lifecycle_scenario/README.md) additionally calls native
registration, snapshot, writer deregistration, publication and validation through
one checked clock adapter rather than supplying that chronology as two states.

Freshness refers to the atomic reservation of the publication label, not the
later physical stamp stores. A reader starting after that reservation can have a
larger transaction ID while the writer still holds bucket guards. Disjoint
writers can store their already-reserved labels out of numeric order. A global
history theorem must justify the relevant reordering; this join does not assume
physical stores form a globally increasing sequence.

## Boundary

The registry/arena relation, underlying mutex ownership and lifetime, guarded
stamp observations, shared-clock linearization and no counter wrap are explicit
primitive/caller obligations. In particular, an arbitrary unowned record with
matching numeric fields is not a real mutex guard. Native candidate completeness,
row/MVCC visibility, unsafe storage and arbitrary concurrent-history refinement
remain open. This is checked composition of conditional source-bound theorems,
not whole-P1 or whole-engine verification. Lean and TLA+ results are not imported
as Verus assumptions.

```sh
python3 verification/publication_slice/generate.py --check
python3 verification/publication_slice/test_generate.py
python3 verification/publication_slice/run.py
```

The pilot requires all named roots, current embedded source, exact verifier
commands and six semantic negative controls. Skipping native key generation,
guard acquisition or validation, discarding acquired guards, and joining the
wrong dependency or reader history must fail intended proof obligations.
