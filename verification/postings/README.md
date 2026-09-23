# Native posting preparation, rollback, and source removal

This campaign conditionally verifies the actual `OccTable` helpers
`prepare_index_destinations`, `rollback_index_destinations`, and
`remove_index_sources`. The generated Verus bodies come from
`aerostore_core/src/occ_partitioned.rs`; they are not a replacement index
implementation.

The posting projection consists of `(binding, semantic key, row ID)` tuples.
Key IDs distinguish actual indexed values, including colliding hash buckets;
they are not bucket numbers. The `held` set expresses permissions for the keys
touched by this operation under the native publication guards. This is an
operation-local projection, not a claim that unrelated index activity stops.

The contracts establish that successful preparation adds exactly the requested
destinations and records their change indices; failed preparation restores the
initial posting projection or poisons the table. Rollback removes only recorded
destinations and poisons if any removal fails. The native loop continues after
removal errors; the theorem does not separately count attempted operations.
Successful source removal removes exactly the old postings; an error poisons
the table. These statements concern returned
`Result` paths, not arbitrary process death or allocator aborts.

Ownership is a precondition. Every destination must be absent initially and
destination tuples must be distinct. Without absence, native insertion could
succeed idempotently on an existing posting, after which rollback would remove
somebody else's posting. The native caller obtains these properties from:

- one final write per row (`final_write_indices`), one change per registered
  binding, and exclusion of equal before/after keys (`index_changes`);
- consistent current table rows and index postings, deterministic identical
  extractors on attached handles, and unique registered index identities;
- row-base revalidation and held row/predicate publication locks throughout
  preparation, rollback or source removal and publication.

Those caller invariants, the key encoding, and their composition with row/WAL
publication are not proved by these three helper bodies. The separate commit
and predicate campaigns cover parts of that composition conditionally.

The native primitive boundary is explicit:

1. Successful insertion adds exactly its absent target posting; insertion
   failure preserves the live posting projection. Native
   `attach_posting_to_key` validates and allocates before publishing links and
   has no fallible operation after publication. `prepend_existing_posting`
   validates its posting before CAS and returns success after that CAS. These
   implementation observations support the assumption; this campaign does not
   refine their unsafe pointer implementations or prove allocation ownership.
2. Successful removal removes its target, including idempotent removal of an
   absent target. Failed removal may already have removed that target, while
   preserving unrelated postings. Native `remove_payload_inner` marks the
   posting deleted before fallible unlink/retirement operations, so an error
   does **not** imply no effect. The callers' poison paths handle that case.
3. Poisoning sets a persistent shared failure flag. This contract does not
   imply instantaneous cancellation of disjoint operations already admitted,
   prove arbitrary-corruption recovery, or establish native atomic refinement.

`generate.py` is a restricted, reviewed source adapter. `contracts.rs` records
the primitive assumptions and posting-set theorems. The runner checks each
required root and requires semantic native mutations to fail solver obligations;
parser/compiler rejection alone does not count as a negative control. Receipts
bind native source, adapter, contracts, generated code, pinned verifier, and
individual logs with hashes and a final input-stability check.

```sh
python3 verification/postings/generate.py --check
python3 verification/postings/test_generate.py
python3 verification/postings/run.py --output target/verification/postings
```

The native allocation-failure and callback-rejection tests exercise real
destination cleanup and unchanged rows/indexes. They complement these proofs;
they do not discharge the unsafe primitive assumptions above.
