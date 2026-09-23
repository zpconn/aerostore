# Native guard identity and acquisition

This campaign checks the actual `transactional_try_lock_bucket`,
`acquire_index_bucket`, and `acquire_index_locks` bodies. The restricted adapter
preserves bucket selection, bounds/error checks, bounded retry control flow,
and the guard collection loop. The registry maps a local binding ordinal to a
shared index header; each returned guard carries its physical arena, header,
and bucket identity. It proves exact requested coverage on success and keeps
different registered indexes distinct even when their numeric buckets match.

The bridge to the predicate key generator lives in
[publication_slice](../publication_slice/README.md). Its registry relation
explicitly requires inverse offset/binding correspondence and a common arena;
matching registry lengths alone would not establish identity.

## Boundary

The contract of the lowest `try_lock` operation still assumes ownership of the
selected mutex. Pointer/arena resolution, immutable registry identity, native
mutex exclusion, Acquire/Release visibility, allocation, and Rust guard Drop
remain primitive obligations. The guard record is a logical projection of the
native resource, not a new runtime object or a standalone proof of linear
ownership. Success proves which guards were acquired; this collection proof
alone does not prove their subsequent lifetime. The source-bound commit
orchestration checks phase/release order separately, with an early-release
negative control. Failure cleanup relies on native RAII. The bounded retry loop
can reject on contention and does not promise eventual acquisition.

## Checks

```sh
python3 verification/guards/generate.py --check
python3 verification/guards/test_generate.py
python3 verification/guards/run.py
```

The runner checks five named roots and six deliberately wrong selections or
dropped-guard variants. Each negative must fail a proof obligation, not parsing
or compilation. The central pilot authenticates current native/adapter/contract
inputs, exact commands, pinned verifier artifacts, roots, mutations and logs.
No proof state or additional synchronization enters release builds.
