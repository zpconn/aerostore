# Guarded native write admission

This campaign translates the full production `OccTable::has_write_base_conflict`
loop over its supplied final-write indices. It proves that a successful `false`
result means every selected base is the current row head and every nonempty base
has zero `xmax`. A successful `true` result means at least one selected base
violates that condition. Slot lookup and row resolution remain fallible, and the
empty-base creation branch remains in the translated program.

`GuardedRead` extends the existing `OrdinaryStorage`/`PublicationStorage` traits.
The validator and publisher therefore use the same row-image projection. Its two
new primitive contracts expose only a head load and an `xmax` load. No primitive
contract supplies the validator's conclusion. Native partition ownership and
the mapping from guarded field loads to this stable image remain assumptions.

The `fresh_private` predicate records the remaining fresh-allocation facts,
including creator identity, zero fresh `xmax`, alias exclusion and rank. It does
not assume that the selected base equals the head or has zero `xmax`.
`validated_base_prepares_publication` derives the existing publisher precondition
from those allocation facts and successful validation. `validation_establishes_plan`
then combines exact before/after keys, row identity and existing posting coherence
to establish `commit_data::planned` and `ordinary_matches`. The source-bound
planning campaign establishes those extracted-key facts separately.

The executable witness uses the existing finite row-memory implementation and
actually invokes the validator for a valid replacement, a deleted base, a stale
head and an empty creation. These are consistency witnesses for the contracts;
they are not an unsafe native allocator proof.

Run from the repository root:

```sh
python3 verification/write_admission/generate.py --check
python3 -m unittest discover -s verification/write_admission -p 'test_*.py'
python3 verification/write_admission/run.py --output target/verification/write-admission
```

The pinned Verus campaign requires four roots and eleven semantic negative
controls. Each control must produce a proof-obligation failure, and the receipt
hashes source inputs, generated artifacts, verifier binaries and output logs.
The declared scope is guarded admission for the ordinary commit path. It does
not prove lock acquisition, arbitrary concurrent histories, weak memory, native
allocator ownership, whole-engine correctness or completion of P1.
