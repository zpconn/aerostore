# Native planning, base validation and publication

This Verus composition computes a write plan from the transaction's pending
writes, validates its base against the guarded row image, and passes the result
to the existing publication/completion proof. It uses one physical row and one
registered index, with any nonempty finite sequence of pending writes to that
row. The final write changes the indexed key; native unchanged-key extraction
has separate `write_plan` evidence and native transaction tests.
The joined proof requires a nonzero physical base pointer. That base may carry
an empty logical value, so creation in a preseeded vacant slot and deletion are
included; insertion from a physically empty chain is outside this composition.

The incoming state supplies retained immutable values, distinct private
allocations, field/guard authority and a coherent acquired row/posting image.
It does **not** supply the final-write indices, an extracted key-change plan,
current-head/base equality, zero base deletion ID or the publication result.

The actual `final_write_indices` loop establishes complete last-write selection
and eliminates superseded writes. The actual `index_changes` loop supplies the
selected row's before/after keys and prevalidation. The actual
`has_write_base_conflict` loop establishes that the selected base is the current
undeleted head. These results establish `commit_data::planned` on the same
`storage.rows` object later passed to ordinary publication.

Successful completion has the exact final image, posting set and returned
report for the last pending write, consumes the registration, and publishes the
actual fresh shared-clock stamp. Data failures preserve old data or poison it.
Planning/base-validation failures in this harness describe the interval before
native caller cleanup; they make no assertion that the complete native commit
returns with an open registration. In particular native serialization failure
attempts abort, which this composition has not yet joined.
The exact whole-image result describes the harness projection. Native success
also recycles superseded private writes before finishing. That omitted cleanup
can release those addresses for reuse; the proof does not claim they remain in
the physical heap unchanged after the native API returns. Joining cleanup needs
a projection that forgets retired private allocations while preserving the
published row and chain.

## Concurrent interpretation and remaining boundary

Native planning precedes guard acquisition. A separate field-frame lemma shows
that key extraction survives head/deletion changes while waiting when its
retained base/new value fields remain immutable. Base validation then checks
the acquired image. The proof does not assume that all database state remains
unchanged during acquisition.

The main harness operates on that acquired projection. It does not execute
registration, index/partition acquisition, predicate/row-read/owner validation
or native caller cleanup. The omitted validation checks must have accepted on
the represented native path; composing their actual decisions remains open.
Other explicit assumptions include physical correspondence of row/posting/stamp
views, native pin/allocator authority, callback/key projection, ordered-map
library behavior, lifecycle acquired-state framing, no-wrap and weak memory.
The typed models and adapters remain trusted translation boundaries.

The source adapter embeds the exact current planning, admission and completion
modules. The latter checks the native driver prefix and suffix, including its
actual operation order. The handwritten composition order is checked too.
This is an interval refinement under enumerated premises, not a replacement
engine, whole-commit theorem, full P1 or arbitrary transaction-history proof.

## Checks

```sh
python3 verification/planned_commit/generate.py --check
python3 verification/planned_commit/test_generate.py
python3 verification/planned_commit/run.py --output target/verification/planned-commit
```

The runner checks the complete crate, five individual roots and twelve semantic
controls. The controls corrupt final selection and record pointers, bypass base
validation, accept conflicts/storage failures, or remove required allocation,
posting or value-frame premises. Each must fail a proof obligation; compilation
errors and timeouts do not count. A two-write live input witness uses distinct
private versions. Source, toolchain, commands and logs are fingerprinted.
These proof and test additions introduce no production instructions or locks.
