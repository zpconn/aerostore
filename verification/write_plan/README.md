# Native write planning

This campaign checks two production methods from `OccTable` through a restricted
source adapter. It adds no production instructions or data structures.

`final_write_indices` is proved for any finite pending write set. Its result
contains exactly the last pending write for each row, omits no row, contains no
duplicate index and follows increasing row ID order. The one-row corollary
allows any number of superseded pending writes and selects the last one.

`index_changes` retains both native loops, fallible base/new pointer resolution,
key projection, the unchanged-key branch and both prevalidation calls. For one
selected write and one bound index, successful output is exactly empty when
the keys agree or the exact before/after change when they differ. Both emitted
keys satisfy the prevalidation primitive's encoding condition. Creation,
movement, deletion and unchanged values are covered by the same Option-valued
key contract and executable consistency witness.

The ordered-map boundary states standard empty-map, overwrite-by-key and sorted
value-enumeration behavior. The final-write result is derived from those
operations. This campaign does not prove the Rust standard library's BTreeMap
implementation. The storage boundary is the same `OrdinaryStorage` projection
used by commit publication; key extraction and encoding checks are explicit
primitive assumptions. Native allocation ownership, pointer validity, callback
purity, acquisition of the guards and preservation across concurrent execution
remain separate obligations. The executable `Memory` instance establishes
consistency of the row/key contracts, not physical shared-memory refinement.

Run `python3 verification/write_plan/generate.py --check`,
`python3 verification/write_plan/test_generate.py`, and
`python3 verification/write_plan/run.py --output target/verification/write-plan`.
The runner checks the pinned verifier, the complete composed module, each
required root and semantic mutations. Receipts bind all inputs, artifacts and
logs by hash, and reject source changes during verification. P1 and whole-engine
refinement remain explicitly incomplete.
