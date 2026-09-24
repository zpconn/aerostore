# Native predicate dependency capture

This campaign proves the dependency-capture loop extracted from the actual
`OccTable::index_lookup`. It does not add an alternate runtime implementation.

```sh
python3 verification/predicate_capture/generate.py
python3 verification/predicate_capture/test_generate.py
python3 verification/predicate_capture/run.py
```

For arbitrary requested buckets and initially unique recorded dependencies,
successful capture records every requested `(index, bucket, stamp)` with a stamp
strictly older than the transaction start. Existing records remain unchanged;
new records can only describe the queried index, a requested bucket and its
observed stamp. The number of added records is at most the number of requested
buckets. Duplicate requested buckets do not create duplicate dependencies.
These properties hold independently of how many row candidates the query finds,
including zero. A repeat lookup with a changed stamp or a too-recent stamp sets
the sticky conflict flag and returns serialization failure. Primitive index
errors may retain a successfully captured prefix without setting that flag.

The restricted adapter retains the native branches, stamp arguments, dependency
record construction, errors and sticky-conflict assignments. It lowers the
`for` loop to an indexed loop, and the standard iterator search to the proved
`find_read` helper. It includes the current production scalar comparison body.
The native prefix and remainder are checked exactly for the declared interface:
bucket guards precede capture, raw candidate lookup follows capture under the
guards, then guard release precedes row materialization. Those source checks
detect unsupported edits; they are **not proofs of the remainder**.

The proof represents only transaction ID, dependencies and the conflict flag.
Index offsets are opaque identities widened from native `u32` to `usize`; only
identity comparison is performed. Initial uniqueness is a caller obligation;
the operation preserves it. The standard vector model assumes allocation
success. The abstract `CaptureIndex` gives stable stamp observations for held
buckets, conditional on actual guard ownership and Acquire/Release visibility.
It does not assume that an immutable proof receiver exclusively owns the native
database. Native index binding/ownership, raw skiplist completeness, MVCC reads,
clock exhaustion and shared-memory safety remain separate obligations.

Seven mutations must reach Verus and fail verification: omitted snapshot or
repeat validation, omitted sticky-conflict assignment, wrong stamp/bucket/index
in the recorded dependency, and an omitted dependency. Adapter tests also reject
early guard release, post-capture dependency clearing, changed signatures and
proof bypass syntax. Each required proof root is checked separately. Receipt
validation requires the exact command, source and tool hashes, named roots,
mutations and actual verifier logs; a success boolean alone is insufficient.

This result composes with the [native predicate algorithms](../predicate/README.md)
under their named primitive contracts. It does not prove the full indexed query
or a transaction-history refinement. Lean's candidate-completeness theorem and
the TLA+ scenarios remain independently checked models, not imported axioms.
