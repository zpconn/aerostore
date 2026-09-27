# Native predicate dependency capture

This campaign proves the dependency-capture loop extracted from the actual
`OccTable::index_lookup`. It does not add an alternate runtime implementation.

```sh
python3 verification/predicate_capture/generate.py
python3 verification/predicate_capture/test_generate.py
python3 verification/predicate_capture/run.py
```

For arbitrary **unique requested buckets** and initially unique recorded dependencies,
successful capture records every requested `(index, bucket, stamp)` with a stamp
strictly older than the transaction start. Existing records remain unchanged;
new records can only describe the queried index, a requested bucket and its
observed stamp. The number of added records is at most the number of requested
buckets. The native bucket selector canonicalizes duplicate input keys before
capture; uniqueness of its returned bucket list is now an explicit premise.
These properties hold independently of how many row candidates the query finds,
including zero. A repeat lookup with a changed stamp or a too-recent stamp sets
the sticky conflict flag and returns serialization failure. Primitive index
errors may retain a successfully captured prefix without setting that flag.

The restricted adapter retains the native branches, stamp arguments, dependency
record construction, errors and sticky-conflict assignments. It lowers the
`for` loop to an indexed loop, and the standard iterator search to the proved
`find_read_prefix` helper. The native loop freezes the initial dependency length
once and searches only that prefix. The helper proves the first matching entry
within the prefix, or absence throughout it, and verifies the slice bound. A
native branch uses the original full-vector iterator when the current vector
length still equals the frozen length, and the bounded slice otherwise. The
adapter retains both arms. A checked assertion establishes that the full-vector
arm searches exactly the frozen prefix, so repeated queries need no extra inner
iterator bound. The condition is evaluated inside each bucket iteration; after
an append the bounded arm excludes the new suffix. A
separate lemma proves that appended entries cannot match the current bucket:
they came from earlier positions in the unique requested list. The initial
prefix remains unchanged, so skipping this suffix preserves the search decision.
The proof includes the current production scalar comparison body.
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

The adapter checks the entire native `transactional_bucket_ids` body, its fixed
4096-bucket domain, and all three configuration branches. Every successful path
must return a canonicalized list; early returns or omitted sorting/deduplication
fail this source check. This is a reviewed source-routing boundary, not a proof
of the native mapping arithmetic. In the default configuration, the standard
library's `sort_unstable` plus `dedup` semantics remain an explicit trusted
premise. For the two optional verified selectors, proof-only callers invoke the
actual extracted sort/bitmap kernels and derive `unique_buckets` from their
canonical-output postconditions. Their executable bodies are checked in the
same full artifact. No Lean claim is imported as an axiom. This does not expand
the transaction proof to the optional retry-diagnostics feature.

The generic capture/validation composition explicitly carries the uniqueness
premise. The lifecycle and indexed-read compositions construct one-element
lists and prove it locally. A small repeated-bucket counterexample proves why
the old arbitrary-list contract cannot be retained for the optimized loop.

Eleven mutations must reach Verus and fail verification: omitted snapshot or
repeat validation, omitted sticky-conflict assignment, wrong stamp/bucket/index
in the recorded dependency, an omitted dependency, zero-length or truncated
prior prefixes, removal of the unique-bucket premise, and selecting the full
search after an append. The last control checks the exact-prefix search bound;
a full-vector search could still be transactionally correct while doing the
extra work this optimization removes. Adapter tests also reject
early guard release, post-capture dependency clearing, changed signatures and
proof bypass syntax, selector bypasses and unreviewed configuration changes.
Each of eight required proof roots is checked separately. Receipt
validation requires the exact command, source and tool hashes, named roots,
mutations and actual verifier logs; a success boolean alone is insufficient.

This result composes with the [native predicate algorithms](../predicate/README.md)
under their named primitive contracts. It does not prove the full indexed query
or a transaction-history refinement. Lean's candidate-completeness theorem and
the TLA+ scenarios remain independently checked models, not imported axioms.

This changes the production OCC token stream intentionally. The
[diagnostic baseline review](../retry_diagnostics/capture_prefix_review.md)
records the exact transition separately; diagnostic normalization may not erase
the optimization. All dependent receipts bind the native selector and extracted
kernel adapter/specification as well as the capture source and proof artifact.
