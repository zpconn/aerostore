# Lean and the Rust extraction bridge

This package proves properties of Rust bodies extracted from `aerostore_verified/src/lib.rs`. Charon reads the real Rust compiler representation; Aeneas produces `AerostoreProofs/AerostoreVerified.lean`; the Lean kernel checks separate proofs against that generated module. `scripts/check_lean.py` regenerates the module on every run and rejects stale or opaque local definitions.

The checked roots are listed in `roots.json`. `Contracts.lean` defines the abstract specification; `BridgeContracts.lean` defines the required claims about the extracted functions. The audit explicitly imports both contract modules before proof modules and checks each required theorem against its fixed type.

The completed bitmap and insertion-sort theorems cover **all representable inputs in the pinned Aeneas model**, including invalid inputs: each terminates with either the exact input set in strict ascending order, or the first out-of-range bucket. The bitmap proof includes initialization, prefix marking, ordered enumeration and validation; the sort proof includes search, element shifting, insertion and prefix accumulation. Both cover safe machine increments and indexing. The required `rust_bitmap_production` and `rust_sort_production` corollaries specialize the results to the native caller's fixed 4096 buckets, cover all inputs and both result cases, and prove successful output has at most 4096 elements. `rust_bucket_implementations_equivalent` proves the two actual extracted functions return exactly the same complete result. The scalar theorem checks the publication stamp comparison. Separate mathematical lemmas establish canonical result uniqueness and monotonic rejection by the abstract clock rule.

These are component proofs. They do not prove native transaction serializability, publication memory ordering, vacuum safety, mmap validity, crash recovery, or the whole engine. The scalar helper is used in the native transaction path; bucket implementations are selectable production candidates under `verified-buckets-sort` and `verified-buckets-bitmap`. The default bucket implementation remains unchanged.

## Parameterized predicate histories

[Predicate.lean](AerostoreProofs/Predicate.lean) adds separately scoped mathematical
proofs over arbitrary natural-number bucket IDs, row IDs, transaction starts, and
finite publication histories. Publication replays the actual overwrite operation;
under an explicit fresh-clock invariant it preserves stamp monotonicity and every
prior affected bucket's publication lower bound. An accepted dependency therefore
excludes an overlapping publication at or after the reader's start, including a
writer whose own start was older. Stamp equality and strict snapshot ordering are
separate audited obligations.

The materialization theorem establishes exact predicate results after overlaying
local inserts, moves and deletes, provided the raw candidate set covers matching
snapshot rows. It includes all locally written row IDs before filtering and removes
rows whose staged value no longer matches. Concrete checked examples exercise an
own insert absent from raw candidates, an empty read rejected by a later creation,
and a conservative retry for a bucket collision.

These are **abstract contracts, not extracted transaction proofs**. The native
skiplist's candidate completeness, stable row identity and pinned MVCC retention,
lock ownership, clock reservation/exhaustion and memory visibility are remaining
implementation obligations. No Lean theorem is imported into Verus as an unchecked
axiom. The four `predicate_*` negative controls mutate the mathematical definitions
in isolation and require the unchanged proof module to fail: ignoring a changed
stamp, accepting an equal start stamp, dropping publication, and omitting own-write
candidates. They are reported separately from the extracted-Rust mutations.

## Lifecycle and shared-clock histories

[Lifecycle.lean](AerostoreProofs/Lifecycle.lean) adds six audited mathematical
roots over an inductively reachable finite history. Registration reserves a
fresh identifier; snapshot records the relevant active writer; ending a writer
precedes reservation of its publication label. The invariant derives freshness
and rejection of affected reads, including an older-writer witness. A separate
machine-arithmetic root makes the no-wrap condition explicit. These definitions
and theorem statements are frozen together; changing them requires boundary
review, not merely rebuilding the proofs.

Publication chronology means reservation order. Actual stores to disjoint buckets
can occur out of that order; connecting the earlier fresh-publication history
contract to those stores requires a commuting-updates/native-history argument.
Neither this abstract theorem nor the finite TLA model supplies it. The four
new semantic controls fail when reservation stops advancing, a writer's start
ID is used as its publication label, publication precedes ending, or wrapping
arithmetic is admitted. There are now 26 audited roots and 13 semantic controls
across the extracted functions and separately labeled abstract contracts at that
lifecycle checkpoint.

## Complete-or-retry indexed lookup

[QueryCompleteness.lean](AerostoreProofs/QueryCompleteness.lean) adds eight audited
roots that derive historical candidate coverage from coherent before/after key
updates, maintained postings, overlap-local stamp histories and reachable
lifecycle chronology. A matching historical row missing from current postings
forces a visited bucket to reject. Private writes and arbitrary extra candidates
are included in exact successful-result equality. The native MVCC predicate is
covered by a separate two-version selection lemma; retained raw chains and their
native concurrent-history mapping remain open. The [contract description](query_completeness.md)
details these premises without assuming the desired candidate completeness.

Seven new semantic controls exercise posting coverage, creator/deleter visibility,
private candidate union/overlay and stamp ordering. Together there are 34 audited
roots and 20 semantic controls. The complete new definition/proof file is frozen,
including the query semantics and all audited statement aliases.

## Running

On Linux x86-64 with Python 3.12+, `rustup`, a C linker, Git, tar and zstd:

```sh
python3 verification/bridge/bootstrap.py
python3 scripts/check_lean.py --output target/verification/lean.json
```

Bootstrap is an explicit networked operation. It authenticates pinned release/source archives, installs the exact Rust/Lean tools under `target/verification-tools`, builds the Aeneas-pinned Charon commit, and obtains the Lean dependencies recorded in `lake-manifest.json`. It does not change the user's default Rust or Lean toolchain. The normal check is offline once provisioned and fails when tools or dependencies are missing.

After a deliberate Rust source change, regenerate and review the artifact before checking:

```sh
python3 scripts/check_lean.py --refresh-generated --output target/verification/lean-refresh.json
```

The gate records exact source fingerprints before and after checking, tool versions/hashes, proof roots, transitive axioms, commands and results. It writes a fresh incomplete report before invoking tools, replaces it atomically at completion, rejects changing sources, and permits output only under `target`. Standard accepted Lean axioms are `propext`, `Classical.choice`, and `Quot.sound`; no custom theorem axiom, `sorryAx`, or native evaluation axiom is permitted. The project build cache is removed before compiling current sources. The pinned `leanchecker --fresh AerostoreProofs` then replays every project and imported declaration into an empty kernel environment; this additional check takes several minutes.

Negative checks mutate the Rust source in isolated scratch directories, run extraction again, and require the unchanged proofs to reject: accepting a stamp equal to the transaction ID, clearing a presence bit, writing the wrong insertion value, and accepting a bucket equal to the array bound in either algorithm. Another negative check injects an ill-typed, axiom-free declaration using `debug.skipKernelTC` and requires kernel replay to reject it. Normal elaboration and axiom listing alone can accept that forgery; even the compiler's `-t0` option did not reject it in this pinned version. Extraction/compilation infrastructure failure is not accepted as a successful negative check.

## Trusted boundary

The proof is about Charon/Aeneas's translation of this Rust subset. The compiler, extractors and their library models remain trusted; this is not a proof of the translators or rustc. The Aeneas binary and its 500 shipped Lean source/compiled model files are checked against the authenticated pinned release archive on each gate run. Lean, Lake, leanchecker and their Lean shared-library hashes are fixed to the authenticated release bytes. Charon is built by the explicit trusted bootstrap from the checksum-pinned source archive and Cargo.lock; its produced binary hashes are recorded, rather than claiming a portable binary reproducibility theorem. Fresh replay provides another check with the same Lean kernel, not an independently implemented proof checker.

The kernels use Aeneas's `Vec::push`, checked indexing and indexed mutation models. These represent logical capacity and successful allocation. The general theorems do not rule out Rust byte-capacity panics or allocation failure for arbitrary bucket counts: Rust's `Vec` byte limit is stricter than the model's logical `usize` length bound. The production corollaries bound successful output to 4096 `usize` elements (32768 element bytes on the pinned x86-64 target); the bitmap initializes exactly 4096 boolean flags. These sizes fit Rust's byte-capacity limit. Ordinary growth at that scale still assumes successful allocation; allocator implementations and OS allocation aborts are not verified. Extraction does not model destructors or arbitrary unsafe memory operations.

During setup, the pinned Aeneas `Vec::insert` model was found to replace an existing element and reject end insertion, which differs from Rust. The production sort candidate was rewritten to use explicit `push`, shifts and a final indexed write; it does not depend on that model. This is why library conformance is an explicit obligation even when extraction and kernel checking succeed.

TLA+, Lean and Verus currently provide separately scoped checks. There is no automatic TLA+-to-Lean refinement proof or import of Lean theorems into Verus. The bridge here is **actual Rust → Charon → Aeneas → Lean**, with Verus independently checking the same Rust source through its own adapter.
