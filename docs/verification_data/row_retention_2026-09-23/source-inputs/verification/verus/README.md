# Production kernel verification with Verus

This pilot verifies the actual stable Rust bodies in
[`aerostore_verified/src/lib.rs`](../../aerostore_verified/src/lib.rs). The
engine uses the stamp helper by default and can select either bucket kernel
through its explicit candidate feature. Its standard sort remains the baseline
until the performance gate selects an improvement.

The proofs establish:

- Both bucket implementations return exactly the input's unique bucket IDs in
  increasing order, with every result below `bucket_count` and length bounded
  by both the input length and bucket count.
- An invalid input returns its **first** invalid value, including invalid values
  after otherwise valid inputs. No caller precondition restricts the input.
- Sorted exact membership and first-invalid semantics uniquely determine the
  result; `canonical_result_unique` proves the implementations' result contracts
  equivalent, including their error branches.
- `stamp_precedes_snapshot` is precisely strict inequality, including equality,
  zero, and the largest representable value.

Every loop has a checked invariant and decreasing bound. Both bucket function
bodies are verified; neither is replaced by an external specification. The
insertion implementation uses `push`, indexed shifts, and a final indexed write.
It deliberately avoids `Vec::insert` because the separately pinned Aeneas model
did not match insertion semantics during the bridge investigation.

## Reproduce

On x86-64 Linux with Python 3 and rustup available:

```sh
python3 verification/verus/install.py
python3 verification/verus/generate.py
python3 verification/verus/test_generate.py
python3 verification/verus/run.py --output target/verification/verus
cargo test --offline -p aerostore_verified
```

The installer downloads the checksum-pinned release and its required compiler
under ignored `target/verification-tools/`; it does not replace the production
Rust toolchain. `toolchain.json` records release/commit, compiler, target, solver,
archive checksum, and important installed artifact checksums. A normal build
of the production crate does not need Verus or its compiler.

`run.py` verifies all roots with `--no-cheating`, then changes a temporary copy
to omit bucket zero and requires failure of a loop invariant. A second mutant
changes the stamp comparison to `<=` and must fail its fixed postcondition.
Syntax errors, missing tools, timeouts, stale adapters, and zero verified roots
are failures. The receipt starts in a nonpassing state and records commands,
input hashes, diagnostics, and completion status.

The current successful run checks 19 function/loop obligations. The omission
mutant fails the exact-membership invariant; the stamp mutant fails the strict
inequality postcondition. Three Rust tests cover boundaries and exhaustive short
input combinations; five adapter tests reject executable annotation suffixes,
changed/disabled or comment-counterfeited roots, external source, dropped tests,
and weakened contracts.

## Connection and trust boundary

Production contains proof-only comments. `generate.py` exposes those comments
inside Verus's macro and inserts **frozen** public contracts from its interface
map. It then removes its proof blocks and return binders and checks that the
remaining body text is exactly the production body. The source is also the
input to the separate Rust-to-Lean extractor.

This small adapter is part of the audited trusted boundary, not a proved Rust
compiler. Its limited annotation grammar permits balanced `proof` blocks,
loop invariants/decreasing bounds without executable statement delimiters, and
simple ghost captures. Ordinary comments are lexed separately so they cannot
counterfeit a function definition. This pilot deliberately rejects executable
imports, modules, unsafe code, macros, and string/character literals instead of
claiming to parse arbitrary Rust. It rejects executable suffixes, configuration
changes, public-contract changes, and common proof bypasses. The outer acceptance gate
freezes this adapter, the specification/proof library, runner, and tool pins.
Verus additionally rejects proof assumptions through `--no-cheating`.

The result remains relative to Verus, its SMT solver, the pinned `vstd` models
for standard slices/vectors, proof erasure, rustc/LLVM, and the platform. Vector
allocation uses ordinary Rust behavior; these proofs do not establish allocator
availability, a fallible allocation contract, or operation latency. The
experimental Verus safe-API checker was tried separately and rejected an
imported `vstd::std_specs::vec::vec_index` precondition. That check is not reported
as passing. This crate uses ordinary safe Rust and introduces no proof-only
preconditions on its public functions.

This verifies bucket canonicalization and a scalar publication decision. It
does **not** prove transaction publication, serializability, weak memory,
process-shared mmap ownership, storage reclamation, WAL, or recovery. Those
engine implementations and their assumptions remain frozen outside this pilot's
optimization boundary. TLA+ and Lean results are separate evidence; no theorem
is imported from either into these Verus proofs.
