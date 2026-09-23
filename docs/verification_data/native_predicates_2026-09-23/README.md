# Native predicate and posting verification checkpoint

The complete **29-check component pilot passed** against stable fingerprints
for 292 input files. The parent implementation is accepted commit
`8d9e9f409a2d215de25e57b7a6c847495d1bf6a4`. This checkpoint adds proofs, models,
regressions and evidence checks; it changes no production Rust path.

The [summary](summary.json) and [complete receipt](formal/report.json) record
the result. The receipt deliberately retains `full_P1_complete = false`,
`whole_engine_verified = false`, and `promotion_eligible = false`.

## New coverage

| Campaign | Independently checked roots | Rejected semantic mutants | What is established |
| --- | ---: | ---: | --- |
| Native predicate data | 11 | 13 | Read/write bucket coverage, conflict decisions and affected stamp updates |
| Native dependency capture | 4 | 7 | Empty-query dependencies, repeated-read checks, provenance, uniqueness and growth bound |
| Capture/validation composition | 5 | 6 | Exact field translation and rejection of overlapping late publication stamps |
| Posting preparation/rollback/removal | 14 | 10 | Exact owned additions/removals, restoration or poison, unrelated-posting preservation |
| Cached/fallback skiplist detachment | 4 | 4 | All-lane detachment before retirement, conditional pinned-content frame |

The 38 new roots are theorem/function roots, not 38 distinct production
operations. The composition campaign imports the exact generated capture and
validation modules. The adapters are restricted, reviewed source transformations;
they are not a general concurrent Rust extraction pipeline.

Lean checked 20 required roots, including six new abstract predicate-history
and own-write overlay roots, with fresh Rust extraction for the existing kernel
bridge, independent kernel replay, a rejected forged theorem, and nine semantic
mutations. Four of those mutations concern the new abstract contracts; they
are not Rust-extraction mutations. No Lean theorem is imported unchecked into
Verus.

TLC passed all 74 declared cases: 29 complete finite searches, 23 intended
safety counterexamples, three intended liveness counterexamples and 19 positive
witnesses. The new predicate model contributes 25 cases. Witness searches and
deliberately broken protocols are classified separately from completed safety
searches. Its [receipt](formal/tla/report.json) identifies exact inputs; the
[retained TLA evidence](../../../verification/tla/evidence/report.json) includes
inspectable traces for the same current campaign.

## Native validation and performance

The final pilot passed 260 core regression tests and 34 transaction/index/query
tests in each of the default, sort and bitmap configurations. Its three
Extended Crucible runs each used eight families, one cycle, four workers and
128 MiB, covering all replay phases and native concurrency contracts. These
are integration smoke runs, not sustained throughput or p99 measurements.
The existing seven-case production-lock campaign and weakened-Acquire negative
control also passed from fresh builds.

Six new deterministic native tests exercise capture followed by creation/key
move, rejection before WAL acceptance, separate index identities sharing a
bucket number, real bucket collisions and own-write overlays, crossed empty
predicates with disjoint writes, and old/new publication stamps. Separate
[native diagnostic evidence](native/report.json) records their initial run,
20 existing indexed tests, and five isolated runtime mutations. Each mutation
reaches its intended assertion: missed conflict, false conflict across indexes,
omitted own row, both crossed predicates publishing, or an unstamped old key.
The exact single-file mutation patches and logs are retained beside the report.
`diagnostic_runner.py` is the original local diagnostic script; it is not a
portable CI gate. The ordinary production tests are mandatory in the pilot.

The existing real cached-window/tall-node/pinned-retirement/shorter-reuse test
also passes; its [command and source receipt](native/cached-window-native.json)
and log are retained.

The [production comparison](production-equivalence.json) checks all 118 tracked
Rust/Cargo inputs against the accepted implementation. The only native source
addition is a complete appended `#[cfg(test)]` module. No locks, atomics,
allocations or proof bookkeeping were added to production. No new throughput
benchmark or performance improvement is claimed. The earlier engineering
acceptance and its inconclusive automatic p99 comparison are unchanged.

## Remaining boundary

These are conditional implementation proofs. Actual guard ownership,
registry/key correspondence, snapshot and publication-clock behavior, raw-index
candidate completeness, row/MVCC publication, unsafe pointer validity and
epoch reclamation remain native obligations. The posting algorithms require
the caller to establish absent/distinct destinations and preserve ownership.
Their removal-error contract allows the target to be removed before error;
the poison obligation is essential. Detachment establishes retirement's
precondition under lane/search/epoch contracts and does not prove retry
termination or safe allocator reuse.

Capture and validation are composed over two represented states. Proving those
states correspond to real interleavings, and composing all native row/index
and cleanup transitions into legal transaction histories, remain the next P1
obligations. General serializability and complete memory/durability verification
are not claimed.

This work intentionally expands the verification boundary. The independent
checker from accepted commit `8d9e9f4` correctly
[rejected the changed boundary](accepted-boundary-rejection.json) before local
rebaselining. The [proposed boundary](proposed_boundary.json) supports the fresh
local component run; it cannot certify itself as an optimization against the
old frozen contract. A future promotion must use an independently reviewed Git
baseline. No automatic performance promotion occurred.

## Reproduce

From the repository root with the pinned tools installed:

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot
```

The runner verifies current source, fixed roots, exact verifier commands,
semantic negative controls and log/tool hashes. Missing or stale artifacts,
unrelated mutation failures and modified sources fail the gate. The archived
receipts describe this checkpoint; normal checks write new evidence under
`target/verification/`. `artifact_sha256.json` fingerprints the retained archive
without making a stronger verification claim.
