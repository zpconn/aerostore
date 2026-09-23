# Lifecycle, snapshot, and predicate-publication verification checkpoint

The complete **37-check component pilot passed** against stable
fingerprints for 340 inputs. Its parent is accepted commit
`c5d27d280630d901b3221f836bbacccaf8c11349`. This checkpoint adds proofs, finite models, test-only hooks,
regressions, and evidence checks; it changes no production Rust path.

The [summary](summary.json) and [complete receipt](formal/report.json) retain
`full_P1_complete = false`, `whole_engine_verified = false`, and
`promotion_eligible = false`. These results expand the verified components;
they do not establish whole-engine verification.

## New native proof coverage

| Campaign | Independently checked roots | Rejected semantic mutants | Scope |
| --- | ---: | ---: | --- |
| Native guard identity/acquisition | 5 | 6 | Exact arena/header/bucket identities and acquisition coverage |
| Native lifecycle/snapshot/shared clock | 20 | 17 | Registration, snapshot bounds, retention, deregistration and reservation ordering |
| Guard/lifecycle/predicate joins | 7 | 6 | Native lock-key coverage, identity and publication invalidation joins |
| Shared-history native-call scenario | 13 | 13 | Actual registration/snapshot/capture/end/publication/validation on one represented clock |

These are **45 new named roots and 42 semantic mutants**.
Roots include supporting lemmas and bridge methods, not just production
operations. The existing commit-driver campaign also rejects 1
new guard-release ordering mutation. Its other existing controls remain required.

The shared-history scenario calls the exact extracted native reader registration,
transaction snapshot, dependency capture, writer deregistration, index-stamp
publication, and conflict validator. Its checked bridge routes registration and
publication through the same represented lifecycle clock and reservation history.
The reader ID, captured dependency and later publication stamp are produced by
those calls; no numeric freshness conclusion is supplied. The returned snapshot
must contain the active older writer. A finite live-state/success-reply witness
covers empty creation and key movement, an initially safe read, and an invalidating
publication. This is consistency evidence, not a native heap construction or a
guarantee that fallible primitive calls always succeed.

The guard campaign preserves actual arena/header/bucket identity through native
acquisition. Its join with native lock-key generation establishes coverage under
explicit registry correspondence. The commit-driver proof checks that predicate
guards remain retained through deregistration and stamp publication. Restricted
source adapters reject unsupported changes; they are not a general concurrent
Rust extraction pipeline.

Lean checked **26 required roots** and **13 semantic controls**,
with fresh extraction of the existing Rust kernels, independent kernel replay,
and rejection of a forged theorem. The lifecycle additions are abstract contracts,
not extracted concurrent Rust proofs; no Lean theorem is imported unchecked into
Verus. TLC passed **94 declared cases**: 35 completed finite
searches, 29 intended safety counterexamples, 3 intended
liveness counterexamples, and 27 positive witnesses. Counterexample and
witness cases are not described as exhaustive safety proofs. Inspect the
[Lean receipt](formal/lean.json) and [TLA receipt](formal/tla/report.json).

## Native validation and performance

The final pilot passed **262 core regression tests**. The default, sort,
and bitmap transaction/index/query configurations and their Extended Crucible
smoke runs also passed; exact commands, counts and outputs are retained under
[formal/](formal/). These are integration smoke runs, not sustained throughput or
p99 measurements. The existing production-lock model campaign and its weakened
Acquire negative control also passed.

Separate [native schedule diagnostics](native/receipt.json) retain
3 successful current-source selections and 5 intended
runtime-assertion failures. The new tests pause real empty-creation and key-move
transactions around deregistration/stamp cuts, check that lookup and a prior empty
query's commit encounter the held predicate guard, and check fresh-reader success.
Existing registration-gap and older-writer schedules are reused. Each native
mutation reaches its declared assertion; compile failures and timeouts do not
count as evidence.

The diagnostic is **optional archived evidence**, not a mandatory formal gate.
It uses the recorded parent crate archive plus the current two native test/hook
files; its original receipt does not fingerprint arbitrary unrelated uncommitted
workspace files. This archive adds a complete source-tree manifest and reproducible
[current fixture](native/current-fixture.patch) and per-mutant patches with hashes
in [mutation-manifest.json](native/mutation-manifest.json). Reconstruct the parent
with the recorded `git archive` command, apply the fixture patch, then the selected
mutant patch, and use the original receipt's command with local source/target paths.
The scripts and original receipts retain their run-time paths for provenance.

The [production comparison](production-equivalence.json), with its retained
[checker](production_equivalence.py), checks 118 tracked Rust/Cargo
inputs against the parent. The differences are complete `#[cfg(test)]` modules
and the exact two test-only hook declarations/calls; production regions remain
equivalent. No production locks, atomics, allocator behavior or runtime proof
bookkeeping were added. **No sustained performance comparison or improvement is claimed.**

## Remaining boundary

The scenario uses a **controlled schedule at the acquired-input boundary**.
The lifecycle lock contract frames its represented metadata across acquisition;
a real blocking mutex alone does not justify that frame. Other threads may change
slots/history while a native caller waits. That acquisition-interference refinement
remains open and is explicitly flagged false. This scenario excludes those
unmodeled slot transitions, while its clock contracts allow intervening allocations
and stale loads.

Real mutex/RAII ownership, physical heap/arena and registry correspondence,
weak-memory behavior, raw candidate completeness, row/MVCC publication, unsafe
pointer validity and reclamation remain native obligations. The scenario supplies
raw guard permissions; its separate acquisition/ordering proofs do not establish
all memory-ownership and lifetime obligations. Finite-clock exhaustion is handled
only as a harness case selection, not as a production overflow fix. Complete
transaction cleanup, arbitrary concurrent histories, general serializability,
and whole-engine memory/durability correctness remain unproved. The reader stays
registered at the end of this proof schedule prefix.

The independent checker from the accepted parent correctly
[rejected the changed boundary](accepted-boundary-rejection.json). The current
[boundary snapshot](proposed_boundary.json) records the reviewed local component
boundary; it does not approve itself as a performance optimization against the
old contract. No automatic performance promotion occurred.

## Reproduce and inspect

From the repository root with pinned tools installed:

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot
```

The archived text receipts retain exact commands, source/tool fingerprints, named
roots, mutant sources, and solver/test logs. [artifact_sha256.json](artifact_sha256.json)
hashes every retained archive artifact except itself. It supports integrity
checks without extending the proof scope. Tool binaries, build trees,
and native source archives are deliberately omitted; the named Git parent and
retained patches reconstruct the native diagnostic inputs.
