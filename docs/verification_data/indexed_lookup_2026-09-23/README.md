# Indexed lookup, ownership and acquisition-interference checkpoint

The complete **45-check component pilot passed** against stable
fingerprints for 414 inputs. Its parent is accepted commit
`1e7311a7127686a2d11b2d5a17347e82857c289d`. Production Rust remains unchanged; native edits add test-only
hooks and deterministic regressions.

The [summary](summary.json) and [complete receipt](formal/report.json) retain
`full_P1_complete = false`, `whole_engine_verified = false`, and
`promotion_eligible = false`. This checkpoint strengthens the components and
connects a conditional indexed-read/validation slice. It does not verify the
complete database or authorize arbitrary engine optimizations.

## New checked components

| Campaign | Independently checked roots | Rejected semantic mutants | Scope |
| --- | ---: | ---: | --- |
| Lifecycle acquisition interference | 17 | 13 | Acquired native suffixes and legal metadata wait traces |
| Opaque guard ownership | 36 | 17 | CAS, borrowing, consuming release and legal foreign interference |
| Native lookup and history | 22 | 21 | MVCC, read provenance, candidate coverage and materialization |
| Indexed read/validation composition | 3 | 12 | Guarded capture through materialization and native conflict validation |

These are **78 named roots and 63 semantic mutants** across the
new campaigns. Roots include supporting lemmas and bridges, not just native
operations. Ownership and slice campaigns also reject **5 affine
misuse controls**, separately classified by their expected frontend diagnostics.
Semantic mutants must reach and fail solver obligations; unrelated compilation
failures and timeouts do not count.

The lifecycle campaign permits arbitrary finite legal metadata changes while
waiting, preserves only the borrowed registration, and runs actual acquired-state
suffixes. It no longer frames the whole entry state across lock acquisition.
The ownership campaign derives opaque leases from the actual successful CAS,
borrows them for guarded access, and consumes them at the actual Release store.
Foreign acquisition/release can interleave with retry and guard-vector cleanup;
an already released key may be reacquired without resurrecting its old lease.

The lookup campaign proves actual version selection, private-write precedence,
key filtering, sorted unique results, exact recorded row/pointer/xmin provenance,
and native row validation. Independent Verus and Lean history arguments derive
historical candidate coverage from coherent posting replay and accepted bucket
stamps. Per-bucket monotonicity permits reordered stores to disjoint buckets.
The indexed composition acquires, captures, copies current candidates, consumes
guards, materializes, reacquires and invokes native predicate/row validators.
It covers a first one-bucket lookup and subsequent validation, not full commit.

Lean checked **34 required roots** and **20 semantic controls**,
with independent kernel replay and forged-theorem rejection. TLC passed
**136 declared cases**: 58 completed finite searches,
40 intended safety counterexamples, 3 intended liveness
counterexamples, and 35 positive witnesses. Witness/counterexample cases
are not exhaustive safety proofs. No Lean or TLA theorem is imported unchecked
into Verus. See [Lean](formal/lean.json) and [TLC](formal/tla/report.json).

## Native tests and performance

The final pilot passed **264 core regression tests**, the default/sort/
bitmap transaction/index/query matrix and Extended Crucible smoke runs, and the
actual-lock Loom campaign with its weakened-Acquire control. These are correctness
and integration checks, not sustained throughput or p99 measurements.

Separate [native diagnostics](native/receipt.json) retain 2 positive
selections and 5 intended assertion failures. Tests pause snapshot
creation before acquisition while other registrations end/reuse slots and change
horizons. They also pause after candidate capture while writers delete, move and
reuse postings, retain an older version through attempted vacuum, apply private
writes, repeat the lookup, and require subsequent conflict rejection. They check
that materialization releases predicate guards before those competing writes.

The diagnostic is optional archived evidence rather than a formal refinement.
Its parent crate archive, [current fixture](native/current-fixture.patch), per-mutant
patches and [source manifest](native/current-source-manifest.json) reconstruct the
exact tested sources. The [mutation manifest](native/mutation-manifest.json)
records hashes and the parent `git archive` command. Original commands retain
absolute run-time paths; substitute local source/build locations when reproducing.
Binaries and crate archives are omitted from this text evidence archive.

The [production comparison](production-equivalence.json) and retained
[checker](production_equivalence.py) check 118 tracked Rust/Cargo
inputs against the parent, excluding only explicitly reviewed changed cfg(test)
modules and hooks. Production regions are equivalent. No runtime locks, stronger
atomics or proof bookkeeping were added. **No new performance improvement or
sustained benchmark result is claimed.**

## Remaining assumptions and next step

The indexed slice restricts correspondence to its actual operation snapshot and
physical key-to-bucket mapping. Its acquired lease selects a history projection;
raw lookup enumerates that projection's current postings. The retained image must
map to snapshot-visible history replay. These explicit storage mappings still
need refinement from native raw pointers, loads, partial publication and vacuum.
Historical candidate completeness itself is derived rather than assumed.

Native copyable registration ownership, unique physical authority/bootstrap,
index/arena routing, frontend RAII/Vec destruction, weak-memory/mmap semantics,
reclamation, finite-clock exhaustion enforcement and arbitrary concurrent
transaction histories remain open. The composition uses separate materialization
and validation images with explicit address retention. It does not claim the
physical heap is frozen, nor does it prove writer commit, WAL or recovery.
The composed callers and guarded history correspondence require the lease's
arena to match the driver. The public stamp/raw primitive declarations have a
broader arena admission condition; a future native primitive implementation
must either handle that broader domain or narrow it explicitly. This does not
weaken the current composed caller's matching-arena obligation.

The next correspondence step is to connect a native publication/retention path
to the storage projections, preserving the current short guard lifetime.

The accepted parent's independent checker correctly
[rejects the changed boundary](accepted-boundary-rejection.json). The proposed
[local boundary](proposed_boundary.json) records these new contracts and checks;
refreshing it does not approve an optimization against the old contract.

## Reproduce

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot
```

[artifact_sha256.json](artifact_sha256.json) hashes each retained artifact except
itself. Exact source/tool fingerprints, commands, generated mutant sources,
solver output, model traces and test logs accompany the receipts under
[formal/](formal/). Missing roots, stale sources, omitted controls and incomplete
searches fail the composed gate.
