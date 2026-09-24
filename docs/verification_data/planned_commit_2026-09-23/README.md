# Native write planning and base-validation checkpoint

The **71-check component pilot passed** against 488 stable source fingerprints.
The accepted parent is `f82788073224f833873f57c25243a553b4c4d511`.
The [summary](summary.json) and [complete report](formal/report.json) retain
`p0_complete = true`, `full_P1_complete = false`,
`whole_engine_verified = false` and `promotion_eligible = false`.

## What changed

Three new Verus campaigns connect native planning and validation to the existing
publication/completion proof:

| Campaign | Individually checked roots | Rejected semantic mutations | Full generated crate |
| --- | ---: | ---: | ---: |
| Final-write and index-change planning | 7 | 13 | 124 verified obligations |
| Current-base validation and admission | 4 | 11 | 118 verified obligations |
| Planning through publication/completion | 5 | 12 | 214 verified obligations |

These counts include embedded components and witnesses. They do not measure a
percentage of engine correctness. Every negative control must fail a proof
obligation; compiler errors and resource exhaustion do not count.

The actual `final_write_indices` loop proves complete selection of the last
pending write for every row, uniqueness and ascending row order. This result is
parameterized over arbitrary finite write sets. The map primitive's overwrite
and ordered enumeration behavior remains an explicit library contract.

For one selected row/index, the actual `index_changes` loop derives exact old
and new keys, omits unchanged keys and prevalidates every emitted key. The actual
`has_write_base_conflict` loop derives current-head/base equality and zero old
`xmax` on acceptance. Neither fact is supplied as a publication precondition in
the new joined harness.

The joined proof calls these routines on the same row storage used by ordinary
publication. For repeated writes to one row with one changed final key, it
derives the exact selected report, row/posting result, deregistration and fresh
shared-clock stamp. The physical base must be nonzero; its logical value may be
empty, allowing creation in a preseeded vacant slot as well as deletion.
Private-allocation ownership, retained immutable values, coherent acquired
postings and field/guard authority remain caller assumptions.

## Remaining P1 boundary

The main harness begins with an acquired row projection. A separate frame lemma
preserves extracted keys when heads or deletion IDs change while waiting,
provided retained base/new value fields stay immutable. It does not execute
native guard acquisition. Predicate/read/owner validation and their rejection
paths remain to be composed with this result.

Native error cleanup and successful non-final-write recycling are also outside
the join. Early error guarantees describe the interval before caller abort.
The exact whole-image success result describes the harness projection, not a
claim that superseded private allocations remain unchanged after the native
API returns and releases them for reuse. The cleanup join must forget retired
private allocations while preserving the published chain.

The next P1 step is actual guard handoff, the remaining validation decisions,
cleanup and the reader/writer scenario. Same-arena physical correspondence,
lifecycle acquired-state framing, atomic/pointer primitives, no-wrap and weak
memory remain enumerated boundaries. P1 permits explicit low-level contracts;
it does not require finishing all P2 history or P3 ownership proofs first.
The [slice documentation](../../../verification/planned_commit/README.md) gives
the exact conditional scope.

## Native and combined validation

Four selected positive scenarios pass and eleven native source mutations fail
at their intended semantic assertions. The new public-API regressions cover:

- Interleaved repeated writes to several rows, savepoint rollback, exact final
  reports, dirty masks, original bases and final postings.
- An unchanged final key after an abandoned intermediate key, retaining the
  original posting and avoiding a false empty-predicate conflict.
- A stale base rejecting all staged writes before any publication, including
  an uncontested row ordered before the stale row, followed by a successful retry.

The missing-base-check control demonstrates partial publication of that earlier
row; the unchanged engine rejects before either row publishes. Existing nested
savepoint/abort coverage remains in the campaign. A prior competing-creation
test now acknowledges completion of each final fresh retry to avoid an unrelated
retry-versus-retry race; its original conflicting worker schedule is unchanged.
These are finite executions, not arbitrary concurrent-history proofs.

The combined pilot also passes the existing retention and P1 native mutation
campaigns, production-lock Loom checks, 266 core regressions, and the default,
sort and bitmap transaction/query suites. All three Extended Crucible smoke runs
check replay and concurrency contracts; these are functional integration runs,
not sustained performance measurements.

Lean retains 34 audited roots, 20 semantic controls, independent kernel replay
and forged-theorem rejection. TLC retains all 136 cases: 58 complete finite
searches, 40 intended safety counterexamples, three intended liveness
counterexamples and 35 reachable witnesses. These independent campaigns are not
unchecked theorem imports into Verus.

## Runtime and experiment boundary

The [runtime audit](runtime-audit.json) compares 118 Rust/Cargo inputs with the
accepted parent. Every production input is byte-identical; only the native
integration-test file changes. This work adds no production instructions,
locks, atomics or proof bookkeeping and makes no new timing claim.

The [accepted checker](baseline_check_formal_coverage.py) rejects the changes
against the [old boundary](accepted-frozen-boundary.json). That retained
[rejection](baseline-rejection.json) is expected because proof contracts,
adapters, tests and mandatory checks have changed. The explicitly reviewed
[local boundary](reviewed-frozen-boundary.json) admits this component pilot;
performance promotion remains false. This is a verification-boundary expansion,
not an optimization accepted by changing its own contract.

The gate checks exact nested-module selectors, current source hashes and all
required roots and mutants. Its 82 adversarial tests pass. The new composition's
order check tokenizes source so comments or string literals cannot satisfy a
required executable call.

## Evidence layout

- `formal/`: original report, commands, logs, receipts, proof mutants and model cases.
- `source-inputs/` and `source-input-sha256.json`: exact 488-input snapshot.
- `native/current-source.tar.gz`: common native fixture; per-campaign mutation
  manifests and patches reconstruct deliberately broken source trees.
- `archive-path-map.json`: original-to-archive path mapping. Native build outputs
  and tool binaries are omitted; their hashes remain in the receipts.
- `artifact_sha256.json`: hashes of retained artifacts, excluding itself.
- `archive_evidence.py`: validates the completed current-source run before copying.

Reproduce with the pinned [verification setup](../../../verification/README.md)
and `python3 scripts/verify_formal.py --profile pilot`. Archived hashes preserve
inspectable evidence; new candidates still need fresh current-source checks.
