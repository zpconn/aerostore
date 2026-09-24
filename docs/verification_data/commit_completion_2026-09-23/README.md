# P0 audit and native publication/completion checkpoint

The **63-check component pilot passed** against 466 stable source fingerprints.
The accepted parent is `4619b8f465d71a263f531eac72a88ca536aa0bcb`.
The [summary](summary.json) and [complete report](formal/report.json) record
`p0_complete = true`, `full_P1_complete = false`,
`whole_engine_verified = false` and `promotion_eligible = false`.

## What changed

P0's contract audit is complete for the declared single-table target. It covers
425 public declarations across 32 core modules, 83 contract families and 31 lock
relationships in 11 operation paths. The inventory specifies success/error
behavior, caller obligations and explicit legacy exclusions. Its checker binds
these contracts to exact source signatures, bodies and complete module hashes.
This is a reviewed coverage milestone, not a proof that every safe Rust API
enforces its documented preconditions or that the declared lock graph is complete.
Raw-pointer ownership, initialized object padding, finite-counter exhaustion and
exclusive reset authority remain explicit implementation obligations.

Two new Verus campaigns connect previously separate publication components:

| Campaign | Individually checked roots | Rejected semantic mutations | Full generated crate |
| --- | ---: | ---: | ---: |
| Ordinary commit data | 13 | 17 | 111 verified obligations |
| Publication and completion | 6 | 14 | 189 verified obligations |

These counts include embedded component lemmas and witnesses; they are not
percentages of database coverage. Every mutant must reach an intended failed
proof obligation. Compiler errors and timeouts do not count as rejection evidence.

The data proof uses the native ordinary commit interval and actual ordinary
publisher. From an admitted one-write/one-index plan, it derives destination
ownership, row/posting agreement and exact returned write provenance. Rollback
and partial-error paths preserve old data or poison access. Existing posting
success contracts were strengthened to retain the poison flag.

The completion proof joins that publication to actual `finish_transaction`,
including `Option::take` and the fallible deregistration call, then reserves and
publishes stamps from the shared ProcArray clock. It derives a fresh stamp,
exact affected buckets, consumed registration and poison on late failure.
A separate lemma shows that the resulting stamp invalidates an affected
dependency under an explicit earlier-reader clock bound.

The next P1 work is to establish this admitted plan through actual planning,
validation and guard handoff, compose cleanup, and join reader capture and native
validation with writer completion. Complete final-write selection, physical
identity across modeled views and lifecycle acquisition framing remain stated
boundaries. The join is conditional; it does not establish a complete concurrent
transaction history. P1 permits enumerated low-level storage/atomic assumptions
without requiring all P2/P3 implementation proofs first.

## Native and combined validation

The new native campaign passes nine selected positive cases and rejects seven
intentionally broken source variants at their required assertions. Two new
public-API tests use actual worker threads: competing empty-query creations
have one committed winner, and a key move forces a reader's staged write to
remain unpublished. Fresh retries observe the winner or commit successfully.
Both earlier-active and later-starting writer orders are covered. Existing cases
cover candidate capture, allocation failure, rollback, poison and stamping cuts.
These are finite scheduled executions, not exhaustive concurrency proofs.

The combined pilot also passes the prior storage/retention tests, actual-lock
Loom campaign, 266 core regressions, and transaction/query suites with default,
sort and bitmap bucket implementations. All three Extended Crucible smoke runs
check the replay and concurrency contracts; they are integration runs, not
sustained timing comparisons.

The unchanged Lean campaign passes 34 roots and 20 semantic controls, independent
kernel replay and forged-theorem rejection. TLC passes all 136 existing cases:
58 complete finite searches, 40 intended safety counterexamples, three intended
liveness counterexamples and 35 reachable witnesses. These results are not
unchecked theorem imports into the new Verus proof.

## Runtime and experiment boundary

The [runtime audit](runtime-audit.json) compares 118 Rust/Cargo inputs to the
accepted parent. All production inputs are byte-identical; the sole changed
native file is an integration test. This checkpoint introduces no runtime
instructions, locks, atomics or proof bookkeeping, and makes no new throughput
or latency claim. The audit is exact byte comparison, not a compiler-equivalence
theorem.

The [accepted checker](baseline_check_formal_coverage.py) rejects these changes
against the [old frozen boundary](accepted-frozen-boundary.json). The retained
[rejection](baseline-rejection.json) is expected: contracts, proof adapters,
mandatory campaigns and tests changed. The [reviewed local boundary](reviewed-frozen-boundary.json)
allows the new component pilot to run, while automatic performance promotion
remains false. This evidence does not accept an optimization under the old
contract by silently replacing its fingerprints.

The historical production-token checker also rejects the new integration-test
bodies because its fixed exclusions cover only older test additions. Its
diagnostic failure is retained under [diagnostics](diagnostics/scope.json);
the byte audit above is the current runtime-change evidence.

## Evidence layout

- `formal/`: unchanged report, commands, logs, model cases, receipts and proof mutants.
- `source-inputs/` and `source-input-sha256.json`: exact 466-input proof/test snapshot.
- `native/current-source.tar.gz`: one common native fixture; `native/*/mutation-manifest.json`
  and patches reconstruct each isolated deliberately broken build.
- `archive-path-map.json`: maps retained raw paths to archive paths. Native build
  trees/tool executables are omitted; their identities remain in the original receipts.
- `artifact_sha256.json`: digests of retained artifacts, excluding itself.
- `archive_evidence.py`: validates the completed current-source run before copying it.

Reproduction uses the pinned setup described in
[verification/README.md](../../../verification/README.md), then
`python3 scripts/verify_formal.py --profile pilot`. Archived hashes support
inspection; accepting a new candidate still requires current-source proofs and
tests through the reviewed gate.
