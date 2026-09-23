# Native-engine verification evidence

This directory records the first durability candidate, before the later
lock-duration optimization and histogram repair. The final source has a
[separate accepted verification run](../verification-optimized/README.md).

The [composed receipt](report.json) records **18 successful checks**, with
unchanged source hashes throughout the accepted run. This advances the component
pilot; `full_P1_complete` and `whole_engine_verified` remain false.

| Evidence | Result and scope |
| --- | --- |
| Verus production helpers | Existing bucket and stamp proofs pass, including semantic negative controls. |
| Native commit orchestration | Three actual operation bodies satisfy conditional event-order contracts; the supporting callback lemma demonstrates admitted success/failure cases. Seven deliberately incorrect variants fail proof obligations. Native primitive and transaction-history refinement remain open. |
| Lean extraction/proofs | Fresh extraction/proof checks and independent replay of the Lean environment pass. These cover the existing safe helper kernels, not the concurrent engine. |
| TLA+/TLC | All 49 cases have their expected outcomes: 17 completed searches, 15 safety counterexamples, 3 liveness counterexamples, and 14 positive witnesses. Finite models are not native Rust refinement proofs. |
| Integration matrix | Default, sort, and bitmap bucket configurations pass integration tests and the extended Crucible's 27 phases and six native concurrency contracts. |
| Native regression gate | Core unit tests and the selected WAL, recovery, writer-lifecycle, and cross-process suites pass. |
| Verification infrastructure | Coverage, stale-source detection, extraction/adaptation, proof-root checks, model classification, and performance-decision tests pass. |

The separate complete release runs contain **394 passing tests, two ignored
tests** across core/macros/verified crates, and **23 passing Tcl tests**.
[validation.json](../validation.json) records the commands, log hashes, and
accepted source hashes. The full logs are [core-workspace.log](../core-workspace.log)
and [tcl-workspace.log](../tcl-workspace.log). These counts aggregate the test
result lines; integration coverage overlaps the composed campaign.

Reproductions against the earlier production source and corrected results are
retained for the [durability bugs](../durability/README.md) and
[primary-key insertion race](../primary-key/README.md). The primary-key fix also
passed 100 repetitions of the original cross-process regression.

The gates rejected two inappropriate approvals during this work:

- [The first composed run](../source-change-rejection.json) had successful
  component checks but failed overall because production source changed while
  it ran. The accepted receipt above comes from a fresh, stable run.
- [The previous baseline's checker](../previous-boundary-rejection.json)
  rejected the changed frozen engine. The refreshed local boundary is a new
  review proposal, not approval of an optimization under the old contract.
  This receipt is explicitly `local_bootstrap_only`.

The [performance campaign](../README.md) is a separate acceptance condition.
Passing these checks establishes neither a speedup nor performance equivalence.
No proof state executes on the database's runtime path.

For the exact theorem boundary and remaining native obligations, see the
[concurrent proof description](../../../../verification/concurrent/README.md)
and [durability contract](../../../../verification/contracts/durability.md).
