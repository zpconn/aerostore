# Optimized native-engine verification evidence

The [composed receipt](report.json) records **18 successful checks**, with
unchanged source hashes throughout this final run. It includes the WAL
preparation optimization and improved Crucible histogram.

| Evidence | Result and scope |
| --- | --- |
| Verus production helpers | Existing bucket and stamp proofs pass, including semantic negative controls. |
| Native commit orchestration | Five extracted operation bodies and one callback-contract witness lemma pass collectively and individually. Eleven deliberately incorrect variants fail semantic proof obligations. Native primitive, memory, and transaction-history refinement remain open. |
| Lean extraction/proofs | Fresh extraction/proof checks and independent kernel replay pass for the existing safe helper kernels. |
| TLA+/TLC | All 49 finite cases have the expected outcomes: 17 completed searches, 15 safety counterexamples, 3 liveness counterexamples, and 14 positive witnesses. These are not native Rust refinement proofs. |
| Integration matrix | Default, sort, and bitmap bucket configurations pass integration tests and the extended Crucible's 27 phases and six native concurrency contracts. |
| Native regression gate | Core unit tests, selected WAL/recovery/writer-lifecycle/cross-process suites, and all eight histogram tests pass. |
| Verification infrastructure | Seven concurrent-adapter tests, 21 performance-runner tests, and the existing coverage, forgery/stale-source, extraction, and model-classification checks pass. |

The complete release runs contain **408 passing tests, two ignored tests**
across core/macros/verified crates, and **23 passing Tcl tests**.
[validation-optimized.json](../validation-optimized.json) records the commands,
log hashes and accepted source hashes. Full logs are
[core-workspace-optimized.log](../core-workspace-optimized.log) and
[tcl-workspace-optimized.log](../tcl-workspace-optimized.log). The two ignored
tests are the separately invoked lock-striping benchmark and long GC stress
test. These aggregate counts overlap the composed campaign's integration checks.

The [durability evidence](../durability/README.md) distinguishes original
`a382ce3` failures from the initial repaired candidate's excessive lock duration.
The optimized path permits a competing indexed commit during serialization,
then rejects stale prepared data before any WAL acceptance. It also checks
preparation panic cleanup, epoch invalidation and safe full-baseline retry.
The [primary-key evidence](../primary-key/README.md) retains four deterministic
old-code failures and the repaired cross-process test's 100 successful repeats.

The [initial accepted run](../verification/README.md) remains available for
comparison. The earlier [source-change rejection](../source-change-rejection.json)
and [old-boundary rejection](../previous-boundary-rejection.json) are preserved;
neither was converted into an approval.

`full_P1_complete` and `whole_engine_verified` remain false. This run is
`local_bootstrap_only`: the changed frozen boundary is a new review proposal,
not an optimization approved against the previous boundary. The exact
[conditional theorem boundary](../../../../verification/concurrent/README.md)
and [durability contract](../../../../verification/contracts/durability.md)
identify what remains assumed or unproved.

The [performance campaign](../README.md) is a separate acceptance condition.
This verification result alone establishes neither a speedup nor performance
equivalence. No proof state executes in production transactions.
