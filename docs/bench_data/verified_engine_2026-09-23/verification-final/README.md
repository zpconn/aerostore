# Final native-engine verification evidence

The [composed receipt](report.json) records **19 successful checks** against
unchanged source throughout the run. This is the source after the poison-admission
repair and addition of mandatory actual-lock model checks. The production mutex
algorithm remains unchanged; rejected performance variants are isolated experiments.

| Evidence | Result and boundary |
| --- | --- |
| Native commit Verus | Five extracted operation bodies and one supporting witness lemma pass together and individually. All 13 deliberately incorrect variants fail semantic proof obligations, including omitted health recheck and abort cleanup. Primitive/data/history refinement remains open. |
| Lean and helper Verus | Existing production bucket and stamp proofs pass, with fresh extraction, independent Lean kernel replay, required theorem roots and negative controls. |
| Actual-lock Loom campaign | Seven required bounded cases have their expected outcomes; a separately compiled Acquire-to-Relaxed mutant fails with the required causality violation. Fresh binaries, source identities and retained artifacts are validated. This is not unbounded progress or native mmap refinement. |
| TLA+/TLC | All 49 cases have expected outcomes: 17 completed searches, 15 safety counterexamples, 3 liveness counterexamples and 14 positive witnesses. These are finite abstract models, not Rust refinement proofs. |
| Integration | Default, sort and bitmap bucket configurations pass integration tests, all 27 extended Crucible phases and all six native concurrency contracts. |
| Native regression gate | Selected core, WAL/recovery, writer-lifecycle, shared-memory and histogram suites pass, including all three new poison regressions. |
| Gate integrity | Thirty gate tests pass, including incomplete, stale, substituted and incorrectly classified lock-model evidence. Existing adapter, model-runner and performance-runner tests also pass. |

The separate [release receipt](../release-final.json) records **411 passing tests
and two ignored tests** across core/macros/verified crates, plus **23 passing Tcl
tests**, with unchanged native source hashes. The ignored tests are the long GC
stress test and explicitly invoked lock-striping benchmark. Counts overlap the
composed campaign; they are not independent totals to add to its results.
Complete release logs are [core](../core-workspace-final.log) and
[Tcl](../tcl-workspace-final.log).

The [poison-admission evidence](../durability/poison-admission.md) retains all
three failures before repair and their corrected outcomes. Earlier
[durability](../durability/README.md) and [primary-key](../primary-key/README.md)
evidence retain the original engine failures. Source archives and receipts keep
those older implementations distinct from this final source.

The new health checks establish admission observations and protected handling
of detected indeterminate synchronous append errors. They do not cancel disjoint
transactions already admitted, enforce raw-writer stream health, or solve process
death during append. The [durability contract](../../../../verification/contracts/durability.md)
and [conditional proof boundary](../../../../verification/concurrent/README.md)
state those exclusions.

`full_P1_complete` and `whole_engine_verified` remain **false**. The refreshed
frozen boundary is an unreviewed proposal: this run is `local_bootstrap_only`,
and `promotion_eligible` remains false. Fingerprints prevent unnoticed drift;
they do not prove the frozen native primitives.

The [performance comparison](../README.md) remains rejected. Its last measured
candidate lost 7.0% median sustained throughput; its extended workloads met the
declared margins. This final poison repair is newer than that timed source and
requires a fresh capture before any performance claim. No runtime lock experiment
was adopted, and proof state does not execute in production transactions.

Reproduce the composed run after installing and sourcing the pinned tools:

```sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/verification-next/formal-poison-lock/report.json
```

[validation-final.json](../validation-final.json) binds this archive to source
and artifact hashes. Large build artifacts and the lock-model executables remain
local under `target/`; the retained commands, source snapshots, tool identities,
logs and mutation patch permit rebuilding them. An archived receipt alone is not
a substitute for running verification against changed source.
