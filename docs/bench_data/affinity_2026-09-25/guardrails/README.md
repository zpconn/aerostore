The current-source component pilot completed **71/71 checks**. The [raw report](report.json) records identical before/after hashes for **523 proof, test and gate inputs**. The separate [execution receipt](execution-receipt.json) anchors the same frozen 482-file benchmark/source snapshot `fef4c866ad708e8d49335f6432a0ce0e24d288b4232e87efece565d22ad3c32e` before and after the run. P0 contract coverage is complete; full P1, whole-engine verification and architecture promotion remain false.

This preserves the existing component proofs, finite TLA models, native mutation controls, regressions and all three deterministic Extended Crucible feature configurations while temporary signature-affinity dispatch is added. It is not a formal proof of the new dispatch policy or scheduler, a complete HyperFeed implementation, worker-failure availability or a speedup. New routing/order/process-fault tests and observed serial histories are separate evidence in the [affinity campaign](../README.md). Functional campaigns ran concurrently with this pilot, so their latency is not a controlled performance comparison.

The [boundary review](boundary-review.json) identifies benchmark/model/measurement changes and tests, retaining the production engine and original Extended Crucible. The [previous boundary rejection](previous-boundary-rejection.json) remains preserved; an explicitly reviewed local boundary refresh now [passes](boundary-check.json). The report remains **local_bootstrap_only**, not acceptance by an independent baseline checker.

A development pilot was deliberately stopped after seven passing checks before correcting a new process-test helper that assumed an optional legacy report field was present. Its [raw report](development-report.json) remains incomplete and failed-to-complete, and the [interruption receipt](development-interruption.json) records unchanged inputs at the stop. That is not a proof failure or a completed pilot. Its logs and generated sources are separately preserved under `development/` in this archive, with the original [source bundle](../development/source.tar.gz) and build provenance in the parent development archive; the final 71-check result applies only to the fresh final snapshot.

The [compressed evidence](formal-evidence.tar.gz) retains raw receipts, logs, generated proof and mutation sources, native source snapshots and boundary snapshots. The [manifest](manifest.json) and [path map](archive-path-map.json) give every logical member's SHA-256 and original path. Identical bytes use relative tar hardlinks; every logical member was verified after creation. All 523 source bindings were [reconstructed from archived bytes](source-reconstruction.json), using the root [482-file source bundle](../source.tar.gz), the 41 additional proof inputs in this archive, and the [full source fingerprint](source-fingerprint.json). The development source bindings were separately reconstructed against their original incomplete report.

The Lean runner normally deletes temporary extraction/mutation sources. This run additionally captured 40 source/translation files before deletion: all 25 mutation source/extraction hashes declared by its final receipt match the captured bytes. Their capture and hash-validation receipts are under `evidence/lean-generated/` in the archive. Lean's 20 negative controls, including the extraction-dependent controls, retain their exact reported scope.

[Strict receipt validation](strict-revalidation.json) ran against complete live evidence before archiving. Compiled native/test/tool binaries, Cargo targets, TLC states and Lean build caches are omitted. The reduced archive cannot rerun validators requiring those binaries; tool identities, source bindings, outcomes and diagnostic counterexamples remain recorded.

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/affinity-final-validation/guardrails/report.json
```

Use a fresh output directory when repeating the command so this evidence is preserved. The [pilot log](pilot.log) records the exact component commands and outcomes.
