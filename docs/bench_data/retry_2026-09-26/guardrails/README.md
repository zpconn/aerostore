The current-source component pilot completed **71/71 checks**. The [raw report](report.json) records identical before/after hashes for **535 proof, test and gate inputs**. The separate [execution receipt](execution-receipt.json) anchors the same frozen 493-file benchmark/source snapshot `a93318d444bd779fcbb6034e11ec51493e59379fbbd9eb7f6481ddbcb147a841` before and after the run. P0 contract coverage is complete; full P1, whole-engine verification and architecture promotion remain false.

This preserves the existing component proofs, finite TLA models, native mutation controls, regressions and all three deterministic Extended Crucible feature configurations. The transaction source adapters claim the configuration with **`retry-diagnostics` disabled**. Exactly reviewed feature-guarded observations are erased under a full default-native-token equality check against `4da551b`, and their site inventory is pinned. This introduces no formal claim for the instrumented feature, the benchmark-only expiry-index policy, complete HyperFeed compatibility, worker-failure availability or a speedup. Diagnostic and index-policy tests and observed transaction histories remain separate evidence in the [retry/index campaign](../README.md). All controlled performance trials completed before this pilot started. The separate functional regression runs establish behavior, not a performance comparison.

The [boundary review](boundary-review.json) identifies benchmark/measurement changes, optional native observations, source-normalization and tests. The default native token stream and original Extended Crucible are preserved. The [previous boundary rejection](previous-boundary-rejection.json) is retained; an explicitly reviewed local boundary refresh now [passes](boundary-check.json). The report remains **local_bootstrap_only**, not acceptance by an independent baseline checker.

The [compressed evidence](formal-evidence.tar.gz) retains raw receipts, logs, generated proof and mutation sources, native source snapshots and boundary snapshots. The [manifest](manifest.json) and [path map](archive-path-map.json) give every logical member's SHA-256 and original path. Identical bytes use relative tar hardlinks; every logical member was verified after creation. All 535 source bindings can be reconstructed from the root [493-file source bundle](../source.tar.gz), the 42 additional proof inputs in this archive, and the [full source fingerprint](source-fingerprint.json).

The Lean runner normally deletes temporary extraction/mutation sources. This run additionally captured 40 source/translation files before deletion: all 25 mutation source/extraction hashes declared by its final receipt match the captured bytes. Capture and hash-validation receipts are under `evidence/lean-generated/` in the archive. Lean's 20 negative controls, including the extraction-dependent controls, retain their exact reported scope.

[Strict receipt validation](strict-revalidation.json) ran against complete live evidence before archiving. Compiled native/test/tool binaries, Cargo targets, TLC states and Lean build caches are omitted. The reduced archive cannot rerun validators requiring those binaries; tool identities, source bindings, outcomes and diagnostic counterexamples remain recorded.

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/retry-final-validation/guardrails/report.json
```

Use a fresh output directory when repeating the command so this evidence is preserved. The [pilot log](pilot.log) records the exact component commands and outcomes.
