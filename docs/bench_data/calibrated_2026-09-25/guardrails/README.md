The current-source component pilot completed **71/71 checks**. The [raw report](report.json) records identical before/after hashes for **522 proof, test and gate inputs**. The separate [execution receipt](execution-receipt.json) anchors the same frozen 481-file benchmark/source snapshot `29f53f412ab151ce026c189a95e67c477015ffa0489a59c8bdc5e254f7451add` before and after the run. P0 contract coverage is complete; full P1, whole-engine verification and architecture promotion remain false.

This preserves the existing component proofs, finite TLA models, native mutation controls, regressions and all three deterministic Extended Crucible feature configurations while the calibrated workload is added. It is not a formal proof of the new scheduler, a complete HyperFeed implementation, worker-failure availability or a speedup. New workload/order/timer tests and observed serial histories are separate evidence in the [calibrated campaign](../README.md). Functional campaigns ran concurrently with this pilot, so their latency is not a controlled performance comparison.

The [boundary review](boundary-review.json) identifies benchmark/model/measurement changes and tests, retaining the production engine and original Extended Crucible. The [previous boundary rejection](previous-boundary-rejection.json) remains preserved; an explicitly reviewed local boundary refresh now [passes](boundary-check.json). The report remains **local_bootstrap_only**, not acceptance by an independent baseline checker.

The [compressed evidence](formal-evidence.tar.gz) retains raw receipts, logs, generated proof and mutation sources, native source snapshots and boundary snapshots. The [manifest](manifest.json) and [path map](archive-path-map.json) give every logical member's SHA-256 and original path. Identical bytes use relative tar hardlinks; every logical member was verified after creation. All 522 source bindings can be reconstructed from the root [481-file source bundle](../source.tar.gz), the 41 additional proof inputs in this archive, and the [full source fingerprint](source-fingerprint.json).

The Lean runner normally deletes temporary extraction/mutation sources. This run additionally captured 40 source/translation files before deletion: all 25 mutation source/extraction hashes declared by its final receipt match the captured bytes. Their capture and hash-validation receipts are under `evidence/lean-generated/` in the archive. Lean's 20 negative controls, including the extraction-dependent controls, retain their exact reported scope.

[Strict receipt validation](strict-revalidation.json) ran against complete live evidence before archiving. Compiled native/test/tool binaries, Cargo targets, TLC states and Lean build caches are omitted. The reduced archive cannot rerun validators requiring those binaries; tool identities, source bindings, outcomes and diagnostic counterexamples remain recorded.

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/calibrated-validation/guardrails/report.json
```

Use a fresh output directory when repeating the command so this evidence is preserved. The [pilot log](pilot.log) records the exact component commands and outcomes.
