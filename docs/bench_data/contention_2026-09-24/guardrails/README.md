# Preserved verification guardrails — 2026-09-24

The final component pilot completed **71/71 checks**; [the raw report](report.json) records `passed: true` and identical before/after hashes for all 501 inputs. P0 contract coverage remains complete. Full P1, whole-engine verification, and architecture promotion remain false. This pass does not override the [failed workload campaigns](../README.md) or the [failed surviving-worker availability requirement](../../../worker_failure_contract.md).

[The boundary audit](reviewed-boundary-audit.json) compares against `6291200b4383538d960f3b1c73942715193f464b`: all 37 production Rust files and 506 existing proof/gate files are byte-identical. The deterministic Extended Crucible is unchanged. Two existing protected files changed: `aerostore_core/Cargo.toml` adds one bench registration with unchanged dependencies, and one function in `wal_crash_recovery.rs` repairs a legacy test's asynchronous durability assumption. Thirteen new harness/test files extend the frozen path set from 339 to 352 files.

This run is explicitly **`local_bootstrap_only`**, following a reviewed local boundary refresh. The independently extracted checker from `6291200` correctly rejects the additions, manifest/test changes, and changed boundary lock; [that rejection is retained](baseline-rejection-after-refresh.json). Refreshing the local boundary does not establish acceptance by the old independent baseline.

The [previous complete pilot](failed-pilot-report.json) remains failed: 70/71 checks passed, with the core regression command failing an asynchronous WAL savepoint restart test. Its retained WAL contains exactly three complete second-wave records and lacks a first-wave-only identity that the test incorrectly expected to survive SIGKILL. Twenty-one focused reruns passed, but the failure was not discarded: a disposable copy with a file lock and bounded dequeue wait reproduced the original assertion failure deterministically.

The repaired test forces that permitted loss, kills and reaps the first daemon, explicitly unlocks before replacement, and rewrites every expected final identity in the second wave. It checks every persisted recovery payload against the literal committed values, rejects rolled-back churn, and retains exact final-state assertions. The final source passed 21 focused repetitions before the fresh full pilot. This changes no engine flush, synchronization, durability, or ownership behavior. It establishes neither surviving application-worker availability nor crash consistency of arbitrary asynchronous histories. Typed decoding, the original WAL bytes, both test snapshots, the patch, and all focused logs remain in the archive.

An earlier pilot was interrupted after nine completed checks so the new supervision test target could stop importing unrelated legacy adapter tests. Its report remains incomplete and failed. The final helper extraction received six debug and six release checks plus a mechanical body-comparison audit; that audit is a drift check, not a formal equivalence proof.

[The evidence archive](formal-evidence.tar.gz) retains raw receipts/logs, generated proof and mutation source, source snapshots, boundary inputs, and both earlier attempts. [The manifest](manifest.json) records hashes, scope, and omissions; [the path map](archive-path-map.json) maps original paths to archive members and explains historical output-directory relocations. Reports retain their original paths. Identical file contents use standard tar hardlinks to earlier relative members; every artifact path retains its own recorded hash. Compiled binaries, Cargo targets, tool binaries, TLC caches, and Lean build caches are omitted. Strict component-evidence validation ran against the fresh complete local output before archival; this reduced archive cannot rerun validators requiring omitted binaries.

The completed run used the existing pinned tools and unchanged pilot driver:

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/contention-validation/formal/report.json
```

Use a new output directory for another run to preserve these receipts. The pilot includes component proofs, finite models, mutation checks, native checks, and deterministic regression/Extended Crucible checks. The new contention workload and worker-failure evidence remain separate.
