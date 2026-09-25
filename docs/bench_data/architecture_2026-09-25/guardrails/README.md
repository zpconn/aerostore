The final component pilot completed **71/71 checks**. The [raw report](report.json)
records current-source verification and identical before/after input hashes.
P0 contract coverage remains complete; full P1, whole-engine verification, and
architecture promotion remain false. This pass does not establish a speedup or
supersede workload/progress/failure evidence in the [architecture campaign](../README.md).

The [reviewed boundary audit](reviewed-boundary-audit.json) compares the final
source to Git baseline `6291200b4383538d960f3b1c73942715193f464b`.
All 37 production Rust files and all eight deterministic Extended Crucible files
are byte-identical. Existing proof logic is unchanged. Among 506 existing
proof/gate files, the sole modification is a
[one-line checker scope expansion](coverage-enrollment.patch): source change
detection now includes `verification/service_protocol`. This enrolls the model
sources in the frozen boundary; it does not make its separate TLC campaign a
Rust refinement proof or a new check in the existing 71-check pilot.

The local boundary now fingerprints 370 files, including 31 additions relative
to the old 339-file baseline. The other existing protected changes are one Cargo
bench registration and the previously reviewed asynchronous WAL regression
repair. Dependencies are unchanged, and that repaired test is byte-identical to
the earlier retained repair. The [local check](local-boundary-check.json) passes
following an explicit review and refresh. The independently extracted old Git
checker correctly [rejects this changed boundary](trusted-baseline-rejection.json).
The receipt is **local_bootstrap_only**, not independent-baseline acceptance.

The initial 370-file refresh and source audit remain in the archive under
`before-reply-deadline/`. A subsequent independent service review identified a
slow-reader reply-deadline gap before this final pilot began. The post-repair, pre-fleet review is retained
under `before-fleet/`. A later workload review added a separate populated fleet
profile; the earlier concentrated lifecycle stress evidence remains unchanged
in the [stress archive](../stress-campaigns/README.md). The service
[evidence](../service-availability/README.md) retains the deterministic failing
regression, repaired source and 27-test debug/release receipts; the
[earlier snapshot](../service-availability-pre-reply-deadline/README.md) retains
exact pre-repair source. No failed or incomplete workload is relabeled by this
component-verification result.

The first fleet-source pilot was deliberately stopped after 37 successful
checks when review identified an omitted cleanup interval in the workload's
reported admission-to-drain duration. Its raw report remains **incomplete**,
and its source hashes were unchanged at the deliberate stop. That report,
logs and interruption receipt are retained under
`before-continuous-timing/`. Later component checks had not executed; the
partial run is not a pilot pass. The workload timing correction and its
regressions are separate from production engine/proof logic.

The [compressed evidence](formal-evidence.tar.gz) contains raw receipts, check
logs, generated proof/mutation source, native source snapshots, and the reviewed
boundary snapshots. The [manifest](manifest.json) records exact hashes and
omissions; the [path map](archive-path-map.json) maps original paths to archive
members. Historical receipts keep their original internal paths; the path map
records each preserved snapshot prefix separately so those paths cannot be
confused with the final run. Identical file contents use ordinary relative tar hardlinks, preserving
all logical paths and per-file hashes. Every archive member was checked after
creation. [Strict validators](strict-revalidation.json) ran against full live
receipts and binaries before archival. Compiled binaries, Cargo targets, TLC
states and Lean build caches are omitted; the reduced archive cannot rerun
validators that require those binaries.

The run used the existing pinned tools and unchanged pilot driver:

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \
  --output target/architecture-validation/formal/report.json
```

Use a fresh output directory to preserve this receipt. The new service protocol
TLC campaign, client-death tests and architecture workloads are separate checks
with their own stated scope.
