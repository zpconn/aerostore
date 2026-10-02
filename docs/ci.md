# Continuous integration and proof-input review

The `CI` workflow builds every workspace target, tests the libraries and
procedural macros, builds documentation, and runs the Python unit suites.
It also checks the committed proof-input lock before setup: stale locks cannot
pass required CI, while docs and benchmark edits need only this cheap check.
It uses Rust 1.93.1, matching the existing verification toolchain. Formatting and
clippy report problems without blocking until the planned formatting baseline
is reviewed. Library tests run serially while the existing process-based tests
are being separated into tiers.

Both workflows use shallow, filtered, sparse checkouts. Bulk benchmark and
verification evidence is excluded. Two small existing benchmark report fixtures
are retained byte-for-byte under `evidence/` for the performance-checker tests.
Ordinary CI checks the catalog, manifests, retained copies and documentation links.
Native integration tests that need an explicitly selected benchmark binary or
PostgreSQL remain separate; their skips are reported.

## Verification

The `Verify` workflow runs for proof-input changes, pushes to master, a nightly
schedule, and manual dispatch. Its component pilot runs the existing proof,
model, native-scenario, negative-control, and integration checks. It does not
establish whole-engine correctness or authorize a performance promotion.

The boundary inventory comes from `git ls-files`. It covers engine and macro
inputs, build configuration, proof code and contracts, toolchain pins, verification
tooling, and explicitly cited test inputs. Ordinary benchmark changes, uncited
tests, and general documentation are outside this proof-input lock. Generated
Verus outputs remain checked against their generators by the live runners.

There are two different comparisons:

- **Review:** the candidate lock must match its actual tracked proof inputs.
  Changes from the base lock are listed for owner review. A consistent changed
  lock is not an independent experiment approval.
- **Experiment:** the checker and anchoring helper come from the independent
  base commit. Changing the candidate lock and code together cannot satisfy the
  strict comparison. Manual experiment mode and experiment-marked PRs select
  this check.

Anchoring helpers run with isolated Python imports. Active untracked Cargo
controls and Python modules that would shadow protected tooling are rejected;
ignored scratch files are not added to the lock.

For a deliberate proof-input change, stage the intended source files first,
then generate and inspect the lock in the same review:

```sh
python3 -I scripts/check_formal_coverage.py --write-boundary
python3 -I scripts/check_formal_coverage.py --review-base master
git diff -- verification/frozen_boundary.json
```

This is a change-detection baseline, not a semantic proof. Proofs still run
against the candidate. The verification and experiment runners do not refresh
the lock for themselves.

## Initial migration and owner review

The old base commit has a checker but no extracted anchoring helper or reviewed
boundary-change mode. Its strict checker is still executed and is expected to
reject the initial gate-changing PR. The candidate pilot can run independently
to supply evidence for that review; its success does not hide the old gate's
failure. This is the explicit Checkpoint 1 transition in
[the overhaul plan](../OVERHAUL_PLAN.md).

A genuine bootstrap without an independent base is informational and never
promotion-eligible. Malformed or unavailable requested bases, failed base
checks, and failed pilots remain failures. After the approved gate migration,
master validation dispatches Verify with `mode=review` and `baseline_ref` set to
the approved PR head. Check that this head is a merge parent and that protected
input hashes are unchanged. The approved head supplies the helper and checker
for this anchored review; the result remains promotion-ineligible. Preserve the
initial master push's legacy rejection or cancellation separately.

`CODEOWNERS` names the owner for the lock, workflows, and verification tooling.
Master uses the interim protection approved at Checkpoint 1 until the Phase 6 review:
up-to-date `ci`, one approval including code-owner review, dismissal of stale
approvals, conversation resolution, and disabled force pushes and branch deletion.
Administrator enforcement is disabled (`enforce_admins: false`), so administrators
can bypass these rules. Each protected merge still requires explicit owner
approval in the current conversation. The automation uses the owner's account,
which cannot approve its own PR; this interim procedure does not provide
independently enforced owner review.

Verify is path-filtered, so it is not a globally required status check. Owner
review must also check the current Verify result on proof-input PRs. A separate
docs-only PR should run ordinary CI and skip Verify.

## Resources and evidence

CI records disk usage and checks a growth allowance plus a free-space reserve.
Runner preparation removes only named unused preinstalled SDKs on fresh
GitHub-hosted machines. It does not run on self-hosted machines or prune project
evidence. Core dumps are disabled for the test and verification commands.

The owner checkout retains recorded binaries in ordinary Cargo output paths.
These CI checks run in fresh hosted workspaces so rebuilding does not overwrite
those historical bytes. See [the disk-space runbook](disk-space.md) before any
local build or cleanup. Boundary and pilot logs and receipts are retained as
workflow artifacts, including failed runs.

TLC's upstream `v1.8.0` download is a rolling build. Setup retrieves the exact
previously tested jar from the official publisher's dated extension package,
checking both the package and jar hashes. This changes its download source, not
its version or executable bytes. CI checks that download before building the
remaining proof tools.
