# Checkpoint 2: archive and launch the smaller repository

Status: **approved by the owner on October 2, 2026; publication qualification in progress**.
The original approved packet is retained in source commit `ab57181a` in the local
backup and as rewritten commit `6c1e3417` in the new repository. Checkpoint 2
approved the operations below and the exact packaging boundary; it did not
approve the follow-up PR merge, original-object cleanup, archive read-only status
or evidence releases. See [the execution log](../OVERHAUL_PLAN.md#appendix-c-execution-log).

The original repository is now `zpconn/aerostore-archive` (ID `1167197125`), with
`archive/pre-rewrite` pinned to `c22acd41`. New `zpconn/aerostore` (ID `1402200979`)
publishes master `6c1e3417`, whose tree matches the approved prepared source.
The fresh public clone is 23.7 MB allocated; three canonical archive fetches
verified 330 files, and five documented historical commits resolve in the archive.
Fresh [CI](https://github.com/zpconn/aerostore/actions/runs/37047817537) passed.
[Verify](https://github.com/zpconn/aerostore/actions/runs/37047821217) is running.
Verify uses the approved mapped baseline `57b32d33`; these local checks do not
replace its full hosted pilot.

## Reviewed local result

The preparation commit is `d0433540fa6f04ad66b48f9977d8ab2644a61281` on
`overhaul/phase-2-externalization`. It has not been pushed to the current public
repository. The later checkpoint commit changes only this packet, the plan's
execution log and measured facts, and the history-tool documentation. The actual
publication rewrite must use that final prepared tip and recheck tree equality;
the rehearsal below is evidence for `d0433540`, not a claim about a later commit.

| Check | Result |
| --- | --- |
| Original source refs after scratch rewrite | Unchanged |
| Prepared and rewritten master trees | Both `788f88474c5811bd136662781fe039e472c54b0a` |
| Removed roots anywhere in rewritten history | None remain |
| Rewritten all-ref pack | 3,757,734 bytes (3.58 MiB) |
| Fresh master-only local clone, entire checkout | 23,670,784 allocated bytes (23.7 MB) |
| Commit map | 128 original commits: 9 unchanged, 114 rewritten, 5 removed |
| Local payload preservation after index-only removal | All 31,978 files, 12,552,340,272 bytes; hashes and recorded stat fields identical |
| Local archive-fetch rehearsals | Three campaigns, 330 files, all hashes verified |
| Python suites | 794 passed, 40 explicit native skips; 834 total in 35 suites |
| Packaging checks | Catalog, local links, workflow lint, frozen boundary, P0 and independent boundary review passed |

The [launch README](../README.md) is 147 lines, keeps the original image and
existing operating-context paragraphs, and preserves caveats and negative results.
The catalog describes 39 archive campaigns and 70 local-only inventory groups.
Local-only entries do not claim published payloads or complete file manifests.
Four small summaries/fixtures are copied byte-for-byte into `evidence/`.

No engine, workload, Rust test, Cargo input, proof contract or assertion changed.
The three changed protected inputs are the obsolete fixture-copy step in
`.github/workflows/verify.yml`, one archive link in the concurrent-verification
README, and two archive links in the P0 audit. Their lock was regenerated and
independently reviewed. A new full hosted pilot is still required after launch;
the successful Phase 1 pilot is not represented as a fresh Phase 2 proof run.

This checkpoint also requests approval of that exact packaging boundary at
`d0433540`, with frozen-lock SHA-256
`f86fb4df7b41d9945c135106f51eb5ded89ae7a03210703394184fc57bfbc6d3`.
The candidate diff and independent review are retained in the evidence packet.
The final documentation-only child must have the same 266 frozen inputs. Once
approved, mapped `d0433540` provides the independent, matching boundary for the
new master's full Verify run. Approval does not substitute for running it.

### Correction to the original history estimate

The first changed commit, `cf856f9649dabbabedcc34249819d237bc005a32`, retains its
tree, parent, author, committer and message; the rewrite strips its `gpgsig`
header. Changed parent IDs propagate thereafter. All 62 pre-evidence trees are
identical, but 53 of those commit IDs change. Across the prepared history, 17
signature headers disappear and one abbreviated commit reference in a message
is updated. Author and committer lines are unchanged. The five pruned commits
contain only removed evidence files. The map has exact source/target coverage
and no parent-mapping discrepancies. Original signatures, messages, commits,
PRs and runs that were already public remain in the original repository, which
will become the archive. The unpublished preparation/checkpoint commits are
retained in the independent local backup and mapped to published rewritten IDs;
their original IDs will not resolve in the public archive. The map's provenance
must make that distinction explicit. Do not push preparation branches to the
archive merely to make every map entry publicly resolvable.

## Operations proposed for approval

These operations run sequentially, with recorded readbacks. Stop if repository
identity, source head, archive pin, tree equality or resource admission differs
from the reviewed state. This approval would authorize the initial new-master
push; subsequent protected merges still require explicit owner approval.

1. **Back up locally.** Recheck guest/Windows space and memory. Make a fresh,
   independent mirror at `/home/zpconn/aerostore-archive.git`, outside this
   working tree, and retain a ref manifest. Refuse an existing destination.
   Capture repository settings, remote refs and the complete prepared tip.
2. **Tag the original public master.** Confirm repository ID `1167197125`
   still names `zpconn/aerostore` and master is
   `c22acd411cba7910f3f4f031fe9e27606c98ec9a`. Create and push annotated tag
   `archive/pre-rewrite` at that exact commit. Its message is
   `Preserve original Aerostore history and evidence before Phase 2 externalization.`
   An existing tag must resolve to the expected object or the operation stops.
3. **Rename the original repository** to `zpconn/aerostore-archive`. Disable
   its Actions and set its description to
   `Original Aerostore history and evidence. Active development: https://github.com/zpconn/aerostore.`
   It remains public and writable until the later archive checkpoint. PR #2
   remains unmerged. Recheck the repository ID, tag and master after renaming.
4. **Rewrite a fresh local mirror.** Fast-forward the local `master` ref to the
   final prepared tip without checking out the old tree over ignored evidence.
   Run the pinned rewrite in a new `runs/overhaul-phase2/2026-10-02/rewrite-publication-001/`
   directory. Require exact source/rewritten tree equality and retain the map,
   logs, report, original refs and rewritten refs. Publish neither other branches
   nor original bulk objects to the new repository.
5. **Create new public `zpconn/aerostore`** with no initialized README, license
   or source push. Record its new repository ID. Initially disable Actions;
   push only `refs/heads/master:refs/heads/master` from the rewritten mirror,
   without force. Set master as the default branch and apply the settings below.
   Re-enable Actions, then dispatch ordinary CI and an anchored Verify in review
   mode. Use the mapped ID of `d0433540fa6f04ad66b48f9977d8ab2644a61281`
   as Verify's baseline only after the exact boundary approval above. Require a
   distinct descendant as the published head, identical frozen inputs and
   `anchored:true`. The prior independent review against the approved Phase 1
   base remains retained. This avoids a redundant unanchored first-push pilot.
6. **Open a follow-up PR** in the new repository containing the actual
   `evidence/history-commit-map.tsv`, its provenance and publication-state updates.
   Use a new branch from a small fresh clone of rewritten new-repository master,
   never from this checkout's original history, and never push those changes
   straight to master. Add narrow catalog validation and tests for the exact map
   and provenance files; the current checker intentionally rejects unknown
   evidence files. Add launch-validation notes once qualification completes.
   Present its concrete diff and checks for owner merge approval; this checkpoint
   does not preapprove that merge.
7. **Repoint this checkout** to the new repository and fetch its master.
   Reconfirm exact prepared-tree equality, then use
   `git checkout --no-overwrite-ignore -B master origin/master`.
   Check all retained payload paths/hashes/stat fields against the pre-untrack
   inventory. Preserve ignored run outputs, binaries and tools. After the mirror
   and ref manifest are verified, remove only these old local branches:
   `overhaul/phase-0-inventory`, `overhaul/phase-1-ci`,
   `overhaul/phase-1-docs-probe`, `overhaul/phase-2-externalization`.
   Any unexpectedly present branch is retained. No original-object pruning.
8. **Qualify the public result.** Check a fresh public master-only clone is
   under 100 MB; require green CI and full anchored Verify; fetch the queue-profile,
   transactional-index and worker-failure campaigns through the canonical catalog;
   verify all hashes. Resolve five documented historical commits in the archive.
   Retain logs, receipts and hosted verification artifacts with hashes.

The new repository description will be:

> Shared-memory transactional database engine in Rust, built with AI agents and kept honest by formal verification, test oracles and benchmark sandboxes.

Topics: `rust`, `database`, `storage-engine`, `mvcc`, `shared-memory`,
`formal-verification`, `verus`, `lean4`, `tla-plus`, `ai-agents`.

Actions default to read-only token permissions, without permission to approve
PRs. Master protection carries forward the approved interim procedure:
up-to-date `ci` required from GitHub Actions app `15368`, one approval with
CODEOWNERS and stale-review dismissal, resolved conversations, no force pushes
or deletion, and `enforce_admins:false`. The owner checks applicable Verify
results and explicitly approves every protected merge. Verify is not a global
required status because its path filters intentionally skip docs-only PRs.
Administrator bypass remains available until the Phase 6 protection review.

## Resources, recovery and remaining checkpoints

Local preparation used a 20 GiB additional-storage allowance, 30 GiB free-space
reserves on both Linux and the Windows VHDX host, and a 4 GiB MemAvailable reserve.
The proposed next allowance is 32 GiB: 6.7 GiB for the retained backup, 13.4 GiB
for the fresh mirror and temporary repacking, 1 GiB for small clones/public
fetches, 10 GiB for hosted-pilot artifact downloads, and 0.9 GiB contingency.
Recompute the estimate if any measured component exceeds that allocation.
Use one large clone at a time and admit download batches separately. Recheck
resources before each batch; do not borrow the free-space reserves. No local
Cargo build or existing evidence cleanup is needed for publication.

The original `.git` and payload files remain present. Scratch repacking made the
test mirror small; it did not reclaim original-repository space or return VHDX
allocation to Windows. Preserve failed attempts and complete prior evidence.

If a step fails, stop outward changes and record the last completed operation,
repository IDs, refs, settings and next safe command. Resume by checking actual
state before repeating a step. Before name reuse, the renamed original remains
the complete source repository. After name reuse, use its explicit archive URL;
do not depend on redirects. Never automatically delete the new repository,
reverse a rename, reset a published ref or rewrite historical receipts as rollback.

After public qualification, request **separate approval** for original-object
reflog expiration/GC and making the archive read-only (2.5.9). Creating
`zpconn/aerostore-evidence` and publishing local-only campaigns (2.6) also has a
separate approval checkpoint. Neither is included here.

## Evidence and exact handoff

Local evidence, intentionally outside Git, is under
`runs/overhaul-phase2/2026-10-02/`:

- `rewrite-dry-run-001/report.json` and `commit-map.tsv`: measured rewrite.
- `rewritten-clone-check/receipt.json`: independent fresh-clone audit.
- `commit-map-review/`: signature, tree, message and parent-map audit.
- `catalog-build/` and `payload-preservation-after-untrack.json`: inventory and preservation.
- `fetch-tests/summary.json`: three successful local fixture fetches.
- `local-validation.json` and `python-unit-tests-final.log`: check results.
- `checkpoint2/`: exact source head, proposed API bodies, resource record,
  README audit, operation list and resume commands.

These local rehearsals do not qualify the future public archive. Phase 2 remains
incomplete until the publication checks and remaining approval steps are handled.
