# Local history rewrite

`rewrite.py` removes configured historical paths in a **new scratch mirror**.
It never rewrites the source checkout, pushes refs, creates archive tags, or
prunes the original object store. Phase 2 publication still needs Checkpoint 2
approval; a successful local report does not authorize those operations.

The prepared source commit must already remove every configured path from its
tree. For evidence externalization, remove paths from the index with
`git rm --cached` and preserve the original files in place, as specified in
`OVERHAUL_PLAN.md`. Commit the prepared index before running this tool. A tip
that still contains payloads is rejected before cloning. The invariant compares
the resulting `master` tree directly with that exact prepared commit; it never
compares against an implicitly modified source tree.

## Prerequisites and admission

- Python 3.10+ and Git with `--end-of-options` and `--path-format` support.
- The standalone `git-filter-repo` v2.47.0 script matching
  [filter-repo-pin.json](filter-repo-pin.json). The pin records both its SHA-256
  and `--version` output. Installation is separate; this tool downloads nothing.
  The project installation is
  `.tools/history/git-filter-repo/v2.47.0/git-filter-repo`.
- A complete local source repository, with no shallow/partial history, grafts,
  or replacement refs. Original branches and evidence may remain present.
- A new output directory whose parent exists. Existing directories, symlinks,
  and locations inside the source Git directory are rejected. A failed output
  is retained; use a different name for a retry.

Before a real mirror run, follow [the disk-space runbook](../../docs/disk-space.md):
measure allocated usage and Linux/Windows free space, record the source pack
size and additional-storage allowance, and admit the batch with the campaign
owner. A mirror plus temporary repacking can coexist, so budget for both.
The Phase 2 campaign has a shared 20 GiB additional-output allowance, 30 GiB
Linux/Windows reserves, and a 4 GiB available-memory reserve. Only one large
clone runs at a time. This tool does not replace that shared admission decision
or silently delete an old attempt to obtain space.

## Run after a prepared commit and batch admission

From the repository root, with a previously created scratch parent:

```sh
python3 tools/history/rewrite.py \
  --source "$PWD" \
  --source-ref refs/heads/overhaul/phase-2-externalization \
  --output runs/history/dry-run-001 \
  --filter-repo .tools/history/git-filter-repo/v2.47.0/git-filter-repo
```

For an immutable selection, pass the complete prepared commit SHA instead of a
branch ref. The report always records the supplied ref and resolved commit.
Uncommitted changes are not used as rewrite inputs.

The script creates `mirror.git` using `git clone --mirror --no-local`, which
avoids shared object stores and hardlinks. It assigns **only the scratch
mirror's** `master` to the selected prepared commit, then invokes the pinned
filter with `--invert-paths` and each configured `--path`. It retains
`git-filter-repo`'s fresh-clone check and never passes `--force`. The filter's
normal cleanup/repack acts only inside that scratch mirror. Source refs are
checked before and after; a concurrent source ref change makes the run fail.

`removal-rules.json` contains the three bulk-evidence roots. Add literal file or
directory paths for later removals; they do not require code changes. Paths
are repository-relative, not globs, regular expressions, shell commands, or
arbitrary filter callbacks. Historical renames require listing every old
path. Any additional path must also be absent from the prepared tip.

## Outputs and checks

Each output directory retains:

- `mirror.git/`: the rewritten local mirror; all cloned refs are filtered.
- `clone.log` and `filter-repo.log`: command output, including failure details.
- `commit-map.tsv`: the filter's byte-identical `mirror.git/filter-repo/commit-map`.
- `report.json`: source ref/head/tree and original refs, tool/config hashes,
  unchanged/rewritten/removed commit counts, new pack bytes, largest reachable
  blobs across all rewritten refs, and invariant results.

A pass requires an identical Git tree ID at rewritten `master` and the selected
source commit. This binds all file bytes, names, modes, symlinks, and submodule
IDs in the tree. It also requires no configured removal path anywhere in the
rewritten reachable history, plus unchanged original refs. Pack size covers
the entire scratch mirror, not a later master-only clone. The report does not
claim that a fresh published clone is under 100 MB; Phase 2.5 checks that later.

The old-to-new map includes unchanged commits and all-zero new IDs for removed
commits. Do not use it to mutate historical receipts: old receipt hashes still
refer to the original archive. Only rewritten `master` is intended for later
publication, through the separately approved Phase 2.5 procedure. Do not mirror
push the scratch repository.

Preflight refusals exit nonzero without creating the output. Failures after
creation retain the scratch directory, logs, and a failing report. No output
or original evidence is automatically removed on success or failure.

## Focused tests

```sh
python3 -m unittest discover -s tools/history -p 'test_*.py' -v
```

Tests use tiny temporary Git repositories, including a historical payload,
an index-only externalization commit, additional literal rules, and tampered
results. They check source preservation, early refusal, reproducible retries
with new output directories, failure reports, commit maps, and tree equality.
Integration cases explicitly skip when the pinned tool is absent; install and
verify it before treating the full suite as validated. No project builds,
large mirror, network download, or production evidence cleanup is performed.

Upstream behavior and options are described in the
[git-filter-repo manual](https://github.com/newren/git-filter-repo/blob/6f79afc8c90c592a3052e6cc53c2ca8907515bca/Documentation/git-filter-repo.txt).
