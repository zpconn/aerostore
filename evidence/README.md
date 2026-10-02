# Evidence

The [catalog](catalog.json) separates exact tracked campaign payloads from
local-only evidence. It retains failed and diagnostic campaigns alongside the
accepted results. `current` identifies the campaign supporting the present
headline; it does not mean every enclosed trial passed. Older campaigns remain
diagnostic evidence unless a specific superseding decision is recorded.

**Publication status:** the original repository is published as
[`zpconn/aerostore-archive`](https://github.com/zpconn/aerostore-archive), at
`archive/pre-rewrite`, pinned to
`c22acd411cba7910f3f4f031fe9e27606c98ec9a` in each fetchable entry.
Public campaign-fetch qualification is separate from publication; the local
catalog check does not download remote payloads.

On October 2, 2026, canonical HTTPS fetches of these three campaigns resolved
`archive/pre-rewrite` to the pinned commit and passed every selected file's
size/hash check, without local source overrides:

| Campaign | Files | Bytes |
| --- | ---: | ---: |
| `hyperfeed_queue_profile_2026-09-30` | 276 | 4,200,258 |
| `transactional_indexes_2026-09-22` | 46 | 1,104,602 |
| `worker-failure-2026-09-24` | 8 | 16,943 |

This qualifies those three fetches, not all 39 archive campaigns. The 70 local
inventory groups remain local-only.

Each of the 39 archive campaigns has a new file manifest with SHA-256, size and
relative path for every tracked payload file. These manifests describe the
unchanged bytes in the original repository, including its existing receipts and
their historical limitations. The remaining 70 entries record local inventory
groups, including retained build/tool roots. They deliberately claim no public
fetch source or complete file-hash manifest. A mixed campaign may also identify
an untracked local supplement excluded from its archive manifest.

Fetch a published campaign into a fresh directory:

```sh
python3 scripts/fetch_evidence.py hyperfeed_queue_profile_2026-09-30
```

The fetcher uses a shallow partial sparse clone, checks the selected revision,
verifies the campaign manifest and every selected file, and writes under
`runs/evidence/`. Missing, unexpected, changed or unsafe paths fail verification.
It never overwrites an existing output. File-URL overrides are explicit local
fixture tests and do not qualify the public archive.

Check the small catalog and retained summaries without downloading campaigns:

```sh
python3 tools/evidence/check_catalog.py
```

[Summaries and fixtures](summaries.json) are byte-identical copies, checked
against the full campaign manifests. The [24-worker comparison](hyperfeed_queue_profile_2026-09-30/workers24-accepted-comparison01.json)
records the 6,400/s and 704/s tested endpoints and their caveats. The
[Crucible summary](transactional_indexes_2026-09-22/sustained-summary.json)
records the older sustained storage workload. These small records are not
self-contained executable reproductions: absolute paths and hashes in their
original content are intentionally unchanged. Local executable/runtime/history
dependencies require the separately approved Phase 2.6 publication packages.

Commit hashes in receipts dated before the history rewrite refer to the archive.
The [old-to-new commit map](history-commit-map.tsv) preserves the publication
rewrite's exact output: 129 original commits, nine unchanged, 115 rewritten and
five evidence-only commits removed. A zero new ID means the commit was removed.
The original archive retains the originally public commits, their signatures
and evidence.

The [map provenance](history-commit-map-provenance.json) binds its SHA-256, byte
count, totals and source-tip mapping. It distinguishes 127 originally public
commits from two unpublished local preparation commits (`d0433540` and
`ab57181a`); those two original IDs are not claimed to exist in the public archive.
The catalog checker validates the declared partition and local bindings. It does
not independently query archive reachability or verify remote payloads.

The original working checkout keeps all legacy evidence at its existing paths;
removing it from Git did not delete those files.

The inventory and publication scope are recorded in the
[overhaul plan](../OVERHAUL_PLAN.md). See the [disk-space runbook](../docs/disk-space.md)
before fetching large campaigns. Subsequent bulk payloads belong in the evidence
repository's approved releases, not in the main Git history.
