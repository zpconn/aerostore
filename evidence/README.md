# Evidence

The [catalog](catalog.json) separates exact tracked campaign payloads from
local-only evidence. It retains failed and diagnostic campaigns alongside the
accepted results. `current` identifies the campaign supporting the present
headline; it does not mean every enclosed trial passed. Older campaigns remain
diagnostic evidence unless a specific superseding decision is recorded.

**Publication status:** the archive rename and tag are prepared, pending owner
approval at Checkpoint 2. The planned archive is
[`zpconn/aerostore-archive`](https://github.com/zpconn/aerostore-archive), at
`archive/pre-rewrite`. Its payload commit is pinned in each fetchable entry.
Until publication, those planned URLs are not a claim that a public fetch works.

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
fixture tests and do not qualify the future public archive.

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
The old-to-new map will be published as `evidence/history-commit-map.tsv` in the
post-rewrite follow-up PR; it does not exist yet. Old hashes and original evidence
remain available in the archive. The working checkout keeps all legacy evidence
at its existing paths; removing it from Git does not delete those files.

The inventory and publication scope are recorded in the
[overhaul plan](../OVERHAUL_PLAN.md). See the [disk-space runbook](../docs/disk-space.md)
before fetching large campaigns. Subsequent bulk payloads belong in the evidence
repository's approved releases, not in the main Git history.
