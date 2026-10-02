#!/usr/bin/env python3
"""Check catalog manifests and retained summaries without fetching payloads."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("evidence_fetch", ROOT / "scripts/fetch_evidence.py")
fetch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch)


def check(root):
    catalog = fetch.metadata_json(fetch.read_bounded(fetch.local_path(root, "evidence/catalog.json"), 4 * fetch.MIB))
    fetch.require(catalog.get("schema_version") == 1, "unsupported catalog schema")
    archive_commit = catalog.get("archive_commit")
    fetch.require(isinstance(archive_commit, str) and fetch.re.fullmatch(r"[0-9a-f]{40}", archive_commit), "exact archive commit required")
    entries = catalog["campaigns"]
    names = [row["id"] for row in entries]
    fetch.require(len(set(names)) == len(names), "duplicate campaign identifier")
    allowed = {"evidence/README.md", "evidence/catalog.json", "evidence/summaries.json"}
    records = {}
    count = 0
    for entry in entries:
        fetch.require(entry["status"] in {"current", "superseded", "failed", "diagnostic"}, "invalid campaign status")
        if entry["availability"] == "local-only":
            fetch.require(not entry.get("source") and not entry.get("manifest"), "local-only entry claims a fetch source")
            continue
        selected, files, _ = fetch.load_campaign(root, "evidence/catalog.json", entry["id"])
        fetch.require(selected["source"].get("commit") == archive_commit, "campaign commit differs from pinned archive")
        allowed.add(selected["manifest"]["path"])
        count += 1
        for name, record in files.items():
            key = selected["source"]["path"] + "/" + name
            fetch.require(key not in records, "overlapping archive manifests")
            records[key] = (selected["source"].get("commit"), record)
    summaries = fetch.metadata_json(fetch.read_bounded(fetch.local_path(root, "evidence/summaries.json"), fetch.MIB))
    fetch.require(summaries.get("schema_version") == 1, "unsupported summary schema")
    seen = set()
    for row in summaries["files"]:
        name = fetch.relative_path(row["path"])
        fetch.require(name.startswith("evidence/") and name not in seen, "invalid or duplicate summary path")
        seen.add(name)
        allowed.add(name)
        data = fetch.read_bounded(fetch.local_path(root, name), 4 * fetch.MIB)
        fetch.require(len(data) == row["bytes"] and hashlib.sha256(data).hexdigest() == row["sha256"], "retained summary differs from recorded bytes")
        fetch.require(row["source_path"] in records, "summary source absent from archive manifests")
        commit, source = records[row["source_path"]]
        fetch.require(row["archive_commit"] == commit and source["bytes"] == row["bytes"]
                      and source["sha256"] == row["sha256"], "summary differs from archive manifest")
    actual = {path.relative_to(root).as_posix() for path in (root / "evidence").rglob("*") if path.is_file() or path.is_symlink()}
    fetch.require(actual == allowed, "unexpected or missing file in small evidence tree: " + repr(sorted(actual ^ allowed)))
    return {"status": "passed", "catalog_entries": len(entries), "archive_manifests": count,
            "archive_files": len(records), "retained_summaries_and_fixtures": len(seen),
            "remote_payloads_fetched_or_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(check(args.root.resolve()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
