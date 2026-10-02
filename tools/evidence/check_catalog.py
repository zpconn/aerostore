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


HISTORY_MAP = "evidence/history-commit-map.tsv"
HISTORY_PROVENANCE = "evidence/history-commit-map-provenance.json"


def check_history_map(root, catalog):
    """Check local map/provenance bindings, without claiming remote reachability."""
    map_path = fetch.local_path(root, HISTORY_MAP)
    provenance_path = fetch.local_path(root, HISTORY_PROVENANCE)
    if not map_path.exists() and not provenance_path.exists():
        return None
    fetch.require(map_path.exists() and provenance_path.exists(),
                  "history map and provenance must be supplied together")
    raw = fetch.read_bounded(map_path, 4 * fetch.MIB)
    provenance = fetch.metadata_json(fetch.read_bounded(provenance_path, fetch.MIB))

    def shape(value, keys, label):
        fetch.require(isinstance(value, dict) and set(value) == set(keys),
                      "invalid history " + label + " fields")

    def commit(value, label):
        fetch.require(isinstance(value, str) and fetch.re.fullmatch(r"[0-9a-f]{40}", value)
                      and value != "0" * 40, "invalid history " + label + " commit")

    shape(provenance, {"schema_version", "map", "source", "archive", "classification"}, "provenance")
    fetch.require(type(provenance["schema_version"]) is int and provenance["schema_version"] == 1,
                  "unsupported history provenance schema")
    descriptor = provenance["map"]
    shape(descriptor, {"path", "sha256", "bytes", "counts"}, "map descriptor")
    fetch.require(descriptor["path"] == HISTORY_MAP, "history map path must be the exact allowed path")
    fetch.require(type(descriptor["bytes"]) is int and descriptor["bytes"] == len(raw),
                  "history map byte count differs")
    fetch.require(isinstance(descriptor["sha256"], str)
                  and fetch.re.fullmatch(r"[0-9a-f]{64}", descriptor["sha256"])
                  and hashlib.sha256(raw).hexdigest() == descriptor["sha256"], "history map SHA-256 differs")

    # Preserve the pinned filter-repo output byte-for-byte, including its header,
    # single-space row separators, lexicographic old-ID order, and final LF.
    header = b"old" + b" " * 38 + b"new\n"
    fetch.require(raw.startswith(header) and raw.endswith(b"\n"), "invalid history map header or final LF")
    rows = raw[len(header):].splitlines(keepends=True)
    fetch.require(rows, "history map must not be empty")
    mapping = {}
    previous = ""
    measured = {"total": 0, "unchanged": 0, "rewritten": 0, "removed": 0}
    for line in rows:
        fetch.require(fetch.re.fullmatch(rb"[0-9a-f]{40} [0-9a-f]{40}\n", line),
                      "invalid history map row")
        old, new = line[:-1].decode("ascii").split(" ")
        commit(old, "old")
        fetch.require(old > previous, "history map old IDs must be unique and sorted")
        previous = old
        mapping[old] = new
        measured["total"] += 1
        measured["removed" if new == "0" * 40 else "unchanged" if old == new else "rewritten"] += 1
    shape(descriptor["counts"], measured, "map counts")
    fetch.require(all(type(value) is int and value >= 0 for value in descriptor["counts"].values())
                  and descriptor["counts"] == measured, "history map counts differ from rows")

    classes = provenance["classification"]
    shape(classes, {"originally_public", "unpublished_preparation"}, "classification")
    for label, values in classes.items():
        fetch.require(isinstance(values, list), "history classification must contain lists")
        for value in values:
            commit(value, label)
        fetch.require(values == sorted(set(values)), "history classification IDs must be unique and sorted")
    public, unpublished = set(classes["originally_public"]), set(classes["unpublished_preparation"])
    fetch.require(not public & unpublished and public | unpublished == set(mapping),
                  "history classification must partition every old map ID exactly once")

    source = provenance["source"]
    shape(source, {"original_tip", "rewritten_tip"}, "source")
    commit(source["original_tip"], "source")
    commit(source["rewritten_tip"], "rewritten")
    fetch.require(source["original_tip"] in unpublished, "history source tip must be unpublished preparation")
    fetch.require(mapping.get(source["original_tip"]) == source["rewritten_tip"],
                  "history source tip mapping differs")

    archive = provenance["archive"]
    shape(archive, {"repo", "ref", "commit", "original_public_branches"}, "archive")
    commit(archive["commit"], "archive")
    fetch.require(archive["commit"] == catalog["archive_commit"] and archive["commit"] in public,
                  "history archive commit differs from catalog or is not classified public")
    entries = [entry for entry in catalog["campaigns"] if entry["availability"] == "archive"]
    fetch.require(entries and all(archive["repo"] == entry["source"]["repo"]
                                  and archive["ref"] == entry["source"]["ref"] for entry in entries),
                  "history archive repository/ref differs from catalog")
    branches = archive["original_public_branches"]
    fetch.require(isinstance(branches, dict) and branches, "history public branch snapshot required")
    for name, tip in branches.items():
        fetch.require(isinstance(name, str) and fetch.re.fullmatch(r"refs/heads/[A-Za-z0-9][A-Za-z0-9._/-]*", name),
                      "invalid history public branch name")
        commit(tip, "public branch")
        fetch.require(tip in public, "history public branch tip is not classified public")
    fetch.require(branches.get("refs/heads/master") == archive["commit"],
                  "history original public master differs from archive pin")
    return {"map_sha256": descriptor["sha256"], "counts": measured,
            "originally_public_commits_declared": len(public),
            "unpublished_preparation_commits_declared": len(unpublished),
            "archive_reachability_verified": False}


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
    history = check_history_map(root, catalog)
    if history is not None:
        allowed.update({HISTORY_MAP, HISTORY_PROVENANCE})
    actual = {path.relative_to(root).as_posix() for path in (root / "evidence").rglob("*") if path.is_file() or path.is_symlink()}
    fetch.require(actual == allowed, "unexpected or missing file in small evidence tree: " + repr(sorted(actual ^ allowed)))
    return {"status": "passed", "catalog_entries": len(entries), "archive_manifests": count,
            "archive_files": len(records), "retained_summaries_and_fixtures": len(seen),
            "history_commit_map": history, "remote_payloads_fetched_or_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(check(args.root.resolve()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
