#!/usr/bin/env python3
"""Prepare a fresh evidence catalog; never modify the source payloads.

The archive manifests describe exact tracked bytes at --archive-commit. Local
inventory entries remain explicitly unavailable and are not file-hash manifests.
This migration tool requires the Phase 0 inventory, which stays outside Git.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess

ROOT = Path(__file__).resolve().parents[2]
BULK = ("docs/bench_data", "docs/verification_data", "docs/worker_failure_data")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as out:
        json.dump(value, out, indent=2, sort_keys=True)
        out.write("\n")


def digest_file(path):
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"Evidence is not a regular file: {path}")
    sha256, blob = hashlib.sha256(), hashlib.sha1()
    blob.update(f"blob {before.st_size}\0".encode())
    with path.open("rb") as src:
        while block := src.read(1024 * 1024):
            sha256.update(block)
            blob.update(block)
    after = path.lstat()
    fingerprint = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if fingerprint(before) != fingerprint(after):
        raise ValueError(f"Evidence changed during hashing: {path}")
    return {"bytes": before.st_size, "sha256": sha256.hexdigest(),
            "git_blob": blob.hexdigest(), "device": before.st_dev,
            "inode": before.st_ino, "mtime_ns": before.st_mtime_ns,
            "ctime_ns": before.st_ctime_ns, "mode": stat.S_IMODE(before.st_mode)}


def campaign_path(name):
    for base in BULK:
        if name.startswith(base + "/"):
            rest = name[len(base) + 1:]
            return base + "/" + (rest.split("/", 1)[0] if "/" in rest else "[root-files]")
    raise ValueError(f"Not a bulk payload path: {name}")


def campaign_id(path):
    if path.endswith("/[root-files]"):
        return path.split("/")[-2].replace("_data", "") + "-root-files"
    name = path.rsplit("/", 1)[-1]
    return "worker-failure-" + name if path.startswith("docs/worker_failure_data/") else name


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-commit", required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "evidence")
    parser.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.archive_commit):
        parser.error("archive commit must be an exact 40-character SHA")
    if args.output.exists() or args.audit_output.exists():
        parser.error("output and audit directories must both be fresh")
    inventory_bytes = args.inventory.read_bytes()
    inventory = json.loads(inventory_bytes)
    inventory_by_path = {item["path"]: item for item in inventory}
    tree = subprocess.check_output(["git", "--no-replace-objects", "ls-tree", "-r", "-z",
                                    args.archive_commit, "--", *BULK], cwd=ROOT)
    tracked = {}
    for record in tree.split(b"\0"):
        if record:
            metadata, name = record.split(b"\t", 1)
            mode, kind, oid = metadata.decode().split()
            if kind != "blob" or mode not in {"100644", "100755"}:
                raise ValueError("Non-regular tracked evidence")
            tracked[name.decode()] = (oid, mode)
    args.output.mkdir(parents=True)
    args.audit_output.mkdir(parents=True)
    groups, observed = defaultdict(list), defaultdict(lambda: {"files": 0, "bytes": 0})
    physical_count = physical_bytes = 0
    found = set()
    with (args.audit_output / "payloads-before.jsonl").open("x") as audit:
        for base in BULK:
            for directory, folders, files in os.walk(ROOT / base, followlinks=False):
                folders.sort()
                for folder in folders:
                    if (Path(directory) / folder).is_symlink():
                        raise ValueError("Symlink evidence directory")
                for file in sorted(files):
                    path = Path(directory) / file
                    name = path.relative_to(ROOT).as_posix()
                    item = digest_file(path)
                    group = campaign_path(name)
                    observed[group]["files"] += 1
                    observed[group]["bytes"] += item["bytes"]
                    physical_count += 1
                    physical_bytes += item["bytes"]
                    audit.write(json.dumps({"path": name, "tracked": name in tracked, **item}, sort_keys=True) + "\n")
                    if name in tracked:
                        oid, mode = tracked[name]
                        if oid != item["git_blob"]:
                            raise ValueError(f"Working payload differs from archive commit: {name}")
                        if (bool(item["mode"] & 0o111)) != (mode == "100755"):
                            raise ValueError(f"Evidence mode differs from archive commit: {name}")
                        found.add(name)
                        source = group.rsplit("/", 1)[0] if group.endswith("/[root-files]") else group
                        groups[group].append({"path": name[len(source) + 1:],
                                              "bytes": item["bytes"], "sha256": item["sha256"]})
    if found != set(tracked):
        raise ValueError("Some tracked evidence paths are absent locally")
    entries = []
    for path, rows in sorted(groups.items()):
        identifier = campaign_id(path)
        old = inventory_by_path.get(path, {})
        date = re.search(r"20\d\d-\d\d-\d\d", path)
        manifest_name = "manifests/" + identifier + ".json"
        manifest = args.output / manifest_name
        write_json(manifest, {"schema_version": 1, "campaign_id": identifier, "files": sorted(rows, key=lambda x: x["path"])})
        source = {"repo": "zpconn/aerostore-archive", "ref": "archive/pre-rewrite",
                  "commit": args.archive_commit, "path": path}
        if path.endswith("/[root-files]"):
            source.update(path=path.rsplit("/", 1)[0], selection="direct-files")
        current = identifier in {"hyperfeed_queue_profile_2026-09-30", "hyperfeed_socket_write_2026-09-30-v2"}
        entry = {"id": identifier, "date": date.group() if date else None,
                 "kind": "verification" if "/verification_data/" in path else "worker-failure" if "/worker_failure_data/" in path else "benchmark",
                 "status": "current" if current else "failed" if identifier == "hyperfeed_socket_write_2026-09-30" else "diagnostic",
                 "claims": old.get("headline_claims", []), "availability": "archive", "source": source,
                 "manifest": {"path": "evidence/" + manifest_name, "sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()},
                 "bytes": sum(row["bytes"] for row in rows), "files": len(rows),
                 "cited_by": sorted({row["doc"] for row in old.get("doc_citations", [])}),
                 "note": "Exact tracked archive payload. Diagnostic/current status does not classify every enclosed trial as passing. Local-only executable and history references may require the separately approved publication packages."}
        if observed[path]["files"] != len(rows):
            entry["local_supplement"] = {"files": observed[path]["files"] - len(rows),
                                         "bytes": observed[path]["bytes"] - entry["bytes"],
                                         "availability": "local-only", "note": "Untracked payloads remain at their original paths and are not included in this archive manifest."}
        entries.append(entry)
    for old in inventory:
        if old["path"] in groups:
            continue
        path = old["path"]
        identifier = "local-" + re.sub(r"[^A-Za-z0-9_.-]+", "-", path).strip("-")
        date = re.search(r"20\d\d-\d\d-\d\d", path)
        entries.append({"id": identifier, "date": date.group() if date else None,
                        "observed_date": "2026-10-02", "kind": "local-inventory", "status": "diagnostic",
                        "availability": "local-only", "local_path": path,
                        "claims": old.get("headline_claims", []), "bytes": old["logical_bytes"],
                        "files": old["file_count"], "cited_by": sorted({row["doc"] for row in old.get("doc_citations", [])}),
                        "note": "Phase 0 inventory observation, not a self-contained or file-hash-verified publication. Includes retained build/tool roots where named. No public fetch source is available; original local paths are preserved."})
    if len({entry["id"] for entry in entries}) != len(entries):
        raise ValueError("Duplicate catalog identifier")
    catalog = {"schema_version": 1, "publication_status": "planned-pending-checkpoint-2",
               "archive_commit": args.archive_commit,
               "inventory_provenance": {"date": "2026-10-02", "sha256": hashlib.sha256(inventory_bytes).hexdigest(),
                                        "scope": "Phase 0 inventory; manifests below freshly bind tracked archive payloads."},
               "campaigns": sorted(entries, key=lambda row: row["id"])}
    write_json(args.output / "catalog.json", catalog)
    write_json(args.audit_output / "catalog-build.json", {"status": "passed", "recorded_utc": datetime.now(timezone.utc).isoformat(),
               "archive_commit": args.archive_commit, "tracked_files": len(tracked),
               "tracked_bytes": sum(entry["bytes"] for entry in entries if entry["availability"] == "archive"),
               "physical_files_hashed": physical_count, "physical_bytes_hashed": physical_bytes,
               "archive_campaigns": len(groups), "catalog_entries": len(entries),
               "original_evidence_modified": False,
               "catalog_sha256": hashlib.sha256((args.output / "catalog.json").read_bytes()).hexdigest()})
    print(json.dumps({"catalog_entries": len(entries), "archive_campaigns": len(groups),
                      "tracked_files": len(tracked), "physical_files_hashed": physical_count}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
