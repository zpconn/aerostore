#!/usr/bin/env python3
"""Fetch one cataloged archive campaign into a fresh runs/evidence directory.

Runtime dependencies: Python's standard library and Git with partial-clone and
sparse-checkout support. Local file:// overrides are explicitly fixture runs.
Existing evidence and the source repository are never modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
GIB = 1024**3
MIB = 1024**2
SHA = re.compile(r"[0-9a-f]{64}")
MAX_FILES = 100_000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def relative_path(value):
    require(isinstance(value, str) and value and len(value) <= 4096
            and not value.startswith("/") and "\\" not in value and "\0" not in value,
            "expected a canonical relative POSIX path")
    require(all(part not in {"", ".", "..", ".git"} and ":" not in part
                and not any(ord(char) < 32 for char in part) for part in value.split("/")),
            "unsafe or ambiguous path: " + value)
    return value


def local_path(root, name):
    path = root / relative_path(name)
    current = root
    for part in Path(name).parts:
        current /= part
        require(not current.is_symlink(), "symlink in local path: " + name)
    return path


def read_bounded(path, limit):
    require(stat.S_ISREG(path.lstat().st_mode), "expected a regular metadata file")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, "metadata exceeds size limit")
    return data


def integer(value):
    return type(value) is int and value >= 0


def metadata_json(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "duplicate JSON metadata key: " + key)
            value[key] = item
        return value
    result = json.loads(raw, object_pairs_hook=unique)
    require(isinstance(result, dict), "metadata must be a JSON object")
    return result


def load_campaign(root, catalog_name, campaign_id):
    catalog_raw = read_bounded(local_path(root, catalog_name), 4 * MIB)
    catalog = metadata_json(catalog_raw)
    require(type(catalog.get("schema_version")) is int and catalog["schema_version"] == 1
            and isinstance(catalog.get("campaigns"), list), "unsupported catalog schema")
    entries = catalog["campaigns"]
    require(all(isinstance(item, dict) for item in entries), "campaign entries must be objects")
    ids = [item.get("id") for item in entries]
    require(all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value)
                for value in ids) and len(set(ids)) == len(ids), "invalid or duplicate campaign IDs")
    require(campaign_id in ids, "campaign is absent from catalog")
    entry = entries[ids.index(campaign_id)]
    require(entry.get("availability") in {"archive", "local-only"}, "unsupported evidence availability")
    require(entry["availability"] == "archive", "campaign is local-only; no fetchable archive is claimed")
    source = entry.get("source", {})
    require(isinstance(source, dict) and isinstance(source.get("repo"), str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", source["repo"])
            and all(part not in {".", ".."} for part in source["repo"].split("/")), "invalid archive repository")
    require(isinstance(source.get("ref"), str) and source["ref"] and not source["ref"].startswith("-"), "explicit archive ref required")
    relative_path(source.get("path"))
    require(source.get("selection", "recursive") in {"recursive", "direct-files"}, "unsupported source selection")
    require("commit" not in source or (isinstance(source["commit"], str)
            and re.fullmatch(r"[0-9a-f]{40}", source["commit"])), "invalid archive commit pin")
    descriptor = entry.get("manifest", {})
    require(isinstance(descriptor, dict) and isinstance(descriptor.get("sha256"), str)
            and SHA.fullmatch(descriptor["sha256"]), "manifest SHA-256 required")
    raw = read_bounded(local_path(root, descriptor.get("path")), 32 * MIB)
    require(hashlib.sha256(raw).hexdigest() == descriptor["sha256"], "manifest SHA-256 mismatch")
    manifest = metadata_json(raw)
    require(type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1 and manifest.get("campaign_id") == campaign_id
            and isinstance(manifest.get("files"), list), "invalid campaign manifest")
    records = manifest["files"]
    require(0 < len(records) <= MAX_FILES, "manifest file count is empty or exceeds limit")
    files = {}
    for record in records:
        require(isinstance(record, dict), "manifest records must be objects")
        name = relative_path(record.get("path"))
        require(name not in files and integer(record.get("bytes")) and isinstance(record.get("sha256"), str)
                and SHA.fullmatch(record["sha256"]), "duplicate path or invalid file metadata")
        require(source.get("selection") != "direct-files" or "/" not in name, "direct-files manifest contains a nested path")
        files[name] = record
    require(not any(str(parent) in files for name in files for parent in PurePosixPath(name).parents if str(parent) != "."),
            "manifest has a file/directory collision")
    require(integer(entry.get("files")) and entry["files"] == len(files) and integer(entry.get("bytes"))
            and entry["bytes"] == sum(record["bytes"] for record in records), "catalog file/byte totals differ from manifest")
    return entry, files, hashlib.sha256(catalog_raw).hexdigest()


def usage(path):
    total = 0
    for directory, folders, files in os.walk(path, followlinks=False):
        folders[:] = [name for name in folders if not (Path(directory) / name).is_symlink()]
        for name in files:
            total += (Path(directory) / name).lstat().st_size
    return total


class Git:
    def __init__(self, work, max_bytes, reserve, deadline):
        self.work, self.max_bytes, self.reserve, self.deadline = work, max_bytes, reserve, deadline
        self.peak_bytes = 0
        self.environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        self.environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                                GIT_TERMINAL_PROMPT="0", GIT_ALLOW_PROTOCOL="https:file", GIT_NO_REPLACE_OBJECTS="1")

    def check_space(self, growth=0):
        current = usage(self.work)
        self.peak_bytes = max(self.peak_bytes, current)
        require(current + growth <= self.max_bytes, "fetch exceeds working storage budget")
        for volume in [self.work, *([Path("/mnt/c")] if Path("/mnt/c").exists() else [])]:
            require(shutil.disk_usage(volume).free >= self.reserve + growth, "fetch would cross filesystem free-space reserve")

    def run(self, *args, cwd=None, input_data=None):
        self.check_space()
        require(time.monotonic() < self.deadline, "fetch deadline expired")
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors, tempfile.TemporaryFile() as input_file:
            if input_data:
                input_file.write(input_data)
            input_file.seek(0)
            process = subprocess.Popen(["git", "--literal-pathspecs", "-c", "core.hooksPath=" + os.devnull,
                                        "-c", "core.autocrlf=false", *args], cwd=cwd or self.work,
                                       env=self.environment, stdin=input_file, stdout=output, stderr=errors,
                                       start_new_session=True)
            try:
                while process.poll() is None:
                    require(time.monotonic() < self.deadline, "fetch deadline expired")
                    self.check_space()
                    require(output.tell() <= 32 * MIB and errors.tell() <= MIB, "Git output exceeds diagnostic limit")
                    time.sleep(0.1)
                self.check_space()
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
            output.seek(0); errors.seek(0)
            data, error = output.read(32 * MIB + 1), errors.read(MIB + 1)
            require(len(data) <= 32 * MIB and len(error) <= MIB, "Git output exceeds diagnostic limit")
            require(b"filtering not recognized by server" not in error.lower(), "source server did not honor partial-clone filtering")
            require(process.returncode == 0, "Git command failed: " + error.decode(errors="replace")[-2000:])
            return data


def validate_tree(raw, source, files):
    prefix = source["path"] + "/"
    actual = set()
    for row in raw.split(b"\0"):
        if not row:
            continue
        metadata, encoded = row.split(b"\t", 1)
        mode, kind, _ = metadata.split()
        name = encoded.decode()
        require(name.startswith(prefix), "source path is not a directory")
        name = relative_path(name[len(prefix):])
        if source.get("selection") == "direct-files" and "/" in name:
            continue
        require(mode in {b"100644", b"100755"} and kind == b"blob", "archive contains a symlink or non-file entry")
        actual.add(name)
    require(actual == set(files), "archive tree has missing or unexpected files: " + repr(sorted(actual ^ set(files))[:20]))


def verify_payload(path, files):
    actual = set()
    for directory, folders, names in os.walk(path, followlinks=False):
        for name in folders:
            require(not (Path(directory) / name).is_symlink(), "symlink directory in fetched payload")
        for name in names:
            candidate = Path(directory) / name
            require(stat.S_ISREG(candidate.lstat().st_mode), "non-regular file in fetched payload")
            relative = candidate.relative_to(path).as_posix()
            require(relative in files, "unexpected fetched file: " + relative)
            expected = files[relative]
            require(candidate.stat().st_size == expected["bytes"], "fetched file size mismatch: " + relative)
            digest = hashlib.sha256()
            with candidate.open("rb") as stream:
                while chunk := stream.read(MIB):
                    digest.update(chunk)
            require(digest.hexdigest() == expected["sha256"], "fetched file SHA-256 mismatch: " + relative)
            actual.add(relative)
    require(actual == set(files), "missing fetched files: " + repr(sorted(set(files) - actual)[:20]))


def fetch(root, campaign_id, *, catalog="evidence/catalog.json", output=None, source_url=None,
          source_ref=None, max_bytes=8 * GIB, max_work_bytes=20 * GIB, reserve_bytes=30 * GIB, timeout=1800):
    root = Path(root).resolve()
    entry, files, catalog_sha = load_campaign(root, catalog, campaign_id)
    require(all(type(value) is int and value > 0 for value in (max_bytes, max_work_bytes, reserve_bytes, timeout)), "resource limits must be positive integers")
    require(entry["bytes"] <= max_bytes, "campaign exceeds payload byte limit")
    source = entry["source"]
    url = "https://github.com/" + source["repo"] + ".git"
    if source_url is not None:
        parsed = urlsplit(source_url)
        require(parsed.scheme == "file" and parsed.netloc in {"", "localhost"} and not parsed.query and not parsed.fragment
                and Path(unquote(parsed.path)).is_absolute(), "source override must be an absolute file:// fixture URL")
        url = source_url
    require(source_ref is None or source_url is not None, "source-ref override requires a local fixture URL")
    ref = source_ref or source["ref"]
    output_name = output or "runs/evidence/" + campaign_id
    destination = local_path(root, output_name)
    require(destination.is_relative_to(root / "runs/evidence") and destination != root / "runs/evidence", "output must be below runs/evidence")
    require(not destination.exists(), "evidence output already exists; choose a fresh path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".fetch-" + campaign_id + "-", dir=destination.parent))
    receipt = {"schema_version": 1, "campaign_id": campaign_id, "status": "failed", "source": source,
               "actual_url": url, "actual_ref": ref, "local_fixture": source_url is not None,
               "production_archive_qualified": False, "catalog_sha256": catalog_sha,
               "manifest_sha256": entry["manifest"]["sha256"], "output": str(destination)}
    receipt_path = staging / "fetch-receipt.json"
    git = Git(staging, max_work_bytes, reserve_bytes, time.monotonic() + timeout)
    try:
        git.check_space(2 * entry["bytes"] + 64 * MIB)
        git.run("check-ref-format", "--branch", ref)
        clone = staging / "repository"
        local_option = ["--upload-pack=git -c uploadpack.allowFilter=true upload-pack"] if source_url else []
        git.run("clone", "--quiet", "--no-checkout", "--depth=1", "--single-branch", "--filter=blob:none", "--sparse",
                "--branch=" + ref, *local_option, "--", url, str(clone))
        if source_url:
            git.run("config", "remote.origin.uploadpack", "git -c uploadpack.allowFilter=true upload-pack", cwd=clone)
        commit = git.run("rev-parse", "HEAD", cwd=clone).decode().strip()
        receipt["resolved_commit"] = commit
        require("commit" not in source or commit == source["commit"], "resolved archive commit differs from catalog pin")
        require(git.run("rev-parse", "--is-shallow-repository", cwd=clone).strip() == b"true", "clone is not shallow")
        require(git.run("config", "--get", "remote.origin.partialclonefilter", cwd=clone).strip() == b"blob:none", "clone is not configured as partial")
        validate_tree(git.run("ls-tree", "-r", "-z", "HEAD", "--", source["path"], cwd=clone), source, files)
        # Every sparse pattern is an exact file, quoted for Git's gitignore syntax.
        patterns = []
        for name in sorted(files):
            pattern = "/" + source["path"] + "/" + name
            patterns.append("".join("\\" + char if char in "\\*?[]#! " else char for char in pattern))
        git.run("sparse-checkout", "set", "--no-cone", "--stdin", cwd=clone,
                input_data=("\n".join(patterns) + "\n").encode())
        git.run("checkout", "--quiet", "--detach", "HEAD", cwd=clone)
        payload = clone / source["path"]
        verify_payload(payload, files)
        git.check_space()
        destination.mkdir(exist_ok=False)
        for name in sorted(files):
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            # Link verified bytes without another payload copy; O_EXCL semantics
            # preserve any concurrently created output. No source repo is touched.
            os.link(payload / name, target, follow_symlinks=False)
        verify_payload(destination, files)
        receipt.update(status="passed", files=len(files), bytes=entry["bytes"],
                       production_archive_qualified=source_url is None,
                       peak_observed_working_bytes=git.peak_bytes,
                       limits={"payload_bytes": max_bytes, "working_bytes": max_work_bytes,
                               "reserve_bytes_each": reserve_bytes, "timeout_seconds": timeout})
        # This is only this invocation's newly created clone; verified payload
        # links and the fetch receipt remain. Historical evidence is untouched.
        shutil.rmtree(clone)
    except Exception as error:
        receipt["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        with receipt_path.open("x") as stream:
            json.dump(receipt, stream, indent=2)
            stream.write("\n")
    return receipt, receipt_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--catalog", default="evidence/catalog.json")
    parser.add_argument("--output", help="fresh repository-relative path below runs/evidence")
    parser.add_argument("--source-url", help="file:// source override for local validation only")
    parser.add_argument("--source-ref", help="explicit local fixture ref; does not override a catalog commit pin")
    parser.add_argument("--max-bytes", type=int, default=8 * GIB)
    parser.add_argument("--max-work-bytes", type=int, default=20 * GIB)
    parser.add_argument("--reserve-bytes", type=int, default=30 * GIB)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    try:
        receipt, path = fetch(args.root, args.campaign, catalog=args.catalog, output=args.output,
                              source_url=args.source_url, source_ref=args.source_ref, max_bytes=args.max_bytes,
                              max_work_bytes=args.max_work_bytes, reserve_bytes=args.reserve_bytes, timeout=args.timeout)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, "Evidence fetch failed: " + str(error) + "\n")
    print(json.dumps({"status": receipt["status"], "output": receipt["output"], "receipt": str(path),
                      "resolved_commit": receipt["resolved_commit"], "local_fixture": receipt["local_fixture"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
