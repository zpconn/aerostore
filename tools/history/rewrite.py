#!/usr/bin/env python3
"""Rewrite a new local scratch mirror; never rewrite the source repository."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import heapq
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
REQUIRED_PATHS = {
    "docs/bench_data", "docs/verification_data", "docs/worker_failure_data",
}
SHA = re.compile(r"[0-9a-f]{40}\Z")


class RewriteError(RuntimeError):
    pass


def environment() -> dict[str, str]:
    # Inherited GIT_DIR, alternates, config injection, or replacement objects
    # must not redirect a scratch-only operation into the original repository.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("GIT_", "PYTHON"))}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
               GIT_NO_REPLACE_OBJECTS="1", GIT_OPTIONAL_LOCKS="0",
               GIT_TERMINAL_PROMPT="0", PYTHONNOUSERSITE="1", LC_ALL="C")
    return env


def git(root: Path, *args: str) -> bytes:
    result = subprocess.run(["git", "--literal-pathspecs", "-C", str(root), *args],
                            env=environment(), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode:
        raise RewriteError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def save_json(path: Path, value: dict) -> None:
    # Every run gets a new directory. A failure report is never overwritten by
    # a later retry or converted into passing historical evidence.
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def read_config(path: Path) -> dict:
    config = json.loads(path.read_text())
    if not isinstance(config, dict) or set(config) != {"schema", "remove_paths", "largest_blobs"}:
        raise RewriteError("config must contain schema, remove_paths, largest_blobs only")
    if config["schema"] != 1 or type(config["schema"]) is not int:
        raise RewriteError("unsupported config schema")
    paths = config["remove_paths"]
    if not isinstance(paths, list) or not paths:
        raise RewriteError("remove_paths must be a nonempty list")
    for path in paths:
        if (not isinstance(path, str) or not path or path.startswith(("/", "-"))
                or any(ord(c) < 32 for c in path) or "\\" in path
                or any(p in {"", ".", "..", ".git"} for p in path.split("/"))
                or PurePosixPath(path).as_posix() != path):
            raise RewriteError(f"removal path must be a canonical repository-relative literal: {path!r}")
    if len(set(paths)) != len(paths) or not REQUIRED_PATHS <= set(paths):
        raise RewriteError("remove_paths must be unique and include all three bulk evidence roots")
    if type(config["largest_blobs"]) is not int or not 1 <= config["largest_blobs"] <= 100:
        raise RewriteError("largest_blobs must be an integer from 1 through 100")
    return config


def refs(root: Path) -> dict:
    rows = git(root, "for-each-ref", "--format=%(refname) %(objectname)").decode().splitlines()
    return {
        "refs": dict(row.split(" ", 1) for row in rows),
        "head": git(root, "rev-parse", "--verify", "HEAD").decode().strip(),
        "symbolic_head": git(root, "rev-parse", "--symbolic-full-name", "HEAD").decode().strip(),
    }


def forbidden_tip_paths(root: Path, head: str, removed: list[str]) -> list[str]:
    paths = git(root, "ls-tree", "-r", "--name-only", "-z", head).split(b"\0")
    prefixes = [path.encode() for path in removed]
    return [path.decode(errors="backslashreplace") for path in paths if path and
            any(path == prefix or path.startswith(prefix + b"/") for prefix in prefixes)]


def preflight_source(source: Path, source_ref: str, output: Path, removed: list[str]) -> dict:
    if not source.is_dir():
        raise RewriteError("--source must name an existing local Git repository")
    if output.exists() or output.is_symlink():
        raise RewriteError("output must not exist; choose a new directory for every run")
    common = Path(git(source, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()).resolve()
    if output == source or source.is_relative_to(output) or output.is_relative_to(common):
        raise RewriteError("output cannot contain the source or be inside its Git directory")
    if git(source, "rev-parse", "--show-object-format").strip() != b"sha1":
        raise RewriteError("this pinned rewrite supports SHA-1 Git repositories only")
    if git(source, "rev-parse", "--is-shallow-repository").strip() != b"false":
        raise RewriteError("source must contain complete history, not a shallow clone")
    config = git(source, "config", "--local", "--list").decode().lower()
    if "extensions.partialclone=" in config or re.search(r"remote\..*\.promisor=", config):
        raise RewriteError("source must be complete; partial/promisor repositories are not accepted")
    if (common / "info/grafts").exists() or any((common / "objects/pack").glob("*.promisor")):
        raise RewriteError("source grafts or promised objects require separate preparation")
    initial = refs(source)
    if any(name.startswith("refs/replace/") for name in initial["refs"]):
        raise RewriteError("source replacement refs require separate preparation")
    if not (SHA.fullmatch(source_ref) or source_ref.startswith("refs/heads/")):
        raise RewriteError("--source-ref must be a full branch ref or complete commit SHA")
    if not SHA.fullmatch(source_ref):
        git(source, "check-ref-format", source_ref)
    head = git(source, "rev-parse", "--verify", "--end-of-options", source_ref + "^{commit}").decode().strip()
    if not SHA.fullmatch(head):
        raise RewriteError("source ref did not resolve to a complete commit SHA")
    present = forbidden_tip_paths(source, head, removed)
    if present:
        raise RewriteError("source tip still contains removal paths; commit externalization first: "
                           + ", ".join(present[:10]))
    return {"path": str(source), "requested_ref": source_ref, "head": head,
            "tree": git(source, "rev-parse", head + "^{tree}").decode().strip(),
            "refs_before": initial, "git_common_dir": str(common)}


def verify_tool(path: Path, pin_path: Path) -> dict:
    pin = json.loads(pin_path.read_text())
    if (not isinstance(pin, dict) or not isinstance(pin.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", pin["sha256"])
            or not isinstance(pin.get("version_output"), str) or not pin["version_output"]):
        raise RewriteError("filter-repo pin requires sha256 and version_output")
    if not path.is_file() or path.is_symlink():
        raise RewriteError("--filter-repo must be a regular standalone Python source file")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != pin["sha256"]:
        raise RewriteError("git-filter-repo source hash differs from the reviewed pin")
    result = subprocess.run([sys.executable, "-I", str(path), "--version"], env=environment(),
                            capture_output=True, text=True)
    if result.returncode or result.stdout.strip() != pin["version_output"]:
        raise RewriteError("git-filter-repo version output differs from the reviewed pin")
    return {**pin, "path": str(path), "pin_path": str(pin_path)}


def logged(command: list[str], cwd: Path, path: Path) -> None:
    with path.open("xb") as stream:
        result = subprocess.run(command, cwd=cwd, env=environment(),
                                stdout=stream, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RewriteError(f"command failed ({result.returncode}); inspect {path}")


def commit_map_summary(data: bytes) -> dict:
    lines = data.decode("ascii").splitlines()
    if not lines or lines[0].split() != ["old", "new"]:
        raise RewriteError("invalid filter-repo commit-map header")
    counts = {"total": 0, "unchanged": 0, "rewritten": 0, "removed": 0}
    old_seen = set()
    for line in lines[1:]:
        pair = line.split()
        if len(pair) != 2 or not all(SHA.fullmatch(value) for value in pair) or pair[0] in old_seen:
            raise RewriteError("invalid or duplicate filter-repo commit-map entry")
        old, new = pair
        old_seen.add(old)
        counts["total"] += 1
        counts["removed" if new == "0" * 40 else "unchanged" if old == new else "rewritten"] += 1
    if not old_seen:
        raise RewriteError("empty filter-repo commit map")
    return counts


def largest_blobs(root: Path, limit: int) -> list[dict]:
    # Stream object metadata; do not load the historical payload bytes or the
    # entire object list into memory. Paths are representative Git names.
    env = environment()
    listing = subprocess.Popen(["git", "-C", str(root), "rev-list", "--objects", "--all"],
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sizing = subprocess.Popen(["git", "-C", str(root), "cat-file",
                               "--batch-check=%(objecttype) %(objectname) %(objectsize) %(rest)"],
                              env=env, stdin=listing.stdout, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    listing.stdout.close()
    largest = []
    try:
        for line in sizing.stdout:
            fields = line.rstrip(b"\n").split(b" ", 3)
            if len(fields) < 3:
                raise RewriteError("invalid object metadata during size report")
            if fields[0] == b"blob":
                row = (int(fields[2]), fields[1].decode(),
                       fields[3].decode(errors="backslashreplace") if len(fields) > 3 else "")
                heapq.heappush(largest, row)
                if len(largest) > limit:
                    heapq.heappop(largest)
        errors = sizing.stderr.read() + listing.stderr.read()
        if sizing.wait() or listing.wait():
            raise RewriteError(errors.decode(errors="replace"))
    finally:
        for process in (sizing, listing):
            if process.poll() is None:
                process.kill()
            process.wait()
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
    return [{"bytes": size, "oid": oid, "representative_path": path}
            for size, oid, path in sorted(largest, reverse=True)]


def rewrite(source: Path, source_ref: str, output: Path, config_path: Path,
            tool_path: Path, pin_path: Path) -> dict:
    source, output = source.resolve(), output.absolute()
    # Resolve the parent but preserve an existing final symlink for rejection.
    output = output.parent.resolve() / output.name
    config = read_config(config_path)
    prepared = preflight_source(source, source_ref, output, config["remove_paths"])
    tool = verify_tool(tool_path.absolute(), pin_path.resolve())
    output.mkdir()  # Parent must already exist; do not create arbitrary trees.
    mirror = output / "mirror.git"
    report = {"schema": 1, "started_utc": datetime.now(timezone.utc).isoformat(),
              "passed": False, "source": prepared, "output": str(output),
              "config": config, "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
              "tool": tool, "scope": "local scratch rewrite only; no publication or source pruning"}
    try:
        logged(["git", "clone", "--mirror", "--no-local", "--", str(source), str(mirror)],
               output, output / "clone.log")
        if (git(mirror, "rev-parse", "--is-bare-repository").strip() != b"true"
                or (mirror / "objects/info/alternates").exists()):
            raise RewriteError("scratch clone is not an independent bare mirror")
        if refs(source) != prepared["refs_before"]:
            raise RewriteError("source refs changed while cloning; use a new run directory")
        git(mirror, "cat-file", "-e", prepared["head"] + "^{commit}")
        # The prepared source branch need not be source master yet. Only this
        # newly created mirror's master is assigned to the recorded source tip.
        git(mirror, "update-ref", "refs/heads/master", prepared["head"])
        git(mirror, "symbolic-ref", "HEAD", "refs/heads/master")
        command = [sys.executable, "-I", tool["path"], "--invert-paths"]
        for path in config["remove_paths"]:
            command += ["--path", path]
        report["filter_command"] = command
        logged(command, mirror, output / "filter-repo.log")
        data = (mirror / "filter-repo/commit-map").read_bytes()
        with (output / "commit-map.tsv").open("xb") as stream:
            stream.write(data)
        report["commit_map"] = {"path": "commit-map.tsv", "sha256": hashlib.sha256(data).hexdigest(),
                                "counts": commit_map_summary(data)}
        rewritten = git(mirror, "rev-parse", "refs/heads/master").decode().strip()
        tree = git(mirror, "rev-parse", rewritten + "^{tree}").decode().strip()
        report["rewritten_master"] = {"head": rewritten, "tree": tree}
        report["tree_byte_identical"] = tree == prepared["tree"]
        if tree != prepared["tree"]:
            raise RewriteError("rewritten master tree differs from the exact prepared source tree")
        # Inspect history, not just the clean tip. Literal pathspecs also prevent
        # a configured path containing glob punctuation from expanding here.
        remaining = git(mirror, "rev-list", "--all", "--", *config["remove_paths"])
        report["removed_paths_absent_from_all_history"] = not remaining.strip()
        if remaining.strip():
            raise RewriteError("configured removal paths remain in rewritten history")
        packs = sorted((mirror / "objects/pack").glob("*.pack"))
        report["pack_bytes"] = sum(path.stat().st_size for path in packs)
        report["pack_files"] = [{"path": str(path.relative_to(mirror)), "bytes": path.stat().st_size}
                                for path in packs]
        report["git_count_objects"] = git(mirror, "count-objects", "-v").decode()
        report["largest_remaining_blobs"] = largest_blobs(mirror, config["largest_blobs"])
        report["rewritten_refs"] = refs(mirror)
        report["passed"] = True
    except (OSError, ValueError, RewriteError) as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            report["source_refs_after"] = refs(source)
            report["source_refs_unchanged"] = report["source_refs_after"] == prepared["refs_before"]
        except (OSError, RewriteError) as error:
            report["source_refs_unchanged"] = False
            report["source_check_error"] = str(error)
        if not report["source_refs_unchanged"]:
            report["passed"] = False
            report.setdefault("error", "source refs changed during the run")
        if "error" in report:
            report["passed"] = False
        report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save_json(output / "report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="local original repository (read only)")
    parser.add_argument("--source-ref", required=True, help="prepared refs/heads/... or complete commit SHA")
    parser.add_argument("--output", required=True, type=Path, help="new scratch directory; parent must exist")
    parser.add_argument("--config", type=Path, default=HERE / "removal-rules.json")
    parser.add_argument("--filter-repo", required=True, type=Path, help="pinned standalone git_filter_repo.py")
    parser.add_argument("--tool-pin", type=Path, default=HERE / "filter-repo-pin.json")
    args = parser.parse_args()
    try:
        report = rewrite(args.source, args.source_ref, args.output, args.config,
                         args.filter_repo, args.tool_pin)
    except (OSError, ValueError, RewriteError) as error:
        print(f"Rewrite refused before cloning: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"passed": report["passed"], "report": str(args.output / "report.json"),
                      "error": report.get("error")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
