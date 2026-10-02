#!/usr/bin/env python3
"""Check the tracked proof-input lock, or a strict independent experiment boundary.

Fingerprints detect byte changes; they do not prove semantic refinement. Ordinary
CI checks lock consistency and reports --review-base changes for owner review.
Experiments use --baseline-ref with this checker loaded from that base revision:
changing both an input and the candidate lock cannot authorize an experiment.
"""
from __future__ import annotations

import argparse
import hashlib
from importlib.machinery import PathFinder
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
LOCK = "verification/frozen_boundary.json"
CLAIMS = "verification/claims.toml"
# Keep explicit: benchmark orchestration and unrelated script tests are not
# proof inputs. Adding a verification entry point requires reviewing this list.
TOOLING = {
    "scripts/" + name for name in (
        "check_formal_coverage.py", "check_lean.py", "check_lock_models.py",
        "check_p0_contracts.py", "check_p1_native_evidence.py",
        "check_planning_native_evidence.py", "check_refinement_evidence.py",
        "check_tla.py", "tlc_download.py", "test_tlc_download.py",
        "setup_verification.py", "run_verified_experiment.py",
        "verify_formal.py", "verify_formal.sh", "test_formal_gate.py",
        "test_p0_contracts.py", "test_tla_runner.py", "ci_formal_anchor.py",
        "ci_formal_pilot.py", "ci_formal_status.py", "test_ci_formal.py",
        "tests/test_formal_coverage.py", "tests/test_verify_triggers.py",
    )
} | {".github/workflows/formal.yml", ".github/workflows/verify.yml", ".github/CODEOWNERS"}
BUILD_NAMES = {"Cargo.toml", "Cargo.lock", "build.rs", "rust-toolchain", "rust-toolchain.toml"}
VERIFICATION_SUFFIXES = {".py", ".sh", ".rs", ".lean", ".tla", ".cfg", ".json", ".toml"}
OUTPUT_DIRECTORIES = {"target", "__pycache__", ".lake", "evidence", "generated"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_name(name: str) -> str:
    """Require an unambiguous repository-relative Git path before any file read."""
    if (not isinstance(name, str) or not name or "\\" in name
            or any(ord(char) < 32 for char in name)):
        raise ValueError(f"invalid boundary path: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != name or name == ".":
        raise ValueError(f"invalid boundary path: {name!r}")
    return name


def input_path(root: Path, name: str) -> Path:
    path = root / checked_name(name)
    # Reject even in-tree symlinks: their targets are not necessarily tracked or
    # frozen, and parent-directory symlinks could redirect reads outside root.
    for part in (path, *path.parents):
        if part == root:
            break
        if part.is_symlink():
            raise ValueError(f"symlink is not a proof input: {name}")
    return path


def tracked_paths(root: Path) -> set[str]:
    output = subprocess.check_output(["git", "ls-files", "--cached", "-z", "--full-name"], cwd=root)
    return {checked_name(name.decode()) for name in output.split(b"\0") if name}


def claim_paths(claims: dict) -> set[str]:
    paths = set()
    for claim in claims["claims"]:
        references = [claim["contract"], *claim["implementation"], *claim.get("tests", [])]
        for name in references:
            if checked_name(name) == LOCK:
                raise ValueError("the boundary lock cannot cite itself as a proof input")
            paths.add(name)
    return paths


def is_proof_input(name: str) -> bool:
    path = PurePosixPath(name)
    if (name in TOOLING or name.startswith(".cargo/")
            or len(path.parts) == 3 and path.parts[1] == ".cargo"):
        return True
    if name.startswith(("aerostore_core/src/", "aerostore_verified/src/", "aerostore_macros/src/")):
        return True
    # Root/workspace package build inputs, not archived source snapshots.
    if path.name in BUILD_NAMES and len(path.parts) <= 2:
        return True
    if not name.startswith("verification/") or name == LOCK:
        return False
    if any(part in OUTPUT_DIRECTORIES for part in path.parts):
        return False
    # Verus outputs are checked against render() by their runners. Freeze the
    # generators/templates, not the regenerated output. Lean source remains an
    # input to Lake; unused extraction metadata and LLBC are not proof inputs.
    if name.endswith(".verus.rs") or name == "verification/lean/AerostoreProofs/translation.json":
        return False
    return (name.startswith("verification/contracts/") or path.name == "lean-toolchain"
            or path.suffix in VERIFICATION_SUFFIXES)


def frozen_paths(root: Path) -> list[str]:
    tracked = tracked_paths(root)
    if CLAIMS not in tracked:
        raise ValueError(f"claim ledger is not tracked: {CLAIMS}")
    claims = tomllib.loads(input_path(root, CLAIMS).read_text())
    cited = claim_paths(claims)
    missing = cited - tracked
    if missing:
        raise ValueError("claim references must be tracked: " + ", ".join(sorted(missing)))
    # Explicit citations include contract docs and native tests which the broad
    # selectors intentionally omit. Untracked/ignored files never enter the set.
    return sorted({name for name in tracked if is_proof_input(name)} | cited)


def ambient_input_errors(root: Path, paths: list[str]) -> list[str]:
    """Reject implicit build/import overrides, without freezing scratch files.

    Git defines the lock. Cargo's automatically loaded controls must still be
    tracked, and Python must resolve protected modules to their checked source.
    These checks inspect paths/specs only; they never import candidate modules.
    """
    errors = []
    tracked = tracked_paths(root)
    build_roots = {PurePosixPath(".")} | {
        PurePosixPath(name).parent for name in paths if PurePosixPath(name).name == "Cargo.toml"}
    for directory in sorted(build_roots):
        for control in (".cargo/config", ".cargo/config.toml", "rust-toolchain", "rust-toolchain.toml", "build.rs"):
            name = (directory / control).as_posix()
            if input_path(root, name).exists() and name not in tracked:
                errors.append(f"untracked active build control: {name}")
    PathFinder.invalidate_caches()
    python_directories = set()
    for name in paths:
        path = input_path(root, name)
        if path.suffix != ".py" or not name.startswith(("scripts/", "verification/")):
            continue
        python_directories.add(path.parent)
        spec = PathFinder.find_spec(path.stem, [str(path.parent)])
        if spec is None or spec.origin != str(path):
            errors.append(f"protected Python module is shadowed or missing: {name}")
    # Runners also launch ordinary Python subprocesses. Local stdlib substitutes
    # could affect those children even if the top-level checker used python -I.
    reserved = sys.stdlib_module_names | {"sitecustomize", "usercustomize"}
    for directory in sorted(python_directories):
        local_names = {path.name.split(".")[0] for path in directory.iterdir()} & reserved
        for name in sorted(local_names):
            spec = PathFinder.find_spec(name, [str(directory)])
            if spec is not None and spec.origin is not None:
                errors.append(f"local Python module shadows standard library: {Path(spec.origin).relative_to(root)}")
    return errors


def read_lock(contents: bytes) -> dict:
    lock = json.loads(contents)
    if lock.get("format_version") != 1 or not isinstance(lock.get("files"), dict):
        raise ValueError("unsupported boundary lock format")
    for name, expected in lock["files"].items():
        if checked_name(name) == LOCK or not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError(f"invalid boundary fingerprint: {name!r}")
    return lock


def baseline_lock(root: Path, revision: str) -> bytes:
    # Resolve first so a caller-supplied revision cannot become a Git option or
    # an alternative object/path expression in the subsequent blob lookup.
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", revision + "^{commit}"],
        cwd=root, text=True).strip()
    return subprocess.check_output(["git", "cat-file", "blob", f"{commit}:{LOCK}"], cwd=root)


def changed_inputs(before: dict[str, str], after: dict[str, str | None]) -> dict[str, list[str]]:
    return {"added": sorted(after.keys() - before.keys()),
            "removed": sorted(before.keys() - after.keys()),
            "modified": sorted(name for name in before.keys() & after.keys() if before[name] != after[name])}


def validate(root: Path, baseline_ref: str | None = None, review_base: str | None = None) -> dict:
    if baseline_ref and review_base:
        raise ValueError("strict comparison and reviewed boundary changes are separate operations")
    root = root.resolve()
    errors = []
    candidate_bytes = input_path(root, LOCK).read_bytes()
    if baseline_ref:
        locked_bytes = baseline_lock(root, baseline_ref)
        lock = read_lock(locked_bytes)
        if candidate_bytes != locked_bytes:
            errors.append("candidate changed the baseline boundary lock")
    else:
        lock = read_lock(candidate_bytes)
    actual_paths = frozen_paths(root)
    errors.extend(ambient_input_errors(root, actual_paths))
    if set(actual_paths) != set(lock["files"]):
        errors.append("frozen boundary path set changed; review new/deleted engine or contract files")
    # Always read the baseline's entire set in strict mode, even when the
    # candidate ledger removed a citation. A candidate cannot narrow its judge.
    actual = {}
    for name in sorted(set(actual_paths) | set(lock["files"])):
        path = input_path(root, name)
        actual[name] = digest(path) if path.is_file() else None
        if name in lock["files"] and actual[name] != lock["files"][name]:
            errors.append(f"frozen boundary changed: {name}")
    claims = tomllib.loads(input_path(root, CLAIMS).read_text())
    if claims.get("whole_engine_verified") is not False:
        errors.append("pilot cannot assert whole-engine verification")
    if claims.get("full_P1_complete") is not False:
        errors.append("component pilot cannot assert full P1 completion")
    ids = [claim["id"] for claim in claims["claims"]]
    if len(ids) != len(set(ids)):
        errors.append("duplicate claim IDs")
    for claim in claims["claims"]:
        for name in [claim["contract"], *claim["implementation"], *claim.get("tests", [])]:
            if not input_path(root, name).is_file():
                errors.append(f"{claim['id']}: missing source {name}")
        if claim["status"] not in {"open", "in_progress", "partial"}:
            errors.append(f"unsupported static proof status for {claim['id']}; use runtime evidence")
    result = {"passed": not errors, "errors": errors,
              "scope": "change detection only; not a whole-engine refinement proof",
              "checked_files": len(lock["files"]), "baseline_ref": baseline_ref,
              "anchoring": "independent_git_baseline" if baseline_ref else "local_bootstrap_only",
              "open_claims": [c["id"] for c in claims["claims"] if c["status"] == "open"]}
    if review_base:
        previous = read_lock(baseline_lock(root, review_base))
        result.update(review_base=review_base,
                      changed_inputs=changed_inputs(previous["files"], {name: actual[name] for name in actual_paths}))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--baseline-ref", help="strict comparison against an independent experiment baseline")
    mode.add_argument("--review-base", help="report changed proof inputs for review; check local lock consistency")
    mode.add_argument("--write-boundary", action="store_true",
                      help="write an explicit reviewed rebaseline; never an experiment operation")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.write_boundary:
            paths = frozen_paths(root)
            errors = ambient_input_errors(root, paths)
            if errors:
                raise ValueError("; ".join(errors))
            files = {name: digest(input_path(root, name)) for name in paths}
            value = {"format_version": 1,
                     "meaning": "Tracked engine and proof inputs; byte-change detection only. Boundary changes require owner review; all proofs still require live verification.",
                     "files": files}
            input_path(root, LOCK).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
            print(f"Wrote {len(files)} boundary fingerprints; review this baseline before promotion.")
            return 0
        result = validate(root, args.baseline_ref, args.review_base)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        result = {"passed": False, "errors": [str(error)]}
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
