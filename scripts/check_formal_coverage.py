#!/usr/bin/env python3
"""Fail closed when an experiment changes its contract or unproved boundary.

Fingerprints are a change detector, not a semantic refinement proof. The normal
verification runner never refreshes them. Rebaselining is an explicit reviewed
operation; --baseline-ref compares the protected files to an independent Git
revision so changing both a contract and its local lock cannot bless itself.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
LOCK = "verification/frozen_boundary.json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def frozen_paths(root: Path) -> list[str]:
    paths = {"Cargo.toml", "Cargo.lock", "aerostore_core/Cargo.toml",
             "aerostore_verified/Cargo.toml", "aerostore_tcl/Cargo.toml",
             "aerostore_macros/Cargo.toml", "verification/assumptions.toml",
             "verification/claims.toml", "verification/refinement_campaigns.json", "scripts/check_formal_coverage.py",
             "scripts/verify_formal.py", "scripts/verify_formal.sh",
             "verification/lean/roots.json", "verification/lean/lakefile.lean",
             "verification/lean/lake-manifest.json", "verification/lean/lean-toolchain",
             "verification/lean/AerostoreProofs/Contracts.lean",
             "verification/lean/AerostoreProofs/BridgeContracts.lean",
             "verification/bridge/toolchain.json", "verification/bridge/bootstrap.py"}
    for directory in ["aerostore_core", "aerostore_macros", "aerostore_tcl",
                      "verification/contracts", "verification/experiments/profiles",
                      "scripts", ".github", ".cargo"]:
        paths.update(str(p.relative_to(root)) for p in (root / directory).rglob("*")
                     if p.is_file() and not any(part in {"target", "__pycache__"} for part in p.relative_to(root).parts))
    for directory in ["verification/verus", "verification/tla", "verification/concurrent",
                      "verification/predicate", "verification/predicate_capture",
                      "verification/predicate_composition", "verification/skiplist_detach", "verification/postings"]:
        if not (root / directory).exists():
            continue
        paths.update(str(p.relative_to(root)) for p in (root / directory).iterdir()
                     if p.is_file() and p.suffix in {".py", ".json", ".tla", ".rs", ".md"}
                     and p.name not in {"kernels.verus.rs", "commit.verus.rs", "predicate.verus.rs", "capture.verus.rs", "composition.verus.rs", "detach.verus.rs", "postings.verus.rs"})
    # New build scripts, alternate Rust modules and Cargo/toolchain configuration
    # must not silently expand the one editable production source file.
    paths.update(str(p.relative_to(root)) for p in (root / "aerostore_verified").rglob("*")
                 if p.is_file() and not any(part in {"target", "__pycache__"} for part in p.relative_to(root).parts)
                 and str(p.relative_to(root)) != "aerostore_verified/src/lib.rs")
    for name in ["build.rs", "rust-toolchain", "rust-toolchain.toml"]:
        if (root / name).exists():
            paths.add(name)
    return sorted(paths)


def validate(root: Path, baseline_ref: str | None = None) -> dict:
    errors = []
    if baseline_ref:
        def baseline_bytes(path: str) -> bytes:
            return subprocess.check_output(["git", "show", f"{baseline_ref}:{path}"], cwd=root)
        locked_bytes = baseline_bytes(LOCK)
        lock = json.loads(locked_bytes)
        # The baseline defines the frozen set; a candidate cannot delete entries.
        if (root / LOCK).read_bytes() != locked_bytes:
            errors.append("candidate changed the baseline boundary lock")
    else:
        lock = json.loads((root / LOCK).read_text())
    actual_paths = frozen_paths(root)
    if set(actual_paths) != set(lock["files"]):
        errors.append("frozen boundary path set changed; review new/deleted engine or contract files")
    for name, expected in lock["files"].items():
        path = root / name
        if not path.is_file() or digest(path) != expected:
            errors.append(f"frozen boundary changed: {name}")
    claims = tomllib.loads((root / "verification/claims.toml").read_text())
    if claims.get("whole_engine_verified") is not False:
        errors.append("pilot cannot assert whole-engine verification")
    ids = [claim["id"] for claim in claims["claims"]]
    if len(ids) != len(set(ids)):
        errors.append("duplicate claim IDs")
    for claim in claims["claims"]:
        for name in [claim["contract"], *claim["implementation"]]:
            if not (root / name).is_file():
                errors.append(f"{claim['id']}: missing source {name}")
        if claim["status"] not in {"open", "in_progress", "partial"}:
            errors.append(f"unsupported static proof status for {claim['id']}; use runtime evidence")
    return {"passed": not errors, "errors": errors,
            "scope": "change detection only; not a whole-engine refinement proof",
            "checked_files": len(lock["files"]), "baseline_ref": baseline_ref,
            "anchoring": "independent_git_baseline" if baseline_ref else "local_bootstrap_only",
            "open_claims": [c["id"] for c in claims["claims"] if c["status"] == "open"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline-ref")
    parser.add_argument("--write-boundary", action="store_true",
                        help="explicit reviewed rebaseline; never called by verification or experiments")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.write_boundary:
        if args.baseline_ref:
            parser.error("rebaselining and comparison are separate operations")
        files = {name: digest(root / name) for name in frozen_paths(root)}
        value = {"format_version": 1,
                 "meaning": "Unproved engine and reviewed experiment contract are frozen; kernel/proof edits still require live verification.",
                 "files": files}
        (root / LOCK).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        print(f"Wrote {len(files)} boundary fingerprints; review this baseline before promotion.")
        return 0
    try:
        result = validate(root, args.baseline_ref)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        result = {"passed": False, "errors": [str(error)]}
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
