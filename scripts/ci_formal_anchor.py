#!/usr/bin/env python3
"""Check a candidate with an independently loaded formal-boundary checker.

CI extracts this standalone, stdlib-only script from the requested base commit
and runs it with Python's -I flag. Do not add repository-local imports: neither
this helper nor the checker may import code from the candidate checkout.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ANCHOR = "scripts/ci_formal_anchor.py"
CHECKER = "scripts/check_formal_coverage.py"
LOCK = "verification/frozen_boundary.json"
SHA = re.compile(r"[0-9a-f]{40}\Z")


def git(root: Path, *arguments: str) -> bytes:
    return subprocess.check_output(
        ["git", *arguments], cwd=root, stderr=subprocess.PIPE,
    )


def blob(root: Path, baseline: str, path: str) -> bytes | None:
    # Tree lookup distinguishes an absent path from a failed lazy blob fetch in
    # a partial clone. An existing but unreadable object must fail closed.
    entries = git(root, "ls-tree", "--full-tree", "-z", baseline, "--", path)
    if not entries:
        return None
    records = entries.split(b"\0")
    if len(records) != 2 or records[-1]:
        raise ValueError(f"ambiguous base tree entry: {path}")
    metadata, separator, name = records[0].partition(b"\t")
    fields = metadata.split()
    if (not separator or name != path.encode() or len(fields) != 3
            or fields[0] not in {b"100644", b"100755"} or fields[1] != b"blob"
            or not SHA.fullmatch(fields[2].decode("ascii"))):
        raise ValueError(f"base input is not a regular blob: {path}")
    return git(root, "cat-file", "blob", fields[2].decode("ascii"))


def check(root: Path, baseline: str, mode: str, bootstrap: bool,
          output: Path) -> tuple[dict, int]:
    """Return a receipt and exit status; bootstrap never grants anchoring."""
    if mode not in {"review", "experiment"}:
        raise ValueError("formal mode must be review or experiment")
    head = git(root, "rev-parse", "HEAD").decode().strip()
    receipt = {
        "anchored": False, "promotion_eligible": False,
        "boundary_checked": False, "baseline_ref": baseline,
        "candidate_ref": head, "mode": mode,
        "status": "bootstrap_unanchored", "pilot_baseline_ref": "",
        "scope": "component pilot only; no automatic promotion",
        "changed_inputs": {"added": [], "removed": [], "modified": []},
    }

    def unanchored(reason: str) -> tuple[dict, int]:
        receipt["reason"] = reason
        # An experiment cannot substitute informational bootstrap for its judge.
        if mode == "experiment":
            receipt["status"] = "experiment_requires_anchor"
            return receipt, 1
        return receipt, 0

    if not baseline or baseline == "0" * 40:
        return unanchored("No independent base was supplied; bootstrap cannot authorize promotion.")
    if not SHA.fullmatch(baseline):
        receipt.update(status="invalid_baseline", reason="Base must be a complete lowercase 40-character Git commit SHA.")
        return receipt, 1
    try:
        git(root, "cat-file", "-e", f"{baseline}^{{commit}}")
    except subprocess.CalledProcessError:
        receipt.update(status="unavailable_baseline", reason="The requested base commit is unavailable; fetch it before checking.")
        return receipt, 1
    if baseline == head:
        return unanchored("The candidate is its own base; bootstrap cannot authorize promotion.")

    trusted_anchor = blob(root, baseline, ANCHOR)
    trusted_checker = blob(root, baseline, CHECKER)
    trusted_lock = blob(root, baseline, LOCK)
    legacy = trusted_anchor is None
    if legacy and not bootstrap:
        receipt.update(status="missing_base_helper", reason="Base has no anchoring helper; the workflow must explicitly select bootstrap.")
        return receipt, 1
    if not legacy:
        # Detect an accidentally executed candidate helper even outside CI.
        if bootstrap or Path(__file__).read_bytes() != trusted_anchor:
            receipt.update(status="untrusted_anchor", reason="The executing helper must be the exact base-commit helper.")
            return receipt, 1
        receipt["trusted_anchor_sha256"] = hashlib.sha256(trusted_anchor).hexdigest()
    if trusted_checker is None or trusted_lock is None:
        return unanchored("Base lacks the checker or frozen boundary; bootstrap cannot authorize promotion.")

    receipt["trusted_checker_sha256"] = hashlib.sha256(trusted_checker).hexdigest()
    # Older bases have no review mode. Preserve their strict rejection during
    # the owner-reviewed gate migration instead of silently relaxing that gate.
    strict = mode == "experiment" or legacy
    with tempfile.TemporaryDirectory(prefix="aerostore-base-checker-") as scratch:
        path = Path(scratch) / "check_formal_coverage.py"
        path.write_bytes(trusted_checker)
        command = [sys.executable, "-I", str(path), "--root", str(root),
                   "--baseline-ref" if strict else "--review-base", baseline]
        result = subprocess.run(command, cwd=root, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
    (output / "trusted-boundary.log").write_text(result.stdout)
    print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    receipt["checker_exit_code"] = result.returncode
    if result.returncode:
        receipt["status"] = "legacy_base_gate_failed" if legacy else "baseline_gate_failed"
        receipt["reason"] = "The independent base checker rejected this candidate."
        return receipt, result.returncode if result.returncode > 0 else 1
    try:
        report = json.loads(result.stdout)
        if not isinstance(report, dict) or report.get("passed") is not True:
            raise ValueError("checker did not report passed=true")
        changes = report.get("changed_inputs", receipt["changed_inputs"])
        if not isinstance(changes, dict) or set(changes) != {"added", "removed", "modified"}:
            raise ValueError("invalid changed_inputs shape")
        if any(not isinstance(paths, list) or any(not isinstance(path, str) for path in paths)
               for paths in changes.values()):
            raise ValueError("invalid changed_inputs paths")
    except (ValueError, TypeError) as error:
        receipt.update(status="invalid_checker_report", reason=str(error))
        return receipt, 1
    receipt.update(boundary_checked=True, changed_inputs=changes)
    if legacy:
        return unanchored("The legacy base checker passed, but no independent anchoring helper exists; owner review is required.")
    if mode == "review" and any(changes.values()):
        receipt.update(status="reviewed_boundary_consistent",
                       reason="Candidate lock is consistent. Changed proof inputs require owner review and do not authorize an experiment.")
    else:
        receipt.update(anchored=True, status="anchored")
        if mode == "experiment":
            receipt["pilot_baseline_ref"] = baseline
    return receipt, 0


def publish(receipt: dict, output: Path) -> None:
    (output / "anchoring.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            for name, value in {
                "anchored": str(receipt["anchored"]).lower(),
                "baseline_ref": receipt["pilot_baseline_ref"],
                "status": receipt["status"], "mode": receipt["mode"],
            }.items():
                stream.write(f"{name}={value}\n")
    summary = [f"Formal baseline: **{receipt['status']}**.",
               "Promotion eligible: **false**. No automatic promotion or whole-engine claim."]
    if receipt.get("reason"):
        summary.append(receipt["reason"])
    for kind, paths in receipt["changed_inputs"].items():
        if paths:
            summary.append(f"\nProof inputs {kind}:")
            summary.extend(f"- <code>{html.escape(path)}</code>" for path in paths)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write("\n".join(summary) + "\n")
    if not receipt["anchored"]:
        print(f"Formal baseline: {receipt['status']}; no promotion authorized.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(os.environ.get("GITHUB_WORKSPACE", ".")))
    parser.add_argument("--baseline-ref", default=os.environ.get("FORMAL_BASE_SHA", ""))
    parser.add_argument("--mode", choices=("review", "experiment"), default=os.environ.get("FORMAL_MODE", "review"))
    parser.add_argument("--bootstrap", action="store_true", help="explicit transition for a base without this helper")
    parser.add_argument("--output-dir", type=Path, default=Path("target/verification/ci"))
    args = parser.parse_args()
    root = args.root.resolve()
    output = (root / args.output_dir).resolve()
    if not output.is_relative_to(root / "target"):
        parser.error("outputs must remain below the candidate target/ directory until Phase 3")
    output.mkdir(parents=True, exist_ok=True)
    try:
        receipt, code = check(root, args.baseline_ref, args.mode, args.bootstrap, output)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        receipt = {"anchored": False, "promotion_eligible": False, "boundary_checked": False,
                   "status": "anchor_error", "reason": str(error), "mode": args.mode,
                   "baseline_ref": args.baseline_ref, "pilot_baseline_ref": "", "changed_inputs": {}}
        code = 1
    publish(receipt, output)
    return code


if __name__ == "__main__":
    sys.exit(main())
