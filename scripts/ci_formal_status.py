#!/usr/bin/env python3
"""Report component evidence honestly; unanchored bootstrap is informational."""
from __future__ import annotations

import argparse
import os
import sys


def verdict(pilot: str, anchored: str, status: str, mode: str) -> tuple[int, str]:
    if mode not in {"review", "experiment"}:
        return 1, "Missing or invalid formal mode."
    if pilot != "success":
        return 1, f"Component pilot result: {pilot or 'missing'}. This failure remains blocking."
    if anchored not in {"true", "false"}:
        return 1, "Missing or invalid anchoring result; no passing evidence can be asserted."
    if anchored == "true":
        if status != "anchored":
            return 1, "Anchoring result conflicts with its status."
        return 0, "Passing component evidence has an independent baseline."
    if mode == "experiment":
        return 1, "A performance experiment requires a passing independent baseline check."
    if status == "reviewed_boundary_consistent":
        return 0, "Candidate lock is consistent; changed proof inputs require owner review."
    if status == "bootstrap_unanchored":
        return 0, "Passing component pilot is informational bootstrap evidence; independent baseline review is required."
    return 1, f"Formal baseline check did not pass: {status or 'missing'}."


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-result", default=os.environ.get("PILOT_RESULT", ""))
    parser.add_argument("--anchored", default=os.environ.get("BASE_ANCHORED", ""))
    parser.add_argument("--anchor-status", default=os.environ.get("ANCHOR_STATUS", ""))
    parser.add_argument("--mode", choices=("review", "experiment"), default=os.environ.get("FORMAL_MODE", "review"))
    args = parser.parse_args()
    code, message = verdict(args.pilot_result, args.anchored, args.anchor_status, args.mode)
    lines = [message,
             "Promotion eligible: **false**. Full protocol, relevant resource, and end-to-end gates remain incomplete.",
             "This check performs no merge, release, deployment, or whole-engine certification."]
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return code


if __name__ == "__main__":
    sys.exit(main())
