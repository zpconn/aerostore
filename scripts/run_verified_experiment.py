#!/usr/bin/env python3
"""Check current proof inputs, then compare production bucket kernel variants.

The output is a measured component experiment, not promotion of a new engine
default or a whole-database verification claim. Incomplete verification blocks
the benchmark gate. Diagnostic microbenchmarks remain independently runnable.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from verify_formal import ROOT, atomic_json, check_build_environment, run_check, source_fingerprint


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/experiment")
    parser.add_argument("--baseline-ref")
    parser.add_argument("--bootstrap", action="store_true", help="explicit unanchored exploratory run; cannot approve an optimization")
    args = parser.parse_args()
    if bool(args.baseline_ref) == args.bootstrap:
        parser.error("supply either an independently reviewed --baseline-ref or --bootstrap")
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("experiment outputs must be under target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt_path = output / "receipt.json"
    receipt = {"format_version": 1, "completed": False, "passed": False,
               "whole_engine_verified": False, "promoted": False,
               "anchoring": "local_bootstrap_only" if args.bootstrap else "independent_git_baseline",
               "baseline_ref": args.baseline_ref, "full_P1_complete": False,
               "scope": "Bucket kernel experiment with explicit proof coverage; not full P1 or engine certification",
               "started_at": datetime.now(timezone.utc).isoformat(),
               "platform": platform.platform(), "machine": platform.machine(), "checks": []}
    atomic_json(receipt_path, receipt)
    try:
        receipt["build_environment"] = check_build_environment()
        before = source_fingerprint(ROOT)
        receipt["source_sha256"] = before
        receipt["compiler"] = subprocess.check_output(["rustc", "-Vv"], cwd=ROOT, text=True).strip()
        receipt["logical_cpus"] = os.cpu_count()
        receipt["cpu_affinity"] = sorted(os.sched_getaffinity(0))
        receipt["cpu_models"] = sorted({line.split(":", 1)[1].strip() for line in
                                        Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")})
        profile = ROOT / "verification/experiments/profiles/bucket_sets.toml"
        receipt["profile_sha256"] = hashlib.sha256(profile.read_bytes()).hexdigest()
        command = [sys.executable, "scripts/verify_formal.py", "--profile", "pilot",
                   "--output", str(output / "verification/report.json")]
        if args.baseline_ref:
            command += ["--baseline-ref", args.baseline_ref]
        verification = run_check("verification", command, output, 7200)
        receipt["checks"].append(verification)
        atomic_json(receipt_path, receipt)
        if not verification["passed"]:
            raise RuntimeError("verification incomplete or failed; no verified benchmark result is produced")
        benchmark = run_check("benchmark", ["cargo", "bench", "--offline", "-p", "aerostore_core",
                              "--bench", "verified_bucket_sets", "--", "--output", str(output / "buckets.json")],
                              output, 600)
        receipt["checks"].append(benchmark)
        if not benchmark["passed"]:
            raise RuntimeError("microbenchmark failed")
        allocation_check = run_check("allocations", ["cargo", "bench", "--offline", "-p", "aerostore_core",
                                   "--bench", "verified_bucket_allocations", "--", "--output", str(output / "allocations.json")],
                                   output, 600)
        receipt["checks"].append(allocation_check)
        if not allocation_check["passed"]:
            raise RuntimeError("allocation measurement failed")
        allocation = json.loads((output / "allocations.json").read_text())
        if not (allocation["passed_output_comparisons"] and allocation["passed_no_leaks"]):
            raise RuntimeError("allocation measurement failed output or leak checks")
        receipt["allocation_results"] = allocation["results"]
        receipt["allocation_scope"] = allocation.get("scope")
        measured = json.loads((output / "buckets.json").read_text())
        baselines = {r["case"]: r for r in measured["results"] if r["algorithm"] == "standard"}
        receipt["comparisons"] = [{"case": row["case"], "candidate": row["algorithm"],
                                    "baseline_ns_per_call": baselines[row["case"]]["median_ns_per_call"],
                                    "candidate_ns_per_call": row["median_ns_per_call"],
                                    "observed_speedup": baselines[row["case"]]["median_ns_per_call"] / row["median_ns_per_call"]}
                                   for row in measured["results"] if row["algorithm"] != "standard"]
        receipt["source_stable"] = before == source_fingerprint(ROOT)
        receipt["completed"] = True
        receipt["passed"] = receipt["source_stable"] and measured["passed_output_comparisons"]
        receipt["promotion_note"] = "No automatic promotion. Compare repeated whole-transaction results and resource/tail-latency gates before changing the default."
    except Exception as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
        receipt["passed"] = False
    finally:
        atomic_json(receipt_path, receipt)
    print(f"Experiment: {'PASS' if receipt['passed'] else 'INCOMPLETE/FAIL'}; receipt {receipt_path}; no default promoted")
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
