#!/usr/bin/env python3
"""One predeclared layout diagnostic; never a production performance gate."""

import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location(
    "performance", ROOT / "scripts/compare_engine_performance.py")
PERF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF)
OUTPUT = Path(__file__).resolve().parent
PLAN = OUTPUT / "plan.json"
MANIFESTS = {
    "baseline": ROOT / "target/verification-next/candidate-fine/manifest.json",
    "candidate": ROOT / "target/verification-next/candidate-aligned/manifest.json",
}


def main():
    plan = json.loads(PLAN.read_text())
    PERF.require(plan["pairs"] == 3 and plan["duration_seconds"] == 30,
                 "diagnostic plan changed")
    PERF.require(plan["minimum_median_throughput_ratio"] == 1.03 and
                 plan["minimum_individual_throughput_ratio"] == 1.0,
                 "diagnostic decision changed")
    receipt = OUTPUT / "result.json"
    PERF.require(not receipt.exists(), "retain this diagnostic; do not overwrite or retry it")
    manifests = {key: PERF.validate_manifest(path) for key, path in MANIFESTS.items()}
    hashes = {key: PERF.digest(path) for key, path in MANIFESTS.items()}
    for manifest in manifests.values():
        PERF.validate_captured_source(manifest)
    baseline, candidate = manifests["baseline"], manifests["candidate"]
    PERF.require(hashes["baseline"] == plan["parent_candidate_manifest_sha256"],
                 "diagnostic baseline differs from plan")
    for name, expected in plan["layout_evidence_sha256"].items():
        PERF.require(PERF.digest(OUTPUT / name) == expected, "layout evidence changed: " + name)
    patch_manifest = json.loads((OUTPUT / "layout-patch-manifest.json").read_text())
    expected_files = set(patch_manifest["files"])
    actual_files = {name for name in baseline["source_sha256"].keys() | candidate["source_sha256"].keys()
                    if baseline["source_sha256"].get(name) != candidate["source_sha256"].get(name)}
    PERF.require(actual_files == expected_files, "unrelated source change in diagnostic")
    with tempfile.TemporaryDirectory(prefix="aerostore-alignment-transition-") as directory:
        scratch = Path(directory)
        reconstructed = dict(candidate["source_sha256"])
        for name in sorted(expected_files):
            PERF.require(name in {"aerostore_core/src/occ_partitioned.rs", "aerostore_core/src/shm.rs",
                                  "aerostore_core/src/bootloader.rs"}, "unexpected patch path")
            destination = scratch / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(Path(candidate["source"]) / name, destination)
        subprocess.run(["git", "apply", "--reverse", "--whitespace=nowarn",
                        str(OUTPUT / "layout.patch")], cwd=scratch, check=True, capture_output=True)
        for name in expected_files:
            reconstructed[name] = PERF.digest(scratch / name)
        PERF.require(reconstructed == baseline["source_sha256"], "patch does not reconstruct exact baseline")
    for key in ("rustc", "rustc_sha256", "fixture_sha256"):
        PERF.require(baseline[key] == candidate[key], "capture mismatch: " + key)
    PERF.require(baseline["build_configuration"]["manifest_profiles"] ==
                 candidate["build_configuration"]["manifest_profiles"], "profile mismatch")
    for name in (*PERF.BENCHES, "wal_ring_benchmark"):
        for key in ("cargo_profile", "features"):
            PERF.require(baseline["binaries"][name][key] == candidate["binaries"][name][key],
                         "artifact configuration mismatch: " + name + "/" + key)
    report = {
        "schema_version": 1, "completed": False, "hypothesis_supported": False,
        "performance_acceptance": False, "speedup_claim": False,
        "scope": "Scratch-only layout diagnostic against the rejected candidate, not original-baseline acceptance",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "plan_sha256": PERF.digest(PLAN), "runner_sha256": PERF.digest(__file__),
        "comparison_library_sha256": PERF.digest(PERF.__file__),
        "manifest_sha256": hashes, "runs": [],
        "exact_layout_patch_transition_verified": True,
    }
    PERF.write_json(receipt, report)
    workload = {"kind": "churn", "duration": 30, "paired_timing": False}
    pairs = []
    try:
        for number in range(1, 4):
            pair = {}
            order = ("baseline", "candidate") if number % 2 else ("candidate", "baseline")
            for variant in order:
                print(f"alignment diagnostic pair={number}/3 variant={variant}", flush=True)
                run = PERF.run_one(manifests[variant], variant, "churn_128m_30s", workload,
                                   number, OUTPUT)
                report["runs"].append(run)
                pair[variant] = run["metrics"]
                PERF.write_json(receipt, report)
            pairs.append(pair)
        comparison = PERF.compare_pairs(pairs, workload)
        throughput = comparison["metrics"]["throughput"]
        ratios = throughput["paired_ratios"]
        # Resource/retry margins are identical to the existing comparison policy.
        # run_one has already required every benchmark correctness/growth gate.
        resource_pass = comparison["status"] == "pass"
        support = (statistics.median(ratios) >= 1.03 and min(ratios) >= 1.0 and resource_pass)
        for key, path in MANIFESTS.items():
            current = PERF.validate_manifest(path)
            PERF.validate_captured_source(current)
            PERF.require(PERF.digest(path) == hashes[key], "capture changed during diagnostic")
        PERF.require(report["plan_sha256"] == PERF.digest(PLAN) and
                     report["runner_sha256"] == PERF.digest(__file__) and
                     report["comparison_library_sha256"] == PERF.digest(PERF.__file__),
                     "diagnostic implementation or plan changed")
        report.update(completed=True, hypothesis_supported=support,
                      comparison=comparison, correctness_and_resources_pass=resource_pass,
                      source_stable=True,
                      finished_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    except Exception as error:
        report["error"] = str(error)
        print(str(error), flush=True)
    PERF.write_json(receipt, report)
    print(json.dumps({key: report[key] for key in
                      ("completed", "hypothesis_supported", "performance_acceptance")}))
    return 0 if report["completed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
