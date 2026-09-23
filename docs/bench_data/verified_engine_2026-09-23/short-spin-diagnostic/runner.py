#!/usr/bin/env python3
"""One predeclared contended-lock diagnostic; never a production performance gate."""

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
    "candidate": ROOT / "target/verification-next/candidate-short-spin/manifest.json",
}


def main():
    plan = json.loads(PLAN.read_text())
    PERF.require(plan["pairs"] == 3 and plan["duration_seconds"] == 30,
                 "diagnostic plan changed")
    PERF.require(plan["maximum_median_p99_ratio"] == 1.10 and
                 plan["maximum_individual_p99_ratio"] == 1.25 and
                 plan["maximum_within_variant_relative_range"] == 0.10,
                 "diagnostic tail/noise decision changed")
    PERF.require(plan["minimum_median_throughput_ratio"] == 1.03 and
                 plan["minimum_individual_throughput_ratio"] == 1.0,
                 "diagnostic decision changed")
    PERF.require(PERF.POLICY["p99_median_ratio_max"] == plan["maximum_median_p99_ratio"] and
                 PERF.POLICY["p99_pair_ratio_max"] == plan["maximum_individual_p99_ratio"] and
                 PERF.POLICY["relative_range_noise_max"] == plan["maximum_within_variant_relative_range"],
                 "comparison library tail/noise policy differs from predeclaration")
    receipt = OUTPUT / "result.json"
    PERF.require(not receipt.exists(), "retain this diagnostic; do not overwrite or retry it")
    manifests = {key: PERF.validate_manifest(path) for key, path in MANIFESTS.items()}
    hashes = {key: PERF.digest(path) for key, path in MANIFESTS.items()}
    for manifest in manifests.values():
        PERF.validate_captured_source(manifest)
    baseline, candidate = manifests["baseline"], manifests["candidate"]
    PERF.require(hashes["baseline"] == plan["parent_candidate_manifest_sha256"],
                 "diagnostic baseline differs from plan")
    for name, expected in plan["source_evidence_sha256"].items():
        PERF.require(PERF.digest(OUTPUT / name) == expected, "source evidence changed: " + name)
    validation = json.loads((OUTPUT / "validation/receipt.json").read_text())
    for key in ("passed", "source_stable", "complete_campaign"):
        PERF.require(validation[key] is True, "current lock validation missing: " + key)
    PERF.require(validation["candidate_shm_lock_sha256"] ==
                 candidate["source_sha256"]["aerostore_core/src/shm_lock.rs"],
                 "Loom receipt belongs to different lock source")
    required = {
        "insertion_deletion_reader_and_gc_preserve_reachability_and_ownership",
        "row_guard_preserves_commit_and_index_order",
        "failed_prepublication_allocations_are_returned_before_unlock",
        "model_detects_the_original_unprotected_predecessor_race",
        "model_detects_missing_row_guard_even_with_serialized_index_moves",
        "shm_lock::loom_tests::contended_handoff_publishes_protected_non_atomic_value",
        "shm_lock::loom_tests::registered_priority_waiter_precedes_ordinary_contender",
    }
    PERF.require(set(validation["required_cases"]) == required, "validation roots changed")
    checks = {check["name"]: check for check in validation["checks"]}
    PERF.require(len(checks) == len(validation["checks"]) and
                 set(checks) == required | {"relaxed_success_cas"}, "validation checks omitted or duplicated")
    for name, check in checks.items():
        PERF.require(check["passed"] is True and check["exit_code"] == (101 if name == "relaxed_success_cas" else 0),
                     "current candidate or negative control did not pass its exact requirement")
        PERF.require(PERF.digest(OUTPUT / "validation" / Path(check["log"]).name) == check["log_sha256"],
                     "validation log changed")
    PERF.require(len(validation["builds"]) == 2, "missing fresh candidate/mutant builds")
    model_binary_hashes = []
    for build in validation["builds"]:
        PERF.require(build["exit_code"] == 0 and build["fresh_test_binary"] is True,
                     "lock models reused an old build")
        PERF.require(PERF.digest(build["executable"]) == build["executable_sha256"],
                     "validated Loom binary changed")
        model_binary_hashes.append(build["executable_sha256"])
    PERF.require(len(set(model_binary_hashes)) == 2, "candidate and mutant reused same binary")
    PERF.require("Causality violation: Concurrent write accesses to `UnsafeCell`" in
                 (OUTPUT / "validation/relaxed-success-cas.log").read_text(),
                 "mutant failed for a different reason")
    expected_files = {"aerostore_core/src/shm_lock.rs"}
    actual_files = {name for name in baseline["source_sha256"].keys() | candidate["source_sha256"].keys()
                    if baseline["source_sha256"].get(name) != candidate["source_sha256"].get(name)}
    PERF.require(actual_files == expected_files, "unrelated source change in diagnostic")
    with tempfile.TemporaryDirectory(prefix="aerostore-short-spin-transition-") as directory:
        scratch = Path(directory)
        reconstructed = dict(candidate["source_sha256"])
        for name in sorted(expected_files):
            PERF.require(name == "aerostore_core/src/shm_lock.rs", "unexpected patch path")
            destination = scratch / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(Path(candidate["source"]) / name, destination)
        subprocess.run(["git", "apply", "--reverse", "--whitespace=nowarn",
                        str(OUTPUT / "change.patch")], cwd=scratch, check=True, capture_output=True)
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
        "scope": "Scratch-only contended-lock diagnostic against the rejected candidate, not original-baseline acceptance",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "plan_sha256": PERF.digest(PLAN), "runner_sha256": PERF.digest(__file__),
        "comparison_library_sha256": PERF.digest(PERF.__file__),
        "manifest_sha256": hashes, "runs": [],
        "exact_source_patch_transition_verified": True,
    }
    PERF.write_json(receipt, report)
    workload = {"kind": "churn", "duration": 30, "paired_timing": True}
    pairs = []
    try:
        for number in range(1, 4):
            pair = {}
            order = ("baseline", "candidate") if number % 2 else ("candidate", "baseline")
            for variant in order:
                print(f"Short-spin diagnostic pair={number}/3 variant={variant}", flush=True)
                run = PERF.run_one(manifests[variant], variant, "churn_128m_30s", workload,
                                   number, OUTPUT)
                report["runs"].append(run)
                pair[variant] = run["metrics"]
                PERF.write_json(receipt, report)
            pairs.append(pair)
        comparison = PERF.compare_pairs(pairs, workload)
        throughput = comparison["metrics"]["throughput"]
        ratios = throughput["paired_ratios"]
        # Apply existing conservative p99 bounds, noise, retry and arena margins.
        # Add the stricter predeclared throughput-improvement criterion below.
        # This remains a short diagnostic, not the complete acceptance campaign.
        policy_pass = comparison["status"] == "pass"
        support = (statistics.median(ratios) >= 1.03 and min(ratios) >= 1.0 and policy_pass)
        for key, path in MANIFESTS.items():
            current = PERF.validate_manifest(path)
            PERF.validate_captured_source(current)
            PERF.require(PERF.digest(path) == hashes[key], "capture changed during diagnostic")
        PERF.require(report["plan_sha256"] == PERF.digest(PLAN) and
                     report["runner_sha256"] == PERF.digest(__file__) and
                     report["comparison_library_sha256"] == PERF.digest(PERF.__file__),
                     "diagnostic implementation or plan changed")
        report.update(completed=True, hypothesis_supported=support,
                      comparison=comparison, diagnostic_policy_pass=policy_pass,
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
