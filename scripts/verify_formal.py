#!/usr/bin/env python3
"""Run the current-source verification campaign and write an honest receipt.

`pilot` is a component/protocol-model gate, never whole-engine verification.
Every required command must exist and pass. Reports are initialized incomplete
before checks begin, so interruption cannot leave an old success at the path.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import os
import signal
import subprocess
import sys
import time
import tomllib
import check_lock_models
import check_refinement_evidence
import check_p1_native_evidence
import check_planning_native_evidence
import check_p0_contracts

ROOT = Path(__file__).resolve().parents[1]


def check_build_environment(root: Path = ROOT) -> dict:
    """Refuse ambient compiler/config overrides instead of proving another build."""
    forbidden = {"RUSTFLAGS", "RUSTDOCFLAGS", "CARGO_ENCODED_RUSTFLAGS",
                 "CARGO_ENCODED_RUSTDOCFLAGS", "RUSTC", "RUSTC_WRAPPER",
                 "RUSTC_WORKSPACE_WRAPPER", "RUSTUP_TOOLCHAIN", "RUSTUP_HOME",
                 "CARGO_HOME", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                 "LEAN_PATH", "LEAN_SRC_PATH", "LEAN_OPTS", "LAKE_HOME",
                 "TLC_LIBRARY", "TLA_LIBRARY", "TLA_LIBRARY_PATH", "CLASSPATH",
                 "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"}
    permitted_local = {"RUSTUP_HOME": str(root / "target/verification-tools/production-rustup"),
                       "CARGO_HOME": str(root / "target/verification-tools/production-cargo"),
                       "RUSTUP_TOOLCHAIN": "1.93.1"}
    overrides = sorted(key for key, value in os.environ.items() if value and
                       (key in forbidden or key.startswith(("CARGO_TARGET_", "CARGO_PROFILE_", "CARGO_BUILD_")))
                       and permitted_local.get(key) != value)
    if overrides:
        raise RuntimeError("unreviewed build environment overrides: " + ", ".join(overrides))
    external_config = []
    for parent in [*root.parents, Path.home()]:
        for name in ["config", "config.toml"]:
            path = parent / ".cargo" / name
            if path.exists():
                external_config.append(str(path))
    if os.environ.get("CARGO_HOME"):
        for name in ["config", "config.toml"]:
            path = Path(os.environ["CARGO_HOME"]) / name
            if path.exists():
                external_config.append(str(path))
    if external_config:
        raise RuntimeError("unreviewed Cargo configuration outside workspace: " + ", ".join(sorted(set(external_config))))
    version = subprocess.check_output(["rustc", "-Vv"], cwd=root, text=True)
    if "release: 1.93.1\n" not in version or "host: x86_64-unknown-linux-gnu\n" not in version:
        raise RuntimeError("pilot requires production Rust 1.93.1 on x86_64-unknown-linux-gnu")
    return {"rustc": version, "ambient_overrides": [], "external_cargo_config": [],
            "pinned_local_environment": {key: value for key, value in permitted_local.items() if key in os.environ}}


def collect_claim_evidence(claims: list[dict], checks: list[dict], directory: Path) -> list[dict]:
    passed = {check["name"] for check in checks if check["passed"]}
    if "p0-contracts" in passed:
        p0 = json.loads((directory / "p0-contracts.log").read_text())
        if not p0.get("passed") or p0 != check_p0_contracts.validate(ROOT):
            raise RuntimeError("P0 contract audit lacks current-source evidence")
    lean = json.loads((directory / "lean.json").read_text()) if "lean" in passed else {}
    verus = json.loads((directory / "verus/receipt.json").read_text()) if "verus" in passed else {}
    concurrent = json.loads((directory / "concurrent/receipt.json").read_text()) if "concurrent" in passed else {}
    tla = json.loads((directory / "tla/report.json").read_text()) if "tla" in passed else {}
    refinements = {name: check_refinement_evidence.validate_receipt(directory / name / "receipt.json", name, ROOT)
                   for name in ["predicate", "predicate-capture", "predicate-composition", "skiplist-detach", "postings",
                                "guards", "lifecycle", "publication-slice", "lifecycle-scenario",
                                "lifecycle-interference", "guard-ownership", "lookup", "indexed-slice",
                                "row-publication", "row-retention", "storage-slice",
                                "commit-data", "commit-completion", "write-plan", "write-admission", "planned-commit"] if name in passed}
    if "p1-native" in passed:
        check_p1_native_evidence.validate_receipt(directory / "p1-native/receipt.json", ROOT)
    if "planning-native" in passed:
        check_planning_native_evidence.validate_receipt(directory / "planning-native/receipt.json", ROOT)
    if "lock-models" in passed:
        check_lock_models.validate_receipt(directory / "lock-models/receipt.json", ROOT)
    lean_mutations = {mutation["name"] for mutation in lean.get("mutation_checks", []) if mutation.get("rejected")}
    required_mutations = {"stamp_accepts_equal", "bitmap_drops_membership", "bitmap_accepts_equal_bound",
                          "sort_writes_wrong_bucket", "sort_accepts_equal_bound",
                          "predicate_ignores_changed_stamp", "predicate_accepts_equal_start",
                          "predicate_drops_publication", "predicate_omits_own_candidates",
                          "lifecycle_reservation_does_not_advance", "lifecycle_uses_writer_start_stamp",
                          "lifecycle_publishes_before_end", "lifecycle_allows_wrapping_reservation",
                          "query_omits_old_bucket", "query_omits_destination_posting", "query_ignores_creator_active",
                          "query_omits_own_candidates", "query_allows_stamp_regression",
                          "query_ignores_deleter_active", "query_filters_before_own_overlay"}
    if "lean" in passed and not (lean.get("passed") and lean.get("completed") and
                                lean.get("kernel_recheck_passed") and lean.get("forged_theorem_rejected") and
                                required_mutations <= lean_mutations):
        raise RuntimeError("Lean command did not produce complete required evidence")
    if "verus" in passed and not (verus.get("passed") and verus.get("status") == "passed"):
        raise RuntimeError("Verus command did not produce complete required evidence")
    concurrent_mutations = {"skip_predicate_validation", "omit_partial_publication_poison",
                            "skip_write_ahead_callback", "omit_callback_error_rollback",
                            "omit_callback_unwind_rollback", "stamp_before_deregister", "skip_wal_binding_check",
                            "omit_record_prepare_error_abort", "omit_prepare_error_abort",
                            "omit_prepare_unwind_abort", "accept_before_validation",
                            "skip_guarded_health_check", "omit_health_failure_abort", "release_predicates_before_finish"}
    rejected_concurrent = {check["name"] for check in concurrent.get("checks", [])
                          if check.get("expected_failure") and check.get("exit_code") != 0
                          and (check.get("errors") or 0) > 0}
    if "concurrent" in passed and not (concurrent.get("passed") and concurrent.get("status") == "passed"
            and concurrent.get("native_primitive_refinement_proved") is False
            and concurrent.get("transaction_history_refinement_proved") is False
            and concurrent_mutations <= rejected_concurrent):
        raise RuntimeError("Concurrent command did not produce complete conditional proof evidence")
    if "tla" in passed and not (tla.get("passed") and tla.get("completed") and tla.get("complete_campaign")):
        raise RuntimeError("TLA command did not finish the full declared campaign")
    lean_roots = {root["name"] for root in lean.get("required_roots", [])}
    verus_roots = {root["name"] for root in verus.get("required_roots", [])}
    concurrent_roots = set(concurrent.get("required_roots", []))
    refinement_roots = {name: set(receipt["required_roots"]) for name, receipt in refinements.items()}
    results = []
    for claim in claims:
        required = set(claim["required_checks"])
        has_evidence = bool(required) and required <= passed
        missing_roots = ((set(claim.get("lean_roots", [])) - lean_roots)
                         | (set(claim.get("verus_roots", [])) - verus_roots)
                         | (set(claim.get("concurrent_roots", [])) - concurrent_roots))
        for name, roots in claim.get("refinement_roots", {}).items():
            missing_roots |= set(roots) - refinement_roots.get(name, set())
        has_evidence = has_evidence and not missing_roots
        results.append({"id": claim["id"], "scope": claim["scope"],
                        "declared_status": claim["status"],
                        "declared_scope_evidence_passed": has_evidence,
                        "required_checks": sorted(required),
                        "missing_roots": sorted(missing_roots),
                        "lean_roots": claim.get("lean_roots", []),
                        "verus_roots": claim.get("verus_roots", []),
                        "concurrent_roots": claim.get("concurrent_roots", []),
                        "refinement_roots": claim.get("refinement_roots", {})})
        if required <= passed and required and missing_roots:
            raise RuntimeError(f"{claim['id']}: missing declared proof roots {sorted(missing_roots)}")
    return results


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def source_fingerprint(root: Path) -> dict[str, str]:
    files: set[Path] = {root / "Cargo.toml", root / "Cargo.lock"}
    for directory in ["aerostore_verified", "aerostore_core", "aerostore_macros", "aerostore_tcl",
                      "verification", "scripts", ".cargo", ".github"]:
        for path in (root / directory).rglob("*"):
            if not path.is_file() or any(part in {".lake", "__pycache__", "target", ".git"} for part in path.parts):
                continue
            if path.suffix in {".rs", ".py", ".sh", ".lean", ".tla", ".cfg", ".toml", ".json", ".md", ".yml", ".yaml"} or path.name == "lean-toolchain":
                files.add(path)
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(files) if path.exists()}


def run_check(name: str, command: list[str], directory: Path, timeout: int) -> dict:
    log_path = directory / f"{name}.log"
    started = time.monotonic()
    print(f"[{name}] {' '.join(command)}", flush=True)
    error = None
    with log_path.open("w") as log:
        try:
            process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
        except (OSError, subprocess.TimeoutExpired) as exception:
            error = str(exception)
            log.write("\nRUNNER ERROR: " + error + "\n")
            code = -1
    result = {"name": name, "command": command, "returncode": code,
              "passed": code == 0, "elapsed_seconds": time.monotonic() - started,
              "log": str(log_path.relative_to(ROOT)), "error": error}
    print(f"[{name}] {'PASS' if code == 0 else 'FAIL'} ({result['elapsed_seconds']:.1f}s); {result['log']}", flush=True)
    if code:
        print("\n".join(log_path.read_text().splitlines()[-35:]), flush=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["models", "proofs", "pilot", "full"], default="pilot")
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/report.json")
    parser.add_argument("--baseline-ref", help="compare frozen contract/boundary against an independent Git revision")
    parser.add_argument("--timeout", type=int, default=1800, help="per-check timeout in seconds")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("reports must be under target/ to avoid modifying verified input artifacts")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    report = {"format_version": 1, "profile": args.profile, "completed": False,
              "passed": False, "whole_engine_verified": False,
              "p0_complete": False, "full_P1_complete": False,
              "anchoring": "independent_git_baseline" if args.baseline_ref else "local_bootstrap_only",
              "baseline_ref": args.baseline_ref,
              "promotion_eligible": False,
              "scope": "Component pilot and abstract finite models; open engine obligations remain",
              "started_at": datetime.now(timezone.utc).isoformat(), "checks": []}
    atomic_json(output, report)
    directory = output.parent
    try:
        report["build_environment"] = check_build_environment()
        before = source_fingerprint(ROOT)
        report["source_before"] = before
        report["git_revision"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        claims = tomllib.loads((ROOT / "verification/claims.toml").read_text())
        report["open_claims"] = [c["id"] for c in claims["claims"] if c["status"] == "open"]
        commands: list[tuple[str, list[str]]] = []
        if args.profile != "models" or args.baseline_ref:
            coverage = [sys.executable, "scripts/check_formal_coverage.py"]
            if args.baseline_ref:
                baseline_checker = directory / "baseline_check_formal_coverage.py"
                baseline_checker.write_bytes(subprocess.check_output(
                    ["git", "show", f"{args.baseline_ref}:scripts/check_formal_coverage.py"], cwd=ROOT))
                coverage = [sys.executable, str(baseline_checker), "--root", str(ROOT),
                            "--baseline-ref", args.baseline_ref]
            commands.append(("coverage", coverage))
        if args.profile != "models":
            commands += [("gate-tests", [sys.executable, "scripts/test_formal_gate.py"]),
                         ("p0-contract-tests", [sys.executable, "scripts/test_p0_contracts.py"]),
                         ("p0-contracts", [sys.executable, "scripts/check_p0_contracts.py"]),
                         ("adapter-tests", [sys.executable, "verification/verus/test_generate.py"]),
                         ("verus", [sys.executable, "verification/verus/run.py", "--output", str(directory / "verus")]),
                         ("concurrent-adapter-tests", [sys.executable, "verification/concurrent/test_generate.py"]),
                         ("concurrent", [sys.executable, "verification/concurrent/run.py", "--output", str(directory / "concurrent")]),
                         ("predicate-adapter-tests", [sys.executable, "verification/predicate/test_generate.py"]),
                         ("predicate", [sys.executable, "verification/predicate/run.py", "--output", str(directory / "predicate")]),
                         ("predicate-capture-adapter-tests", [sys.executable, "verification/predicate_capture/test_generate.py"]),
                         ("predicate-capture", [sys.executable, "verification/predicate_capture/run.py", "--output", str(directory / "predicate-capture")]),
                         ("predicate-composition-adapter-tests", [sys.executable, "verification/predicate_composition/test_generate.py"]),
                         ("predicate-composition", [sys.executable, "verification/predicate_composition/run.py", "--output", str(directory / "predicate-composition")]),
                         ("skiplist-detach-adapter-tests", [sys.executable, "verification/skiplist_detach/test_generate.py"]),
                         ("skiplist-detach", [sys.executable, "verification/skiplist_detach/run.py", "--output", str(directory / "skiplist-detach")]),
                         ("postings-adapter-tests", [sys.executable, "verification/postings/test_generate.py"]),
                         ("postings", [sys.executable, "verification/postings/run.py", "--output", str(directory / "postings")]),
                         ("guards-adapter-tests", [sys.executable, "verification/guards/test_generate.py"]),
                         ("guards", [sys.executable, "verification/guards/run.py", "--output", str(directory / "guards")]),
                         ("lifecycle-adapter-tests", [sys.executable, "verification/lifecycle/test_generate.py"]),
                         ("lifecycle", [sys.executable, "verification/lifecycle/run.py", "--output", str(directory / "lifecycle")]),
                         ("publication-slice-adapter-tests", [sys.executable, "verification/publication_slice/test_generate.py"]),
                         ("publication-slice", [sys.executable, "verification/publication_slice/run.py", "--output", str(directory / "publication-slice")]),
                         ("lifecycle-scenario-adapter-tests", [sys.executable, "verification/lifecycle_scenario/test_generate.py"]),
                         ("lifecycle-scenario", [sys.executable, "verification/lifecycle_scenario/run.py", "--output", str(directory / "lifecycle-scenario")]),
                         ("lifecycle-interference-adapter-tests", [sys.executable, "verification/lifecycle_interference/test_generate.py"]),
                         ("lifecycle-interference", [sys.executable, "verification/lifecycle_interference/run.py", "--output", str(directory / "lifecycle-interference")]),
                         ("guard-ownership-adapter-tests", [sys.executable, "verification/guard_ownership/test_generate.py"]),
                         ("guard-ownership", [sys.executable, "verification/guard_ownership/run.py", "--output", str(directory / "guard-ownership")]),
                         ("lookup-adapter-tests", [sys.executable, "verification/lookup/test_generate.py"]),
                         ("lookup", [sys.executable, "verification/lookup/run.py", "--output", str(directory / "lookup")]),
                         ("indexed-slice-adapter-tests", [sys.executable, "verification/indexed_slice/test_generate.py"]),
                         ("indexed-slice", [sys.executable, "verification/indexed_slice/run.py", "--output", str(directory / "indexed-slice")]),
                         ("row-publication-adapter-tests", [sys.executable, "verification/row_publication/test_generate.py"]),
                         ("row-publication", [sys.executable, "verification/row_publication/run.py", "--output", str(directory / "row-publication")]),
                         ("row-retention-adapter-tests", [sys.executable, "verification/row_retention/test_generate.py"]),
                         ("row-retention", [sys.executable, "verification/row_retention/run.py", "--output", str(directory / "row-retention")]),
                         ("row-initialization-adapter-tests", [sys.executable, "verification/row_initialization/test_generate.py"]),
                         ("storage-slice-adapter-tests", [sys.executable, "verification/storage_slice/test_generate.py"]),
                         ("storage-slice", [sys.executable, "verification/storage_slice/run.py", "--output", str(directory / "storage-slice")]),
                         ("commit-data-adapter-tests", [sys.executable, "verification/commit_data/test_generate.py"]),
                         ("commit-data", [sys.executable, "verification/commit_data/run.py", "--output", str(directory / "commit-data")]),
                         ("commit-completion-adapter-tests", [sys.executable, "verification/commit_completion/test_generate.py"]),
                         ("commit-completion", [sys.executable, "verification/commit_completion/run.py", "--output", str(directory / "commit-completion")]),
                         ("write-plan-adapter-tests", [sys.executable, "verification/write_plan/test_generate.py"]),
                         ("write-plan", [sys.executable, "verification/write_plan/run.py", "--output", str(directory / "write-plan")]),
                         ("write-admission-adapter-tests", [sys.executable, "verification/write_admission/test_generate.py"]),
                         ("write-admission", [sys.executable, "verification/write_admission/run.py", "--output", str(directory / "write-admission")]),
                         ("planned-commit-adapter-tests", [sys.executable, "verification/planned_commit/test_generate.py"]),
                         ("planned-commit", [sys.executable, "verification/planned_commit/run.py", "--output", str(directory / "planned-commit")]),
                         ("lean", [sys.executable, "scripts/check_lean.py", "--output", str(directory / "lean.json")]),
                         ("kernel-tests", ["cargo", "test", "--offline", "-p", "aerostore_verified"])]
        if args.profile != "proofs":
            commands.append(("tla-runner-tests", [sys.executable, "scripts/test_tla_runner.py"]))
            commands.append(("tla", [sys.executable, "scripts/check_tla.py", "--output", str(directory / "tla")]))
        if args.profile in {"pilot", "full"}:
            commands += [("p1-native-runner-tests", [sys.executable, "-m", "unittest", "discover", "-s", "verification/p1_native", "-p", "test_*.py"]),
                         ("p1-native", [sys.executable, "verification/p1_native/run.py", "--output", str(directory / "p1-native")]),
                         ("planning-native-runner-tests", [sys.executable, "-m", "unittest", "discover", "-s", "verification/planning_native", "-p", "test_*.py"]),
                         ("planning-native", [sys.executable, "verification/planning_native/run.py", "--output", str(directory / "planning-native")]),
                         ("retention-native-runner-tests", [sys.executable, "-m", "unittest", "discover", "-s", "verification/retention_native", "-p", "test_*.py"]),
                         ("production-equivalence-checker-tests", [sys.executable, "verification/lookup_native/test_production_equivalence.py"]),
                         ("retention-native", [sys.executable, "verification/retention_native/run.py",
                                                "--output", str(directory / "retention-native")]),
                         ("lock-models", [sys.executable, "scripts/check_lock_models.py",
                                         "--output", str(directory / "lock-models")]),
                         ("performance-gate-tests", [sys.executable, "scripts/test_compare_engine_performance.py"]),
                         ("core-regressions", ["cargo", "test", "--offline", "--locked", "-p", "aerostore_core",
                             "--release", "--lib", "--test", "wal_protocol_regressions", "--test", "wal_delta_recovery_pk_map",
                             "--test", "wal_writer_lifecycle", "--test", "wal_crash_recovery", "--test", "shm_shared_memory",
                             "--test", "crucible_latency_histogram",
                             "--test", "crucible_seed",
                             "--", "--test-threads=1"])]
            for feature in ["default", "verified-buckets-sort", "verified-buckets-bitmap"]:
                command = ["cargo", "test", "--offline", "-p", "aerostore_core", "--release"]
                if feature != "default":
                    command += ["--features", feature]
                command += ["--test", "occ_transactional_index", "--test", "transactional_query_execution",
                            "--test", "occ_write_skew", "--", "--test-threads=1"]
                commands.append((f"integration-{feature}", command))
                crucible = ["cargo", "bench", "--offline", "-p", "aerostore_core"]
                if feature != "default":
                    crucible += ["--features", feature]
                crucible += ["--bench", "hyperfeed_extended_crucible", "--", "--engine", "aerostore",
                             "--mode", "all", "--families", "8", "--cycles", "1", "--workers", "4",
                             "--shm-mib", "128", "--output", str(directory / f"crucible-{feature}.json")]
                commands.append((f"crucible-{feature}", crucible))
        for name, command in commands:
            report["checks"].append(run_check(name, command, directory, args.timeout))
            atomic_json(output, report)
            if name == "coverage" and not report["checks"][-1]["passed"]:
                report["gate_error"] = "Boundary check failed; candidate proof/test runners were not executed"
                break
        after = source_fingerprint(ROOT)
        report["source_after"] = after
        report["source_stable"] = before == after
        if not report["source_stable"]:
            report["source_changed"] = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
        report["claim_evidence"] = collect_claim_evidence(claims["claims"], report["checks"], directory)
        if any(check["name"] == "p0-contracts" and check["passed"] for check in report["checks"]):
            report["p0_contract_audit"] = json.loads((directory / "p0-contracts.log").read_text())
            report["p0_complete"] = report["p0_contract_audit"]["p0_complete"]
        report["completed"] = True
        report["passed"] = bool(commands) and all(c["passed"] for c in report["checks"]) and report["source_stable"]
        if args.profile == "full":
            report["passed"] = False
            report["full_gate_error"] = "Whole-engine obligations remain open; pilot evidence cannot satisfy full verification"
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        report["passed"] = False
    finally:
        atomic_json(output, report)
    print(f"Report: {output}\nProfile {args.profile}: {'PASS' if report['passed'] else 'INCOMPLETE/FAIL'}; whole engine: UNPROVED")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
