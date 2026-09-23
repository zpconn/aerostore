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
    lean = json.loads((directory / "lean.json").read_text()) if "lean" in passed else {}
    verus = json.loads((directory / "verus/receipt.json").read_text()) if "verus" in passed else {}
    concurrent = json.loads((directory / "concurrent/receipt.json").read_text()) if "concurrent" in passed else {}
    tla = json.loads((directory / "tla/report.json").read_text()) if "tla" in passed else {}
    if "lock-models" in passed:
        check_lock_models.validate_receipt(directory / "lock-models/receipt.json", ROOT)
    lean_mutations = {mutation["name"] for mutation in lean.get("mutation_checks", []) if mutation.get("rejected")}
    required_mutations = {"stamp_accepts_equal", "bitmap_drops_membership", "bitmap_accepts_equal_bound",
                          "sort_writes_wrong_bucket", "sort_accepts_equal_bound"}
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
                            "skip_guarded_health_check", "omit_health_failure_abort"}
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
    results = []
    for claim in claims:
        required = set(claim["required_checks"])
        has_evidence = bool(required) and required <= passed
        missing_roots = ((set(claim.get("lean_roots", [])) - lean_roots)
                         | (set(claim.get("verus_roots", [])) - verus_roots)
                         | (set(claim.get("concurrent_roots", [])) - concurrent_roots))
        has_evidence = has_evidence and not missing_roots
        results.append({"id": claim["id"], "scope": claim["scope"],
                        "declared_status": claim["status"],
                        "declared_scope_evidence_passed": has_evidence,
                        "required_checks": sorted(required),
                        "missing_roots": sorted(missing_roots),
                        "lean_roots": claim.get("lean_roots", []),
                        "verus_roots": claim.get("verus_roots", []),
                        "concurrent_roots": claim.get("concurrent_roots", [])})
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
              "full_P1_complete": False,
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
                         ("adapter-tests", [sys.executable, "verification/verus/test_generate.py"]),
                         ("verus", [sys.executable, "verification/verus/run.py", "--output", str(directory / "verus")]),
                         ("concurrent-adapter-tests", [sys.executable, "verification/concurrent/test_generate.py"]),
                         ("concurrent", [sys.executable, "verification/concurrent/run.py", "--output", str(directory / "concurrent")]),
                         ("lean", [sys.executable, "scripts/check_lean.py", "--output", str(directory / "lean.json")]),
                         ("kernel-tests", ["cargo", "test", "--offline", "-p", "aerostore_verified"])]
        if args.profile != "proofs":
            commands.append(("tla-runner-tests", [sys.executable, "scripts/test_tla_runner.py"]))
            commands.append(("tla", [sys.executable, "scripts/check_tla.py", "--output", str(directory / "tla")]))
        if args.profile in {"pilot", "full"}:
            commands += [("lock-models", [sys.executable, "scripts/check_lock_models.py",
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
