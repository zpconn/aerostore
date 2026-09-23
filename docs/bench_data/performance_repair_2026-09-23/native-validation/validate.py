#!/usr/bin/env python3
"""Run the complete native release suites and retain source-bound receipts."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("performance", ROOT / "scripts/compare_engine_performance.py")
performance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(performance)


def main():
    receipt = HERE / "receipt.json"
    if receipt.exists():
        raise RuntimeError("retain the existing receipt; do not overwrite a validation run")
    env = os.environ.copy()
    for key in list(env):
        if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTDOC", "RUSTC_WRAPPER",
                   "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTFLAGS", "CARGO_BUILD_RUSTC_WRAPPER",
                   "CARGO_BUILD_TARGET"} or key.startswith("CARGO_PROFILE_") or (
                key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS")):
            env.pop(key)
    env.update(RUSTUP_HOME=str(ROOT / "target/verification-tools/production-rustup"),
               CARGO_HOME=str(ROOT / "target/verification-tools/production-cargo"),
               RUSTUP_TOOLCHAIN="1.93.1", RUSTUP_NO_UPDATE_CHECK="1", CARGO_ENCODED_RUSTFLAGS="")
    rustc = subprocess.check_output(["rustup", "which", "rustc"], cwd=ROOT, env=env, text=True).strip()
    cargo = subprocess.check_output(["rustup", "which", "cargo"], cwd=ROOT, env=env, text=True).strip()
    env["RUSTC"] = rustc
    compiler = subprocess.check_output([rustc, "--version", "--verbose"], env=env, text=True)
    if "release: 1.93.1\n" not in compiler:
        raise RuntimeError("unexpected compiler")
    before = performance.source_hashes(ROOT)
    report = {"completed": False, "passed": False, "source_before": before,
              "rustc": compiler, "rustc_sha256": performance.digest(rustc),
              "runner_sha256": performance.digest(__file__),
              "comparison_library_sha256": performance.digest(performance.__file__),
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "checks": [],
              "scope": "Native release regression suites, not a whole-engine correctness proof or timing acceptance"}
    performance.write_json(receipt, report)
    suites = [
        ("core-workspace", ["-p", "aerostore_core", "-p", "aerostore_macros", "-p", "aerostore_verified"],
         "target/verification-next/workspace-tests"),
        ("tcl-workspace", ["-p", "aerostore_tcl"], "target/verification-next/tcl-tests"),
    ]
    try:
        for name, packages, target in suites:
            command = [cargo, "test", "--offline", "--locked", *packages, "--release", "--target-dir", target]
            started = time.monotonic()
            path = HERE / (name + ".log")
            print("Starting " + name, flush=True)
            with path.open("w") as output:
                result = subprocess.run(command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
            counts = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", path.read_text())
            totals = [sum(int(row[i]) for row in counts) for i in range(3)]
            passed = result.returncode == 0 and totals[0] > 0 and totals[1] == 0
            report["checks"].append({"name": name, "command": command, "exit_code": result.returncode,
                                     "passed": passed, "tests_passed": totals[0], "tests_failed": totals[1],
                                     "tests_ignored": totals[2], "log": path.name,
                                     "log_sha256": performance.digest(path),
                                     "elapsed_seconds": time.monotonic() - started})
            performance.write_json(receipt, report)
            print(name + ": " + str(report["checks"][-1]), flush=True)
        report["completed"] = True
    finally:
        report["source_after"] = performance.source_hashes(ROOT)
        report["source_stable"] = before == report["source_after"]
        report["passed"] = report["completed"] and report["source_stable"] and all(c["passed"] for c in report["checks"])
        performance.write_json(receipt, report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
