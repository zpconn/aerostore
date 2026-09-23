#!/usr/bin/env python3
"""Collect exactly one instrumented CPU profile per preserved executable."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location("performance", ROOT / "scripts/compare_engine_performance.py")
PERF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF)
OUTPUT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "target/verification-next/profile"


def main():
    ARTIFACTS.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": 1, "completed": False, "performance_acceptance": False,
        "scope": "One instrumented diagnostic per executable; timing is not comparable with acceptance campaigns",
        "sampling_period_ms": 10, "follow_forks": True, "duration_seconds": 30,
        "profiler": subprocess.check_output(["gprofng", "--version"], text=True),
        "profiler_sha256": PERF.digest("/usr/bin/gprofng"),
        "runner_sha256": PERF.digest(__file__), "comparison_library_sha256": PERF.digest(PERF.__file__),
        "runs": [],
    }
    PERF.write_json(OUTPUT / "result.json", report)
    for variant in ("baseline", "candidate"):
        manifest_path = ROOT / f"target/verification-next/{variant}-fine/manifest.json"
        manifest = PERF.validate_manifest(manifest_path)
        PERF.validate_captured_source(manifest)
        binary = manifest["binaries"]["hyperfeed_crucible"]["resolved_path"]
        directory = ARTIFACTS / variant
        directory.mkdir()
        tmp = directory / "tmp"
        tmp.mkdir()
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("AEROSTORE_") or key in ("LD_PRELOAD", "LD_LIBRARY_PATH"):
                env.pop(key)
        env.update(AEROSTORE_CRUCIBLE_AEROSTORE_ONLY="1",
                   AEROSTORE_CRUCIBLE_PROFILE_FILTER="profile_2g",
                   AEROSTORE_CRUCIBLE_SHM_MIB="128", AEROSTORE_CRUCIBLE_DURATION_SECS="30",
                   AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS="5000",
                   AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=str(directory / "allocation.csv"),
                   TMPDIR=str(tmp))
        experiment = directory / "cpu.er"
        command = ["/usr/bin/gprofng", "collect", "app", "-p", "10", "-F", "on",
                   "-a", "usedldobjects", "-o", str(experiment), binary, "--noplot"]
        record = {"variant": variant, "command": command, "binary_sha256": PERF.digest(binary),
                  "manifest_sha256": PERF.digest(manifest_path), "environment_before": PERF.snapshot_environment(),
                  "workload_environment": {k: v for k, v in env.items() if k.startswith("AEROSTORE_") or k == "TMPDIR"},
                  "completed": False, "performance_acceptance": False}
        report["runs"].append(record)
        PERF.write_json(OUTPUT / "result.json", report)
        print(f"Instrumented CPU profile: {variant}", flush=True)
        start = time.monotonic()
        with (directory / "output.log").open("w") as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=240)
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
        record.update(exit_code=code, elapsed_wall_seconds=time.monotonic() - start,
                      environment_after=PERF.snapshot_environment(), completed=True)
        text = (directory / "output.log").read_text()
        try:
            PERF.require(code == 0, "instrumented workload failed")
            record["instrumented_metrics_not_acceptance"] = PERF.parse_churn(text, 30)
            record["correctness_passed"] = True
        except Exception as error:
            record["correctness_passed"] = False
            record["error"] = str(error)
        PERF.validate_captured_source(manifest)
        PERF.require(record["binary_sha256"] == PERF.digest(binary) and
                     record["manifest_sha256"] == PERF.digest(manifest_path), "profile input changed")
        record["source_stable"] = True
        for name in ("output.log", "allocation.csv"):
            if (directory / name).exists():
                shutil.copy2(directory / name, OUTPUT / f"{variant}-{name}")
        if record["correctness_passed"]:
            shutil.rmtree(tmp)
        PERF.write_json(OUTPUT / "result.json", report)
    report["completed"] = True
    PERF.write_json(OUTPUT / "result.json", report)
    print(json.dumps({"completed": True, "performance_acceptance": False}))


if __name__ == "__main__":
    main()
