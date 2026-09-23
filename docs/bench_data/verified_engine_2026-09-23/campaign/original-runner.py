#!/usr/bin/env python3
"""Compare preserved default-engine binaries with fixed, paired workloads.

Capture is separate from timing. No compilation occurs during a campaign.
This is a finite regression screen, not a HyperFeed speedup or durability proof.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import statistics
import subprocess
import time
import tomllib


ROOT = Path(__file__).resolve().parents[1]
BENCHES = ("hyperfeed_crucible", "hyperfeed_extended_crucible")
WAL_TEST = "benchmark_async_synchronous_commit_modes"
POLICY = {
    "minimum_pairs": 3,
    "throughput_median_ratio_min": 0.95,
    "throughput_pair_ratio_min": 0.90,
    "p99_median_ratio_max": 1.10,
    "p99_pair_ratio_max": 1.25,
    "relative_range_noise_max": 0.10,
    "retry_ratio_max": 1.20,
    "retry_rate_absolute_slack": 0.02,
    "arena_ratio_max": 1.05,
    "arena_absolute_slack_bytes": 1 << 20,
}
WORKLOADS = {
    "churn_128m_120s": {"kind": "churn", "duration": 120, "paired_timing": True},
    "churn_128m_240s": {"kind": "churn", "duration": 240, "paired_timing": False},
    "extended_64f_128c_8w": {"kind": "extended", "families": 64, "cycles": 128, "workers": 8,
                               "seed": 8675309, "paired_timing": True},
    "extended_256f_32c_16w": {"kind": "extended", "families": 256, "cycles": 32, "workers": 16,
                                "seed": 20260922, "paired_timing": True},
    "wal_sync_async_10000": {"kind": "wal", "paired_timing": True},
}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def source_hashes(source):
    files = [source / "Cargo.toml", source / "Cargo.lock"]
    for crate in ("aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl"):
        files += [p for p in (source / crate).rglob("*") if p.is_file() and
                  (p.suffix in (".rs", ".toml") or p.name == "build.rs")]
    return {str(p.relative_to(source)): digest(p) for p in sorted(files)}


def fixture_hashes(source):
    files = list((source / "aerostore_core/benches").rglob("*.rs"))
    files += [source / "aerostore_core/tests/wal_ring_benchmark.rs"]
    return {str(p.relative_to(source)): digest(p) for p in sorted(files)}


def compiler():
    cargo = Path(subprocess.check_output(["rustup", "which", "cargo"], text=True).strip())
    rustc = cargo.with_name("rustc")
    identity = subprocess.check_output([rustc, "--version", "--verbose"], text=True)
    if "release: 1.93.1\n" not in identity or "host: x86_64-unknown-linux-gnu\n" not in identity:
        raise ValueError("this comparison requires Rust 1.93.1 for x86_64 Linux")
    return cargo, rustc, identity


def capture(args):
    source, output = args.source.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cargo, rustc, identity = compiler()
    initial = source_hashes(source)
    report = {"schema_version": 1, "complete": False, "source": str(source),
              "source_commit": args.source_commit, "features": "default", "rustc": identity,
              "rustc_sha256": digest(rustc), "cargo": subprocess.check_output([cargo, "--version"], text=True),
              "source_sha256": initial, "fixture_sha256": fixture_hashes(source),
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "commands": [], "binaries": {}}
    write_json(output / "manifest.json", report)
    env = os.environ.copy()
    for key in list(env):
        if key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTDOC",
                   "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTFLAGS",
                   "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_TARGET") or key.startswith("CARGO_PROFILE_") or (
                   key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS")):
            env.pop(key)
    env["RUSTC"] = str(rustc)
    env["RUSTUP_TOOLCHAIN"] = "stable-x86_64-unknown-linux-gnu"
    # Empty encoded flags override Cargo config-file rustflags too. Both captures
    # use the manifest's production profiles without a caller's instrumenting cfg.
    env["CARGO_ENCODED_RUSTFLAGS"] = ""
    report["build_configuration"] = {
        "manifest_profiles": tomllib.loads((source / "Cargo.toml").read_text()).get("profile", {}),
        "compiler_environment": {key: env.get(key) for key in
                                 ("RUSTC", "RUSTUP_TOOLCHAIN", "RUSTUP_HOME", "CARGO_HOME", "CARGO_ENCODED_RUSTFLAGS")},
        "target": "x86_64-unknown-linux-gnu",
    }
    shared = ["--offline", "--locked", "--manifest-path", str(source / "Cargo.toml"),
              "--target-dir", str(output / "build"), "-p", "aerostore_core", "--no-run",
              "--message-format=json-render-diagnostics"]
    commands = [[str(cargo), "bench", *shared, "--bench", BENCHES[0], "--bench", BENCHES[1]],
                [str(cargo), "test", "--release", *shared, "--test", "wal_ring_benchmark"]]
    for index, command in enumerate(commands):
        messages, errors = output / f"build-{index}.jsonl", output / f"build-{index}.log"
        with messages.open("w") as stdout, errors.open("w") as stderr:
            result = subprocess.run(command, cwd=source, env=env, stdout=stdout, stderr=stderr)
        report["commands"].append({"command": command, "exit_code": result.returncode})
        write_json(output / "manifest.json", report)
        if result.returncode:
            raise RuntimeError(errors.read_text()[-6000:])
        for line in messages.read_text().splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            name = item.get("target", {}).get("name")
            if item.get("reason") == "compiler-artifact" and item.get("executable") and name in (*BENCHES, "wal_ring_benchmark"):
                destination = output / name
                shutil.copy2(item["executable"], destination)
                report["binaries"][name] = {"path": str(destination), "sha256": digest(destination),
                                            "size_bytes": destination.stat().st_size,
                                            "cargo_profile": item["profile"], "features": item["features"]}
    if initial != source_hashes(source):
        raise RuntimeError("source changed during executable capture")
    if set(report["binaries"]) != {*BENCHES, "wal_ring_benchmark"}:
        raise RuntimeError("capture omitted a required executable")
    report.update(complete=True, finished_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    write_json(output / "manifest.json", report)
    print(json.dumps({"complete": True, "manifest": str(output / "manifest.json")}))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fields(text, name):
    rows = re.findall(r"^" + re.escape(name) + r": (.*)$", text, re.MULTILINE)
    require(len(rows) == 1, "missing or ambiguous " + name)
    return dict(re.findall(r"(\w+)=([^\s]+)", rows[0]))


def finite_number(value):
    result = float(value)
    require(math.isfinite(result) and result >= 0, "non-finite or negative metric")
    return result


def parse_churn(text, duration):
    rows = [line.split("|")[1:-1] for line in text.splitlines() if line.startswith("| aerostore |")]
    require(len(rows) == 1 and len(rows[0]) == 12, "missing or ambiguous Aerostore result row")
    row = rows[0]
    correctness = fields(text, "hyperfeed_crucible_correctness")
    require(correctness.get("exact_match") == "true", "table/index mismatch")
    drain = fields(text, "hyperfeed_crucible_gc_drain")
    require(all(int(drain[key]) == 0 for key in ("retired_backlog", "gc_recycle_errors", "retired_postings")),
            "nonempty or erroneous GC drain")
    require(fields(text, "hyperfeed_crucible_allocation_audit").get("status") == "pass", "allocation audit missing")
    stability = fields(text, "hyperfeed_crucible_stability")
    require("short_run" not in stability.values(), "short runs cannot establish sustained stability")
    timing, retry = fields(text, "hyperfeed_crucible_engine_timing"), fields(text, "hyperfeed_crucible_retry")
    require(int(timing["operation_failures"]) == 0 and int(row[10]) == 0 and int(row[11]) == 0,
            "operation or index mutation failure")
    elapsed = finite_number(timing["aerostore_elapsed_secs"])
    require(duration <= elapsed <= duration + 1, "workload duration/drain outside fixed envelope")
    require(abs(finite_number(row[1]) - int(row[2]) / elapsed) <= max(0.02, finite_number(row[1]) * 0.001),
            "throughput disagrees with operation count and elapsed time")
    require(int(retry["max_insert_attempts"]) <= 128 and int(retry["pressure_state"]) != 2,
            "retry/pressure correctness gate failed")
    require(int(stability["tail_fresh_bytes"]) <= int(stability["fresh_growth_budget_bytes"]),
            "tail fresh allocation exceeds seeded working set")
    require(finite_number(stability["retained_tps"]) >= 0.5, "sustained throughput collapsed")
    return {"throughput": finite_number(row[1]), "operations": int(row[2]),
            "p50_us": finite_number(row[3]), "p90_us": finite_number(row[4]), "p99_us": finite_number(row[5]),
            "retry_rate": int(row[9]) / max(1, int(row[2])), "retries": int(row[9]),
            "arena_high_water_bytes": int(stability["arena_head_bytes"]),
            "tail_fresh_bytes": int(stability["tail_fresh_bytes"]),
            "fresh_growth_budget_bytes": int(stability["fresh_growth_budget_bytes"]),
            "retained_tps": finite_number(stability["retained_tps"]), "measured_seconds": elapsed}


def parse_extended(report, workload):
    require(report.get("completed") is True and report.get("passed") is True, "extended report did not pass")
    require(not report.get("operational_errors"), "extended operational errors")
    config = report["config"]
    for key in ("families", "cycles", "workers", "seed"):
        require(config[key] == workload[key], "extended configuration mismatch: " + key)
    require(config["engine"] == "aerostore" and config["mode"] == "all" and config["shm_mib"] == 128,
            "extended engine/mode/arena differs")
    contracts = [r for r in report["contracts"] if r["engine"] == "aerostore"]
    require(len(contracts) == 1 and len(contracts[0]["cases"]) == 6 and
            all(c["passed"] for c in contracts[0]["cases"]), "native contract coverage missing or failed")
    replays = [r for r in report["replays"] if r["engine"] == "aerostore"]
    require(len(replays) == 1, "missing or duplicate Aerostore replay")
    replay = replays[0]
    require(replay["passed"] and replay["error"] is None, "replay failed")
    require(len(replay["phases"]) == 27 * workload["cycles"], "missing replay phases")
    require(replay["messages"] == workload["families"] * workload["cycles"] * 30, "message count differs")
    require(replay["committed_messages"] == workload["families"] * workload["cycles"] * 29,
            "committed message count differs")
    require(replay["index_audit"] is not None, "missing index ownership audit")
    seconds = finite_number(replay["message_wall_seconds"])
    require(seconds > 0 and math.isclose(finite_number(replay["committed_messages_per_second"]),
            replay["committed_messages"] / seconds, rel_tol=1e-9), "inconsistent extended timing")
    return {"throughput": finite_number(replay["committed_messages_per_second"]),
            "operations": replay["committed_messages"], "messages": replay["messages"],
            "p50_us": finite_number(replay["latency_p50_us"]), "p99_us": finite_number(replay["latency_p99_us"]),
            "retry_rate": replay["retries"] / max(1, replay["messages"]), "retries": replay["retries"],
            "arena_high_water_bytes": replay["index_audit"]["arena_high_water_bytes"],
            "measured_seconds": finite_number(replay["message_wall_seconds"]),
            "trace_fingerprint": report["trace_fingerprint"],
            "phase_state_fingerprints": [p["state_fingerprint"] for p in replay["phases"]]}


def parse_wal(text):
    require("test result: ok. 1 passed; 0 failed;" in text, "the fixed WAL benchmark did not pass exactly once")
    measured = fields(text, "wal_ring_benchmark")
    require(int(measured["txns"]) == 10000, "WAL operation count changed")
    return {"sync_throughput": finite_number(measured["sync_tps"]),
            "async_throughput": finite_number(measured["async_tps"]), "operations_per_mode": 10000,
            "synchronous_p99_measured": False}


def validate_manifest(path):
    manifest = json.loads(path.read_text())
    require(manifest.get("complete") is True and manifest.get("features") == "default", "incomplete/nondefault capture")
    require("release: 1.93.1\n" in manifest["rustc"], "wrong captured compiler")
    for name in (*BENCHES, "wal_ring_benchmark"):
        binary = manifest["binaries"][name]
        location = Path(binary["path"])
        if not location.is_absolute():
            location = ROOT / location
        require(location.is_file() and digest(location) == binary["sha256"], "stale binary: " + name)
        binary["resolved_path"] = str(location)
    return manifest


def validate_captured_source(manifest):
    require(source_hashes(Path(manifest["source"])) == manifest["source_sha256"],
            "captured source changed; recapture before comparing this working tree")


def compare_pairs(pairs, workload):
    """Use explicit effect/noise bounds; three pairs are not a significance proof."""
    reasons, noise, metrics = [], [], {}
    keys = ("sync_throughput", "async_throughput") if workload["kind"] == "wal" else ("throughput", "p99_us")
    for key in keys:
        baseline = [p["baseline"][key] for p in pairs]
        candidate = [p["candidate"][key] for p in pairs]
        require(all(x > 0 for x in baseline + candidate), "zero performance metric")
        ratios = [c / b for b, c in zip(baseline, candidate)]
        median = statistics.median(ratios)
        spreads = {"baseline": (max(baseline) - min(baseline)) / statistics.median(baseline),
                   "candidate": (max(candidate) - min(candidate)) / statistics.median(candidate)}
        metrics[key] = {"baseline": baseline, "candidate": candidate, "paired_ratios": ratios,
                        "median_paired_ratio": median, "relative_range": spreads}
        if workload["paired_timing"]:
            if max(spreads.values()) > POLICY["relative_range_noise_max"]:
                noise.append(key + " varies beyond the fixed noise bound")
            if key == "p99_us":
                if median > POLICY["p99_median_ratio_max"] or max(ratios) > POLICY["p99_pair_ratio_max"]:
                    reasons.append("p99 latency exceeds regression margin")
            elif median < POLICY["throughput_median_ratio_min"] or min(ratios) < POLICY["throughput_pair_ratio_min"]:
                reasons.append(key + " falls below regression margin")
    if workload["kind"] != "wal":
        b_retries = statistics.median(p["baseline"]["retry_rate"] for p in pairs)
        c_retries = statistics.median(p["candidate"]["retry_rate"] for p in pairs)
        retry_limit = max(b_retries * POLICY["retry_ratio_max"], b_retries + POLICY["retry_rate_absolute_slack"])
        if c_retries > retry_limit:
            reasons.append("retry rate exceeds regression margin")
        b_arena = statistics.median(p["baseline"]["arena_high_water_bytes"] for p in pairs)
        c_arena = statistics.median(p["candidate"]["arena_high_water_bytes"] for p in pairs)
        if c_arena > b_arena * POLICY["arena_ratio_max"] + POLICY["arena_absolute_slack_bytes"]:
            reasons.append("arena high-water mark exceeds regression margin")
        metrics["resources"] = {"baseline_retry_rate": b_retries, "candidate_retry_rate": c_retries,
                                "retry_limit": retry_limit, "baseline_arena_bytes": b_arena,
                                "candidate_arena_bytes": c_arena}
    status = "inconclusive_noise" if noise else "regression" if reasons else "pass"
    return {"status": status, "reasons": reasons, "noise": noise, "metrics": metrics,
            "timing_acceptance": workload["paired_timing"], "speedup_claim": False}


def snapshot_environment():
    return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "platform": platform.platform(),
            "cpu_affinity": sorted(os.sched_getaffinity(0)), "loadavg": list(os.getloadavg()),
            "memory": Path("/proc/meminfo").read_text()}


def run_one(binary_manifest, variant, name, workload, repetition, output):
    run_dir = output / name / f"pair-{repetition:02}-{variant}"
    run_dir.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("AEROSTORE_") or key in ("LD_PRELOAD", "LD_LIBRARY_PATH"):
            env.pop(key)
    temp = run_dir / "tmp"
    temp.mkdir()
    env["TMPDIR"] = str(temp)
    if workload["kind"] == "churn":
        binary = binary_manifest["binaries"][BENCHES[0]]["resolved_path"]
        command = [binary, "--noplot"]
        env.update(AEROSTORE_CRUCIBLE_AEROSTORE_ONLY="1", AEROSTORE_CRUCIBLE_PROFILE_FILTER="profile_2g",
                   AEROSTORE_CRUCIBLE_SHM_MIB="128", AEROSTORE_CRUCIBLE_DURATION_SECS=str(workload["duration"]),
                   AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=str(run_dir / "allocation.csv"),
                   AEROSTORE_CRUCIBLE_SAMPLE_INTERVAL_MS="5000")
    elif workload["kind"] == "extended":
        binary = binary_manifest["binaries"][BENCHES[1]]["resolved_path"]
        command = [binary, "--engine", "aerostore", "--mode", "all", "--shm-mib", "128",
                   "--output", str(run_dir / "extended.json")]
        for key in ("families", "cycles", "workers", "seed"):
            command += ["--" + key, str(workload[key])]
    else:
        binary = binary_manifest["binaries"]["wal_ring_benchmark"]["resolved_path"]
        command = [binary, WAL_TEST, "--exact", "--nocapture", "--test-threads=1"]
    record = {"variant": variant, "workload": name, "pair": repetition, "command": command,
              "binary_sha256": digest(binary), "environment_before": snapshot_environment(),
              "workload_environment": {k: v for k, v in env.items() if k.startswith("AEROSTORE_") or k == "TMPDIR"},
              "completed": False, "passed": False}
    write_json(run_dir / "run.json", record)
    started = time.monotonic()
    with (run_dir / "output.log").open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=3600)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    record.update(exit_code=code, elapsed_wall_seconds=time.monotonic() - started,
                  environment_after=snapshot_environment(), completed=True)
    try:
        require(code == 0, "benchmark exited unsuccessfully")
        text = (run_dir / "output.log").read_text()
        if workload["kind"] == "churn":
            record["metrics"] = parse_churn(text, workload["duration"])
        elif workload["kind"] == "extended":
            record["metrics"] = parse_extended(json.loads((run_dir / "extended.json").read_text()), workload)
        else:
            record["metrics"] = parse_wal(text)
        record["passed"] = True
    except Exception as error:
        record["error"] = str(error)
    record["output_sha256"] = digest(run_dir / "output.log")
    write_json(run_dir / "run.json", record)
    require(record["passed"], f"{name}/{variant} failed: {record.get('error')}; see {run_dir}")
    # These are disposable benchmark arenas/WAL files, not retained measurement evidence.
    shutil.rmtree(temp)
    return record


def campaign(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    require(args.pairs >= POLICY["minimum_pairs"], "at least three paired timing runs required")
    manifest_hashes = {"baseline": digest(args.baseline), "candidate": digest(args.candidate)}
    baseline, candidate = validate_manifest(args.baseline), validate_manifest(args.candidate)
    validate_captured_source(baseline)
    validate_captured_source(candidate)
    require(baseline["rustc"] == candidate["rustc"] and baseline["rustc_sha256"] == candidate["rustc_sha256"],
            "compiler mismatch")
    require(baseline["build_configuration"]["manifest_profiles"] ==
            candidate["build_configuration"]["manifest_profiles"], "Cargo profile mismatch")
    for name in (*BENCHES, "wal_ring_benchmark"):
        for field in ("cargo_profile", "features"):
            require(baseline["binaries"][name][field] == candidate["binaries"][name][field],
                    "artifact build configuration mismatch: " + name + "/" + field)
    require(baseline["fixture_sha256"] == candidate["fixture_sha256"], "benchmark fixture changed")
    names = args.workloads.split(",")
    require(len(names) == len(set(names)) and all(n in WORKLOADS for n in names), "invalid workload selection")
    report = {"schema_version": 1, "completed": False, "passed": False, "speedup_claim": False,
              "whole_engine_verified": False, "policy": POLICY, "workloads": {n: WORKLOADS[n] for n in names},
              "quiet_window_note": args.quiet_window_note, "baseline": baseline, "candidate": candidate,
              "capture_manifest_sha256": manifest_hashes,
              "script_sha256": digest(__file__), "runs": [], "comparisons": {},
              "scope": ["Finite default-engine regression screen; no production HyperFeed or PostgreSQL speedup claim",
                        "Crucible includes asynchronous WAL ring/writer; synchronous commit timing uses separate 10000-update test",
                        "WAL test reports sync/async throughput, not synchronous latency tails or crash durability",
                        "Three-pair effect/noise thresholds are operational screening rules, not statistical significance",
                        "240-second runs check correctness/resources/degradation; a single pair does not pass throughput timing"]}
    write_json(output / "campaign.json", report)
    try:
        for name in names:
            workload, pairs = WORKLOADS[name], []
            count = args.pairs if workload["paired_timing"] else 1
            for repetition in range(1, count + 1):
                pair = {}
                order = ("baseline", "candidate") if repetition % 2 else ("candidate", "baseline")
                for variant in order:
                    print(f"{name} pair={repetition}/{count} variant={variant}", flush=True)
                    run = run_one(baseline if variant == "baseline" else candidate,
                                  variant, name, workload, repetition, output)
                    report["runs"].append(run)
                    pair[variant] = run["metrics"]
                    write_json(output / "campaign.json", report)
                if workload["kind"] == "extended":
                    for key in ("trace_fingerprint", "phase_state_fingerprints", "messages", "operations"):
                        require(pair["baseline"][key] == pair["candidate"][key], "paired reference result changed: " + key)
                pairs.append(pair)
            report["comparisons"][name] = compare_pairs(pairs, workload)
            write_json(output / "campaign.json", report)
        # Detect replacement of captured executables while the campaign ran.
        validate_manifest(args.baseline)
        validate_manifest(args.candidate)
        validate_captured_source(baseline)
        validate_captured_source(candidate)
        require(manifest_hashes == {"baseline": digest(args.baseline), "candidate": digest(args.candidate)},
                "capture manifest changed during timing")
        report.update(completed=True, passed=all(c["status"] == "pass" for c in report["comparisons"].values()))
    except Exception as error:
        report["error"] = str(error)
        print(str(error), flush=True)
    write_json(output / "campaign.json", report)
    print(json.dumps({"passed": report["passed"], "completed": report["completed"], "output": str(output)}))
    return 0 if report["passed"] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("capture", help="build and preserve immutable benchmark executables")
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--source-commit", required=True)
    build.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run", help="time preserved executables; requires a coordinated quiet window")
    run.add_argument("--baseline", type=Path, required=True)
    run.add_argument("--candidate", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--pairs", type=int, default=3)
    run.add_argument("--workloads", default=",".join(WORKLOADS))
    run.add_argument("--quiet-window-note", required=True)
    args = parser.parse_args()
    if args.action == "capture":
        capture(args)
        return 0
    return campaign(args)


if __name__ == "__main__":
    raise SystemExit(main())
