#!/usr/bin/env python3
"""Compare preserved default-engine binaries with fixed, paired workloads.

Capture is separate from timing. No compilation occurs during a campaign.
This is a finite regression screen, not a HyperFeed speedup or durability proof.
"""

import argparse
from fractions import Fraction
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
import tempfile
import time
import tomllib


ROOT = Path(__file__).resolve().parents[1]
BENCHES = ("hyperfeed_crucible", "hyperfeed_extended_crucible")
WAL_TEST = "benchmark_async_synchronous_commit_modes"
CHURN_SEED_ALGORITHM = "worker_add_xorshift64_v1"
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


def production_hashes(hashes):
    return {name: value for name, value in hashes.items()
            if "/benches/" not in name and "/tests/" not in name}


def verify_fixture_transition(source, current, parent, patch):
    """Reverse the exact patch in scratch and reconstruct the parent source map."""
    patch = patch.resolve()
    git_env = os.environ.copy()
    for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"):
        git_env.pop(key, None)
    git_env.update(GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
    with tempfile.TemporaryDirectory(prefix="aerostore-fixture-transition-") as directory:
        scratch = Path(directory)
        stat = subprocess.check_output(["git", "apply", "--numstat", "-z", str(patch)],
                                       cwd=scratch, env=git_env)
        names = []
        for entry in stat.decode().split("\0"):
            if not entry:
                continue
            parts = entry.split("\t")
            require(len(parts) == 3 and all(n.isdigit() for n in parts[:2]),
                    "fixture patch must contain ordinary text-file changes")
            name = parts[2]
            relative = Path(name)
            require(not relative.is_absolute() and ".." not in relative.parts and
                    str(relative) == name and relative.suffix == ".rs" and
                    (name.startswith("aerostore_core/benches/") or name.startswith("aerostore_core/tests/")),
                    "fixture patch path is outside the benchmark/test Rust allowlist")
            require(name not in names, "duplicate fixture patch path")
            names.append(name)
            location = source / relative
            if location.exists():
                require(not location.is_symlink() and location.resolve().is_relative_to(source.resolve()),
                        "fixture source escapes its captured tree")
                destination = scratch / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(location, destination)
        require(bool(names), "empty fixture patch")
        subprocess.run(["git", "apply", "--reverse", "--whitespace=nowarn", str(patch)],
                       cwd=scratch, env=git_env, check=True, capture_output=True)
        reconstructed = dict(current)
        for name in names:
            restored = scratch / name
            if restored.is_file():
                reconstructed[name] = digest(restored)
            else:
                reconstructed.pop(name, None)
        require(reconstructed == parent, "exact fixture patch does not reconstruct its parent source")
    return sorted(names)


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
    report["production_sha256"] = production_hashes(initial)
    if args.fixture_patch is not None or args.fixture_parent_capture is not None:
        require(args.fixture_patch is not None and args.fixture_parent_capture is not None,
                "fixture capture requires both the identical patch and its preserved parent capture")
        parent = validate_manifest(args.fixture_parent_capture)
        require(production_hashes(parent["source_sha256"]) == report["production_sha256"],
                "production changed during a benchmark-only fixture recapture")
        touched = verify_fixture_transition(source, initial, parent["source_sha256"], args.fixture_patch)
        patch = output / "fixture.patch"
        shutil.copy2(args.fixture_patch, patch)
        report["fixture_patch"] = {"path": str(patch), "sha256": digest(patch)}
        report["fixture_parent_capture"] = {"path": str(args.fixture_parent_capture.resolve()),
                                             "sha256": digest(args.fixture_parent_capture),
                                             "production_unchanged": True,
                                             "exact_patch_transition_verified": True,
                                             "patch_paths": touched}
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


def parse_seed(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9]+", value) is not None,
            "seed must be an unsigned decimal u64")
    seed = int(value)
    require(seed <= (1 << 64) - 1, "seed exceeds u64")
    return seed


def workload_seed(workload, repetition):
    if workload["kind"] != "churn":
        return None
    require(not ("seed" in workload and "seeds" in workload), "ambiguous churn seed configuration")
    if "seeds" in workload:
        require(isinstance(workload["seeds"], list) and 1 <= repetition <= len(workload["seeds"]),
                "missing predeclared seed for this pair")
        seed = workload["seeds"][repetition - 1]
    else:
        seed = workload.get("seed")
    if seed is not None:
        require(type(seed) is int and 0 <= seed <= (1 << 64) - 1, "invalid workload seed")
    return seed


def selected_workloads(names, pairs, churn_seeds=None, sustained_churn_seed=None):
    require(len(names) == len(set(names)) and all(n in WORKLOADS for n in names), "invalid workload selection")
    workloads = {name: dict(WORKLOADS[name]) for name in names}
    if churn_seeds is not None:
        seeds = [parse_seed(value) for value in churn_seeds.split(",")]
        require(len(seeds) == pairs, "provide exactly one churn seed per pair")
        selected = [w for w in workloads.values() if w["kind"] == "churn" and w["paired_timing"]]
        require(bool(selected), "paired churn seeds provided without a paired churn workload")
        for workload in selected:
            workload["seeds"] = list(seeds)
    if sustained_churn_seed is not None:
        seed = parse_seed(sustained_churn_seed)
        selected = [w for w in workloads.values() if w["kind"] == "churn" and not w["paired_timing"]]
        require(bool(selected), "sustained seed provided without a sustained churn workload")
        for workload in selected:
            workload["seed"] = seed
    return workloads


def parse_churn(text, duration, expected_seed=None):
    seed_metadata = None
    if expected_seed is not None or re.search(r"^hyperfeed_crucible_seed:", text, re.MULTILINE):
        require(expected_seed is None or (type(expected_seed) is int and 0 <= expected_seed <= (1 << 64) - 1),
                "invalid expected workload seed")
        expected = ({"mode": "fixed", "seed": str(expected_seed), "algorithm": CHURN_SEED_ALGORITHM}
                    if expected_seed is not None else
                    {"mode": "entropy", "seed": "none", "algorithm": "pid_time_xorshift64_v1"})
        markers = re.findall(r"^hyperfeed_crucible_seed: (.*)$", text, re.MULTILINE)
        require(markers == [" ".join(f"{key}={value}" for key, value in expected.items())],
                "workload seed marker differs from the declared run")
        seed_metadata = expected
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
    interval = None
    precision = "coarse_log2_lower_bound"
    if re.search(r"^hyperfeed_crucible_latency_bounds:", text, re.MULTILINE):
        bounds = fields(text, "hyperfeed_crucible_latency_bounds")
        require(bounds.get("engine") == "aerostore" and int(bounds["histogram_subdivisions"]) == 64,
                "unexpected histogram engine/resolution")
        require(int(bounds["samples"]) == int(row[2]), "histogram sample count differs from operations")
        lower, upper = int(bounds["p99_lower_ns"]), int(bounds["p99_upper_ns"])
        require(0 < lower <= upper <= (1 << 64) - 1 and 64 * upper <= 65 * lower,
                "invalid or excessively wide p99 interval")
        require(abs(finite_number(row[5]) - upper / 1000) <= 0.0051,
                "displayed p99 disagrees with integer upper bound")
        interval = [lower, upper]
        precision = "integer_64_subdivisions"
    return {"throughput": finite_number(row[1]), "operations": int(row[2]),
            "p50_us": finite_number(row[3]), "p90_us": finite_number(row[4]), "p99_us": finite_number(row[5]),
            "p99_interval_ns": interval, "p99_precision": precision,
            "retry_rate": int(row[9]) / max(1, int(row[2])), "retries": int(row[9]),
            "arena_high_water_bytes": int(stability["arena_head_bytes"]),
            "tail_fresh_bytes": int(stability["tail_fresh_bytes"]),
            "fresh_growth_budget_bytes": int(stability["fresh_growth_budget_bytes"]),
            "retained_tps": finite_number(stability["retained_tps"]), "measured_seconds": elapsed,
            "workload_seed": expected_seed, "seed_metadata": seed_metadata}


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
            "p99_interval_ns": [round(finite_number(replay["latency_p99_us"]) * 1000)] * 2,
            "p99_precision": "sorted_message_samples",
            "retry_rate": replay["retries"] / max(1, replay["messages"]), "retries": replay["retries"],
            "arena_high_water_bytes": replay["index_audit"]["arena_high_water_bytes"],
            "measured_seconds": finite_number(replay["message_wall_seconds"]),
            "trace_fingerprint": report["trace_fingerprint"],
            "phase_state_fingerprints": [p["state_fingerprint"] for p in replay["phases"]]}


def parse_wal(text):
    require("test result: ok. 1 passed; 0 failed;" in text, "the fixed WAL benchmark did not pass exactly once")
    require(re.search(r"^test " + re.escape(WAL_TEST) + r" \.\.\.", text, re.MULTILINE) is not None,
            "the expected WAL test name is missing")
    rows = re.findall(r"^(?:test " + re.escape(WAL_TEST) + r" \.\.\. )?wal_ring_benchmark: (.*)$",
                      text, re.MULTILINE)
    require(len(rows) == 1, "missing or ambiguous wal_ring_benchmark")
    measured = dict(re.findall(r"(\w+)=([^\s]+)", rows[0]))
    require(int(measured["txns"]) == 10000, "WAL operation count changed")
    return {"sync_throughput": finite_number(measured["sync_tps"]),
            "async_throughput": finite_number(measured["async_tps"]), "operations_per_mode": 10000,
            "synchronous_p99_measured": False}


def validate_manifest(path):
    manifest = json.loads(path.read_text())
    require(manifest.get("complete") is True and manifest.get("features") == "default", "incomplete/nondefault capture")
    require("release: 1.93.1\n" in manifest["rustc"], "wrong captured compiler")
    if manifest.get("fixture_patch") is not None:
        patch = manifest["fixture_patch"]
        require(Path(patch["path"]).is_file() and digest(patch["path"]) == patch["sha256"],
                "fixture patch artifact changed")
        parent = manifest["fixture_parent_capture"]
        require(Path(parent["path"]).is_file() and digest(parent["path"]) == parent["sha256"],
                "parent capture manifest changed")
        require(parent["production_unchanged"] is True and
                parent["exact_patch_transition_verified"] is True and
                production_hashes(json.loads(Path(parent["path"]).read_text())["source_sha256"]) ==
                production_hashes(manifest["source_sha256"]), "fixture capture changed production")
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
    """Use explicit effect/noise bounds; histogram endpoints are inclusive."""
    reasons, noise, resolution, metrics = [], [], [], {}
    keys = ("sync_throughput", "async_throughput") if workload["kind"] == "wal" else ("throughput", "p99_us")
    for key in keys:
        baseline = [p["baseline"][key] for p in pairs]
        candidate = [p["candidate"][key] for p in pairs]
        require(all(x > 0 for x in baseline + candidate), "zero performance metric")
        ratios = [c / b for b, c in zip(baseline, candidate)]
        median = statistics.median(ratios)
        spreads = {"baseline": (max(baseline) - min(baseline)) / statistics.median(baseline),
                   "candidate": (max(candidate) - min(candidate)) / statistics.median(candidate)}
        metric = {"baseline": baseline, "candidate": candidate, "paired_ratios": ratios,
                  "median_paired_ratio": median, "relative_range": spreads,
                  "status": "not_evaluated" if not workload["paired_timing"] else "pass"}
        metrics[key] = metric
        if not workload["paired_timing"]:
            continue
        if key == "p99_us":
            intervals = {variant: [p[variant].get("p99_interval_ns") for p in pairs]
                         for variant in ("baseline", "candidate")}
            metric["intervals_ns"] = intervals
            if any(interval is None for rows in intervals.values() for interval in rows):
                resolution.append("coarse p99 buckets cannot establish the latency regression bound")
                metric["status"] = "inconclusive_resolution"
                continue
            require(all(len(v) == 2 and all(isinstance(x, int) and not isinstance(x, bool) for x in v)
                        and 0 < v[0] <= v[1] <= (1 << 64) - 1
                        for rows in intervals.values() for v in rows), "invalid p99 interval")
            lower_ratios = [Fraction(c[0], b[1]) for b, c in zip(intervals["baseline"], intervals["candidate"])]
            upper_ratios = [Fraction(c[1], b[0]) for b, c in zip(intervals["baseline"], intervals["candidate"])]
            median_lower, median_upper = statistics.median(lower_ratios), statistics.median(upper_ratios)
            metric.update(paired_ratio_lower_bounds=[float(v) for v in lower_ratios],
                          paired_ratio_upper_bounds=[float(v) for v in upper_ratios],
                          median_paired_ratio_lower_bound=float(median_lower),
                          median_paired_ratio_upper_bound=float(median_upper))
            # For every possible exact percentile inside the reported intervals,
            # this upper bound covers the within-executable relative range.
            interval_spreads = {variant: Fraction(max(v[1] for v in rows) - min(v[0] for v in rows)) /
                               statistics.median(Fraction(v[0]) for v in rows) for variant, rows in intervals.items()}
            metric["relative_range_upper_bound"] = {variant: float(value) for variant, value in interval_spreads.items()}
            if max(interval_spreads.values()) > Fraction(str(POLICY["relative_range_noise_max"])):
                noise.append("p99 interval spread exceeds the fixed noise bound")
                metric["status"] = "inconclusive_noise"
            if (median_lower > Fraction(str(POLICY["p99_median_ratio_max"])) or
                    max(lower_ratios) > Fraction(str(POLICY["p99_pair_ratio_max"]))):
                reasons.append("p99 latency necessarily exceeds regression margin")
                if metric["status"] == "pass":
                    metric["status"] = "regression"
            elif (median_upper > Fraction(str(POLICY["p99_median_ratio_max"])) or
                    max(upper_ratios) > Fraction(str(POLICY["p99_pair_ratio_max"]))):
                resolution.append("p99 interval overlaps the regression threshold")
                if metric["status"] == "pass":
                    metric["status"] = "inconclusive_resolution"
        else:
            if max(spreads.values()) > POLICY["relative_range_noise_max"]:
                noise.append(key + " varies beyond the fixed noise bound")
                metric["status"] = "inconclusive_noise"
            if median < POLICY["throughput_median_ratio_min"] or min(ratios) < POLICY["throughput_pair_ratio_min"]:
                reasons.append(key + " falls below regression margin")
                if metric["status"] == "pass":
                    metric["status"] = "regression"
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
    status = ("inconclusive_noise" if noise else "inconclusive_resolution" if resolution else
              "regression" if reasons else "pass")
    return {"status": status, "reasons": reasons, "noise": noise, "resolution": resolution,
            "metrics": metrics, "timing_acceptance": workload["paired_timing"], "speedup_claim": False}


def snapshot_environment():
    return {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "platform": platform.platform(),
            "cpu_affinity": sorted(os.sched_getaffinity(0)), "loadavg": list(os.getloadavg()),
            "memory": Path("/proc/meminfo").read_text()}


def run_one(binary_manifest, variant, name, workload, repetition, output):
    seed = workload_seed(workload, repetition)
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
        if seed is not None:
            env["AEROSTORE_CRUCIBLE_SEED"] = str(seed)
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
              "workload_seed": seed,
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
            record["metrics"] = parse_churn(text, workload["duration"], expected_seed=seed)
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
    require(baseline.get("fixture_patch", {}).get("sha256") == candidate.get("fixture_patch", {}).get("sha256"),
            "benchmark-only fixture patch differs between variants")
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
    workloads = selected_workloads(names, args.pairs, args.churn_seeds, args.sustained_churn_seed)
    report = {"schema_version": 1, "completed": False, "passed": False, "speedup_claim": False,
              "whole_engine_verified": False, "policy": POLICY, "workloads": workloads,
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
            workload, pairs = workloads[name], []
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
    build.add_argument("--fixture-patch", type=Path)
    build.add_argument("--fixture-parent-capture", type=Path)
    run = commands.add_parser("run", help="time preserved executables; requires a coordinated quiet window")
    run.add_argument("--baseline", type=Path, required=True)
    run.add_argument("--candidate", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--pairs", type=int, default=3)
    run.add_argument("--workloads", default=",".join(WORKLOADS))
    run.add_argument("--quiet-window-note", required=True)
    run.add_argument("--churn-seeds", help="Comma-separated decimal u64 seeds, one per paired churn repetition")
    run.add_argument("--sustained-churn-seed", help="Decimal u64 seed for the single sustained churn pair")
    args = parser.parse_args()
    if args.action == "capture":
        capture(args)
        return 0
    return campaign(args)


if __name__ == "__main__":
    raise SystemExit(main())
