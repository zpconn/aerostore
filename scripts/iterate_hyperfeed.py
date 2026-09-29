#!/usr/bin/env python3
"""Short, paired HyperFeed performance screens; never a sustained-capacity gate.

Capture once per edit, screen against a preserved baseline, qualify finalists.
Outputs are fresh and retained. No automatic build during timing, PostgreSQL
restart, full-history check, archive upload, or evidence deletion is performed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import time

import hyperfeed_screen_capture as capture
import hyperfeed_screen_resources as resources
import hyperfeed_screen_assessment as assessment
import hyperfeed_proof_impact as proof_impact

ROOT = Path(__file__).resolve().parents[1]
GIB = 1 << 30
SEEDS = [20260929, 20260930]
LANES = {"foreground": (30, 300), "burst": (120, 300), "maintenance": (40, 5)}
ADAPTERS = {"aerostore_core/benches/contention_crucible/" + name + ".rs"
            for name in ("service", "aerostore", "postgres")}
SCOPE = ("Short synthetic screening only. No sustained capacity, full-history "
         "correctness, production HyperFeed, durability, or MMHF qualification.")


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    temporary = Path(path).with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def capture_path(path):
    path = Path(path).resolve()
    if path.is_file():
        return path
    if (path / "capture.json").exists():
        return path / "capture.json"
    return path / "capture" / "capture.json"


def schedule(lane, seeds):
    require(lane in LANES, "unknown screen lane")
    require(len(seeds) == 2 and len(set(seeds)) == 2 and
            all(type(seed) is int and 0 <= seed < 2**64 for seed in seeds),
            "declare two distinct unsigned seeds")
    return [{"id": f"{i:02d}-{variant}-s{seed}", "variant": variant,
             "seed": seed, "lane": lane}
            for i, (variant, seed) in enumerate(
                [("baseline", seeds[0]), ("candidate", seeds[0]),
                 ("candidate", seeds[1]), ("baseline", seeds[1])], 1)]


def compare_inputs(base, candidate, allowed):
    require(base["comparison_build_identity"] == candidate["comparison_build_identity"],
            "compiler/build identities differ; capture both variants with the same build recipe")
    left, right = base["source"]["files"], candidate["source"]["files"]
    changes = sorted(name for name in left.keys() | right.keys() if left.get(name) != right.get(name))
    require(left.get("scripts/qualify_hyperfeed.py") == right.get("scripts/qualify_hyperfeed.py"),
            "qualification driver differs between variants")
    treatment, auxiliary = [], []
    for name in changes:
        if "/tests/" in name or name.startswith("scripts/") or not name.endswith((".rs", ".toml", ".lock", ".cfg")):
            auxiliary.append(name)
            continue
        if "/benches/" in name:
            require(name in ADAPTERS, "workload/accounting/fixture source changed: " + name)
        require(name.endswith(".rs") and name in allowed,
                "undeclared implementation or build-input change: " + name)
        treatment.append(name)
    require(set(allowed) == set(treatment), "--allow-change must name exactly the changed implementation files")
    return {"implementation_changes": treatment, "auxiliary_changes": auxiliary,
            "identical_binary": base["benchmark"]["sha256"] == candidate["benchmark"]["sha256"]}


def qualifier_parameters(manifest, output, lane, rate, seed, arena_backing="file"):
    require(arena_backing in {"file", "memfd"}, "unknown arena backing")
    seconds, timer = LANES[lane]
    values = {
        "binary": manifest["benchmark"]["path"], "output": output,
        "engines": "service-unix", "rates": rate, "workers": 16, "seeds": seed,
        "workload": "calibrated", "evidence": "metrics", "families": 1024,
        "seconds": seconds, "hot-percent": 0, "slo-ms": 50,
        "max-backlog": 10000, "max-messages": 100000, "shm-mib": 2048,
        "arena-backing": arena_backing,
        "cpu-budget": 24, "outcome-tolerance": 0, "max-noop-fraction": 0,
        "minimum-drain-fraction": .99, "pg-write-mode": "buffered",
        "pg-analyze-after-seconds": 5, "pg-candidate-query": "split",
        "maintenance-selection": "prefix", "rpc-delay-us": 0,
        "maintenance-mode": "sweep", "projection-batch-size": 4,
        "housekeeping-batch-size": 32, "max-maintenance-batches": 4096,
        "dispatch": "signature-affinity", "affinity-ttl-ms": 600,
        "signature-pattern": "both", "rolling-cycle-messages": 0,
        "rolling-retention-seconds": 0, "projection-interval-seconds": timer,
        "housekeeping-interval-seconds": timer, "expiry-index": "housekeeping",
        "due-index": "ordered", "due-index-origin": 1700000000000000000,
        "due-index-width": 1000000000, "expiry-publication": "ordered",
        "expiry-index-origin": 1700000000000000000,
        "expiry-index-width": 1000000000, "retry-diagnostics": "off",
        "timeout-seconds": seconds + 90,
    }
    return values


def expected_config(values):
    aliases = {"engines": "engine", "rates": "arrival_rate", "seeds": "seed",
               "expiry-index": "expiry_index_policy", "due-index": "due_index_policy",
               "expiry-publication": "expiry_publication_policy"}
    driver_only = {"binary", "output", "slo-ms", "cpu-budget", "outcome-tolerance",
                   "max-noop-fraction", "minimum-drain-fraction", "timeout-seconds"}
    return {aliases.get(key, key.replace("-", "_")):
            (value == "on" if key == "retry-diagnostics" else value)
            for key, value in values.items() if key not in driver_only}


def qualifier_command(manifest, output, lane, rate, seed, arena_backing="file"):
    values = qualifier_parameters(manifest, output, lane, rate, seed, arena_backing)
    driver = Path(manifest["source"]["root"]) / "scripts/qualify_hyperfeed.py"
    return [sys.executable, str(driver), *[v for key, value in values.items()
                                          for v in ("--" + key, str(value))]]


def guarded_command(command, cpus):
    # This runs inside the cgroup: systemd does not inherit the caller's rlimits
    # or affinity. exec preserves both limits for all benchmark descendants.
    code = ("import os,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
            "os.sched_setaffinity(0,{" + ",".join(map(str, cpus)) + "}); "
            "os.execv(sys.argv[1],sys.argv[1:])")
    return [sys.executable, "-c", code, *command]


def run_envelope(command, folder, cpus, timeout, controller):
    argv = [sys.executable, str(controller), "--output", str(folder / "resources"),
            "--memory-max", str(36 * GIB), "--swap-max", str(4 * GIB),
            "--reserve-bytes", str(4 * GIB), "--timeout-seconds", str(timeout),
            "--", *guarded_command(command, cpus)]
    write(folder / "command.json", {"command": argv, "cpu_affinity": cpus,
                                    "core_limit_bytes": 0, "started_at": now()})
    with (folder / "controller.log").open("x") as log:
        process = subprocess.Popen(argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            code = process.wait(timeout=timeout + 90)
        finally:
            if process.poll() is None:
                process.terminate()  # Controller finally stops its owned service and slice.
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
    result = read(folder / "resources/result.json")
    require(result.get("cleanup", {}).get("owned_processes_terminated") is True,
            "owned process cleanup is unconfirmed; no following stage may start")
    return {"controller_exit_code": code, "result": result,
            "readiness": read(folder / "resources/readiness.json"),
            "samples": [json.loads(line) for line in
                        (folder / "resources/samples.jsonl").read_text().splitlines()]}


@contextmanager
def campaign_lock():
    (ROOT / "target").mkdir(exist_ok=True)
    with (ROOT / "target/hyperfeed-iteration.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def prepare(args, prospective):
    output = args.output.resolve()
    require(output.is_relative_to(ROOT / "target") and not output.exists(),
            "use a fresh output directory beneath this repository's target/")
    for variable in ("LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH"):
        require(not os.environ.get(variable), "unexpected runtime override: " + variable)
    # A session created with `init` owns one budget across captures and screens.
    # Walking ancestors reuses that immutable baseline rather than resetting
    # allocated growth whenever another candidate or lane gets a fresh directory.
    budget = next((parent / "resource-baseline.json" for parent in output.parents
                   if parent.is_relative_to(ROOT / "target")
                   and (parent / "resource-baseline.json").exists()), None)
    if budget is None:
        preflight = resources.preflight(ROOT / "target", output,
                                        prospective_bytes=prospective, host_volume=args.host_volume)
        budget = output / "resource-baseline.json"
    else:
        if args.host_volume is not None:
            require(str(args.host_volume.resolve()) == read(budget)["initial_snapshot"]["host_volume"],
                    "host volume differs from the session budget")
        preflight = resources.check_budget(budget, prospective_bytes=prospective)
        output.mkdir(parents=True, exist_ok=False)
    write(output / "resource-admission.json", {"baseline": str(budget),
          "baseline_sha256": digest(budget), "decision": preflight})
    require(preflight["passed"], "resource admission refused: " + str(preflight.get("reasons")))
    driver = output / "driver"
    driver.mkdir()
    names = ["iterate_hyperfeed.py", "hyperfeed_screen_capture.py", "hyperfeed_screen_resources.py",
             "hyperfeed_screen_assessment.py", "run_memory_envelope.py", "assess_hyperfeed_capacity.py",
             "qualify_hyperfeed.py", "hyperfeed_proof_impact.py"]
    for name in names:
        shutil.copy2(ROOT / "scripts" / name, driver / name)
    write(output / "driver.json", {name: digest(driver / name) for name in names})
    return output, budget, driver


def cpus_for_screen():
    cpus = sorted(os.sched_getaffinity(0))
    require(len(cpus) >= 24, "this screen profile requires 24 available logical CPUs")
    return cpus[:24]


def final_resource_status(record, result):
    record["resources_after"] = result
    if not result["passed"]:
        record.update(completed=False, stop_classification="censored_resources")
        if "comparison" in record:
            record["comparison_before_resource_exclusion"] = record["comparison"]
        record["comparison"] = {"classification": "inconclusive", "decision": "inconclusive",
            "screening_only": True, "sustained_capacity_established": False,
            "capacity_gain_established": False, "reasons": result.get("reasons", [])}


def screen(args):
    paths = {label: capture_path(getattr(args, label)) for label in ("baseline", "candidate")}
    variants = {label: capture.validate_capture(path) for label, path in paths.items()}
    comparison = compare_inputs(variants["baseline"], variants["candidate"], set(args.allow_change))
    cpus = cpus_for_screen()
    plan = schedule(args.lane, args.seeds)
    output, budget, driver = prepare(args, 8 * GIB)
    write(output / "proof-impact.json", proof_impact.report(variants["baseline"], variants["candidate"]))
    record = {"format": "hyperfeed-iteration-screen-v1", "started_at": now(), "completed": False,
              "scope": SCOPE, "sustained_capacity_established": False,
              "source_comparison": comparison, "lane": args.lane, "rate": args.rate,
              "arena_backing": args.arena_backing,
              "cpu_affinity": cpus, "plan": plan, "trials": [],
              "captures": {label: {"path": str(path), "sha256": digest(path)}
                           for label, path in paths.items()},
              "policy": assessment.lane_policy(args.lane),
              "comparison_policy": {**assessment.COMPARISON_POLICY,
                                    "seeds": args.seeds, "lanes": [args.lane],
                                    "identical_binary": comparison["identical_binary"]}}
    path = output / "screen.json"
    write(path, record)
    last_envelope = None
    try:
        for cell in plan:
            record["active_cell"] = cell["id"]
            last_envelope = None
            write(path, record)
            check = resources.check_budget(budget, prospective_bytes=2 * GIB)
            require(check["passed"], "resource admission refused: " + str(check.get("reasons")))
            manifest = capture.validate_capture(paths[cell["variant"]])
            require(digest(paths[cell["variant"]]) == record["captures"][cell["variant"]]["sha256"],
                    "capture manifest changed during screen")
            folder = output / cell["id"]
            folder.mkdir()
            write(folder / "admission.json", check)
            print(f"START {cell['id']} lane={args.lane} rate={args.rate}", flush=True)
            command = qualifier_command(manifest, folder / "qualification", args.lane, args.rate, cell["seed"], args.arena_backing)
            envelope = run_envelope(command, folder, cpus, LANES[args.lane][0] + 150,
                                    driver / "run_memory_envelope.py")
            last_envelope = envelope
            campaign_path = folder / "qualification/campaign.json"
            campaign = read(campaign_path)
            require(campaign.get("completed") and campaign.get("source_stable"), "incomplete or changed-source trial")
            require(campaign.get("source_before", {}).get("sha256") == manifest["source"]["sha256"]
                    == campaign.get("source_after", {}).get("sha256"), "trial does not match frozen source")
            require(campaign.get("binary_before_sha256") == manifest["benchmark"]["sha256"]
                    == campaign.get("binary_after_sha256"), "trial does not match retained executable")
            require(len(campaign.get("trials", [])) == 1, "expected one trial per cell")
            require(campaign.get("host_before", {}).get("cpu_affinity") == cpus,
                    "actual trial CPU affinity differs from plan")
            capture.validate_capture(paths[cell["variant"]])
            trial_policy = {**record["policy"], "expected_config": expected_config(
                qualifier_parameters(manifest, folder / "qualification", args.lane, args.rate, cell["seed"], args.arena_backing))}
            result = assessment.assess_screen_trial(campaign["trials"][0], envelope, trial_policy)
            row = {**cell, "assessment": result, "campaign": str(campaign_path),
                   "campaign_sha256": digest(campaign_path)}
            write(folder / "assessment.json", row)
            record["trials"].append(row)
            write(path, record)
            print(f"DONE {cell['id']} {result['classification']}", flush=True)
            require(result["valid_measurement"], "invalid/censored screen; remaining cells not started")
        record["comparison"] = assessment.compare_screens(record["trials"], record["comparison_policy"])
        record["completed"] = True
    except BaseException as error:
        record["error"] = type(error).__name__ + ": " + str(error)
        last = record["trials"][-1] if record["trials"] else None
        record["stop_classification"] = (last["assessment"]["classification"]
            if last and last["id"] == record.get("active_cell") else "invalid_or_interrupted_evidence")
        if last_envelope is not None:
            try:
                health = assessment.capacity.resource_assessment(last_envelope["result"],
                    last_envelope["readiness"], last_envelope["samples"], record["policy"])
                if not health["passed"]:
                    record["stop_classification"] = "censored_resources"
            except (KeyError, ValueError, TypeError):
                record["stop_classification"] = "invalid_resource_evidence"
        record["comparison"] = assessment.compare_screens(record["trials"], record["comparison_policy"])
    finally:
        record["finished_at"] = now()
        final_resource_status(record, resources.check_budget(budget, prospective_bytes=0))
        write(path, record)
    print(json.dumps({"completed": record["completed"], "comparison": record.get("comparison"),
                      "error": record.get("error"), "report": str(path)}, indent=2), flush=True)
    return 0 if record["completed"] else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("init", "capture", "import", "screen"):
        p = commands.add_parser(name)
        p.add_argument("--output", type=Path, required=True)
        p.add_argument("--host-volume", type=Path, help="actual Windows volume hosting this WSL VHDX (default /mnt/c)")
        if name == "capture":
            p.add_argument("--jobs", type=int, default=8, choices=range(1, 9))
        elif name == "import":
            p.add_argument("--build-receipt", type=Path, required=True)
        elif name == "screen":
            p.add_argument("--baseline", type=Path, required=True)
            p.add_argument("--candidate", type=Path, required=True)
            p.add_argument("--allow-change", action="append", default=[])
            p.add_argument("--lane", choices=LANES, default="foreground")
            p.add_argument("--arena-backing", choices=["file", "memfd"], default="file",
                           help="same native arena configuration for both implementation captures")
            p.add_argument("--rate", type=int, default=3584)
            p.add_argument("--seeds", type=int, nargs=2, default=SEEDS)
    args = parser.parse_args(argv)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    if args.action == "screen":
        require(16 <= args.rate <= 8192, "screen rate must be between 16 and 8192 incoming messages/s")
    with campaign_lock():
        if args.action == "screen":
            return screen(args)
        output, budget, driver = prepare(args, (8 if args.action in {"init", "capture"} else 1) * GIB)
        if args.action == "init":
            print(json.dumps({"session": str(output), "resource_baseline": str(budget)}, indent=2))
            return 0
        if args.action == "import":
            manifest = capture.import_capture(args.build_receipt, output / "capture")
        else:
            host_volume = read(budget)["initial_snapshot"]["host_volume"]
            command = [sys.executable, "-c",
                       "import sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); "
                       "from hyperfeed_screen_capture import capture_variant; "
                       "capture_variant(Path(sys.argv[2]),jobs=int(sys.argv[3]),repo=Path(sys.argv[4]), "
                       "host_volume=Path(sys.argv[5]) if sys.argv[5] else None)",
                       str(driver), str(output / "capture"), str(args.jobs), str(ROOT), host_volume or ""]
            envelope = run_envelope(command, output, cpus_for_screen(), 1800, driver / "run_memory_envelope.py")
            require(envelope["controller_exit_code"] == 0, "capture build failed; see controller evidence")
            manifest = capture.validate_capture(output / "capture")
        resource_result = resources.check_budget(budget, prospective_bytes=0)
        write(output / "resources-after.json", resource_result)
        require(resource_result["passed"], "final capture resource check failed; see resources-after.json")
        print(json.dumps({"capture": str(output / "capture/capture.json"),
                          "binary_sha256": manifest["benchmark"]["sha256"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
