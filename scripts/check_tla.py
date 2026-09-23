#!/usr/bin/env python3
"""Run the declared finite TLC pilot, checking failures by property and trace.

No Rust correctness/proof-transport claim is made. Download is opt-in, pinned
by SHA-256. Complete finite searches, bug counterexamples and reachability
witnesses are different result classes in the generated evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "verification" / "tla"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tla_value(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    raise ValueError(f"unsupported TLC constant: {value!r}")


def classify(output: str, returncode: int | None, case: dict) -> tuple[bool, str]:
    expected = case["expected"]
    if expected not in {"complete", "counterexample", "liveness_counterexample", "witness"}:
        raise ValueError(f"unknown expected TLC outcome: {expected}")
    if expected == "complete":
        passed = (
            returncode == 0
            and "Model checking completed. No error has been found." in output
            and re.search(r"^[\d,]+ states generated, [\d,]+ distinct states found, 0 states left on queue\.$",
                          output, re.MULTILINE) is not None
            and not re.search(r"^Error:", output, re.MULTILINE)
        )
        return passed, "completed_finite_search" if passed else "incomplete_or_failed"
    if expected == "liveness_counterexample":
        # The pinned TLC names the property. Require that exact diagnostic
        # and an actual cyclic/stuttering counterexample, not a safety error.
        violation = f"Error: Temporal property {case['temporal_property']} was violated."
        errors = set(re.findall(r"^Error:.*$", output, re.MULTILINE))
        allowed_errors = {violation,
                          "Error: The following behavior constitutes a counter-example:"}
        # Exit codes 12/13 are the invariant/temporal violation codes in the
        # pinned TLC. A killed process or timeout after printing a partial
        # trace must not count as a completed expected failure.
        passed = (returncode == 13
                  and violation in errors
                  and re.search(r"^State 1:", output, re.MULTILINE) is not None
                  and re.search(r"^(?:Back to state [1-9][0-9]*:|State [1-9][0-9]*: Stuttering)",
                                output, re.MULTILINE) is not None
                  and errors <= allowed_errors)
        return passed, "intended_liveness_counterexample" if passed else "wrong_or_missing_failure"
    property_name = case["failure_property"]
    violation = f"Error: Invariant {property_name} is violated."
    allowed_errors = {violation, "Error: The behavior up to this point is:"}
    errors = set(re.findall(r"^Error:.*$", output, re.MULTILINE))
    passed = (
        returncode == 12
        and violation in errors
        and re.search(r"^State 1:", output, re.MULTILINE) is not None
        and errors <= allowed_errors
    )
    return passed, ("reachable_witness" if expected == "witness" else "intended_counterexample") if passed else "wrong_or_missing_failure"


def run_in_stage(args: argparse.Namespace, stage: Path) -> int:
    toolchain = json.loads((MODELS / "toolchain.json").read_text())
    campaign = json.loads((MODELS / "campaign.json").read_text())
    if args.list:
        for case in campaign["cases"]:
            print(f"{case['name']}: {case['expected']}")
        return 0
    selected = [case for case in campaign["cases"] if not args.case or case["name"] in args.case]
    unknown = set(args.case or []) - {case["name"] for case in selected}
    if unknown or not selected:
        raise ValueError(f"unknown/empty case selection: {sorted(unknown)}")
    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.json"
    # A failed rerun cannot leave an old successful marker behind.
    report = {"format_version": 1, "passed": False, "completed": False,
              "complete_campaign": len(selected) == len(campaign["cases"]),
              "scope": campaign["scope"], "toolchain": toolchain, "results": []}
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    blocked_environment = ("TLC_LIBRARY", "TLA_LIBRARY", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS",
                           "_JAVA_OPTIONS", "CLASSPATH")
    injected = [name for name in blocked_environment if os.environ.get(name)]
    if injected:
        raise ValueError("unreviewed Java/TLA environment override: " + ", ".join(injected))
    java_environment = dict(os.environ)
    for name in blocked_environment:
        java_environment.pop(name, None)
    jar = args.jar.resolve() if args.jar else ROOT / toolchain["default_path"]
    if not jar.exists():
        if not args.download:
            raise ValueError(f"missing TLC jar: {jar}; rerun with --download")
        jar.parent.mkdir(parents=True, exist_ok=True)
        temporary = jar.with_suffix(".download")
        try:
            with urllib.request.urlopen(toolchain["release_url"], timeout=60) as response:
                temporary.write_bytes(response.read())
            if digest(temporary) != toolchain["sha256"]:
                raise ValueError("downloaded TLC SHA-256 differs from pinned artifact")
            temporary.replace(jar)
        finally:
            temporary.unlink(missing_ok=True)
    if digest(jar) != toolchain["sha256"]:
        raise ValueError(f"TLC SHA-256 mismatch: {jar}")
    sources = [MODELS / (model + ".tla") for model in sorted({case["model"] for case in campaign["cases"]})]
    sources += [MODELS / "campaign.json", MODELS / "toolchain.json", Path(__file__).resolve()]
    report["source_sha256"] = {str(path.relative_to(ROOT)): digest(path) for path in sources}
    for source in sources:
        if source.suffix == ".tla":
            copied = stage / source.name
            shutil.copyfile(source, copied)
            if digest(copied) != report["source_sha256"][str(source.relative_to(ROOT))]:
                raise ValueError("model changed while staging: " + source.name)
    report["module_resolution"] = "fresh temporary cwd containing only declared .tla source copies and generated configurations; standard modules from pinned jar"
    report["java_version"] = subprocess.run([args.java, "-version"], cwd=stage, env=java_environment,
                                            capture_output=True, text=True, check=True).stderr.strip()
    report["resource_limits"] = {"timeout_seconds_per_case": args.timeout, "heap": "512m", "workers": 1,
                                 "depth_limit": None, "state_constraint": None, "symmetry": None,
                                 "fingerprint_index": 0, "random_seed": 1}
    for case in selected:
        case_dir = output_dir / case["name"]
        case_dir.mkdir(parents=True, exist_ok=True)
        config = "CONSTANTS\n" + "\n".join(f"  {key} = {tla_value(value)}" for key, value in case["constants"].items())
        invariants = ["TypeOK", case["safety_property"]]
        if case["expected"] == "witness":
            invariants.append(case["failure_property"])
        config += ("\nSPECIFICATION Spec\n" if case.get("use_spec") else "\nINIT Init\nNEXT Next\n")
        config += "INVARIANTS " + " ".join(invariants) + "\n"
        if case.get("temporal_property"):
            config += "PROPERTY " + case["temporal_property"] + "\n"
        config_path = case_dir / "model.cfg"
        config_path.write_text(config)
        staged_config = stage / (case["name"] + ".cfg")
        staged_config.write_text(config)
        command = [args.java, "-XX:+UseParallelGC", "-Xmx512m", "-cp", str(jar), "tlc2.TLC",
                   "-workers", "1", "-fp", "0", "-seed", "1", "-cleanup", "-difftrace",
                   "-noGenerateSpecTE", "-metadir", str(case_dir / "states"),
                   "-config", str(staged_config), str(stage / (case["model"] + ".tla"))]
        started = time.monotonic()
        try:
            result = subprocess.run(command, cwd=stage, env=java_environment,
                                    capture_output=True, text=True, timeout=args.timeout)
            output = result.stdout + result.stderr
            returncode = result.returncode
        except subprocess.TimeoutExpired as error:
            stdout = error.stdout or b""
            stderr = error.stderr or b""
            output = (stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout)
            output += (stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr)
            output += "\nRUNNER TIMEOUT: finite campaign did not complete.\n"
            returncode = None
        passed, outcome = classify(output, returncode, case)
        if toolchain["reported_version"] not in output:
            passed, outcome = False, "unexpected_tool_version"
        (case_dir / "tlc.log").write_text(output)
        # Expected counterexamples stop TLC early, leaving binary search-state
        # caches. Text traces/configuration are sufficient retained evidence.
        shutil.rmtree(case_dir / "states", ignore_errors=True)
        stats = re.search(r"([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue", output)
        counts = None if not stats else {key: int(value.replace(",", "")) for key, value in zip(
            ["generated", "distinct", "queued"], stats.groups())}
        report["results"].append({**case, "passed": passed, "outcome": outcome, "exit_code": returncode,
                                  "elapsed_seconds": time.monotonic() - started, "states": counts,
                                  "command": command, "config_sha256": digest(config_path),
                                  "log": str((case_dir / "tlc.log").relative_to(output_dir))})
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(f"{'PASS' if passed else 'FAIL'} {case['name']}: {outcome}", flush=True)
    report["passed"] = all(result["passed"] for result in report["results"])
    if any(digest(ROOT / path) != expected for path, expected in report["source_sha256"].items()):
        report["passed"] = False
        report["error"] = "verification inputs changed during the campaign"
    report["campaign_finished"] = True
    report["completed"] = report["passed"]
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Evidence: {report_path}")
    return 0 if report["passed"] else 1


def run(args: argparse.Namespace) -> int:
    # Repository-root .tla files, generated Java overrides and class files
    # cannot shadow standard modules in this fresh allowlisted search path.
    with tempfile.TemporaryDirectory(prefix="aerostore-tlc-models-") as temporary:
        return run_in_stage(args, Path(temporary))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, default=os.environ.get("TLC_JAR"))
    parser.add_argument("--java", default="java")
    parser.add_argument("--download", action="store_true", help="download pinned TLC only if missing")
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/tla")
    parser.add_argument("--case", action="append", help="run named case; repeat to select several")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        return run(args)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"TLC runner failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
