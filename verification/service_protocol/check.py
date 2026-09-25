#!/usr/bin/env python3
"""Run the small service protocol campaign through the existing strict TLC runner.

Rust source hashes identify the associated prototype; they do not establish
refinement. The unchanged existing verification campaign remains separate.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/service-prototype/tla")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    report_path = args.output / "report.json"
    report_path.write_text(json.dumps({"passed": False, "completed": False, "status": "starting"}) + "\n")
    sources = [Path(__file__).resolve(), ROOT / "aerostore_core/benches/contention_crucible/service.rs",
               ROOT / "aerostore_core/tests/contention_service.rs"]
    fingerprints = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    spec = importlib.util.spec_from_file_location("service_tlc_runner", ROOT / "scripts/check_tla.py")
    runner = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(runner)
        runner.MODELS = Path(__file__).resolve().parent
        status = runner.run(argparse.Namespace(output=args.output, timeout=args.timeout, java=args.java,
                                               jar=None, download=False, case=None, list=False))
        report = json.loads(report_path.read_text())
        report["associated_prototype_sha256"] = fingerprints
        report["implementation_refinement_proved"] = False
        report["bounds"] = "two one-shot clients, one killed and one surviving; abstract atomic engine operations; accepted commits assumed successful (native conflict/fatal/indeterminate results excluded); weak fairness"
        if any(hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest for path, digest in fingerprints.items()):
            status = 1
            report.update(passed=False, completed=False, error="associated source changed during campaign")
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        return status
    except Exception as error:
        report = json.loads(report_path.read_text())
        report.update(passed=False, completed=False, error=str(error))
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
