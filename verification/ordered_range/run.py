#!/usr/bin/env python3
"""Source-bound finite native coverage, not a universal range-mapping proof."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import check_p0_contracts
import verify_formal

SOURCE = ROOT / "aerostore_core/src/shm_index.rs"
TEST = "shm_index::tests::ordered_publication_dependency_coverage"
FEATURES = [None, "verified-buckets-sort", "verified-buckets-bitmap"]
PINNED_RUST = "01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_digest(source):
    code = check_p0_contracts.mask_noncode(source)
    starts = list(re.finditer(r"\bfn\s+ordered_publication_dependency_coverage\s*\(\s*\)\s*\{", code))
    if len(starts) != 1:
        raise RuntimeError("missing or ambiguous ordered range coverage test")
    start = starts[0].start()
    end = check_p0_contracts.closing(code, code.index("{", start))
    return hashlib.sha256(source[start:end + 1].encode()).hexdigest()


def output_passes(returncode, output):
    summaries = re.findall(r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured;", output, re.MULTILINE)
    selected = re.findall(r"^test ([\w:]+) \.\.\. (\w+)\s*$", output, re.MULTILINE)
    return returncode == 0 and summaries == [("ok", "1", "0", "0", "0")] and selected == [(TEST, "ok")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/ordered-range")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    path = output / "receipt.json"
    receipt = {"schema": 1, "scope": "finite_native_ordered_range_dependency_coverage",
               "passed": False, "completed": False, "source_stable": False,
               "universal_mapping_coverage_proved": False, "whole_engine_verified": False,
               "required_test": TEST, "required_features": [f or "default" for f in FEATURES], "checks": []}

    def save():
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(path)

    save()
    try:
        verify_formal.check_build_environment()
        before = verify_formal.source_fingerprint(ROOT)
        receipt["input_sha256"] = before
        receipt["test_source_sha256"] = test_digest(SOURCE.read_text())
        env = dict(os.environ)
        env.update(RUSTUP_HOME=str(ROOT / "target/verification-tools/production-rustup"),
                   CARGO_HOME=str(ROOT / "target/verification-tools/production-cargo"),
                   RUSTUP_TOOLCHAIN="1.93.1", RUSTUP_NO_UPDATE_CHECK="1")
        cargo = Path(subprocess.check_output(["rustup", "which", "cargo"], env=env, text=True).strip())
        rustc = cargo.with_name("rustc")
        version = subprocess.check_output([str(rustc), "--version", "--verbose"], env=env, text=True)
        if "commit-hash: " + PINNED_RUST not in version:
            raise RuntimeError("native compiler differs from the reviewed toolchain")
        receipt["rustc"] = version
        receipt["tool_sha256"] = {str(cargo): digest(cargo), str(rustc): digest(rustc)}
        env.update(RUSTC=str(rustc), CARGO_ENCODED_RUSTFLAGS="")
        for feature in FEATURES:
            name = feature or "default"
            command = [str(cargo), "test", "--release", "--offline", "--locked",
                       "-p", "aerostore_core", "--no-default-features"]
            if feature:
                command += ["--features", feature]
            command += ["--lib", TEST, "--", "--exact", "--test-threads=1", "--nocapture"]
            start = time.monotonic()
            result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=300)
            log = output / (name + ".log")
            log.write_text(result.stdout + result.stderr)
            passed = output_passes(result.returncode, log.read_text())
            receipt["checks"].append({"name": name, "command": command, "exit_code": result.returncode,
                                      "elapsed_seconds": time.monotonic() - start,
                                      "passed": passed, "log": str(log.relative_to(ROOT)), "log_sha256": digest(log)})
            save()
            if not passed:
                raise RuntimeError("native coverage did not run exactly one passing nonignored test: " + name)
        receipt["final_input_sha256"] = verify_formal.source_fingerprint(ROOT)
        receipt["source_stable"] = before == receipt["final_input_sha256"]
        if not receipt["source_stable"]:
            raise RuntimeError("source changed during native coverage checks")
        receipt.update(completed=True, passed=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt["error"] = str(error)
    save()
    print(json.dumps({k: v for k, v in receipt.items() if k not in {"input_sha256", "final_input_sha256"}}, indent=2))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
