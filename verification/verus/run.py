#!/usr/bin/env python3
"""Verify the production kernels and require meaningful negative controls."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time

import generate

ROOT = generate.ROOT
HERE = Path(__file__).resolve().parent
PIN = HERE / "toolchain.json"
PROOF_ROOTS = ["canonical_sequence_unique", "first_invalid_unique", "canonical_result_unique"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/verus")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("verification outputs must be under target/ so they cannot replace proof inputs")
    output.mkdir(parents=True, exist_ok=True)
    receipt_path = output / "receipt.json"
    receipt = {"schema": 1, "status": "running", "passed": False, "checks": []}
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    try:
        pins = json.loads(PIN.read_text())
        distribution = ROOT / pins["distribution"]
        verifier = distribution / "verus"
        for relative, expected in pins["artifact_sha256"].items():
            if sha256(distribution / relative) != expected:
                raise RuntimeError("pinned verifier artifact differs: " + relative)
        inputs = [generate.SOURCE, generate.SPECS, Path(generate.__file__).resolve(), Path(__file__).resolve(), PIN]
        hashes = {str(path.relative_to(ROOT)): sha256(path) for path in inputs}
        receipt["input_sha256"] = hashes
        receipt["toolchain"] = pins
        production_roots = [{"name": signature.split("(")[0].split()[-1],
                             "signature": signature, "contract": contract,
                             "scope": "production_function_body"}
                            for signature, contract in generate.INTERFACES.items()]
        if [root["name"] for root in production_roots] != [
                "canonical_buckets_sort", "canonical_buckets_bitmap", "stamp_precedes_snapshot"]:
            raise RuntimeError("the required production root set changed")
        receipt["required_roots"] = production_roots + [
            {"name": name, "scope": "contract_uniqueness_theorem", "specification": "verification/verus/spec.rs"}
            for name in PROOF_ROOTS]
        source = generate.SOURCE.read_text()
        generated = generate.render(source)
        if not generate.OUTPUT.exists() or generate.OUTPUT.read_text() != generated:
            raise RuntimeError("stale generated proof; run python3 verification/verus/generate.py")
        environment = dict(os.environ)
        environment["RUSTUP_HOME"] = str(ROOT / pins["rustup_home"])
        environment["RUSTUP_TOOLCHAIN"] = pins["rust_toolchain"]
        environment["VERUS_Z3_PATH"] = str(distribution / "z3")
        base = [str(verifier), "--crate-type=lib", "--crate-name", "aerostore_verified",
                "--edition=2021", "--target", pins["platform"],
                "--no-cheating", "--triggers-mode", "silent", "--rlimit", "20"]

        def invoke(name: str, path: Path, expected_failure: str | None = None,
                   required_root: str | None = None) -> dict:
            command = base + (["--verify-root", "--verify-function", required_root] if required_root else []) + [str(path)]
            started = time.monotonic()
            process = subprocess.Popen(command, cwd=ROOT, env=environment, text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                log = process.communicate(timeout=120)[0]
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                log = process.communicate()[0]
                (output / (name + ".log")).write_text(log + "\nVerifier exceeded the 120-second wall-clock limit.\n")
                raise
            (output / (name + ".log")).write_text(log)
            match = re.search(r"verification results:: (\d+) verified, (\d+) errors", log)
            check = {"name": name, "command": command, "exit_code": process.returncode,
                     "elapsed_seconds": time.monotonic() - started,
                     "verified": int(match[1]) if match else None,
                     "errors": int(match[2]) if match else None,
                     "source_sha256": sha256(path), "expected_failure": expected_failure,
                     "required_root": required_root}
            receipt["checks"].append(check)
            if expected_failure is None:
                minimum = 1 if required_root else len(receipt["required_roots"])
                if process.returncode != 0 or not match or int(match[1]) < minimum or int(match[2]) != 0:
                    raise RuntimeError("production proof failed; see " + name + ".log")
            elif process.returncode == 0 or not match or int(match[2]) == 0 or expected_failure not in log:
                raise RuntimeError("negative control failed for the wrong reason: " + name)
            return check

        invoke("production", generate.OUTPUT)
        # Full verification above discharges every called proof lemma. These
        # additional named checks prove each required root really exists and
        # verifies; a nonempty aggregate count cannot substitute for a root.
        for root in receipt["required_roots"]:
            check = invoke("root_" + root["name"], generate.OUTPUT, required_root=root["name"])
            root["verified"] = True
            root["verified_obligations"] = check["verified"]
        negative_controls = [
            ("omitted_bucket", "output.push(bucket);", "if bucket != 0 { output.push(bucket); }", "invariant not satisfied"),
            ("equal_stamp", "stamp < transaction_id", "stamp <= transaction_id", "postcondition not satisfied"),
        ]
        for name, original, replacement, diagnostic in negative_controls:
            # The bitmap output statement is the final occurrence. The earlier
            # insertion-sort push remains unchanged in the omission mutant.
            if name == "omitted_bucket":
                start = source.index("pub fn canonical_buckets_bitmap")
                if source[start:].count(original) != 1:
                    raise RuntimeError("omission mutation no longer identifies one bitmap output")
                mutated = source[:start] + source[start:].replace(original, replacement, 1)
            else:
                if source.count(original) != 1:
                    raise RuntimeError("stamp mutation no longer identifies one scalar comparison")
                mutated = source.replace(original, replacement, 1)
            path = output / (name + ".verus.rs")
            path.write_text(generate.render(mutated))
            invoke(name, path, diagnostic)
        for path in inputs:
            if hashes[str(path.relative_to(ROOT))] != sha256(path):
                raise RuntimeError("proof inputs changed during verification: " + str(path))
        receipt.update(status="passed", passed=True)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "receipt": str(receipt_path),
                      "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
