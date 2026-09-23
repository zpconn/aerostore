#!/usr/bin/env python3
"""Verify cross-module native capture/validation composition and semantic controls."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import time

import generate

ROOT = generate.ROOT
HERE = Path(__file__).resolve().parent
PIN = ROOT / "verification/verus/toolchain.json"
ROOTS = ["read_keys_are_held", "capture_preserves_guard_coverage",
         "captured_read_survives_translation", "publication_supplies_late_bucket", "capture_then_validate"]
MUTATIONS = [
    ("translate_wrong_stamp", "stamp: read.stamp });", "stamp: 0 });"),
    ("translate_wrong_bucket", "bucket: read.bucket, stamp: read.stamp });", "bucket: 0, stamp: read.stamp });"),
    ("translate_wrong_index", "index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp });",
     "index_offset: 0, bucket: read.bucket, stamp: read.stamp });"),
    ("accept_native_conflict", "ReadResult::Checked(conflict)", "ReadResult::Checked(false)"),
    ("skip_native_capture", "capture::capture_dependencies(index, tx, buckets)", "Ok::<(), capture::Error>(())"),
    ("skip_native_validation", """let conflict = match predicate::index_read_conflict(later, &validated) {
        Err(_) => return ReadResult::ValidationError,
        Ok(conflict) => conflict,
    };""", "let conflict = false;"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files = [ROOT / "aerostore_core/src/occ_partitioned.rs", ROOT / "aerostore_verified/src/lib.rs",
             generate.TEMPLATE, generate.OUTPUT, Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
             ROOT / "verification/concurrent/generate.py", PIN]
    for name in ("predicate_capture", "predicate"):
        files += [ROOT / "verification" / name / filename for filename in ("contracts.rs", "generate.py")]
        files += [ROOT / "verification" / name / ("capture.verus.rs" if name == "predicate_capture" else "predicate.verus.rs")]
    return files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/predicate-composition")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_capture_validation_composition",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "whole_lookup_refinement_proved": False, "transaction_history_refinement_proved": False}
    path = output / "receipt.json"

    def save():
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(path)

    save()
    try:
        pin = json.loads(PIN.read_text())
        distribution = ROOT / pin["distribution"]
        for name, expected in pin["artifact_sha256"].items():
            if digest(distribution / name) != expected:
                raise RuntimeError("verifier artifact differs: " + name)
        receipt["toolchain"] = pin
        fingerprints = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        receipt["input_sha256"] = fingerprints
        if generate.OUTPUT.read_text() != generate.render():
            raise RuntimeError("stale generated composition")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_predicate_composition", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "50"]

        def invoke(name, artifact, root=None, negative=False):
            command = base + (["--verify-root", "--verify-function", root] if root else []) + [str(artifact)]
            started = time.monotonic()
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, start_new_session=True)
            try:
                log = process.communicate(timeout=120)[0]
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                log = process.communicate()[0]
                (output / (name + ".log")).write_text(log)
                raise
            log_path = output / (name + ".log")
            log_path.write_text(log)
            summary = re.search(r"verification results:: (\d+) verified, (\d+) errors", log)
            check = {"name": name, "command": command, "exit_code": process.returncode,
                "elapsed_seconds": time.monotonic() - started, "source_sha256": digest(artifact),
                "log": str(log_path.relative_to(ROOT)), "log_sha256": digest(log_path),
                "expected_failure": negative, "required_root": root,
                "verified": int(summary[1]) if summary else None, "errors": int(summary[2]) if summary else None}
            receipt["checks"].append(check)
            save()
            if negative:
                if (process.returncode == 0 or not summary or int(summary[2]) == 0
                    or not re.search(r"(?:precondition|postcondition|invariant|assertion) not satisfied|assertion failed", log)):
                    raise RuntimeError("negative control failed for wrong reason: " + name)
            elif process.returncode != 0 or not summary or int(summary[2]) != 0 or int(summary[1]) < (1 if root else len(ROOTS)):
                raise RuntimeError("composition proof failed: " + name)

        invoke("native_composition", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, old, new in MUTATIONS:
            if source.count(old) != 1:
                raise RuntimeError("mutation anchor absent or ambiguous: " + name)
            artifact = output / (name + ".rs")
            artifact.write_text(source.replace(old, new))
            invoke(name, artifact, "capture_then_validate", True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("composition proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
