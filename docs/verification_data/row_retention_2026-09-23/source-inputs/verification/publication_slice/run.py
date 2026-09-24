#!/usr/bin/env python3
"""Verify checked joins of native guard, lifecycle, and predicate contracts."""
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
ROOTS = ["registry_agreement_is_injective", "read_key_binding_valid", "native_keys_are_valid",
         "acquire_required_guards", "guard_coverage_supplies_validation_permission",
         "lifecycle_publication_invalidates_read", "validate_after_lifecycle_publication"]
MUTATIONS = [
    ("omit_native_key_generation", "acquire_required_guards", "predicate::index_lock_keys::<P, C>(driver, tx, changes)", "Ok::<Vec<(usize, usize)>, predicate::Error>(Vec::new())"),
    ("omit_native_guard_acquisition", "acquire_required_guards", "guards::acquire_index_locks(locks, &keys)", "Ok::<Vec<guards::Guard>, guards::Error>(Vec::new())"),
    ("discard_acquired_guards", "acquire_required_guards", "Ok(Locked { keys, guards: acquired })", "Ok(Locked { keys, guards: Vec::new() })"),
    ("ignore_native_validation", "validate_after_lifecycle_publication", "predicate::index_read_conflict(driver, tx)", "Ok(false)"),
    ("use_wrong_dependency", "lifecycle_publication_invalidates_read", "reads, reader, touched, stamp, read_index);", "reads, reader, touched, stamp, 0);"),
    ("use_wrong_reader_history", "lifecycle_publication_invalidates_read", "lifecycle::reservation_after_reader(clock_before, clock_after, stamp, reader);", "lifecycle::reservation_after_reader(clock_before, clock_after, stamp, 0);"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files = [generate.TEMPLATE, generate.OUTPUT, Path(generate.__file__), Path(__file__), HERE / "test_generate.py", PIN]
    for name in generate.COMPONENTS:
        module = generate.component(name)
        files += [module.SOURCE, module.CONTRACTS, module.OUTPUT, Path(module.__file__)]
        files += [getattr(module, attribute) for attribute in ("PUBLICATION", "SHM", "HELPER", "INDEX")
                  if hasattr(module, attribute)]
    files += [ROOT / "aerostore_core/src/occ_partitioned.rs", ROOT / "aerostore_core/src/shm_index.rs",
              ROOT / "aerostore_core/src/shm_lock.rs", ROOT / "aerostore_verified/src/lib.rs",
              ROOT / "verification/concurrent/generate.py"]
    return sorted(set(files))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/publication-slice")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_lifecycle_guard_predicate_join",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_concurrent_history_refinement_proved": False, "whole_lookup_refinement_proved": False}
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
            raise RuntimeError("stale generated publication slice")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_publication_slice", "--crate-type=lib",
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
                raise RuntimeError("publication slice proof failed: " + name)

        invoke("native_publication_slice", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            if source.count(old) != 1:
                raise RuntimeError("mutation anchor absent or ambiguous: " + name)
            artifact = output / (name + ".rs")
            artifact.write_text(source.replace(old, new))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("publication slice inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
