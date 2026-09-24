#!/usr/bin/env python3
"""Shared native lifecycle/capture/publication scenario and semantic negative controls."""
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
ROOTS = [
    "CaptureView::header_offset", "CaptureView::transactional_stamp", "capture_one_dependency",
    "Bridge::find_binding", "Bridge::transactional_key_bucket", "Bridge::transactional_stamp",
    "Bridge::reserve_stamp", "Bridge::transactional_publish_stamp", "change_keys_ignore_clock",
    "read_keys_ignore_clock", "cleared_writer_is_absent", "registered_reader_then_writer_publication",
    "creation_and_move_have_live_witnesses",
]
MUTATIONS = [
    ("skip_native_capture", "capture_one_dependency",
     "capture::capture_dependencies(&view, &mut captured, &buckets)", "Ok::<(), capture::Error>(())"),
    ("translate_wrong_stamp", "capture_one_dependency",
     "bucket: read.bucket, stamp: read.stamp })", "bucket: read.bucket, stamp: 0 })"),
    ("translate_wrong_bucket", "capture_one_dependency",
     "bucket: read.bucket, stamp: read.stamp })", "bucket: 0, stamp: read.stamp })"),
    ("translate_wrong_index", "capture_one_dependency",
     "index_offset: read.index_offset, bucket: read.bucket", "index_offset: 0, bucket: read.bucket"),
    ("capture_wrong_source_bucket", "CaptureView::transactional_stamp",
     "self.index.load_stamp(self.binding, bucket)", "self.index.load_stamp(self.binding, 0)"),
    ("stale_bridge_reservation", "Bridge::reserve_stamp",
     "let stamp = lifecycle::reserve_publication_clock(&mut self.lifecycle);",
     "let stamp = lifecycle::reserve_publication_clock(&mut self.lifecycle).saturating_sub(1);"),
    ("separate_clock_projection", "Bridge::reserve_stamp",
     "clock: self.lifecycle.state().clock,", "clock: self.reserved_stamp,"),
    ("omit_history_projection", "Bridge::reserve_stamp",
     "reservations: self.lifecycle.state().reservations,", "reservations: Seq::empty(),"),
    ("store_wrong_stamp", "Bridge::transactional_publish_stamp",
     "self.index.store_stamp(binding, bucket, stamp)", "self.index.store_stamp(binding, bucket, 0)"),
    ("skip_native_snapshot", "registered_reader_then_writer_publication",
     """let snapshot_result = lifecycle::create_transaction_snapshot(&mut driver.lifecycle, reader);
    driver.lifecycle.release_lifecycle();""",
     """let snapshot_result = Ok::<lifecycle::ProcSnapshot, lifecycle::ProcArrayError>(
        lifecycle::ProcSnapshot { xmin: 0, xmax: 0, in_flight: Vec::new(), in_flight_len: 0 });"""),
    ("skip_writer_deregistration", "registered_reader_then_writer_publication",
     """let finish = lifecycle::end_transaction(&mut driver.lifecycle, writer);
    driver.lifecycle.release_lifecycle();""", "let finish = Ok::<(), lifecycle::ProcArrayError>(());"),
    ("skip_native_publication", "registered_reader_then_writer_publication",
     "predicate::publish_index_stamps::<Bridge<L, I>, C>(driver, changes)", "Ok::<(), predicate::Error>(())"),
    ("skip_native_validation", "registered_reader_then_writer_publication",
     """let conflict = match predicate::index_read_conflict(driver, &tx) {
        Ok(conflict) => conflict,
        Err(_) => return Err(ScenarioError::Validation),
    };""", "let conflict = false;"),
]


def mutate(template, name, root, old, new):
    if template.count(old) != 1:
        raise ValueError("mutation anchor absent or ambiguous: " + name)
    return template.replace(old, new, 1)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files = [ROOT / "aerostore_core/src/occ_partitioned.rs", ROOT / "aerostore_core/src/procarray.rs",
             ROOT / "aerostore_core/src/shm.rs", ROOT / "aerostore_verified/src/lib.rs",
             generate.TEMPLATE, generate.OUTPUT, Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
             ROOT / "verification/concurrent/generate.py", PIN]
    for name in generate.COMPONENTS:
        module = generate.component(name)
        files += [module.CONTRACTS, module.OUTPUT, Path(module.__file__)]
    return files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lifecycle-scenario")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_shared_history_capture_publication_scenario",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "whole_lookup_refinement_proved": False, "transaction_history_refinement_proved": False,
        "acquisition_interference_refinement_proved": False, "native_overflow_handling_proved": False,
        "native_guard_lifetime_refinement_proved": False}
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
        source = generate.TEMPLATE.read_text()
        if generate.OUTPUT.read_text() != generate.render(source):
            raise RuntimeError("stale generated shared-history scenario")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_lifecycle_scenario", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "60"]

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
                raise RuntimeError("shared-history scenario failed: " + name)

        invoke("native_lifecycle_scenario", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root.replace("::", "_"), generate.OUTPUT, root)
        for mutation in MUTATIONS:
            name, root, _, _ = mutation
            artifact = output / (name + ".rs")
            artifact.write_text(generate.render(mutate(source, *mutation)))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("shared-history scenario inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
