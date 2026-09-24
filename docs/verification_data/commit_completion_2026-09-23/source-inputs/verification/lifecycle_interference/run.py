#!/usr/bin/env python3
"""Fresh source-bound native lifecycle proofs and semantic negative controls."""
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
    "wait_step_preserves", "wait_step_preserves_owner", "extends_transitive",
    "trace_prefix_invariant", "acquisition_preserves_owned_history",
    "begin_transaction_acquired", "end_transaction_acquired", "create_transaction_snapshot_acquired",
    "snapshot_locked_acquired", "begin_after_wait", "snapshot_after_wait", "end_after_wait",
    "native_begin_is_register_event", "native_end_is_end_event", "native_snapshot_is_horizon_event",
    "history_reader_survives_wait", "interfering_slot_changes_have_witness",
]
# kind distinguishes a native source edit from a checked-composition edit.
MUTATIONS = [
    ("allow_owned_slot_interference", "wait_step_preserves_owner", "template", "slot != r.slot_idx", "true"),
    ("rollback_registration_history", "wait_step_preserves", "template", "reservations: a.reservations.push(txid), ..a", "reservations: Seq::empty(), ..a"),
    ("reuse_nonfresh_registration", "wait_step_preserves", "template", "a.clock <= txid < u64::MAX", "0 < txid < u64::MAX"),
    ("overwrite_horizon_owner", "wait_step_preserves", "template", "Slot { snapshot_xmin: xmin, ..a.slots[slot as int] }", "Slot { txid: xmin, snapshot_xmin: xmin }"),
    ("return_preacquisition_snapshot_view", "snapshot_after_wait", "wrapper", "(result, Ghost(acquired))", "(result, Ghost(entry))"),
    ("return_preacquisition_end_view", "end_after_wait", "wrapper", "(result, Ghost(acquired))", "(result, Ghost(entry))"),
    ("omit_snapshot_acquisition", "snapshot_after_wait", "wrapper", "let trace = driver.acquire(Tracked(Some(owner)));", "let trace = Ghost(WaitTrace { states: seq![driver.state()], events: Seq::empty() });"),
    ("mint_wrong_owner_identity", "begin_after_wait", "wrapper", "arena: driver.identity()", "arena: 0"),
    ("mint_wrong_registration", "begin_after_wait", "wrapper", "OwnerToken { registration,", "OwnerToken { registration: ProcArrayRegistration { slot_idx: registration.slot_idx, txid: 0 },"),
    ("end_wrong_registration", "end_after_wait", "wrapper", "end_transaction_acquired(driver, registration)", "end_transaction_acquired(driver, ProcArrayRegistration { slot_idx: registration.slot_idx, txid: 0 })"),
    ("clear_wrong_native_owner", "end_transaction_acquired", "native", "slot.txid.store(EMPTY_SLOT, Ordering::Release);", "slot.txid.store(registration.txid, Ordering::Release);"),
    ("omit_native_horizon", "create_transaction_snapshot_acquired", "native", "slot.snapshot_xmin.store(snapshot.xmin, Ordering::Release);", ""),
    ("omit_native_snapshot_member", "snapshot_locked_acquired", "native", "in_flight[in_flight_len as usize].write(txid);", ""),
]


def mutation_source(source, template, item):
    name, proof_root, kind, old, new = item
    if kind == "native":
        native = proof_root.removesuffix("_acquired")
        marker = "    " + ("fn " if native == "snapshot_locked" else "pub fn ") + native + "("
        begin = source.index(marker)
        end = source.index(generate.lifecycle.ENDS[native], begin)
        selected = source[begin:end]
        if selected.count(old) != 1:
            raise ValueError("native mutation anchor absent or ambiguous: " + name)
        return generate.render(source[:begin] + selected.replace(old, new, 1) + source[end:], template)
    if kind == "wrapper":
        begin = template.index("pub fn " + proof_root + "<")
        end = template.find("\npub ", begin + 1)
        if end < 0:
            end = len(template)
        selected = template[begin:end]
        if selected.count(old) != 1:
            raise ValueError("wrapper mutation anchor absent or ambiguous: " + name)
        template = template[:begin] + selected.replace(old, new, 1) + template[end:]
    else:
        if template.count(old) != 1:
            raise ValueError("template mutation anchor absent or ambiguous: " + name)
        template = template.replace(old, new, 1)
    return generate.render(source, template)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.lifecycle.PUBLICATION, generate.lifecycle.SHM,
            generate.lifecycle.CONTRACTS, generate.lifecycle.OUTPUT, Path(generate.lifecycle.__file__),
            generate.TEMPLATE, generate.OUTPUT, Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
            ROOT / "verification/concurrent/generate.py", ROOT / "verification/predicate/generate.py", PIN]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lifecycle-interference")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_lifecycle_acquisition_interference",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_atomic_mutex_refinement_proved": False,
        "native_interference_ownership_refinement_proved": False,
        "transaction_history_refinement_proved": False, "clock_exhaustion_handling_proved": False}
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
        source = generate.SOURCE.read_text()
        template = generate.TEMPLATE.read_text()
        if generate.OUTPUT.read_text() != generate.render(source, template):
            raise RuntimeError("stale generated lifecycle operations")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_lifecycle_interference", "--crate-type=lib",
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
                raise RuntimeError("lifecycle proof failed: " + name)

        invoke("native_lifecycle_interference", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        for mutation in MUTATIONS:
            name, proof_root = mutation[:2]
            artifact = output / (name + ".rs")
            artifact.write_text(mutation_source(source, template, mutation))
            invoke(name, artifact, proof_root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("lifecycle proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
