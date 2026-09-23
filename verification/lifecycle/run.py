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
ROOTS = ['checked_slot',
 'reservation_preserves_clock_history',
 'observation_preserves_state',
 'snapshot_scan_bounds',
 'snapshot_minimum_covers_slot',
 'snapshot_fields_unchanged',
 'reservation_after_reader',
 'active_slot_membership',
 'snapshot_covers_active_writer',
 'retention_covers_active_snapshot',
 'reservation_history_strict_order',
 'lifecycle_contract_has_live_and_stale_witnesses',
 'begin_transaction',
 'end_transaction',
 'snapshot_locked',
 'create_snapshot',
 'create_transaction_snapshot',
 'oldest_snapshot_xmin',
 'reserve_publication_clock',
 'deregister_then_reserve']
MUTATIONS = [('omit_begin_lock', 'begin_transaction', 'let _lifecycle = self.lifecycle.lock();', ''),
 ('omit_end_lock', 'end_transaction', 'let _lifecycle = self.lifecycle.lock();', ''),
 ('omit_snapshot_lock', 'create_snapshot', 'let _lifecycle = self.lifecycle.lock();', ''),
 ('omit_transaction_snapshot_lock', 'create_transaction_snapshot', 'let _lifecycle = self.lifecycle.lock();', ''),
 ('omit_retention_lock', 'oldest_snapshot_xmin', 'let _lifecycle = self.lifecycle.lock();', ''),
 ('skip_registration',
  'begin_transaction',
  'if slot\n                .txid',
  'if false && slot\n                .txid'),
 ('omit_initial_horizon', 'begin_transaction', 'slot.snapshot_xmin.store(txid, Ordering::Release);', ''),
 ('wrong_slot_clear',
  'end_transaction',
  'slot.txid.store(EMPTY_SLOT, Ordering::Release);',
  'slot.txid.store(registration.txid, Ordering::Release);'),
 ('omit_retention_clear', 'end_transaction', 'slot.snapshot_xmin.store(EMPTY_SLOT, Ordering::Release);', ''),
 ('bypass_slot_ownership', 'end_transaction', 'if observed != registration.txid {', 'if false {'),
 ('omit_snapshot_horizon',
  'create_transaction_snapshot',
  'slot.snapshot_xmin.store(snapshot.xmin, Ordering::Release);',
  ''),
 ('snapshot_reads_retention',
  'snapshot_locked',
  'slot.load(Ordering::Relaxed)',
  'slot.snapshot_xmin.load(Ordering::Relaxed)'),
 ('omit_active_txid_copy', 'snapshot_locked', 'in_flight[in_flight_len as usize].write(txid);', ''),
 ('wrong_snapshot_minimum', 'snapshot_locked', 'xmin = xmin.min(txid);', 'xmin = xmin.max(txid);'),
 ('omit_xmax_compensation', 'snapshot_locked', 'xmax = xmax.max(max_in_flight.saturating_add(1));', ''),
 ('retention_uses_active_id',
  'oldest_snapshot_xmin',
  'slot.snapshot_xmin.load(Ordering::Acquire)',
  'slot.load(Ordering::Acquire)'),
 ('stale_publication_reservation',
  'reserve_publication_clock',
  'self.shm.global_txid().fetch_add(1, Ordering::AcqRel)',
  'self.shm.global_txid().fetch_add(1, Ordering::AcqRel).saturating_sub(1)')]

def mutate(source, method, old, new):
    native = "publish_index_stamps" if method == "reserve_publication_clock" else method
    marker = "    pub fn " + native + "("
    if marker not in source:
        marker = "    fn " + native + "("
    begin = source.index(marker)
    endings = [position for key in ("\n    pub fn ", "\n    fn ", "\n    pub(crate) fn ")
        if (position := source.find(key, begin + len(marker))) >= 0]
    end = min(endings) if endings else len(source)
    selected = source[begin:end]
    if selected.count(old) != 1:
        raise RuntimeError("mutation anchor absent or ambiguous: " + method + ": " + old)
    return source[:begin] + selected.replace(old, new, 1) + source[end:]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.PUBLICATION, generate.SHM, generate.CONTRACTS, generate.OUTPUT,
            Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
            ROOT / "verification/concurrent/generate.py", ROOT / "verification/predicate/generate.py", PIN]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lifecycle")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_lifecycle_snapshot_shared_clock",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_atomic_mutex_refinement_proved": False,
        "acquisition_interference_refinement_proved": False,
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
        publication, shm = generate.PUBLICATION.read_text(), generate.SHM.read_text()
        if generate.OUTPUT.read_text() != generate.render(source, publication, shm):
            raise RuntimeError("stale generated lifecycle operations")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_lifecycle", "--crate-type=lib",
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
                    or not re.search(r"(?:precondition|postcondition|invariant) not satisfied|assertion failed", log)):
                    raise RuntimeError("negative control failed for wrong reason: " + name)
            elif process.returncode != 0 or not summary or int(summary[2]) != 0 or int(summary[1]) < (1 if root else len(ROOTS)):
                raise RuntimeError("lifecycle proof failed: " + name)

        invoke("native_lifecycle", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        for name, proof_root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            if proof_root == "reserve_publication_clock":
                generated = generate.render(source, mutate(publication, proof_root, old, new), shm)
            else:
                generated = generate.render(mutate(source, proof_root, old, new), publication, shm)
            artifact.write_text(generated)
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
