#!/usr/bin/env python3
"""Fresh source-bound native posting proofs and semantic negative controls."""
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
    'rollback_index_destinations',
    'prepare_index_destinations',
    'remove_index_sources',
    'destination_member',
    'source_member',
    'destination_prefix_subset',
    'restoration_frame',
    'destination_new',
    'destination_not_prior',
    'recorded_push',
    'prefix_record_push',
    'prefix_record_skip',
    'recorded_member',
    'recorded_suffix',
]
# Each native mutation is scoped to one actual helper before lowering.
MUTATIONS = [
    ("omit_prepare_rollback", "prepare_index_destinations", "self.rollback_index_destinations(changes, &inserted)", "Ok::<(), Error>(())"),
    ("omit_rollback_poison", "rollback_index_destinations", "self.poison_indexes();", ""),
    ("omit_source_poison", "remove_index_sources", "self.poison_indexes();", ""),
    ("wrong_rollback_row", "rollback_index_destinations", "&previous.row_id", "&0"),
    ("wrong_rollback_key", "rollback_index_destinations", 'previous.after.as_ref().expect("inserted destination")', 'previous.before.as_ref().expect("inserted destination")'),
    ("wrong_source_row", "remove_index_sources", "&change.row_id", "&0"),
    ("wrong_destination_row", "prepare_index_destinations", "after.clone(), change.row_id", "after.clone(), 0"),
    ("omit_insert_record", "prepare_index_destinations", "inserted.push(change_idx);", ""),
    ("wrong_insert_record", "prepare_index_destinations", "inserted.push(change_idx);", "inserted.push(0);"),
    ("skip_source_removal", "remove_index_sources", "self.indexes[change.binding]\n                    .index\n                    .transactional_remove(before, &change.row_id)", "Ok::<(), Error>(())"),
]
END_MARKERS = {
    "prepare_index_destinations": "\n    fn rollback_index_destinations(",
    "rollback_index_destinations": "\n    fn remove_index_sources(",
    "remove_index_sources": "\n    pub(crate) fn poison_after_wal_failure(",
}


def mutate(source, method, old, new):
    begin = source.index("    fn " + method + "(")
    end = source.index(END_MARKERS[method], begin)
    selected = source[begin:end]
    if selected.count(old) != 1:
        raise RuntimeError("mutation anchor absent or ambiguous: " + method + ": " + old)
    return source[:begin] + selected.replace(old, new, 1) + source[end:]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.CONTRACTS, generate.OUTPUT,
            Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
            ROOT / "verification/concurrent/generate.py", ROOT / "verification/predicate/generate.py", PIN]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/postings")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_posting_preparation_rollback_removal",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_unsafe_posting_refinement_proved": False, "transaction_history_refinement_proved": False, "destination_ownership_derivation_proved": False}
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
        if generate.OUTPUT.read_text() != generate.render(source):
            raise RuntimeError("stale generated posting operations")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_postings", "--crate-type=lib",
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
                raise RuntimeError("posting proof failed: " + name)

        invoke("native_postings", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        for name, proof_root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(generate.render(mutate(source, proof_root, old, new)))
            invoke(name, artifact, proof_root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("posting proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
