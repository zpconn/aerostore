#!/usr/bin/env python3
"""Check native predicate data contracts and semantic negative controls."""
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
ROOTS = ["index_lock_keys", "index_read_conflict", "publish_index_stamps",
         "stamp_precedes_snapshot", "publication_invalidates_dependency",
         "changed_stamp_invalidates_dependency", "read_key_member",
         "stamp_unchanged", "stamp_update", "validate_after_late_publication",
         "changed_binding_in_range"]
MUTATIONS = [
    ("omit_read_bucket", "index_lock_keys", "keys.insert((binding, read.bucket));", ""),
    ("wrong_read_bucket", "index_lock_keys", "keys.insert((binding, read.bucket));", "keys.insert((binding, 0));"),
    ("wrong_write_binding", "index_lock_keys", "keys.insert((change.binding, index.transactional_key_bucket(key)?));", "keys.insert((0, index.transactional_key_bucket(key)?));"),
    ("omit_write_bucket", "index_lock_keys", "keys.insert((change.binding, index.transactional_key_bucket(key)?));", "let _ = index.transactional_key_bucket(key)?;"),
    ("ignore_sticky_conflict", "index_read_conflict", "if tx.index_conflict {", "if false {"),
    ("ignore_changed_stamp", "index_read_conflict", "if stamp != read.stamp ||", "if false ||"),
    ("ignore_snapshot_boundary", "index_read_conflict", "!aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid)", "false && !aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid)"),
    ("publish_wrong_bucket", "publish_index_stamps", ".transactional_publish_stamp(bucket, stamp)?;", ".transactional_publish_stamp(0, stamp)?;"),
    ("publish_stale_stamp", "publish_index_stamps", ".transactional_publish_stamp(bucket, stamp)?;", ".transactional_publish_stamp(bucket, 0)?;"),
    ("omit_publication", "publish_index_stamps", "self.indexes[binding]\n                .index\n                .transactional_publish_stamp(bucket, stamp)?;", "if false { self.indexes[binding].index.transactional_publish_stamp(bucket, stamp)?; }"),
    ("skip_nonempty_publication", "publish_index_stamps", "if changes.is_empty() {", "if true {"),
    ("wrong_touched_binding", "publish_index_stamps", "touched.insert((\n                    change.binding,", "touched.insert((\n                    0,"),
    ("nonstrict_snapshot_boundary", "stamp_precedes_snapshot", "stamp < transaction_id", "stamp <= transaction_id"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/predicate")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
               "scope": "conditional_source_bound_predicate_data_refinement",
               "required_roots": ROOTS, "required_mutations": [row[0] for row in MUTATIONS],
               "native_predicate_algorithms_proved_under_primitive_contracts": False,
               "native_storage_atomics_refinement_proved": False,
               "transaction_history_refinement_proved": False,
               "source_stable": False}
    receipt_path = output / "receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    inputs = [generate.SOURCE, generate.HELPER, generate.CONTRACTS, generate.OUTPUT,
              Path(generate.__file__), Path(__file__), HERE / "test_generate.py", PIN,
              ROOT / "verification/concurrent/generate.py"]
    try:
        pins = json.loads(PIN.read_text())
        distribution = ROOT / pins["distribution"]
        for name, expected in pins["artifact_sha256"].items():
            if digest(distribution / name) != expected:
                raise RuntimeError("verifier artifact checksum differs: " + name)
        receipt["input_sha256"] = {str(path.relative_to(ROOT)): digest(path) for path in inputs}
        receipt["toolchain"] = pins
        source, helper = generate.SOURCE.read_text(), generate.HELPER.read_text()
        generated = generate.render(source, helper)
        if not generate.OUTPUT.exists() or generate.OUTPUT.read_text() != generated:
            raise RuntimeError("stale generated operation; run verification/predicate/generate.py")
        env = dict(os.environ)
        env.update(RUSTUP_HOME=str(ROOT / pins["rustup_home"]), RUSTUP_TOOLCHAIN=pins["rust_toolchain"],
                   VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_predicate", "--crate-type=lib",
                "--edition=2021", "--target", pins["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "50"]

        def invoke(name: str, path: Path, negative: bool = False, required_root: str | None = None) -> None:
            started = time.monotonic()
            command = base + (["--verify-root", "--verify-function", required_root] if required_root else []) + [str(path)]
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
                     "elapsed_seconds": time.monotonic() - started,
                     "verified": int(summary[1]) if summary else None,
                     "errors": int(summary[2]) if summary else None,
                     "expected_failure": negative, "required_root": required_root,
                     "source_sha256": digest(path), "log": str(log_path.relative_to(ROOT)),
                     "log_sha256": digest(log_path)}
            receipt["checks"].append(check)
            if negative:
                if (process.returncode == 0 or not summary or int(summary[2]) == 0
                        or not re.search(r"(?:precondition|postcondition|invariant|assertion) not satisfied|assertion failed", log)):
                    raise RuntimeError("negative control failed for wrong reason: " + name)
            elif process.returncode != 0 or not summary or int(summary[1]) < (1 if required_root else len(ROOTS)) or int(summary[2]) != 0:
                raise RuntimeError("native predicate proof failed: " + name)

        invoke("native_predicate", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, required_root=root)
        for name, root, old, new in MUTATIONS:
            selected = helper if root == "stamp_precedes_snapshot" else source
            # Select the intended native method, never an unrelated test or
            # similar operation elsewhere in the production module.
            if root != "stamp_precedes_snapshot":
                offset = selected.index("    fn " + root + "(")
                method_end = selected.find("\n    fn ", offset + 1)
                end = method_end if method_end >= 0 else len(selected)
            else:
                offset, end = 0, len(selected)
            section = selected[offset:end]
            if section.count(old) != 1:
                raise RuntimeError("mutation no longer uniquely identifies operation: " + name)
            mutant = selected[:offset] + section.replace(old, new, 1) + selected[end:]
            rendered = generate.render(source, mutant) if root == "stamp_precedes_snapshot" else generate.render(mutant, helper)
            path = output / (name + ".rs")
            path.write_text(rendered)
            invoke(name, path, negative=True, required_root=root)
        receipt["final_input_sha256"] = {str(path.relative_to(ROOT)): digest(path) for path in inputs}
        if receipt["final_input_sha256"] != receipt["input_sha256"]:
            raise RuntimeError("proof source changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True,
                       native_predicate_algorithms_proved_under_primitive_contracts=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "scope": receipt["scope"],
                      "receipt": str(receipt_path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
