#!/usr/bin/env python3
"""Check conditional native-commit orchestration and semantic negative controls."""
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


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/concurrent")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
               "scope": "conditional_source_bound_commit_orchestration",
               "native_primitive_refinement_proved": False,
               "required_roots": ["commit_with_record_impl", "CommitPrimitives::prepare_before_publish", "CommitPrimitives::abort_preparation", "CommitPrimitives::invoke_before_publish", "CommitPrimitives::rollback_prepared_commit", "callback_contract_has_witnesses"],
               "transaction_history_refinement_proved": False}
    receipt_path = output / "receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    try:
        pins = json.loads(PIN.read_text())
        distribution = ROOT / pins["distribution"]
        for name, expected in pins["artifact_sha256"].items():
            if digest(distribution / name) != expected:
                raise RuntimeError("verifier artifact checksum differs: " + name)
        inputs = [generate.SOURCE, generate.CONTRACTS, generate.OUTPUT, Path(generate.__file__), Path(__file__), PIN]
        fingerprints = {str(path.relative_to(ROOT)): digest(path) for path in inputs}
        receipt["input_sha256"] = fingerprints
        receipt["toolchain"] = pins
        source = generate.SOURCE.read_text()
        generated = generate.render(source)
        if not generate.OUTPUT.exists() or generate.OUTPUT.read_text() != generated:
            raise RuntimeError("stale generated operation; run verification/concurrent/generate.py")
        env = dict(os.environ)
        env.update(RUSTUP_HOME=str(ROOT / pins["rustup_home"]), RUSTUP_TOOLCHAIN=pins["rust_toolchain"],
                   VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_commit", "--crate-type=lib",
                "--edition=2021", "--target", pins["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "40"]

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
            (output / (name + ".log")).write_text(log)
            summary = re.search(r"verification results:: (\d+) verified, (\d+) errors", log)
            check = {"name": name, "command": command, "exit_code": process.returncode,
                     "elapsed_seconds": time.monotonic() - started,
                     "verified": int(summary[1]) if summary else None,
                     "errors": int(summary[2]) if summary else None,
                     "expected_failure": negative, "required_root": required_root, "source_sha256": digest(path)}
            receipt["checks"].append(check)
            if negative:
                if (process.returncode == 0 or not summary or int(summary[2]) == 0
                        or not re.search(r"(?:precondition|postcondition) not satisfied", log)):
                    raise RuntimeError("negative control failed for wrong reason: " + name)
            elif process.returncode != 0 or not summary or int(summary[1]) < (1 if required_root else len(receipt["required_roots"])) or int(summary[2]) != 0:
                raise RuntimeError("conditional commit proof failed: " + name)

        invoke("native_commit", generate.OUTPUT)
        for root in receipt["required_roots"]:
            invoke("root_" + root.replace("::", "_"), generate.OUTPUT, required_root=root)
        mutations = [
            ("skip_predicate_validation", "self.index_read_conflict(tx)?", "false"),
            ("omit_partial_publication_poison", "self.poison_indexes();", ""),
            ("skip_wal_binding_check", "self.ensure_unlogged_write_allowed()?;", ""),
            ("skip_guarded_health_check", "self.check_not_poisoned()", "Ok::<(), Error>(())"),
            ("omit_health_failure_abort", "self.abort_preparation(tx)?;\n            return Err(err.into());", "return Err(err.into());"),
            ("skip_write_ahead_callback", "self.invoke_before_publish(tx, &index_changes, &inserted, &record, before_publish)?;", ""),
            ("omit_callback_error_rollback", "self.rollback_prepared_commit(tx, changes, inserted)?;", ""),
            ("omit_callback_unwind_rollback", "let _ = self.rollback_prepared_commit(tx, changes, inserted);", ""),
            ("stamp_before_deregister", "let finish = match self.finish_transaction(tx)", "let finish = match self.publish_index_stamps(&index_changes)"),
            ("omit_record_prepare_error_abort", "self.abort_preparation(tx)?;\n                return Err(err.into());", "return Err(err.into());"),
            ("omit_prepare_error_abort", "self.abort_preparation(tx)?;\n                Err(err)", "Err(err)"),
            ("omit_prepare_unwind_abort", "let _ = self.abort_preparation(tx);", ""),
        ]
        offset = source.index("    fn commit_with_record_impl<")
        for name, old, new in mutations:
            selected = source[offset:]
            if old not in selected:
                raise RuntimeError("mutation no longer identifies its intended operation: " + name)
            mutant = source[:offset] + selected.replace(old, new, 1)
            path = output / (name + ".rs")
            path.write_text(generate.render(mutant))
            required_root = ("CommitPrimitives::prepare_before_publish"
                             if name in {"omit_record_prepare_error_abort", "omit_prepare_error_abort", "omit_prepare_unwind_abort"}
                             else "CommitPrimitives::invoke_before_publish"
                             if name in {"omit_callback_error_rollback", "omit_callback_unwind_rollback"}
                             else "commit_with_record_impl")
            invoke(name, path, negative=True, required_root=required_root)
        # Keep the mutant well-typed while delaying every dependency check
        # until after destination preparation and WAL acceptance.
        start = source.index("        if self.index_read_conflict(tx)?", offset)
        end = source.index("        // All index destinations", start)
        validation = source[start:end]
        mutant = source[:start] + source[end:]
        marker = "        self.remove_index_sources(&index_changes)?;"
        if mutant.count(marker) != 1:
            raise RuntimeError("acceptance-order mutation no longer identifies publication")
        mutant = mutant.replace(marker, validation + marker, 1)
        path = output / "accept_before_validation.rs"
        path.write_text(generate.render(mutant))
        invoke("accept_before_validation", path, negative=True, required_root="commit_with_record_impl")
        for path in inputs:
            if digest(path) != fingerprints[str(path.relative_to(ROOT))]:
                raise RuntimeError("proof source changed during verification: " + str(path))
        receipt.update(passed=True, status="passed")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "scope": receipt["scope"],
                      "receipt": str(receipt_path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
