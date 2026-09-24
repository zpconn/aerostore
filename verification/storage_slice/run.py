#!/usr/bin/env python3
"""Verify native storage cutpoints and their derived retained-snapshot history."""
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
ROOTS = ["native_horizon_covers_reader", "seeded_native_preconditions",
    "publication_derives_snapshot_history", "publication_derives_completed_history",
    "native_publication_derives_history", "publication_derives_retained_prefix",
    "native_vacuum_derives_history", "pruned_tail_reuse_preserves_history",
    "native_read_after_reuse", "native_validation_rejects_invisible_writer",
    "old_snapshot_cases_have_witnesses", "native_vacuum_releases_anchor",
    "native_constructor_clears_metadata", "native_initialize_preserves_reader"]
MUTATIONS = [
    ("omit_slot_ownership", "native_horizon_covers_reader", "old(driver).state().slots[slot].txid==tx.txid,", ""),
    ("omit_reader_pin", "native_horizon_covers_reader", "old(driver).state().slots[slot].snapshot_xmin==tx.snapshot_xmin,", ""),
    ("skip_native_publication", "native_publication_derives_history", "publication::publish_prepared_write_set(driver,record)", "Ok::<(),lookup::Error>(())"),
    ("skip_native_vacuum", "native_vacuum_derives_history", "retention::vacuum_row_acquired(driver,0,horizon,Ghost(lookup::snapshot(tx)))", "Ok::<Vec<retention::Reclaimed>,lookup::Error>(Vec::new())"),
    ("omit_snapshot_horizon_bound", "native_vacuum_derives_history", "birth<horizon<=tx.snapshot_xmin,", "birth<horizon,"),
    ("omit_tail_eligibility", "native_vacuum_derives_history", "birth<horizon<=tx.snapshot_xmin,", "horizon<=tx.snapshot_xmin,"),
    ("skip_native_read", "native_read_after_reuse", "lookup::read(driver,tx,0)", "Ok::<Option<Option<usize>>,lookup::Error>(None)"),
    ("accept_invisible_writer", "native_validation_rejects_invisible_writer", "lookup::has_serialization_conflict(driver,tx)", "Ok::<bool,lookup::Error>(false)"),
    ("omit_advanced_horizon", "native_vacuum_releases_anchor", "1<birth<writer<horizon<=tx.snapshot_xmin,", "1<birth<writer, horizon<=tx.snapshot_xmin,"),
    ("skip_native_initialization", "native_initialize_preserves_reader", "initialization::initialize_row(driver,row_ptr,Some(key),reuser,0)", "Ok::<(),lookup::Error>(())"),
    ("wrong_reused_value", "native_initialize_preserves_reader", "initialization::initialize_row(driver,row_ptr,Some(key),reuser,0)", "initialization::initialize_row(driver,row_ptr,None,reuser,0)"),
    ("initialize_protected_anchor", "native_initialize_preserves_reader", "row_ptr==l.tail,", "row_ptr==l.anchor,"),
    ("omit_exclusive_allocation", "native_initialize_preserves_reader", "old(driver).state().exclusive.contains(l.tail),", ""),
    ("reuse_visible_anchor", "reused", "p.rows.insert(l.tail,", "p.rows.insert(l.anchor,"),
]
INITIALIZATION = generate.component("row_initialization")
MUTATIONS += [("native_"+item[0], "__initialization__", item, "") for item in INITIALIZATION.MUTATIONS]


def verified_mutation_root(root):
    if root == "__initialization__": return None
    return "pruned_tail_reuse_preserves_history" if root == "reused" else root


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files=[generate.TEMPLATE,generate.OUTPUT,Path(generate.__file__),Path(__file__),HERE/"test_generate.py",PIN]
    for name in ("lookup","row_publication","row_retention","lifecycle","row_initialization"):
        module=generate.component(name)
        files += [module.SOURCE,module.CONTRACTS,module.OUTPUT,Path(module.__file__)]
    files += [ROOT/"verification/lookup/history.rs", ROOT/"verification/concurrent/generate.py",
        ROOT/"verification/row_retention/horizon.rs", ROOT/"aerostore_core/src/vacuum.rs",
        ROOT/"verification/predicate/generate.py", ROOT/"verification/predicate_capture/generate.py",
        ROOT/"verification/predicate_capture/contracts.rs", ROOT/"aerostore_verified/src/lib.rs",
        ROOT/"aerostore_core/src/shm.rs"]
    return list(dict.fromkeys(files))


def mutation(source,item):
    name,root,old,new=item
    if root == "__initialization__":
        original=INITIALIZATION.render_module()
        changed=INITIALIZATION.render_module(INITIALIZATION.mutation_source(old))
        if source.count(original)!=1: raise RuntimeError("initialization module is absent or ambiguous")
        return source.replace(original,changed,1)
    match=re.search(r"pub (?:proof |open spec )?fn "+re.escape(root)+r"(?:<|\()",source)
    if not match: raise RuntimeError("missing mutation root: "+root)
    start=match.start();end=source.find("\npub ",start+1)
    if end<0:end=len(source)
    part=source[start:end]
    if part.count(old)!=1: raise RuntimeError("mutation anchor absent or ambiguous: "+name)
    return source[:start]+part.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/storage-slice")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_single_row_retained_storage_slice",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "whole_storage_history_refinement_proved": False, "native_reclamation_refinement_proved": False, "transaction_history_refinement_proved": False, "multirow_publication_refinement_proved": False, "native_allocator_ownership_refinement_proved": False, "weak_memory_refinement_proved": False}
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
            raise RuntimeError("stale generated storage slice")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_storage_slice", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "80"]

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
                raise RuntimeError("storage proof failed: " + name)

        invoke("native_storage_slice", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new)))
            invoke(name, artifact, verified_mutation_root(root), True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("storage proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
