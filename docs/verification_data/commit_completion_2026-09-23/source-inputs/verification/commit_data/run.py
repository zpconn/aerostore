#!/usr/bin/env python3
"""Verify source-bound single-index posting and row-publication composition."""
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
    "planned_derives_destination_ownership", "complete_postings_match_published_row",
    "prepare_destinations", "rollback_destinations", "remove_sources", "publish_rows", "poison",
    "publish_indexed_write", "publish_rows_ordinary", "native_ordinary_data_segment",
    "admitted_input_witness", "successful_relation_witness", "create_move_delete_have_witnesses",
]
# The boolean distinguishes roots mutated through a shared specification or
# native module (requiring a full-program check) from a single root body.
MUTATIONS = [
    ("native_remove_before_prepare", "__native_order__", "", "", None),
    ("omit_initial_posting_coherence", "planned", "&& row_postings_match(before,plan.changes[0].binding,plan.changes[0].row_id)", "", None),
    ("omit_before_key_link", "planned", "&& plan.changes[0].before==head_key(before.image,plan.changes[0].row_id)", "", None),
    ("omit_after_key_link", "planned", "&& before.image.rows[plan.record.writes[0].new_offset].value==plan.changes[0].after", "", None),
    ("omit_row_identity", "planned", "&& plan.changes[0].row_id==plan.record.writes[0].row_id", "", None),
    ("allow_unchanged_index_key", "planned", "&& plan.changes[0].before!=plan.changes[0].after", "", None),
    ("skip_destination_prepare", "prepare_destinations", "postings::prepare_index_destinations(&mut storage.postings,&plan.changes)", "Ok::<Vec<usize>,postings::Error>(Vec::new())", "prepare_destinations"),
    ("skip_destination_rollback", "rollback_destinations", "postings::rollback_index_destinations(&mut storage.postings,&plan.changes,inserted)", "Ok::<(),postings::Error>(())", "rollback_destinations"),
    ("skip_source_removal", "remove_sources", "postings::remove_index_sources(&mut storage.postings,&plan.changes)", "Ok::<(),postings::Error>(())", "remove_sources"),
    ("skip_prepared_row_publication", "publish_rows", "publication::publish_prepared_write_set(&mut storage.rows,&plan.record)", "Ok::<(),lookup::Error>(())", "publish_rows"),
    ("skip_ordinary_row_publication", "publish_rows_ordinary", "ordinary::publish_write_set(&mut storage.rows,&ordinary_plan.tx,&ordinary_plan.indices)", "Ok::<Vec<ordinary::PublishedWrite>,lookup::Error>(Vec::new())", "publish_rows_ordinary"),
    ("omit_native_failure_poison", "native_ordinary_data_segment", "poison ( storage ) ;", "", "native_ordinary_data_segment"),
    ("omit_explicit_poison", "poison", "storage.postings.poison_indexes();", "", "poison"),
    ("ordinary_wrong_row_report", "publish_write_set", "row_id : write . row_id", "row_id : 0", None),
    ("ordinary_wrong_dirty_report", "publish_write_set", "dirty_columns_bitmask : write . dirty_columns_bitmask", "dirty_columns_bitmask : 0", None),
    ("ordinary_wrong_value_report", "publish_write_set", "value : driver . load_value ( new_row )", "value : base_value", None),
    ("ordinary_wrong_base_value_report", "publish_write_set", "base_value ,", "base_value : driver . load_value ( new_row ) ,", None),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files=[generate.CONTRACTS,generate.OUTPUT,Path(generate.__file__),Path(__file__),HERE/"test_generate.py",PIN,
        HERE/"ordinary.rs",HERE/"ordinary_generate.py",HERE/"commit_segment.py"]
    for name in ("lookup","postings","row_publication"):
        module=generate.component(name)
        files += [module.SOURCE,module.CONTRACTS,module.OUTPUT,Path(module.__file__)]
    files += [ROOT/"verification/lookup/history.rs",ROOT/"verification/concurrent/generate.py",
        ROOT/"verification/predicate/generate.py",ROOT/"verification/predicate_capture/generate.py",
        ROOT/"verification/predicate_capture/contracts.rs",ROOT/"aerostore_verified/src/lib.rs",
        ROOT/"aerostore_core/src/shm.rs"]
    return list(dict.fromkeys(files))


def mutation(source,item):
    name,root,old,new,verified_root=item
    if root=="__native_order__":
        native=generate.SOURCE.read_text()
        first="let inserted = self.prepare_index_destinations(&index_changes)?;"
        second="self.remove_index_sources(&index_changes)?;"
        if native.count(first)!=1 or native.count(second)!=1:raise RuntimeError("native order anchor changed")
        changed=native.replace(first,"TEMPORARY_ORDER_MARKER",1).replace(second,first,1).replace("TEMPORARY_ORDER_MARKER",second,1)
        original=generate.render_module()
        if source.count(original)!=1:raise RuntimeError("commit data module absent/ambiguous")
        return source.replace(original,generate.render_module(changed),1)
    match=re.search(r"pub (?:proof |open spec )?fn "+re.escape(root)+r"(?:<|\()",source)
    if not match:raise RuntimeError("missing mutation root: "+root)
    start=match.start();end=source.find("\npub ",start+1)
    if end<0:end=len(source)
    part=source[start:end]
    if part.count(old)!=1:raise RuntimeError("mutation anchor absent or ambiguous: "+name)
    return source[:start]+part.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/commit-data")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_single_index_row_commit_data",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "whole_storage_history_refinement_proved": False, "native_reclamation_refinement_proved": False, "transaction_history_refinement_proved": False, "multirow_publication_refinement_proved": False, "native_allocator_ownership_refinement_proved": False, "weak_memory_refinement_proved": False, "full_commit_refinement_proved": False, "WAL_refinement_proved": False}
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
            raise RuntimeError("stale generated commit data composition")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_commit_data", "--crate-type=lib",
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
                raise RuntimeError("commit data proof failed: " + name)

        invoke("native_commit_data", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new, verified_root in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new,verified_root)))
            invoke(name, artifact, verified_root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("commit data proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
