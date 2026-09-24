#!/usr/bin/env python3
"""Check native final-write/key planning and semantic controls."""
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
ROOT=generate.ROOT
HERE=Path(__file__).resolve().parent
PIN=ROOT/'verification/verus/toolchain.json'
ROOTS=['planning::prefix_insert','planning::map_yields_selection','planning::final_write_indices',
    'planning::selected_one_row','planning::index_changes','planning::repeated_write_selection_is_live',
    'planning::key_change_has_live_execution']
MUTATIONS=[
 ('overwrite_wrong_row','planning::final_write_indices','by_row . insert ( write . row_id , idx )','by_row . insert ( 0 , idx )'),
 ('overwrite_wrong_index','planning::final_write_indices','by_row . insert ( write . row_id , idx )','by_row . insert ( write . row_id , 0 )'),
 ('omit_overwrite','planning::final_write_indices','by_row . insert ( write . row_id , idx ) ;',''),
 ('omit_map_output','planning::final_write_indices','let values = by_row . into_values ( ) ;','let values:Vec<usize> = Vec::new();'),
 ('omit_one_row_premise','planning::selected_one_row','forall|i:int| 0<=i<tx.write_set.len() ==> #[trigger] tx.write_set[i].row_id==tx.write_set[0].row_id,',''),
 ('load_before_from_new','planning::index_changes','driver . load_value ( driver . resolve ( write . base_offset ) ? )','driver . load_value ( driver . resolve ( write . new_offset ) ? )'),
 ('load_after_from_base','planning::index_changes','let after = & driver . load_value ( driver . resolve ( write . new_offset ) ? )','let after = & driver . load_value ( driver . resolve ( write . base_offset ) ? )'),
 ('skip_before_prevalidation','planning::index_changes','if let Some ( key ) = & before {\n    driver . prevalidate ( binding , key , & write . row_id ) ? ;\n}',''),
 ('skip_after_prevalidation','planning::index_changes','if let Some ( key ) = & after {\n    driver . prevalidate ( binding , key , & write . row_id ) ? ;\n}',''),
 ('omit_unchanged_filter','planning::index_changes','if before == after {\n    binding = binding + 1 ;\n    continue ;\n}',''),
 ('wrong_change_row','planning::index_changes','row_id : write . row_id','row_id : 0'),
 ('wrong_change_binding','planning::index_changes','postings :: IndexChange {\n    binding ,','postings :: IndexChange {\n    binding:1 ,'),
 ('omit_change_push','planning::index_changes','changes . push ( postings :: IndexChange {\n    binding , row_id : write . row_id , before , after ,\n}\n) ;',''),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files=[PIN]
    for name in ('write_plan','commit_data','lookup','row_publication','postings','predicate_capture','predicate','concurrent'):
        files.extend(sorted(p for p in (ROOT/'verification'/name).iterdir() if p.suffix in ('.py','.rs') or p.name=='README.md'))
    files.extend(ROOT/p for p in ('aerostore_core/src/occ_partitioned.rs','aerostore_core/src/shm_index.rs',
        'aerostore_core/src/shm_lock.rs','aerostore_verified/src/lib.rs'))
    return list(dict.fromkeys(files))


def mutation(source,item):
    name,root,old,new=item
    root=root.rsplit("::",1)[-1]
    match=re.search(r"pub (?:proof |open spec )?fn "+re.escape(root)+r"(?:<|\()",source)
    if not match: raise RuntimeError("missing mutation root: "+root)
    start=match.start();end=source.find("\npub ",start+1)
    if end<0:end=len(source)
    part=source[start:end]
    if part.count(old)!=1: raise RuntimeError("mutation anchor absent or ambiguous: "+name)
    return source[:start]+part.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/write-plan")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_final_selection_and_one_index_key_planning",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "full_P1_complete":False,"whole_commit_refinement_proved":False,
        "arbitrary_native_history_refinement_proved":False,"native_allocator_ownership_refinement_proved":False,
        "weak_memory_refinement_proved":False}
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
            raise RuntimeError("stale generated write planning")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_write_plan", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "80"]

        def invoke(name, artifact, root=None, negative=False):
            command = base + (["--verify-only-module", root.rsplit("::",1)[0], "--verify-function", root.rsplit("::",1)[1]] if root else []) + [str(artifact)]
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
                raise RuntimeError("write planning proof failed: " + name)

        invoke("native_write_planning", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root.replace("::","_"), generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new)))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("write planning proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
