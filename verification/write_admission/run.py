#!/usr/bin/env python3
"""Check guarded native write validation and publication admission."""
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
ROOTS=['admission::has_write_base_conflict','admission::validated_base_prepares_publication',
       'admission::validation_establishes_plan','admission::validator_has_live_witness']
MUTATIONS=[
 ('skip_head_validation','has_write_base_conflict','if current_head != expected_head','if false','admission::has_write_base_conflict'),
 ('skip_xmax_validation','has_write_base_conflict','if driver . load_xmax ( base_row ) != 0','if false','admission::has_write_base_conflict'),
 ('repeat_first_selected_index','has_write_base_conflict','let idx=&indices[i];','let idx=&indices[0];','admission::has_write_base_conflict'),
 ('skip_validation_loop','has_write_base_conflict','while i<indices.len()','while false','admission::has_write_base_conflict'),
 ('invert_success','has_write_base_conflict','Ok ( false )','Ok ( true )','admission::has_write_base_conflict'),
 ('turn_conflict_into_error','has_write_base_conflict','if current_head != expected_head {\n    return Ok ( true ) ;\n}',
  'if current_head != expected_head { return Err (lookup::Error::Storage); }','admission::has_write_base_conflict'),
 ('omit_validated_base','validated_base_prepares_publication',',base_valid(image,write)','',
  'admission::validated_base_prepares_publication'),
 ('omit_fresh_creator','fresh_private','&& image.rows[write.new_offset].xmin==writer','',None),
 ('omit_fresh_xmax','fresh_private','&& image.rows[write.new_offset].xmax==0','',None),
 ('omit_native_change_agreement','validation_establishes_plan',
  'change_matches(before.image,ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int],plan.changes[0]),','',
  'admission::validation_establishes_plan'),
 ('omit_record_write_agreement','validation_establishes_plan',
  'plan.record.writes[0]==ordinary::row_write(ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),','',
  'admission::validation_establishes_plan'),
]

def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files=[PIN,generate.SOURCE]
    for name in ('write_admission','commit_data','lookup','row_publication','postings'):
        files.extend(sorted(p for p in (ROOT/'verification'/name).iterdir()
                            if p.suffix in ('.py','.rs') or p.name=='README.md'))
    files.extend(ROOT/p for p in ('verification/concurrent/generate.py','verification/predicate/generate.py',
        'verification/predicate_capture/generate.py','verification/predicate_capture/contracts.rs',
        'aerostore_verified/src/lib.rs','aerostore_core/src/shm.rs'))
    return list(dict.fromkeys(files))


def mutation(source,item):
    name,root,old,new,verified_root=item
    match=re.search(r"pub (?:proof |open spec )?fn "+re.escape(root)+r"(?:<|\()",source)
    if not match: raise RuntimeError("missing mutation root: "+root)
    start=match.start();end=source.find("\npub ",start+1)
    if end<0:end=len(source)
    part=source[start:end]
    if part.count(old)!=1: raise RuntimeError("mutation anchor absent or ambiguous: "+name)
    return source[:start]+part.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/write-admission")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_guarded_write_base_admission",
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
            raise RuntimeError("stale generated write admission")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_write_admission", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "80"]

        def invoke(name, artifact, root=None, negative=False):
            selection = []
            if root:
                module,function=root.rsplit('::',1)
                selection=["--verify-only-module",module,"--verify-function",function]
            command = base + selection + [str(artifact)]
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
                raise RuntimeError("admission proof failed: " + name)

        invoke("native_write_admission", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root.replace('::','_'), generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new, verified_root in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new,verified_root)))
            invoke(name, artifact, verified_root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("admission proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
