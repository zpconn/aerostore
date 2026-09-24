#!/usr/bin/env python3
"""Check native publication/completion composition and semantic controls."""
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
ROOTS=['finish_transaction','publication_changes','publish_then_complete','owned_registration_finishes',
       'completion_entry_has_live_witness','completed_write_invalidates_capture']
MUTATIONS=[
 ('retain_finished_registration','finish_transaction','tx . registration . take ( )','tx . registration'),
 ('finish_wrong_slot','finish_transaction','lifecycle :: end_transaction ( driver , registration )',
  'lifecycle :: end_transaction ( driver , lifecycle::ProcArrayRegistration{slot_idx:0,..registration} )'),
 ('wrong_change_binding','publication_changes','binding:c.binding,before:c.before,after:c.after','binding:0,before:c.before,after:c.after'),
 ('wrong_change_before','publication_changes','binding:c.binding,before:c.before,after:c.after','binding:c.binding,before:None,after:c.after'),
 ('wrong_change_after','publication_changes','binding:c.binding,before:c.before,after:c.after','binding:c.binding,before:c.before,after:None'),
 ('skip_native_data','publish_then_complete','native_ordinary_data_segment(storage,plan,ordinary_plan)',
  'Ok::<Vec<ordinary::PublishedWrite>,DataError>(Vec::new())'),
 ('skip_native_finish','publish_then_complete','finish_transaction(&mut publisher.lifecycle,token)',
  'Ok::<(),lifecycle::ProcArrayError>(())'),
 ('skip_native_stamps','publish_then_complete','predicate::publish_index_stamps::<scenario::Bridge<L,I>,C>(publisher,&changes)',
  'Ok::<(),predicate::Error>(())'),
 ('omit_deregistration_poison','publish_then_complete','Err(_)=>{poison(storage);return Err(CompletionError::Deregistration);}',
  'Err(_)=>{return Err(CompletionError::Deregistration);}'),
 ('omit_stamp_poison','publish_then_complete','Err(_)=>{poison(storage);return Err(CompletionError::Stamp);}',
  'Err(_)=>{return Err(CompletionError::Stamp);}'),
 ('omit_plan_agreement','publish_then_complete','ordinary_matches(*plan,*ordinary_plan),',''),
 ('omit_registration_context','publish_then_complete','old(publisher).writer_txid==old(token).registration.unwrap().txid,',''),
 ('wrong_returned_stamp','publish_then_complete','Ok(Completion{writes,stamp:publisher.reserved_stamp})',
  'Ok(Completion{writes,stamp:plan.record.txid})'),
 ('omit_freshness','completed_write_invalidates_capture','reader<=before.clock,before.clock<=stamp,','before.clock<=stamp,'),
]

def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    files=[PIN]
    for name in ('commit_completion','commit_data','lookup','row_publication','postings','lifecycle_scenario',
                 'lifecycle','predicate_capture','predicate','concurrent'):
        files.extend(sorted(p for p in (ROOT/'verification'/name).iterdir() if p.suffix in ('.py','.rs') or p.name=='README.md'))
    files.extend(ROOT/p for p in ('aerostore_core/src/occ_partitioned.rs','aerostore_core/src/procarray.rs',
        'aerostore_core/src/shm.rs','aerostore_core/src/shm_index.rs','aerostore_core/src/shm_lock.rs',
        'aerostore_verified/src/lib.rs'))
    return list(dict.fromkeys(files))


def mutation(source,item):
    name,root,old,new=item
    match=re.search(r"pub (?:proof |open spec )?fn "+re.escape(root)+r"(?:<|\()",source)
    if not match: raise RuntimeError("missing mutation root: "+root)
    start=match.start();end=source.find("\npub ",start+1)
    if end<0:end=len(source)
    part=source[start:end]
    if part.count(old)!=1: raise RuntimeError("mutation anchor absent or ambiguous: "+name)
    return source[:start]+part.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/commit-completion")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_one_write_publication_completion",
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
            raise RuntimeError("stale generated commit completion")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_commit_completion", "--crate-type=lib",
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
                raise RuntimeError("completion proof failed: " + name)

        invoke("native_commit_completion", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new)))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("completion proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
