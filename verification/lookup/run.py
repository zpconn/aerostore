#!/usr/bin/env python3
"""Verify native lookup/MVCC selection and conditional complete-or-retry history composition."""
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
ROOTS = ['is_visible',
 'row_locked_by_other_tx',
 'find_visible_row_ptr',
 'latest_pending',
 'record_read',
 'read',
 'has_serialization_conflict',
 'materialize',
 'invisible_delete_preserves_visibility',
 'invisible_head_append_preserves_snapshot',
 'protocol_step_preserves',
 'reachable_protocol_valid',
 'event_chronology_from_history',
 'posting_update_exact',
 'posting_history_exact',
 'stamp_history_covers',
 'accepted_stamps_force_early_events',
 'accepted_replay_matches',
 'accepted_history_has_candidates',
 'materialize_after_checked_history',
 'later_deletion_rejects_recorded_read',
 'query_contract_has_success_witness']
MUTATIONS = [('ignore_creator_active', 'is_visible', 'active_contains ( & tx . snapshot_active , row . xmin )', 'false'),
 ('ignore_deleter_active', 'is_visible', 'active_contains ( & tx . snapshot_active , xmax )', 'false'),
 ('accept_own_deleted_row',
  'is_visible',
  'if xmax == tx . txid {\n    return false ;',
  'if xmax == tx . txid {\n    return true ;'),
 ('skip_chain_successor', 'find_visible_row_ptr', 'head_offset = row . next ;', 'head_offset = EMPTY_PTR ;'),
 ('treat_chain_limit_as_absence',
  'find_visible_row_ptr',
  'return Err ( Error :: SerializationFailure ) ;',
  'return Ok(None);'),
 ('ignore_private_write', 'read', 'latest_pending ( & tx . write_set , row_id )', 'None::<PendingWrite>'),
 ('ignore_foreign_row_lock', 'row_locked_by_other_tx', 'owner != txid', 'false'),
 ('omit_record_read', 'read', 'record_read ( tx , row_id , row_ptr , observed_xmin ) ;', ''),
 ('record_wrong_row',
  'record_read',
  'row_id , row_ptr , xmin : xmin ,',
  'row_id : 0 , row_ptr , xmin : xmin ,'),
 ('record_wrong_xmin', 'record_read', 'row_id , row_ptr , xmin : xmin ,', 'row_id , row_ptr , xmin : 0 ,'),
 ('ignore_row_identity_change', 'has_serialization_conflict', 'row . xmin != read . xmin', 'false'),
 ('ignore_row_deletion_active',
  'has_serialization_conflict',
  'active_contains ( & tx . snapshot_active , xmax )',
  'false'),
 ('omit_own_candidates',
  'materialize',
  'candidates.insert(tx.write_set[wi].row_id);',
  'let _omitted = tx.write_set[wi].row_id;'),
 ('ignore_predicate_filter', 'materialize', 'predicate . evaluate ( & value )', 'true'),
 ('duplicate_result',
  'materialize',
  'result . push ( row_id ) ;',
  'result . push ( row_id ) ; result . push(row_id);'),
 ('omit_old_posting_removal', 'posting_update_exact', 'before.remove((e.before.unwrap(),e.row))', 'before'),
 ('omit_new_posting', 'posting_update_exact', 'removed.insert((e.after.unwrap(),e.row))', 'removed'),
 ('allow_stamp_regression',
  'stamp_history_covers',
  'stamp_at(initial,events,i,p,b)<=events[i].stamp',
  'true'),
 ('omit_old_bucket',
  'accepted_stamps_force_early_events',
  'Set::empty().insert(p.bucket(e.before.unwrap()))',
  'Set::empty()'),
 ('publish_before_finish',
  'protocol_step_preserves',
  'a.ended.contains(writer) && !a.published.contains_key(writer)',
  'a.active.contains(writer) && !a.published.contains_key(writer)'),
 ('reuse_writer_stamp',
  'protocol_step_preserves',
  'a.clock<=stamp<u64::MAX',
  'stamp==writer && stamp<u64::MAX')]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.CONTRACTS, generate.HISTORY, generate.OUTPUT,
            Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
            ROOT / "verification/concurrent/generate.py",
            ROOT / "verification/predicate_capture/generate.py",
            ROOT / "verification/predicate_capture/contracts.rs",
            ROOT / "aerostore_verified/src/lib.rs", PIN]


def mutation(source, item):
    name, root, old, new = item
    # Abstract history mutations target a unique definition; native mutations
    # target exactly the selected function, never its asserted contract.
    if root in {"posting_update_exact", "stamp_history_covers", "accepted_stamps_force_early_events", "protocol_step_preserves"}:
        if source.count(old) != 1:
            raise RuntimeError("history mutation anchor absent or ambiguous: " + name)
        return source.replace(old,new,1)
    match = re.search(r"pub (?:proof )?fn " + re.escape(root) + r"(?:<|\()", source)
    if not match:
        raise RuntimeError("missing mutant root: "+root)
    start = match.start()
    end = source.find("\npub ", start+1)
    if end < 0: end=len(source)
    part=source[start:end]
    # Generated native body begins at a standalone opening brace.
    body=part.index("\n{\n")+3
    head, code = part[:body], part[body:]
    if code.count(old) != 1:
        raise RuntimeError("native mutation anchor absent or ambiguous: "+name)
    return source[:start]+head+code.replace(old,new,1)+source[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lookup")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_lookup_materialization_and_history",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_heap_history_refinement_proved": False, "native_reclamation_refinement_proved": False, "transaction_history_refinement_proved": False}
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
            raise RuntimeError("stale generated lookup")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_lookup", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "60"]

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
                raise RuntimeError("lookup proof failed: " + name)

        invoke("native_lookup", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new)))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("lookup proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
