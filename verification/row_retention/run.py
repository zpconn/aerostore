#!/usr/bin/env python3
"""Verify native vacuum horizon dispatch, acquired-row retention and semantic controls."""
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
ROOTS = ['reachable_rank', 'reachable_unroll', 'splice_valid', 'below_cut_unchanged', 'splice_reachable', 'eligible_is_invisible', 'splice_first_visible', 'prefix_edges', 'selection_is_visible', 'prefix_reflexive', 'safe_reflexive', 'vacuum_row_acquired', 'retained_anchor_and_later_reclaim_witness', 'locked_version_survives_witness', 'min_horizon', 'compute_global_xmin', 'public_vacuum_reclaim_once', 'run_vacuum_pass']
MUTATIONS = [('reclaim_equal_horizon', 'vacuum_row_acquired', 'xmax < global_xmin', 'xmax <= global_xmin'), ('reclaim_live_version', 'vacuum_row_acquired', 'xmax != 0 && xmax < global_xmin', 'xmax < global_xmin'), ('reclaim_locked_version', 'vacuum_row_acquired', '&& ! curr_row . locked', ''), ('omit_unlink', 'vacuum_row_acquired', 'driver . store_next ( prev_ptr , next_offset , row_id ) ;', ''), ('drop_remaining_tail', 'vacuum_row_acquired', 'driver . store_next ( prev_ptr , next_offset , row_id ) ;', 'driver . store_next ( prev_ptr , 0 , row_id ) ;'), ('recycle_predecessor', 'vacuum_row_acquired', 'driver . recycle ( row_id , curr_ptr ) ? ;', 'driver . recycle ( row_id , prev_ptr ) ? ;'), ('recycle_before_unlink', 'vacuum_row_acquired', 'driver . store_next ( prev_ptr , next_offset , row_id ) ;', 'driver . recycle ( row_id , curr_ptr ) ? ; driver . store_next ( prev_ptr , next_offset , row_id ) ;'), ('omit_predecessor_advance', 'vacuum_row_acquired', 'prev_offset = curr_offset ;', ''), ('skip_native_scan', 'vacuum_row_acquired', 'while curr_offset != 0 invariant', 'while false invariant'), ('wrong_reclaimed_value', 'vacuum_row_acquired', 'let reclaimed_value = curr_row . value ;', 'let reclaimed_value = None ;'), ('wrong_head_report', 'vacuum_row_acquired', 'live_head_value : Some ( live_head_value )', 'live_head_value : None'), ('attempt_reclaim_head', 'vacuum_row_acquired', 'let mut curr_offset = head_row . next ;', 'let mut curr_offset = head_offset ;'), ('omit_reader_creator_exclusion', 'eligible_is_invisible', '&& forall|p:u32| s.rows.contains_key(p) ==> s.rows[p].xmin != tx.txid', '&& true'), ('omit_global_horizon_bound', 'eligible_is_invisible', 'snapshot_well_formed(tx) && horizon <= tx.xmin', 'snapshot_well_formed(tx)'), ('omit_active_lower_bound', 'eligible_is_invisible', '&& forall|i: int| 0 <= i < tx.active.len() ==> tx.xmin <= tx.active[i]', '&& true')]

MUTATIONS += [
    ("public_omits_horizon_clamp", "public_vacuum_reclaim_once", "min_horizon ( requested_xmin , retained_xmin )", "requested_xmin"),
    ("public_ignores_conservative_request", "public_vacuum_reclaim_once", "min_horizon ( requested_xmin , retained_xmin )", "retained_xmin"),
    ("public_dispatches_maximum", "public_vacuum_reclaim_once", "min_horizon ( requested_xmin , retained_xmin )", "u64::MAX"),
    ("public_omits_kernel_dispatch", "public_vacuum_reclaim_once", "driver . reclaim_before ( min_horizon ( requested_xmin , retained_xmin ) )", "Ok(Vec::new())"),
    ("collector_dispatches_maximum", "run_vacuum_pass", "driver . reclaim_before ( global_xmin )", "driver.reclaim_before(u64::MAX)"),
    ("collector_omits_kernel_dispatch", "run_vacuum_pass", "driver . reclaim_before ( global_xmin )", "Ok::<Vec<Reclaimed>,Error>(Vec::new())"),
    ("horizon_min_selects_max", "min_horizon", "if requested<retained {requested} else {retained}", "if requested>retained {requested} else {retained}"),
    ("horizon_min_ignores_retention", "min_horizon", "if requested<retained {requested} else {retained}", "requested"),
    ("retention_scan_returns_maximum", "compute_global_xmin", "driver.release_lifecycle();\n    result", "driver.release_lifecycle();\n    u64::MAX"),
]

def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE,generate.CONTRACTS,generate.OUTPUT,Path(generate.__file__),Path(__file__),HERE/'test_generate.py',
        generate.lookup.CONTRACTS,generate.lookup.HISTORY,generate.lookup.OUTPUT,Path(generate.lookup.__file__),
        ROOT/'verification/concurrent/generate.py',ROOT/'verification/predicate_capture/generate.py',
        ROOT/'verification/predicate_capture/contracts.rs',ROOT/'aerostore_verified/src/lib.rs',PIN,
        generate.VACUUM_SOURCE,generate.HORIZON,ROOT/'aerostore_core/src/procarray.rs',
        ROOT/'verification/lifecycle/contracts.rs',ROOT/'verification/lifecycle/generate.py',
        ROOT/'verification/lifecycle/lifecycle.verus.rs',ROOT/'aerostore_core/src/shm.rs',
        ROOT/'verification/predicate/generate.py']


def mutation(source, item):
    name, root, old, new = item
    # Abstract history mutations target a unique definition; native mutations
    # target exactly the selected function, never its asserted contract.
    if root == "eligible_is_invisible":
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
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/row-retention")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_acquired_row_vacuum_retention",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_pointer_recycler_refinement_proved": False, "native_row_guard_interference_refinement_proved": False, "global_horizon_composition_proved": False, "transaction_history_refinement_proved": False}
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
            raise RuntimeError("stale generated row retention")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_row_retention", "--crate-type=lib",
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
                raise RuntimeError("row retention proof failed: " + name)

        invoke("native_row_retention", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        source = generate.OUTPUT.read_text()
        for name, root, old, new in MUTATIONS:
            artifact = output / (name + ".rs")
            artifact.write_text(mutation(source,(name,root,old,new)))
            invoke(name, artifact, root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("row retention proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
