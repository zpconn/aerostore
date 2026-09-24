#!/usr/bin/env python3
"""Verify actual prepared single-row publication and retained-snapshot correspondence."""
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
ROOTS = ["publish_prepared_write_set", "image_head_present", "publication_prefix_valid",
    "retained_chain_frame", "publication_preserves_old_snapshot", "successful_publication_connects_lookup",
    "publication_has_live_witness", "native_publication_has_executable_witness"]
MUTATIONS = [
    ("omit_base_deletion", "publish_prepared_write_set", "driver . compare_xmax ( base_row , 0 , record . txid , Ghost ( write . row_id ) , Ghost ( write . new_offset ) )", "Ok::<(),Error>(())"),
    ("write_zero_deleter", "publish_prepared_write_set", "driver . compare_xmax ( base_row , 0 , record . txid , Ghost ( write . row_id ) , Ghost ( write . new_offset ) )", "driver.compare_xmax(base_row,0,0,Ghost(write.row_id),Ghost(write.new_offset))"),
    ("wrong_xmax_expectation", "publish_prepared_write_set", "driver . compare_xmax ( base_row , 0 , record . txid , Ghost ( write . row_id ) , Ghost ( write . new_offset ) )", "driver.compare_xmax(base_row,record.txid,record.txid,Ghost(write.row_id),Ghost(write.new_offset))"),
    ("omit_version_link", "publish_prepared_write_set", "driver . store_next ( new_row , write . base_offset , Ghost ( write . row_id ) ) ;", ""),
    ("truncate_version_link", "publish_prepared_write_set", "driver . store_next ( new_row , write . base_offset , Ghost ( write . row_id ) ) ;", "driver.store_next(new_row,0,Ghost(write.row_id));"),
    ("omit_head_publication", "publish_prepared_write_set", "driver . compare_head ( slot , write . base_offset , write . new_offset )", "Ok::<(),Error>(())"),
    ("publish_empty_head", "publish_prepared_write_set", "driver . compare_head ( slot , write . base_offset , write . new_offset )", "driver.compare_head(slot,write.base_offset,0)"),
    ("wrong_head_expectation", "publish_prepared_write_set", "driver . compare_head ( slot , write . base_offset , write . new_offset )", "driver.compare_head(slot,write.new_offset,write.new_offset)"),
    ("skip_publication_loop", "publish_prepared_write_set", "while i<record.writes.len()", "while i<0"),
    ("omit_publication_authority", "publish_prepared_write_set", "        old(driver).authorized(record.writes[0].row_id,record.writes[0].new_offset),", ""),
    ("omit_snapshot_writer_invisibility", "publication_preserves_old_snapshot", "!lookup::creator_visible(writer,tx),writer>=tx.xmax || tx.active.contains(writer),", "writer>=tx.xmax || tx.active.contains(writer),"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.CONTRACTS, generate.OUTPUT, Path(generate.__file__),
        Path(__file__), HERE/"test_generate.py", generate.lookup.CONTRACTS,
        generate.lookup.HISTORY, generate.lookup.OUTPUT, Path(generate.lookup.__file__),
        ROOT/"verification/concurrent/generate.py", ROOT/"verification/predicate_capture/generate.py",
        ROOT/"verification/predicate_capture/contracts.rs", ROOT/"aerostore_verified/src/lib.rs", PIN]


def mutation(source, item):
    name, root, old, new = item
    if name in {"omit_snapshot_writer_invisibility", "omit_publication_authority"}:
        begin=source.index("pub proof fn publication_preserves_old_snapshot") if name=="omit_snapshot_writer_invisibility" else source.index("pub fn publish_prepared_write_set")
        end=source.index("pub proof fn successful_publication_connects_lookup",begin) if name=="omit_snapshot_writer_invisibility" else source.index("pub proof fn image_head_present",begin)
        part=source[begin:end]
        if part.count(old)!=1: raise RuntimeError("missing precondition mutation: "+name)
        return source[:begin]+part.replace(old,new,1)+source[end:]
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
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/row-publication")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_single_prepared_row_publication",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "whole_storage_history_refinement_proved": False, "native_reclamation_refinement_proved": False, "transaction_history_refinement_proved": False, "multirow_publication_refinement_proved": False, "unprepared_publication_refinement_proved": False}
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
            raise RuntimeError("stale generated row publication")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_row_publication", "--crate-type=lib",
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
                raise RuntimeError("row publication proof failed: " + name)

        invoke("native_publication", generate.OUTPUT)
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
