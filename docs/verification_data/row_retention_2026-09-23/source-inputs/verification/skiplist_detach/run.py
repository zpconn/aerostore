#!/usr/bin/env python3
"""Fresh source-bound native detach-loop proofs and semantic negative controls."""
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
ROOTS = ["cached_detach", "refreshed_detach", "detach_before_retire", "primitive_contracts_have_success_and_retry_witnesses"]
MUTATIONS = [
    ("missing_upper_lane", "for level in (0..height).rev()", "for level in (0..1).rev()", "cached_detach"),
    ("retire_after_missing_successor", "if succs[level] != node_offset {\n                detached_all = false;", "if succs[level] != node_offset {", "cached_detach"),
    ("retire_after_failed_cas", "                detached_all = false;\n                break;\n            }\n        }", "                break;\n            }\n        }", "cached_detach"),
    ("fallback_short_circuit", "if detached_all {\n                break;", "if true {\n                break;", "detach_before_retire"),
]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.CONTRACTS, generate.OUTPUT,
            Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
            ROOT / "verification/concurrent/generate.py", PIN]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/skiplist-detach")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_skiplist_detachment",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "native_unsafe_heap_refinement_proved": False, "epoch_reclamation_refinement_proved": False, "retry_termination_proved": False}
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
        source = generate.SOURCE.read_text()
        if generate.OUTPUT.read_text() != generate.render(source):
            raise RuntimeError("stale generated detachment operation")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_detach", "--crate-type=lib",
            "--edition=2021", "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent", "--rlimit", "40"]

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
                    or not re.search(r"(?:precondition|postcondition|invariant) not satisfied", log)):
                    raise RuntimeError("negative control failed for wrong reason: " + name)
            elif process.returncode != 0 or not summary or int(summary[2]) != 0 or int(summary[1]) < (1 if root else len(ROOTS)):
                raise RuntimeError("detach proof failed: " + name)

        invoke("native_detach", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root, generate.OUTPUT, root)
        begin = source.index("        let mut detached_all = true;", source.index("    fn unlink_node("))
        end = source.index("\n    fn decrement_distinct_key_count(", begin)
        selected = source[begin:end]
        for name, old, new, proof_root in MUTATIONS:
            expected = 2 if name == "missing_upper_lane" else 1
            if selected.count(old) != expected:
                raise RuntimeError("mutation anchor absent or ambiguous: " + name)
            mutant = source[:begin] + selected.replace(old, new, 1) + source[end:]
            artifact = output / (name + ".rs")
            artifact.write_text(generate.render(mutant))
            invoke(name, artifact, proof_root, True)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("detach proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
