#!/usr/bin/env python3
"""Source-bound guard ownership, interference, semantic mutants and affine misuse controls."""
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
ROOTS = ['grant', 'revoke', 'try_acquire', 'drop_lease', 'step_does_not_resurrect', 'no_resurrection_transitive', 'step_preserves_protected', 'trace_preserves_protected', 'environment', 'borrowed_leases_define_protection', 'protected_borrows_remain_authorized', 'protection_can_shrink', 'protected_key_authorizes', 'acquire_preserving_borrows', 'drop_preserving_borrows', 'try_lock', 'read_permission', 'competing_acquire_is_excluded', 'release_then_handoff', 'other_mutex_preserves_leases', 'initialize_authority', 'physical_keys_are_distinct', 'domain_survives_interference', 'bucket_reply_has_witness', 'physical_domain_has_live_witness', 'transactional_try_lock_bucket', 'acquire_index_bucket', 'authorized_survives_other_key', 'release_all', 'ownership_has_live_handoff_witness', 'interleaved_step_preserves_lease', 'interleaved_trace_preserves_lease', 'environment_for_borrows', 'foreign_release_between_attempts', 'rely_allows_foreign_release_and_retains_local_owner', 'rely_allows_new_owner_after_release_without_resurrecting_old_lease']
MUTATIONS = [
    ("omit_physical_domain", "template:transactional_try_lock_bucket", "transactional_try_lock_bucket",
     "physical_domain(driver, old(authority)),", "true,"),
    ("cas_when_locked", "source:try_acquire", "try_acquire", "compare_exchange(0, 1,", "compare_exchange(1, 1,"),
    ("cas_does_not_lock", "source:try_acquire", "try_acquire", "compare_exchange(0, 1,", "compare_exchange(0, 0,"),
    ("cas_success_relaxed", "source:try_acquire", "try_acquire", "Ordering::Acquire", "Ordering::Relaxed"),
    ("guard_from_failed_cas", "source:try_acquire", "try_acquire", ".ok()", ".err()"),
    ("drop_leaves_locked", "source:drop", "drop_lease", "store(0, Ordering::Release)", "store(1, Ordering::Release)"),
    ("drop_relaxed", "source:drop", "drop_lease", "Ordering::Release", "Ordering::Relaxed"),
    ("forget_priority_guard", "source:try_lock", "try_lock", "drop(guard);", "std::mem::forget(guard);"),
    ("wrong_selected_bucket", "index:transactional_try_lock_bucket", "transactional_try_lock_bucket", ".get(bucket)", ".get(0)"),
    ("wrong_guard_identity", "generated:try_acquire", "try_acquire", "physical: key, token:", "physical: (0, 0, 0), token:"),
    ("wrong_release_identity", "generated:drop_lease", "drop_lease", "driver.store(physical,", "driver.store((0, 0, 0),"),
    ("reuse_grant_serial", "template:grant", "grant", "authority.next = serial + 1;", "authority.next = serial;"),
    ("omit_caller_protection", "generated:try_lock", "try_lock", "environment(driver, Ghost(protected),", "environment(driver, Ghost(Set::empty()),"),
    ("omit_local_protection", "generated:try_lock", "try_lock", "Ghost ( with_local )", "Ghost ( protected )"),
    ("skip_release_all", "template:release_all", "release_all", "drop_lease(driver, lease, Tracked(&mut *authority));", "let _forgotten = lease;"),
    ("freeze_foreign_state", "template:legal_environment", "rely_allows_foreign_release_and_retains_local_owner",
     "exists|w: (Seq<Authority>, Seq<Step>)| environment_trace(before, after, protected, w)",
     "cells(before) == cells(after) && exists|w: (Seq<Authority>, Seq<Step>)| environment_trace(before, after, protected, w)"),
    ("allow_release_of_borrowed_lease", "template:environment_trace", "environment",
     "!releases(w.1[i], id.0, id.1)", "releases(w.1[i], id.0, id.1) || !releases(w.1[i], id.0, id.1)"),
]
TYPE_MUTATIONS = {
    "double_drop": ("E0382", """pub fn misuse<D: ownership::Atomics>(driver: &D, lease: ownership::ReadLease,
        Tracked(authority): Tracked<&mut ownership::Authority>)
        requires ownership::valid(old(authority)), ownership::authorized(old(authority), &lease),
    {
        ownership::drop_lease(driver, lease, Tracked(&mut *authority));
        ownership::drop_lease(driver, lease, Tracked(&mut *authority));
    }"""),
    "copy_borrowed_lease": ("E0507", """pub fn misuse(lease: &ownership::ReadLease) -> ownership::ReadLease { *lease }"""),
    "mint_second_authority": ("E0603", """pub proof fn misuse() {
        let tracked authority = ownership::initialize_authority(Set::empty());
    }"""),
    "forge_numeric_lease": ("disallowed: constructor for an opaque datatype", """pub fn misuse(key: ownership::Key, Tracked(token): Tracked<ownership::LeaseToken>)
        -> ownership::ReadLease {
        ownership::ReadLease { physical: key, token: Tracked(token) }
    }"""),
}


def scoped_replace(text, method, old, new):
    match = re.search(r"(?:pub (?:open |closed )?)?(?:proof |spec )?fn " + re.escape(method) + r"(?:<[^\n]*>)?\(", text)
    if not match:
        raise ValueError("missing mutation method: " + method)
    end = re.search(r"\n(?:    )?(?:pub(?:\([^\n]*\))? (?:open |closed )?)?(?:proof |spec )?fn ", text[match.end():])
    stop = match.end() + end.start() if end else len(text)
    selected = text[match.start():stop]
    if selected.count(old) != 1:
        raise ValueError("mutation anchor absent/ambiguous: " + method + ": " + old)
    return text[:match.start()] + selected.replace(old, new, 1) + text[stop:]


def mutation_artifact(mutation):
    name, target, root, old, new = mutation
    kind, method = target.split(":")
    paths = {"source": generate.SOURCE, "index": generate.INDEX, "occ": generate.OCC,
             "template": generate.CONTRACTS}
    if kind == "generated":
        return scoped_replace(generate.render(), method, old, new)
    return generate.render(**{kind: scoped_replace(paths[kind].read_text(), method, old, new)})


def type_artifact(name):
    return "mod ownership {\n" + generate.render() + "\n}\nuse vstd::prelude::*;\nverus! {\n" + TYPE_MUTATIONS[name][1] + "\n}\n"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    return [generate.SOURCE, generate.INDEX, generate.OCC, generate.CONTRACTS, generate.OUTPUT,
        Path(generate.__file__), Path(__file__), HERE / "test_generate.py",
        ROOT / "verification/guards/generate.py", ROOT / "verification/concurrent/generate.py", PIN]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/guard-ownership")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": 1, "passed": False, "status": "running", "checks": [],
        "scope": "conditional_native_atomic_guard_ownership_and_interference",
        "required_roots": ROOTS, "required_mutations": [m[0] for m in MUTATIONS],
        "required_type_mutations": {n: v[0] for n, v in TYPE_MUTATIONS.items()}, "type_checks": [],
        "whole_lookup_refinement_proved": False, "transaction_history_refinement_proved": False,
        "native_weak_memory_refinement_proved": False, "native_arena_initialization_refinement_proved": False,
        "unbounded_progress_proved": False}
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
            raise RuntimeError("stale generated guard ownership proof")
        env = dict(os.environ, RUSTUP_HOME=str(ROOT / pin["rustup_home"]),
            RUSTUP_TOOLCHAIN=pin["rust_toolchain"], VERUS_Z3_PATH=str(distribution / "z3"))
        base = [str(distribution / "verus"), "--crate-name", "aerostore_guard_ownership", "--crate-type=lib",
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
                raise RuntimeError("guard ownership proof failed: " + name)

        invoke("native_guard_ownership", generate.OUTPUT)
        for root in ROOTS:
            invoke("root_" + root.replace("::", "_"), generate.OUTPUT, root)
        for mutation in MUTATIONS:
            name, _, root, _, _ = mutation
            artifact = output / (name + ".rs")
            artifact.write_text(mutation_artifact(mutation))
            invoke(name, artifact, root, True)
        for name, (diagnostic, _) in TYPE_MUTATIONS.items():
            artifact = output / (name + ".rs")
            artifact.write_text(type_artifact(name))
            command = base + [str(artifact)]
            started = time.monotonic()
            process = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
            log = process.stdout + process.stderr
            log_path = output / (name + ".log")
            log_path.write_text(log)
            check = {"name": name, "command": command, "exit_code": process.returncode,
                "elapsed_seconds": time.monotonic() - started, "source_sha256": digest(artifact),
                "log": str(log_path.relative_to(ROOT)), "log_sha256": digest(log_path),
                "classification": "ownership_type_rejection", "expected_diagnostic": diagnostic}
            receipt["type_checks"].append(check)
            save()
            pattern = r"error\[" + re.escape(diagnostic) + r"\]" if re.fullmatch(r"E[0-9]{4}", diagnostic) else re.escape(diagnostic)
            if process.returncode == 0 or not re.search(pattern, log):
                raise RuntimeError("ownership misuse failed for wrong reason: " + name)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in inputs()}
        if receipt["final_input_sha256"] != fingerprints:
            raise RuntimeError("guard ownership proof inputs changed during verification")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
