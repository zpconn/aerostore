#!/usr/bin/env python3
"""Isolated native acquisition/materialization schedules and semantic mutants.

This is deterministic schedule testing, not formal refinement. Every variant
gets its own archived source tree and Cargo target directory; compiler errors,
timeouts and signals never count as successful negative evidence.
"""
from pathlib import Path
import argparse
import hashlib
import io
import json
import os
import re
import signal
import subprocess
import tarfile
import time

ROOT = Path(__file__).resolve().parents[2]
OCC = "aerostore_core/src/occ_partitioned.rs"
PROC = "aerostore_core/src/procarray.rs"
CRATES = ["aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl"]
PINNED_RUST = "01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf"
CAPTURE = "captured_lookup_retains_history_through_delete_aba_and_own_write_overlay"
ACQUISITION = "lifecycle_acquisition_reobserves_reused_slots_and_preserves_owned_registration"


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def digest(path):
    return digest_bytes(path.read_bytes())


def scoped(source, start, end, old, new):
    a = source.index(start)
    b = source.index(end, a)
    body = source[a:b]
    if body.count(old) != 1:
        raise RuntimeError("missing/ambiguous native mutation anchor: " + old)
    return source[:a] + body.replace(old, new, 1) + source[b:]


def variants(occ, proc):
    begin = "    pub fn create_transaction_snapshot("
    end = "\n    /// Oldest pinned"
    stale = scoped(proc, begin, end,
        "        #[cfg(test)]\n        SNAPSHOT_ACQUIRING_HOOK", 
        "        let api_entry_snapshot = self.snapshot_locked(global_txid);\n        #[cfg(test)]\n        SNAPSHOT_ACQUIRING_HOOK")
    stale = scoped(stale, begin, end, "let snapshot = self.snapshot_locked(global_txid);", "let snapshot = api_entry_snapshot;")
    yield "use_preacquisition_snapshot", PROC, stale, ACQUISITION, "snapshot must use metadata acquired after the wait"
    lost = scoped(proc, begin, end, "slot.snapshot_xmin.store(snapshot.xmin, Ordering::Release);", "")
    yield "omit_retained_snapshot_horizon", PROC, lost, CAPTURE, "captured candidates must materialize the pinned historical predicate"
    visibility = scoped(occ, "    fn is_visible(", "\n    fn record_read(",
        "row.xmin >= tx.snapshot_xmax || tx.snapshot_active.contains(&row.xmin)", "row.xmin >= tx.snapshot_xmax")
    yield "ignore_older_active_creator", OCC, visibility, CAPTURE, "captured candidates must materialize the pinned historical predicate"
    overlay = scoped(occ, "    pub fn index_lookup(", "\n    fn validate_index_bindings(",
        "        candidates.extend(tx.write_set.iter().map(|write| write.row_id));\n", "")
    yield "omit_own_write_candidates", OCC, overlay, CAPTURE, "final own writes must remove and add raw candidates"
    hold = scoped(occ, "    pub fn index_lookup(", "\n    fn validate_index_bindings(",
        "        drop(guards);", "        let _delayed_guards = guards;")
    yield "retain_guards_during_materialization", OCC, hold, CAPTURE, "captured candidate materialization must not retain predicate guards"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lookup-native")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("choose an empty output directory to preserve previous evidence")
    receipt = {"schema": 1, "scope": "native_lookup_acquisition_materialization_regressions",
        "formal_refinement_proved": False, "passed": False, "status": "running", "checks": []}
    receipt_path = output / "receipt.json"

    def save():
        temporary = receipt_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(receipt_path)

    save()
    try:
        paths = [ROOT / OCC, ROOT / PROC, Path(__file__)]
        initial_hashes = {str(p.relative_to(ROOT)): digest(p) for p in paths}
        receipt["input_sha256"] = initial_hashes
        native = {OCC: (ROOT / OCC).read_bytes(), PROC: (ROOT / PROC).read_bytes()}
        receipt["parent_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        archive = subprocess.check_output(["git", "archive", receipt["parent_commit"], "Cargo.toml", "Cargo.lock", *CRATES], cwd=ROOT)
        (output / "parent-crates.tar").write_bytes(archive)
        receipt["parent_archive_sha256"] = digest_bytes(archive)
        cargo = Path(subprocess.check_output(["rustup", "which", "--toolchain", "stable", "cargo"], text=True).strip())
        rustc = cargo.with_name("rustc")
        version = subprocess.check_output([str(rustc), "--version", "--verbose"], text=True)
        if "commit-hash: " + PINNED_RUST not in version:
            raise RuntimeError("native Rust toolchain differs from reviewed compiler")
        receipt["rustc"] = version
        receipt["tool_sha256"] = {str(cargo): digest(cargo), str(rustc): digest(rustc)}
        env = dict(os.environ)
        for key in list(env):
            if key.startswith(("AEROSTORE_", "CARGO_PROFILE_")) or key in {
                "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTDOC", "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTFLAGS", "CARGO_BUILD_RUSTC_WRAPPER",
                "CARGO_BUILD_TARGET", "RUSTUP_TOOLCHAIN", "CARGO_TARGET_DIR",
            } or (key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS")):
                env.pop(key)
        env.update(RUSTC=str(rustc), RUSTUP_TOOLCHAIN="stable-x86_64-unknown-linux-gnu", CARGO_ENCODED_RUSTFLAGS="")

        def source_tree(name, changed=None):
            directory = output / name / "source"
            directory.mkdir(parents=True)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                tar.extractall(directory, filter="data")
            for path, content in native.items():
                (directory / path).write_bytes(content)
            if changed:
                (directory / changed[0]).write_text(changed[1])
            return directory

        def invoke(name, source, selection, expected_failure=False, required_assertion=None):
            command = [str(cargo), "test", "--offline", "--locked", "--target-dir", str(source.parent / "cargo-target"),
                "-p", "aerostore_core", *selection, "--", "--nocapture"]
            started = time.monotonic()
            process = subprocess.Popen(command, cwd=source, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, start_new_session=True)
            try:
                log = process.communicate(timeout=180)[0]
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                log = process.communicate()[0]
                (output / (name + ".log")).write_text(log)
                raise
            log_path = output / (name + ".log")
            log_path.write_text(log)
            count = re.search(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed", log)
            passed = (process.returncode == 0 and count and count[1] == "ok" and int(count[2]) > 0)
            if expected_failure:
                passed = (process.returncode == 101 and count and count[1] == "FAILED" and int(count[3]) > 0
                    and "panicked at" in log and required_assertion in log and not re.search(r"error\[E\d+\]", log))
            check = {"name": name, "command": command, "cwd": str(source), "exit_code": process.returncode,
                "elapsed_seconds": time.monotonic() - started, "expected_assertion_failure": expected_failure,
                "required_assertion": required_assertion, "passed": bool(passed), "log": str(log_path.relative_to(ROOT)),
                "log_sha256": digest(log_path), "source_sha256": {p: digest(source / p) for p in native}}
            receipt["checks"].append(check)
            save()
            print(json.dumps({"check": name, "passed": bool(passed), "exit_code": process.returncode}), flush=True)
            if not passed:
                raise RuntimeError("native schedule or negative-control assertion failed: " + name)

        baseline = source_tree("current")
        invoke("current_acquisition_interference", baseline, ["--lib", ACQUISITION])
        invoke("current_captured_historical_overlay", baseline, ["--lib", CAPTURE])
        for name, path, changed, test, assertion in variants(native[OCC].decode(), native[PROC].decode()):
            source = source_tree(name, (path, changed))
            invoke(name, source, ["--lib", test], True, assertion)
        receipt["final_input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in paths}
        if receipt["final_input_sha256"] != initial_hashes:
            raise RuntimeError("native campaign sources changed during execution")
        receipt.update(passed=True, status="passed", source_stable=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
    save()
    print(json.dumps({"passed": receipt["passed"], "receipt": str(receipt_path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
