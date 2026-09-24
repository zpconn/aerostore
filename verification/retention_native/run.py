#!/usr/bin/env python3
"""Isolated native publication/retention/reuse schedules and semantic mutants.

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
VACUUM = "aerostore_core/src/vacuum.rs"
CRATES = ["aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl"]
PINNED_RUST = "01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf"
INTEGRATION = "aerostore_core/tests/occ_transactional_index.rs"
PARTIAL = "retention_native_partial_publication_retains_snapshot_until_reuse"
CURSOR = "retention_native_loaded_cursor_survives_pruned_tail_reuse"
LOCKED = "row_guard_outliving_commit_pins_its_version_without_unlocking_a_later_owner"
OVERLAP = "newer_overlapping_reader_does_not_inherit_an_obsolete_vacuum_horizon"
PUBLIC_HORIZON = "public_vacuum_clamps_caller_horizon_to_retained_snapshot"
TESTS = [("current_partial_publication", ["--lib", PARTIAL]),
         ("current_loaded_cursor", ["--lib", CURSOR]),
         ("current_guard_retention", ["--test", "occ_transactional_index", LOCKED]),
         ("current_horizon_progress", ["--test", "occ_transactional_index", OVERLAP]),
         ("current_public_horizon", ["--test", "occ_transactional_index", PUBLIC_HORIZON])]


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
    start, end = "    pub(crate) fn vacuum_reclaim_before(", "\nstruct PartitionLockGuard"
    horizon = scoped(occ, start, end, "xmax < global_xmin", "true")
    yield "ignore_retention_horizon", OCC, horizon, ["--lib", CURSOR], "retention-native: vacuum may prune only below the retained visible anchor"
    missing = scoped(proc, "    pub fn oldest_snapshot_xmin(", "\n    fn snapshot_locked(",
        "slot.snapshot_xmin.load(Ordering::Acquire)", "slot.load(Ordering::Acquire)")
    yield "use_registration_instead_of_snapshot_horizon", PROC, missing, ["--lib", PARTIAL], "retention-native: older active writer horizon must retain both bases"
    unlink = scoped(occ, start, end, "prev_row.next.store(next_offset, Ordering::Release);", "")
    yield "recycle_without_unlinking", OCC, unlink, ["--lib", CURSOR], "retention-native: vacuum must unlink a retired tail before reuse"
    stale = scoped(occ, "    fn initialize_row(", "\n    fn resolve_row_ptr_raw(",
        "std::ptr::write(row_mut, OccRow::new(value, xmin, next));",
        "let stale_xmax = (*row_mut).xmax.load(Ordering::Acquire);\n            std::ptr::write(row_mut, OccRow::new(value, xmin, next));\n            (*row_mut).xmax.store(stale_xmax, Ordering::Release);")
    yield "preserve_retired_xmax_on_reuse", OCC, stale, ["--lib", CURSOR], "retention-native: reused storage must clear retired deletion metadata"
    locked = scoped(occ, start, end, "&& !curr_row.is_locked.load(Ordering::Acquire)", "")
    yield "reclaim_version_with_live_row_guard", OCC, locked, ["--test", "occ_transactional_index", LOCKED], "a guard still refers to the retired row even after its transaction commits"
    inherit = scoped(proc, "    fn snapshot_locked(", "\n    /// Startup recovery only:",
        "xmin = xmin.min(txid);", "xmin = xmin.min(slot.snapshot_xmin.load(Ordering::Relaxed));")
    yield "inherit_obsolete_horizon", PROC, inherit, ["--test", "occ_transactional_index", OVERLAP], "assertion failed: !aerostore_core::run_vacuum_pass(&table).unwrap().is_empty()"
    active = scoped(occ, "    fn is_visible(", "\n    fn record_read(",
        "row.xmin >= tx.snapshot_xmax || tx.snapshot_active.contains(&row.xmin)", "row.xmin >= tx.snapshot_xmax")
    yield "ignore_active_creator_during_partial_publication", OCC, active, ["--lib", PARTIAL], "retention-native: partial publication must expose one old snapshot"

    unclamped = scoped(occ, "    pub fn vacuum_reclaim_once(", "    pub(crate) fn vacuum_reclaim_before(",
        "requested_xmin.min(retained_xmin)", "requested_xmin")
    yield "omit_public_horizon_clamp", OCC, unclamped, ["--test", "occ_transactional_index", PUBLIC_HORIZON], "a caller must not advance the retained horizon"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/retention-native")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("choose an empty output directory to preserve previous evidence")
    receipt = {"schema": 1, "scope": "native_publication_retention_reuse_regressions",
        "formal_refinement_proved": False, "passed": False, "status": "running", "checks": []}
    receipt_path = output / "receipt.json"

    def save():
        temporary = receipt_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(receipt_path)

    save()
    try:
        paths = [ROOT / OCC, ROOT / PROC, ROOT / VACUUM, ROOT / INTEGRATION, Path(__file__), Path(__file__).with_name("test_run.py")]
        initial_hashes = {str(p.relative_to(ROOT)): digest(p) for p in paths}
        receipt["input_sha256"] = initial_hashes
        native = {path: (ROOT / path).read_bytes() for path in (OCC, PROC, VACUUM, INTEGRATION)}
        receipt["parent_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        archive = subprocess.check_output(["git", "archive", receipt["parent_commit"], "Cargo.toml", "Cargo.lock", *CRATES], cwd=ROOT)
        (output / "parent-crates.tar").write_bytes(archive)
        receipt["parent_archive_sha256"] = digest_bytes(archive)
        selected_toolchain = os.environ.get("RUSTUP_TOOLCHAIN", "stable")
        cargo = Path(subprocess.check_output(["rustup", "which", "--toolchain", selected_toolchain, "cargo"], text=True).strip())
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
        env.update(RUSTC=str(rustc), RUSTUP_TOOLCHAIN=selected_toolchain, CARGO_ENCODED_RUSTFLAGS="")

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
                log = process.communicate(timeout=240)[0]
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
        for name, selection in TESTS:
            invoke(name, baseline, selection)
        for name, path, changed, test, assertion in variants(native[OCC].decode(), native[PROC].decode()):
            source = source_tree(name, (path, changed))
            invoke(name, source, test, True, assertion)
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
