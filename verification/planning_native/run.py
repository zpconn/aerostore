#!/usr/bin/env python3
"""Native final-write planning scenarios and assertion-failing semantic mutants.

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
CRATES = ["aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl"]
PINNED_RUST = "01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf"
INTEGRATION = "aerostore_core/tests/occ_transactional_index.rs"
FINAL = "planning_final_writes_preserve_exact_report_after_savepoint_rollback"
UNCHANGED = "planning_unchanged_key_keeps_posting_and_empty_predicate_valid"
STALE = "planning_stale_base_rejects_all_staged_rows_before_publication"
TESTS = [
    ("current_final_write_report", ["--test", "occ_transactional_index", FINAL]),
    ("current_unchanged_key", ["--test", "occ_transactional_index", UNCHANGED]),
    ("current_stale_write_base", ["--test", "occ_transactional_index", STALE]),
    ("current_nested_savepoint_abort", ["--test", "occ_transactional_index", "own_writes_nested_savepoints_and_abort_preserve_exact_index_state"]),
]

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


def variants(occ):
    final, final_end = "    fn final_write_indices(", "\n    fn recycle_non_final_writes("
    changes, changes_end = "    fn index_changes(", "\n    fn index_lock_keys("
    publish, publish_end = "    fn publish_write_set(", "\n    fn has_write_base_conflict("
    report_assertion = "planning-native: report must contain exactly each row's last retained write and original base"
    index_assertion = "planning-native: only original-to-final key changes may affect postings"
    def control(name, start, end, old, new, test=FINAL, assertion=report_assertion):
        return name, OCC, scoped(occ, start, end, old, new), ["--test", "occ_transactional_index", test], assertion
    yield control("select_first_write_per_row", final, final_end,
        "by_row.insert(write.row_id, idx);", "by_row.entry(write.row_id).or_insert(idx);")
    yield control("omit_first_selected_row", final, final_end,
        "by_row.into_values().collect()", "by_row.into_values().skip(1).collect()")
    yield control("derive_before_key_from_new_pointer", changes, changes_end,
        "let before = &self.resolve_row_ptr(&write.base_ptr)?.value;",
        "let before = &self.resolve_row_ptr(&write.new_ptr)?.value;", assertion=index_assertion)
    yield control("derive_after_key_from_base_pointer", changes, changes_end,
        "let after = &self.resolve_row_ptr(&write.new_ptr)?.value;",
        "let after = &self.resolve_row_ptr(&write.base_ptr)?.value;", assertion=index_assertion)
    yield control("emit_unchanged_key_change", changes, changes_end,
        "if before == after {", "if false {", test=UNCHANGED,
        assertion="planning-native: unchanged final key must retain exactly the original posting")
    yield control("bypass_write_base_validation", "    fn commit_with_record_impl<", "\n    fn prepare_before_publish<",
        "|| self.has_write_base_conflict(tx, &final_write_indices)?", "|| false", test=STALE,
        assertion="planning-native: stale base must reject before publishing any staged row")
    yield control("report_wrong_base_offset", publish, publish_end,
        "                base_offset,\n", "                base_offset: new_offset,\n")
    yield control("report_wrong_new_offset", publish, publish_end,
        "                new_offset,\n", "                new_offset: base_offset,\n")
    yield control("report_wrong_base_value", publish, publish_end,
        "                base_value,\n", "                base_value: new_row.value,\n")
    yield control("report_wrong_final_value", publish, publish_end,
        "                value: new_row.value,\n", "                value: base_value,\n")
    yield control("omit_report_dirty_mask", publish, publish_end,
        "                dirty_columns_bitmask: write.dirty_columns_bitmask,\n",
        "                dirty_columns_bitmask: 0,\n")


def native_paths():
    paths = {ROOT / "Cargo.toml", ROOT / "Cargo.lock"}
    for crate in CRATES:
        paths.add(ROOT / crate / "Cargo.toml")
        paths.update((ROOT / crate).rglob("*.rs"))
    return sorted(paths)


def outcome_passes(returncode, log, expected_failure=False, required_assertion=None):
    count = re.search(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed", log)
    if not count or re.search(r"error\[E\d+\]", log):
        return False
    if expected_failure:
        return bool(returncode == 101 and count[1] == "FAILED" and count[2] == "0" and count[3] == "1"
            and "panicked at" in log and required_assertion and required_assertion in log)
    return returncode == 0 and count[1] == "ok" and count[2] == "1" and count[3] == "0"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/planning-native")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("evidence must be below target/")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("choose an empty output directory to preserve previous evidence")
    receipt = {"schema": 1, "scope": "native_final_write_planning_scenarios",
        "formal_refinement_proved": False, "full_P1_complete": False, "passed": False, "status": "running", "checks": []}
    receipt_path = output / "receipt.json"

    def save():
        temporary = receipt_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(receipt_path)

    save()
    try:
        paths = native_paths() + [Path(__file__), Path(__file__).with_name("test_run.py"), Path(__file__).with_name("README.md")]
        initial_hashes = {str(p.relative_to(ROOT)): digest(p) for p in paths}
        receipt["input_sha256"] = initial_hashes
        native = {str(path.relative_to(ROOT)): path.read_bytes() for path in native_paths()}
        receipt["required_positive_checks"] = [name for name, _ in TESTS]
        receipt["required_mutations"] = [variant[0] for variant in variants(native[OCC].decode())]
        receipt["parent_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        archive = subprocess.check_output(["git", "archive", receipt["parent_commit"], "Cargo.toml", "Cargo.lock", *CRATES], cwd=ROOT)
        (output / "parent-crates.tar").write_bytes(archive)
        receipt["parent_archive_sha256"] = digest_bytes(archive)
        selected_toolchain = os.environ.get("RUSTUP_TOOLCHAIN", "1.93.1")
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
                (directory / path).parent.mkdir(parents=True, exist_ok=True)
                (directory / path).write_bytes(content)
            if changed:
                (directory / changed[0]).write_text(changed[1])
            return directory

        def invoke(name, source, selection, expected_failure=False, required_assertion=None):
            command = [str(cargo), "test", "--offline", "--locked", "--target-dir", str(source.parent / "cargo-target"),
                "-p", "aerostore_core", *selection, "--", "--test-threads=1", "--nocapture"]
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
            passed = outcome_passes(process.returncode, log, expected_failure, required_assertion)
            binaries = {}
            for binary in re.findall(r"Running .*? \(([^)]+)\)", log):
                binary_path = Path(binary)
                if not binary_path.is_absolute():
                    binary_path = source / binary_path
                if binary_path.is_file():
                    binaries[str(binary_path)] = digest(binary_path)
            check = {"name": name, "command": command, "cwd": str(source), "exit_code": process.returncode,
                "elapsed_seconds": time.monotonic() - started, "expected_assertion_failure": expected_failure,
                "required_assertion": required_assertion, "passed": bool(passed), "binary_sha256": binaries, "log": str(log_path.relative_to(ROOT)),
                "log_sha256": digest(log_path), "source_sha256": {p: digest(source / p) for p in native}}
            receipt["checks"].append(check)
            save()
            print(json.dumps({"check": name, "passed": bool(passed), "exit_code": process.returncode}), flush=True)
            if not passed:
                raise RuntimeError("native schedule or negative-control assertion failed: " + name)

        baseline = source_tree("current")
        for name, selection in TESTS:
            invoke(name, baseline, selection)
        for name, path, changed, test, assertion in variants(native[OCC].decode()):
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
