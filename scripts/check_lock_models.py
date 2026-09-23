#!/usr/bin/env python3
"""Bounded checks of the production lock, with fresh artifacts and a sync mutant.

This is not a weak-memory/mmap refinement or an unbounded progress proof.
Every required case runs against the actual imported production lock. Temporary
source copies are disposable; logs, binaries, hashes and the mutation survive.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import difflib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]
LOCK = "aerostore_core/src/shm_lock.rs"
MODELS = "aerostore_core/tests/shm_mutation_model.rs"
CASES = (
    "insertion_deletion_reader_and_gc_preserve_reachability_and_ownership",
    "row_guard_preserves_commit_and_index_order",
    "failed_prepublication_allocations_are_returned_before_unlock",
    "model_detects_the_original_unprotected_predecessor_race",
    "model_detects_missing_row_guard_even_with_serialized_index_moves",
    "shm_lock::loom_tests::contended_handoff_publishes_protected_non_atomic_value",
    "shm_lock::loom_tests::registered_priority_waiter_precedes_ordinary_contender",
)
EXPECTED_COUNTEREXAMPLES = {
    CASES[3]: "detached predecessor loses insertion",
    CASES[4]: "row and index publication reordered",
}
MUTANT_CASE = CASES[5]
MUTANT_NAME = "relaxed_success_cas"
MUTANT_OLD = ".compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)"
MUTANT_NEW = ".compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)"
BOUNDS = {"preemption_bound": 2, "max_branches": 10000,
          "max_permutations": None, "max_duration": None}


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def source_files(root: Path) -> list[Path]:
    files = [root / "Cargo.toml", root / "Cargo.lock"]
    manifest = tomllib.loads(files[0].read_text())
    for member in manifest["workspace"]["members"]:
        directory = (root / member).resolve()
        require(directory.is_relative_to(root) and directory.is_dir(), "unsupported workspace member")
        for parent, dirs, names in os.walk(directory):
            dirs[:] = sorted(name for name in dirs if name not in {"target", ".git", ".lake", "__pycache__"})
            require(all(not (Path(parent) / name).is_symlink() for name in dirs), "source directory symlink is unsupported")
            for name in sorted(names):
                path = Path(parent) / name
                require(not path.is_symlink(), "source file symlink is unsupported: " + str(path))
                if path.is_file():
                    files.append(path)
    for name in ("config", "config.toml"):
        path = root / ".cargo" / name
        if path.exists():
            files.append(path)
    return sorted(set(files))


def fingerprints(root: Path) -> dict[str, str]:
    return {str(path.relative_to(root)): digest(path) for path in source_files(root)}


def check_model_shape(root: Path) -> None:
    models = (root / MODELS).read_text()
    require('#[path = "../src/shm_lock.rs"]\nmod shm_lock;' in models,
            "model must import the actual production lock")
    require("#![cfg(aerostore_loom)]" in models, "missing Loom-only model configuration")
    lock = (root / LOCK).read_text()
    require("#[cfg(all(test, aerostore_loom))]" in lock, "missing focused lock models")
    for text in (models, lock):
        require(len(re.findall(r"model\.preemption_bound\s*=\s*Some\(2\)\s*;", text)) == 1,
                "model preemption bound changed")
        require(len(re.findall(r"model\.max_branches\s*=\s*10_000\s*;", text)) == 1,
                "model branch bound changed")
        require(not re.search(r"\.(?:max_permutations|max_duration)\s*=", text),
                "early-stop model cutoff is forbidden")


def check_case_output(result: dict, case: str, *, mutant: bool = False) -> None:
    log = Path(result["log"]).read_text()
    require(re.search(r"^running 1 test$", log, re.MULTILINE)
            and re.search(r"^test " + re.escape(case) + r"(?: - should panic)? \.\.\.", log, re.MULTILINE),
            "required model did not run exactly once: " + case)
    if mutant:
        require(result["exit_code"] == 101
                and re.search(r"^test result: FAILED\. 0 passed; 1 failed; 0 ignored;", log, re.MULTILINE)
                and "Causality violation:" in log and "Concurrent write accesses to `UnsafeCell`" in log,
                "broken synchronization was not rejected for the intended Loom causality failure")
    else:
        require(result["exit_code"] == 0
                and re.search(r"^test result: ok\. 1 passed; 0 failed; 0 ignored;", log, re.MULTILINE),
                "required model did not complete: " + case)
        if case in EXPECTED_COUNTEREXAMPLES:
            require(EXPECTED_COUNTEREXAMPLES[case] in log, "wrong intended model counterexample")


def validate_receipt(path: Path, root: Path) -> dict:
    """Validate retained artifacts, not just a runner's success boolean.

    This detects incomplete/stale/substituted evidence. The frozen runner and
    trusted workflow remain necessary; a JSON receipt is not an execution proof.
    """
    try:
        receipt = json.loads(path.read_text())
        require(receipt["schema"] == 1 and receipt["status"] == "passed"
                and all(receipt[name] is True for name in
                        ("passed", "source_stable", "complete_campaign")), "incomplete lock-model campaign")
        require(receipt["scope"] == "bounded_production_lock_models"
                and receipt["native_weak_memory_refinement_proved"] is False
                and receipt["unbounded_progress_proved"] is False, "unsupported lock-model scope")
        require(receipt["bounds"] == BOUNDS and receipt["required_cases"] == list(CASES)
                and receipt["required_mutant"] == MUTANT_NAME, "lock-model cases or bounds changed")
        require(Path(receipt["source_root"]).resolve() == root.resolve()
                and Path(receipt["environment_root"]).resolve() == root.resolve(), "lock-model root mismatch")
        current = fingerprints(root)
        require(receipt["input_sha256"] == current == receipt["final_input_sha256"]
                and receipt["candidate_shm_lock_sha256"] == current[LOCK], "stale lock-model source")
        require(receipt["runner_sha256"] == digest(root / "scripts/check_lock_models.py")
                and receipt["environment_checker_sha256"] == digest(root / "scripts/verify_formal.py"),
                "stale lock-model runner")
        require(receipt["model_environment"]["RUSTFLAGS"] == "--cfg aerostore_loom", "wrong model configuration")
        require(len(receipt["tool_sha256"]) == 2 and
                all(digest(Path(name)) == value for name, value in receipt["tool_sha256"].items()),
                "compiler tool changed")
        check_model_shape(root)
        builds = receipt["builds"]
        require(len(builds) == 2 and len(receipt["listings"]) == 2, "missing fresh lock-model builds")
        binaries = []
        for build, listing in zip(builds, receipt["listings"]):
            executable = Path(build["executable"])
            require(build["exit_code"] == 0 and build["fresh_test_binary"] is True
                    and executable.resolve().is_relative_to(path.parent.resolve())
                    and digest(executable) == build["executable_sha256"], "missing or substituted model executable")
            command = build["command"]
            for flag in ("--offline", "--locked", "--release", "--no-run", "--message-format=json"):
                require(flag in command, "model build flags changed")
            require(command[command.index("--test") + 1] == "shm_mutation_model", "wrong model target")
            require(digest(Path(build["log"])) == build["log_sha256"], "model build log changed")
            artifacts = []
            for line in Path(build["log"]).read_text().splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if (event.get("reason") == "compiler-artifact" and event.get("executable")
                        and event.get("target", {}).get("name") == "shm_mutation_model"):
                    artifacts.append(event)
            require(len(artifacts) == 1 and artifacts[0].get("fresh") is False
                    and Path(artifacts[0]["executable"]) == executable
                    and Path(artifacts[0]["target"]["src_path"]) == Path(build["cwd"]) / MODELS,
                    "missing fresh compiler artifact for actual model source")
            require(listing["exit_code"] == 0 and listing["command"] == [str(executable), "--list"]
                    and digest(Path(listing["log"])) == listing["log_sha256"]
                    and set(re.findall(r"^(.+): test$", Path(listing["log"]).read_text(), re.MULTILINE)) == set(CASES),
                    "missing required model listing")
            binaries.append(build["executable_sha256"])
        require(binaries[0] != binaries[1]
                and Path(builds[0]["executable"]).parent != Path(builds[1]["executable"]).parent,
                "mutant reused the production executable or target")
        require(builds[0]["lock_sha256"] == current[LOCK]
                and all(build["model_sha256"] == current[MODELS] for build in builds), "compiled source mismatch")
        expected_bad = (root / LOCK).read_text().replace(MUTANT_OLD, MUTANT_NEW)
        require((root / LOCK).read_text().count(MUTANT_OLD) == 1
                and builds[1]["lock_sha256"] == receipt["mutant"]["source_sha256"]
                == hashlib.sha256(expected_bad.encode()).hexdigest(), "wrong synchronization mutant")
        require(receipt["mutant"]["name"] == MUTANT_NAME and receipt["mutant"]["case"] == MUTANT_CASE
                and receipt["mutant"]["patch_sha256"] == digest(path.parent / "relaxed-success-cas.patch"),
                "missing mutation identity")
        checks = receipt["checks"]
        require([check["name"] for check in checks] == [*CASES, MUTANT_NAME], "missing or duplicate lock-model check")
        for index, check in enumerate(checks):
            mutant = index == len(CASES)
            build = builds[int(mutant)]
            case = MUTANT_CASE if mutant else CASES[index]
            require(check["passed"] is True and check["executable_sha256"] == build["executable_sha256"]
                    and check["command"] == [build["executable"], "--exact", case, "--nocapture", "--test-threads=1"]
                    and digest(Path(check["log"])) == check["log_sha256"], "missing or substituted lock-model check")
            require(check.get("expected_failure") is True if mutant else
                    check.get("expected_counterexample") is (case in EXPECTED_COUNTEREXAMPLES),
                    "incorrect expected outcome")
            check_case_output(check, case, mutant=mutant)
        return receipt
    except (OSError, ValueError, KeyError, TypeError, IndexError, RuntimeError) as error:
        raise RuntimeError("incomplete or invalid lock-model evidence: " + str(error)) from error


def run_command(command: list[str], cwd: Path, env: dict, log: Path, timeout: int) -> dict:
    started = time.monotonic()
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    return {"command": command, "cwd": str(cwd), "exit_code": code,
            "elapsed_seconds": time.monotonic() - started,
            "log": str(log), "log_sha256": digest(log)}


def fresh_binary(source: Path, output: Path, name: str, cargo: Path, env: dict,
                 receipt: dict, persist) -> tuple[Path, str]:
    # Never share targets between source roots: Cargo may otherwise reuse a
    # same-name package's relative dep-info without rebuilding a changed copy.
    build = Path(tempfile.mkdtemp(prefix=name + "-build-", dir=output))
    command = [str(cargo), "test", "--offline", "--locked", "--release",
               "--manifest-path", str(source / "Cargo.toml"), "--target-dir", str(build),
               "-p", "aerostore_core", "--test", "shm_mutation_model",
               "--no-run", "--message-format=json"]
    result = run_command(command, source, env, output / (name + "-build.log"), 600)
    receipt["builds"].append(result)
    persist()
    require(result["exit_code"] == 0, name + " failed to compile")
    artifacts = []
    for line in Path(result["log"]).read_text().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if (event.get("reason") == "compiler-artifact"
                and event.get("target", {}).get("name") == "shm_mutation_model"
                and event.get("executable")):
            artifacts.append(event)
    require(len(artifacts) == 1, "missing or ambiguous model executable")
    artifact = artifacts[0]
    require(artifact.get("fresh") is False, "model executable was not freshly compiled")
    require(Path(artifact["target"]["src_path"]).resolve() == (source / MODELS).resolve(),
            "model executable came from another source")
    executable = Path(artifact["executable"]).resolve()
    require(executable.is_relative_to(build.resolve()) and executable.is_file(), "unexpected executable path")
    binary_hash = digest(executable)
    result.update(fresh_test_binary=True, executable=str(executable), executable_sha256=binary_hash,
                  lock_sha256=digest(source / LOCK), model_sha256=digest(source / MODELS))
    listing = run_command([str(executable), "--list"], source, env, output / (name + "-list.log"), 30)
    receipt["listings"].append(listing)
    listed = set(re.findall(r"^(.+): test$", Path(listing["log"]).read_text(), re.MULTILINE))
    require(listing["exit_code"] == 0 and listed == set(CASES), "exact seven required model cases are missing or changed")
    result["listed_cases"] = sorted(listed)
    persist()
    return executable, binary_hash


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--environment-root", type=Path, help="Pinned tool environment root (defaults to source root)")
    args = parser.parse_args()
    root = args.root.resolve()
    environment_root = (args.environment_root or root).resolve()
    output = (args.output or root / "target/verification/lock-models").resolve()
    if not output.is_relative_to(root / "target"):
        parser.error("evidence must be below the selected root's target/")
    output.mkdir(parents=True, exist_ok=True)
    receipt_path = output / "receipt.json"
    receipt = {"schema": 1, "passed": False, "status": "incomplete",
               "scope": "bounded_production_lock_models", "required_cases": list(CASES),
               "required_mutant": MUTANT_NAME, "builds": [], "listings": [], "checks": [],
               "bounds": BOUNDS,
               "native_weak_memory_refinement_proved": False, "unbounded_progress_proved": False,
               "started_at": datetime.now(timezone.utc).isoformat()}
    persist = lambda: write_json(receipt_path, receipt)
    persist()
    inputs = None
    tools_before = {}
    runner_hash = digest(Path(__file__))
    try:
        require(not any(key.startswith("LOOM_") and value for key, value in os.environ.items()),
                "ambient LOOM_* overrides are forbidden")
        require(not os.environ.get("RUSTC_BOOTSTRAP"), "RUSTC_BOOTSTRAP is forbidden")
        checker = environment_root / "scripts/verify_formal.py"
        spec = importlib.util.spec_from_file_location("lock_model_build_environment", checker)
        require(spec is not None and spec.loader is not None, "build-environment checker missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        receipt["build_environment"] = module.check_build_environment(environment_root)
        inputs = fingerprints(root)
        receipt["input_sha256"] = inputs
        receipt["source_root"] = str(root)
        receipt["environment_root"] = str(environment_root)
        receipt["runner_sha256"] = runner_hash
        receipt["candidate_shm_lock_sha256"] = inputs[LOCK]
        receipt["environment_checker_sha256"] = digest(checker)
        check_model_shape(root)
        sysroot = Path(subprocess.check_output(["rustc", "--print", "sysroot"], cwd=root, text=True).strip())
        rustc, cargo = sysroot / "bin/rustc", sysroot / "bin/cargo"
        tools_before = {str(path): digest(path) for path in (rustc, cargo)}
        receipt["tool_sha256"] = tools_before
        receipt["cargo_version"] = subprocess.check_output([str(cargo), "--version"], cwd=root, text=True).strip()
        env = dict(os.environ)
        env.update(RUSTFLAGS="--cfg aerostore_loom", RUSTC=str(rustc))
        receipt["model_environment"] = {key: env.get(key) for key in
            ("RUSTFLAGS", "RUSTC", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "CARGO_HOME")}
        receipt["status"] = "running"
        persist()
        # Keep transient sources outside the repository, so a copied local
        # Cargo config is not inherited a second time from the repository root.
        with tempfile.TemporaryDirectory(prefix="aerostore-lock-model-sources-") as temporary:
            scratch = Path(temporary)
            for parent in scratch.parents:
                require(not any((parent / ".cargo" / name).exists() for name in ("config", "config.toml")),
                        "unreviewed Cargo config above temporary sources")
            candidate, mutant = scratch / "candidate", scratch / "mutant"
            for name in inputs:
                for destination in (candidate, mutant):
                    copied = destination / name
                    copied.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(root / name, copied)
                    require(digest(copied) == inputs[name], "source changed during copy: " + name)
            text = (candidate / LOCK).read_text()
            require(text.count(MUTANT_OLD) == 1, "success-CAS mutant no longer targets exactly one operation")
            bad = text.replace(MUTANT_OLD, MUTANT_NEW)
            (mutant / LOCK).write_text(bad)
            (output / "production-shm_lock.rs").write_text(text)
            (output / "production-shm_mutation_model.rs").write_text((candidate / MODELS).read_text())
            (output / "relaxed-success-cas.patch").write_text("".join(difflib.unified_diff(
                text.splitlines(True), bad.splitlines(True), fromfile="a/" + LOCK, tofile="b/" + LOCK)))
            receipt["mutant"] = {"name": MUTANT_NAME, "case": MUTANT_CASE,
                "source_sha256": digest(mutant / LOCK), "patch_sha256": digest(output / "relaxed-success-cas.patch")}
            executable, binary_hash = fresh_binary(candidate, output, "production", cargo, env, receipt, persist)
            for index, case in enumerate(CASES):
                result = run_command([str(executable), "--exact", case, "--nocapture", "--test-threads=1"],
                                     candidate, env, output / f"case-{index + 1}.log", 120)
                result.update(name=case, expected_counterexample=case in EXPECTED_COUNTEREXAMPLES,
                              executable_sha256=binary_hash)
                receipt["checks"].append(result)
                persist()
                check_case_output(result, case)
                require(digest(executable) == binary_hash, "test executable changed while checking")
                result["passed"] = True
                persist()
            bad_executable, bad_hash = fresh_binary(mutant, output, "mutant", cargo, env, receipt, persist)
            require(bad_hash != binary_hash, "mutation reused the production executable")
            result = run_command([str(bad_executable), "--exact", MUTANT_CASE, "--nocapture", "--test-threads=1"],
                                 mutant, env, output / "relaxed-success-cas.log", 120)
            result.update(name=MUTANT_NAME, case=MUTANT_CASE, expected_failure=True,
                          executable_sha256=bad_hash)
            receipt["checks"].append(result)
            persist()
            check_case_output(result, MUTANT_CASE, mutant=True)
            require(digest(bad_executable) == bad_hash, "mutant executable changed while checking")
            result["passed"] = True
            require(fingerprints(candidate) == inputs, "temporary production source changed")
            expected_mutant = dict(inputs)
            expected_mutant[LOCK] = receipt["mutant"]["source_sha256"]
            require(fingerprints(mutant) == expected_mutant, "temporary mutation changed unrelated source")
        require(fingerprints(root) == inputs, "production source changed while checking")
        require(all(digest(Path(path)) == value for path, value in tools_before.items()), "compiler tools changed")
        require(digest(checker) == receipt["environment_checker_sha256"], "environment checker changed")
        require(digest(Path(__file__)) == runner_hash, "lock-model runner changed")
        receipt.update(passed=True, status="passed", source_stable=True, complete_campaign=True)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        receipt.update(passed=False, status="failed", error=str(error))
    except KeyboardInterrupt:
        receipt.update(passed=False, status="interrupted", error="interrupted before completion")
    finally:
        if inputs is not None:
            try:
                receipt["final_input_sha256"] = fingerprints(root)
                receipt["source_stable"] = receipt["final_input_sha256"] == inputs
                if not receipt["source_stable"]:
                    receipt.update(passed=False, status="failed", error="production source changed while checking")
            except (OSError, RuntimeError, ValueError) as error:
                receipt.update(passed=False, status="failed", source_stable=False, error=str(error))
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        persist()
    print(json.dumps({"passed": receipt["passed"], "status": receipt["status"],
                      "receipt": str(receipt_path), "error": receipt.get("error")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
