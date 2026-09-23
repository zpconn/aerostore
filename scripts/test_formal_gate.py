#!/usr/bin/env python3
"""Adversarial checks for the experiment's stale-evidence/change gate."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch
import verify_formal
import check_lock_models as lock_models

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("coverage", HERE / "check_formal_coverage.py")
coverage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage)


class FrozenBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="aerostore-formal-gate-")
        self.root = Path(self.directory.name)
        # Only copy the reviewed files, not build outputs or downloaded tools.
        required = set(coverage.frozen_paths(coverage.ROOT))
        claims = tomllib.loads((coverage.ROOT / "verification/claims.toml").read_text())
        for claim in claims["claims"]:
            required.update(claim["implementation"])
        for name in required:
            source = coverage.ROOT / name
            if source.exists():
                destination = self.root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
        kernel = self.root / "aerostore_verified/src/lib.rs"
        kernel.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(coverage.ROOT / "aerostore_verified/src/lib.rs", kernel)
        files = {name: coverage.digest(self.root / name) for name in coverage.frozen_paths(self.root)}
        (self.root / coverage.LOCK).write_text(json.dumps({"format_version": 1, "files": files}))

    def tearDown(self):
        self.directory.cleanup()

    def test_unchanged_boundary_passes(self):
        self.assertTrue(coverage.validate(self.root)["passed"])

    def test_weakened_contract_fails(self):
        (self.root / "verification/contracts/bucket_set.md").write_text("Always return an empty set.\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_modified_atomic_primitive_fails(self):
        with (self.root / "aerostore_core/src/shm_lock.rs").open("a") as output:
            output.write("\n// changed primitive boundary\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_new_unproved_engine_module_fails(self):
        (self.root / "aerostore_core/src/unchecked_optimization.rs").write_text("pub fn bypass() {}\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_proof_runner_fails(self):
        (self.root / "scripts/check_lean.py").write_text("raise SystemExit(0)\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_weakened_concurrent_primitive_contract_fails(self):
        (self.root / "verification/concurrent/contracts.rs").write_text("// silently assume publication\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_required_roots_fails(self):
        (self.root / "verification/lean/roots.json").write_text('{"roots": []}')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_extra_kernel_module_fails(self):
        (self.root / "aerostore_verified/src/escape.rs").write_text("pub fn bypass() {}\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_new_cargo_configuration_fails(self):
        (self.root / ".cargo").mkdir(exist_ok=True)
        (self.root / ".cargo/config.toml").write_text('[build]\nrustflags = ["--cfg", "skip_proof"]\n')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_kernel_edit_still_needs_separate_live_proof(self):
        (self.root / "aerostore_verified/src/lib.rs").write_text("// Candidate can change; proof runner must recheck.\n")
        # This gate intentionally does not pretend to prove the editable kernel.
        self.assertTrue(coverage.validate(self.root)["passed"])

    def test_self_rebaseline_cannot_satisfy_independent_commit(self):
        def git(*arguments):
            return subprocess.check_output(["git", *arguments], cwd=self.root, stderr=subprocess.DEVNULL, text=True)
        git("init", "-q")
        git("add", ".")
        git("-c", "user.name=Gate Test", "-c", "user.email=gate-test@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "-qm", "reviewed fixture baseline")
        baseline = git("rev-parse", "HEAD").strip()
        (self.root / "verification/contracts/bucket_set.md").write_text("Weakened contract\n")
        files = {name: coverage.digest(self.root / name) for name in coverage.frozen_paths(self.root)}
        (self.root / coverage.LOCK).write_text(json.dumps({"format_version": 1, "files": files}))
        self.assertTrue(coverage.validate(self.root)["passed"])
        self.assertFalse(coverage.validate(self.root, baseline)["passed"])


class EvidenceTests(unittest.TestCase):
    def test_concurrent_success_without_negative_controls_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "concurrent").mkdir()
            (root / "concurrent/receipt.json").write_text(json.dumps({
                "passed": True, "status": "passed", "native_primitive_refinement_proved": False,
                "transaction_history_refinement_proved": False, "checks": [],
            }))
            with self.assertRaisesRegex(RuntimeError, "complete conditional proof evidence"):
                verify_formal.collect_claim_evidence([], [{"name": "concurrent", "passed": True}], root)

    def test_ambient_compiler_flags_fail(self):
        with patch.dict("os.environ", {"RUSTFLAGS": "--cfg ignore_contract"}):
            with self.assertRaisesRegex(RuntimeError, "unreviewed build environment"):
                verify_formal.check_build_environment()

    def test_missing_root_cannot_satisfy_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lean.json").write_text(json.dumps({"passed": True, "completed": True,
                "kernel_recheck_passed": True, "forged_theorem_rejected": True,
                "mutation_checks": [{"name": name, "rejected": True} for name in
                    ["stamp_accepts_equal", "bitmap_drops_membership", "bitmap_accepts_equal_bound",
                     "sort_writes_wrong_bucket", "sort_accepts_equal_bound"]], "required_roots": []}))
            with self.assertRaisesRegex(RuntimeError, "missing declared proof roots"):
                verify_formal.collect_claim_evidence([{"id": "test", "scope": "test", "status": "partial",
                    "required_checks": ["lean"], "lean_roots": ["must_exist"]}],
                    [{"name": "lean", "passed": True}], root)


class LockModelEvidenceTests(unittest.TestCase):
    """Small synthetic artifacts test the receipt checker, never claim a model run."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="aerostore-lock-receipt-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.output = self.root / "target/lock-models"
        self.output.mkdir(parents=True)
        self.path = self.output / "receipt.json"
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["aerostore_core"]\n')
        (self.root / "Cargo.lock").write_text("# fixture\n")
        for name in [lock_models.LOCK, lock_models.MODELS,
                     "scripts/check_lock_models.py", "scripts/verify_formal.py"]:
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(coverage.ROOT / name, destination)
        inputs = lock_models.fingerprints(self.root)
        original = (self.root / lock_models.LOCK).read_text()
        mutated = original.replace(lock_models.MUTANT_OLD, lock_models.MUTANT_NEW)
        patch_path = self.output / "relaxed-success-cas.patch"
        patch_path.write_text("synthetic patch fixture\n")
        bad_source = self.output / "mutant.rs"
        bad_source.write_text(mutated)
        self.receipt = {
            "schema": 1, "status": "passed", "passed": True, "source_stable": True, "complete_campaign": True,
            "scope": "bounded_production_lock_models", "native_weak_memory_refinement_proved": False,
            "unbounded_progress_proved": False, "bounds": lock_models.BOUNDS,
            "required_cases": list(lock_models.CASES), "required_mutant": lock_models.MUTANT_NAME,
            "source_root": str(self.root), "environment_root": str(self.root),
            "input_sha256": inputs, "final_input_sha256": inputs,
            "candidate_shm_lock_sha256": inputs[lock_models.LOCK],
            "runner_sha256": lock_models.digest(self.root / "scripts/check_lock_models.py"),
            "environment_checker_sha256": lock_models.digest(self.root / "scripts/verify_formal.py"),
            "model_environment": {"RUSTFLAGS": "--cfg aerostore_loom"},
            "tool_sha256": {}, "builds": [], "listings": [], "checks": [],
            "mutant": {"name": lock_models.MUTANT_NAME, "case": lock_models.MUTANT_CASE,
                       "source_sha256": lock_models.digest(bad_source), "patch_sha256": lock_models.digest(patch_path)},
        }
        for name in ("rustc", "cargo"):
            tool = self.output / name
            tool.write_text("synthetic " + name)
            self.receipt["tool_sha256"][str(tool)] = lock_models.digest(tool)
        for variant in (0, 1):
            directory = self.output / str(variant)
            directory.mkdir()
            executable = directory / "model"
            executable.write_text("synthetic binary " + str(variant))
            artifact = {"reason": "compiler-artifact", "fresh": False, "executable": str(executable),
                        "target": {"name": "shm_mutation_model", "src_path": str(directory / lock_models.MODELS)}}
            build = self.log_result(directory / "build.log", json.dumps(artifact), 0)
            build.update(command=["cargo", "test", "--offline", "--locked", "--release", "--no-run",
                                  "--message-format=json", "--test", "shm_mutation_model"], cwd=str(directory),
                         executable=str(executable), executable_sha256=lock_models.digest(executable),
                         fresh_test_binary=True, model_sha256=inputs[lock_models.MODELS],
                         lock_sha256=inputs[lock_models.LOCK] if variant == 0 else lock_models.digest(bad_source))
            self.receipt["builds"].append(build)
            listing = self.log_result(directory / "list.log", "\n".join(case + ": test" for case in lock_models.CASES), 0)
            listing["command"] = [str(executable), "--list"]
            self.receipt["listings"].append(listing)
        for index, name in enumerate([*lock_models.CASES, lock_models.MUTANT_NAME]):
            mutant = index == len(lock_models.CASES)
            case = lock_models.MUTANT_CASE if mutant else name
            build = self.receipt["builds"][int(mutant)]
            contents = "running 1 test\ntest " + case + " ... "
            if mutant:
                contents += "FAILED\nCausality violation: Concurrent write accesses to `UnsafeCell`.\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;\n"
            else:
                contents += "ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n" + lock_models.EXPECTED_COUNTEREXAMPLES.get(case, "")
            check = self.log_result(self.output / (str(index) + ".log"), contents, 101 if mutant else 0)
            check.update(name=name, passed=True, executable_sha256=build["executable_sha256"],
                         command=[build["executable"], "--exact", case, "--nocapture", "--test-threads=1"])
            if mutant:
                check["expected_failure"] = True
            else:
                check["expected_counterexample"] = case in lock_models.EXPECTED_COUNTEREXAMPLES
            self.receipt["checks"].append(check)

    def log_result(self, path, contents, code):
        path.write_text(contents)
        return {"log": str(path), "log_sha256": lock_models.digest(path), "exit_code": code}

    def validate(self):
        self.path.write_text(json.dumps(self.receipt))
        with patch.object(verify_formal, "ROOT", self.root):
            return verify_formal.collect_claim_evidence([], [{"name": "lock-models", "passed": True}], self.output.parent)

    def test_complete_receipt_validates(self):
        self.assertEqual(self.validate(), [])

    def test_missing_receipt_fails(self):
        with patch.object(verify_formal, "ROOT", self.root), self.assertRaisesRegex(RuntimeError, "lock-model evidence"):
            verify_formal.collect_claim_evidence([], [{"name": "lock-models", "passed": True}], self.output.parent)

    def test_success_boolean_without_artifacts_fails(self):
        self.receipt = {"passed": True, "status": "passed"}
        with self.assertRaisesRegex(RuntimeError, "lock-model evidence"):
            self.validate()

    def test_missing_or_duplicate_case_fails(self):
        self.receipt["checks"][1] = self.receipt["checks"][0]
        with self.assertRaisesRegex(RuntimeError, "missing or duplicate"):
            self.validate()

    def test_missing_negative_control_fails(self):
        self.receipt["checks"].pop()
        with self.assertRaisesRegex(RuntimeError, "missing or duplicate"):
            self.validate()

    def test_arbitrary_mutant_failure_fails(self):
        check = self.receipt["checks"][-1]
        check.update(self.log_result(Path(check["log"]), "running 1 test\ntest " + lock_models.MUTANT_CASE
                     + " ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;\nunrelated panic\n", 101))
        with self.assertRaisesRegex(RuntimeError, "intended Loom causality"):
            self.validate()

    def test_zero_tests_fails(self):
        check = self.receipt["checks"][0]
        check.update(self.log_result(Path(check["log"]), "running 0 tests\n0 passed; 0 failed; 0 ignored\n", 0))
        with self.assertRaisesRegex(RuntimeError, "run exactly once"):
            self.validate()

    def test_compile_failure_fails(self):
        self.receipt["builds"][1]["exit_code"] = 101
        with self.assertRaisesRegex(RuntimeError, "model executable"):
            self.validate()

    def test_cached_artifact_fails(self):
        build = self.receipt["builds"][0]
        log = Path(build["log"])
        artifact = json.loads(log.read_text())
        artifact["fresh"] = True
        build.update(self.log_result(log, json.dumps(artifact), 0))
        with self.assertRaisesRegex(RuntimeError, "fresh compiler artifact"):
            self.validate()

    def test_changed_binary_fails(self):
        Path(self.receipt["builds"][0]["executable"]).write_text("substituted binary")
        with self.assertRaisesRegex(RuntimeError, "substituted model executable"):
            self.validate()

    def test_changed_source_fails(self):
        with (self.root / lock_models.LOCK).open("a") as output:
            output.write("\n// changed source\n")
        with self.assertRaisesRegex(RuntimeError, "stale lock-model source"):
            self.validate()

    def test_broader_claim_fails(self):
        self.receipt["unbounded_progress_proved"] = True
        with self.assertRaisesRegex(RuntimeError, "unsupported lock-model scope"):
            self.validate()


class RunnerTests(unittest.TestCase):
    def run_fixture(self, profile, coverage_pass=True, baseline=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "verification").mkdir()
            (root / "verification/claims.toml").write_text("claims = []\n")
            output = root / "target/report.json"
            arguments = ["verify_formal.py", "--profile", profile, "--output", str(output)]
            if baseline:
                arguments += ["--baseline-ref", "a" * 40]
            calls = []

            def check(name, command, directory, timeout):
                calls.append((name, command))
                return {"name": name, "passed": coverage_pass if name == "coverage" else True}

            with patch.object(verify_formal, "ROOT", root), \
                 patch.object(verify_formal, "check_build_environment", return_value={}), \
                 patch.object(verify_formal, "source_fingerprint", return_value={}), \
                 patch.object(verify_formal, "collect_claim_evidence", return_value=[]), \
                 patch.object(verify_formal, "run_check", side_effect=check), \
                 patch.object(verify_formal.subprocess, "check_output", side_effect=lambda *a, **kw: "head" if kw.get("text") else b"# trusted checker\n"), \
                 patch.object(sys, "argv", arguments):
                code = verify_formal.main()
            return code, json.loads(output.read_text()), calls

    def test_models_with_baseline_actually_checks_baseline(self):
        code, _, calls = self.run_fixture("models", baseline=True)
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][0], "coverage")
        self.assertIn("--baseline-ref", calls[0][1])

    def test_failed_boundary_prevents_candidate_runners(self):
        code, report, calls = self.run_fixture("pilot", coverage_pass=False, baseline=True)
        self.assertEqual(code, 1)
        self.assertFalse(report["passed"])
        self.assertEqual([name for name, _ in calls], ["coverage"])

    def test_full_cannot_pass_by_removing_open_statuses(self):
        code, report, _ = self.run_fixture("full")
        self.assertEqual(code, 1)
        self.assertFalse(report["passed"])
        self.assertFalse(report["whole_engine_verified"])

    def test_pilot_requires_lock_models(self):
        code, _, calls = self.run_fixture("pilot")
        self.assertEqual(code, 0)
        commands = dict(calls)
        self.assertIn("lock-models", commands)
        self.assertIn("scripts/check_lock_models.py", commands["lock-models"])


if __name__ == "__main__":
    unittest.main()
