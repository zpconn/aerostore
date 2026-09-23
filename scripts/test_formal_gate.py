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


if __name__ == "__main__":
    unittest.main()
