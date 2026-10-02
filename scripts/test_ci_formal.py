#!/usr/bin/env python3
"""Exercise the extracted CI helpers without proof tools, builds, or databases."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


status = load("ci_formal_status")
pilot = load("ci_formal_pilot")
anchor = load("ci_formal_anchor")


class AnchorTests(unittest.TestCase):
    def setUp(self):
        target = HERE.parent / "target"
        target.mkdir(exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="ci-formal-tests-", dir=target)
        self.addCleanup(temporary.cleanup)
        self.scratch = Path(temporary.name)
        self.root = self.scratch / "candidate"
        self.root.mkdir()
        self.write("verification/claims.toml", "whole_engine_verified = false\nfull_P1_complete = false\nclaims = []\n")
        self.write("Cargo.toml", "[workspace]\n")
        self.write("aerostore_core/src/lib.rs", "pub fn value() -> u8 { 1 }\n")
        self.write("scripts/check_lock_models.py", "# synthetic protected first-party module\n")
        self.write(".gitignore", "/target/\n")
        for name in ("check_formal_coverage.py", "ci_formal_anchor.py", "ci_formal_pilot.py", "ci_formal_status.py"):
            self.write("scripts/" + name, (HERE / name).read_text())
        self.git("init", "--quiet")
        self.git("add", ".")
        self.refresh()
        self.git("add", ".")
        self.commit("base")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.write("README.md", "candidate documentation\n")
        self.git("add", "README.md")
        self.commit("candidate")
        self.trusted = self.scratch / "trusted-anchor.py"
        self.trusted.write_text(self.git("show", f"{self.base}:scripts/ci_formal_anchor.py"))
        self.counter = 0

    def write(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True, stderr=subprocess.PIPE)

    def commit(self, message):
        self.git("-c", "user.name=CI fixture", "-c", "user.email=ci@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message)

    def refresh(self):
        subprocess.run([sys.executable, "-I", str(HERE / "check_formal_coverage.py"),
                        "--root", str(self.root), "--write-boundary"],
                       check=True, capture_output=True, text=True)

    def run_anchor(self, baseline=None, mode="review", bootstrap=False, helper=None, env=None):
        self.counter += 1
        directory = f"target/check-{self.counter}"
        output = self.scratch / f"github-output-{self.counter}"
        summary = self.scratch / f"github-summary-{self.counter}"
        environment = dict(os.environ, GITHUB_OUTPUT=str(output), GITHUB_STEP_SUMMARY=str(summary))
        environment.update(env or {})
        command = [sys.executable, "-I", str(helper or self.trusted), "--root", str(self.root),
                   "--baseline-ref=" + (self.base if baseline is None else baseline),
                   "--mode", mode, "--output-dir", directory]
        if bootstrap:
            command.append("--bootstrap")
        result = subprocess.run(command, cwd=self.root, env=environment, text=True, capture_output=True)
        receipt = json.loads((self.root / directory / "anchoring.json").read_text())
        self.assertFalse(receipt["promotion_eligible"])
        return result, receipt, output.read_text(), summary.read_text()

    def test_unchanged_boundary_has_independent_anchor(self):
        result, receipt, outputs, _ = self.run_anchor()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(receipt["anchored"])
        self.assertEqual(receipt["status"], "anchored")
        self.assertIn("baseline_ref=\n", outputs)
        self.assertIn("trusted_checker_sha256", receipt)

    def test_reviewed_changes_are_reported_without_experiment_authority(self):
        self.write("aerostore_core/src/lib.rs", "pub fn value() -> u8 { 2 }\n")
        self.refresh()
        result, receipt, outputs, summary = self.run_anchor()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(receipt["anchored"])
        self.assertTrue(receipt["boundary_checked"])
        self.assertEqual(receipt["status"], "reviewed_boundary_consistent")
        self.assertEqual(receipt["changed_inputs"]["modified"], ["aerostore_core/src/lib.rs"])
        self.assertIn("aerostore_core/src/lib.rs", summary)
        self.assertIn("baseline_ref=\n", outputs)
        strict, strict_receipt, _, _ = self.run_anchor(mode="experiment")
        self.assertNotEqual(strict.returncode, 0)
        self.assertEqual(strict_receipt["status"], "baseline_gate_failed")

    def test_inconsistent_candidate_lock_fails_review(self):
        self.write("aerostore_core/src/lib.rs", "pub fn value() -> u8 { 99 }\n")
        result, receipt, _, _ = self.run_anchor()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "baseline_gate_failed")

    def test_strict_pass_forwards_only_the_checked_baseline(self):
        result, receipt, outputs, _ = self.run_anchor(mode="experiment")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(receipt["anchored"])
        self.assertEqual(receipt["pilot_baseline_ref"], self.base)
        self.assertIn("baseline_ref=" + self.base + "\n", outputs)

    def test_tampered_candidate_helpers_cannot_replace_base_judges(self):
        marker = self.root / "candidate-code-executed"
        malicious = f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\nraise SystemExit(0)\n"
        self.write("scripts/ci_formal_anchor.py", malicious)
        self.write("scripts/check_formal_coverage.py", malicious)
        result, receipt, _, _ = self.run_anchor(mode="experiment")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "baseline_gate_failed")
        self.assertFalse(marker.exists())
        # Even a consistent edited lock cannot turn this into a strict pass.
        self.refresh()
        result, receipt, _, _ = self.run_anchor(mode="experiment")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())
        # Review mode still uses the base checker and surfaces both edits.
        result, receipt, _, _ = self.run_anchor()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(receipt["status"], "reviewed_boundary_consistent")
        self.assertFalse(marker.exists())

    def test_accidental_candidate_helper_invocation_is_rejected(self):
        path = self.root / "scripts/ci_formal_anchor.py"
        path.write_text(path.read_text() + "\n# unreviewed candidate helper\n")
        result, receipt, _, _ = self.run_anchor(helper=path)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "untrusted_anchor")

    def test_candidate_python_path_cannot_supply_base_helper_dependencies(self):
        marker = self.root / "import-executed"
        poison = f"from pathlib import Path\nPath({str(marker)!r}).touch()\nraise RuntimeError('candidate import')\n"
        environment_path = self.scratch / "untrusted-python-path"
        environment_path.mkdir()
        for name in ("json.py", "hashlib.py", "sitecustomize.py"):
            self.write(name, poison)
            (environment_path / name).write_text(poison)
        result, receipt, _, _ = self.run_anchor(env={"PYTHONPATH": str(environment_path)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(receipt["anchored"])
        self.assertFalse(marker.exists())

    def test_local_stdlib_shadow_is_rejected_before_unisolated_children(self):
        marker = self.root / "stdlib-executed"
        self.write("scripts/json.py", f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
        result, receipt, _, _ = self.run_anchor()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "baseline_gate_failed")
        self.assertFalse(marker.exists())

    def test_first_party_package_shadow_is_rejected_without_execution(self):
        marker = self.root / "package-executed"
        self.write("scripts/check_lock_models/__init__.py",
                   f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
        for mode in ("review", "experiment"):
            with self.subTest(mode=mode):
                result, receipt, _, _ = self.run_anchor(mode=mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "baseline_gate_failed")
                self.assertFalse(marker.exists())

    def test_missing_or_same_head_base_is_informational_only_for_review(self):
        for baseline in ("", "0" * 40, self.git("rev-parse", "HEAD").strip()):
            with self.subTest(baseline=baseline):
                result, receipt, outputs, _ = self.run_anchor(baseline=baseline, bootstrap=True)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "bootstrap_unanchored")
                self.assertFalse(receipt["anchored"])
                self.assertIn("baseline_ref=\n", outputs)
                result, receipt, _, _ = self.run_anchor(baseline=baseline, mode="experiment", bootstrap=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "experiment_requires_anchor")

    def test_bad_requested_base_fails_including_option_injection(self):
        for baseline in ("--help", "-c core.sshCommand=touch marker", "HEAD", "a" * 39, "A" * 40,
                         "a" * 40 + "\nanchored=true"):
            with self.subTest(baseline=baseline):
                result, receipt, outputs, _ = self.run_anchor(baseline=baseline, bootstrap=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "invalid_baseline")
                self.assertNotIn("anchored=true", outputs)

    def test_unavailable_requested_base_is_not_bootstrap(self):
        result, receipt, _, _ = self.run_anchor(baseline="f" * 40, bootstrap=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "unavailable_baseline")

    def test_existing_unreadable_base_blob_fails_instead_of_bootstrapping(self):
        for index, name in enumerate((anchor.ANCHOR, anchor.CHECKER, anchor.LOCK)):
            with self.subTest(path=name):
                object_id = self.git("rev-parse", f"{self.base}:{name}").strip()
                real_git = anchor.git

                def fail_object_read(root, *arguments):
                    if arguments == ("cat-file", "blob", object_id):
                        raise subprocess.CalledProcessError(128, ["git", *arguments],
                                                            stderr=b"promisor object fetch failed")
                    return real_git(root, *arguments)

                directory = f"target/read-failure-{index}"
                arguments = ["ci_formal_anchor.py", "--root", str(self.root),
                             "--baseline-ref=" + self.base, "--output-dir", directory]
                environment = {"GITHUB_OUTPUT": str(self.scratch / f"failed-output-{index}"),
                               "GITHUB_STEP_SUMMARY": str(self.scratch / f"failed-summary-{index}")}
                with patch.object(anchor, "git", side_effect=fail_object_read), \
                        patch.object(sys, "argv", arguments), patch.dict(os.environ, environment), \
                        patch("builtins.print"):
                    self.assertEqual(anchor.main(), 1)
                receipt = json.loads((self.root / directory / "anchoring.json").read_text())
                self.assertEqual(receipt["status"], "anchor_error")
                self.assertFalse(receipt["anchored"])
                self.assertFalse(receipt["promotion_eligible"])

    def test_base_tree_entry_must_be_a_regular_blob(self):
        with self.assertRaisesRegex(ValueError, "not a regular blob"):
            anchor.blob(self.root, self.base, "scripts")
        path = self.root / "base-symlink"
        path.symlink_to("Cargo.toml")
        self.git("add", "base-symlink")
        self.commit("symlink fixture")
        with self.assertRaisesRegex(ValueError, "not a regular blob"):
            anchor.blob(self.root, self.git("rev-parse", "HEAD").strip(), "base-symlink")

    def legacy_base(self, checker=True):
        self.git("rm", "scripts/ci_formal_anchor.py")
        if not checker:
            self.git("rm", "scripts/check_formal_coverage.py")
        self.refresh()
        self.git("add", "verification/frozen_boundary.json")
        self.commit("legacy fixture base")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.write("README.md", "a later documentation change\n")
        self.git("add", "README.md")
        self.commit("candidate after legacy")

    def test_legacy_base_requires_explicit_bootstrap(self):
        self.legacy_base()
        result, receipt, _, _ = self.run_anchor()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "missing_base_helper")
        result, receipt, _, _ = self.run_anchor(bootstrap=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(receipt["status"], "bootstrap_unanchored")
        self.assertTrue(receipt["boundary_checked"])

    def test_legacy_checker_rejection_survives_owner_review_transition(self):
        self.legacy_base()
        self.write("aerostore_core/src/lib.rs", "pub fn changed() {}\n")
        self.refresh()
        result, receipt, _, _ = self.run_anchor(bootstrap=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "legacy_base_gate_failed")
        self.assertFalse(receipt["anchored"])

    def test_legacy_checker_needs_no_review_option_and_preserves_failure_code(self):
        self.write("scripts/check_formal_coverage.py",
                   "import argparse\n"
                   "parser = argparse.ArgumentParser()\n"
                   "parser.add_argument('--root')\n"
                   "parser.add_argument('--baseline-ref', required=True)\n"
                   "parser.parse_args()\n"
                   "print('{\"passed\": false}')\n"
                   "raise SystemExit(17)\n")
        self.git("add", "scripts/check_formal_coverage.py")
        self.legacy_base()
        result, receipt, _, _ = self.run_anchor(bootstrap=True)
        self.assertEqual(result.returncode, 17)
        self.assertEqual(receipt["checker_exit_code"], 17)
        self.assertEqual(receipt["status"], "legacy_base_gate_failed")

    def test_base_without_any_checker_is_informational(self):
        self.legacy_base(checker=False)
        result, receipt, _, _ = self.run_anchor(bootstrap=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "bootstrap_unanchored")
        self.assertFalse(receipt["boundary_checked"])

    def test_existing_base_helper_cannot_be_forced_into_bootstrap(self):
        result, receipt, _, _ = self.run_anchor(bootstrap=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "untrusted_anchor")

    def test_zero_exit_without_checker_pass_report_fails(self):
        self.write("scripts/check_formal_coverage.py", "print('{}')\n")
        self.refresh()
        self.git("add", ".")
        self.commit("broken trusted checker fixture")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.write("README.md", "later candidate\n")
        self.git("add", "README.md")
        self.commit("candidate with broken checker")
        result, receipt, _, _ = self.run_anchor()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(receipt["status"], "invalid_checker_report")


class PilotTests(unittest.TestCase):
    def test_command_preserves_mode_and_fixed_output(self):
        root = Path("/tmp/candidate")
        command = pilot.command(root, "a" * 40, "experiment")
        self.assertIn("--baseline-ref", command)
        self.assertEqual(command[-1], "a" * 40)
        self.assertIn("target/verification/ci/pilot/report.json", command)
        self.assertIn("-I", command)
        for baseline, mode in (("--help", "experiment"), ("0" * 40, "experiment"),
                               ("", "experiment"), ("a" * 40, "review"), ("", "typo")):
            with self.subTest(baseline=baseline, mode=mode), self.assertRaises(ValueError):
                pilot.command(root, baseline, mode)

    def test_real_child_failure_is_preserved_and_local_helpers_still_import(self):
        target = HERE.parent / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="ci-pilot-tests-", dir=target) as temporary:
            root = Path(temporary)
            scripts = root / "scripts"
            scripts.mkdir()
            (scripts / "fixture_helper.py").write_text("EXIT = 7\n")
            (scripts / "json.py").write_text("raise RuntimeError('candidate shadowed stdlib')\n")
            (scripts / "sitecustomize.py").write_text("raise RuntimeError('candidate sitecustomize')\n")
            (scripts / "verify_formal.py").write_text(
                "import json, os, pathlib, sys, fixture_helper\n"
                "output = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])\n"
                "output.parent.mkdir(parents=True)\n"
                "output.write_text(json.dumps({'argv': sys.argv, 'passed': False, 'pythonpath': os.environ.get('PYTHONPATH')}))\n"
                "raise SystemExit(fixture_helper.EXIT)\n")
            result = subprocess.run([sys.executable, "-I", str(HERE / "ci_formal_pilot.py"),
                                     "--root", str(root), "--mode", "review", "--baseline-ref="],
                                    env=dict(os.environ, PYTHONPATH=str(scripts)), capture_output=True, text=True)
            self.assertEqual(result.returncode, 7, result.stdout + result.stderr)
            report = json.loads((root / "target/verification/ci/pilot/report.json").read_text())
            self.assertIn("pilot", report["argv"])
            self.assertNotIn("--baseline-ref", report["argv"])
            self.assertIsNone(report["pythonpath"])


class StatusTests(unittest.TestCase):
    def test_bootstrap_is_informational_but_pilot_failure_is_blocking(self):
        for anchored, state in (("false", "bootstrap_unanchored"),
                                ("false", "reviewed_boundary_consistent"), ("true", "anchored")):
            with self.subTest(anchored=anchored, state=state):
                self.assertEqual(status.verdict("success", anchored, state, "review")[0], 0)
                for result in ("failure", "cancelled", "skipped", ""):
                    self.assertEqual(status.verdict(result, anchored, state, "review")[0], 1)

    def test_missing_invalid_or_failed_anchors_do_not_pass(self):
        for anchored, state, mode in (("", "", "review"), ("TRUE", "anchored", "review"),
                                      ("true", "bootstrap_unanchored", "review"),
                                      ("false", "legacy_base_gate_failed", "review"),
                                      ("false", "bootstrap_unanchored", "experiment"),
                                      ("false", "reviewed_boundary_consistent", "experiment"),
                                      ("true", "anchored", "typo")):
            with self.subTest(anchored=anchored, state=state, mode=mode):
                self.assertEqual(status.verdict("success", anchored, state, mode)[0], 1)

    def test_status_cli_writes_scope_without_promotion(self):
        target = HERE.parent / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="ci-status-tests-", dir=target) as temporary:
            summary = Path(temporary) / "summary.md"
            result = subprocess.run([sys.executable, "-I", str(HERE / "ci_formal_status.py"),
                                     "--pilot-result", "success", "--anchored", "false",
                                     "--anchor-status", "bootstrap_unanchored", "--mode", "review"],
                                    env=dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("informational bootstrap", summary.read_text())
            self.assertIn("Promotion eligible: **false**", summary.read_text())


if __name__ == "__main__":
    unittest.main()
