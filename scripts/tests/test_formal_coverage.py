"""Proof-input boundaries exercised against real, isolated Git repositories."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check_formal_coverage.py"
SPEC = importlib.util.spec_from_file_location("formal_coverage", SCRIPT)
coverage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage)


class TrackedBoundaryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="formal-boundary-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q")
        self.write("Cargo.toml", "[workspace]\nmembers = []\n")
        self.write("Cargo.lock", "version = 4\n")
        self.write("aerostore_core/src/lib.rs", "pub fn core() {}\n")
        self.write("aerostore_verified/src/lib.rs", "pub fn kernel() {}\n")
        self.write("aerostore_macros/Cargo.toml", "[lib]\nproc-macro = true\n")
        self.write("aerostore_macros/src/lib.rs", "fn row_layout() {}\n")
        self.write("verification/contracts/core.md", "The operation preserves its invariant.\n")
        self.write("aerostore_core/tests/claimed.rs", "fn claimed_contract() {}\n")
        self.write("docs/claimed-contract.md", "Explicitly cited contract.\n")
        self.write("scripts/check_formal_coverage.py", SCRIPT.read_text())
        self.write("verification/claims.toml", '''whole_engine_verified = false
full_P1_complete = false
[[claims]]
id = "NATIVE"
status = "partial"
contract = "docs/claimed-contract.md"
implementation = ["aerostore_core/src/lib.rs", "aerostore_core/tests/claimed.rs"]
''')
        self.git("add", ".")
        self.refresh()
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True, stderr=subprocess.PIPE)

    def write(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def commit(self):
        self.git("add", ".")
        self.git("-c", "user.name=Boundary Test", "-c", "user.email=boundary@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "fixture")

    def run_checker(self, *args, script=SCRIPT):
        return subprocess.run([sys.executable, "-I", str(script), "--root", str(self.root), *args],
                              text=True, capture_output=True)

    def refresh(self):
        result = self.run_checker("--write-boundary")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_unchanged_local_and_strict_checks_pass(self):
        self.assertTrue(coverage.validate(self.root)["passed"])
        self.assertTrue(coverage.validate(self.root, self.base)["passed"])

    def test_untracked_and_ignored_files_do_not_contaminate_lock(self):
        self.write(".gitignore", "ignored.rs\n__pycache__/\n")
        self.write("aerostore_core/src/untracked.rs", "not staged")
        self.write("aerostore_core/src/ignored.rs", "ignored")
        self.write("verification/__pycache__/scratch.py", "ignored cache")
        before = (self.root / coverage.LOCK).read_bytes()
        self.refresh()
        self.assertEqual(before, (self.root / coverage.LOCK).read_bytes())
        self.assertTrue(coverage.validate(self.root, self.base)["passed"])

    def test_added_tracked_engine_or_tool_input_requires_lock_update(self):
        for name in ("aerostore_core/src/new.rs", "aerostore_verified/src/new.rs",
                     "aerostore_macros/src/new.rs",
                     ".cargo/config.toml", "rust-toolchain.toml", "extra_crate/Cargo.toml",
                     "extra_crate/build.rs", "verification/new_model/Spec.tla"):
            with self.subTest(name=name):
                self.write(name, "tracked new proof input")
                self.git("add", "--", name)
                self.assertIn(name, coverage.frozen_paths(self.root))
                self.assertFalse(coverage.validate(self.root)["passed"])
                self.git("rm", "-f", "--", name)

    def test_macro_implementation_is_a_transitive_proof_input(self):
        name = "aerostore_macros/src/lib.rs"
        self.assertIn(name, coverage.frozen_paths(self.root))
        self.write(name, "fn changed_row_layout() {}")
        report = coverage.validate(self.root, self.base)
        self.assertFalse(report["passed"])
        self.assertIn(f"frozen boundary changed: {name}", report["errors"])

    def test_untracked_active_build_controls_fail_without_entering_lock(self):
        self.write(".gitignore", ".cargo/\nrust-toolchain*\nbuild.rs\n")
        for name in (".cargo/config", ".cargo/config.toml", "rust-toolchain", "rust-toolchain.toml",
                     "build.rs", "aerostore_macros/build.rs", "aerostore_macros/.cargo/config.toml"):
            with self.subTest(name=name):
                path = self.write(name, "unreviewed build control")
                self.assertNotIn(name, coverage.frozen_paths(self.root))
                for baseline in (None, self.base):
                    report = coverage.validate(self.root, baseline)
                    self.assertFalse(report["passed"])
                    self.assertIn(f"untracked active build control: {name}", report["errors"])
                self.assertEqual(self.run_checker("--write-boundary").returncode, 1)
                path.unlink()

    def test_tracked_build_control_can_be_reviewed(self):
        for name in (".cargo/config.toml", "aerostore_macros/.cargo/config.toml"):
            with self.subTest(name=name):
                self.write(name, "[build]\njobs = 2\n")
                self.git("add", "--", name)
                self.assertFalse(coverage.validate(self.root)["passed"])
                self.refresh()
                self.assertTrue(coverage.validate(self.root)["passed"])
                self.assertFalse(coverage.validate(self.root, self.base)["passed"])

    def test_first_party_package_shadow_is_rejected_without_execution(self):
        name = "scripts/check_formal_coverage/__init__.py"
        marker = self.root / "poison-executed"
        self.write(name, f'from pathlib import Path\nPath({str(marker)!r}).touch()\n')
        for tracked in (False, True):
            if tracked:
                self.git("add", "--", name)
            for baseline in (None, self.base):
                report = coverage.validate(self.root, baseline)
                self.assertFalse(report["passed"])
                self.assertIn("protected Python module is shadowed or missing: scripts/check_formal_coverage.py",
                              report["errors"])
            self.assertFalse(marker.exists())

    def test_local_stdlib_shadows_are_rejected_without_execution(self):
        for name in ("scripts/json.py", "scripts/json/__init__.py", "scripts/sitecustomize.py"):
            with self.subTest(name=name):
                path = self.write(name, "raise AssertionError('must never execute')\n")
                report = coverage.validate(self.root, self.base)
                self.assertFalse(report["passed"])
                self.assertIn(f"local Python module shadows standard library: {name}", report["errors"])
                path.unlink()

    def test_docs_benches_unclaimed_tests_and_generated_outputs_are_excluded(self):
        names = ("docs/guide.md", "aerostore_core/benches/example.rs",
                 "aerostore_core/tests/unclaimed.rs", "scripts/iterate_hyperfeed.py",
                 "scripts/test_rolling_workload.py", "verification/README.md",
                 "verification/new_model/README.md", "verification/new_model/spec.verus.rs",
                 "verification/tla/evidence/report.json", "verification/tla/evidence/model.cfg",
                 "verification/bridge/generated/core.llbc",
                 "verification/lean/AerostoreProofs/translation.json")
        for name in names:
            self.write(name, "not a proof input")
        self.git("add", ".")
        self.assertTrue(coverage.validate(self.root, self.base)["passed"])
        self.assertTrue(set(names).isdisjoint(coverage.frozen_paths(self.root)))

    def test_claimed_tests_and_docs_and_verification_contracts_are_inputs(self):
        expected = {"docs/claimed-contract.md", "aerostore_core/tests/claimed.rs",
                    "verification/contracts/core.md"}
        self.assertTrue(expected <= set(coverage.frozen_paths(self.root)))
        for name in expected:
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_text()
                path.write_text("weakened")
                self.assertFalse(coverage.validate(self.root)["passed"])
                path.write_text(original)

    def test_explicit_tests_field_is_supported(self):
        name = "aerostore_core/tests/extra.rs"
        self.write(name, "additional required test")
        self.git("add", "--", name)
        with (self.root / coverage.CLAIMS).open("a") as output:
            output.write(f'tests = ["{name}"]\n')
        self.assertIn(name, coverage.frozen_paths(self.root))

    def test_claim_cannot_depend_on_an_untracked_file(self):
        self.write("docs/local-only.md", "not committed")
        path = self.root / coverage.CLAIMS
        path.write_text(path.read_text().replace("docs/claimed-contract.md", "docs/local-only.md"))
        with self.assertRaisesRegex(ValueError, "references must be tracked"):
            coverage.frozen_paths(self.root)

    def test_reviewed_input_and_lock_change_passes_local_but_fails_strict(self):
        self.write("aerostore_core/src/lib.rs", "pub fn changed() {}")
        self.refresh()
        self.assertTrue(coverage.validate(self.root)["passed"])
        self.assertFalse(coverage.validate(self.root, self.base)["passed"])
        report = coverage.validate(self.root, review_base=self.base)
        self.assertTrue(report["passed"])
        self.assertEqual(report["changed_inputs"], {"added": [], "removed": [],
                                                  "modified": ["aerostore_core/src/lib.rs"]})
        self.assertIsNone(report["baseline_ref"])
        self.assertEqual(report["anchoring"], "local_bootstrap_only")

    def test_review_diff_reports_added_and_removed_inputs(self):
        self.git("rm", "--", "verification/contracts/core.md")
        self.write("verification/contracts/new.md", "replacement contract")
        self.git("add", ".")
        self.refresh()
        report = coverage.validate(self.root, review_base=self.base)
        self.assertTrue(report["passed"])
        self.assertEqual(report["changed_inputs"], {
            "added": ["verification/contracts/new.md"], "removed": ["verification/contracts/core.md"], "modified": []})

    def test_review_diff_does_not_hide_a_stale_lock(self):
        self.write("aerostore_verified/src/lib.rs", "changed kernel")
        report = coverage.validate(self.root, review_base=self.base)
        self.assertFalse(report["passed"])
        self.assertEqual(report["changed_inputs"]["modified"], ["aerostore_verified/src/lib.rs"])

    def test_removing_a_claim_cannot_narrow_strict_base_scope(self):
        self.write(coverage.CLAIMS, "whole_engine_verified = false\nfull_P1_complete = false\nclaims = []\n")
        self.write("docs/claimed-contract.md", "weakened and no longer cited")
        self.refresh()
        self.assertTrue(coverage.validate(self.root)["passed"])
        report = coverage.validate(self.root, self.base)
        self.assertFalse(report["passed"])
        self.assertIn("frozen boundary changed: docs/claimed-contract.md", report["errors"])

    def test_base_checker_rejects_tampered_candidate_checker_and_lock(self):
        baseline_checker = self.root.parent / "base_checker.py"
        baseline_checker.write_text(self.git("show", self.base + ":scripts/check_formal_coverage.py"))
        self.write("scripts/check_formal_coverage.py", "raise SystemExit(0)\n")
        self.write("aerostore_core/src/lib.rs", "tampered source")
        self.refresh()
        result = self.run_checker("--baseline-ref", self.base, script=baseline_checker)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("candidate changed the baseline boundary lock", json.loads(result.stdout)["errors"])

    def test_lock_and_claim_path_escapes_are_rejected(self):
        for name in ("../outside", "/etc/passwd", "aerostore_core/../outside", "./Cargo.toml", "x\\y"):
            with self.subTest(name=name):
                lock = {"format_version": 1, "files": {name: "a" * 64}}
                with self.assertRaisesRegex(ValueError, "invalid boundary path"):
                    coverage.read_lock(json.dumps(lock).encode())
                claims = {"claims": [{"contract": name, "implementation": []}]}
                with self.assertRaisesRegex(ValueError, "invalid boundary path"):
                    coverage.claim_paths(claims)

    def test_symlinks_and_parent_directory_escapes_are_rejected(self):
        source = self.root / "aerostore_core/src/lib.rs"
        source.unlink()
        outside = self.root.parent / "outside.rs"
        outside.write_text("outside source")
        source.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "symlink"):
            coverage.validate(self.root)
        source.unlink()
        (self.root / "aerostore_core/src").rmdir()
        (self.root / "aerostore_core/src").symlink_to(self.root.parent, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            coverage.validate(self.root)

    def test_cli_errors_are_json_and_modes_are_mutually_exclusive(self):
        result = self.run_checker("--baseline-ref", "does-not-exist")
        self.assertEqual(result.returncode, 1)
        self.assertFalse(json.loads(result.stdout)["passed"])
        result = self.run_checker("--baseline-ref", self.base, "--review-base", self.base)
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
