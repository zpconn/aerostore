"""Small local Git fixtures; no production mirror, network, or compiler builds."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("history_rewrite", HERE / "rewrite.py")
rewrite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rewrite)
TOOL = HERE.parents[1] / ".tools/history/git-filter-repo/v2.47.0/git-filter-repo"


class HistoryFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="aerostore-history-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.source = self.directory / "original"
        self.source.mkdir()
        self.git("init", "-b", "master")
        self.git("config", "user.name", "History Fixture")
        self.git("config", "user.email", "history-fixture@example.invalid")
        (self.source / "README.md").write_text("retained\n")
        self.commit("before evidence")
        self.early = self.head()
        for directory in sorted(rewrite.REQUIRED_PATHS):
            path = self.source / directory / "retained-original.bin"
            path.parent.mkdir(parents=True)
            path.write_bytes((directory + "\n").encode() * 100)
        (self.source / "code.txt").write_text("code added alongside evidence\n")
        self.commit("evidence introduced")
        self.bulk_head = self.head()
        self.git("branch", "historical", self.bulk_head)
        self.git("checkout", "-b", "prepared")
        self.git("rm", "-r", "--cached", *sorted(rewrite.REQUIRED_PATHS))
        (self.source / "evidence").mkdir()
        (self.source / "evidence/README.md").write_text("payloads remain in the original archive\n")
        self.git("add", "evidence/README.md")
        self.git("commit", "-m", "externalize index only")
        self.prepared = self.head()
        self.config = HERE / "removal-rules.json"
        self.pin = HERE / "filter-repo-pin.json"
        self.initial_refs = rewrite.refs(self.source)
        self.initial_payloads = self.payloads()

    def git(self, *args):
        return rewrite.git(self.source, *args)

    def head(self):
        return self.git("rev-parse", "HEAD").decode().strip()

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-m", message)

    def payloads(self):
        return {str(path.relative_to(self.source)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in self.source.glob("docs/*/retained-original.bin")}

    def run_rewrite(self, output="attempt-1", source_ref="refs/heads/prepared"):
        return rewrite.rewrite(self.source, source_ref, self.directory / output,
                               self.config, TOOL, self.pin)

    def assert_source_preserved(self):
        self.assertEqual(rewrite.refs(self.source), self.initial_refs)
        self.assertEqual(self.payloads(), self.initial_payloads)


class PreflightTests(HistoryFixture):
    def test_unexternalized_tip_fails_before_output_or_tool_execution(self):
        with mock.patch.object(rewrite, "verify_tool", side_effect=AssertionError("tool ran")):
            with self.assertRaisesRegex(rewrite.RewriteError, "still contains removal paths"):
                self.run_rewrite(source_ref="refs/heads/master")
        self.assertFalse((self.directory / "attempt-1").exists())
        self.assert_source_preserved()

    def test_existing_output_is_untouched(self):
        output = self.directory / "attempt-1"
        output.mkdir()
        (output / "receipt").write_text("old failure evidence")
        with self.assertRaisesRegex(rewrite.RewriteError, "output must not exist"):
            self.run_rewrite()
        self.assertEqual((output / "receipt").read_text(), "old failure evidence")
        self.assert_source_preserved()

    def test_output_in_source_git_directory_is_rejected(self):
        with self.assertRaisesRegex(rewrite.RewriteError, "Git directory"):
            self.run_rewrite(output=str(self.source / ".git/rewrite"))
        self.assertFalse((self.source / ".git/rewrite").exists())

    def test_symlink_output_cannot_redirect_rewrite(self):
        (self.directory / "attempt-1").symlink_to(self.source, target_is_directory=True)
        with self.assertRaisesRegex(rewrite.RewriteError, "output must not exist"):
            self.run_rewrite()
        self.assert_source_preserved()

    def test_invalid_and_unknown_refs_do_not_create_output(self):
        for ref in ["--help", "HEAD", "refs/heads/--bad..", "f" * 40]:
            with self.subTest(ref=ref), self.assertRaises(rewrite.RewriteError):
                self.run_rewrite(source_ref=ref)
        self.assertFalse((self.directory / "attempt-1").exists())

    def test_replacement_refs_are_rejected(self):
        self.git("replace", self.early, self.bulk_head)
        with self.assertRaisesRegex(rewrite.RewriteError, "replacement refs"):
            self.run_rewrite()

    def test_partial_source_is_rejected_before_clone(self):
        self.git("config", "remote.origin.promisor", "true")
        with self.assertRaisesRegex(rewrite.RewriteError, "partial/promisor"):
            self.run_rewrite()
        self.assertFalse((self.directory / "attempt-1").exists())

    def test_git_environment_cannot_redirect_source_or_output(self):
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.directory / "nonexistent"),
                                         "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "alias.clone",
                                         "GIT_CONFIG_VALUE_0": "!exit 99"}):
            result = rewrite.preflight_source(self.source, "refs/heads/prepared",
                                             self.directory / "fresh", list(rewrite.REQUIRED_PATHS))
        self.assertEqual(result["head"], self.prepared)


class ConfigTests(unittest.TestCase):
    def test_invalid_removal_rules_are_not_treated_as_cli_or_glob_options(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "rules.json"
            for bad in ["../secret", "/absolute", "--force", "docs//x", "docs/./x", ".git", "docs/x/", 8]:
                with self.subTest(path=bad):
                    path.write_text(json.dumps({"schema": 1, "remove_paths": [*rewrite.REQUIRED_PATHS, bad],
                                                "largest_blobs": 20}))
                    with self.assertRaises(rewrite.RewriteError):
                        rewrite.read_config(path)

    def test_three_bulk_roots_cannot_be_accidentally_omitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "rules.json"
            path.write_text(json.dumps({"schema": 1, "remove_paths": ["docs/bench_data"], "largest_blobs": 20}))
            with self.assertRaisesRegex(rewrite.RewriteError, "all three"):
                rewrite.read_config(path)

    def test_map_reports_unchanged_rewritten_and_removed_commits(self):
        data = ("old new\n" + "a"*40 + " " + "a"*40 + "\n" + "b"*40 + " " + "c"*40
                + "\n" + "d"*40 + " " + "0"*40 + "\n").encode()
        self.assertEqual(rewrite.commit_map_summary(data),
                         {"total": 3, "unchanged": 1, "rewritten": 1, "removed": 1})
        with self.assertRaisesRegex(rewrite.RewriteError, "duplicate"):
            rewrite.commit_map_summary(data + data.splitlines(keepends=True)[1])


@unittest.skipUnless(TOOL.is_file(), "install the pinned local git-filter-repo for integration fixtures")
class RewriteTests(HistoryFixture):
    def test_rewrite_preserves_original_and_exact_prepared_master_tree(self):
        report = self.run_rewrite()
        self.assertTrue(report["passed"], report)
        self.assertTrue(report["tree_byte_identical"])
        self.assertTrue(report["source_refs_unchanged"])
        self.assertTrue(report["removed_paths_absent_from_all_history"])
        self.assertEqual(report["source"]["head"], self.prepared)
        self.assertEqual(report["source"]["requested_ref"], "refs/heads/prepared")
        self.assertGreater(report["pack_bytes"], 0)
        self.assertGreater(report["commit_map"]["counts"]["rewritten"], 0)
        self.assertEqual(report["commit_map"]["counts"]["unchanged"], 1)
        self.assertIn("README.md", [row["representative_path"] for row in report["largest_remaining_blobs"]])
        mapping = dict(line.split() for line in (self.directory / "attempt-1/commit-map.tsv").read_text().splitlines()[1:])
        self.assertEqual(mapping[self.early], self.early)
        self.assertNotEqual(mapping[self.bulk_head], self.bulk_head)
        mirror = self.directory / "attempt-1/mirror.git"
        self.assertFalse((mirror / "objects/info/alternates").exists())
        self.assert_source_preserved()

    def test_second_run_requires_new_output_and_is_deterministic(self):
        first = self.run_rewrite()
        second = self.run_rewrite("attempt-2", source_ref=self.prepared)
        self.assertTrue(first["passed"], first)
        self.assertTrue(second["passed"], second)
        self.assertEqual(first["rewritten_master"], second["rewritten_master"])
        self.assertEqual(first["commit_map"], second["commit_map"])
        with self.assertRaisesRegex(rewrite.RewriteError, "output must not exist"):
            self.run_rewrite()
        self.assert_source_preserved()

    def test_additional_literal_removal_rule_is_enforced(self):
        (self.source / "private[1].txt").write_text("private history")
        self.git("add", "private[1].txt")
        self.git("commit", "-m", "extra historic path")
        self.git("checkout", "-b", "extra-prepared")
        self.git("rm", "--cached", "private[1].txt")
        (self.source / "extra.txt").write_text("kept")
        self.git("add", "extra.txt")
        self.git("commit", "-m", "prepare extra removal")
        self.config = self.directory / "rules.json"
        self.config.write_text(json.dumps({"schema": 1, "remove_paths": [*sorted(rewrite.REQUIRED_PATHS),
                                                                      "private[1].txt"], "largest_blobs": 3}))
        report = self.run_rewrite(source_ref="refs/heads/extra-prepared")
        self.assertTrue(report["passed"], report)
        self.assertEqual(len(report["largest_remaining_blobs"]), 3)

    def test_pin_mismatch_refuses_before_cloning(self):
        fake = self.directory / "git-filter-repo"
        fake.write_text("raise AssertionError('unreviewed tool ran')\n")
        with self.assertRaisesRegex(rewrite.RewriteError, "hash differs"):
            rewrite.rewrite(self.source, self.prepared, self.directory / "attempt-1",
                            self.config, fake, self.pin)
        self.assertFalse((self.directory / "attempt-1").exists())

    def test_cli_reports_success_and_fails_on_reusing_evidence(self):
        command = [sys.executable, str(HERE / "rewrite.py"), "--source", str(self.source),
                   "--source-ref", self.prepared, "--output", str(self.directory / "cli"),
                   "--filter-repo", str(TOOL)]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)["passed"])
        again = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(again.returncode, 1)
        self.assertIn("output must not exist", again.stderr)
        self.assert_source_preserved()

    def test_tool_symlink_is_not_silently_resolved_before_validation(self):
        alias = self.directory / "tool-alias"
        alias.symlink_to(TOOL)
        with self.assertRaisesRegex(rewrite.RewriteError, "regular standalone"):
            rewrite.rewrite(self.source, self.prepared, self.directory / "attempt-1",
                            self.config, alias, self.pin)

    def test_failed_filter_retains_report_and_source(self):
        original = rewrite.logged
        def fail_filter(command, cwd, path):
            if path.name == "filter-repo.log":
                path.write_text("fixture failure\n")
                raise rewrite.RewriteError("fixture filter failed")
            original(command, cwd, path)
        with mock.patch.object(rewrite, "logged", side_effect=fail_filter):
            report = self.run_rewrite()
        self.assertFalse(report["passed"])
        self.assertIn("fixture filter failed", report["error"])
        self.assertTrue((self.directory / "attempt-1/clone.log").is_file())
        self.assertTrue((self.directory / "attempt-1/filter-repo.log").is_file())
        self.assert_source_preserved()

    def test_failed_tree_invariant_is_retained_and_returns_failure(self):
        original = rewrite.logged
        def corrupt_after_filter(command, cwd, path):
            original(command, cwd, path)
            if path.name == "filter-repo.log":
                rewrite.git(cwd, "update-ref", "refs/heads/master", self.early)
        with mock.patch.object(rewrite, "logged", side_effect=corrupt_after_filter):
            report = self.run_rewrite()
        self.assertFalse(report["passed"])
        self.assertFalse(report["tree_byte_identical"])
        self.assertIn("differs", report["error"])
        self.assertEqual(json.loads((self.directory / "attempt-1/report.json").read_text()), report)
        self.assert_source_preserved()


if __name__ == "__main__":
    unittest.main()
