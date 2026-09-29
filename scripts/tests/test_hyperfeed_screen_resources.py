#!/usr/bin/env python3
"""No builds or benchmark processes: bounded screen resource admission checks."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hyperfeed_screen_resources as resources

GIB = resources.GIB


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.target = self.repo / "target"
        self.target.mkdir()
        self.output = self.target / "screen"
        self.snapshot = dict(target_path=str(self.target), target_allocated_bytes=100 * GIB,
                             git_path=str(self.repo / ".git"), git_allocated_bytes=5 * GIB,
                             guest_path=str(self.output), guest_free_bytes=80 * GIB,
                             is_wsl=True, host_volume="/mnt/c", host_free_bytes=70 * GIB,
                             mem_available_bytes=40 * GIB)

    def admit(self, **kwargs):
        with patch.object(resources, "resource_snapshot", return_value=copy.deepcopy(self.snapshot)):
            return resources.preflight(self.target, self.output, **kwargs)

    def check(self, baseline, snapshot=None, prospective=3 * GIB):
        with patch.object(resources, "resource_snapshot", return_value=copy.deepcopy(snapshot or self.snapshot)):
            return resources.check_budget(baseline, prospective_bytes=prospective)

    def test_baseline_written_once_and_reused(self):
        baseline = self.admit()
        path = Path(baseline["baseline_path"])
        original = path.read_bytes()
        self.assertTrue(baseline["passed"])
        self.assertEqual(json.loads(original), baseline)
        with self.assertRaises(FileExistsError):
            self.admit()
        current = copy.deepcopy(self.snapshot)
        current["target_allocated_bytes"] += 10 * GIB
        self.assertTrue(self.check(path, current)["passed"])
        current["target_allocated_bytes"] += 8 * GIB
        verdict = self.check(path, current)
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["forecast_growth_bytes"], 21 * GIB)
        self.assertEqual(path.read_bytes(), original)

    def test_git_growth_counts_against_original_budget(self):
        baseline = self.admit()
        current = copy.deepcopy(self.snapshot)
        current["target_allocated_bytes"] += 10 * GIB
        current["git_allocated_bytes"] += 8 * GIB
        self.assertFalse(self.check(baseline, current)["passed"])

    def test_deleting_old_target_files_does_not_offset_git_growth(self):
        baseline = self.admit()
        current = copy.deepcopy(self.snapshot)
        current["target_allocated_bytes"] -= 20 * GIB
        current["git_allocated_bytes"] += 18 * GIB
        result = self.check(baseline, current)
        self.assertEqual(result["allocated_growth_bytes"], 18 * GIB)
        self.assertFalse(result["passed"])

    def test_each_filesystem_keeps_reserve_after_prospective_batch(self):
        baseline = self.admit()
        for key in ("guest_free_bytes", "host_free_bytes"):
            with self.subTest(key=key):
                current = copy.deepcopy(self.snapshot)
                current[key] = 33 * GIB
                self.assertTrue(self.check(baseline, current)["passed"])
                current[key] -= 1
                self.assertFalse(self.check(baseline, current)["passed"])

    def test_memory_reserve_is_separate_from_disk_budget(self):
        baseline = self.admit()
        current = copy.deepcopy(self.snapshot)
        current["mem_available_bytes"] = 4 * GIB
        self.assertTrue(self.check(baseline, current)["passed"])
        current["mem_available_bytes"] -= 1
        self.assertFalse(self.check(baseline, current)["passed"])

    def test_budget_may_decrease_but_not_increase_beyond_20gib(self):
        baseline = self.admit(total_budget_bytes=10 * GIB)
        self.assertFalse(self.check(baseline, prospective=11 * GIB)["passed"])
        baseline["total_budget_bytes"] = 21 * GIB
        with self.assertRaises(ValueError):
            self.check(baseline)

    def test_invalid_budget_is_rejected_before_creating_output(self):
        for value in (-1, 0, True, 1.5, 21 * GIB):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.admit(total_budget_bytes=value)
            self.assertFalse(self.output.exists())

    def test_failed_admission_is_preserved(self):
        self.snapshot["host_free_bytes"] = 30 * GIB
        result = self.admit()
        self.assertFalse(result["passed"])
        self.assertTrue(Path(result["baseline_path"]).is_file())

    def test_prospective_cannot_be_negative_or_fractional(self):
        baseline = self.admit()
        for value in (-1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.check(baseline, prospective=value)

    def test_nonwsl_without_host_is_supported(self):
        self.snapshot.update(is_wsl=False, host_volume=None, host_free_bytes=None)
        baseline = self.admit()
        self.assertTrue(self.check(baseline)["passed"])

    def test_resource_identity_and_reserves_cannot_change(self):
        baseline = self.admit()
        for key, value in (("host_volume", "/mnt/d"), ("is_wsl", False),
                           ("target_path", "/different/target")):
            current = copy.deepcopy(self.snapshot)
            current[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check(baseline, current)
        baseline["disk_reserve_bytes"] -= 1
        with self.assertRaises(ValueError):
            self.check(baseline)

    def test_output_must_be_beneath_measured_target(self):
        for output in (self.repo / "elsewhere", self.target):
            with self.subTest(output=output), self.assertRaises(ValueError):
                resources.preflight(self.target, output)

    def test_wsl_host_requires_real_mount(self):
        with patch.object(resources, "is_wsl", return_value=True), \
                patch.object(resources, "_directory", return_value=Path("/mnt/c")), \
                patch.object(Path, "is_mount", return_value=False):
            with self.assertRaises(ValueError):
                resources._host_volume(None)

    def test_wsl_default_and_explicit_host_volume(self):
        with patch.object(resources, "is_wsl", return_value=True), \
                patch.object(resources, "_directory", side_effect=lambda value: Path(value)), \
                patch.object(Path, "is_mount", return_value=True):
            self.assertEqual(resources._host_volume(None), (Path("/mnt/c"), True))
            self.assertEqual(resources._host_volume("/mnt/d"), (Path("/mnt/d"), True))

    def test_promoted_envelope_is_exact_recorded_source(self):
        promoted = Path(resources.__file__).with_name("run_memory_envelope.py")
        self.assertEqual(hashlib.sha256(promoted.read_bytes()).hexdigest(),
                         "76c594e1d5dc3c9a9c825369a8c3068222dfe11de6ae94db07e02e1f14e055d2")


if __name__ == "__main__":
    unittest.main()
