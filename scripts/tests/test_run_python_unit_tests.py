import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "python_unit_runner", Path(__file__).resolve().parents[1] / "run_python_unit_tests.py")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class PythonUnitRunnerTests(unittest.TestCase):
    def test_tools_suites_are_selected_but_run_artifacts_are_not(self):
        paths = ["tools/history/test_rewrite.py", "tools/evidence/test_check_catalog.py",
                 "tools/tests/test_check_links.py", "runs/history/test_rewrite.py"]
        self.assertEqual(runner.test_directories(paths),
                         ["tools/evidence", "tools/history", "tools/tests"])

    def test_campaigns_with_same_module_name_use_separate_processes(self):
        paths = ["verification/first/test_generate.py", "verification/second/test_generate.py",
                 "scripts/test_formal_gate.py", "scripts/tests/test_capture.py",
                 "docs/bench_data/old/test_generate.py", "verification/first/generate.py"]
        directories = runner.test_directories(paths)
        self.assertEqual(directories, ["scripts", "scripts/tests", "verification/first", "verification/second"])
        with patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(runner.run_suites(Path("/repo"), directories, {}), 0)
        self.assertEqual(run.call_count, 4)
        self.assertEqual([call.args[0][5] for call in run.call_args_list], directories)

    def test_explicit_native_and_database_selectors_are_not_inherited(self):
        environment = {name: "explicit-integration-value" for name in runner.INTEGRATION_SELECTORS}
        environment["PATH"] = "/usr/bin"
        self.assertEqual(runner.unit_environment(environment), {"PATH": "/usr/bin"})
        self.assertEqual(environment["AEROSTORE_CONTENTION_BINARY"], "explicit-integration-value")

    def test_failure_is_reported_after_remaining_independent_suites_run(self):
        outcomes = [subprocess.CompletedProcess([], 1), subprocess.CompletedProcess([], 0)]
        with patch.object(runner.subprocess, "run", side_effect=outcomes) as run:
            self.assertEqual(runner.run_suites(Path("/repo"), ["scripts", "verification/guards"], {}), 1)
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
