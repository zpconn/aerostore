#!/usr/bin/env python3
"""Real worker-process evidence checks with an explicitly selected feature build.

Build the contention benchmark with --features retry-diagnostics and select it
using AEROSTORE_CONTENTION_BINARY. These checks do not measure diagnostic cost:
they verify bounded evidence survives native and service worker protocols.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import qualify_hyperfeed as gate


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"),
                     "select a retry-diagnostics feature benchmark explicitly")
class RetryDiagnosticProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        parent = Path(os.environ.get("AEROSTORE_RETRY_DIAGNOSTICS_TEST_OUTPUT",
                                     gate.ROOT / "target/retry-diagnostics-validation/integration"))
        parent.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="retry-evidence-", dir=parent))

    def run_case(self, name, engine, enabled, *options):
        directory = self.output / name
        directory.mkdir()
        command = [str(self.binary), "--engine", engine, "--mode", "scenarios",
                   "--families", "16", "--workers", "4", "--seed", "20260925", "--hot-percent", "0",
                   "--shm-mib", "128", "--retry-diagnostics", "on" if enabled else "off",
                   "--output", str(directory / "report.json"), *options]
        before = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        receipt = gate.run_process(command, directory / "run.log", 60)
        evidence = {"command": command, "process_receipt": receipt,
                    "binary_before_sha256": before,
                    "binary_after_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "scope": "Functional worker evidence regression; no diagnostic-overhead or capacity claim."}
        (directory / "test-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        self.assertFalse(receipt["timed_out"], evidence)
        self.assertTrue(receipt["owned_processes_terminated"], evidence)
        self.assertEqual(evidence["binary_before_sha256"], evidence["binary_after_sha256"])
        report = json.loads((directory / "report.json").read_text())
        self.assertTrue(report["retry_diagnostics_compiled"], "use a feature-enabled binary")
        return receipt, report

    def test_complete_sweeps_preserve_cumulative_done_evidence_in_each_transport(self):
        for engine in ("aerostore", "service-unix"):
            for enabled in (False, True):
                with self.subTest(engine=engine, enabled=enabled):
                    receipt, report = self.run_case(
                        f"sweeps-{engine}-{enabled}", engine, enabled,
                        "--mode", "sustained", "--workload", "calibrated", "--evidence", "full",
                        "--maintenance-mode", "sweep", "--seconds", "3", "--arrival-rate", "64",
                        "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1",
                        "--max-messages", "1000", "--max-backlog", "1000")
                    self.assertEqual(receipt["exit_code"], 0, report)
                    self.assertTrue(report["passed"])
                    self.assertTrue(report["completed"])
                    self.assertEqual(len(report["runs"]), 1)
                    for run in report["runs"]:
                        self.assertTrue(run["correctness_history_verified"], run)
                        self.assertEqual(gate.experiment_report_errors(run, report["config"]), [])
                        traces = run["worker_retry_diagnostics"]
                        self.assertEqual(sum(trace["failed_attempts"] for trace in traces), run["retries"])
                        self.assertEqual(run["completed_messages"], 196)
                        self.assertEqual(run["store_metrics"]["commits"], run["completed_transactions"])
                        self.assertTrue(run["global_maintenance_sweep_complete"])
                        for trace in traces:
                            self.assertEqual(gate.retry_trace_errors(trace, enabled), [])
                            for sample in trace["samples"]:
                                self.assertEqual(sample["error_kind"], "conflict")
                                self.assertTrue(sample["cleanup_ok"])
                                self.assertTrue(sample["metrics_status"]["complete"])
                                expected = "local_adapter" if engine == "aerostore" else "service_cache"
                                self.assertEqual(sample["metrics_status"]["source"], expected)

    def test_failed_job_keeps_pre_drop_counters_in_both_trace_modes(self):
        for engine in ("aerostore", "service-unix"):
            for enabled in (False, True):
                with self.subTest(engine=engine, enabled=enabled):
                    receipt, report = self.run_case(
                        f"failed-job-{engine}-{enabled}", engine, enabled,
                        "--mode", "sustained", "--workload", "calibrated", "--evidence", "full",
                        "--maintenance-mode", "sweep", "--max-maintenance-batches", "1",
                        "--seconds", "2", "--arrival-rate", "16", "--hot-percent", "0",
                        "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1",
                        "--max-messages", "100", "--max-backlog", "100")
                    self.assertNotEqual(receipt["exit_code"], 0)
                    self.assertFalse(report["passed"])
                    run = report["runs"][0]
                    self.assertFalse(run["execution_completed"])
                    directory = Path(run["evidence_directory"])
                    progress = json.loads((directory / "failure-progress.json").read_text())
                    self.assertIn("batch", progress["error"])
                    evidence = progress["failure_evidence"]
                    self.assertEqual(evidence["version"], 1)
                    self.assertTrue(evidence["metrics_status"]["complete"])
                    self.assertGreaterEqual(evidence["metrics"]["commits"], 1)
                    self.assertEqual(gate.retry_trace_errors(evidence["retry_diagnostics"], enabled), [])
                    history = [json.loads(line) for line in (directory / "history.jsonl").read_text().splitlines()]
                    own = [item for item in history if item["worker"] == progress["worker"]]
                    self.assertEqual(evidence["metrics"]["commits"], len(own))
                    self.assertEqual(progress["completed_transactions"], len(history))
                    self.assertGreaterEqual(progress["completed_transactions"], len(own))
                    self.assertEqual(progress["completed_by_worker"][progress["worker"]], 0)
                    self.assertTrue(all(not item["job_completed"] for item in own))

    @unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_DEFAULT_BINARY"),
                         "select a default-feature binary to check diagnostic rejection")
    def test_default_binary_rejects_requested_branch_diagnostics(self):
        binary = Path(os.environ["AEROSTORE_CONTENTION_DEFAULT_BINARY"]).resolve(strict=True)
        directory = self.output / "default-feature-rejection"
        directory.mkdir()
        command = [str(binary), "--engine", "aerostore", "--retry-diagnostics", "on",
                   "--output", str(directory / "report.json")]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=10)
        (directory / "stdout.log").write_text(completed.stdout)
        (directory / "stderr.log").write_text(completed.stderr)
        (directory / "test-evidence.json").write_text(json.dumps({"command": command,
            "exit_code": completed.returncode, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}, indent=2) + "\n")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("retry-diagnostics", completed.stderr)
        self.assertIn("feature", completed.stderr)
        if (directory / "report.json").exists():
            self.assertFalse(json.loads((directory / "report.json").read_text()).get("passed", False))

    def test_setup_failure_emits_exactly_one_error_without_fabricated_metrics(self):
        directory = self.output / "setup-failure"
        directory.mkdir()
        command = [str(self.binary), "--internal-worker", str(directory / "missing-config.json")]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=10)
        (directory / "stdout.jsonl").write_text(completed.stdout)
        (directory / "stderr.log").write_text(completed.stderr)
        (directory / "test-evidence.json").write_text(json.dumps({"command": command,
            "exit_code": completed.returncode, "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest()}, indent=2) + "\n")
        self.assertNotEqual(completed.returncode, 0)
        replies = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(len(replies), 1)
        self.assertEqual(set(replies[0]), {"Error"})
        self.assertIsNone(replies[0]["Error"]["evidence"])


if __name__ == "__main__":
    unittest.main()
