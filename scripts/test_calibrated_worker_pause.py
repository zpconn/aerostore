#!/usr/bin/env python3
"""Pause only a disposable calibrated timer worker before its first job.

Select a freshly built binary with AEROSTORE_CONTENTION_BINARY. These tests
retain all evidence under target/calibrated-validation/fault-tests by default.
They require Linux pidfds and /proc; no database daemon or live mapping is reset.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

import qualify_hyperfeed as gate


def process_identity(pid: int) -> dict:
    base = Path("/proc") / str(pid)
    fields = (base / "stat").read_text().rsplit(") ", 1)[1].split()
    return {"pid": pid, "parent": int(fields[1]), "start_ticks": int(fields[19]),
            "state": fields[0],
            "arguments": [os.fsdecode(item) for item in (base / "cmdline").read_bytes().split(b"\0") if item]}


def wait_for(predicate, deadline: float, description: str):
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(.01)
    raise AssertionError(f"timed out waiting for {description}")


def read_json_if_ready(path: Path):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"), "select a built benchmark binary explicitly")
class CalibratedWorkerPauseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise unittest.SkipTest("Linux pidfd signal support is required")
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        parent = Path(os.environ.get("AEROSTORE_CALIBRATED_PAUSE_OUTPUT", gate.ROOT / "target/calibrated-validation/fault-tests"))
        parent.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="pause-regression-", dir=parent))

    def run_paused_case(self, name: str, max_backlog: int):
        directory = self.output / name
        directory.mkdir()
        report_path = directory / "report.json"
        command = [str(self.binary), "--engine", "aerostore", "--mode", "sustained",
                   "--workload", "calibrated", "--evidence", "full", "--workers", "1",
                   "--families", "4", "--hot-percent", "0", "--seed", "20260925",
                   "--seconds", "3", "--arrival-rate", "2", "--max-messages", "100",
                   "--max-backlog", str(max_backlog), "--projection-interval-seconds", "1",
                   "--housekeeping-interval-seconds", "1", "--shm-mib", "128",
                   "--output", str(report_path)]
        evidence = {"command": command, "passed": False,
                    "binary_before_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "scope": "Accelerated timer scheduling regression; SIGSTOP only while the selected owned worker is idle before its first transaction."}
        pid_queue = queue.Queue()
        original_popen = subprocess.Popen

        def capture_process(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            pid_queue.put(process.pid)
            return process

        descriptor = None
        stopped = False
        worker_identity = None
        worker_path = None
        try:
            with patch.object(gate.subprocess, "Popen", capture_process), ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(gate.run_process, command, directory / "run.log", 25)
                owner_pid = pid_queue.get(timeout=5)
                owner_identity = process_identity(owner_pid)
                evidence["owner_identity"] = owner_identity
                try:
                    def offered_schedule():
                        report = read_json_if_ready(report_path)
                        if not isinstance(report, dict) or not isinstance(report.get("evidence_directory"), str):
                            return None
                        case = Path(report["evidence_directory"]) / "aerostore-0"
                        schedule = read_json_if_ready(case / "offered-schedule.json")
                        return (case, schedule) if schedule is not None else None

                    case, schedule = wait_for(offered_schedule, time.monotonic() + 8, "published offered schedule")
                    self.assertEqual(schedule["offered_by_worker"], [6, 2, 2])
                    worker_path = (case / "worker-1.json").resolve(strict=True)
                    config = json.loads(worker_path.read_text())
                    self.assertEqual(config["worker_id"], 1)
                    self.assertEqual(config["workload"], "calibrated")
                    self.assertEqual(config["engine"], "aerostore")
                    coordinator_pid = config["expected_parent_pid"]
                    coordinator = process_identity(coordinator_pid)
                    self.assertEqual(coordinator["parent"], owner_pid)
                    self.assertEqual(coordinator["arguments"][1:], ["--internal-coordinator", str(case / "private-config.json")])

                    def find_worker():
                        for entry in Path("/proc").iterdir():
                            if not entry.name.isdigit():
                                continue
                            try:
                                candidate = process_identity(int(entry.name))
                            except (OSError, ValueError, IndexError):
                                continue
                            if candidate["parent"] == coordinator_pid and candidate["arguments"][1:] == ["--internal-worker", str(worker_path)]:
                                return candidate
                        return None

                    worker_identity = wait_for(find_worker, time.monotonic() + 2, "exact owned projection worker")
                    descriptor = os.pidfd_open(worker_identity["pid"])

                    def validate_worker():
                        observed = process_identity(worker_identity["pid"])
                        self.assertEqual(observed["start_ticks"], worker_identity["start_ticks"])
                        self.assertEqual(observed["arguments"], worker_identity["arguments"])
                        self.assertEqual(observed["parent"], coordinator_pid)
                        self.assertEqual(json.loads(worker_path.read_text())["expected_parent_pid"], coordinator_pid)
                        return observed

                    validate_worker()
                    signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
                    stopped = True
                    wait_for(lambda: True if validate_worker()["state"] in {"T", "t"} else None,
                             time.monotonic() + 1, "projection worker stopped")
                    paused_ns = time.monotonic_ns()
                    self.assertLess(paused_ns, schedule["admission_started_ns"] + 1_000_000_000,
                                    "worker was not paused before its first timer transaction")
                    evidence.update(worker_identity=worker_identity, worker_config=str(worker_path),
                                    schedule=schedule, paused_ns=paused_ns)
                    resume_at = schedule["admission_finished_ns"] + 200_000_000
                    while time.monotonic_ns() < resume_at:
                        self.assertFalse(future.done(), "benchmark exited before paused worker could resume")
                        time.sleep(max(0, min(.02, (resume_at - time.monotonic_ns()) / 1e9)))
                    validate_worker()
                    evidence["resumed_ns"] = time.monotonic_ns()
                    signal.pidfd_send_signal(descriptor, signal.SIGCONT)
                    stopped = False
                finally:
                    if stopped and descriptor is not None:
                        try:
                            # pidfd targets the original process even if its
                            # numeric PID was reused after an unexpected exit.
                            if worker_identity is not None and worker_path is not None:
                                observed = process_identity(worker_identity["pid"])
                                if (observed["start_ticks"] == worker_identity["start_ticks"]
                                        and observed["arguments"] == worker_identity["arguments"]):
                                    signal.pidfd_send_signal(descriptor, signal.SIGCONT)
                        except (OSError, ValueError, IndexError):
                            pass
                    outcome = future.result(timeout=35)
                    evidence["process_receipt"] = outcome
            self.assertFalse(outcome["timed_out"], evidence)
            self.assertTrue(outcome["owned_processes_terminated"], evidence)
            report = json.loads(report_path.read_text())
            evidence["binary_after_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
            self.assertEqual(evidence["binary_before_sha256"], evidence["binary_after_sha256"])
            return directory, case, evidence, outcome, report
        finally:
            if descriptor is not None:
                os.close(descriptor)
            (directory / "pause-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")

    def test_paused_timer_retains_every_tick_and_drains_after_admission(self):
        directory, case, evidence, outcome, report = self.run_paused_case("drain-after-deadline", 100)
        self.assertEqual(outcome["exit_code"], 0, report)
        self.assertTrue(report["passed"])
        run = report["runs"][0]
        self.assertEqual(run["oracle_status"], "Valid")
        self.assertEqual(run["completed_by_worker"], [6, 2, 2])
        self.assertEqual(run["offered_messages"], 10)
        self.assertGreaterEqual(run["drain_confirmed_ns"], evidence["resumed_ns"])
        self.assertEqual(gate.continuous_timing_errors(run, 10), [])
        history = [json.loads(line) for line in (case / "history.jsonl").read_text().splitlines()]
        projection = [item for item in history if item["workload_class"] == "projection"]
        self.assertEqual(len(projection), 2)
        self.assertEqual([item["scheduled_ns"] - run["admission_started_ns"] for item in projection], [1_000_000_000, 2_000_000_000])
        self.assertTrue(all(item["message_started_ns"] >= evidence["resumed_ns"] for item in projection))
        self.assertGreaterEqual(run["workload_classes"]["projection"]["arrival_queue_delay_p99_us"], 2_000_000)
        evidence["passed"] = True
        (directory / "pause-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")

    def test_paused_timer_exceeding_backlog_fails_with_uncompleted_offers_retained(self):
        directory, case, evidence, outcome, report = self.run_paused_case("backlog-rejected", 1)
        self.assertNotEqual(outcome["exit_code"], 0)
        self.assertFalse(report["passed"])
        progress = json.loads((case / "failure-progress.json").read_text())
        self.assertEqual(progress["worker"], 1)
        self.assertIn("calibrated offered backlog 2", progress["error"])
        self.assertEqual(progress["offered_by_worker"], [6, 2, 2])
        self.assertEqual(progress["completed_by_worker"][1], 0)
        self.assertFalse(progress["execution_completed"])
        self.assertFalse(progress["passed"])
        history = [json.loads(line) for line in (case / "history.jsonl").read_text().splitlines()]
        self.assertFalse(any(item["workload_class"] == "projection" for item in history))
        evidence["passed"] = True
        evidence["expected_benchmark_failure"] = True
        (directory / "pause-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
