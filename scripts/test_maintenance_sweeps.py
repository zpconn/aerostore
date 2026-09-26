#!/usr/bin/env python3
"""Real-process regressions for query-discovered maintenance batches.

Select a freshly built benchmark with AEROSTORE_CONTENTION_BINARY. Evidence is
retained under target/maintenance-validation/integration by default. These are
functional checks, not a throughput comparison or worker-death recovery test.
The pause case sends signals only through a verified owned worker's pidfd.
"""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import copy
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
from test_calibrated_worker_pause import process_identity, read_json_if_ready, wait_for


POLICY = {"slo_ms": 100, "max_noop_fraction": .25,
          "minimum_drain_fraction": .95, "outcome_tolerance": 0}


def read_history(directory):
    return [json.loads(line) for line in (directory / "history.jsonl").read_text().splitlines()]


def trial(report, receipt):
    return {"config": report["config"], "report": report,
            "exit_code": receipt["exit_code"], "timed_out": receipt["timed_out"],
            "source_stable": True}


def foreground_messages(items):
    return sorted((item["receipt"]["message"] for item in items
                   if item["workload_class"] == "foreground"), key=lambda message: message["id"])


def timer_inputs(items):
    """Logical timer inputs are fixed; discovered batch counts may differ."""
    inputs = {}
    for item in items:
        if item["workload_class"] == "foreground":
            continue
        message = copy.deepcopy(item["receipt"]["message"])
        job_id = item.get("job_id", message["id"])
        message["id"] = job_id
        if job_id in inputs and inputs[job_id] != message:
            raise AssertionError("one maintenance job changed its input/cutoff between transactions")
        inputs[job_id] = message
    return [inputs[key] for key in sorted(inputs)]


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"), "select a built benchmark binary explicitly")
class MaintenanceSweepTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        parent = Path(os.environ.get("AEROSTORE_MAINTENANCE_TEST_OUTPUT",
                                     gate.ROOT / "target/maintenance-validation/integration"))
        parent.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="sweep-regression-", dir=parent))

    def command(self, directory, mode="sweep", evidence="full", cap=4096):
        return [str(self.binary), "--engine", "aerostore", "--mode", "sustained",
                "--workload", "calibrated", "--maintenance-mode", mode,
                "--projection-batch-size", "4", "--housekeeping-batch-size", "32",
                "--max-maintenance-batches", str(cap), "--evidence", evidence,
                "--workers", "4", "--families", "16", "--hot-percent", "0",
                "--seed", "20260925", "--seconds", "3", "--arrival-rate", "16",
                "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1",
                "--max-messages", "100", "--max-backlog", "100", "--shm-mib", "128",
                "--output", str(directory / "report.json")]

    def plain(self, name, **options):
        directory = self.output / name
        directory.mkdir()
        command = self.command(directory, **options)
        before = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        receipt = gate.run_process(command, directory / "run.log", 45)
        evidence = {"command": command, "process_receipt": receipt,
                    "binary_before_sha256": before,
                    "binary_after_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "scope": "Functional maintenance job and transaction accounting regression; not a throughput comparison."}
        (directory / "test-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        self.assertFalse(receipt["timed_out"], evidence)
        self.assertTrue(receipt["owned_processes_terminated"], evidence)
        self.assertEqual(evidence["binary_before_sha256"], evidence["binary_after_sha256"])
        return directory, receipt, json.loads((directory / "report.json").read_text())

    def assert_success(self, report, receipt, full=True):
        self.assertEqual(receipt["exit_code"], 0, report)
        self.assertIs(report["passed"], True)
        self.assertIs(report["completed"], True)
        self.assertEqual(len(report["runs"]), 1)
        run = report["runs"][0]
        self.assertEqual(run["completed_messages"], 52)
        self.assertEqual(run["offered_messages"], 52)
        self.assertEqual(run["completed_by_worker"], [12, 12, 12, 12, 2, 2])
        self.assertEqual(run["oracle_status"], "Valid" if full else "NotCheckedMetricsOnly")
        self.assertEqual(run["correctness_history_verified"], full)
        self.assertEqual(gate.continuous_timing_errors(run, 52), [])
        self.assertEqual(gate.calibrated_report_errors(run, report["config"], run["completed_by_worker"]), [])
        assessment = gate.assess_trial(trial(report, receipt), POLICY)
        self.assertTrue(assessment["execution_valid"], assessment)
        self.assertFalse(assessment["qualified_capacity_trial"])
        items = read_history(Path(run["evidence_directory"]))
        self.assertEqual(len(foreground_messages(items)), 48)
        return run, items

    def assert_sweep_receipts(self, run, items, full=True):
        self.assertEqual(run["completed_transactions"], len(items))
        self.assertEqual(run["store_metrics"]["commits"], len(items))
        self.assertGreater(len(items), run["completed_messages"])
        self.assertIs(run["global_maintenance_sweep_complete"], True)
        grouped = defaultdict(list)
        transaction_ids = set()
        for item in items:
            message_id = item["receipt"]["message"]["id"]
            self.assertNotIn(message_id, transaction_ids)
            transaction_ids.add(message_id)
            self.assertEqual(bool(item["receipt"]["body"]["operations"]), full)
            if item["workload_class"] != "foreground":
                grouped[item["job_id"]].append(item)
        jobs = run["maintenance_jobs"]
        self.assertEqual(len(jobs), 4)
        self.assertEqual(set(grouped), {job["job_id"] for job in jobs})
        self.assertEqual(Counter(job["class"] for job in jobs), {"projection": 2, "housekeeping": 2})
        timer_inputs(items)  # Every batch has the same original event-time cutoff.
        for job in jobs:
            batches = grouped[job["job_id"]]
            self.assertEqual([item["batch_index"] for item in batches], list(range(job["batches"])))
            self.assertEqual([item["job_completed"] for item in batches], [False] * (len(batches) - 1) + [True])
            self.assertEqual([item["maintenance_terminal"] for item in batches], [False] * (len(batches) - 1) + [True])
            self.assertEqual(job["nonempty_batches"], len(batches) - 1)
            self.assertEqual(job["terminal_batches"], 1)
            self.assertIs(job["terminal_empty"], True)
            self.assertEqual(job["first_transaction_id"], batches[0]["receipt"]["message"]["id"])
            self.assertEqual(job["terminal_transaction_id"], batches[-1]["receipt"]["message"]["id"])
            self.assertEqual(job["started_ns"], batches[0]["message_started_ns"])
            self.assertEqual(job["finished_ns"], batches[-1]["receipt"]["finished"])
            self.assertEqual(job["received_ns"], batches[-1]["received_ns"])
            self.assertEqual(job["retries"], sum(item["retries"] for item in batches))
            effect = "claimed_events" if job["class"] == "projection" else "expired_records"
            limit = 4 if job["class"] == "projection" else 32
            processed = []
            for index, item in enumerate(batches):
                self.assertEqual(item["worker"], job["worker"])
                self.assertEqual(item["job_ordinal"], job["job_ordinal"])
                self.assertEqual(item["scheduled_ns"], job["scheduled_ns"])
                self.assertEqual(item["receipt"]["message"]["id"],
                                 8_000_000_000 + (job["job_id"] - 4_000_000_000) * 4096 + index)
                count = item["receipt"]["body"]["outcome"][effect]
                processed.append(count)
                self.assertTrue(0 <= count <= limit)
                self.assertEqual(count == 0, index == len(batches) - 1)
                if index:
                    self.assertLessEqual(batches[index - 1]["receipt"]["finished"], item["message_started_ns"])
                if full:
                    query_kind = "GlobalDue" if job["class"] == "projection" else "GlobalExpired"
                    queries = [operation["Query"] for operation in item["receipt"]["body"]["operations"]
                               if "Query" in operation and query_kind in operation["Query"]["query"]]
                    self.assertEqual(len(queries), 1)
                    self.assertEqual(min(len(queries[0]["rows"]), limit), count)
                    self.assertEqual(queries[0]["rows"] == [], item["maintenance_terminal"])
            self.assertEqual(job["processed_rows"], sum(processed))
        for role in ("projection", "housekeeping"):
            first = min((job for job in jobs if job["class"] == role), key=lambda job: job["job_ordinal"])
            self.assertGreaterEqual(first["nonempty_batches"], 2)
        audit = run["maintenance_job_audit"]
        self.assertIs(audit["checked"], True)
        self.assertIs(audit["passed"], True)
        self.assertEqual(audit["scope"], "complete_sweep_batched_transactions")
        self.assertEqual(audit["completed_jobs"], len(jobs))
        for field, per_job in (("committed_batches", "batches"), ("nonempty_batches", "nonempty_batches"),
                               ("terminal_batches", "terminal_batches"), ("processed_rows", "processed_rows")):
            self.assertEqual(audit[field], sum(job[per_job] for job in jobs))
        names = {"Plan": "plan", "Position": "position", "GlobalProject": "global_projection",
                 "GlobalHousekeeping": "global_housekeeping"}
        transactions = Counter()
        for item in items:
            kind = item["receipt"]["message"]["kind"]
            transactions[names[kind if isinstance(kind, str) else next(iter(kind))]] += 1
        self.assertEqual(run["transaction_kinds"], transactions)
        self.assertEqual({kind: statistics["transactions"] for kind, statistics in run["per_kind"].items()}, transactions)
        return jobs

    def test_complete_sweeps_reconcile_jobs_transactions_and_reject_tampered_audits(self):
        _, receipt, report = self.plain("sweep-full")
        run, items = self.assert_success(report, receipt)
        self.assert_sweep_receipts(run, items)
        for field, value in [("terminal_empty", False), ("batches", 0), ("processed_rows", -1), ("job_id", 0)]:
            tampered = copy.deepcopy(report)
            tampered["runs"][0]["maintenance_jobs"][0][field] = value
            assessment = gate.assess_trial(trial(tampered, receipt), POLICY)
            self.assertFalse(assessment["execution_valid"], (field, assessment))
        _, control_receipt, control = self.plain("bounded-batch-control", mode="batch")
        control_run, control_items = self.assert_success(control, control_receipt)
        self.assertEqual(control_run["store_metrics"]["commits"], 52)
        self.assertFalse(control_run["global_maintenance_sweep_complete"])
        self.assertEqual(len(control_items), 52)
        self.assertEqual(foreground_messages(items), foreground_messages(control_items))
        self.assertEqual(timer_inputs(items), timer_inputs(control_items))

    def test_metrics_companion_never_claims_its_own_history_was_verified(self):
        _, full_receipt, full = self.plain("companion-full")
        full_run, full_items = self.assert_success(full, full_receipt)
        self.assert_sweep_receipts(full_run, full_items)
        _, metrics_receipt, metrics = self.plain("companion-metrics", evidence="metrics")
        run, items = self.assert_success(metrics, metrics_receipt, full=False)
        self.assert_sweep_receipts(run, items, full=False)
        self.assertEqual(foreground_messages(full_items), foreground_messages(items))
        self.assertEqual(timer_inputs(full_items), timer_inputs(items))
        self.assertEqual(gate.key(full["config"]), gate.key(metrics["config"]))
        assessment = gate.assess_trial(trial(metrics, metrics_receipt), POLICY)
        self.assertFalse(assessment["history_verified"])
        self.assertFalse(assessment["correctness_companion_verified"])
        paired = gate.assess_trial(trial(metrics, metrics_receipt), POLICY, {gate.key(full["config"])})
        self.assertTrue(paired["correctness_companion_verified"], paired)
        self.assertFalse(paired["history_verified"])
        changed = {**full["config"], "projection_batch_size": 8}
        mismatched = gate.assess_trial(trial(metrics, metrics_receipt), POLICY, {gate.key(changed)})
        self.assertFalse(mismatched["correctness_companion_verified"])

    def test_batch_cap_retains_committed_work_without_claiming_sweep_completion(self):
        _, receipt, report = self.plain("batch-cap-after-commit", cap=1)
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        run = report["runs"][0]
        self.assertFalse(run["execution_completed"])
        case = Path(run["evidence_directory"])
        progress = json.loads((case / "failure-progress.json").read_text())
        self.assertFalse(progress["passed"])
        self.assertFalse(progress["execution_completed"])
        self.assertIn("batch", progress["error"].lower())
        self.assertTrue(progress["pending_maintenance"])
        items = read_history(case)
        batches = [item for item in items if item["workload_class"] != "foreground"]
        self.assertTrue(batches, "the first successful maintenance commit was lost on cap failure")
        self.assertEqual(progress["completed_transactions"], len(items))
        self.assertEqual(progress["completed_by_worker"][-2:], [0, 0])
        self.assertTrue(all(item["batch_index"] == 0 and item["job_completed"] is False
                            and item["maintenance_terminal"] is False for item in batches))
        self.assertTrue(all(item["receipt"]["body"]["outcome"]["claimed_events"]
                            + item["receipt"]["body"]["outcome"]["expired_records"] > 0 for item in batches))

    def test_paused_maintenance_keeps_each_deadline_and_drains_complete_jobs(self):
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            self.skipTest("Linux pidfds are required for the controlled pause")
        directory = self.output / "paused-projection-drain"
        directory.mkdir()
        command = self.command(directory)
        evidence = {"command": command, "passed": False,
                    "binary_before_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "scope": "Pause an owned idle projection worker before its first transaction; retain both scheduled sweeps and drain after admission. No worker is killed."}
        pid_queue = queue.Queue()
        original_popen = subprocess.Popen

        def capture_process(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            pid_queue.put(process.pid)
            return process

        descriptor, worker, stopped = None, None, False
        try:
            with patch.object(gate.subprocess, "Popen", capture_process), ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(gate.run_process, command, directory / "run.log", 35)
                owner = process_identity(pid_queue.get(timeout=5))
                evidence["owner_identity"] = owner
                try:
                    def offered_schedule():
                        report = read_json_if_ready(directory / "report.json")
                        if not isinstance(report, dict) or not isinstance(report.get("evidence_directory"), str):
                            return None
                        case = Path(report["evidence_directory"]) / "aerostore-0"
                        schedule = read_json_if_ready(case / "offered-schedule.json")
                        return (case, schedule) if schedule is not None else None

                    case, schedule = wait_for(offered_schedule, time.monotonic() + 10, "offered maintenance schedule")
                    self.assertEqual(schedule["offered_by_worker"], [12, 12, 12, 12, 2, 2])
                    config_path = (case / "worker-4.json").resolve(strict=True)
                    config = json.loads(config_path.read_text())
                    self.assertEqual(config["worker_id"], 4)
                    self.assertEqual(config["engine"], "aerostore")
                    self.assertEqual(config["workload"], "calibrated")
                    coordinator_pid = config["expected_parent_pid"]
                    coordinator = process_identity(coordinator_pid)
                    self.assertEqual(coordinator["parent"], owner["pid"])
                    self.assertEqual(coordinator["arguments"][1:], ["--internal-coordinator", str(case / "private-config.json")])

                    def find_worker():
                        children = Path(f"/proc/{coordinator_pid}/task/{coordinator_pid}/children").read_text().split()
                        for child in children:
                            try:
                                identity = process_identity(int(child))
                            except (OSError, ValueError, IndexError):
                                continue
                            if identity["parent"] == coordinator_pid and identity["arguments"][1:] == ["--internal-worker", str(config_path)]:
                                return identity
                        return None

                    worker = wait_for(find_worker, time.monotonic() + 2, "exact owned projection worker")
                    descriptor = os.pidfd_open(worker["pid"])

                    def validate_worker():
                        observed = process_identity(worker["pid"])
                        self.assertEqual(observed["start_ticks"], worker["start_ticks"])
                        self.assertEqual(observed["arguments"], worker["arguments"])
                        self.assertEqual(observed["parent"], coordinator_pid)
                        self.assertEqual(json.loads(config_path.read_text())["expected_parent_pid"], coordinator_pid)
                        return observed

                    validate_worker()
                    signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
                    stopped = True
                    wait_for(lambda: True if validate_worker()["state"] in {"T", "t"} else None,
                             time.monotonic() + 1, "projection worker stopped")
                    evidence.update(worker_identity=worker, worker_config=str(config_path), schedule=schedule,
                                    paused_ns=time.monotonic_ns())
                    self.assertLess(evidence["paused_ns"], schedule["admission_started_ns"] + 1_000_000_000)
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
                            signal.pidfd_send_signal(descriptor, signal.SIGCONT)
                        except OSError:
                            pass
                    receipt = future.result(timeout=45)
                    evidence["process_receipt"] = receipt
            self.assertFalse(receipt["timed_out"], evidence)
            self.assertTrue(receipt["owned_processes_terminated"], evidence)
            evidence["binary_after_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
            self.assertEqual(evidence["binary_before_sha256"], evidence["binary_after_sha256"])
            report = json.loads((directory / "report.json").read_text())
            run, items = self.assert_success(report, receipt)
            jobs = self.assert_sweep_receipts(run, items)
            projection = sorted((job for job in jobs if job["class"] == "projection"), key=lambda job: job["job_ordinal"])
            self.assertEqual([job["scheduled_ns"] - run["admission_started_ns"] for job in projection],
                             [1_000_000_000, 2_000_000_000])
            self.assertTrue(all(job["started_ns"] >= evidence["resumed_ns"] for job in projection))
            self.assertLessEqual(projection[0]["finished_ns"], projection[1]["started_ns"])
            self.assertGreaterEqual(run["drain_confirmed_ns"], projection[-1]["received_ns"])
            self.assertGreaterEqual(run["workload_classes"]["projection"]["arrival_queue_delay_p99_us"], 2_000_000)
            evidence["passed"] = True
        finally:
            if descriptor is not None:
                os.close(descriptor)
            (directory / "pause-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
