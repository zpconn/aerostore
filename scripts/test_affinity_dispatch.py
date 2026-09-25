#!/usr/bin/env python3
"""Real-process checks for the optional temporary-signature dispatcher.

Select the built bench with AEROSTORE_CONTENTION_BINARY. Evidence remains under
target/affinity-validation/integration by default. The fault test uses a pidfd to
pause only its own idle worker before admission; it never resets a live mapping.
These are functional tests, not throughput or worker-death availability trials.
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
from test_calibrated_worker_pause import process_identity, read_json_if_ready, wait_for


FOREGROUND_ID_START = 1_000_000


def observations(path):
    """The final line may still be buffered during the controlled pause."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return []
    lines = data.split(b"\n")[:-1]
    return [json.loads(line) for line in lines if line]


def signature(message):
    return message["callsign"], message["tail"]


def expected_owners(items, workers, ttl_ms, identity=False, active_families=3):
    """Reference routing uses offered times and input signatures, never outcomes."""
    cache = {}
    cursor = 0
    owners = {}
    for item in sorted(items, key=lambda row: row["receipt"]["message"]["id"]):
        message = item["receipt"]["message"]
        sequence = message["id"] - FOREGROUND_ID_START
        if identity:
            owner = (sequence % active_families) % workers
        else:
            key = signature(message)
            entry = cache.get(key)
            arrival = item["scheduled_ns"]
            if entry is not None and arrival < entry[1]:
                owner = entry[0]
            else:
                owner = cursor
                cursor = (cursor + 1) % workers
            cache[key] = owner, arrival + ttl_ms * 1_000_000
        owners[message["id"]] = owner
    return owners


def assignment_fingerprint(owners):
    value = 0xCBF29CE484222325
    for message_id, owner in sorted(owners.items()):
        for byte in ((message_id - FOREGROUND_ID_START).to_bytes(8, "little")
                     + owner.to_bytes(8, "little")):
            value = ((value ^ byte) * 0x100000001B3) & ((1 << 64) - 1)
    return f"{value:016x}"


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"), "select a built benchmark binary explicitly")
class AffinityDispatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        parent = Path(os.environ.get("AEROSTORE_AFFINITY_TEST_OUTPUT",
                                     gate.ROOT / "target/affinity-validation/integration"))
        parent.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="dispatch-regression-", dir=parent))

    def command(self, directory, dispatch, pattern, evidence, rate, ttl_ms):
        command = [str(self.binary), "--engine", "aerostore", "--mode", "sustained",
                   "--workload", "calibrated", "--dispatch", dispatch,
                   "--signature-pattern", pattern, "--evidence", evidence,
                   "--workers", "2", "--families", "4", "--hot-percent", "0",
                   "--seed", "20260925", "--seconds", "3", "--arrival-rate", str(rate),
                   "--max-messages", "100", "--max-backlog", "100", "--shm-mib", "128",
                   "--output", str(directory / "report.json")]
        if ttl_ms is not None:
            command += ["--affinity-ttl-ms", str(ttl_ms)]
        return command

    def assert_success(self, directory, receipt, count, full=True):
        self.assertFalse(receipt["timed_out"], receipt)
        self.assertTrue(receipt["owned_processes_terminated"], receipt)
        report = json.loads((directory / "report.json").read_text())
        self.assertEqual(receipt["exit_code"], 0, report)
        self.assertIs(report["passed"], True)
        self.assertIs(report["completed"], True)
        run = report["runs"][0]
        self.assertEqual(run["completed_messages"], count)
        self.assertEqual(run["offered_messages"], count)
        self.assertEqual(run["correctness_history_verified"], full)
        self.assertEqual(run["oracle_status"], "Valid" if full else "NotCheckedMetricsOnly")
        self.assertEqual(gate.continuous_timing_errors(run, count), [])
        self.assertEqual(run["completed_by_worker"][-2:], [0, 0])
        items = observations(Path(run["evidence_directory"]) / "history.jsonl")
        self.assertEqual(len(items), count)
        self.assertEqual(sorted(item["receipt"]["message"]["id"] for item in items),
                         list(range(FOREGROUND_ID_START, FOREGROUND_ID_START + count)))
        return report, run, items

    def assert_dispatch_and_worker_fifo(self, run, items, ttl_ms, identity=False):
        expected = expected_owners(items, 2, ttl_ms, identity=identity)
        counts = [0, 0, 0, 0]
        previous = {}
        for item in items:
            message_id = item["receipt"]["message"]["id"]
            owner = expected[message_id]
            self.assertEqual(item["worker"], owner)
            counts[owner] += 1
            self.assertEqual(item["scheduled_ns"] - run["admission_started_ns"],
                             (message_id - FOREGROUND_ID_START) * 1_000_000_000
                             // run["offered_rate_per_second"])
            if owner in previous:
                before = previous[owner]
                self.assertLess(before["receipt"]["message"]["id"], message_id)
                self.assertLessEqual(before["receipt"]["finished"], item["message_started_ns"])
            previous[owner] = item
        self.assertEqual(run["completed_by_worker"], counts)
        audit = run["dispatch_audit"]
        self.assertIs(audit["checked"], True)
        self.assertIs(audit["passed"], True)
        self.assertEqual(audit["worker_counts"], counts[:2])
        self.assertEqual(audit["assignment_fingerprint"], assignment_fingerprint(expected))
        return expected

    def run_plain(self, name, dispatch, evidence, ttl_ms):
        directory = self.output / name
        directory.mkdir()
        command = self.command(directory, dispatch, "mixed", evidence, 12, ttl_ms)
        before = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        receipt = gate.run_process(command, directory / "run.log", 30)
        proof = {"command": command, "process_receipt": receipt,
                 "binary_before_sha256": before,
                 "binary_after_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                 "scope": "Functional corpus and dispatcher regression; not a timed comparison."}
        (directory / "test-evidence.json").write_text(json.dumps(proof, indent=2) + "\n")
        self.assertEqual(proof["binary_before_sha256"], proof["binary_after_sha256"])
        report, run, items = self.assert_success(directory, receipt, 36, evidence == "full")
        self.assert_dispatch_and_worker_fifo(run, items, ttl_ms or 0, dispatch == "identity")
        self.assertEqual(gate.calibrated_report_errors(run, report["config"], run["completed_by_worker"]), [])
        return report, run, items

    def test_mixed_aliases_preserve_corpus_with_identity_control_and_metrics_companion(self):
        corpora = []
        for name, dispatch, evidence, ttl in [
                ("alias-identity", "identity", "full", None),
                ("alias-affinity-full", "signature-affinity", "full", 600),
                ("alias-affinity-metrics", "signature-affinity", "metrics", 600)]:
            report, run, items = self.run_plain(name, dispatch, evidence, ttl)
            ordered = sorted(items, key=lambda row: row["receipt"]["message"]["id"])
            messages = [item["receipt"]["message"] for item in ordered]
            corpora.append(messages)
            for sequence, message in enumerate(messages):
                identity = sequence % 3
                form = ((sequence // 3) // 2) % 3
                self.assertEqual(message["callsign"], 0 if form == 2 else 100 + identity // 4)
                self.assertEqual(message["tail"], 0 if form == 1 else 10_000 + identity)
                self.assertFalse(ordered[sequence]["receipt"]["body"]["outcome"]["missing_family"])
            self.assertEqual(run["per_flight_order"].get("required", True), dispatch == "identity")
            if dispatch == "identity":
                self.assertTrue(run["per_flight_order"]["passed"])
            else:
                self.assertGreater(run["dispatch_audit"]["hits"], 0)
                self.assertGreater(run["dispatch_audit"]["expired_misses"], 0)
                self.assertGreater(run["dispatch_audit"]["flights_with_multiple_workers"], 0)
            self.assertTrue(all(bool(item["receipt"]["body"]["operations"]) == (evidence == "full")
                                for item in items))
        self.assertEqual(corpora[0], corpora[1])
        self.assertEqual(corpora[1], corpora[2])

    def test_expired_affinity_can_overtake_paused_work_without_invalidating_history(self):
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            self.skipTest("Linux pidfds are required for the controlled pause")
        directory = self.output / "expiry-overtakes-paused-worker"
        directory.mkdir()
        command = self.command(directory, "signature-affinity", "both", "full", 6, 100)
        evidence = {"command": command, "passed": False,
                    "binary_before_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "scope": "An idle owned foreground worker is paused before admission; input-time affinity may expire while its work remains queued. No worker is killed and no engine recovery is claimed."}
        pid_queue = queue.Queue()
        original_popen = subprocess.Popen

        def capture_process(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            pid_queue.put(process.pid)
            return process

        descriptor = None
        stopped = False
        worker = None
        try:
            with patch.object(gate.subprocess, "Popen", capture_process), ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(gate.run_process, command, directory / "run.log", 25)
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

                    case, schedule = wait_for(offered_schedule, time.monotonic() + 8, "offered affinity schedule")
                    self.assertEqual(schedule["offered_by_worker"], [9, 9, 0, 0])
                    config_path = (case / "worker-0.json").resolve(strict=True)
                    config = json.loads(config_path.read_text())
                    self.assertEqual(config["worker_id"], 0)
                    self.assertEqual(config["engine"], "aerostore")
                    self.assertEqual(config["workload"], "calibrated")
                    coordinator_pid = config["expected_parent_pid"]
                    coordinator = process_identity(coordinator_pid)
                    self.assertEqual(coordinator["parent"], owner["pid"])
                    self.assertEqual(coordinator["arguments"][1:],
                                     ["--internal-coordinator", str(case / "private-config.json")])

                    def find_worker():
                        children = Path(f"/proc/{coordinator_pid}/task/{coordinator_pid}/children").read_text().split()
                        for child in children:
                            try:
                                identity = process_identity(int(child))
                            except (OSError, ValueError, IndexError):
                                continue
                            if (identity["parent"] == coordinator_pid
                                    and identity["arguments"][1:] == ["--internal-worker", str(config_path)]):
                                return identity
                        return None

                    worker = wait_for(find_worker, time.monotonic() + 1, "exact owned foreground worker")
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
                             time.monotonic() + 1, "foreground worker stopped")
                    paused = time.monotonic_ns()
                    self.assertLess(paused, schedule["admission_started_ns"],
                                    "pause must occur while idle, before its first transaction")
                    evidence.update(worker_identity=worker, schedule=schedule, paused_ns=paused)
                    resume_after = schedule["admission_started_ns"] + 1_200_000_000

                    def overtaking_receipt():
                        self.assertFalse(future.done(), "benchmark stopped before controlled resume")
                        self.assertIn(validate_worker()["state"], {"T", "t"})
                        if time.monotonic_ns() < resume_after:
                            return None
                        for item in observations(case / "history.jsonl"):
                            if item["receipt"]["message"]["id"] == FOREGROUND_ID_START + 3:
                                self.assertEqual(item["worker"], 1)
                                return item
                        return None

                    evidence["overtaking_receipt"] = wait_for(overtaking_receipt, time.monotonic() + 5,
                                                               "later same-flight message on surviving worker")
                    validate_worker()
                    evidence["resumed_ns"] = time.monotonic_ns()
                    signal.pidfd_send_signal(descriptor, signal.SIGCONT)
                    stopped = False
                finally:
                    if stopped and descriptor is not None:
                        try:
                            observed = process_identity(worker["pid"])
                            if (observed["start_ticks"] == worker["start_ticks"]
                                    and observed["arguments"] == worker["arguments"]):
                                signal.pidfd_send_signal(descriptor, signal.SIGCONT)
                        except (OSError, ValueError, IndexError):
                            pass
                    receipt = future.result(timeout=35)
                    evidence["process_receipt"] = receipt

            report, run, items = self.assert_success(directory, receipt, 18)
            self.assert_dispatch_and_worker_fifo(run, items, 100)
            by_id = {item["receipt"]["message"]["id"]: item for item in items}
            earlier, later = by_id[FOREGROUND_ID_START], by_id[FOREGROUND_ID_START + 3]
            self.assertEqual(signature(earlier["receipt"]["message"]), signature(later["receipt"]["message"]))
            self.assertLessEqual(later["receipt"]["finished"], evidence["resumed_ns"])
            self.assertLessEqual(evidence["resumed_ns"], earlier["message_started_ns"])
            self.assertGreater(earlier["receipt"]["body"]["outcome"]["ignored_stale"], 0)
            self.assertGreater(sum(item["receipt"]["body"]["outcome"]["ignored_stale"] for item in items), 0)
            self.assertFalse(run["per_flight_order"]["required"])
            self.assertFalse(run["per_flight_order"]["passed"])
            self.assertGreater(run["dispatch_audit"]["expired_owner_changes"], 0)
            self.assertEqual(gate.calibrated_report_errors(run, report["config"], [9, 9, 0, 0]), [])
            # Exercise usefulness classification on these real stale outcomes.
            # This is the scope assessor, not a fabricated campaign/build receipt.
            assessment = gate.assess_calibrated_scope(
                {"continuous_timing_passed": True, "correctness_companion_verified": True, "reasons": []},
                run, report["config"], {"slo_ms": 50, "minimum_drain_fraction": .9})
            self.assertFalse(assessment["useful_work_passed"])
            self.assertFalse(assessment["foreground_effect_coverage_passed"])
            self.assertFalse(assessment["performance_passed"])
            self.assertFalse(assessment["qualified_capacity_trial"])
            evidence["qualification_scope_check"] = assessment
            evidence["binary_after_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
            self.assertEqual(evidence["binary_before_sha256"], evidence["binary_after_sha256"])
            evidence["passed"] = True
        finally:
            if descriptor is not None:
                os.close(descriptor)
            (directory / "test-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
