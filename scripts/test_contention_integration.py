#!/usr/bin/env python3
"""Real-binary regressions for arrival timing and fail-closed evidence modes.

Run after rebuilding the bench:
  AEROSTORE_CONTENTION_BINARY=/absolute/path/to/hyperfeed_contention_crucible \
    python3 -m unittest discover -s scripts -p test_contention_integration.py -v

No PostgreSQL server is required. Outputs are retained under target/ by default;
set AEROSTORE_CONTENTION_INTEGRATION_OUTPUT to select their parent directory.
Without an explicitly selected binary these integration tests are skipped.
"""
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest

import qualify_hyperfeed as gate


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"), "set AEROSTORE_CONTENTION_BINARY to run real-binary regressions")
class ContentionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        output = Path(os.environ.get("AEROSTORE_CONTENTION_INTEGRATION_OUTPUT", gate.ROOT / "target/contention-integration"))
        output.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="regression-", dir=output))

    def run_case(self, name, *options, command_prefix=()):
        directory = self.output / name
        directory.mkdir()
        report = directory / "report.json"
        command = [str(self.binary), "--engine", "aerostore", "--mode", "sustained",
                   "--workload", "lifecycle", "--evidence", "metrics", "--workers", "1",
                   "--families", "2", "--hot-percent", "80", "--seed", "20260924",
                   "--seconds", "1", "--arrival-rate", "1", "--max-messages", "1000",
                   "--shm-mib", "128", "--output", str(report), *options]
        receipt = gate.run_process([*command_prefix, *command], directory / "run.log", 45)
        self.assertFalse(receipt["timed_out"], f"timeout; retained log: {directory / 'run.log'}")
        self.assertTrue(report.exists(), f"missing failure/success report: {directory}")
        return receipt, json.loads(report.read_text())

    def successful(self, name, *options):
        receipt, report = self.run_case(name, *options)
        self.assertEqual(receipt["exit_code"], 0, report)
        self.assertIs(report["passed"], True)
        self.assertIs(report["completed"], True)
        self.assertEqual(len(report["runs"]), 1)
        return report["runs"][0]

    def test_one_arrival_waits_for_full_admission_interval_and_metrics_omit_history(self):
        run = self.successful("one-arrival")
        self.assertEqual(run["offered_messages"], 1)
        self.assertEqual(run["completed_messages"], 1)
        self.assertEqual(run["completed_by_worker"], [1])
        self.assertGreaterEqual(run["elapsed_seconds"], 1.0)
        self.assertTrue(run["requested_duration_reached"])
        self.assertFalse(run["worker_message_cap_reached"])
        self.assertFalse(run["history_checked"])
        self.assertFalse(run["correctness_history_verified"])
        self.assertEqual(run["oracle_status"], "NotCheckedMetricsOnly")
        history = Path(run["evidence_directory"]) / "history.jsonl"
        observations = [json.loads(line) for line in history.read_text().splitlines()]
        self.assertEqual(len(observations), 1)
        observation = observations[0]
        self.assertEqual(observation["receipt"]["body"]["operations"], [])
        self.assertLessEqual(observation["scheduled_ns"], observation["message_started_ns"])
        self.assertEqual(observation["end_to_end_latency_ns"], observation["received_ns"] - observation["scheduled_ns"])
        self.assertEqual(observation["service_latency_ns"], observation["receipt"]["finished"] - observation["message_started_ns"])
        self.assertGreaterEqual(run["worker_stops"][0]["finished_ns"], observation["scheduled_ns"] + 1_000_000_000)

    def test_full_and_metrics_process_the_same_fixed_single_worker_corpus(self):
        full = self.successful("full-corpus", "--arrival-rate", "32", "--evidence", "full")
        metrics = self.successful("metrics-corpus", "--arrival-rate", "32")
        self.assertTrue(full["history_checked"])
        self.assertTrue(full["correctness_history_verified"])
        self.assertEqual(full["oracle_status"], "Valid")
        self.assertFalse(metrics["correctness_history_verified"])
        for run in (full, metrics):
            self.assertEqual(run["completed_messages"], 32)
            self.assertEqual(run["store_metrics"]["commits"], 32)
            self.assertEqual(run["message_kinds"], gate.lifecycle_kind_counts(32))
            self.assertGreaterEqual(run["elapsed_seconds"], 1.0)
        def observations(run):
            history = Path(run["evidence_directory"]) / "history.jsonl"
            return [json.loads(line)["receipt"] for line in history.read_text().splitlines()]
        full_receipts, metrics_receipts = observations(full), observations(metrics)
        self.assertEqual([r["message"] for r in full_receipts], [r["message"] for r in metrics_receipts])
        self.assertEqual([r["body"]["outcome"] for r in full_receipts], [r["body"]["outcome"] for r in metrics_receipts])
        self.assertTrue(all(r["body"]["operations"] for r in full_receipts))
        self.assertTrue(all(not r["body"]["operations"] for r in metrics_receipts))
        self.assertEqual(json.loads((Path(full["evidence_directory"]) / "final.json").read_text()),
                         json.loads((Path(metrics["evidence_directory"]) / "final.json").read_text()))

    def test_continuous_drain_includes_deliberately_slow_worker_shutdown(self):
        # Inject delay into the disposable coordinator's first waitpid only.
        # This exercises real worker teardown without a benchmark-only delay knob.
        source = self.output / "slow_waitpid.c"
        library = self.output / "slow_waitpid.so"
        source.write_text(r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
pid_t waitpid(pid_t pid, int *status, int options) {
    static atomic_int delayed = 0;
    pid_t (*real_waitpid)(pid_t, int *, int) = dlsym(RTLD_NEXT, "waitpid");
    char command[4096] = {0};
    FILE *file = fopen("/proc/self/cmdline", "rb");
    if (file) {
        size_t size = fread(command, 1, sizeof(command) - 1, file);
        fclose(file);
        for (size_t i = 0; i < size; ++i) if (!command[i]) command[i] = ' ';
        if (strstr(command, "--internal-coordinator") && !atomic_exchange(&delayed, 1)) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 400000000};
            while (nanosleep(&delay, &delay)) {}
        }
    }
    return real_waitpid(pid, status, options);
}
""")
        subprocess.run(["cc", "-shared", "-fPIC", "-o", str(library), str(source), "-ldl"], check=True)
        receipt, report = self.run_case("slow-shutdown", command_prefix=["env", f"LD_PRELOAD={library}"])
        self.assertEqual(receipt["exit_code"], 0, report)
        run = report["runs"][0]
        self.assertTrue(run["passed"])
        # This numeric bound fails for the former elapsed+drain formula even
        # without requiring the new timestamp fields to exist.
        self.assertLessEqual(run["completed_messages_per_second_including_drain"],
                             run["completed_messages"] / (run["elapsed_seconds"] + 0.35))
        self.assertEqual(run["timing_scope"], "continuous_client_monotonic")
        start, done, stopped, drained = (run[key] for key in
            ("admission_started_ns", "workload_completed_ns", "workers_stopped_ns", "drain_confirmed_ns"))
        self.assertLess(start, done)
        self.assertGreaterEqual(stopped - done, 350_000_000)
        self.assertGreaterEqual(drained, stopped)
        self.assertAlmostEqual(run["elapsed_seconds"], (done - start) / 1e9, places=8)
        self.assertAlmostEqual(run["elapsed_seconds_including_drain"], (drained - start) / 1e9, places=8)
        self.assertAlmostEqual(run["completed_messages_per_second_including_drain"],
                               run["completed_messages"] / run["elapsed_seconds_including_drain"], places=8)

    def test_message_cap_cannot_silently_truncate_offered_corpus(self):
        receipt, report = self.run_case("truncated", "--arrival-rate", "32", "--max-messages", "1")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        self.assertEqual(len(report["runs"]), 1)
        self.assertFalse(report["runs"][0]["execution_completed"])
        self.assertIn("offered corpus", report["runs"][0]["error"])

    def test_calibrated_short_run_keeps_idle_maintenance_workers_and_no_early_jobs(self):
        run = self.successful("calibrated-no-maintenance", "--workload", "calibrated",
                              "--families", "16", "--hot-percent", "0", "--workers", "4",
                              "--arrival-rate", "32", "--evidence", "full")
        self.assertTrue(run["correctness_history_verified"])
        self.assertEqual(run["completed_messages"], 32)
        self.assertEqual(len(run["completed_by_worker"]), 6)
        self.assertEqual(run["completed_by_worker"][-2:], [0, 0])
        self.assertNotIn("global_projection", run["message_kinds"])
        self.assertNotIn("global_housekeeping", run["message_kinds"])
        self.assertGreaterEqual(run["elapsed_seconds"], 1.0)

    def test_calibrated_timers_and_flight_order_survive_real_concurrent_execution(self):
        options = ("--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                   "--workers", "4", "--arrival-rate", "64", "--seconds", "3",
                   "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1")
        corpus = []
        for evidence in ("full", "metrics"):
            run = self.successful("calibrated-timers-" + evidence, *options, "--evidence", evidence)
            self.assertEqual(run["correctness_history_verified"], evidence == "full")
            self.assertEqual(run["completed_messages"], 196)
            self.assertEqual(run["message_kinds"]["global_projection"], 2)
            self.assertEqual(run["message_kinds"]["global_housekeeping"], 2)
            self.assertEqual(run["completed_by_worker"][-2:], [2, 2])
            self.assertEqual(gate.continuous_timing_errors(run, 196), [])
            history = Path(run["evidence_directory"]) / "history.jsonl"
            observations = [json.loads(line) for line in history.read_text().splitlines()]
            corpus.append(sorted((item["receipt"]["message"] for item in observations), key=lambda m: m["id"]))
            flights = {}
            timers = {"GlobalProject": [], "GlobalHousekeeping": []}
            for item in observations:
                message = item["receipt"]["message"]
                kind = message["kind"]
                if isinstance(kind, dict) and next(iter(kind)) in timers:
                    timers[next(iter(kind))].append(item["scheduled_ns"] - run["admission_started_ns"])
                else:
                    identity = (message["callsign"], message["tail"])
                    flights.setdefault(identity, []).append(item)
                    outcome = item["receipt"]["body"]["outcome"]
                    self.assertEqual(outcome["ignored_stale"], 0)
                    self.assertGreater(outcome["updated_views"], 0)
                    self.assertFalse(outcome["missing_family"])
                    self.assertFalse(outcome["allocation_deferred"])
            for deadlines in timers.values():
                self.assertEqual(sorted(deadlines), [1_000_000_000, 2_000_000_000])
            self.assertGreater(len(flights), 1)
            for messages in flights.values():
                ordered = sorted(messages, key=lambda item: item["receipt"]["message"]["id"])
                self.assertEqual(len({item["worker"] for item in ordered}), 1)
                for previous, following in zip(ordered, ordered[1:]):
                    self.assertLessEqual(previous["receipt"]["finished"], following["message_started_ns"])
                    self.assertLess(previous["receipt"]["message"]["event_time"],
                                    following["receipt"]["message"]["event_time"])
            self.assertTrue(all(item["scheduled_ns"] <= item["message_started_ns"] for item in observations))
        self.assertEqual(corpus[0], corpus[1])

    def test_calibrated_idle_foreground_workers_do_not_require_invented_arrivals(self):
        run = self.successful("calibrated-idle-foreground", "--workload", "calibrated",
                              "--families", "4", "--hot-percent", "0", "--workers", "8",
                              "--arrival-rate", "1", "--evidence", "full")
        self.assertTrue(run["correctness_history_verified"])
        self.assertEqual(run["completed_by_worker"], [1] + [0] * 9)
        self.assertEqual(run["total_process_workers"], 10)
        for worker in run["worker_activity"][1:]:
            self.assertEqual(worker["completed"], 0)
            self.assertEqual(worker["busy_ns"], 0)
            self.assertEqual(worker["utilization"], 0)
        for kind in ("projection", "housekeeping", "maintenance"):
            self.assertIsNone(run["workload_classes"][kind]["p99_us_including_retries"])

    def test_calibrated_message_cap_applies_to_independently_scheduled_maintenance(self):
        # Four foreground messages fit their identity-affine worker caps, but
        # each timer admits three jobs and cannot silently discard its third.
        receipt, report = self.run_case("calibrated-maintenance-cap", "--workload", "calibrated",
                                        "--families", "4", "--hot-percent", "0", "--workers", "4",
                                        "--seconds", "4", "--arrival-rate", "1", "--max-messages", "2",
                                        "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        self.assertFalse(report["runs"][0]["execution_completed"])
        self.assertIn("calibrated offered corpus", report["runs"][0]["error"])

    def test_interactive_delay_exposes_backlog_instead_of_throttling_arrivals(self):
        receipt, report = self.run_case("backlog", "--engine", "service-unix", "--arrival-rate", "100",
                                        "--max-backlog", "1", "--rpc-delay-us", "10000")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        self.assertEqual(len(report["runs"]), 1)
        self.assertFalse(report["runs"][0]["execution_completed"])
        self.assertIn("backlog", report["runs"][0]["error"])
        self.assertIn("arrivals were not throttled", report["runs"][0]["error"])

    def test_fleet_keeps_population_and_executes_cross_family_background_work(self):
        run = self.successful("fleet", "--workload", "fleet", "--families", "16",
                              "--hot-percent", "0", "--workers", "4", "--arrival-rate", "128",
                              "--seconds", "3", "--evidence", "full")
        self.assertTrue(run["correctness_history_verified"])
        self.assertGreaterEqual(run["initial_fleet"]["live_families"], 8)
        self.assertGreaterEqual(run["final_fleet"]["live_families"], 8)
        for kind, field in [("global_projection", "claimed_events"), ("global_reschedule", "rescheduled_events"),
                            ("global_cancel", "cancelled_events"), ("global_housekeeping", "expired_records")]:
            self.assertGreater(run["per_kind"][kind]["outcomes"][field], 0)
        global_rows = []
        with (Path(run["evidence_directory"]) / "history.jsonl").open() as history:
            for line in history:
                for operation in json.loads(line)["receipt"]["body"]["operations"]:
                    query = operation.get("Query", {})
                    if "GlobalDue" in query.get("query", {}):
                        global_rows.append(query["rows"])
        self.assertGreater(max(map(len, global_rows)), 16, "global query must precede the 16-event effect limit")
        self.assertGreaterEqual(max(len({row["family"] for row in rows}) for rows in global_rows), 8)

    def test_fleet_rejects_silently_ignored_hotspot_configuration(self):
        receipt, report = self.run_case("invalid-fleet", "--workload", "fleet", "--families", "16",
                                        "--hot-percent", "80")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        self.assertEqual(report["stage"], "configuration")


if __name__ == "__main__":
    unittest.main()
