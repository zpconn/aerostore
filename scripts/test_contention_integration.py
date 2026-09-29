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

    def test_statistics_schedule_is_explicitly_inapplicable_on_native(self):
        run = self.successful("native-statistics-policy", "--seconds", "2",
                              "--pg-analyze-after-seconds", "1")
        self.assertEqual(run["postgres_statistics"], {
            "format": "postgres-statistics-v1", "requested_after_seconds": 1,
            "effective_policy": "not_applicable", "initial_analyze_executed": False,
            "runtime_analyze": None})

    def test_candidate_query_is_explicitly_inapplicable_on_native_and_service(self):
        for engine in ("aerostore", "service-unix"):
            for query in ("or", "split"):
                run = self.successful(f"candidate-query-{engine}-{query}",
                    "--engine", engine, "--pg-candidate-query", query)
                self.assertEqual(run["postgres_candidate_query"], {
                    "format": "postgres-candidate-query-v1", "requested": query,
                    "effective": "not_applicable"})

    def test_candidate_query_rejects_unknown_shape(self):
        receipt, report = self.run_case("candidate-query-invalid", "--pg-candidate-query", "union")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertIs(report["passed"], False)
        self.assertEqual(report["stage"], "configuration")

    def test_maintenance_selection_rejects_unknown_contract(self):
        receipt, report = self.run_case("maintenance-selection-invalid", "--maintenance-selection", "limit")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertIs(report["passed"], False)
        self.assertEqual(report["stage"], "configuration")

    def assert_prefix_history(self, run):
        observed = set()
        global_kinds = {"GlobalProject", "GlobalCancel", "GlobalReschedule", "GlobalHousekeeping"}
        for line in (Path(run["evidence_directory"]) / "history.jsonl").read_text().splitlines():
            receipt = json.loads(line)["receipt"]
            message = receipt["message"]
            kind = message["kind"]
            name = next(iter(kind)) if isinstance(kind, dict) else kind
            self.assertEqual(message.get("maintenance_selection", "complete"),
                             "prefix" if name in global_kinds else "complete")
            for operation in receipt["body"]["operations"]:
                query = operation.get("Query", {})
                for variant, order in (("FirstDue", "due"), ("FirstExpired", "event_time")):
                    if variant in query.get("query", {}):
                        observed.add(variant)
                        rows = query["rows"]
                        self.assertLessEqual(len(rows), query["query"][variant]["limit"])
                        self.assertEqual(rows, sorted(rows, key=lambda row: (row[order], row["id"])))
        self.assertEqual(observed, {"FirstDue", "FirstExpired"})

    def test_native_fleet_prefix_covers_all_global_handlers(self):
        run = self.successful("native-fleet-prefix", "--workload", "fleet", "--families", "16",
            "--hot-percent", "0", "--workers", "4", "--arrival-rate", "128", "--seconds", "3",
            "--evidence", "full", "--maintenance-selection", "prefix")
        self.assertTrue(run["correctness_history_verified"])
        self.assertEqual(run["maintenance_selection_metadata"], {
            "format": "maintenance-selection-v1", "requested": "prefix", "effective": "complete_read_prefix"})
        for name in ("global_projection", "global_cancel", "global_reschedule", "global_housekeeping"):
            self.assertGreater(run["per_kind"][name]["completed"], 0)
        self.assert_prefix_history(run)

    def test_service_calibrated_prefix_binds_schedule_and_full_history(self):
        receipt, report = self.run_case("service-calibrated-prefix", "--engine", "service-unix",
            "--workload", "calibrated", "--families", "4", "--hot-percent", "0", "--seconds", "3",
            "--arrival-rate", "8", "--projection-interval-seconds", "1", "--housekeeping-interval-seconds", "1",
            "--maintenance-mode", "sweep", "--maintenance-selection", "prefix", "--evidence", "full")
        self.assertEqual(receipt["exit_code"], 0, report)
        run = report["runs"][0]
        self.assertTrue(run["correctness_history_verified"])
        self.assertEqual(gate.maintenance_selection_report_errors(run, report["config"]), [])
        self.assertEqual(run["calibrated_schedule"]["maintenance_selection"], "prefix")
        self.assertEqual(run["maintenance_selection_metadata"]["effective"], "complete_read_prefix")
        self.assert_prefix_history(run)

    @unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_PG_URL"), "requires disposable PostgreSQL")
    def test_postgres_prefix_uses_bound_ordered_index_treatment_in_both_evidence_modes(self):
        url = os.environ["AEROSTORE_CONTENTION_PG_URL"]
        for evidence in ("full", "metrics"):
            receipt, report = self.run_case(f"pg-maintenance-prefix-{evidence}",
                "--engine", "postgres", "--pg-url", url, "--workload", "calibrated", "--families", "4",
                "--hot-percent", "0", "--seconds", "3", "--arrival-rate", "8", "--projection-interval-seconds", "1",
                "--housekeeping-interval-seconds", "1", "--maintenance-mode", "sweep",
                "--maintenance-selection", "prefix", "--evidence", evidence)
            self.assertEqual(receipt["exit_code"], 0, report)
            run = report["runs"][0]
            self.assertTrue(run["passed"])
            self.assertEqual(gate.maintenance_selection_report_errors(run, report["config"]), [])
            self.assertEqual(run["maintenance_selection_metadata"]["effective"], "ordered_sql_prefix")
            self.assertEqual(run["correctness_history_verified"], evidence == "full")
            if evidence == "full":
                self.assert_prefix_history(run)

    @unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_PG_URL"), "requires disposable PostgreSQL")
    def test_postgres_candidate_query_is_bound_in_both_evidence_modes(self):
        url = os.environ["AEROSTORE_CONTENTION_PG_URL"]
        for evidence in ("full", "metrics"):
            for query in ("or", "split"):
                receipt, report = self.run_case(f"pg-candidate-query-{evidence}-{query}",
                    "--engine", "postgres", "--pg-url", url, "--arrival-rate", "16",
                    "--evidence", evidence, "--pg-candidate-query", query)
                self.assertEqual(receipt["exit_code"], 0, report)
                run = report["runs"][0]
                self.assertTrue(run["passed"])
                self.assertEqual(report["config"]["pg_candidate_query"], query)
                self.assertEqual(run["postgres_candidate_query"], {
                    "format": "postgres-candidate-query-v1", "requested": query,
                    "effective": query})
                self.assertEqual(gate.postgres_candidate_query_report_errors(run, report["config"]), [])
                self.assertEqual(run["correctness_history_verified"], evidence == "full")

    def test_statistics_schedule_rejects_unreachable_or_ambiguous_deadlines(self):
        for number, flags in enumerate((
                ["--pg-analyze-after-seconds", "1"],
                ["--pg-analyze-after-seconds", "-1"],
                ["--seconds", "2", "--pg-analyze-after-seconds", "1", "--arrival-rate", "0"],
                ["--seconds", "2", "--pg-analyze-after-seconds", "1", "--mode", "all"])):
            receipt, report = self.run_case(f"statistics-invalid-{number}", *flags)
            self.assertNotEqual(receipt["exit_code"], 0)
            self.assertIs(report["passed"], False)
            self.assertEqual(report["stage"], "configuration")

    @unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_PG_URL"), "requires disposable PostgreSQL")
    def test_postgres_statistics_action_is_measured_and_bound_in_both_evidence_modes(self):
        url = os.environ["AEROSTORE_CONTENTION_PG_URL"]
        for evidence, delay in (("full", 0), ("full", 1), ("metrics", 1)):
            receipt, report = self.run_case(f"pg-statistics-{evidence}-{delay}",
                "--engine", "postgres", "--pg-url", url, "--seconds", "3",
                "--arrival-rate", "16", "--evidence", evidence,
                "--pg-analyze-after-seconds", str(delay))
            self.assertEqual(receipt["exit_code"], 0, report)
            run = report["runs"][0]
            self.assertTrue(run["passed"])
            self.assertEqual(gate.postgres_statistics_report_errors(run, report["config"]), [])
            stats = run["postgres_statistics"]
            if delay:
                saved = json.loads((Path(run["evidence_directory"]) / "postgres-statistics.json").read_text())
                self.assertEqual(saved, stats)
                action = stats["runtime_analyze"]
                self.assertEqual(action["status"], "succeeded")
                self.assertGreaterEqual(action["dispatched_ns"], run["admission_started_ns"] + 1_000_000_000)
                self.assertLessEqual(action["finished_ns"], run["admission_started_ns"] + 3_000_000_000)
            else:
                self.assertIsNone(stats["runtime_analyze"])
            self.assertEqual(run["correctness_history_verified"], evidence == "full")

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

    def test_larger_calibrated_metrics_cap_is_opt_in_without_inflating_inputs(self):
        run = self.successful("calibrated-larger-metrics-cap", "--workload", "calibrated",
                              "--families", "4", "--hot-percent", "0", "--workers", "1",
                              "--arrival-rate", "1", "--max-messages", "1000000")
        self.assertEqual(run["completed_messages"], 1)
        self.assertEqual(run["offered_messages"], 1)
        self.assertEqual(run["completed_by_worker"], [1, 0, 0])
        self.assertFalse(run["history_checked"])
        self.assertFalse(run["worker_message_cap_reached"])

    def test_larger_metrics_cap_rejects_unsafe_modes_and_global_overflow(self):
        changes = [
            ("full", ("--evidence", "full", "--max-messages", "100001")),
            ("legacy", ("--workload", "lifecycle", "--hot-percent", "80", "--max-messages", "100001")),
            ("perworker", ("--max-messages", "1000001")),
            ("global", ("--seconds", "9", "--arrival-rate", "1000000", "--workers", "16", "--max-messages", "1000000")),
        ]
        for name, options in changes:
            with self.subTest(name=name):
                receipt, report = self.run_case("calibrated-larger-cap-invalid-" + name,
                    "--workload", "calibrated", "--families", "4", "--hot-percent", "0", *options)
                self.assertNotEqual(receipt["exit_code"], 0)
                self.assertFalse(report["passed"])
                self.assertEqual(report["stage"], "configuration")

    def test_calibrated_worker_skew_rejects_before_any_foreground_receipt(self):
        # Three active identities over two workers produce [4,2], although
        # ceil(total/workers)=3. The prelaunch check must use exact ownership.
        receipt, report = self.run_case("calibrated-owner-cap-skew", "--workload", "calibrated",
            "--families", "4", "--hot-percent", "0", "--workers", "2",
            "--arrival-rate", "6", "--max-messages", "3")
        self.assertNotEqual(receipt["exit_code"], 0)
        self.assertFalse(report["passed"])
        run = report["runs"][0]
        self.assertFalse(run["execution_completed"])
        self.assertIn("no jobs may be truncated", run["error"])
        history = Path(run["evidence_directory"]) / "history.jsonl"
        self.assertTrue(not history.exists() or history.stat().st_size == 0)


if __name__ == "__main__":
    unittest.main()
