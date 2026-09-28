#!/usr/bin/env python3
"""Statistics treatments must execute and match the correctness companion."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import qualify_hyperfeed as gate
from test_hyperfeed_qualification import POLICY, trial


def treated_trial(evidence="full", delay=1, engine="postgres"):
    item = trial(engine=engine, evidence=evidence)
    item["config"]["pg_analyze_after_seconds"] = delay
    item["report"]["config"]["pg_analyze_after_seconds"] = delay
    pg = engine == "postgres"
    action = None
    if pg and delay:
        action = {"status": "succeeded", "requested_after_seconds": delay,
                  "admission_started_ns": 1_000_000_000, "admission_finished_ns": 6_000_000_000,
                  "scheduled_ns": (1 + delay) * 1_000_000_000,
                  "dispatched_ns": (1 + delay) * 1_000_000_000 + 100,
                  "finished_ns": (1 + delay) * 1_000_000_000 + 100_000,
                  "maximum_dispatch_lateness_ns": 1_000_000_000,
                  "backend_pid": 123, "schema_oid": 321, "relation_oid": 322,
                  "command_succeeded": True, "error": None}
        for name, observed, count in (("before", action["dispatched_ns"] - 1, 1),
                                      ("after", action["finished_ns"] + 1, 2)):
            action[name] = {"analyze_count": count, "autoanalyze_count": 0,
                            "n_mod_since_analyze": 0, "last_analyze": "2026-09-28",
                            "last_autoanalyze": None, "observed_ns": observed, "counters_may_lag": True}
    item["report"]["runs"][0]["postgres_statistics"] = {
        "format": "postgres-statistics-v1", "requested_after_seconds": delay,
        "effective_policy": "not_applicable" if not pg else "initial_and_scheduled" if delay else "initial_only",
        "initial_analyze_executed": pg, "runtime_analyze": action}
    return item


class StatisticsGateTests(unittest.TestCase):
    def test_exact_treatment_is_required_for_metrics_companion(self):
        full, metrics = treated_trial(), treated_trial(evidence="metrics")
        self.assertTrue(gate.assess_trial(full, POLICY)["history_verified"])
        self.assertTrue(gate.assess_trial(metrics, POLICY, {gate.key(full["config"])})["correctness_companion_verified"])
        for delay in (0, 2):
            other = treated_trial(delay=delay)
            self.assertFalse(gate.assess_trial(metrics, POLICY, {gate.key(other["config"])})["correctness_companion_verified"])
        self.assertFalse(gate.assess_trial(metrics, POLICY)["correctness_companion_verified"])
        # Historical absent setting means initial-only, never a runtime refresh.
        old = trial(engine="postgres")
        self.assertEqual(gate.key(old["config"]), gate.key({**old["config"], "pg_analyze_after_seconds": 0}))
        self.assertTrue(gate.assess_trial(old, POLICY)["execution_valid"])

    def test_modern_config_cannot_erase_statistics_receipt(self):
        for engine in ("postgres", "service-unix", "aerostore"):
            for delay in (0, 1):
                item = treated_trial(engine=engine, delay=delay)
                run = item["report"]["runs"][0]
                self.assertEqual(gate.postgres_statistics_report_errors(run, item["config"]), [])
                del run["postgres_statistics"]
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_treatment_identity_and_success_fail_closed(self):
        base = treated_trial()
        mutations = {
            "status": ["failed", "running", "cancelled", None],
            "command_succeeded": [False, 1], "error": ["permission denied", ""],
            "requested_after_seconds": [0, 2, True],
            "backend_pid": [0, -1, True, "123", 2**31],
            "schema_oid": [0, True, "321", 2**32],
            "relation_oid": [0, 1.0, "322", 2**32],
            "maximum_dispatch_lateness_ns": [2_000_000_000, True, None],
            "before": [None, []], "after": [None, []],
        }
        for field, values in mutations.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    item = copy.deepcopy(base)
                    item["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"][field] = value
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        for field in base["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"]:
            item = copy.deepcopy(base)
            del item["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"][field]
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"], field)

    def test_treatment_must_execute_in_the_observed_admission_interval(self):
        for changes in ({"admission_started_ns": 2_000_000_000},
                        {"admission_finished_ns": 7_000_000_000},
                        {"scheduled_ns": 2_000_000_001},
                        {"dispatched_ns": 1_999_999_999},
                        {"dispatched_ns": 3_000_000_001, "finished_ns": 3_000_000_002},
                        {"finished_ns": 2_000_000_099},
                        {"finished_ns": 6_000_000_001},
                        {"dispatched_ns": True}, {"finished_ns": float("nan")}):
            item = treated_trial()
            item["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"].update(changes)
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"], changes)

    def test_policy_and_off_modes_are_exact(self):
        for engine in ("postgres", "service-unix"):
            for delay in (0, 1):
                for field, value in (("format", "unknown"), ("requested_after_seconds", True),
                                     ("effective_policy", "automatic"), ("initial_analyze_executed", 1)):
                    item = treated_trial(engine=engine, delay=delay)
                    item["report"]["runs"][0]["postgres_statistics"][field] = value
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"], (engine, delay, field))
        for engine in ("postgres", "aerostore"):
            item = treated_trial(engine=engine, delay=0)
            item["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"] = {}
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_observations_validate_types_without_demanding_synchronous_counters(self):
        item = treated_trial()
        action = item["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"]
        # PG counters may lag, and automatic analysis may also run concurrently.
        action["after"].update(analyze_count=1, autoanalyze_count=2)
        self.assertTrue(gate.assess_trial(item, POLICY)["execution_valid"])
        for name in ("before", "after"):
            for field, value in (("analyze_count", True), ("autoanalyze_count", -1),
                                 ("n_mod_since_analyze", "0"), ("last_analyze", 0),
                                 ("last_autoanalyze", False), ("observed_ns", True),
                                 ("observed_ns", 1 if name == "after" else 7_000_000_000),
                                 ("counters_may_lag", 1)):
                bad = copy.deepcopy(item)
                bad["report"]["runs"][0]["postgres_statistics"]["runtime_analyze"][name][field] = value
                self.assertFalse(gate.assess_trial(bad, POLICY)["execution_valid"], (name, field))

    def test_invalid_schedules_reject_before_execution(self):
        for delay in (-1, 5, 3600, 2**64):
            with tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    gate.main(["--binary", "/missing/benchmark", "--output", directory,
                               "--engines", "aerostore", "--slo-ms", "100", "--seconds", "5",
                               "--pg-analyze-after-seconds", str(delay)])
                self.assertEqual(error.exception.code, 2)
        for delay in (True, -1, 1.0, "1", None, 5, 2**64):
            item = treated_trial()
            item["config"]["pg_analyze_after_seconds"] = delay
            self.assertTrue(gate.postgres_statistics_report_errors(item["report"]["runs"][0], item["config"]))

    def test_cli_records_and_forwards_option(self):
        for delay in (0, 1):
            with tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "benchmark"
                binary.write_bytes(b"not executed")
                output = Path(directory) / "output"
                with patch.object(gate, "snapshot_sources", return_value={"sha256": "source", "files": {}}), \
                     patch.object(gate, "host_info", return_value={}), \
                     patch.object(gate.subprocess, "check_output", return_value="fixture"), \
                     patch.object(gate, "run_process", side_effect=RuntimeError("stop before execution")) as start:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(gate.main(["--binary", str(binary), "--output", str(output),
                            "--engines", "aerostore", "--rates", "32", "--workers", "1", "--seeds", "11",
                            "--slo-ms", "100", "--pg-analyze-after-seconds", str(delay)]), 1)
                    start.assert_called_once()
                cell = json.loads((output / "campaign.json").read_text())["trials"][0]
                self.assertEqual(cell["config"]["pg_analyze_after_seconds"], delay)
                command = cell["command"]
                self.assertEqual(command[command.index("--pg-analyze-after-seconds") + 1], str(delay))


if __name__ == "__main__":
    unittest.main()
