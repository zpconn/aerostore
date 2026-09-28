#!/usr/bin/env python3
"""Ordered maintenance selection changes the query contract and companions."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import qualify_hyperfeed as gate
from test_hyperfeed_qualification import POLICY, calibrated_trial, trial


def apply_selection(item, selection):
    item["config"]["maintenance_selection"] = selection
    item["report"]["config"]["maintenance_selection"] = selection
    run = item["report"]["runs"][0]
    pg = item["config"]["engine"] == "postgres"
    effective = "complete_query" if selection == "complete" else "ordered_sql_prefix" if pg else "complete_read_prefix"
    run["maintenance_selection_metadata"] = {"format": "maintenance-selection-v1",
                                            "requested": selection, "effective": effective}
    if item["config"]["workload"] == "calibrated":
        run["calibrated_schedule"]["maintenance_selection"] = selection
    if pg:
        indexes = []
        for name, columns, predicate in (("due_prefix_idx" if selection == "prefix" else "due_idx",
                ["due", "id"] if selection == "prefix" else ["due"], "active AND kind=3"),
                ("expiry_prefix_idx" if selection == "prefix" else "event_time_idx",
                ["event_time", "id"] if selection == "prefix" else ["event_time"], "active AND kind IN (2,4,5)")):
            indexes.append({"name": name, "key_columns": columns, "predicate": predicate,
                            "valid": True, "ready": True, "access_method": "btree",
                            "definition": f"CREATE INDEX {name} ON fixture.records USING btree ({', '.join(columns)}) WHERE {predicate}"})
        indexes.extend({"name": name} for name in ("records_pkey", "callsign_idx", "tail_idx", "family_idx",
                       "positions_idx", "family_due_idx", "family_expired_idx"))
        run["query_plan_audit"] = {"maintenance_selection": selection, "indexes": indexes,
            "prefix_limit_samples": {"first_due": 4, "first_expired": 32} if selection == "prefix" else None,
            "plans": {name: {"sql": "SELECT fixture", "plan": [{"Plan": {"Node Type": "Limit"}}]}
                      for name in ("first_due", "first_expired")} if selection == "prefix" else {}}
    return item


def selected_trial(selection="prefix", engine="postgres", evidence="full"):
    return apply_selection(trial(engine=engine, evidence=evidence), selection)


class MaintenanceSelectionTests(unittest.TestCase):
    def test_requested_and_effective_selection_requires_exact_companions(self):
        for engine in gate.ENGINES:
            for selection in ("complete", "prefix"):
                with self.subTest(engine=engine, selection=selection):
                    full = selected_trial(selection, engine)
                    metrics = selected_trial(selection, engine, "metrics")
                    self.assertTrue(gate.assess_trial(full, POLICY)["history_verified"])
                    match = gate.assess_trial(metrics, POLICY, {gate.key(full["config"])})
                    self.assertTrue(match["correctness_companion_verified"])
                    self.assertFalse(match["history_verified"])
                    other = selected_trial("complete" if selection == "prefix" else "prefix", engine)
                    self.assertFalse(gate.assess_trial(metrics, POLICY, {gate.key(other["config"])})["correctness_companion_verified"])

    def test_legacy_omission_means_complete_and_modern_defaults_need_metadata(self):
        old = trial(engine="postgres")
        self.assertTrue(gate.assess_trial(old, POLICY)["history_verified"])
        self.assertEqual(gate.key(old["config"]), gate.key({**old["config"], "maintenance_selection": "complete"}))
        self.assertNotEqual(gate.key(old["config"]), gate.key({**old["config"], "maintenance_selection": "prefix"}))
        for location in ("driver", "report"):
            item = copy.deepcopy(old)
            (item["config"] if location == "driver" else item["report"]["config"])["maintenance_selection"] = "complete"
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_metadata_types_and_engine_effective_behavior_fail_closed(self):
        for engine in gate.ENGINES:
            for selection in ("complete", "prefix"):
                base = selected_trial(selection, engine)
                metadata = base["report"]["runs"][0]["maintenance_selection_metadata"]
                for field in metadata:
                    for value in (None, True, 1, [], {}, "unknown", "complete_query", "ordered_sql_prefix", "complete_read_prefix"):
                        if value == metadata[field]:
                            continue
                        item = copy.deepcopy(base)
                        item["report"]["runs"][0]["maintenance_selection_metadata"][field] = value
                        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"], (engine, selection, field, value))
                    item = copy.deepcopy(base)
                    del item["report"]["runs"][0]["maintenance_selection_metadata"][field]
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
                item = copy.deepcopy(base)
                del item["report"]["runs"][0]["maintenance_selection_metadata"]
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_prefix_requires_valid_ready_ordered_partial_index_catalog(self):
        for index in (0, 1):
            for field, value in (("valid", False), ("ready", 1), ("access_method", "hash"),
                    ("key_columns", ["id", "due"]), ("predicate", None), ("predicate", ""),
                    ("definition", None), ("definition", ""), ("name", "other")):
                item = selected_trial()
                item["report"]["runs"][0]["query_plan_audit"]["indexes"][index][field] = value
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"], (index, field, value))
        for replacement in ([], None, [None]):
            item = selected_trial()
            item["report"]["runs"][0]["query_plan_audit"]["indexes"] = replacement
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        item = selected_trial()
        indexes = item["report"]["runs"][0]["query_plan_audit"]["indexes"]
        indexes.append(copy.deepcopy(indexes[0]))
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_complete_control_cannot_inherit_prefix_indexes_or_plan_treatment(self):
        for field, value in (("maintenance_selection", "prefix"),
                             ("prefix_limit_samples", {"first_due": 4, "first_expired": 32}),
                             ("indexes", selected_trial()["report"]["runs"][0]["query_plan_audit"]["indexes"])):
            item = selected_trial("complete")
            item["report"]["runs"][0]["query_plan_audit"][field] = value
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        for field, value in (("prefix_limit_samples", {"first_due": True, "first_expired": 32}),
                             ("prefix_limit_samples", {"first_due": 8, "first_expired": 32}),
                             ("plans", {}), ("plans", {"first_due": {"sql": "SELECT", "plan": []}})):
            item = selected_trial()
            item["report"]["runs"][0]["query_plan_audit"][field] = value
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_calibrated_schedule_binds_contract_without_opening_capacity_gate(self):
        for selection in ("complete", "prefix"):
            item = apply_selection(calibrated_trial(), selection)
            verdict = gate.assess_trial(item, POLICY)
            self.assertTrue(verdict["history_verified"])
            self.assertFalse(verdict["qualified_capacity_trial"])
            self.assertFalse(verdict["capacity_failure"])
            for bad in (None, "complete" if selection == "prefix" else "prefix", True):
                copy_item = copy.deepcopy(item)
                copy_item["report"]["runs"][0]["calibrated_schedule"]["maintenance_selection"] = bad
                self.assertFalse(gate.assess_trial(copy_item, POLICY)["execution_valid"])

    def test_invalid_selection_and_report_config_substitution_reject(self):
        for selection in (None, True, 0, [], {}, "unknown"):
            item = selected_trial()
            item["config"]["maintenance_selection"] = selection
            self.assertTrue(gate.maintenance_selection_report_errors(item["report"]["runs"][0], item["config"]))
        item = selected_trial()
        item["report"]["config"]["maintenance_selection"] = "complete"
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                gate.main(["--binary", "/missing/benchmark", "--output", directory,
                           "--engines", "aerostore", "--slo-ms", "100", "--maintenance-selection", "unknown"])
            self.assertEqual(error.exception.code, 2)

    def test_cli_forwards_and_records_default_and_prefix_contracts(self):
        for flags, expected in (([], "complete"), (["--maintenance-selection", "complete"], "complete"),
                                (["--maintenance-selection", "prefix"], "prefix")):
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
                            "--slo-ms", "100", *flags]), 1)
                    start.assert_called_once()
                    executed = start.call_args.args[0]
                item = json.loads((output / "campaign.json").read_text())["trials"][0]
                self.assertEqual(item["config"]["maintenance_selection"], expected)
                for command in (item["command"], executed):
                    self.assertEqual(command[command.index("--maintenance-selection") + 1], expected)


if __name__ == "__main__":
    unittest.main()
