#!/usr/bin/env python3
"""Candidate SQL treatments must be explicit and use exact full companions."""
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


def candidate_trial(engine="postgres", evidence="full", query="split"):
    item = trial(engine=engine, evidence=evidence)
    item["config"]["pg_candidate_query"] = query
    item["report"]["config"]["pg_candidate_query"] = query
    item["report"]["runs"][0]["postgres_candidate_query"] = {
        "format": "postgres-candidate-query-v1", "requested": query,
        "effective": query if engine == "postgres" else "not_applicable"}
    return item


class CandidateQueryGateTests(unittest.TestCase):
    def test_both_shapes_require_exact_companion_on_all_engines(self):
        for engine in gate.ENGINES:
            for query in ("or", "split"):
                with self.subTest(engine=engine, query=query):
                    full = candidate_trial(engine=engine, query=query)
                    metrics = candidate_trial(engine=engine, query=query, evidence="metrics")
                    self.assertTrue(gate.assess_trial(full, POLICY)["history_verified"])
                    matched = gate.assess_trial(metrics, POLICY, {gate.key(full["config"])})
                    self.assertTrue(matched["correctness_companion_verified"])
                    self.assertFalse(matched["history_verified"])
                    other = candidate_trial(engine=engine, query="or" if query == "split" else "split")
                    self.assertFalse(gate.assess_trial(metrics, POLICY,
                        {gate.key(other["config"])})["correctness_companion_verified"])
                    self.assertFalse(gate.assess_trial(metrics, POLICY)["correctness_companion_verified"])

    def test_historical_omission_means_original_or_shape(self):
        old = trial(engine="postgres", evidence="metrics")
        explicit = candidate_trial(evidence="metrics", query="or")
        self.assertEqual(gate.key(old["config"]), gate.key(explicit["config"]))
        self.assertTrue(gate.assess_trial(old, POLICY, {gate.key(explicit["config"])})["correctness_companion_verified"])
        split = candidate_trial(evidence="metrics", query="split")
        self.assertNotEqual(gate.key(old["config"]), gate.key(split["config"]))
        self.assertFalse(gate.assess_trial(split, POLICY, {gate.key(old["config"])})["correctness_companion_verified"])

    def test_explicit_request_cannot_lose_or_null_its_metadata(self):
        for engine in gate.ENGINES:
            for query in ("or", "split"):
                for replacement in (None, [], "split", 1):
                    with self.subTest(engine=engine, query=query, replacement=replacement):
                        item = candidate_trial(engine=engine, query=query)
                        item["report"]["runs"][0]["postgres_candidate_query"] = replacement
                        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
                item = candidate_trial(engine=engine, query=query)
                del item["report"]["runs"][0]["postgres_candidate_query"]
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        # The executable reporting the modern default also requires metadata,
        # even if a caller removed that option from the driver configuration.
        item = trial(engine="postgres")
        item["report"]["config"]["pg_candidate_query"] = "or"
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_requested_effective_and_format_metadata_fail_closed(self):
        for engine in gate.ENGINES:
            for query in ("or", "split"):
                base = candidate_trial(engine=engine, query=query)
                expected = base["report"]["runs"][0]["postgres_candidate_query"]
                for field in expected:
                    for value in (None, True, 0, {}, [], "unknown", "or", "split", "not_applicable"):
                        if value == expected[field]:
                            continue
                        with self.subTest(engine=engine, query=query, field=field, value=value):
                            item = copy.deepcopy(base)
                            item["report"]["runs"][0]["postgres_candidate_query"][field] = value
                            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
                    item = copy.deepcopy(base)
                    del item["report"]["runs"][0]["postgres_candidate_query"][field]
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_invalid_configuration_and_report_substitution_reject(self):
        for query in (None, True, 0, [], {}, "", "union", "OR"):
            item = candidate_trial()
            item["config"]["pg_candidate_query"] = query
            self.assertTrue(gate.postgres_candidate_query_report_errors(
                item["report"]["runs"][0], item["config"]))
        for where in ("driver", "report"):
            item = candidate_trial(query="split")
            config = item["config"] if where == "driver" else item["report"]["config"]
            config["pg_candidate_query"] = "or"
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        item = candidate_trial(query="split")
        del item["report"]["config"]["pg_candidate_query"]
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_candidate_query_does_not_open_calibrated_capacity_gate(self):
        for query in ("or", "split"):
            item = calibrated_trial()
            item["config"]["pg_candidate_query"] = query
            item["report"]["config"]["pg_candidate_query"] = query
            item["report"]["runs"][0]["postgres_candidate_query"] = {
                "format": "postgres-candidate-query-v1", "requested": query,
                "effective": "not_applicable"}
            verdict = gate.assess_trial(item, POLICY)
            self.assertTrue(verdict["history_verified"])
            self.assertFalse(verdict["qualified_capacity_trial"])
            self.assertFalse(verdict["capacity_failure"])

    def test_cli_rejects_unknown_shape_before_starting_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                gate.main(["--binary", "/missing/benchmark", "--output", directory,
                           "--engines", "aerostore", "--slo-ms", "100",
                           "--pg-candidate-query", "union"])
            self.assertEqual(error.exception.code, 2)

    def test_cli_records_and_forwards_default_and_explicit_shapes(self):
        for flags, expected in (([], "or"), (["--pg-candidate-query", "or"], "or"),
                                (["--pg-candidate-query", "split"], "split")):
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
                cell = json.loads((output / "campaign.json").read_text())["trials"][0]
                self.assertEqual(cell["config"]["pg_candidate_query"], expected)
                for command in (cell["command"], executed):
                    self.assertEqual(command[command.index("--pg-candidate-query") + 1], expected)


if __name__ == "__main__":
    unittest.main()
