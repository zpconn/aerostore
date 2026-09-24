#!/usr/bin/env python3
"""Fail-closed TLC evidence classification tests; no Java/network required."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_tla


COMPLETE = (
    "Model checking completed. No error has been found.\n"
    "321 states generated, 123 distinct states found, 0 states left on queue.\n"
)
INVARIANT = (
    "Error: Invariant Safety is violated.\n"
    "Error: The behavior up to this point is:\n"
    "State 1: <Initial predicate>\n/x = 0\n"
    "State 2: <Mutant>\n/x = 1\n"
)
TEMPORAL = (
    "Error: Temporal property EventuallySafe was violated.\n"
    "Error: The following behavior constitutes a counter-example:\n"
    "State 1: <Initial predicate>\n/x = 0\n"
    "State 2: <Cycle>\n/x = 1\n"
    "Back to state 1: <Cycle>\n"
)
COMPLETE_CASE = {"expected": "complete"}
SAFETY_CASE = {"expected": "counterexample", "failure_property": "Safety"}
LIVENESS_CASE = {"expected": "liveness_counterexample", "temporal_property": "EventuallySafe"}


class EvidenceClassificationTests(unittest.TestCase):
    def test_complete_requires_success_marker_and_empty_queue(self):
        self.assertEqual(check_tla.classify(COMPLETE, 0, COMPLETE_CASE), (True, "completed_finite_search"))
        for text, code in [
            (COMPLETE.replace("0 states left", "10 states left"), 0),
            (COMPLETE.replace("0 states left", "1 states left"), 0),
            (COMPLETE.splitlines()[0], 0),
            (COMPLETE.splitlines()[1], 0),
            (COMPLETE + "Error: Deadlock reached.\n", 0),
            (COMPLETE, 1), (COMPLETE, None), (COMPLETE, -9),
        ]:
            with self.subTest(text=text, code=code):
                self.assertFalse(check_tla.classify(text, code, COMPLETE_CASE)[0])

    def test_named_safety_failure_and_witness_have_distinct_result_classes(self):
        self.assertEqual(check_tla.classify(INVARIANT, 12, SAFETY_CASE), (True, "intended_counterexample"))
        witness = SAFETY_CASE | {"expected": "witness"}
        self.assertEqual(check_tla.classify(INVARIANT, 12, witness), (True, "reachable_witness"))

    def test_wrong_property_and_unrelated_errors_never_count_as_expected_failure(self):
        for text in [
            INVARIANT.replace("Invariant Safety", "Invariant TypeOK"),
            INVARIANT.replace("State 1:", "State 2:"),
            INVARIANT + "Error: Parsing or semantic analysis failed.\n",
            INVARIANT + "Error: Deadlock reached.\n",
            "Parse Error ***\nEncountered Unexpected EOF\n",
            "Error: Parsing or semantic analysis failed.\n",
            INVARIANT.replace("Error: Invariant", "quoted Error: Invariant"),
        ]:
            with self.subTest(text=text):
                self.assertFalse(check_tla.classify(text, 12, SAFETY_CASE)[0])

    def test_timeouts_signals_and_wrong_exit_codes_reject_even_a_printed_trace(self):
        for case, text in [(SAFETY_CASE, INVARIANT), (LIVENESS_CASE, TEMPORAL)]:
            for code in [None, -9, -15, 0, 1, 2]:
                with self.subTest(case=case, code=code):
                    self.assertFalse(check_tla.classify(text, code, case)[0])
        self.assertFalse(check_tla.classify(INVARIANT, 13, SAFETY_CASE)[0])
        self.assertFalse(check_tla.classify(TEMPORAL, 12, LIVENESS_CASE)[0])

    def test_liveness_requires_named_property_trace_and_actual_lasso(self):
        self.assertEqual(check_tla.classify(TEMPORAL, 13, LIVENESS_CASE), (True, "intended_liveness_counterexample"))
        stutter = TEMPORAL.replace("Back to state 1: <Cycle>", "State 3: Stuttering")
        self.assertTrue(check_tla.classify(stutter, 13, LIVENESS_CASE)[0])
        for text in [
            TEMPORAL.replace("EventuallySafe", "UnrelatedProperty"),
            TEMPORAL.replace("State 1:", "State 2:"),
            TEMPORAL.replace("Back to state 1: <Cycle>\n", ""),
            TEMPORAL.replace("Back to state 1: <Cycle>", "Stuttering is merely mentioned in a comment"),
            TEMPORAL + "Error: Deadlock reached.\n",
        ]:
            with self.subTest(text=text):
                self.assertFalse(check_tla.classify(text, 13, LIVENESS_CASE)[0])

    def test_safety_and_temporal_counterexamples_cannot_substitute_for_each_other(self):
        self.assertFalse(check_tla.classify(INVARIANT, 12, LIVENESS_CASE)[0])
        self.assertFalse(check_tla.classify(TEMPORAL, 13, SAFETY_CASE)[0])
        self.assertFalse(check_tla.classify(INVARIANT + TEMPORAL, 13, LIVENESS_CASE)[0])
        self.assertFalse(check_tla.classify(TEMPORAL + INVARIANT, 12, SAFETY_CASE)[0])

    def test_unknown_expected_class_is_configuration_error(self):
        with self.assertRaisesRegex(ValueError, "unknown expected"):
            check_tla.classify(INVARIANT, 12, {"expected": "typo", "failure_property": "Safety"})

    def test_committed_campaign_logs_classify_as_declared(self):
        evidence = check_tla.MODELS / "evidence"
        report = json.loads((evidence / "report.json").read_text())
        cases = json.loads((check_tla.MODELS / "campaign.json").read_text())["cases"]
        self.assertEqual({c["name"] for c in cases}, {r["name"] for r in report["results"]})
        results = {result["name"]: result for result in report["results"]}
        for case in cases:
            with self.subTest(case=case["name"]):
                result = results[case["name"]]
                actual = check_tla.classify((evidence / result["log"]).read_text(), result["exit_code"], case)
                self.assertEqual(actual, (True, result["outcome"]))


class RunnerFailureTests(unittest.TestCase):
    def test_module_override_environment_rejected_and_stale_pass_cleared(self):
        for name in ("TLC_LIBRARY", "TLA_LIBRARY", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "CLASSPATH"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                output.mkdir()
                report_path = output / "report.json"
                report_path.write_text('{"passed": true, "completed": true}')
                args = argparse.Namespace(list=False, case=["publication_empty_b1"], output=output)
                with patch.dict(os.environ, {name: "unreviewed override"}, clear=True), patch.object(check_tla.subprocess, "run") as run:
                    with self.assertRaisesRegex(ValueError, name):
                        check_tla.run(args)
                    run.assert_not_called()
                report = json.loads(report_path.read_text())
                self.assertFalse(report["passed"])
                self.assertFalse(report["completed"])
                self.assertEqual(report["results"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
