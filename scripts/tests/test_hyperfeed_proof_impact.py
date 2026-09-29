#!/usr/bin/env python3
"""Advisory impact regressions; no compiler, verifier or benchmark invocation."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hyperfeed_proof_impact as impact

SOURCE = "aerostore_core/src/occ_partitioned.rs"
RUNNER = "verification/predicate/run.py"
README = "verification/predicate/README.md"


def digest(data):
    return hashlib.sha256(data).hexdigest()


class ImpactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def capture(self, name, *, changed=None, documentation=False, stale_boundary=False):
        root = self.root / name
        root.mkdir()
        original = {SOURCE: "core", impact.SERVICE: "service", RUNNER: "runner", "Cargo.toml": "build"}
        frozen = {path: digest(value.encode()) for path, value in original.items()}
        frozen["verification/README.md"] = "e" * 64
        data = {**original, **(changed or {})}
        if stale_boundary:
            frozen[impact.SERVICE] = "a" * 64
        inputs = [SOURCE, RUNNER] + ([README] if documentation else [])
        data[impact.CAMPAIGNS] = json.dumps({"campaigns": {"predicate": {"scope": "conditional", "inputs": inputs}}})
        data[impact.BOUNDARY] = json.dumps({"files": frozen})
        files = {}
        for path, text in data.items():
            destination = root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(text)
            files[path] = digest(text.encode())
        return {"format": "aerostore-hyperfeed-screen-capture-v1", "complete": True,
                "source": {"root": str(root), "files": files,
                           "sha256": digest(json.dumps(files, sort_keys=True, separators=(",", ":")).encode())},
                "comparison_build_identity": {"compiler": "pinned"}}

    def test_service_change_leaves_declared_core_inputs_unchanged_but_has_no_proof(self):
        result = impact.report(self.capture("base"), self.capture("candidate", changed={impact.SERVICE: "new service"}))
        self.assertEqual(result["declared_components"][0]["status"], "unchanged_declared_inputs")
        self.assertEqual(result["unmapped_implementation_changes"], [impact.SERVICE])
        self.assertTrue(result["service_protocol"]["native_framing_review_needed"])
        self.assertFalse(result["service_protocol"]["implementation_refinement_proved"])
        self.assertFalse(result["formal_gate_passed"])
        self.assertFalse(result["proof_receipts_validated"])
        self.assertTrue(result["advisory_only"])
        self.assertEqual(result["frozen_boundary"]["changed_paths_in_baseline_boundary"], [impact.SERVICE])

    def test_core_change_reports_component_and_existing_command(self):
        result = impact.report(self.capture("base"), self.capture("candidate", changed={SOURCE: "new core"}))
        component = result["declared_components"][0]
        self.assertEqual(component["status"], "changed_declared_inputs")
        self.assertEqual(component["changed_inputs"], [SOURCE])
        self.assertEqual(component["existing_component_command"],
                         ["python3", RUNNER, "--output", result["recommendation_output_namespace"] + "/predicate"])
        self.assertFalse(result["unmapped_implementation_changes"])

    def test_repeated_reports_never_recommend_reusing_the_same_evidence_directory(self):
        baseline = self.capture("base")
        candidate = self.capture("candidate", changed={SOURCE: "new core", impact.SERVICE: "new service"})
        first, second = impact.report(baseline, candidate), impact.report(baseline, candidate)
        self.assertNotEqual(first["recommendation_output_namespace"], second["recommendation_output_namespace"])
        for result in (first, second):
            namespace = result["recommendation_output_namespace"]
            self.assertRegex(namespace, r"^target/proof-impact-review/[0-9a-f]{32}$")
            self.assertTrue(result["output_directory_must_be_fresh"])
            self.assertIn("require every output directory to be absent", result["command_templates_note"])
            component_output = result["declared_components"][0]["existing_component_command"][-1]
            model_output = result["service_protocol"]["existing_model_command"][-1]
            self.assertEqual(component_output, namespace + "/predicate")
            self.assertEqual(model_output, namespace + "/service-protocol")
            self.assertNotEqual(component_output, model_output)

    def test_uncaptured_documentation_cannot_be_called_unchanged(self):
        result = impact.report(self.capture("base", documentation=True), self.capture("candidate", documentation=True))
        component = result["declared_components"][0]
        self.assertEqual(component["status"], "incomplete_input_coverage")
        self.assertEqual(component["uncaptured_inputs"], [README])
        self.assertIsNone(component["existing_component_command"])

    def test_campaign_declaration_change_is_not_hidden(self):
        result = impact.report(self.capture("base"), self.capture("candidate", documentation=True))
        self.assertTrue(result["declared_components"][0]["declaration_changed"])
        self.assertEqual(result["declared_components"][0]["status"], "changed_declared_inputs")

    def test_existing_boundary_drift_is_reported_separately_from_candidate(self):
        result = impact.report(self.capture("base", stale_boundary=True), self.capture("candidate", stale_boundary=True))
        frozen = result["frozen_boundary"]
        self.assertTrue(frozen["baseline_already_out_of_date_for_captured_inputs"])
        self.assertEqual(frozen["baseline_observed_drift"], [impact.SERVICE])
        self.assertEqual(frozen["candidate_observed_drift"], [impact.SERVICE])
        self.assertEqual(frozen["baseline_uncaptured_paths"], ["verification/README.md"])
        self.assertFalse(frozen["gate_executed"])

    def test_build_changes_are_not_mistaken_for_unchanged_engine_proof(self):
        baseline = self.capture("base")
        candidate = self.capture("candidate", changed={"Cargo.toml": "different build"})
        candidate["comparison_build_identity"]["compiler"] = "different compiler"
        result = impact.report(baseline, candidate)
        self.assertEqual(result["build_input_changes"], ["Cargo.toml"])
        self.assertTrue(result["build_identity_changed"])
        self.assertFalse(result["formal_gate_passed"])

    def test_map_tampering_and_incomplete_capture_fail_closed(self):
        baseline, candidate = self.capture("base"), self.capture("candidate")
        for change in (lambda value: value.update(complete=False),
                       lambda value: value["source"]["files"].update({SOURCE: "a" * 64})):
            broken = copy.deepcopy(candidate)
            change(broken)
            with self.assertRaises(ValueError):
                impact.report(baseline, broken)

    def test_changed_captured_metadata_is_not_replaced_by_live_checkout(self):
        baseline, candidate = self.capture("base"), self.capture("candidate")
        (Path(candidate["source"]["root"]) / impact.CAMPAIGNS).write_text("{}")
        with self.assertRaisesRegex(ValueError, "changed captured metadata"):
            impact.report(baseline, candidate)

    def test_unsafe_source_path_is_rejected(self):
        baseline, candidate = self.capture("base"), self.capture("candidate")
        candidate["source"]["files"]["../outside.rs"] = "a" * 64
        with self.assertRaisesRegex(ValueError, "Unsafe captured source path"):
            impact.report(baseline, candidate)

    def test_output_is_fresh_and_never_overwrites_evidence(self):
        baseline, candidate = self.capture("base"), self.capture("candidate")
        paths = []
        for name, value in (("base.json", baseline), ("candidate.json", candidate)):
            path = self.root / name
            path.write_text(json.dumps(value))
            paths.append(path)
        output = self.root / "impact.json"
        argv = ["impact", "--baselinecapture", str(paths[0]), "--candidatecapture", str(paths[1]), "--output", str(output)]
        with patch.object(sys, "argv", argv):
            self.assertEqual(impact.main(), 0)
            original = output.read_bytes()
            with self.assertRaises(FileExistsError):
                impact.main()
        self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
