#!/usr/bin/env python3
"""Check enrollment of the new model directory using a disposable boundary.

This never refreshes the workspace's reviewed boundary and is not proof of the
protocol. It catches accidental omission of its model/campaign/checker sources.
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("coverage", ROOT / "scripts/check_formal_coverage.py")
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)


class ServiceBoundaryTests(unittest.TestCase):
    def test_model_mutation_is_rejected_after_local_fixture_review(self):
        with tempfile.TemporaryDirectory(prefix="service-boundary-test-") as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            # A minimal valid claims fixture isolates the source-coverage rule.
            (root / "verification").mkdir()
            (root / "verification/claims.toml").write_text(
                "whole_engine_verified = false\nfull_P1_complete = false\nclaims = []\n")
            model = root / "verification/service_protocol/ServiceProtocol.tla"
            model.parent.mkdir(parents=True)
            model.write_text("---- MODULE ServiceProtocol ----\n====\n")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            files = {name: coverage.digest(root / name) for name in coverage.frozen_paths(root)}
            self.assertIn(str(model.relative_to(root)), files)
            (root / coverage.LOCK).write_text(json.dumps({"format_version": 1, "files": files}))
            self.assertTrue(coverage.validate(root)["passed"])
            model.write_text(model.read_text() + "\\* changed model\n")
            result = coverage.validate(root)
            self.assertFalse(result["passed"])
            self.assertIn("frozen boundary changed: verification/service_protocol/ServiceProtocol.tla", result["errors"])

    def test_entire_service_protocol_source_set_is_enrolled(self):
        protected = set(coverage.frozen_paths(ROOT))
        for name in coverage.tracked_paths(ROOT):
            if name.startswith("verification/service_protocol/") and Path(name).suffix in {".tla", ".json", ".py"}:
                self.assertIn(name, protected)
        # The uncited narrative README is not an executable model or contract.
        self.assertNotIn("verification/service_protocol/README.md", protected)


if __name__ == "__main__":
    unittest.main()
