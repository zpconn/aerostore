#!/usr/bin/env python3
"""Exercise fail-closed parsing and decisions against retained real reports."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import compare_engine_performance as gate


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/bench_data/transactional_indexes_2026-09-22"


class ComparisonGateTests(unittest.TestCase):
    def setUp(self):
        self.churn = (EVIDENCE / "churn_128m/crucible_profile_2g_120s.log").read_text()
        self.extended = json.loads((EVIDENCE / "aerostore_64f_16c_8w_128m.json").read_text())
        self.config = dict(families=64, cycles=16, workers=8, seed=8675309)

    def test_real_retained_reports_parse(self):
        self.assertGreater(gate.parse_churn(self.churn, 120)["operations"], 1000000)
        self.assertEqual(gate.parse_extended(self.extended, self.config)["messages"], 30720)

    def test_missing_allocation_audit_rejected(self):
        with self.assertRaises(ValueError):
            gate.parse_churn(self.churn.replace("status=pass", "status=missing"), 120)

    def test_correctness_failure_cannot_be_hidden_by_fast_rate(self):
        report = copy.deepcopy(self.extended)
        report["contracts"][0]["cases"][0]["passed"] = False
        report["replays"][0]["committed_messages_per_second"] = 1e12
        with self.assertRaises(ValueError):
            gate.parse_extended(report, self.config)

    def test_stale_configuration_rejected(self):
        report = copy.deepcopy(self.extended)
        report["config"]["cycles"] = 2
        with self.assertRaises(ValueError):
            gate.parse_extended(report, self.config)

    def test_missing_phase_rejected(self):
        report = copy.deepcopy(self.extended)
        report["replays"][0]["phases"].pop()
        with self.assertRaises(ValueError):
            gate.parse_extended(report, self.config)

    def test_negative_nan_or_infinite_metrics_rejected(self):
        for value in (-1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                gate.finite_number(value)

    def pair(self, throughput=100, p99=100, retry=0.1, arena=4000000):
        baseline = dict(throughput=100, p99_us=100, retry_rate=0.1, arena_high_water_bytes=4000000)
        candidate = dict(throughput=throughput, p99_us=p99, retry_rate=retry, arena_high_water_bytes=arena)
        return dict(baseline=baseline, candidate=candidate)

    def test_material_slowdown_rejected(self):
        report = gate.compare_pairs([self.pair(throughput=80)] * 3, gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(report["status"], "regression")

    def test_noisy_faster_median_cannot_pass(self):
        report = gate.compare_pairs([self.pair(throughput=n) for n in (100, 110, 120)],
                                    gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(report["status"], "inconclusive_noise")
        self.assertFalse(report["speedup_claim"])

    def test_retry_or_resource_regression_rejected(self):
        for pair in (self.pair(retry=0.3), self.pair(arena=8000000)):
            with self.subTest(pair=pair):
                self.assertEqual(gate.compare_pairs([pair] * 3, gate.WORKLOADS["churn_128m_120s"])["status"],
                                 "regression")

    def test_single_long_pair_cannot_claim_timing_acceptance(self):
        result = gate.compare_pairs([self.pair()], gate.WORKLOADS["churn_128m_240s"])
        self.assertFalse(result["timing_acceptance"])

    def test_changed_executable_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, manifest = Path(directory) / "binary", Path(directory) / "manifest.json"
            binary.write_bytes(b"original")
            capture = {"complete": True, "features": "default", "rustc": "release: 1.93.1\n",
                       "binaries": {n: {"path": str(binary), "sha256": gate.digest(binary)}
                                    for n in (*gate.BENCHES, "wal_ring_benchmark")}}
            manifest.write_text(json.dumps(capture))
            binary.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                gate.validate_manifest(manifest)

    def test_changed_source_after_capture_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "Cargo.toml").write_text("[workspace]\n")
            (source / "Cargo.lock").write_text("version = 4\n")
            capture = {"source": str(source), "source_sha256": gate.source_hashes(source)}
            gate.validate_captured_source(capture)
            (source / "Cargo.toml").write_text("[workspace]\n# changed\n")
            with self.assertRaises(ValueError):
                gate.validate_captured_source(capture)


if __name__ == "__main__":
    unittest.main()
