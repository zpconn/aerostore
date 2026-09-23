#!/usr/bin/env python3
"""Exercise fail-closed parsing and decisions against retained real reports."""

import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

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

    def test_seeded_report_requires_exact_seed_algorithm_and_unique_marker(self):
        marker = "hyperfeed_crucible_seed: mode=fixed seed=2026092301 algorithm=worker_add_xorshift64_v1\n"
        result = gate.parse_churn(marker + self.churn, 120, expected_seed=2026092301)
        self.assertEqual(result["workload_seed"], 2026092301)
        self.assertEqual(result["seed_metadata"]["mode"], "fixed")
        for text in (self.churn, marker + marker + self.churn,
                     marker.replace("2026092301", "2026092302") + self.churn,
                     marker.replace("worker_add_xorshift64_v1", "different") + self.churn,
                     marker.replace("seed=2026092301", "seed=7 seed=2026092301") + self.churn,
                     marker.rstrip() + " ignored junk\n" + self.churn,
                     marker.replace("mode=fixed", "mode=entropy") + self.churn):
            with self.subTest(text=text[:100]), self.assertRaises(ValueError):
                gate.parse_churn(text, 120, expected_seed=2026092301)
        with self.assertRaises(ValueError):
            gate.parse_churn(marker + self.churn, 120)
        self.assertEqual(gate.parse_churn(marker.replace("2026092301", "0") + self.churn, 120,
                                        expected_seed=0)["workload_seed"], 0)

    def test_entropy_and_historical_reports_remain_distinct_from_seeded_runs(self):
        self.assertIsNone(gate.parse_churn(self.churn, 120)["seed_metadata"])
        marker = "hyperfeed_crucible_seed: mode=entropy seed=none algorithm=pid_time_xorshift64_v1\n"
        self.assertEqual(gate.parse_churn(marker + self.churn, 120)["seed_metadata"]["mode"], "entropy")
        with self.assertRaises(ValueError):
            gate.parse_churn(marker + self.churn, 120, expected_seed=0)

    def test_seed_parser_accepts_full_u64_domain_and_rejects_malformed_input(self):
        for text, expected in (("0", 0), ("18446744073709551615", (1 << 64) - 1)):
            self.assertEqual(gate.parse_seed(text), expected)
        for text in ("", "-1", "+1", " 1", "1 ", "1.0", "18446744073709551616", "١"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                gate.parse_seed(text)

    def test_seed_plan_preserves_pair_identity_without_mutating_default_workloads(self):
        names = ["churn_128m_120s", "churn_128m_240s"]
        before = copy.deepcopy(gate.WORKLOADS)
        selected = gate.selected_workloads(names, 3, "11,22,33", "44")
        self.assertEqual([gate.workload_seed(selected[names[0]], n) for n in (1, 2, 3)], [11, 22, 33])
        self.assertEqual(gate.workload_seed(selected[names[1]], 1), 44)
        self.assertEqual(gate.WORKLOADS, before)
        for seeds in ("11,22", "11,22,33,44", "11,,33"):
            with self.subTest(seeds=seeds), self.assertRaises(ValueError):
                gate.selected_workloads(names, 3, seeds, None)
        with self.assertRaises(ValueError):
            gate.selected_workloads(["wal_sync_async_10000"], 3, "11,22,33", None)
        with self.assertRaises(ValueError):
            gate.workload_seed(selected[names[0]], 4)

    def test_run_one_injects_declared_pair_seed_into_environment_and_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fixture"
            binary.write_bytes(b"not executed: subprocess is replaced")
            capture = {"binaries": {gate.BENCHES[0]: {"resolved_path": str(binary)}}}
            workload = {"kind": "churn", "duration": 120, "paired_timing": True, "seeds": [11, 22, 33]}
            def fake_process(command, **kwargs):
                self.assertEqual(kwargs["env"]["AEROSTORE_CRUCIBLE_SEED"], "22")
                kwargs["stdout"].write("hyperfeed_crucible_seed: mode=fixed seed=22 algorithm=worker_add_xorshift64_v1\n" + self.churn)
                kwargs["stdout"].flush()
                return SimpleNamespace(wait=lambda **ignored: 0)
            with patch.dict(gate.os.environ, {"AEROSTORE_CRUCIBLE_SEED": "999"}), \
                 patch.object(gate.subprocess, "Popen", side_effect=fake_process), \
                 patch.object(gate, "snapshot_environment", return_value={}):
                result = gate.run_one(capture, "candidate", "churn", workload, 2, root)
            self.assertEqual(result["workload_seed"], 22)
            self.assertEqual(result["metrics"]["workload_seed"], 22)
            self.assertEqual(result["workload_environment"]["AEROSTORE_CRUCIBLE_SEED"], "22")

    def test_wal_libtest_inline_prefix_is_parsed(self):
        text = ("running 1 test\ntest benchmark_async_synchronous_commit_modes ... "
                "wal_ring_benchmark: txns=10000 sync_tps=547.94 async_tps=1112746.73 ratio=2030.79x\n"
                "ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 18.26s\n")
        self.assertEqual(gate.parse_wal(text)["operations_per_mode"], 10000)
        self.assertEqual(gate.parse_wal(text.replace("... wal_ring_benchmark", "... \nwal_ring_benchmark"))["sync_throughput"], 547.94)
        for bad in (text.replace("benchmark_async_synchronous_commit_modes", "different_test"),
                    text + "wal_ring_benchmark: txns=10000 sync_tps=10 async_tps=100 ratio=10x\n"):
            with self.assertRaises(ValueError):
                gate.parse_wal(bad)

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
        baseline = dict(throughput=100, p99_us=100, p99_interval_ns=[100000, 100000],
                        retry_rate=0.1, arena_high_water_bytes=4000000)
        candidate = dict(throughput=throughput, p99_us=p99, p99_interval_ns=[int(p99 * 1000)] * 2,
                         retry_rate=retry, arena_high_water_bytes=arena)
        return dict(baseline=baseline, candidate=candidate)

    def bounded_churn(self, lower=130048, upper=131071, samples=None):
        if samples is None:
            samples = gate.parse_churn(self.churn, 120)["operations"]
        return self.churn + (f"\nhyperfeed_crucible_latency_bounds: profile=diagnostic engine=aerostore "
                             f"histogram_subdivisions=64 samples={samples} "
                             f"p99_lower_ns={lower} p99_upper_ns={upper}\n")

    def test_integer_histogram_bounds_parse(self):
        result = gate.parse_churn(self.bounded_churn(), 120)
        self.assertEqual(result["p99_interval_ns"], [130048, 131071])
        self.assertEqual(result["p99_precision"], "integer_64_subdivisions")

    def test_missing_inverted_wide_or_mismatched_bounds_rejected(self):
        for value in (self.bounded_churn().replace("p99_upper_ns=131071", "missing=131071"),
                      self.bounded_churn(lower=131072), self.bounded_churn(lower=1000),
                      self.bounded_churn(samples=1), self.bounded_churn(upper=130100)):
            with self.subTest(value=value[-200:]), self.assertRaises((ValueError, KeyError)):
                gate.parse_churn(value, 120)

    def test_equal_coarse_buckets_cannot_pass_latency_acceptance(self):
        pair = self.pair()
        for variant in pair.values():
            variant.pop("p99_interval_ns")
        result = gate.compare_pairs([pair] * 3, gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(result["status"], "inconclusive_resolution")
        self.assertEqual(result["metrics"]["throughput"]["status"], "pass")

    def test_interval_threshold_overlap_is_inconclusive(self):
        pair = self.pair(p99=111)
        pair["baseline"]["p99_interval_ns"] = [100000, 101000]
        pair["candidate"]["p99_interval_ns"] = [110000, 111000]
        result = gate.compare_pairs([pair] * 3, gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(result["status"], "inconclusive_resolution")
        self.assertLess(result["metrics"]["p99_us"]["median_paired_ratio_lower_bound"], 1.1)
        self.assertGreater(result["metrics"]["p99_us"]["median_paired_ratio_upper_bound"], 1.1)

    def test_interval_wholly_inside_margin_passes(self):
        pair = self.pair(p99=108)
        pair["baseline"]["p99_interval_ns"] = [100000, 101000]
        pair["candidate"]["p99_interval_ns"] = [107000, 108000]
        result = gate.compare_pairs([pair] * 3, gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(result["status"], "pass")

    def test_interval_wholly_outside_margin_rejected(self):
        pair = self.pair(p99=113)
        pair["baseline"]["p99_interval_ns"] = [100000, 101000]
        pair["candidate"]["p99_interval_ns"] = [112000, 113000]
        result = gate.compare_pairs([pair] * 3, gate.WORKLOADS["churn_128m_120s"])
        self.assertEqual(result["status"], "regression")

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

    def test_fixture_recapture_does_not_allow_production_or_patch_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, patch = root / "binary", root / "fixture.patch"
            binary.write_bytes(b"executable")
            patch.write_text("benchmark-only patch\n")
            parent = {"complete": True, "features": "default", "rustc": "release: 1.93.1\n",
                      "source_sha256": {"aerostore_core/src/execution.rs": "production", "aerostore_core/benches/test.rs": "old"},
                      "binaries": {n: {"path": str(binary), "sha256": gate.digest(binary)}
                                   for n in (*gate.BENCHES, "wal_ring_benchmark")}}
            parent_path = root / "parent.json"
            parent_path.write_text(json.dumps(parent))
            child = copy.deepcopy(parent)
            child["source_sha256"]["aerostore_core/benches/test.rs"] = "new"
            child["fixture_patch"] = {"path": str(patch), "sha256": gate.digest(patch)}
            child["fixture_parent_capture"] = {"path": str(parent_path), "sha256": gate.digest(parent_path),
                                                "production_unchanged": True,
                                                "exact_patch_transition_verified": True}
            child_path = root / "child.json"
            child_path.write_text(json.dumps(child))
            gate.validate_manifest(child_path)
            child["source_sha256"]["aerostore_core/src/execution.rs"] = "changed"
            child_path.write_text(json.dumps(child))
            with self.assertRaises(ValueError):
                gate.validate_manifest(child_path)
            child["source_sha256"]["aerostore_core/src/execution.rs"] = "production"
            child_path.write_text(json.dumps(child))
            patch.write_text("different patch\n")
            with self.assertRaises(ValueError):
                gate.validate_manifest(child_path)

    def test_exact_patch_must_reconstruct_the_parent_and_stay_in_fixture_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            fixture = source / "aerostore_core/benches/example.rs"
            fixture.parent.mkdir(parents=True)
            fixture.write_text("old\n")
            parent = {"aerostore_core/benches/example.rs": gate.digest(fixture)}
            fixture.write_text("new\n")
            current = {"aerostore_core/benches/example.rs": gate.digest(fixture)}
            patch = root / "fixture.patch"
            patch.write_text("--- a/aerostore_core/benches/example.rs\n"
                             "+++ b/aerostore_core/benches/example.rs\n@@ -1 +1 @@\n-old\n+new\n")
            self.assertEqual(gate.verify_fixture_transition(source, current, parent, patch),
                             ["aerostore_core/benches/example.rs"])
            with self.assertRaises(ValueError):
                gate.verify_fixture_transition(source, current, {**parent, "unrelated.rs": "extra"}, patch)
            patch.write_text(patch.read_text().replace("/benches/", "/src/"))
            with self.assertRaises(ValueError):
                gate.verify_fixture_transition(source, current, parent, patch)


if __name__ == "__main__":
    unittest.main()
