#!/usr/bin/env python3
"""Bounded real-process rolling lifecycle checks; no capacity claim.

Select a freshly built binary with AEROSTORE_CONTENTION_BINARY. Each fixture
retains its report, full history, process supervision, and source/binary hashes.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest

import qualify_hyperfeed as gate


@unittest.skipUnless(os.environ.get("AEROSTORE_CONTENTION_BINARY"), "select a freshly built benchmark")
class RollingWorkloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ["AEROSTORE_CONTENTION_BINARY"]).resolve(strict=True)
        parent = Path(os.environ.get("AEROSTORE_ROLLING_TEST_OUTPUT", gate.ROOT / "target/rolling-validation"))
        parent.mkdir(parents=True, exist_ok=True)
        cls.output = Path(tempfile.mkdtemp(prefix="rolling-process-", dir=parent))

    def run_fixture(self, engine, workers, families, rate):
        directory = self.output / f"{engine}-w{workers}"
        directory.mkdir()
        command = [str(self.binary), "--engine", engine, "--mode", "sustained",
                   "--workload", "calibrated", "--evidence", "full",
                   "--maintenance-mode", "sweep", "--rolling-cycle-messages", "16",
                   "--rolling-retention-seconds", "1", "--projection-interval-seconds", "1",
                   "--housekeeping-interval-seconds", "1", "--dispatch", "signature-affinity",
                   "--affinity-ttl-ms", "600", "--signature-pattern", "both",
                   "--workers", str(workers), "--families", str(families), "--hot-percent", "0",
                   "--seed", "20260927", "--seconds", "10", "--arrival-rate", str(rate),
                   "--max-messages", "10000", "--max-backlog", "1000", "--shm-mib", "256",
                   "--output", str(directory / "report.json")]
        before = gate.snapshot_sources()
        binary_before = gate.sha256(self.binary)
        process = gate.run_process(command, directory / "run.log", 120)
        after = gate.snapshot_sources()
        binary_after = gate.sha256(self.binary)
        gate.atomic_json(directory / "test-evidence.json", dict(
            command=command, process=process, source_before=before, source_after=after,
            binary_before_sha256=binary_before, binary_after_sha256=binary_after,
            scope="Ten-second accelerated functional test; projection coverage and capacity unqualified."))
        self.assertEqual(before, after, "sources changed during process test")
        self.assertEqual(binary_before, binary_after)
        self.assertEqual(process["exit_code"], 0, process)
        self.assertFalse(process["timed_out"])
        self.assertTrue(process["owned_processes_terminated"])
        report = json.loads((directory / "report.json").read_text())
        self.assertTrue(report["passed"], report)
        run, = report["runs"]
        assessment = gate.assess_trial(dict(
            config=report["config"], report=report, source_stable=True,
            exit_code=process["exit_code"], timed_out=False),
            dict(slo_ms=1000, max_noop_fraction=.25, minimum_drain_fraction=.8, outcome_tolerance=0))
        self.assertTrue(assessment["execution_valid"], assessment)
        self.assertTrue(assessment["history_verified"], assessment)
        self.assertFalse(assessment["qualified_capacity_trial"])
        self.assertFalse(assessment["rolling_coverage_passed"], "ten seconds cannot cover30-second projections")
        self.assertEqual(run["initial_fleet"]["live_families"], 0)
        self.assertEqual(run["calibrated_schedule"]["housekeeping_seed_cohorts"], 0)
        self.assertGreater(run["rolling_lifecycle"]["retired_families"], 0)
        self.assertGreater(run["rolling_lifecycle"]["reused_family_generations"], 0)
        self.assertEqual(run["workload_classes"]["foreground"]["completed"], 10 * rate)
        self.assertEqual(run["oracle_status"], "Valid")
        self.assertTrue(run["maintenance_job_audit"]["passed"])
        self.assertEqual(run["total_process_workers"], workers + 2)
        return run

    def test_direct_four_workers_reuses_retired_slots_with_complete_history(self):
        self.run_fixture("aerostore", 4, 16, 64)

    def test_central_service_32_workers_reuses_retired_slots_with_complete_history(self):
        self.run_fixture("service-unix", 32, 64, 256)


if __name__ == "__main__":
    unittest.main()
