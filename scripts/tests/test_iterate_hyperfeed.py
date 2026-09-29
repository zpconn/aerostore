import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import iterate_hyperfeed as loop


def manifest():
    return {"comparison_build_identity": {"compiler": "same"},
            "source": {"root": "/frozen/source", "files": {
                "scripts/qualify_hyperfeed.py": "driver",
                "aerostore_core/src/lib.rs": "engine",
                "aerostore_core/benches/contention_crucible/service.rs": "transport",
                "aerostore_core/benches/contention_crucible/capacity.rs": "accounting"}},
            "benchmark": {"path": "/frozen/benchmark", "sha256": "a" * 64}}


class IterationTests(unittest.TestCase):
    def test_crossover_pairs_use_same_seed_and_reverse_order(self):
        rows = loop.schedule("foreground", [10, 20])
        self.assertEqual([(r["variant"], r["seed"]) for r in rows],
                         [("baseline", 10), ("candidate", 10), ("candidate", 20), ("baseline", 20)])
        self.assertEqual(len({r["id"] for r in rows}), 4)

    def test_duplicate_seed_refused(self):
        with self.assertRaises(ValueError):
            loop.schedule("foreground", [10, 10])

    def test_identical_binary_control(self):
        self.assertTrue(loop.compare_inputs(manifest(), manifest(), set())["identical_binary"])

    def test_declared_transport_change_allowed(self):
        a, b = manifest(), manifest()
        path = "aerostore_core/benches/contention_crucible/service.rs"
        b["source"]["files"][path] = "new"
        self.assertEqual(loop.compare_inputs(a, b, {path})["implementation_changes"], [path])
        with self.assertRaises(ValueError):
            loop.compare_inputs(a, b, set())

    def test_accounting_change_cannot_be_declared_as_optimization(self):
        a, b = manifest(), manifest()
        path = "aerostore_core/benches/contention_crucible/capacity.rs"
        b["source"]["files"][path] = "new"
        with self.assertRaisesRegex(ValueError, "workload/accounting"):
            loop.compare_inputs(a, b, {path})

    def test_changed_driver_or_build_recipe_rejected(self):
        for change in ("driver", "compiler"):
            a, b = manifest(), manifest()
            if change == "driver":
                b["source"]["files"]["scripts/qualify_hyperfeed.py"] = "new"
            else:
                b["comparison_build_identity"]["compiler"] = "new"
            with self.assertRaises(ValueError):
                loop.compare_inputs(a, b, set())

    def test_new_tests_are_recorded_without_changing_workload(self):
        a, b = manifest(), manifest()
        b["source"]["files"]["aerostore_core/tests/new.rs"] = "test"
        self.assertEqual(loop.compare_inputs(a, b, set())["auxiliary_changes"],
                         ["aerostore_core/tests/new.rs"])

    def test_foreground_and_stress_commands_keep_other_contracts_fixed(self):
        commands = []
        for lane in ("foreground", "maintenance"):
            command = loop.qualifier_command(manifest(), Path("/output"), lane, 3584, 29)
            self.assertEqual(command[1], "/frozen/source/scripts/qualify_hyperfeed.py")
            fields = dict(zip(command[2::2], command[3::2]))
            self.assertEqual(fields["--evidence"], "metrics")
            self.assertEqual(fields["--workers"], "16")
            self.assertEqual(fields["--families"], "1024")
            commands.append(fields)
        changed = {key for key in commands[0] if commands[0][key] != commands[1][key]}
        self.assertEqual(changed, {"--seconds", "--projection-interval-seconds",
                                   "--housekeeping-interval-seconds", "--timeout-seconds"})

    def test_child_applies_core_and_cpu_limits_before_exec(self):
        command = loop.guarded_command(["/usr/bin/true"], [0, 1])
        self.assertIn("RLIMIT_CORE,(0,0)", command[2])
        self.assertLess(command[2].index("setrlimit"), command[2].index("execv"))
        self.assertIn("sched_setaffinity(0,{0,1})", command[2])

    def test_expected_configuration_binds_rate_and_all_adapter_policies(self):
        values = loop.qualifier_parameters(manifest(), Path("/output"), "foreground", 3584, 29)
        expected = loop.expected_config(values)
        self.assertEqual(expected["arrival_rate"], 3584)
        self.assertEqual(expected["engine"], "service-unix")
        self.assertEqual(expected["seed"], 29)
        self.assertEqual(expected["expiry_index_policy"], "housekeeping")
        self.assertEqual(expected["due_index_policy"], "ordered")
        self.assertEqual(expected["expiry_publication_policy"], "ordered")
        self.assertIs(expected["retry_diagnostics"], False)
        self.assertNotIn("cpu_budget", expected)
        self.assertEqual(expected["families"], 1024)

    def test_prepare_reuses_ancestor_budget(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session = root / "target/session"
            session.mkdir(parents=True)
            baseline = session / "resource-baseline.json"
            baseline.write_text('{}\n')
            args = argparse.Namespace(output=session / "candidate", host_volume=None)
            with patch.object(loop, "ROOT", root), \
                 patch.object(loop.resources, "check_budget", return_value={"passed": True}) as check, \
                 patch.object(loop.resources, "preflight") as preflight, \
                 patch.object(loop.shutil, "copy2"), patch.object(loop, "digest", return_value="hash"):
                _, bound, _ = loop.prepare(args, 123)
            self.assertEqual(bound, baseline)
            check.assert_called_once_with(baseline, prospective_bytes=123)
            preflight.assert_not_called()
            self.assertEqual(baseline.read_text(), '{}\n')

    def test_failed_final_resource_audit_cannot_leave_a_promising_verdict(self):
        record = {"completed": True, "comparison": {"classification": "promising"}}
        loop.final_resource_status(record, {"passed": False, "reasons": ["disk reserve"]})
        self.assertFalse(record["completed"])
        self.assertEqual(record["stop_classification"], "censored_resources")
        self.assertEqual(record["comparison"]["classification"], "inconclusive")
        self.assertEqual(record["comparison_before_resource_exclusion"]["classification"], "promising")


if __name__ == "__main__":
    unittest.main()
