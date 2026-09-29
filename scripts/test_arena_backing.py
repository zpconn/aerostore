#!/usr/bin/env python3
"""Placement identity and observed-storage regressions; no database processes."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import qualify_hyperfeed as gate
import run_remote_contention as remote
from test_hyperfeed_qualification import POLICY, trial


def metadata(backing="file", engine="aerostore", filesystem=None):
    native = engine != "postgres"
    fs = filesystem or ("tmpfs" if backing == "memfd" else "ext2/ext3/ext4")
    return {"format": "arena-backing-v1", "requested": backing,
            "effective": backing if native else "not_applicable", "observed": native,
            "filesystem_type": fs if native else None,
            "filesystem_magic": {"tmpfs": "0x1021994", "ext2/ext3/ext4": "0xef53", "other": "0x794c7630"}[fs] if native else None,
            "lifetime": ("path_survives_owner_until_unlinked" if backing == "file" else
                         "new_attachments_require_live_owner_existing_mappings_survive") if native else None,
            "wal_placement": "case_directory_file" if native else "postgres_managed"}


def with_backing(item, backing):
    item["config"]["arena_backing"] = item["report"]["config"]["arena_backing"] = backing
    item["report"]["runs"][0]["arena_backing_metadata"] = metadata(backing, item["config"]["engine"])
    return item


class ArenaBackingTests(unittest.TestCase):
    def test_legacy_default_is_unobserved_and_cannot_match_memfd(self):
        old = trial()
        checked = gate.assess_trial(old, POLICY)
        self.assertTrue(checked["history_verified"])
        self.assertEqual(checked["arena_backing"]["status"], "legacy_unobserved")
        self.assertFalse(checked["arena_backing"]["observed"])
        self.assertEqual(gate.key(old["config"]), gate.key(with_backing(trial(), "file")["config"]))
        memfd = with_backing(trial(evidence="metrics"), "memfd")
        self.assertNotEqual(gate.key(old["config"]), gate.key(memfd["config"]))
        self.assertFalse(gate.assess_trial(memfd, POLICY, {gate.key(old["config"])})["correctness_companion_verified"])

    def test_observation_and_companion_bind_each_engine_and_backing(self):
        for engine in gate.ENGINES:
            for backing in ("file", "memfd"):
                with self.subTest(engine=engine, backing=backing):
                    full = with_backing(trial(engine=engine), backing)
                    checked = gate.assess_trial(full, POLICY)
                    self.assertTrue(checked["history_verified"], checked)
                    self.assertEqual(checked["arena_backing"]["status"],
                                     "not_applicable" if engine == "postgres" else "observed")
                    measured = with_backing(trial(engine=engine, evidence="metrics"), backing)
                    self.assertTrue(gate.assess_trial(measured, POLICY,
                        {gate.key(full["config"])})["correctness_companion_verified"])

    def test_modern_default_cannot_drop_observation_or_config(self):
        for engine in gate.ENGINES:
            for backing in ("file", "memfd"):
                for malformed in (None, [], 0, False, "file"):
                    item = with_backing(trial(engine=engine), backing)
                    item["report"]["runs"][0]["arena_backing_metadata"] = malformed
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
                item = with_backing(trial(engine=engine), backing)
                del item["report"]["runs"][0]["arena_backing_metadata"]
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        # Dropping the driver field cannot erase the modern executable's duty.
        item = trial()
        item["report"]["config"]["arena_backing"] = "file"
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_every_observation_field_is_checked_including_pg_noop(self):
        for engine in gate.ENGINES:
            for backing in ("file", "memfd"):
                base = with_backing(trial(engine=engine), backing)
                receipt = base["report"]["runs"][0]["arena_backing_metadata"]
                for field in receipt:
                    values = (None, True, 0, [], {}, "wrong")
                    for value in values:
                        if type(value) is type(receipt[field]) and value == receipt[field]:
                            continue
                        with self.subTest(engine=engine, backing=backing, field=field, value=value):
                            item = copy.deepcopy(base)
                            item["report"]["runs"][0]["arena_backing_metadata"][field] = value
                            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
                    item = copy.deepcopy(base)
                    del item["report"]["runs"][0]["arena_backing_metadata"][field]
                    self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_file_mode_reports_actual_filesystem_but_memfd_requires_tmpfs(self):
        for fs in ("ext2/ext3/ext4", "tmpfs", "other"):
            item = with_backing(trial(), "file")
            item["report"]["runs"][0]["arena_backing_metadata"] = metadata("file", filesystem=fs)
            self.assertTrue(gate.assess_trial(item, POLICY)["execution_valid"])
            if fs != "tmpfs":
                item = with_backing(trial(), "memfd")
                item["report"]["runs"][0]["arena_backing_metadata"] = metadata("memfd", filesystem=fs)
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        for fs, magic in (("ext2/ext3/ext4", "0x1021994"), ("tmpfs", "0xef53"), ("other", "0xef53")):
            item = with_backing(trial(), "file")
            item["report"]["runs"][0]["arena_backing_metadata"].update(filesystem_type=fs, filesystem_magic=magic)
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_requested_or_reported_mismatch_and_invalid_configuration_reject(self):
        for invalid in (None, [], {}, True, 0, "tmpfs", ""):
            item = with_backing(trial(), "memfd")
            item["config"]["arena_backing"] = invalid
            self.assertFalse(gate.arena_backing_assessment(item["report"]["runs"][0], item["config"])["passed"])
        for where in ("config", "report"):
            item = with_backing(trial(), "memfd")
            config = item["config"] if where == "config" else item["report"]["config"]
            config["arena_backing"] = "file"
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_duplicate_observations_require_strict_types_in_every_receipt(self):
        for engine in ("aerostore", "postgres"):
            for backing in ("file", "memfd"):
                for where in ("root", "run"):
                    for number in (1, 1.0) if engine != "postgres" else (0, 0.0):
                        with self.subTest(engine=engine, backing=backing, where=where, number=number):
                            item = with_backing(trial(engine=engine), backing)
                            item["report"]["arena_backing_metadata"] = metadata(backing, engine)
                            self.assertTrue(gate.trial_arena_assessment(item)["passed"])
                            container = item["report"] if where == "root" else item["report"]["runs"][0]
                            container["arena_backing_metadata"]["observed"] = number
                            # These mappings compare equal despite the invalid type.
                            self.assertEqual(item["report"]["arena_backing_metadata"],
                                             item["report"]["runs"][0]["arena_backing_metadata"])
                            checked = gate.trial_arena_assessment(item)
                            self.assertFalse(checked["passed"], checked)
                            self.assertEqual(checked["status"], "invalid")

    def test_same_requested_backing_allows_pg_matrix_without_claiming_pg_arena(self):
        rows = [with_backing(trial(engine=engine, seed=seed), "memfd")
                for engine in ("aerostore", "postgres") for seed in (11, 22, 33)]
        result = gate.build_gate(rows, POLICY, ["aerostore", "postgres"], [32], [1], [11, 22, 33])
        self.assertTrue(result["corpus_configuration_matches"])

    def test_qualifier_records_and_forwards_default_and_explicit_choice(self):
        for flags, expected in (([], "file"), (["--arena-backing", "memfd"], "memfd")):
            with tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "benchmark"; binary.write_bytes(b"never executed")
                output = Path(directory) / "evidence"
                with patch.object(gate, "snapshot_sources", return_value={"sha256": "source", "files": {}}), \
                     patch.object(gate, "host_info", return_value={}), \
                     patch.object(gate.subprocess, "check_output", return_value="fixture"), \
                     patch.object(gate, "run_process", side_effect=RuntimeError("stop before execution")) as start, \
                     contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(gate.main(["--binary", str(binary), "--output", str(output),
                        "--engines", "aerostore", "--rates", "32", "--workers", "1", "--seeds", "11",
                        "--slo-ms", "100", *flags]), 1)
                cell = json.loads((output / "campaign.json").read_text())["trials"][0]
                self.assertEqual(cell["config"]["arena_backing"], expected)
                for command in (cell["command"], start.call_args.args[0]):
                    self.assertEqual(command[command.index("--arena-backing") + 1], expected)

    def test_ambient_override_rejected_before_launch_and_old_success_invalidated(self):
        for value in ("", "file", "memfd"):
            with tempfile.TemporaryDirectory() as directory, \
                 patch.dict(os.environ, {"AEROSTORE_CONTENTION_ARENA_BACKING": value}), \
                 patch.object(gate, "run_process") as start, contextlib.redirect_stderr(io.StringIO()):
                path = Path(directory) / "campaign.json"; path.write_text('{"passed":true}')
                with self.assertRaises(SystemExit):
                    gate.main(["--binary", "/missing", "--output", directory, "--slo-ms", "100"])
                start.assert_not_called()
                self.assertFalse(json.loads(path.read_text())["passed"])

    def test_remote_requires_matching_observed_server_backing(self):
        remote.validate_dispatch_setup({}, {})
        for backing in ("file", "memfd"):
            expected = {"arena_backing": backing}
            setup = {**expected, "arena_backing_metadata": metadata(backing)}
            remote.validate_dispatch_setup(setup, expected)
            for bad in ({}, expected, {**setup, "arena_backing": "other"},
                        {**setup, "arena_backing_metadata": {**metadata(backing), "observed": False}}):
                with self.subTest(backing=backing, bad=bad), self.assertRaises(RuntimeError):
                    remote.validate_dispatch_setup(bad, expected)

    def test_remote_commands_forward_backing_to_both_peers(self):
        for flags, backing in (([], "file"), (["--arena-backing", "memfd"], "memfd")):
            with tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "benchmark"; binary.write_bytes(b"never executed")
                output = Path(directory) / "evidence"
                argv = ["run_remote_contention.py", "--binary", str(binary), "--output-dir", str(output), *flags]
                with patch.object(sys, "argv", argv), \
                     patch.object(remote.subprocess, "Popen", side_effect=RuntimeError("stop before execution")), \
                     contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(remote.main(), 1)
                result = json.loads((output / "orchestration.json").read_text())
                self.assertEqual(result["requested_arena_backing"], backing)
                for command in (result["server_command"], result["client_command"]):
                    self.assertEqual(command[command.index("--arena-backing") + 1], backing)


if __name__ == "__main__":
    unittest.main()
