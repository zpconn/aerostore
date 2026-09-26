"""Handshake and shell-boundary regressions for the remote benchmark helper."""
import shlex
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import run_remote_contention as runner


class RemoteHandshakeTests(unittest.TestCase):
    def test_expiry_and_diagnostic_setup_defaults_cannot_hide_peer_mismatch(self):
        runner.validate_dispatch_setup({}, runner.EXPERIMENT_DEFAULTS)
        expected = {"expiry_index_policy":"housekeeping", "retry_diagnostics":True}
        runner.validate_dispatch_setup(expected, expected)
        for setup in ({}, {**expected,"expiry_index_policy":"all-active"},
                      {**expected,"retry_diagnostics":False}, {**expected,"retry_diagnostics":1}):
            with self.subTest(setup=setup), self.assertRaisesRegex(RuntimeError, "differs from the client"):
                runner.validate_dispatch_setup(setup, expected)

    def test_maintenance_setup_defaults_and_every_option_must_match(self):
        runner.validate_dispatch_setup({}, runner.MAINTENANCE_DEFAULTS)
        expected = dict(maintenance_mode="sweep", projection_batch_size=8,
                        housekeeping_batch_size=64, max_maintenance_batches=100)
        runner.validate_dispatch_setup(expected, expected)
        for field, value in (("maintenance_mode", "batch"), ("projection_batch_size", 4),
                             ("housekeeping_batch_size", 32), ("max_maintenance_batches", 4096),
                             ("projection_batch_size", True)):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "differs from the client"):
                runner.validate_dispatch_setup({**expected, field: value}, expected)
        with self.assertRaisesRegex(RuntimeError, "differs from the client"):
            runner.validate_dispatch_setup({}, expected)

    def test_sweep_parameters_reach_both_peer_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "fixture-binary"
            binary.write_bytes(b"fixture; never executed")
            output = Path(directory) / "evidence"
            options = {"--maintenance-mode": "sweep", "--projection-batch-size": "8",
                       "--housekeeping-batch-size": "64", "--max-maintenance-batches": "100",
                       "--expiry-index":"housekeeping", "--retry-diagnostics":"on"}
            argv = ["run_remote_contention.py", "--binary", str(binary), "--output-dir", str(output),
                    "--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                    *[part for pair in options.items() for part in pair]]
            with patch.object(sys, "argv", argv), patch.object(runner.subprocess, "Popen", side_effect=RuntimeError("stop before execution")):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(runner.main(), 1)
            manifest = json.loads((output / "orchestration.json").read_text())
            for command in (manifest["server_command"], manifest["client_command"]):
                for flag, value in options.items():
                    self.assertEqual(command[command.index(flag) + 1], value)

    def test_dispatch_setup_defaults_and_mismatched_alias_policy(self):
        runner.validate_dispatch_setup({}, {"dispatch": "identity", "affinity_ttl_ms": 0, "signature_pattern": "both"})
        expected = {"dispatch": "signature-affinity", "affinity_ttl_ms": 1000, "signature_pattern": "mixed"}
        runner.validate_dispatch_setup(expected, expected)
        for setup in ({}, {**expected, "dispatch": "identity"}, {**expected, "affinity_ttl_ms": 999},
                      {**expected, "signature_pattern": "both"}, {**expected, "affinity_ttl_ms": True}):
            with self.subTest(setup=setup), self.assertRaisesRegex(RuntimeError, "differs from the client"):
                runner.validate_dispatch_setup(setup, expected)

    def test_affinity_and_mixed_identity_parameters_reach_both_peer_commands(self):
        for dispatch, ttl in (("signature-affinity", 1000), ("identity", 0)):
            with self.subTest(dispatch=dispatch), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "fixture-binary"
                binary.write_bytes(b"fixture; never executed")
                output = Path(directory) / "evidence"
                argv = ["run_remote_contention.py", "--binary", str(binary), "--output-dir", str(output),
                        "--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                        "--dispatch", dispatch, "--affinity-ttl-ms", str(ttl), "--signature-pattern", "mixed"]
                with patch.object(sys, "argv", argv), patch.object(runner.subprocess, "Popen", side_effect=RuntimeError("stop before execution")) as start:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(runner.main(), 1)
                    start.assert_called_once()
                manifest = json.loads((output / "orchestration.json").read_text())
                for command in (manifest["server_command"], manifest["client_command"]):
                    for flag, value in (("--dispatch", dispatch), ("--affinity-ttl-ms", str(ttl)), ("--signature-pattern", "mixed")):
                        self.assertEqual(command[command.index(flag) + 1], value)
                self.assertFalse(manifest["passed"])

    def test_invalid_calibrated_schedule_is_rejected_before_starting_any_process(self):
        for options in (["--projection-interval-seconds", "0"],
                        ["--housekeeping-interval-seconds", "3601"],
                        ["--arrival-rate", "0"],
                        ["--dispatch", "signature-affinity"],
                        ["--dispatch", "signature-affinity", "--affinity-ttl-ms", "3600001"],
                        ["--affinity-ttl-ms", "1"],
                        ["--projection-batch-size", "0"], ["--projection-batch-size", "17"],
                        ["--housekeeping-batch-size", "0"], ["--housekeeping-batch-size", "65"],
                        ["--max-maintenance-batches", "0"], ["--max-maintenance-batches", "4097"],
                        ["--workload", "lifecycle", "--maintenance-mode", "sweep"],
                        ["--workload", "fleet", "--projection-batch-size", "8"],
                        ["--workload", "fleet", "--signature-pattern", "mixed"]):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "must-not-be-created"
                argv = ["run_remote_contention.py", "--binary", "/nonexistent/benchmark",
                        "--output-dir", str(output), "--workload", "calibrated",
                        "--families", "16", "--hot-percent", "0", *options]
                with patch.object(sys, "argv", argv), patch.object(runner.subprocess, "Popen") as start:
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                        runner.main()
                    self.assertEqual(error.exception.code, 2)
                    start.assert_not_called()
                    self.assertFalse(output.exists())

    def test_configuration_placeholder_and_malformed_json_values_are_not_ready(self):
        placeholder = {"schema": 1, "passed": False, "completed": False, "stage": "configuration"}
        for frame in [None, [], True, placeholder, {"version": 1, "run_id": "old"}]:
            self.assertIsNone(runner.ready_setup(frame))
        ready = {"version": 1, "run_id": "new", "initial_rows": [], "endpoint": {"Tcp": "127.0.0.1:9000"}}
        frames = iter([placeholder, ready])
        result = runner.wait_until(lambda: runner.ready_setup(next(frames)), time.monotonic()+2)
        self.assertIs(result, ready)

    def test_stale_completion_marker_never_stops_a_different_server_run(self):
        for frame in [{"run_id": "old", "completed": True}, {"run_id": "new", "completed": 1},
                      {"run_id": "new", "completed": False}, [], None]:
            with self.assertRaisesRegex(RuntimeError, "not for this run"):
                runner.validate_marker(frame, "new")
        runner.validate_marker({"run_id": "new", "completed": True}, "new")

    def test_stale_or_malformed_final_state_is_rejected_before_copy(self):
        for frame in [{"version": 1, "run_id": "old", "passed": True},
                      {"version": 2, "run_id": "new", "passed": True},
                      {"version": 1, "run_id": "new", "passed": "true"}, [], None]:
            with self.assertRaisesRegex(RuntimeError, "missing, malformed or stale"):
                runner.validate_final(frame, "new")
        failure = {"version": 1, "run_id": "new", "passed": False, "error": "audit failed"}
        self.assertIs(runner.validate_final(failure, "new"), failure)

    def test_server_exit_is_not_hidden_while_waiting_for_a_ready_frame(self):
        process = Mock(returncode=2)
        process.poll.return_value = 2
        with self.assertRaisesRegex(RuntimeError, "server exited 2"):
            runner.wait_until(lambda: None, time.monotonic()+60, [("server", process)])
        process.poll.assert_called_once()

    def test_remote_shell_arguments_are_quoted_as_arguments(self):
        arguments = ["exec", "/tmp/a binary;touch UNAUTHORIZED", "--output", "/tmp/$(id)/file 'name'"]
        command = runner.ssh_command("fixture-host", arguments)
        self.assertEqual(shlex.split(command[-1]), arguments)
        self.assertIn("BatchMode=yes", command)
        self.assertEqual(command[-2], "fixture-host")

    def test_independent_supervisor_kills_and_reaps_orphaned_owner_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "supervisor.json"
            pidfile = Path(directory) / "child.pid"
            owner = """import os,signal,sys,time
child=os.fork()
if child==0:
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    while True: time.sleep(10)
with open(sys.argv[1],'w') as stream: stream.write(str(child))
os._exit(0)
"""
            result = subprocess.run([sys.executable, "-c", runner.REMOTE_SUPERVISOR,
                                     "2", str(report), sys.executable, "-c", owner, str(pidfile)],
                                    capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            evidence = json.loads(report.read_text())
            self.assertEqual(evidence["reason"], "owner_exited")
            self.assertTrue(evidence["owned_group_kill_sent"])
            self.assertEqual(evidence["adopted_descendants_reaped"], 1)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)

    def test_independent_supervisor_enforces_owner_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "supervisor.json"
            result = subprocess.run([sys.executable, "-c", runner.REMOTE_SUPERVISOR,
                                     "0.1", str(report), sys.executable, "-c", "import time;time.sleep(60)"],
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            evidence = json.loads(report.read_text())
            self.assertEqual(evidence["reason"], "supervisor_deadline")
            self.assertTrue(evidence["owned_group_kill_sent"])
            with self.assertRaises(ProcessLookupError):
                os.kill(evidence["owner_pid"], 0)


if __name__ == "__main__":
    unittest.main()
