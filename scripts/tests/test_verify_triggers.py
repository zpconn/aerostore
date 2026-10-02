"""Keep proof inputs and new import shadows reachable by the verification gate."""
import ast
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("trigger_coverage", ROOT / "scripts/check_formal_coverage.py")
coverage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage)


def workflow_patterns():
    # This workflow deliberately keeps its PR path list as quoted scalar lines.
    # Fail if that format changes instead of silently parsing a partial list.
    text = (ROOT / ".github/workflows/verify.yml").read_text()
    block = text.split("    paths:\n", 1)[1].split("  push:\n", 1)[0]
    result = []
    for line in block.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith("      - '"):
            raise AssertionError("unrecognized workflow path entry: " + line)
        result.append(ast.literal_eval(line.strip()[2:]))
    return result


def glob_expression(pattern):
    # The checked-in patterns use only *, ** and literal characters.
    result = []
    while pattern:
        if pattern.startswith("**/"):
            result.append("(?:.*/)?")
            pattern = pattern[3:]
        elif pattern.startswith("**"):
            result.append(".*")
            pattern = pattern[2:]
        elif pattern.startswith("*"):
            result.append("[^/]*")
            pattern = pattern[1:]
        else:
            result.append(re.escape(pattern[0]))
            pattern = pattern[1:]
    return "".join(result)


def triggers(path, patterns):
    selected = False
    for pattern in patterns:
        negative = pattern.startswith("!")
        if re.fullmatch(glob_expression(pattern[1:] if negative else pattern), path):
            selected = not negative
    return selected


class VerifyTriggerTests(unittest.TestCase):
    def test_every_tracked_proof_input_and_lock_reaches_verify(self):
        patterns = workflow_patterns()
        for name in [*coverage.frozen_paths(ROOT), coverage.LOCK]:
            with self.subTest(path=name):
                self.assertTrue(triggers(name, patterns))

    def test_new_import_shadows_and_build_controls_reach_verify(self):
        patterns = workflow_patterns()
        for path in ["scripts/json.py", "scripts/check_lock_models/__init__.py",
                     "scripts/hashlib/__init__.py", ".cargo/config.toml",
                     "aerostore_core/.cargo/config.toml", "aerostore_core/build.rs",
                     "aerostore_core/rust-toolchain.toml", "aerostore_macros/src/lib.rs"]:
            with self.subTest(path=path):
                self.assertTrue(triggers(path, patterns))

    def test_ordinary_docs_and_known_bench_changes_do_not_trigger_verify(self):
        patterns = workflow_patterns()
        for path in ["README.md", "docs/hyperfeed_queue_profile.md", "verification/README.md",
                     "verification/service_protocol/README.md", "aerostore_core/benches/hyperfeed_crucible.rs",
                     "scripts/iterate_hyperfeed.py", "scripts/qualify_hyperfeed.py",
                     "scripts/tests/test_hyperfeed_screen_capture.py"]:
            with self.subTest(path=path):
                self.assertFalse(triggers(path, patterns))


def shell_step(name):
    text = (ROOT / ".github/workflows/verify.yml").read_text()
    step = text.split("      - name: " + name + "\n", 1)[1].split("\n      - name:", 1)[0]
    return "\n".join(line[10:] for line in step.split("        run: |\n", 1)[1].splitlines())


class WorkflowEnforcementTests(unittest.TestCase):
    def run_anchor(self, scenario, mode="review"):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            (root / "scripts/ci_formal_anchor.py").write_text(
                "import os\nwith open(os.environ['GITHUB_OUTPUT'], 'a') as out: "
                "out.write('anchored=true\\nbaseline_ref=forged\\n')\n")
            git = root / "bin/git"
            git.write_text("""#!/bin/bash
case "$1" in
  rev-parse) echo bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ;;
  cat-file) exit 0 ;;
  ls-tree)
    case "$SCENARIO" in
      unavailable-tree) exit 128 ;;
      unavailable-blob) printf '100644 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\t%s\n' "$4" ;;
      symlink) printf '120000 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\t%s\n' "$4" ;;
      absent) exit 0 ;;
      *) exit 2 ;;
    esac ;;
  show) exit 128 ;;
  *) exit 2 ;;
esac
""")
            git.chmod(0o755)
            output = root / "output"
            output.touch()
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                       SCENARIO=scenario, RUNNER_TEMP=str(root), GITHUB_WORKSPACE=str(root),
                       GITHUB_OUTPUT=str(output), FORMAL_BASE_SHA="a" * 40, FORMAL_MODE=mode)
            result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c",
                                     shell_step("Check with the independent base helper and checker")],
                                    cwd=root, env=env, capture_output=True, text=True)
            return result, output.read_text()

    def test_unavailable_or_nonregular_base_helper_never_becomes_bootstrap(self):
        for scenario in ["unavailable-tree", "unavailable-blob", "symlink"]:
            with self.subTest(scenario=scenario):
                result, output = self.run_anchor(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(output, "")

    def test_missing_base_helper_forces_unanchored_review(self):
        result, output = self.run_anchor("absent")
        self.assertEqual(result.returncode, 0, result.stderr)
        values = dict(line.split("=", 1) for line in output.splitlines())
        self.assertEqual(values["anchored"], "false")
        self.assertEqual(values["baseline_ref"], "")
        self.assertEqual(values["status"], "bootstrap_unanchored")

    def test_candidate_cannot_make_an_experiment_bootstrap_pass(self):
        result, output = self.run_anchor("absent", mode="experiment")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("anchored=false", output)

    def test_final_shell_cannot_hide_pilot_or_boundary_failure_or_unanchored_experiment(self):
        with tempfile.TemporaryDirectory() as temporary:
            (Path(temporary) / "aerostore-base-status.py").write_text("raise SystemExit(0)\n")
            for pilot, boundary, mode, anchored in [
                    ("failure", "success", "review", "true"),
                    ("skipped", "success", "review", "false"),
                    ("success", "failure", "review", "false"),
                    ("success", "success", "experiment", "false")]:
                with self.subTest(pilot=pilot, boundary=boundary, mode=mode):
                    env = dict(os.environ, RUNNER_TEMP=temporary, PILOT_RESULT=pilot,
                               BOUNDARY_RESULT=boundary, FORMAL_MODE=mode, BASE_ANCHORED=anchored)
                    result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c",
                                             shell_step("Report and enforce verification results")],
                                            env=env, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
