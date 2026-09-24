#!/usr/bin/env python3
"""Adversarial checks for the experiment's stale-evidence/change gate."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch
import verify_formal
import check_lock_models as lock_models
import check_refinement_evidence as refinement
import check_p1_native_evidence as p1_native
import check_planning_native_evidence as planning_native

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("coverage", HERE / "check_formal_coverage.py")
coverage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(coverage)


class FrozenBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="aerostore-formal-gate-")
        self.root = Path(self.directory.name)
        # Only copy the reviewed files, not build outputs or downloaded tools.
        required = set(coverage.frozen_paths(coverage.ROOT))
        claims = tomllib.loads((coverage.ROOT / "verification/claims.toml").read_text())
        for claim in claims["claims"]:
            required.update(claim["implementation"])
        for name in required:
            source = coverage.ROOT / name
            if source.exists():
                destination = self.root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
        kernel = self.root / "aerostore_verified/src/lib.rs"
        kernel.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(coverage.ROOT / "aerostore_verified/src/lib.rs", kernel)
        files = {name: coverage.digest(self.root / name) for name in coverage.frozen_paths(self.root)}
        (self.root / coverage.LOCK).write_text(json.dumps({"format_version": 1, "files": files}))

    def tearDown(self):
        self.directory.cleanup()

    def test_unchanged_boundary_passes(self):
        self.assertTrue(coverage.validate(self.root)["passed"])

    def test_weakened_contract_fails(self):
        (self.root / "verification/contracts/bucket_set.md").write_text("Always return an empty set.\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_modified_atomic_primitive_fails(self):
        with (self.root / "aerostore_core/src/shm_lock.rs").open("a") as output:
            output.write("\n// changed primitive boundary\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_new_unproved_engine_module_fails(self):
        (self.root / "aerostore_core/src/unchecked_optimization.rs").write_text("pub fn bypass() {}\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_proof_runner_fails(self):
        (self.root / "scripts/check_lean.py").write_text("raise SystemExit(0)\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_weakened_concurrent_primitive_contract_fails(self):
        (self.root / "verification/concurrent/contracts.rs").write_text("// silently assume publication\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_weakened_predicate_contract_fails(self):
        (self.root / "verification/predicate_capture/contracts.rs").write_text("// drop captured dependency\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_p1_native_runner_fails(self):
        (self.root / "verification/p1_native/run.py").write_text("raise SystemExit(0)\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_p0_api_inventory_fails(self):
        (self.root / "verification/contracts/p0_inventory.json").write_text('{"p0_complete": true}')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_weakened_commit_data_relation_fails(self):
        (self.root / "verification/commit_data/contracts.rs").write_text("// omit row/posting agreement\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_completion_adapter_fails(self):
        (self.root / "verification/commit_completion/generate.py").write_text("# omit native deregistration\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_planning_campaigns_fail(self):
        for filename in ("verification/write_plan/generate.py", "verification/write_admission/run.py",
                         "verification/planned_commit/generate.py", "verification/planning_native/run.py"):
            with self.subTest(filename=filename):
                path = self.root / filename
                original = path.read_bytes()
                path.write_text("# omit required planning evidence\n")
                self.assertFalse(coverage.validate(self.root)["passed"])
                path.write_bytes(original)

    def test_descriptive_flag_cannot_claim_full_p1(self):
        path = self.root / "verification/claims.toml"
        path.write_text(path.read_text().replace("full_P1_complete = false", "full_P1_complete = true", 1))
        lock = json.loads((self.root / coverage.LOCK).read_text())
        lock["files"]["verification/claims.toml"] = coverage.digest(path)
        (self.root / coverage.LOCK).write_text(json.dumps(lock))
        report = coverage.validate(self.root)
        self.assertFalse(report["passed"])
        self.assertIn("component pilot cannot assert full P1 completion", report["errors"])

    def test_omitted_refinement_campaign_fails(self):
        (self.root / "verification/refinement_campaigns.json").write_text('{"campaigns": {}}')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_changed_required_roots_fails(self):
        (self.root / "verification/lean/roots.json").write_text('{"roots": []}')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_extra_kernel_module_fails(self):
        (self.root / "aerostore_verified/src/escape.rs").write_text("pub fn bypass() {}\n")
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_new_cargo_configuration_fails(self):
        (self.root / ".cargo").mkdir(exist_ok=True)
        (self.root / ".cargo/config.toml").write_text('[build]\nrustflags = ["--cfg", "skip_proof"]\n')
        self.assertFalse(coverage.validate(self.root)["passed"])

    def test_kernel_edit_still_needs_separate_live_proof(self):
        (self.root / "aerostore_verified/src/lib.rs").write_text("// Candidate can change; proof runner must recheck.\n")
        # This gate intentionally does not pretend to prove the editable kernel.
        self.assertTrue(coverage.validate(self.root)["passed"])

    def test_self_rebaseline_cannot_satisfy_independent_commit(self):
        def git(*arguments):
            return subprocess.check_output(["git", *arguments], cwd=self.root, stderr=subprocess.DEVNULL, text=True)
        git("init", "-q")
        git("add", ".")
        git("-c", "user.name=Gate Test", "-c", "user.email=gate-test@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "-qm", "reviewed fixture baseline")
        baseline = git("rev-parse", "HEAD").strip()
        (self.root / "verification/contracts/bucket_set.md").write_text("Weakened contract\n")
        files = {name: coverage.digest(self.root / name) for name in coverage.frozen_paths(self.root)}
        (self.root / coverage.LOCK).write_text(json.dumps({"format_version": 1, "files": files}))
        self.assertTrue(coverage.validate(self.root)["passed"])
        self.assertFalse(coverage.validate(self.root, baseline)["passed"])


class EvidenceTests(unittest.TestCase):
    def test_new_commit_campaigns_reject_bare_success(self):
        for name in ("commit-data", "commit-completion", "write-plan", "write-admission", "planned-commit"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / name).mkdir()
                (root / name / "receipt.json").write_text('{"passed":true,"status":"passed","source_stable":true}')
                with self.assertRaisesRegex(RuntimeError, "unsupported proof scope"):
                    verify_formal.collect_claim_evidence([], [{"name": name, "passed": True}], root)

    def test_p0_success_without_current_audit_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "p0-contracts.log").write_text('{"passed": true, "p0_complete": true}')
            with patch.object(verify_formal.check_p0_contracts, "validate", return_value={"passed": True, "p0_complete": False}), self.assertRaisesRegex(RuntimeError, "current-source evidence"):
                verify_formal.collect_claim_evidence([], [{"name": "p0-contracts", "passed": True}], root)

    def test_concurrent_success_without_negative_controls_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "concurrent").mkdir()
            (root / "concurrent/receipt.json").write_text(json.dumps({
                "passed": True, "status": "passed", "native_primitive_refinement_proved": False,
                "transaction_history_refinement_proved": False, "checks": [],
            }))
            with self.assertRaisesRegex(RuntimeError, "complete conditional proof evidence"):
                verify_formal.collect_claim_evidence([], [{"name": "concurrent", "passed": True}], root)

    def test_ambient_compiler_flags_fail(self):
        with patch.dict("os.environ", {"RUSTFLAGS": "--cfg ignore_contract"}):
            with self.assertRaisesRegex(RuntimeError, "unreviewed build environment"):
                verify_formal.check_build_environment()

    def test_missing_root_cannot_satisfy_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lean.json").write_text(json.dumps({"passed": True, "completed": True,
                "kernel_recheck_passed": True, "forged_theorem_rejected": True,
                "mutation_checks": [{"name": name, "rejected": True} for name in
                    ["stamp_accepts_equal", "bitmap_drops_membership", "bitmap_accepts_equal_bound",
                     "sort_writes_wrong_bucket", "sort_accepts_equal_bound",
                     "predicate_ignores_changed_stamp", "predicate_accepts_equal_start",
                     "predicate_drops_publication", "predicate_omits_own_candidates",
                     "lifecycle_reservation_does_not_advance", "lifecycle_uses_writer_start_stamp",
                     "lifecycle_publishes_before_end", "lifecycle_allows_wrapping_reservation",
                     "query_omits_old_bucket", "query_omits_destination_posting", "query_ignores_creator_active",
                     "query_omits_own_candidates", "query_allows_stamp_regression", "query_ignores_deleter_active",
                     "query_filters_before_own_overlay"]], "required_roots": []}))
            with self.assertRaisesRegex(RuntimeError, "missing declared proof roots"):
                verify_formal.collect_claim_evidence([{"id": "test", "scope": "test", "status": "partial",
                    "required_checks": ["lean"], "lean_roots": ["must_exist"]}],
                    [{"name": "lean", "passed": True}], root)


class RefinementEvidenceTests(unittest.TestCase):
    """Synthetic artifacts exercise receipt validation, never assert real proofs."""
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aerostore-refinement-receipt-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output = self.root / "target/predicate-capture"
        self.output.mkdir(parents=True)
        self.path = self.output / "receipt.json"
        manifest = json.loads((coverage.ROOT / refinement.MANIFEST).read_text())
        campaign = manifest["campaigns"]["predicate-capture"]
        for filename in campaign["inputs"]:
            path = self.root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("synthetic input: " + filename)
        (self.root / refinement.MANIFEST).write_text(json.dumps(manifest))
        distribution = "target/tools"
        tool = self.root / distribution / "verus"
        tool.parent.mkdir(parents=True)
        tool.write_text("synthetic verifier")
        pin = {"distribution": distribution, "platform": "synthetic-platform",
               "artifact_sha256": {"verus": refinement.digest(tool)}}
        (self.root / "verification/verus/toolchain.json").write_text(json.dumps(pin))
        inputs = {filename: refinement.digest(self.root / filename) for filename in campaign["inputs"]}
        self.receipt = {"passed": True, "status": "passed", "source_stable": True,
            "scope": campaign["scope"], "required_roots": campaign["roots"],
            "required_mutations": list(campaign["mutations"]), "input_sha256": inputs,
            "final_input_sha256": dict(inputs), "toolchain": pin, "checks": []}
        self.receipt.update({flag: False for flag in campaign["false_flags"]})
        names = [campaign["full_check"], *["root_" + r for r in campaign["roots"]], *campaign["mutations"]]
        for name in names:
            negative = name in campaign["mutations"]
            required = campaign["mutations"].get(name)
            if name.startswith("root_"):
                required = name[5:]
            artifact = self.output / (name + ".rs") if negative else self.root / campaign["generated"]
            if negative:
                artifact.write_text("synthetic negative input " + name)
            command = [str(tool), "--crate-name", campaign["crate_name"], "--crate-type=lib", "--edition=2021",
                "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent",
                "--rlimit", str(campaign["rlimit"])]
            if required:
                command += ["--verify-root", "--verify-function", required]
            command += [str(artifact)]
            count = 0 if negative else 1 if required else len(campaign["roots"])
            errors = int(negative)
            contents = ("error: invariant not satisfied\n" if negative else "") + f"verification results:: {count} verified, {errors} errors\n"
            log = self.output / (name + ".log")
            log.write_text(contents)
            self.receipt["checks"].append({"name": name, "expected_failure": negative,
                "required_root": required, "command": command, "exit_code": int(negative),
                "source_sha256": refinement.digest(artifact), "log": str(log.relative_to(self.root)),
                "log_sha256": refinement.digest(log), "verified": count, "errors": errors})

    def validate(self):
        self.path.write_text(json.dumps(self.receipt))
        return refinement.validate_receipt(self.path, "predicate-capture", self.root)

    def test_complete_fixture(self):
        self.assertTrue(self.validate()["passed"])

    def use_nested_module_fixture(self):
        manifest_path = self.root / refinement.MANIFEST
        manifest = json.loads(manifest_path.read_text())
        campaign = manifest["campaigns"]["predicate-capture"]
        campaign["verify_module"] = "admission"
        campaign["roots"] = ["admission::" + root for root in campaign["roots"]]
        campaign["mutations"] = {name: "admission::" + root if root else None
                                 for name, root in campaign["mutations"].items()}
        manifest_path.write_text(json.dumps(manifest))
        self.receipt["required_roots"] = campaign["roots"]
        for check in self.receipt["checks"]:
            if check["required_root"]:
                basename = check["required_root"]
                check["required_root"] = "admission::" + basename
                if check["name"].startswith("root_"):
                    check["name"] = "root_admission_" + basename
                command = check["command"]
                start = command.index("--verify-root")
                command[start:start + 3] = ["--verify-only-module", "admission", "--verify-function", basename]

    def test_nested_module_selector_validates(self):
        self.use_nested_module_fixture()
        self.assertTrue(self.validate()["passed"])

    def test_nested_module_cannot_use_root_selector(self):
        self.use_nested_module_fixture()
        command = self.receipt["checks"][1]["command"]
        start = command.index("--verify-only-module")
        command[start:start + 2] = ["--verify-root"]
        with self.assertRaisesRegex(RuntimeError, "wrong verified root"):
            self.validate()

    def test_nested_module_cannot_select_different_module(self):
        self.use_nested_module_fixture()
        command = self.receipt["checks"][1]["command"]
        command[command.index("--verify-only-module") + 1] = "unrelated"
        with self.assertRaisesRegex(RuntimeError, "wrong verified root"):
            self.validate()

    def test_nested_module_function_must_be_unqualified(self):
        self.use_nested_module_fixture()
        check = self.receipt["checks"][1]
        command = check["command"]
        command[command.index("--verify-function") + 1] = check["required_root"]
        with self.assertRaisesRegex(RuntimeError, "wrong verified root"):
            self.validate()

    def test_nested_module_declared_root_cannot_escape(self):
        self.use_nested_module_fixture()
        manifest_path = self.root / refinement.MANIFEST
        manifest = json.loads(manifest_path.read_text())
        manifest["campaigns"]["predicate-capture"]["verify_module"] = "other"
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError, "root outside declared verification module"):
            self.validate()

    def test_empty_success_receipt(self):
        self.receipt = {"passed": True, "status": "passed"}
        with self.assertRaisesRegex(RuntimeError, "incomplete campaign"):
            self.validate()

    def test_missing_mutation(self):
        self.receipt["checks"].pop()
        with self.assertRaisesRegex(RuntimeError, "missing, duplicate"):
            self.validate()

    def test_duplicate_root(self):
        self.receipt["checks"][2] = self.receipt["checks"][1]
        with self.assertRaisesRegex(RuntimeError, "missing, duplicate"):
            self.validate()

    def test_stale_native_source(self):
        (self.root / "aerostore_core/src/occ_partitioned.rs").write_text("changed native source")
        with self.assertRaisesRegex(RuntimeError, "stale proof source"):
            self.validate()

    def test_stale_tool(self):
        (self.root / "target/tools/verus").write_text("different verifier")
        with self.assertRaisesRegex(RuntimeError, "substituted verifier"):
            self.validate()

    def test_source_changed_during_run(self):
        self.receipt["final_input_sha256"] = {}
        with self.assertRaisesRegex(RuntimeError, "source changed during"):
            self.validate()

    def test_parser_failure_is_not_counterexample(self):
        check = self.receipt["checks"][-1]
        log = self.root / check["log"]
        log.write_text("error: unexpected token\n")
        check["log_sha256"] = refinement.digest(log)
        with self.assertRaisesRegex(RuntimeError, "missing or ambiguous verifier result"):
            self.validate()

    def test_wrong_verified_root(self):
        self.receipt["checks"][1]["command"][-2] = "unrelated_function"
        with self.assertRaisesRegex(RuntimeError, "wrong verified root"):
            self.validate()

    def test_broader_claim(self):
        self.receipt["transaction_history_refinement_proved"] = True
        with self.assertRaisesRegex(RuntimeError, "unsupported proof scope"):
            self.validate()

    def test_extra_verifier_filter(self):
        command = self.receipt["checks"][1]["command"]
        command[-1:-1] = ["--verify-function", "unrelated_function"]
        with self.assertRaisesRegex(RuntimeError, "unsupported verifier command"):
            self.validate()

    def test_invalid_negative_exit_code(self):
        for code in [None, "failed", True]:
            self.receipt["checks"][-1]["exit_code"] = code
            with self.subTest(code=code), self.assertRaisesRegex(RuntimeError, "invalid verifier exit code"):
                self.validate()

    def test_edited_log(self):
        (self.root / self.receipt["checks"][0]["log"]).write_text("new output")
        with self.assertRaisesRegex(RuntimeError, "stale proof log"):
            self.validate()

    def test_no_proofs(self):
        check = self.receipt["checks"][1]
        log = self.root / check["log"]
        log.write_text("verification results:: 0 verified, 0 errors\n")
        check.update(verified=0, log_sha256=refinement.digest(log))
        with self.assertRaisesRegex(RuntimeError, "required proof did not pass"):
            self.validate()

    def add_type_control(self):
        manifest_path = self.root / refinement.MANIFEST
        manifest = json.loads(manifest_path.read_text())
        manifest["campaigns"]["predicate-capture"]["type_mutations"] = {"double_drop": "E0382"}
        manifest_path.write_text(json.dumps(manifest))
        self.receipt["required_type_mutations"] = {"double_drop": "E0382"}
        artifact = self.output / "double_drop.rs"
        artifact.write_text("synthetic affine misuse")
        log = self.output / "double_drop.log"
        log.write_text("error[E0382]: use of moved value\n")
        command = self.receipt["checks"][0]["command"][:-1] + [str(artifact)]
        self.receipt["type_checks"] = [{"name": "double_drop", "classification": "ownership_type_rejection",
            "expected_diagnostic": "E0382", "exit_code": 1, "command": command,
            "source_sha256": refinement.digest(artifact), "log": str(log.relative_to(self.root)),
            "log_sha256": refinement.digest(log)}]
        return self.receipt["type_checks"][0]

    def test_affine_control_is_separate_from_semantic_mutants(self):
        self.add_type_control()
        self.assertTrue(self.validate()["passed"])

    def test_missing_affine_control_fails(self):
        self.add_type_control()
        self.receipt["type_checks"] = []
        with self.assertRaisesRegex(RuntimeError, "missing, duplicate, or reordered ownership"):
            self.validate()

    def test_unrelated_affine_compiler_error_fails(self):
        check = self.add_type_control()
        log = self.root / check["log"]
        log.write_text("error[E0308]: mismatched types\n")
        check["log_sha256"] = refinement.digest(log)
        with self.assertRaisesRegex(RuntimeError, "ownership misuse failed for wrong reason"):
            self.validate()

    def test_affine_control_cannot_disable_verification(self):
        check = self.add_type_control()
        check["command"].insert(-1, "--no-verify")
        with self.assertRaisesRegex(RuntimeError, "unsupported ownership type command"):
            self.validate()

    def test_opaque_constructor_control_uses_exact_frontend_diagnostic(self):
        check = self.add_type_control()
        diagnostic = "disallowed: constructor for an opaque datatype"
        manifest_path = self.root / refinement.MANIFEST
        manifest = json.loads(manifest_path.read_text())
        manifest["campaigns"]["predicate-capture"]["type_mutations"]["double_drop"] = diagnostic
        manifest_path.write_text(json.dumps(manifest))
        self.receipt["required_type_mutations"]["double_drop"] = diagnostic
        check["expected_diagnostic"] = diagnostic
        log = self.root / check["log"]
        log.write_text("error: " + diagnostic + "\n")
        check["log_sha256"] = refinement.digest(log)
        self.assertTrue(self.validate()["passed"])
        log.write_text("note: " + diagnostic + "\nerror: unexpected token\n")
        check["log_sha256"] = refinement.digest(log)
        with self.assertRaisesRegex(RuntimeError, "ownership misuse failed for wrong reason"):
            self.validate()


class LockModelEvidenceTests(unittest.TestCase):
    """Small synthetic artifacts test the receipt checker, never claim a model run."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="aerostore-lock-receipt-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.output = self.root / "target/lock-models"
        self.output.mkdir(parents=True)
        self.path = self.output / "receipt.json"
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["aerostore_core"]\n')
        (self.root / "Cargo.lock").write_text("# fixture\n")
        for name in [lock_models.LOCK, lock_models.MODELS,
                     "scripts/check_lock_models.py", "scripts/verify_formal.py"]:
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(coverage.ROOT / name, destination)
        inputs = lock_models.fingerprints(self.root)
        original = (self.root / lock_models.LOCK).read_text()
        mutated = original.replace(lock_models.MUTANT_OLD, lock_models.MUTANT_NEW)
        patch_path = self.output / "relaxed-success-cas.patch"
        patch_path.write_text("synthetic patch fixture\n")
        bad_source = self.output / "mutant.rs"
        bad_source.write_text(mutated)
        self.receipt = {
            "schema": 1, "status": "passed", "passed": True, "source_stable": True, "complete_campaign": True,
            "scope": "bounded_production_lock_models", "native_weak_memory_refinement_proved": False,
            "unbounded_progress_proved": False, "bounds": lock_models.BOUNDS,
            "required_cases": list(lock_models.CASES), "required_mutant": lock_models.MUTANT_NAME,
            "source_root": str(self.root), "environment_root": str(self.root),
            "input_sha256": inputs, "final_input_sha256": inputs,
            "candidate_shm_lock_sha256": inputs[lock_models.LOCK],
            "runner_sha256": lock_models.digest(self.root / "scripts/check_lock_models.py"),
            "environment_checker_sha256": lock_models.digest(self.root / "scripts/verify_formal.py"),
            "model_environment": {"RUSTFLAGS": "--cfg aerostore_loom"},
            "tool_sha256": {}, "builds": [], "listings": [], "checks": [],
            "mutant": {"name": lock_models.MUTANT_NAME, "case": lock_models.MUTANT_CASE,
                       "source_sha256": lock_models.digest(bad_source), "patch_sha256": lock_models.digest(patch_path)},
        }
        for name in ("rustc", "cargo"):
            tool = self.output / name
            tool.write_text("synthetic " + name)
            self.receipt["tool_sha256"][str(tool)] = lock_models.digest(tool)
        for variant in (0, 1):
            directory = self.output / str(variant)
            directory.mkdir()
            executable = directory / "model"
            executable.write_text("synthetic binary " + str(variant))
            artifact = {"reason": "compiler-artifact", "fresh": False, "executable": str(executable),
                        "target": {"name": "shm_mutation_model", "src_path": str(directory / lock_models.MODELS)}}
            build = self.log_result(directory / "build.log", json.dumps(artifact), 0)
            build.update(command=["cargo", "test", "--offline", "--locked", "--release", "--no-run",
                                  "--message-format=json", "--test", "shm_mutation_model"], cwd=str(directory),
                         executable=str(executable), executable_sha256=lock_models.digest(executable),
                         fresh_test_binary=True, model_sha256=inputs[lock_models.MODELS],
                         lock_sha256=inputs[lock_models.LOCK] if variant == 0 else lock_models.digest(bad_source))
            self.receipt["builds"].append(build)
            listing = self.log_result(directory / "list.log", "\n".join(case + ": test" for case in lock_models.CASES), 0)
            listing["command"] = [str(executable), "--list"]
            self.receipt["listings"].append(listing)
        for index, name in enumerate([*lock_models.CASES, lock_models.MUTANT_NAME]):
            mutant = index == len(lock_models.CASES)
            case = lock_models.MUTANT_CASE if mutant else name
            build = self.receipt["builds"][int(mutant)]
            contents = "running 1 test\ntest " + case + " ... "
            if mutant:
                contents += "FAILED\nCausality violation: Concurrent write accesses to `UnsafeCell`.\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;\n"
            else:
                contents += "ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n" + lock_models.EXPECTED_COUNTEREXAMPLES.get(case, "")
            check = self.log_result(self.output / (str(index) + ".log"), contents, 101 if mutant else 0)
            check.update(name=name, passed=True, executable_sha256=build["executable_sha256"],
                         command=[build["executable"], "--exact", case, "--nocapture", "--test-threads=1"])
            if mutant:
                check["expected_failure"] = True
            else:
                check["expected_counterexample"] = case in lock_models.EXPECTED_COUNTEREXAMPLES
            self.receipt["checks"].append(check)

    def log_result(self, path, contents, code):
        path.write_text(contents)
        return {"log": str(path), "log_sha256": lock_models.digest(path), "exit_code": code}

    def validate(self):
        self.path.write_text(json.dumps(self.receipt))
        with patch.object(verify_formal, "ROOT", self.root):
            return verify_formal.collect_claim_evidence([], [{"name": "lock-models", "passed": True}], self.output.parent)

    def test_complete_receipt_validates(self):
        self.assertEqual(self.validate(), [])

    def test_missing_receipt_fails(self):
        with patch.object(verify_formal, "ROOT", self.root), self.assertRaisesRegex(RuntimeError, "lock-model evidence"):
            verify_formal.collect_claim_evidence([], [{"name": "lock-models", "passed": True}], self.output.parent)

    def test_success_boolean_without_artifacts_fails(self):
        self.receipt = {"passed": True, "status": "passed"}
        with self.assertRaisesRegex(RuntimeError, "lock-model evidence"):
            self.validate()

    def test_missing_or_duplicate_case_fails(self):
        self.receipt["checks"][1] = self.receipt["checks"][0]
        with self.assertRaisesRegex(RuntimeError, "missing or duplicate"):
            self.validate()

    def test_missing_negative_control_fails(self):
        self.receipt["checks"].pop()
        with self.assertRaisesRegex(RuntimeError, "missing or duplicate"):
            self.validate()

    def test_arbitrary_mutant_failure_fails(self):
        check = self.receipt["checks"][-1]
        check.update(self.log_result(Path(check["log"]), "running 1 test\ntest " + lock_models.MUTANT_CASE
                     + " ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;\nunrelated panic\n", 101))
        with self.assertRaisesRegex(RuntimeError, "intended Loom causality"):
            self.validate()

    def test_zero_tests_fails(self):
        check = self.receipt["checks"][0]
        check.update(self.log_result(Path(check["log"]), "running 0 tests\n0 passed; 0 failed; 0 ignored\n", 0))
        with self.assertRaisesRegex(RuntimeError, "run exactly once"):
            self.validate()

    def test_compile_failure_fails(self):
        self.receipt["builds"][1]["exit_code"] = 101
        with self.assertRaisesRegex(RuntimeError, "model executable"):
            self.validate()

    def test_cached_artifact_fails(self):
        build = self.receipt["builds"][0]
        log = Path(build["log"])
        artifact = json.loads(log.read_text())
        artifact["fresh"] = True
        build.update(self.log_result(log, json.dumps(artifact), 0))
        with self.assertRaisesRegex(RuntimeError, "fresh compiler artifact"):
            self.validate()

    def test_changed_binary_fails(self):
        Path(self.receipt["builds"][0]["executable"]).write_text("substituted binary")
        with self.assertRaisesRegex(RuntimeError, "substituted model executable"):
            self.validate()

    def test_changed_source_fails(self):
        with (self.root / lock_models.LOCK).open("a") as output:
            output.write("\n// changed source\n")
        with self.assertRaisesRegex(RuntimeError, "stale lock-model source"):
            self.validate()

    def test_broader_claim_fails(self):
        self.receipt["unbounded_progress_proved"] = True
        with self.assertRaisesRegex(RuntimeError, "unsupported lock-model scope"):
            self.validate()


class P1NativeEvidenceTests(unittest.TestCase):
    """Synthetic fixtures test gate rejection, not real native correctness."""
    campaign_name = "p1-native"
    campaign_scope = "native_p1_complete_transaction_scenarios"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aerostore-p1-receipt-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output = self.root / "target" / self.campaign_name
        self.output.mkdir(parents=True)
        self.path = self.output / "receipt.json"
        campaign_dir = self.root / "verification" / self.campaign_name.replace("-", "_")
        campaign_dir.mkdir(parents=True)
        shutil.copyfile(coverage.ROOT / "verification" / self.campaign_name.replace("-", "_") / "run.py", campaign_dir / "run.py")
        for name in ("test_run.py", "README.md"):
            (campaign_dir / name).write_text("synthetic fixture " + name)
        for filename in ["Cargo.toml", "Cargo.lock", *[crate + "/Cargo.toml" for crate in
                         ("aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl")]]:
            path = self.root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("synthetic native input")
        occ = self.root / "aerostore_core/src/occ_partitioned.rs"
        occ.parent.mkdir(parents=True)
        shutil.copyfile(coverage.ROOT / "aerostore_core/src/occ_partitioned.rs", occ)
        spec = importlib.util.spec_from_file_location("p1_native_fixture", campaign_dir / "run.py")
        self.campaign = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.campaign)
        native = {str(p.relative_to(self.root)): p.read_bytes() for p in self.campaign.native_paths()}
        inputs = {name: p1_native.digest(self.root / name) for name in native}
        inputs.update({str(p.relative_to(self.root)): p1_native.digest(p) for p in campaign_dir.iterdir() if p.is_file()})
        cargo = self.output / "tools/cargo"
        cargo.parent.mkdir()
        cargo.write_text("synthetic cargo")
        cargo.with_name("rustc").write_text("synthetic rustc")
        controls = list(self.campaign.variants(native[self.campaign.OCC].decode()))
        self.receipt = {"passed": True, "status": "passed", "source_stable": True,
            "scope": self.campaign_scope, "formal_refinement_proved": False,
            "full_P1_complete": False, "input_sha256": inputs, "final_input_sha256": dict(inputs),
            "required_positive_checks": [name for name, _ in self.campaign.TESTS],
            "required_mutations": [control[0] for control in controls],
            "rustc": "commit-hash: " + self.campaign.PINNED_RUST + "\nrelease: 1.93.1\n",
            "tool_sha256": {str(p): p1_native.digest(p) for p in (cargo, cargo.with_name("rustc"))}, "checks": []}
        expected = [(name, selection, None, None) for name, selection in self.campaign.TESTS]
        expected += [(name, selection, (filename, changed), assertion)
                     for name, filename, changed, selection, assertion in controls]
        for name, selection, changed, assertion in expected:
            negative = changed is not None
            source = self.output / (name if negative else "current") / "source"
            contents = dict(native)
            if changed:
                contents[changed[0]] = changed[1].encode()
            for filename, content in contents.items():
                path = source / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            executable = source.parent / "cargo-target/test-binary"
            executable.parent.mkdir(exist_ok=True)
            executable.write_text("synthetic native test binary")
            log = self.output / (name + ".log")
            log.write_text("Running tests (" + str(executable) + ")\ntest " + selection[-1] + " ... "
                           + ("FAILED\npanicked at selected.rs: " + assertion + "\ntest result: FAILED. 0 passed; 1 failed"
                              if negative else "ok\ntest result: ok. 1 passed; 0 failed"))
            self.receipt["checks"].append({"name": name, "expected_assertion_failure": negative,
                "required_assertion": assertion, "passed": True, "exit_code": 101 if negative else 0,
                "cwd": str(source), "command": [str(cargo), "test", "--offline", "--locked", "--target-dir",
                    str(executable.parent), "-p", "aerostore_core", *selection, "--", "--test-threads=1", "--nocapture"],
                "source_sha256": {filename: p1_native.digest(source / filename) for filename in native},
                "binary_sha256": {str(executable): p1_native.digest(executable)},
                "log": str(log.relative_to(self.root)), "log_sha256": p1_native.digest(log)})

    def validate(self):
        self.path.write_text(json.dumps(self.receipt))
        with patch.object(verify_formal, "ROOT", self.root):
            return verify_formal.collect_claim_evidence([], [{"name": self.campaign_name, "passed": True}], self.output.parent)

    def test_complete_native_receipt_validates(self):
        self.assertEqual(self.validate(), [])

    def test_forged_native_success_fails(self):
        self.receipt = {"passed": True, "status": "passed"}
        with self.assertRaisesRegex(RuntimeError, "incomplete campaign"):
            self.validate()

    def test_missing_native_mutation_fails(self):
        self.receipt["checks"].pop()
        with self.assertRaisesRegex(RuntimeError, "missing, duplicate"):
            self.validate()

    def test_stale_native_input_fails(self):
        (self.root / "Cargo.lock").write_text("substituted dependency")
        with self.assertRaisesRegex(RuntimeError, "stale source"):
            self.validate()

    def test_changed_native_fixture_fails(self):
        check = self.receipt["checks"][-1]
        (Path(check["cwd"]) / self.campaign.OCC).write_text("substituted mutation")
        with self.assertRaisesRegex(RuntimeError, "substituted native fixture"):
            self.validate()

    def test_unrelated_native_failure_fails(self):
        check = self.receipt["checks"][-1]
        log = self.root / check["log"]
        log.write_text(log.read_text().replace(check["required_assertion"], "unrelated panic"))
        check["log_sha256"] = p1_native.digest(log)
        with self.assertRaisesRegex(RuntimeError, "wrong-kind native failure"):
            self.validate()

    def test_substituted_native_binary_fails(self):
        binary = next(iter(self.receipt["checks"][0]["binary_sha256"]))
        Path(binary).write_text("substituted binary")
        with self.assertRaisesRegex(RuntimeError, "substituted native executable"):
            self.validate()

    def test_native_tests_cannot_claim_full_p1(self):
        self.receipt["full_P1_complete"] = True
        with self.assertRaisesRegex(RuntimeError, "unsupported scope"):
            self.validate()


class PlanningNativeEvidenceTests(P1NativeEvidenceTests):
    campaign_name = "planning-native"
    campaign_scope = "native_final_write_planning_scenarios"


class RunnerTests(unittest.TestCase):
    def run_fixture(self, profile, coverage_pass=True, baseline=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "verification").mkdir()
            (root / "verification/claims.toml").write_text("claims = []\n")
            output = root / "target/report.json"
            arguments = ["verify_formal.py", "--profile", profile, "--output", str(output)]
            if baseline:
                arguments += ["--baseline-ref", "a" * 40]
            calls = []

            def check(name, command, directory, timeout):
                calls.append((name, command))
                if name == "p0-contracts":
                    (directory / "p0-contracts.log").write_text('{"passed": true, "p0_complete": false}')
                return {"name": name, "passed": coverage_pass if name == "coverage" else True}

            with patch.object(verify_formal, "ROOT", root), \
                 patch.object(verify_formal, "check_build_environment", return_value={}), \
                 patch.object(verify_formal, "source_fingerprint", return_value={}), \
                 patch.object(verify_formal, "collect_claim_evidence", return_value=[]), \
                 patch.object(verify_formal, "run_check", side_effect=check), \
                 patch.object(verify_formal.subprocess, "check_output", side_effect=lambda *a, **kw: "head" if kw.get("text") else b"# trusted checker\n"), \
                 patch.object(sys, "argv", arguments):
                code = verify_formal.main()
            return code, json.loads(output.read_text()), calls

    def test_models_with_baseline_actually_checks_baseline(self):
        code, _, calls = self.run_fixture("models", baseline=True)
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][0], "coverage")
        self.assertIn("--baseline-ref", calls[0][1])

    def test_failed_boundary_prevents_candidate_runners(self):
        code, report, calls = self.run_fixture("pilot", coverage_pass=False, baseline=True)
        self.assertEqual(code, 1)
        self.assertFalse(report["passed"])
        self.assertEqual([name for name, _ in calls], ["coverage"])

    def test_full_cannot_pass_by_removing_open_statuses(self):
        code, report, _ = self.run_fixture("full")
        self.assertEqual(code, 1)
        self.assertFalse(report["passed"])
        self.assertFalse(report["whole_engine_verified"])

    def test_pilot_requires_lock_models(self):
        code, _, calls = self.run_fixture("pilot")
        self.assertEqual(code, 0)
        commands = dict(calls)
        self.assertIn("lock-models", commands)
        self.assertIn("scripts/check_lock_models.py", commands["lock-models"])

    def test_pilot_requires_p1_native_and_p0_audit(self):
        code, report, calls = self.run_fixture("pilot")
        self.assertEqual(code, 0)
        commands = dict(calls)
        for name in ("p1-native", "p1-native-runner-tests", "planning-native", "planning-native-runner-tests", "p0-contracts", "p0-contract-tests"):
            self.assertIn(name, commands)
        self.assertFalse(report["p0_complete"])
        self.assertFalse(report["full_P1_complete"])

    def test_proof_profiles_require_all_native_refinement_campaigns(self):
        for profile in ("proofs", "pilot", "full"):
            with self.subTest(profile=profile):
                code, report, calls = self.run_fixture(profile)
                self.assertEqual(code, 1 if profile == "full" else 0)
                self.assertFalse(report["full_P1_complete"])
                commands = dict(calls)
                for name in ["predicate", "predicate-capture", "predicate-composition", "skiplist-detach", "postings",
                             "guards", "lifecycle", "publication-slice", "lifecycle-scenario",
                             "lifecycle-interference", "guard-ownership", "lookup", "indexed-slice", "row-publication", "row-retention", "storage-slice", "commit-data", "commit-completion", "write-plan", "write-admission", "planned-commit"]:
                    self.assertIn(name, commands)
                    self.assertIn(name + "-adapter-tests", commands)
                for name in ("planning-native", "planning-native-runner-tests"):
                    if profile == "proofs":
                        self.assertNotIn(name, commands)
                    else:
                        self.assertIn(name, commands)


if __name__ == "__main__":
    unittest.main()
