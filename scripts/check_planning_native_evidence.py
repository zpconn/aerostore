#!/usr/bin/env python3
"""Check current-source native planning scenario evidence, including intended failures."""
from pathlib import Path
import hashlib
import importlib.util
import json
import re

ROOT = Path(__file__).resolve().parents[1]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_receipt(path: Path, root: Path = ROOT) -> dict:
    root, path = root.resolve(), path.resolve()

    def require(condition, message):
        if not condition:
            raise RuntimeError("planning-native evidence: " + message)

    try:
        runner = root / "verification/planning_native/run.py"
        spec = importlib.util.spec_from_file_location("planning_native_evidence_runner", runner)
        campaign = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(campaign)
        receipt = json.loads(path.read_text())
        require(path.is_relative_to(root / "target"), "receipt outside build evidence directory")
        require(receipt.get("passed") is True and receipt.get("status") == "passed"
                and receipt.get("source_stable") is True, "incomplete campaign")
        require(receipt.get("scope") == "native_final_write_planning_scenarios"
                and receipt.get("formal_refinement_proved") is False
                and receipt.get("full_P1_complete") is False, "unsupported scope")
        native = {str(p.relative_to(root)): p.read_bytes() for p in campaign.native_paths()}
        required_inputs = set(native) | {"verification/planning_native/" + name
                                        for name in ("run.py", "test_run.py", "README.md")}
        inputs = receipt.get("input_sha256", {})
        require(set(inputs) == required_inputs, "missing or extra source inputs")
        require(receipt.get("final_input_sha256") == inputs, "source changed during campaign")
        for filename, sha in inputs.items():
            require(digest(root / filename) == sha, "stale source: " + filename)
        controls = list(campaign.variants(native[campaign.OCC].decode()))
        positives = [name for name, _ in campaign.TESTS]
        negatives = [control[0] for control in controls]
        require(receipt.get("required_positive_checks") == positives, "changed required native cases")
        require(receipt.get("required_mutations") == negatives, "changed required native mutations")
        checks = receipt.get("checks", [])
        require([check.get("name") for check in checks] == positives + negatives,
                "missing, duplicate or reordered checks")
        tools = receipt.get("tool_sha256", {})
        require(len(tools) == 2, "missing native compiler tools")
        cargo = next((Path(name) for name in tools if Path(name).name == "cargo"), None)
        require(cargo is not None and str(cargo.with_name("rustc")) in tools,
                "incorrect native compiler tool pair")
        for filename, sha in tools.items():
            require(digest(Path(filename)) == sha, "substituted native compiler tool")
        require("commit-hash: " + campaign.PINNED_RUST in receipt.get("rustc", "")
                and "release: 1.93.1\n" in receipt.get("rustc", ""), "unreviewed native compiler")
        expected = [(name, selection, None, None) for name, selection in campaign.TESTS]
        expected += [(name, selection, (filename, changed), assertion)
                     for name, filename, changed, selection, assertion in controls]
        for check, (name, selection, changed, assertion) in zip(checks, expected):
            negative = changed is not None
            source = path.parent / (name if negative else "current") / "source"
            target = source.parent / "cargo-target"
            command = [str(cargo), "test", "--offline", "--locked", "--target-dir", str(target),
                       "-p", "aerostore_core", *selection, "--", "--test-threads=1", "--nocapture"]
            require(check.get("command") == command and check.get("cwd") == str(source),
                    "wrong native command or fixture: " + name)
            require(check.get("expected_assertion_failure") is negative
                    and check.get("required_assertion") == assertion and check.get("passed") is True,
                    "wrong expected outcome: " + name)
            require(type(check.get("exit_code")) is int, "invalid native exit code: " + name)
            expected_sources = {filename: hashlib.sha256(content).hexdigest()
                                for filename, content in native.items()}
            if changed:
                expected_sources[changed[0]] = hashlib.sha256(changed[1].encode()).hexdigest()
            require(check.get("source_sha256") == expected_sources, "incorrect mutation/source map: " + name)
            for filename, sha in expected_sources.items():
                require(digest(source / filename) == sha, "substituted native fixture: " + name)
            log_path = path.parent / (name + ".log")
            require((root / check["log"]).resolve() == log_path, "unexpected native log path")
            require(digest(log_path) == check["log_sha256"], "substituted native log")
            log = log_path.read_text()
            require(campaign.outcome_passes(check.get("exit_code"), log, negative, assertion),
                    "missing test or wrong-kind native failure: " + name)
            require(re.search(r"test (?:[A-Za-z0-9_]+::)*" + re.escape(selection[-1]) + r" \.\.\.", log),
                    "selected native test absent: " + name)
            logged = []
            for name_in_log in re.findall(r"Running .*? \(([^)]+)\)", log):
                executable = Path(name_in_log)
                logged.append(executable if executable.is_absolute() else source / executable)
            binaries = check.get("binary_sha256", {})
            require(len(logged) == 1 and set(binaries) == {str(logged[0])},
                    "missing or substituted native executable record")
            for filename, sha in binaries.items():
                require(Path(filename).resolve().is_relative_to(target.resolve())
                        and digest(Path(filename)) == sha, "substituted native executable")
        return receipt
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        raise RuntimeError("planning-native evidence: " + str(error)) from error
