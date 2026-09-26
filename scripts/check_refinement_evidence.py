#!/usr/bin/env python3
"""Validate native refinement receipts against current sources and real logs.

This authenticates the declared campaign, not a proof of the trusted adapter or
the verifier. Boundary review and live execution remain separate requirements.
"""
from pathlib import Path
import hashlib
import json
import re

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "verification/refinement_campaigns.json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_receipt(path: Path, name: str, root: Path = ROOT) -> dict:
    path, root = path.resolve(), root.resolve()
    def require(condition: bool, message: str):
        if not condition:
            raise RuntimeError(f"{name} refinement evidence: {message}")

    try:
        campaign = json.loads((root / MANIFEST).read_text())["campaigns"][name]
        receipt = json.loads(path.read_text())
        require(receipt.get("passed") is True and receipt.get("status") == "passed"
                and receipt.get("source_stable") is True, "incomplete campaign")
        require(receipt.get("scope") == campaign["scope"], "unsupported proof scope")
        for flag in campaign["false_flags"]:
            require(receipt.get(flag) is False, "unsupported proof scope: " + flag)
        require(receipt.get("required_roots") == campaign["roots"], "missing or changed required roots")
        require(receipt.get("required_mutations") == list(campaign["mutations"]), "missing or changed required mutations")
        inputs = receipt.get("input_sha256", {})
        require(set(inputs) == set(campaign["inputs"]), "missing or extra proof inputs")
        require(receipt.get("final_input_sha256") == inputs, "source changed during proof")
        for filename, sha in inputs.items():
            require(digest(root / filename) == sha, "stale proof source: " + filename)
        # The already-bound shared generator pins the only permitted source
        # normalizer. Recheck that dependency here as well as at generator
        # import, so an old receipt cannot mask a changed current dependency.
        generator = "verification/concurrent/generate.py"
        if generator in inputs:
            pins = re.findall(r'^_DIAGNOSTIC_NORMALIZER_SHA256 = "([0-9a-f]{64})"$',
                              (root / generator).read_text(), re.MULTILINE)
            require(len(pins) == 1, "missing or ambiguous source-normalization dependency pin")
            dependency = root / "verification/retry_diagnostics/normalize.py"
            require(dependency.is_file(), "missing source-normalization dependency")
            require(digest(dependency) == pins[0], "stale source-normalization dependency")
        pin = json.loads((root / "verification/verus/toolchain.json").read_text())
        require(receipt.get("toolchain") == pin, "unreviewed verifier pin")
        for filename, sha in pin["artifact_sha256"].items():
            require(digest(root / pin["distribution"] / filename) == sha, "substituted verifier artifact")
        checks = receipt.get("checks", [])
        expected = [campaign["full_check"], *["root_" + r.replace("::", "_") for r in campaign["roots"]],
                    *campaign["mutations"]]
        require([c.get("name") for c in checks] == expected, "missing, duplicate, or reordered checks")
        for check in checks:
            check_name = check["name"]
            require(type(check.get("exit_code")) is int, "missing or invalid verifier exit code")
            negative = check_name in campaign["mutations"]
            expected_root = campaign["mutations"].get(check_name)
            if check_name.startswith("root_"):
                expected_root = next(r for r in campaign["roots"] if "root_" + r.replace("::", "_") == check_name)
            require(check.get("expected_failure") is negative and check.get("required_root") == expected_root,
                    "incorrect obligation: " + check_name)
            command = check["command"]
            artifact = Path(command[-1])
            if not artifact.is_absolute():
                artifact = root / artifact
            expected_artifact = path.parent / (check_name + ".rs") if negative else root / campaign["generated"]
            require(artifact.resolve() == expected_artifact.resolve(), "unexpected proof artifact")
            expected_command = [str(root / pin["distribution"] / "verus"),
                "--crate-name", campaign["crate_name"], "--crate-type=lib", "--edition=2021",
                "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent",
                "--rlimit", str(campaign["rlimit"])]
            if expected_root:
                module = campaign.get("verify_module")
                if module is None:
                    expected_command += ["--verify-root", "--verify-function", expected_root]
                else:
                    require(isinstance(module, str) and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*(?:::[A-Za-z_][A-Za-z_0-9]*)*", module),
                            "invalid verification module")
                    prefix = module + "::"
                    require(expected_root.startswith(prefix)
                            and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", expected_root[len(prefix):]),
                            "root outside declared verification module")
                    expected_command += ["--verify-only-module", module, "--verify-function", expected_root[len(prefix):]]
            expected_command += [str(expected_artifact)]
            require(command == expected_command, "wrong verified root or unsupported verifier command")
            require(digest(artifact) == check["source_sha256"], "stale proof artifact")
            log_path = root / check["log"]
            require(log_path.resolve().is_relative_to(path.parent.resolve()), "log escaped campaign directory")
            require(digest(log_path) == check["log_sha256"], "stale proof log")
            log = log_path.read_text()
            summaries = re.findall(r"verification results:: (\d+) verified, (\d+) errors", log)
            require(len(summaries) == 1, "missing or ambiguous verifier result")
            verified, errors = map(int, summaries[0])
            require(check.get("verified") == verified and check.get("errors") == errors, "altered verification counts")
            if negative:
                require(check.get("exit_code") != 0 and errors > 0
                        and re.search(r"(?:precondition|postcondition|invariant|assertion) not satisfied|assertion failed", log),
                        "mutation failed for wrong reason: " + check_name)
            else:
                require(check.get("exit_code") == 0 and errors == 0
                        and verified >= (1 if expected_root else len(campaign["roots"])),
                        "required proof did not pass: " + check_name)
        # Affine ownership misuse is intentionally rejected by the frontend.
        # Keep it separate from semantic mutants, which must reach the solver.
        type_mutations = campaign.get("type_mutations", {})
        require(receipt.get("required_type_mutations", {}) == type_mutations,
                "missing or changed ownership type controls")
        type_checks = receipt.get("type_checks", [])
        require([c.get("name") for c in type_checks] == list(type_mutations),
                "missing, duplicate, or reordered ownership type checks")
        for check in type_checks:
            check_name = check["name"]
            diagnostic = type_mutations[check_name]
            require(check.get("classification") == "ownership_type_rejection"
                    and check.get("expected_diagnostic") == diagnostic,
                    "incorrect ownership type obligation")
            require(type(check.get("exit_code")) is int and check["exit_code"] != 0,
                    "ownership misuse did not fail")
            artifact = path.parent / (check_name + ".rs")
            expected_command = [str(root / pin["distribution"] / "verus"),
                "--crate-name", campaign["crate_name"], "--crate-type=lib", "--edition=2021",
                "--target", pin["platform"], "--no-cheating", "--triggers-mode", "silent",
                "--rlimit", str(campaign["rlimit"]), str(artifact)]
            require(check.get("command") == expected_command, "unsupported ownership type command")
            require(digest(artifact) == check["source_sha256"], "stale ownership type artifact")
            log_path = root / check["log"]
            require(log_path.resolve().is_relative_to(path.parent), "ownership type log escaped campaign directory")
            require(digest(log_path) == check["log_sha256"], "stale ownership type log")
            if re.fullmatch(r"E\d{4}", diagnostic):
                diagnostic_pattern = r"error\[" + diagnostic + r"\]"
            else:
                require(diagnostic == "disallowed: constructor for an opaque datatype",
                        "unsupported ownership frontend diagnostic")
                diagnostic_pattern = r"error: " + re.escape(diagnostic)
            require(re.search(diagnostic_pattern, log_path.read_text()),
                    "ownership misuse failed for wrong reason")
        return receipt
    except (OSError, ValueError, KeyError, TypeError, IndexError, StopIteration) as error:
        raise RuntimeError(f"{name} refinement evidence: missing or malformed artifact: {error}") from error
