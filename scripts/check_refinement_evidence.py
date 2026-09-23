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
                expected_command += ["--verify-root", "--verify-function", expected_root]
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
        return receipt
    except (OSError, ValueError, KeyError, TypeError, IndexError, StopIteration) as error:
        raise RuntimeError(f"{name} refinement evidence: missing or malformed artifact: {error}") from error
