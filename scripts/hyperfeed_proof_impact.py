#!/usr/bin/env python3
"""Advisory proof-input differences between captures; never a verification gate.

No proof receipts are accepted or cached here. Unchanged declared input hashes
are a scoping observation, not a proof of transport behavior or whole-engine
correctness. Capture inventories omit some documentation and CI inputs.
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import uuid

CAMPAIGNS = "verification/refinement_campaigns.json"
BOUNDARY = "verification/frozen_boundary.json"
SERVICE = "aerostore_core/benches/contention_crucible/service.rs"
SERVICE_TEST = "aerostore_core/tests/contention_service.rs"
IMPLEMENTATION = ("aerostore_core/", "aerostore_verified/", "aerostore_macros/", "aerostore_tcl/")


def _safe_path(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or str(path) != name or name == ".":
        raise ValueError("Unsafe captured source path: " + name)
    return path


def _files(capture):
    if capture.get("format") != "aerostore-hyperfeed-screen-capture-v1" or capture.get("complete") is not True:
        raise ValueError("Impact reporting requires a complete screen capture")
    files = capture["source"]["files"]
    if not isinstance(files, dict) or not files:
        raise ValueError("Missing captured source inventory")
    for name, digest in files.items():
        _safe_path(name)
        if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest):
            raise ValueError("Malformed source digest: " + name)
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if digest != capture["source"]["sha256"]:
        raise ValueError("Captured source inventory digest differs")
    return files


def _metadata(capture, name):
    root = Path(capture["source"]["root"]).resolve(strict=True)
    path = root / _safe_path(name)
    if path.is_symlink() or not path.resolve(strict=True).is_relative_to(root):
        raise ValueError("Metadata path escaped captured source")
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != capture["source"]["files"].get(name):
        raise ValueError("Missing or changed captured metadata: " + name)
    return json.loads(data)


def report(baseline_manifest, candidate_manifest):
    """Compare source maps; caller separately validates executable provenance."""
    before, after = _files(baseline_manifest), _files(candidate_manifest)
    previous = _metadata(baseline_manifest, CAMPAIGNS)["campaigns"]
    current = _metadata(candidate_manifest, CAMPAIGNS)["campaigns"]
    baseline_boundary = _metadata(baseline_manifest, BOUNDARY)["files"]
    candidate_boundary = _metadata(candidate_manifest, BOUNDARY)["files"]
    output_namespace = "target/proof-impact-review/" + uuid.uuid4().hex
    changed = {name for name in before.keys() | after.keys() if before.get(name) != after.get(name)}
    components, mapped = [], set()
    for name in sorted(previous.keys() | current.keys()):
        old, new = previous.get(name, {}), current.get(name, {})
        inputs = set(old.get("inputs", [])) | set(new.get("inputs", []))
        for path in inputs:
            _safe_path(path)
        mapped |= inputs
        touched = sorted(inputs & changed)
        missing = sorted(path for path in inputs if path not in before or path not in after)
        declaration_changed = old != new
        status = ("changed_declared_inputs" if touched or declaration_changed else
                  "incomplete_input_coverage" if missing else "unchanged_declared_inputs")
        runner = "verification/" + name.replace("-", "_") + "/run.py"
        command = (["python3", runner, "--output", output_namespace + "/" + name]
                   if runner in new.get("inputs", []) and runner in after and re.fullmatch(r"[a-z0-9-]+", name) else None)
        components.append({"name": name, "status": status, "changed_inputs": touched,
                           "uncaptured_inputs": missing, "declaration_changed": declaration_changed,
                           "scope": new.get("scope", old.get("scope")),
                           "existing_component_command": command if touched or declaration_changed else None})
    drift = lambda files, frozen: sorted(name for name, digest in frozen.items()
                                         if name in files and files[name] != digest)
    service_changed = sorted(name for name in changed if name in (SERVICE, SERVICE_TEST)
                             or name.startswith("verification/service_protocol/"))
    return {
        "format": "aerostore-hyperfeed-proof-impact-v1", "advisory_only": True,
        "recommendation_output_namespace": output_namespace,
        "output_directory_must_be_fresh": True,
        "command_templates_note": "Recommended commands use a unique namespace for this report. Before execution, require every output directory to be absent; never rerun into an existing evidence directory.",
        "proofs_executed": False, "proof_receipts_validated": False,
        "formal_gate_passed": False, "whole_engine_verified": False,
        "baseline_source_sha256": baseline_manifest["source"]["sha256"],
        "candidate_source_sha256": candidate_manifest["source"]["sha256"],
        "changed_files": sorted(changed), "declared_components": components,
        "unmapped_implementation_changes": sorted(name for name in changed - mapped
                                                   if name.startswith(IMPLEMENTATION) and name.endswith(".rs")),
        "build_input_changes": sorted(name for name in changed if name.endswith(("Cargo.toml", "Cargo.lock", "build.rs"))
                                      or name.startswith(".cargo/") or name in ("rust-toolchain", "rust-toolchain.toml")),
        "build_identity_changed": baseline_manifest.get("comparison_build_identity") != candidate_manifest.get("comparison_build_identity"),
        "frozen_boundary": {
            "gate_executed": False, "boundary_manifest_changed": baseline_boundary != candidate_boundary,
            "baseline_observed_drift": drift(before, baseline_boundary),
            "baseline_already_out_of_date_for_captured_inputs": bool(drift(before, baseline_boundary)),
            "candidate_observed_drift": drift(after, candidate_boundary),
            "changed_paths_in_baseline_boundary": sorted(changed & baseline_boundary.keys()),
            "baseline_uncaptured_paths": sorted(baseline_boundary.keys() - before.keys()),
            "candidate_uncaptured_paths": sorted(candidate_boundary.keys() - after.keys()),
            "note": "Partial source-map comparison only; frozen path-set and trusted-baseline checks still apply. Never refresh the boundary here."},
        "service_protocol": {"changed_associated_inputs": service_changed,
            "implementation_refinement_proved": False,
            "native_framing_review_needed": SERVICE in changed,
            "existing_native_test_command": ["cargo", "test", "--offline", "--locked", "-p", "aerostore_core",
                                               "--release", "--test", "contention_service", "--", "--test-threads=1"] if service_changed else None,
            "existing_model_command": ["python3", "verification/service_protocol/check.py", "--output", output_namespace + "/service-protocol"] if service_changed else None,
            "scope": "Abstract ownership/outcome model excludes socket parsing. Review partial writes, interruptions, zero writes, deadlines and lost commit replies in native tests."},
        "limitations": ["Declared input sets are not a complete transitive compiler, feature, macro, caller or model dependency graph.",
                        "Unchanged inputs do not authenticate an old proof receipt; no receipt reuse or formal acceptance is granted.",
                        "Lean, core Verus, TLC and native campaigns outside refinement_campaigns.json are not classified here.",
                        "Existing formal runner has no affected-only/cache profile. Full gate policies and reviewed rebaseline requirements remain unchanged.",
                        "Caller must validate captures and executable provenance separately; commands are recommendations and were not executed."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baselinecapture", type=Path, required=True)
    parser.add_argument("--candidatecapture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = report(json.loads(args.baselinecapture.read_text()), json.loads(args.candidatecapture.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
