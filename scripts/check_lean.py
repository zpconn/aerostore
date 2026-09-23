#!/usr/bin/env python3
"""Fail-closed real Rust -> Charon -> Aeneas -> Lean checks and axiom audit.

Setup is explicit: python3 verification/bridge/bootstrap.py.
No installed tool, extraction failure, stale generated body, absent root, unknown
axiom, or proof failure can be reported as a passing bridge.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "verification/lean"
TOOLS = ROOT / "target/verification-tools"
PIN = None


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=path.name + ".", delete=False) as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.replace(path)


def source_fingerprint():
    files = [ROOT / "aerostore_verified/src/lib.rs", ROOT / "aerostore_verified/Cargo.toml",
             ROOT / "verification/bridge/toolchain.json", ROOT / "verification/bridge/bootstrap.py",
             PROJECT / "roots.json", PROJECT / "lakefile.lean", PROJECT / "lean-toolchain",
             PROJECT / "lake-manifest.json", PROJECT / "AerostoreProofs.lean", Path(__file__).resolve()]
    files += sorted((PROJECT / "AerostoreProofs").glob("*.lean"))
    return {str(path.relative_to(ROOT)): digest(path) for path in files if path.is_file()}


def verify_release_models():
    """Authenticate cached semantic models against the pinned release, not themselves."""
    for relative, expected in PIN["release_binary_sha256"].items():
        if digest(TOOLS / relative) != expected:
            raise RuntimeError("release binary digest mismatch: " + relative)
    artifact = next(a for a in PIN["archives"] if a["file"] == "aeneas-linux-x86_64.tar.gz")
    archive = TOOLS / artifact["file"]
    if digest(archive) != artifact["sha256"]:
        raise RuntimeError("pinned Aeneas archive digest mismatch")
    verified = {}
    with tarfile.open(archive) as bundle:
        for member in bundle:
            name = member.name.removeprefix("./")
            if member.isfile() and name.startswith("backends/lean/") and name.endswith((".lean", ".olean")):
                expected = hashlib.file_digest(bundle.extractfile(member), "sha256").hexdigest()
                installed = TOOLS / "aeneas" / name
                if not installed.is_file() or digest(installed) != expected:
                    raise RuntimeError("Aeneas semantic model differs from pinned archive: " + name)
                verified[name] = expected
    if not verified:
        raise RuntimeError("no semantic models checked in pinned Aeneas archive")
    return {"files_checked": len(verified), "tree_sha256": hashlib.sha256(
        json.dumps(verified, sort_keys=True).encode()).hexdigest(), "archive_sha256": artifact["sha256"]}


def main():
    global PIN
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification/lean.json")
    parser.add_argument("--refresh-generated", action="store_true")
    args = parser.parse_args()
    args.output = args.output.resolve()
    if not args.output.is_relative_to((ROOT / "target").resolve()) or args.output.suffix != ".json":
        parser.error("--output must be a .json file beneath this repository's target directory")
    report = {"schema_version": 1, "passed": False, "completed": False,
              "whole_engine_verified": False, "commands": [],
              "bucket_sort_lean_refinement": "not_proved"}
    start = time.monotonic()
    write_report(args.output, report)
    try:
        PIN = json.loads((ROOT / "verification/bridge/toolchain.json").read_text())
    except Exception as error:
        report["error"] = str(error)
        write_report(args.output, report)
        print(str(error), flush=True)
        return 2
    report["tools"] = PIN
    initial_fingerprint = source_fingerprint()
    lean_bin = TOOLS / ("lean-" + PIN["lean_version"] + "-linux") / "bin"
    charon = TOOLS / ("charon-" + PIN["charon_commit"]) / "charon/target/release/charon"
    aeneas = TOOLS / "aeneas/aeneas"
    env = os.environ.copy()
    env.update({"RUSTUP_HOME": str(TOOLS / "rustup"), "CARGO_HOME": str(TOOLS / "cargo"),
                "RUSTUP_TOOLCHAIN": PIN["rust_toolchain"],
                "MATHLIB_CACHE_DIR": str(TOOLS / "mathlib-cache"),
                "PATH": str(lean_bin) + os.pathsep + env.get("PATH", "")})

    def run(command, cwd=ROOT, expected=0, custom_env=None, timeout=900):
        command = list(map(str, command))
        command_start = time.monotonic()
        result = subprocess.run(command, cwd=cwd, env=custom_env or env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        report["commands"].append({"command": command, "cwd": str(cwd),
                                   "exit_code": result.returncode, "output": result.stdout,
                                   "elapsed_seconds": round(time.monotonic() - command_start, 3)})
        if result.returncode != expected:
            raise RuntimeError(f"unexpected exit {result.returncode}: {' '.join(command)}\n{result.stdout[-6000:]}")
        return result.stdout

    def extract(source, destination):
        destination.mkdir(parents=True, exist_ok=True)
        llbc = destination / "aerostore_verified.llbc"
        run([charon, "rustc", "--preset", "aeneas", "--sysroot", "default", "--dest-file", llbc,
             "--", "--edition=2021", "--target=" + PIN["target"],
             "--crate-type", "lib", "--crate-name", "aerostore_verified", source])
        data = json.loads(llbc.read_text())
        if data.get("has_errors") or data.get("charon_version") != PIN["charon_version"]:
            raise RuntimeError("Charon extraction is erroneous or incompatible")
        embedded = [f for f in data["translated"]["files"] if f["crate_name"] == "aerostore_verified"]
        if not any(f.get("contents") == (ROOT / source).read_text() for f in embedded):
            raise RuntimeError("LLBC does not contain exact current Rust source")
        run([aeneas, "-backend", "lean", "-dest", destination, "-namespace", "AerostoreExtracted",
             "-abort-on-error", "-no-progress-bar", "-emit-json", llbc])
        metadata = json.loads((destination / "translation.json").read_text())
        locals_ = [f for f in metadata["functions"] if f["is_local"]]
        if any(f["is_opaque"] for f in locals_):
            raise RuntimeError("a local Rust function was made opaque")
        names = {f["lean_name"] for f in locals_}
        for name in ["canonical_buckets_sort", "canonical_buckets_bitmap", "stamp_precedes_snapshot"]:
            if "AerostoreExtracted." + name not in names:
                raise RuntimeError("missing extracted function: " + name)
        return destination / "AerostoreVerified.lean"

    try:
        for tool in [lean_bin / "lean", lean_bin / "lake", lean_bin / "leanchecker", charon, aeneas]:
            if not tool.is_file():
                raise RuntimeError(f"missing pinned tool: {tool}; run verification/bridge/bootstrap.py")
        versions = {"lean": run([lean_bin / "lean", "--version"]),
                    "charon": run([charon, "version"]), "aeneas": run([aeneas, "-version"])}
        if PIN["lean_version"] not in versions["lean"] or PIN["charon_commit"] not in versions["charon"]:
            raise RuntimeError("installed Lean/Charon version does not match pins")
        if PIN["aeneas_release"] not in versions["aeneas"]:
            raise RuntimeError("installed Aeneas does not match pin")
        report["tool_versions"] = versions
        report["semantic_model_integrity"] = verify_release_models()
        report["tool_sha256"] = {
            str(path.relative_to(ROOT)): digest(path)
            for path in [lean_bin / "lean", lean_bin / "lake", lean_bin / "leanchecker", charon,
                         charon.with_name("charon-driver"), aeneas]
        }
        # Refuse a missing/mismatched Git dependency before Lake can try to
        # fetch or update it. Networked materialization belongs to bootstrap.
        manifest = json.loads((PROJECT / "lake-manifest.json").read_text())
        dependency_revisions = {}
        for package in manifest["packages"]:
            if package["type"] != "git":
                continue
            package_path = PROJECT / manifest["packagesDir"] / package["name"]
            if not (package_path / ".git").exists():
                raise RuntimeError("missing Lean dependency: " + package["name"])
            actual_revision = run(["git", "rev-parse", "HEAD"], cwd=package_path).strip()
            if actual_revision != package["rev"]:
                raise RuntimeError("Lean dependency revision mismatch: " + package["name"])
            if run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=package_path).strip():
                raise RuntimeError("modified Lean dependency source: " + package["name"])
            dependency_revisions[package["name"]] = actual_revision
        report["dependency_revisions"] = dependency_revisions
        roots = json.loads((PROJECT / "roots.json").read_text())
        with tempfile.TemporaryDirectory(prefix="aerostore-lean-", dir=ROOT / "target") as scratch:
            stage = Path(scratch)
            generated = extract(Path("aerostore_verified/src/lib.rs"), stage / "extraction")
            checked_in = PROJECT / "AerostoreProofs/AerostoreVerified.lean"
            if args.refresh_generated:
                checked_in.write_bytes(generated.read_bytes())
                (checked_in.parent / "translation.json").write_bytes(generated.with_name("translation.json").read_bytes())
                (ROOT / "verification/bridge/generated/aerostore_verified.llbc").write_bytes(generated.with_name("aerostore_verified.llbc").read_bytes())
                initial_fingerprint = source_fingerprint()
            if not checked_in.exists() or checked_in.read_bytes() != generated.read_bytes():
                raise RuntimeError("generated Lean differs from actual Rust; use --refresh-generated then review")
            # Rebuild current local source even if build traces or cached .olean
            # files were modified. Dependency caches remain separately checked.
            local_build = PROJECT / ".lake/build"
            if local_build.exists():
                shutil.rmtree(local_build)
            run([lean_bin / "lake", "build"], cwd=PROJECT)
            audit = stage / "Audit.lean"
            lines = ["import AerostoreProofs.Contracts", "import AerostoreProofs.BridgeContracts",
                     "import AerostoreProofs"]
            for root in roots["roots"]:
                lines += [f"example : {root['contract']} := {root['name']}", f"#print axioms {root['name']}"]
            audit.write_text("\n".join(lines) + "\n")
            output = run([lean_bin / "lake", "env", "lean", "-DwarningAsError=true", audit], cwd=PROJECT)
            found = re.findall(r"depends on axioms:\s*\[([^\]]*)\]", output)
            clean = output.count("does not depend on any axioms")
            if len(found) + clean != len(roots["roots"]):
                raise RuntimeError("axiom report omitted a required root")
            axioms = sorted({a.strip() for block in found for a in block.split(",") if a.strip()})
            if set(axioms) - set(roots["allowed_axioms"]):
                raise RuntimeError("unexpected theorem axioms: " + repr(axioms))
            report["required_roots"] = roots["roots"]
            report["axioms"] = axioms
            # Elaborator options can bypass normal kernel checking while still
            # producing an axiom-free, ill-typed declaration. Replay all imports
            # and project constants into an empty kernel environment instead.
            replay_output = run([lean_bin / "lake", "env", "leanchecker", "--fresh", "--verbose",
                                 "AerostoreProofs"], cwd=PROJECT, timeout=1500)
            if "replaying AerostoreProofs with --fresh" not in replay_output:
                raise RuntimeError("fresh kernel replay did not identify the required proof module")
            report["kernel_recheck_passed"] = True
            report["kernel_recheck_scope"] = "all_project_and_imported_constants_in_fresh_environment"

            forged_dir = stage / "kernel-forgery"
            forged_dir.mkdir()
            forged_source = forged_dir / "Forged.lean"
            forged_source.write_text("""import Lean
set_option debug.skipKernelTC true
open Lean Elab Command
elab "injectFalse" : command => do
  liftCoreM <| addDecl (.thmDecl {
    name := `forgedFalse
    levelParams := []
    type := mkConst ``False
    value := mkConst ``True.intro })
injectFalse
#print axioms forgedFalse
""")
            forgery_compilation = run([lean_bin / "lean", "-o", "Forged.olean", "Forged.lean"], cwd=forged_dir)
            if "'forgedFalse' does not depend on any axioms" not in forgery_compilation:
                raise RuntimeError("kernel-bypass negative check did not create the intended axiom-free forgery")
            forged_env = env | {"LEAN_PATH": str(forged_dir)}
            forgery_rejection = run([lean_bin / "leanchecker", "--verbose", "Forged"], cwd=forged_dir,
                                    expected=1, custom_env=forged_env)
            if "(kernel) declaration type mismatch, 'forgedFalse'" not in forgery_rejection:
                raise RuntimeError("forged declaration did not fail for the expected kernel type mismatch")
            report["forged_theorem_rejected"] = True
            # Mutate Rust, re-extract it, and require unchanged refinement proofs to fail.
            source = (ROOT / "aerostore_verified/src/lib.rs").read_text()
            lean_path = run([lean_bin / "lake", "env", "printenv", "LEAN_PATH"], cwd=PROJECT).strip()
            mutations = [
                ("stamp_accepts_equal", "    stamp < transaction_id\n", "    stamp <= transaction_id\n", "Bridge.lean"),
                ("bitmap_drops_membership", "        present[input[i]] = true;", "        present[input[i]] = false;", "Bitmap.lean"),
                ("bitmap_accepts_equal_bound", "        if input[i] >= bucket_count {", "        if input[i] > bucket_count {", "Bitmap.lean"),
                ("sort_writes_wrong_bucket", "            output[position] = bucket;", "            output[position] = 0;", "Sort.lean"),
                ("sort_accepts_equal_bound", "        if bucket >= bucket_count {", "        if bucket > bucket_count {", "Sort.lean"),
            ]
            report["mutation_checks"] = []
            for name, old, new, proof in mutations:
                if source.count(old) != 1:
                    raise RuntimeError("mutation anchor is absent or ambiguous: " + name)
                mutant_dir = stage / name
                mutant_dir.mkdir()
                mutant = mutant_dir / "mutant.rs"
                mutant.write_text(source.replace(old, new))
                mutant_generated = extract(mutant, mutant_dir / "extraction")
                module_dir = mutant_dir / "modules/AerostoreProofs"
                module_dir.mkdir(parents=True)
                shutil.copyfile(PROJECT / ".lake/build/lib/lean/AerostoreProofs/Contracts.olean", module_dir / "Contracts.olean")
                mutant_env = env | {"LEAN_PATH": str(module_dir.parent) + os.pathsep + lean_path}
                run([lean_bin / "lean", "-o", module_dir / "AerostoreVerified.olean", mutant_generated], custom_env=mutant_env)
                run([lean_bin / "lean", "-o", module_dir / "BridgeContracts.olean", PROJECT / "AerostoreProofs/BridgeContracts.lean"], custom_env=mutant_env)
                mutation_output = run([lean_bin / "lean", PROJECT / "AerostoreProofs" / proof], expected=1, custom_env=mutant_env)
                expected_diagnostic = "Tactic `rfl` failed" if proof == "Bridge.lean" else "error: unsolved goals"
                forbidden_diagnostics = ["unknown module", "object file", "unknownIdentifier", "unknown identifier",
                                         "unexpected token", "unknown constant", "unknown tactic", "maximum recursion depth"]
                if expected_diagnostic not in mutation_output or any(s in mutation_output for s in forbidden_diagnostics):
                    raise RuntimeError("mutation failed for an unexpected reason: " + name)
                report["mutation_checks"].append({"name": name, "rejected": True, "proof": proof,
                                                  "rust_sha256": digest(mutant), "extracted_sha256": digest(mutant_generated)})
            # Independent mathematical-contract mutations. These are not Rust
            # refinement checks: they test that the parameterized predicate
            # proofs depend on stamp equality/freshness, overwrite publication,
            # and inclusion of local writes, rather than proving a vacuous type.
            contracts_source = (PROJECT / "AerostoreProofs/Contracts.lean").read_text()
            predicate_mutations = [
                ("predicate_ignores_changed_stamp", "current = recorded ∧ current < transactionStart",
                 "current = current ∧ current < transactionStart"),
                ("predicate_accepts_equal_start", "current = recorded ∧ current < transactionStart",
                 "current = recorded ∧ current ≤ transactionStart"),
                ("predicate_drops_publication", "(Classical.propDecidable _) event.stamp (before bucket)",
                 "(Classical.propDecidable _) (before bucket) (before bucket)"),
                ("predicate_omits_own_candidates", "row ∈ candidates ∨ own row ≠ none", "row ∈ candidates ∨ False"),
            ]
            for name, old, new in predicate_mutations:
                if contracts_source.count(old) != 1:
                    raise RuntimeError("predicate mutation anchor is absent or ambiguous: " + name)
                module_dir = stage / name / "AerostoreProofs"
                module_dir.mkdir(parents=True)
                mutant = module_dir / "Contracts.lean"
                mutant.write_text(contracts_source.replace(old, new))
                mutant_env = env | {"LEAN_PATH": str(module_dir.parent) + os.pathsep + lean_path}
                run([lean_bin / "lean", "-o", module_dir / "Contracts.olean", mutant], custom_env=mutant_env)
                mutation_output = run([lean_bin / "lean", PROJECT / "AerostoreProofs/Predicate.lean"],
                                      expected=1, custom_env=mutant_env)
                intended_diagnostics = ["error: unsolved goals", "error: Type mismatch",
                                        "error: Application type mismatch", "omega could not prove"]
                forbidden_diagnostics = ["unknown module", "object file", "unknownIdentifier", "unknown identifier",
                                         "unexpected token", "unknown constant", "unknown tactic", "maximum recursion depth"]
                if not any(s in mutation_output for s in intended_diagnostics) or any(
                        s in mutation_output for s in forbidden_diagnostics):
                    raise RuntimeError("predicate mutation failed for an unexpected reason: " + name)
                report["mutation_checks"].append({"name": name, "rejected": True, "proof": "Predicate.lean",
                                                  "scope": "abstract_contract_not_rust_refinement",
                                                  "contract_sha256": digest(mutant)})
            # Lifecycle history controls mutate transitions/arithmetic premises,
            # not the desired late-publication conclusion. This is independent
            # abstract history evidence, not native atomic refinement.
            lifecycle_source = (PROJECT / "AerostoreProofs/Lifecycle.lean").read_text()
            lifecycle_mutations = [
                ("lifecycle_reservation_does_not_advance", "LifecycleStep s {s with clock := s.clock + 1, starts :=",
                 "LifecycleStep s {s with clock := s.clock, starts :="),
                ("lifecycle_uses_writer_start_stamp", "Function.update s.publication tx (some s.clock)",
                 "Function.update s.publication tx (s.starts tx)"),
                ("lifecycle_publishes_before_end", "(ended : tx ∈ s.finished)", "(ended : True)"),
                ("lifecycle_allows_wrapping_reservation", "∀ clock : Nat, clock < 2^64 - 1 →",
                 "∀ clock : Nat, clock ≤ 2^64 - 1 →"),
            ]
            for name, old, new in lifecycle_mutations:
                if lifecycle_source.count(old) != 1:
                    raise RuntimeError("lifecycle mutation anchor is absent or ambiguous: " + name)
                mutant_dir = stage / name
                mutant_dir.mkdir()
                mutant = mutant_dir / "Lifecycle.lean"
                mutant.write_text(lifecycle_source.replace(old, new))
                mutation_output = run([lean_bin / "lake", "env", "lean", "-Dpp.deepTerms.threshold=8", mutant], cwd=PROJECT, expected=1)
                intended_diagnostics = ["error: unsolved goals", "error: Type mismatch",
                                        "error: Application type mismatch", "omega could not prove",
                                        "Tactic `change` failed"]
                forbidden_diagnostics = ["unknown module", "object file", "unknownIdentifier", "unknown identifier",
                                         "unexpected token", "unknown constant", "unknown tactic", "maximum recursion depth"]
                if not any(s in mutation_output for s in intended_diagnostics) or any(
                        s in mutation_output for s in forbidden_diagnostics):
                    raise RuntimeError("lifecycle mutation failed for an unexpected reason: " + name)
                report["mutation_checks"].append({"name": name, "rejected": True, "proof": "Lifecycle.lean",
                                                  "scope": "abstract_history_not_native_atomic_refinement",
                                                  "transition_sha256": digest(mutant)})
            # The complete-or-retry proof must depend on maintained old/new
            # postings, overlap-local stamp ordering, real MVCC predicates and
            # its own private-write candidate/filter path. Do not mutate the
            # desired completeness conclusion or assume candidate coverage.
            query_source = (PROJECT / "AerostoreProofs/QueryCompleteness.lean").read_text()
            query_mutations = [
                ("query_omits_old_bucket",
                 "(event.beforeKey = some key ∨ event.afterKey = some key) ∧ bucket key = b",
                 "event.afterKey = some key ∧ bucket key = b"),
                ("query_omits_destination_posting",
                 "{pair | pair.2 = event.row ∧ event.afterKey = some pair.1}", "{pair | False}"),
                ("query_ignores_creator_active",
                 "creator = s.reader ∨ creator < s.xmin ∨ (creator < s.xmax ∧ creator ∉ s.active)",
                 "creator = s.reader ∨ creator < s.xmin ∨ creator < s.xmax"),
                ("query_omits_own_candidates", "candidates ∪ {row | own row ≠ none}", "candidates"),
                ("query_allows_stamp_regression", "(∀ b ∈ event.buckets, before b ≤ event.stamp)",
                 "(∀ b ∈ event.buckets, 0 ≤ event.stamp)"),
                ("query_ignores_deleter_active", "(s.xmax ≤ deleter ∨ deleter ∈ s.active)",
                 "s.xmax ≤ deleter"),
                ("query_filters_before_own_overlay",
                 "row ∈ queryOwnCandidates candidates own ∧ rowMatches (overlayOwnWrites snapshot own) predicate row",
                 "row ∈ queryOwnCandidates candidates own ∧ rowMatches snapshot predicate row"),
            ]
            for name, old, new in query_mutations:
                if query_source.count(old) != 1:
                    raise RuntimeError("query mutation anchor is absent or ambiguous: " + name)
                mutant_dir = stage / name
                mutant_dir.mkdir()
                mutant = mutant_dir / "QueryCompleteness.lean"
                mutant.write_text(query_source.replace(old, new))
                mutation_output = run([lean_bin / "lake", "env", "lean", "-Dpp.deepTerms.threshold=8", mutant], cwd=PROJECT, expected=1)
                intended_diagnostics = ["error: unsolved goals", "error: Type mismatch",
                                        "error: Application type mismatch", "omega could not prove",
                                        "Tactic `change` failed"]
                forbidden_diagnostics = ["unknown module", "object file", "unknownIdentifier", "unknown identifier",
                                         "unexpected token", "unknown constant", "unknown tactic", "maximum recursion depth"]
                if not any(s in mutation_output for s in intended_diagnostics) or any(
                        s in mutation_output for s in forbidden_diagnostics):
                    raise RuntimeError("query mutation failed for an unexpected reason: " + name)
                report["mutation_checks"].append({"name": name, "rejected": True, "proof": "QueryCompleteness.lean",
                                                  "scope": "abstract_query_history_not_native_heap_refinement",
                                                  "transition_sha256": digest(mutant)})
        report["source_sha256_before"] = initial_fingerprint
        report["source_sha256"] = source_fingerprint()
        if report["source_sha256"] != initial_fingerprint:
            raise RuntimeError("verification source changed while the Lean gate was running")
        report.update(passed=True, completed=True, bucket_bitmap_lean_refinement="proved_all_inputs_vec_model",
                      bucket_sort_lean_refinement="proved_all_inputs_vec_model",
                      production_4096_corollaries=True, complete_result_equivalence=True)
    except Exception as error:
        report["error"] = str(error)
        print(str(error), flush=True)
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - start, 3)
        write_report(args.output, report)
    print(json.dumps({"passed": report["passed"], "output": str(args.output)}))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
