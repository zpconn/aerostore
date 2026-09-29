#!/usr/bin/env python3
"""Preserve screening inputs while reusing one ordinary Cargo cache.

These captures are engineering experiment inputs, not formal-gate receipts.
The caller owns campaign resource admission and must serialize timed trials.
"""

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import shlex
import shutil
import subprocess
import tarfile
import time
import tomllib

import qualify_hyperfeed as qualification


ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "aerostore_fast_iteration_v1"
FLAGS = ["--cfg", NAMESPACE, "--check-cfg=cfg(" + NAMESPACE + ")"]
BENCH = "hyperfeed_contention_crucible"
FORMAT = "aerostore-hyperfeed-screen-capture-v1"
RESERVE = 30 << 30


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(path):
    path = Path(path).resolve(strict=True)
    return {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}


def write_json(path, value):
    qualification.atomic_json(Path(path), value)


def source_digest(files):
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def safe_relative(name):
    path = PurePosixPath(name)
    require(not path.is_absolute() and ".." not in path.parts and str(path) == name and name != ".",
            "unsafe source path: " + name)
    return Path(name)


def copy_sources(repo, destination, snapshot):
    for name, expected in snapshot["files"].items():
        source = repo / safe_relative(name)
        require(not source.is_symlink() and source.resolve().is_relative_to(repo.resolve()),
                "source symlink or escape: " + name)
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        require(digest(target) == expected, "source changed while copying: " + name)


def extract_sources(archive, destination, snapshot):
    """Extract exactly the receipt's regular files, without tar link traversal."""
    require(source_digest(snapshot["files"]) == snapshot["sha256"], "invalid source map digest")
    seen, total = set(), 0
    with tarfile.open(archive, "r:gz") as stream:
        for member in stream:
            relative = safe_relative(member.name)
            require(member.isfile() and member.name in snapshot["files"] and member.name not in seen,
                    "unexpected, duplicate, or nonregular archive member: " + member.name)
            total += member.size
            require(total <= 256 << 20 and len(seen) < 10000, "source archive exceeds bounded capture size")
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            with stream.extractfile(member) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            require(digest(target) == snapshot["files"][member.name], "source archive member hash mismatch")
            seen.add(member.name)
    require(seen == set(snapshot["files"]), "source archive omitted receipt inputs")


def runtime_dependencies(binary):
    result = subprocess.run(["ldd", str(binary)], text=True, capture_output=True, check=True)
    require("not found" not in result.stdout, "missing benchmark runtime dependency")
    dependencies = {}
    for line in result.stdout.splitlines():
        match = re.search(r"(?:=>\s+)?(/[^\s]+)\s+\(", line)
        if match:
            item = identity(match.group(1))
            dependencies[item["path"]] = item
    require(bool(dependencies), "ldd did not identify runtime dependencies")
    return list(dependencies.values())


def validate_capture(path):
    path = Path(path)
    if path.is_dir():
        path /= "capture.json"
    value = json.loads(path.read_text())
    require(value.get("format") == FORMAT and value.get("complete") is True, "incomplete screen capture")
    root = path.resolve().parent
    require(Path(value["source"]["root"]) == root / "source", "source capture moved or escaped")
    files = value["source"]["files"]
    require(source_digest(files) == value["source"]["sha256"], "source map digest changed")
    for name, expected in files.items():
        source = root / "source" / safe_relative(name)
        require(not source.is_symlink() and source.resolve().is_relative_to(root / "source")
                and digest(source) == expected, "captured source changed: " + name)
    require(qualification.snapshot_sources(root / "source") == {"files": files, "sha256": value["source"]["sha256"]},
            "captured source inventory changed")
    require(Path(value["benchmark"]["path"]) == root / "benchmark", "benchmark capture moved or escaped")
    for item in [value["benchmark"], *value["runtime_dependencies"]]:
        require(digest(item["path"]) == item["sha256"], "retained runtime artifact changed: " + item["path"])
    for key in ("imported_receipt", "original_receipt", "compiler_binary", "cargo_binary"):
        if key in value:
            require(digest(value[key]["path"]) == value[key]["sha256"], "capture provenance changed: " + key)
    require(value["build_configuration"].get("workspace_profiles") ==
            tomllib.loads((root / "source/Cargo.toml").read_text()).get("profile", {}),
            "recorded compiler profile differs from captured workspace")
    require(value.get("comparison_build_identity") == comparison_identity(value), "build identity differs from provenance")
    require("scripts/qualify_hyperfeed.py" in files, "capture omitted qualification driver")
    return value


def comparison_identity(value):
    """Exclude locations while retaining compiler flags and build semantics."""
    configuration = value["build_configuration"]
    environment = configuration["environment"]
    flags = environment.get("CARGO_ENCODED_RUSTFLAGS")
    flags = flags.split("\x1f") if flags is not None else shlex.split(environment.get("RUSTFLAGS", ""))
    return {"capture_mode": value["mode"], "compiler": value["compiler"], "cargo": value["cargo"],
            "flags": flags, "features": configuration["features"],
            "cargo_profile": configuration.get("cargo_profile"),
            "workspace_profiles": configuration.get("workspace_profiles"),
            "cargo_config_hashes": [item["sha256"] for item in configuration.get("cargo_configs", [])]}


def start_capture(output, mode):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    value = {"format": FORMAT, "mode": mode, "complete": False,
             "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "formal_scope": "Performance screening only; no formal verification acceptance implied."}
    write_json(output / "capture.json", value)
    return output, value


def finish_capture(output, value, snapshot):
    value["build_configuration"]["workspace_profiles"] = tomllib.loads(
        (output / "source/Cargo.toml").read_text()).get("profile", {})
    value.update(source={"root": str(output / "source"), **snapshot}, benchmark=identity(output / "benchmark"),
                 runtime_dependencies=runtime_dependencies(output / "benchmark"), complete=True,
                 finished_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    value["comparison_build_identity"] = comparison_identity(value)
    write_json(output / "capture.json", value)
    return validate_capture(output)


def import_capture(receipt, output):
    """Import a trusted local build receipt; never relabel it as a new build."""
    receipt = Path(receipt).resolve(strict=True)
    original = json.loads(receipt.read_text())
    require(original.get("passed") is True and original.get("completed") is True
            and original.get("exit_code") == 0 and original.get("source_stable") is True,
            "historical build receipt did not pass")
    snapshot = original["source_before"]
    require(snapshot == original["source_after"], "historical source changed during build")
    archive, binary = Path(original["source_archive_path"]), Path(original["binary_path"])
    require(digest(archive) == original["source_archive_sha256"], "historical source archive changed")
    require(digest(binary) == original["binary_sha256"], "historical benchmark changed")
    for item in original.get("runtime_after", {}).get("checks", []):
        require(item.get("matches") is True and digest(item["path"]) == item["sha256"],
                "historical runtime dependency changed")
    output, value = start_capture(output, "imported_trusted_build_receipt")
    shutil.copyfile(receipt, output / "imported-build-receipt.json")
    value.update(imported_receipt=identity(output / "imported-build-receipt.json"),
                 original_receipt=identity(receipt), compiler=original["compiler"], cargo=original["cargo"],
                 build_configuration={"command": original["command"],
                                      "environment": original["build_environment"],
                                      "cargo_profile": original.get("cargo_artifact", {}).get("profile"),
                                      "features": original.get("features", [])})
    extract_sources(archive, output / "source", snapshot)
    shutil.copy2(binary, output / "benchmark")
    require(digest(output / "benchmark") == original["binary_sha256"], "benchmark changed during import")
    return finish_capture(output, value, snapshot)


@contextlib.contextmanager
def namespace_lock(repo, snapshot):
    """Admit a previously unused cfg once, then serialize its reusable cache."""
    marker = repo / "target" / ("." + NAMESPACE + ".json")
    marker.parent.mkdir(exist_ok=True)
    with (marker.with_suffix(".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for name in snapshot["files"]:
            if name.endswith((".rs", ".toml")):
                require(NAMESPACE.encode() not in (repo / name).read_bytes(), "cfg namespace affects source semantics")
        admission = {"namespace": NAMESPACE, "flags": FLAGS, "cache_policy": "mutable; never retain Cargo output paths"}
        if marker.exists():
            require(json.loads(marker.read_text()) == admission, "unexpected namespace owner")
        else:
            for profile in ("debug", "release"):
                for path in (repo / "target" / profile / ".fingerprint").glob("*/*.json"):
                    require(NAMESPACE.encode() not in path.read_bytes(), "cfg namespace already used without admission")
            write_json(marker, admission)
        yield admission


def build_environment(repo, rustc):
    environment = os.environ.copy()
    for name in list(environment):
        if name.startswith(("CARGO_", "RUST")) or name in ("CC", "CXX", "CFLAGS", "CXXFLAGS", "LDFLAGS"):
            environment.pop(name)
    environment.update(CARGO_TARGET_DIR=str(repo / "target"), CARGO_ENCODED_RUSTFLAGS="\x1f".join(FLAGS),
                       RUSTC=str(rustc), RUSTC_WRAPPER="", RUSTC_WORKSPACE_WRAPPER="",
                       RUSTUP_TOOLCHAIN="stable-x86_64-unknown-linux-gnu")
    return environment


def capture_variant(output, jobs=8, repo=ROOT, host_volume=None):
    """Build current working bytes in the normal target, then preserve one copy.

    Do not run this concurrently with source edits or another Cargo build. The
    shared namespace is reusable screening cache, never a verification target.
    """
    repo = Path(repo).resolve()
    require(type(jobs) is int and 1 <= jobs <= 8, "build jobs must be between 1 and 8")
    filesystems = [repo]
    if host_volume is not None:
        filesystems.append(Path(host_volume).resolve(strict=True))
    for filesystem in filesystems:
        require(shutil.disk_usage(filesystem).free >= RESERVE + (8 << 30), "insufficient build disk headroom")
    cargo = Path(subprocess.check_output(["rustup", "which", "cargo"], text=True).strip())
    rustc = cargo.with_name("rustc")
    compiler = subprocess.check_output([str(rustc), "-Vv"], text=True)
    require("release: 1.93.1\n" in compiler and "host: x86_64-unknown-linux-gnu\n" in compiler,
            "screening captures require Rust 1.93.1 on x86_64 Linux")
    snapshot = qualification.snapshot_sources(repo)
    output, value = start_capture(output, "working_tree_shared_cache_build")
    copy_sources(repo, output / "source", snapshot)
    configs = [ancestor / ".cargo" / name for ancestor in (repo, *repo.parents)
               for name in ("config", "config.toml")]
    configs += [Path.home() / ".cargo" / name for name in ("config", "config.toml")]
    configuration = [identity(path) for path in configs if path.is_file()]
    environment = build_environment(repo, rustc)
    command = [str(cargo), "build", "--offline", "--locked", "--release", "-j", str(jobs),
               "-p", "aerostore_core", "--bench", BENCH, "--message-format=json"]
    value.update(compiler=compiler, cargo=subprocess.check_output([str(cargo), "-V"], text=True),
                 compiler_binary=identity(rustc), cargo_binary=identity(cargo),
                 build_configuration={"command": command, "cargo_configs": configuration,
                                      "features": [], "profile": "release", "environment": {
                                          k: v for k, v in environment.items() if k.startswith(("CARGO_", "RUST"))}})
    write_json(output / "capture.json", value)
    with namespace_lock(repo, snapshot) as admission:
        value["cache_admission"] = admission
        # Cargo JSON includes mutable cache paths. Consume it only in memory;
        # retain compiler diagnostics separately, never compiler-artifact events.
        with (output / "build.log").open("w") as log:
            result = subprocess.run(command, cwd=repo, env=environment, text=True, stdout=subprocess.PIPE,
                                    stderr=log, preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)))
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        with (output / "build.log").open("a") as log:
            for event in events:
                if event.get("reason") == "compiler-message":
                    log.write(event.get("message", {}).get("rendered") or "")
        value["build_exit_code"] = result.returncode
        write_json(output / "capture.json", value)
        require(result.returncode == 0, "screen capture build failed; see build.log")
        artifacts = [event for event in events if event.get("reason") == "compiler-artifact"
                     and event.get("target", {}).get("name") == BENCH and event.get("executable")]
        require(len(artifacts) == 1, "missing or ambiguous contention benchmark artifact")
        artifact = artifacts[0]
        value["build_configuration"].update(cargo_profile=artifact["profile"], features=artifact["features"])
        shutil.copy2(artifact["executable"], output / "benchmark")
        require(snapshot == qualification.snapshot_sources(repo), "working sources changed during capture")
        require(configuration == [identity(path) for path in configs if path.is_file()], "Cargo configuration changed")
    return finish_capture(output, value, snapshot)
