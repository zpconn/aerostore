#!/usr/bin/env python3
"""Provision pinned extraction tools locally; explicit networked setup, never a proof gate."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "target/verification-tools"
PIN = json.loads((Path(__file__).with_name("toolchain.json")).read_text())


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    if sys.version_info < (3, 12):
        raise RuntimeError("Python 3.12 or later is required")
    TOOLS.mkdir(parents=True, exist_ok=True)
    for artifact in PIN["archives"]:
        archive = TOOLS / artifact["file"]
        if not archive.exists():
            print("Downloading", artifact["url"], flush=True)
            temporary = archive.with_suffix(archive.suffix + ".download")
            urllib.request.urlretrieve(artifact["url"], temporary)
            temporary.replace(archive)
        if digest(archive) != artifact["sha256"]:
            raise RuntimeError(f"archive digest mismatch: {archive}")
        if not (TOOLS / artifact["marker"]).exists():
            destination = TOOLS / artifact["directory"]
            destination.mkdir(parents=True, exist_ok=True)
            if archive.name.endswith(".zst"):
                subprocess.run(["tar", "--zstd", "-xf", str(archive), "-C", str(destination)], check=True)
            else:
                with tarfile.open(archive) as bundle:
                    bundle.extractall(destination, filter="data")
    rustup = shutil.which("rustup")
    if rustup is None:
        raise RuntimeError("rustup executable required; no system installation attempted")
    env = os.environ.copy()
    env.update({
        "RUSTUP_HOME": str(TOOLS / "rustup"),
        "CARGO_HOME": str(TOOLS / "cargo"),
        "RUSTUP_TOOLCHAIN": PIN["rust_toolchain"],
        "MATHLIB_CACHE_DIR": str(TOOLS / "mathlib-cache"),
        "CHARON_GIT_COMMIT": PIN["charon_commit"],
    })
    subprocess.run([rustup, "set", "auto-self-update", "disable"], env=env, check=True)
    subprocess.run([rustup, "toolchain", "install", PIN["rust_toolchain"], "--profile", "minimal",
                    "--component", "rustc-dev,rust-src,llvm-tools"], env=env, check=True)
    source = TOOLS / ("charon-" + PIN["charon_commit"]) / "charon"
    subprocess.run([rustup, "run", PIN["rust_toolchain"], "cargo", "build", "--release", "--locked",
                    "--bin", "charon", "--bin", "charon-driver"], cwd=source, env=env, check=True)
    lean_bin = TOOLS / ("lean-" + PIN["lean_version"] + "-linux") / "bin"
    env["PATH"] = str(lean_bin) + os.pathsep + env.get("PATH", "")
    project = ROOT / "verification/lean"
    manifest = project / "lake-manifest.json"
    locked_manifest = manifest.read_bytes()
    # Loading a manifest materializes its exact Git revisions. `lake update`
    # would resolve branches again and is intentionally not used in bootstrap.
    subprocess.run([str(lean_bin / "lake"), "exe", "cache", "get"], cwd=project, env=env, check=True)
    subprocess.run([str(lean_bin / "lake"), "build"], cwd=project, env=env, check=True)
    if manifest.read_bytes() != locked_manifest:
        raise RuntimeError("Lean dependency manifest changed during locked bootstrap")
    print("Pinned Charon, Aeneas, Lean, and dependencies are ready. Run scripts/check_lean.py.")


if __name__ == "__main__":
    main()
