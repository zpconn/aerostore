#!/usr/bin/env python3
"""Provision pinned verification tools under target/, without global installs.

Requires Linux x86_64, Python 3.12+, git, rustup, a C toolchain, tar/zstd,
and Java 21 (for TLC). Network access is explicit by running this setup.
This provisions tools/dependencies; it does not establish a verification pass.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "target/verification-tools"
PRODUCTION_RUST = "1.93.1"
COMPONENTS = ("rust", "bridge", "verus", "tla")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_environment() -> dict[str, str]:
    # Keep rustup proxies on PATH: prepending a concrete stable rustc directory
    # would defeat Charon's explicit switch to its separate nightly toolchain.
    return {"RUSTUP_HOME": str(TOOLS / "production-rustup"),
            "CARGO_HOME": str(TOOLS / "production-cargo"),
            "RUSTUP_TOOLCHAIN": PRODUCTION_RUST,
            "RUSTUP_NO_UPDATE_CHECK": "1"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", action="append", choices=COMPONENTS,
                        help="provision selected components; repeat as needed; default all")
    parser.add_argument("--skip-fetch", action="store_true", help="skip production Cargo dependency prefetch")
    parser.add_argument("--dry-run", action="store_true", help="print the setup plan without downloads or mutations")
    args = parser.parse_args()
    selected = [name for name in COMPONENTS if not args.only or name in args.only]
    overrides = local_environment()
    plan = {"components": selected, "production_rust": PRODUCTION_RUST,
            "local_environment": overrides,
            "commands": {
                "rust": ["rustup set auto-self-update disable (local RUSTUP_HOME)",
                         f"rustup toolchain install {PRODUCTION_RUST} --profile minimal",
                         "cargo fetch --locked --target x86_64-unknown-linux-gnu"],
                "bridge": [f"{sys.executable} verification/bridge/bootstrap.py"],
                "verus": ["rustup set auto-self-update disable (local Verus RUSTUP_HOME)",
                          f"{sys.executable} verification/verus/install.py"],
                "tla": ["download only if missing; verify verification/tla/toolchain.json SHA-256", "java -version"]},
            "note": "No global Rust default, package manager, profile, or system installation changes."}
    if args.dry_run:
        print(json.dumps(plan, indent=2))
        return 0
    TOOLS.mkdir(parents=True, exist_ok=True)
    receipt_path = TOOLS / "setup.json"
    receipt = {"format_version": 1, "passed": False, "completed": False,
               "meaning": "tool setup only, not a proof or promotion decision",
               "started_at": datetime.now(timezone.utc).isoformat(), "plan": plan, "commands": []}
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    environment = os.environ | overrides

    def run(command: list[str], env: dict[str, str] | None = None) -> None:
        print("setup:", shlex.join(command), flush=True)
        result = subprocess.run(command, cwd=ROOT, env=env or environment, check=False)
        receipt["commands"].append({"command": command, "exit_code": result.returncode})
        if result.returncode:
            raise RuntimeError(f"setup command failed ({result.returncode}): {shlex.join(command)}")

    try:
        if sys.version_info < (3, 12):
            raise RuntimeError("Python 3.12 or newer is required")
        if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
            raise RuntimeError("the pinned extraction/Verus distributions support Linux x86_64")
        required = set()
        if set(selected) & {"rust", "bridge", "verus"}:
            required |= {"rustup", "git", "cc"}
        if "bridge" in selected:
            required |= {"tar", "zstd"}
        if "tla" in selected:
            required.add("java")
        missing = sorted(name for name in required if shutil.which(name) is None)
        if missing:
            raise RuntimeError("missing host prerequisites (install outside this script): " + ", ".join(missing))
        manifests = {"Cargo.lock": ROOT / "Cargo.lock",
                     "verification/lean/lake-manifest.json": ROOT / "verification/lean/lake-manifest.json"}
        before = {name: sha256(path) for name, path in manifests.items() if path.exists()}
        if "rust" in selected:
            run(["rustup", "set", "auto-self-update", "disable"])
            run(["rustup", "toolchain", "install", PRODUCTION_RUST, "--profile", "minimal"])
            run(["rustup", "run", PRODUCTION_RUST, "rustc", "--version"])
            if not args.skip_fetch:
                run(["rustup", "run", PRODUCTION_RUST, "cargo", "fetch", "--locked", "--target", "x86_64-unknown-linux-gnu"])
        if "bridge" in selected:
            run([sys.executable, "verification/bridge/bootstrap.py"])
        if "verus" in selected:
            pin = json.loads((ROOT / "verification/verus/toolchain.json").read_text())
            verus_env = environment | {"RUSTUP_HOME": str(ROOT / pin["rustup_home"])}
            run(["rustup", "set", "auto-self-update", "disable"], verus_env)
            run([sys.executable, "verification/verus/install.py"], verus_env)
        if "tla" in selected:
            version = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
            java_version = version.stdout + version.stderr
            if not re.search(r'version "21(?:[.\-"+])', java_version):
                raise RuntimeError("Java 21 is required for the recorded TLC configuration: " + java_version)
            pin = json.loads((ROOT / "verification/tla/toolchain.json").read_text())
            jar = ROOT / pin["default_path"]
            if not jar.exists():
                jar.parent.mkdir(parents=True, exist_ok=True)
                temporary = jar.with_suffix(".download")
                try:
                    print("setup: downloading pinned TLC", flush=True)
                    with urllib.request.urlopen(pin["release_url"], timeout=60) as response:
                        temporary.write_bytes(response.read())
                    if sha256(temporary) != pin["sha256"]:
                        raise RuntimeError("downloaded TLC artifact checksum mismatch")
                    temporary.replace(jar)
                finally:
                    temporary.unlink(missing_ok=True)
            if sha256(jar) != pin["sha256"]:
                raise RuntimeError("installed TLC artifact checksum mismatch")
            receipt["tlc_sha256"] = pin["sha256"]
            receipt["java_version"] = java_version.strip()
        changed = [name for name, expected in before.items() if sha256(manifests[name]) != expected]
        if changed:
            raise RuntimeError("setup changed locked dependencies; review instead of accepting drift: " + ", ".join(changed))
        receipt["dependency_manifest_sha256"] = before
        # Partial tool setup must not publish an environment pointing at a
        # production Rust installation that this invocation did not prepare.
        if "rust" in selected:
            (TOOLS / "environment.json").write_text(json.dumps(overrides, indent=2) + "\n")
            (TOOLS / "environment.sh").write_text(
                "# Generated local verification environment; source explicitly.\n" +
                "".join(f"export {name}={shlex.quote(value)}\n" for name, value in overrides.items()))
            print("Production environment: source target/verification-tools/environment.sh")
        receipt.update(passed=True, completed=True)
        print("Pinned setup completed. Run scripts/verify_formal.py separately.")
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        receipt["error"] = str(error)
        print("Verification setup failed:", error, file=sys.stderr)
        return 1
    finally:
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    sys.exit(main())
