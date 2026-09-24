#!/usr/bin/env python3
"""Install the checksum-pinned release in ignored target storage, never system-wide."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
PIN = Path(__file__).with_name("toolchain.json")


def main() -> None:
    pins = json.loads(PIN.read_text())
    destination = ROOT / pins["distribution"]
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="aerostore-verus-") as temporary:
            archive = Path(temporary) / "verus.zip"
            urllib.request.urlretrieve(pins["archive_url"], archive)
            if hashlib.sha256(archive.read_bytes()).hexdigest() != pins["archive_sha256"]:
                raise SystemExit("Verus archive checksum mismatch")
            with zipfile.ZipFile(archive) as zipped:
                for entry in zipped.infolist():
                    target = (destination.parent / entry.filename).resolve()
                    if not target.is_relative_to(destination.parent.resolve()):
                        raise SystemExit("unsafe path in release archive")
                zipped.extractall(destination.parent)
            for executable in ["verus", "rust_verify", "z3"]:
                (destination / executable).chmod(0o755)
    for relative, expected in pins["artifact_sha256"].items():
        if hashlib.sha256((destination / relative).read_bytes()).hexdigest() != expected:
            raise SystemExit("installed Verus artifact checksum mismatch: " + relative)
    environment = dict(os.environ, RUSTUP_HOME=str(ROOT / pins["rustup_home"]))
    rustup = shutil.which("rustup")
    if not rustup:
        raise SystemExit("rustup must already be installed")
    subprocess.run([rustup, "toolchain", "install", pins["rust_toolchain"], "--profile", "minimal"],
                   env=environment, check=True)


if __name__ == "__main__":
    main()
