#!/usr/bin/env python3
"""Run the tracked Python unit suites without selecting native integration builds.

Each test directory gets a fresh interpreter: verification campaigns reuse module
names such as generate and run. Native integration classes retain their explicit
opt-in decorators and are reported as skipped. No build or database is started.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION_SELECTORS = (
    "AEROSTORE_CONTENTION_BINARY",
    "AEROSTORE_CONTENTION_DEFAULT_BINARY",
    "AEROSTORE_CONTENTION_PG_URL",
)


def test_directories(paths: list[str]) -> list[str]:
    """Select only tracked tests in the maintained Python suite roots."""
    selected = set()
    for name in paths:
        path = Path(name)
        if not path.name.startswith("test_") or path.suffix != ".py":
            continue
        if path.parent in (Path("scripts"), Path("scripts/tests")) or (
            path.parts and path.parts[0] in {"verification", "tools"}
        ):
            selected.add(str(path.parent))
    return sorted(selected)


def unit_environment(environment: dict[str, str]) -> dict[str, str]:
    result = dict(environment)
    for name in INTEGRATION_SELECTORS:
        result.pop(name, None)
    return result


def run_suites(root: Path, directories: list[str], environment: dict[str, str]) -> int:
    failed = []
    for directory in directories:
        print(f"::group::Python unit tests: {directory}", flush=True)
        command = [sys.executable, "-m", "unittest", "discover", "-s", directory,
                   "-p", "test_*.py", "-v"]
        result = subprocess.run(command, cwd=root, env=unit_environment(environment), check=False)
        print("::endgroup::", flush=True)
        if result.returncode:
            failed.append(directory)
    print(json.dumps({"directories": directories, "failed_directories": failed,
                      "native_integration_selected": False}, indent=2))
    return int(bool(failed))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--list", action="store_true", help="list selected directories without running tests")
    args = parser.parse_args()
    root = args.root.resolve()
    paths = subprocess.check_output(["git", "ls-files", "-z", "--", "scripts", "verification", "tools"],
                                    cwd=root).decode().split("\0")
    directories = test_directories(paths)
    if not directories:
        parser.error("no tracked Python unit suites found")
    if args.list:
        print(json.dumps(directories, indent=2))
        return 0
    return run_suites(root, directories, dict(os.environ))


if __name__ == "__main__":
    raise SystemExit(main())
