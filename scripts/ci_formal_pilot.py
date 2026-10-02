#!/usr/bin/env python3
"""Launch the component pilot, preserving the verification process exit status."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

PILOT_BOOTSTRAP = (
    "import runpy, sys; from pathlib import Path; "
    "script = Path(sys.argv.pop(1)).resolve(); "
    "sys.path.append(str(script.parent)); sys.argv[0] = str(script); "
    "runpy.run_path(str(script), run_name='__main__')"
)


def command(root: Path, baseline: str, mode: str) -> list[str]:
    if mode not in {"review", "experiment"}:
        raise ValueError("formal mode must be review or experiment")
    # Isolate Python from candidate PYTHONPATH/sitecustomize and put the script
    # helpers after stdlib paths. A new scripts/json.py cannot replace stdlib.
    result = [sys.executable, "-I", "-c", PILOT_BOOTSTRAP,
              str(root / "scripts/verify_formal.py"), "--profile", "pilot",
              "--output", "target/verification/ci/pilot/report.json"]
    if baseline:
        if not re.fullmatch(r"[0-9a-f]{40}", baseline) or baseline == "0" * 40:
            raise ValueError("pilot baseline must be a complete nonzero commit SHA")
        if mode != "experiment":
            raise ValueError("review-mode pilots check candidate lock consistency without a strict experiment baseline")
        result += ["--baseline-ref", baseline]
    elif mode == "experiment":
        raise ValueError("experiment pilot requires an independently checked baseline")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(os.environ.get("GITHUB_WORKSPACE", ".")))
    parser.add_argument("--baseline-ref", default=os.environ.get("FORMAL_BASE_SHA", ""))
    parser.add_argument("--mode", choices=("review", "experiment"), default=os.environ.get("FORMAL_MODE", "review"))
    args = parser.parse_args()
    root = args.root.resolve()
    # -I protects this child, but the verification runner also launches Python
    # subprocesses. Do not pass import/startup overrides on to those children.
    environment = {name: value for name, value in os.environ.items() if not name.startswith("PYTHON")}
    environment["PYTHONNOUSERSITE"] = "1"
    try:
        result = subprocess.run(command(root, args.baseline_ref, args.mode), cwd=root, env=environment)
    except (OSError, ValueError) as error:
        print(f"Cannot launch formal pilot: {error}", file=sys.stderr)
        return 1
    return result.returncode if result.returncode >= 0 else 128 - result.returncode


if __name__ == "__main__":
    sys.exit(main())
