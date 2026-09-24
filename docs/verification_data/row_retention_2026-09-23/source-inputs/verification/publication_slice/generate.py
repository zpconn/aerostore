#!/usr/bin/env python3
"""Embed exact current native modules; no hand-maintained algorithm copies."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "slice.rs"
OUTPUT = HERE / "slice.verus.rs"
COMPONENTS = {"predicate": "PREDICATE_MODULE", "guards": "GUARDS_MODULE", "lifecycle": "LIFECYCLE_MODULE"}


def component(name):
    spec = importlib.util.spec_from_file_location(name + "_slice_adapter", ROOT / "verification" / name / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render():
    result = TEMPLATE.read_text()
    for name, marker in COMPONENTS.items():
        module = component(name)
        generated = module.render(module.SOURCE.read_text())
        if module.OUTPUT.read_text() != generated:
            raise ValueError("stale component source: " + name)
        marker = "/* " + marker + " */"
        if result.count(marker) != 1:
            raise ValueError("missing/duplicated module marker: " + name)
        result = result.replace(marker, generated)
    return "// Generated checked native publication-slice composition.\n" + result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    output = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != output:
            raise SystemExit("stale native publication slice")
    else:
        OUTPUT.write_text(output)


if __name__ == "__main__":
    main()
