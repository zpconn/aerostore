#!/usr/bin/env python3
"""Embed the exact source-bound capture and predicate modules for composition."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "composition.rs"
OUTPUT = HERE / "composition.verus.rs"


def component(name):
    spec = importlib.util.spec_from_file_location(name + "_composition_adapter", ROOT / "verification" / name / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render():
    result = TEMPLATE.read_text()
    for name, marker in [("predicate_capture", "/* CAPTURE_MODULE */"), ("predicate", "/* PREDICATE_MODULE */")]:
        module = component(name)
        generated = module.render(module.SOURCE.read_text())
        if module.OUTPUT.read_text() != generated:
            raise ValueError("stale component source: " + name)
        if result.count(marker) != 1:
            raise ValueError("missing or duplicated module placeholder: " + name)
        result = result.replace(marker, generated)
    return "// Generated composition of exact source-bound proof modules.\n" + result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != result:
            raise SystemExit("stale composition artifact: run verification/predicate_composition/generate.py")
    else:
        OUTPUT.write_text(result)


if __name__ == "__main__":
    main()
