#!/usr/bin/env python3
"""Embed exact current source-bound components into the shared-clock bridge."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "scenario.rs"
OUTPUT = HERE / "scenario.verus.rs"
COMPONENTS = {"lifecycle": "LIFECYCLE_MODULE", "predicate": "PREDICATE_MODULE", "predicate_capture": "CAPTURE_MODULE"}


def component(name):
    spec = importlib.util.spec_from_file_location(name + "_scenario_adapter", ROOT / "verification" / name / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render(template=None):
    result = TEMPLATE.read_text() if template is None else template
    for name, marker in COMPONENTS.items():
        module = component(name)
        generated = module.render(module.SOURCE.read_text())
        if module.OUTPUT.read_text() != generated:
            raise ValueError("stale source-bound scenario component: " + name)
        placeholder = "/* " + marker + " */"
        if result.count(placeholder) != 1:
            raise ValueError("missing/duplicate component marker: " + name)
        result = result.replace(placeholder, generated)
    return "// Generated shared lifecycle/clock scenario; see generate.py.\n" + result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != rendered:
            raise SystemExit("stale shared lifecycle scenario")
    else:
        OUTPUT.write_text(rendered)


if __name__ == "__main__":
    main()
