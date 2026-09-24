#!/usr/bin/env python3
"""Embed current checked components and bind the composed native lookup stages."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
TEMPLATE = HERE / "slice.rs"
OUTPUT = HERE / "slice.verus.rs"
COMPONENTS = {"guard_ownership": "OWNERSHIP_MODULE", "lookup": "LOOKUP_MODULE",
              "predicate_capture": "CAPTURE_MODULE", "predicate": "PREDICATE_MODULE"}


def component(name):
    spec = importlib.util.spec_from_file_location(name + "_indexed_slice_adapter", ROOT / "verification" / name / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render(template=None, source=None):
    source = SOURCE.read_text() if source is None else source
    result = TEMPLATE.read_text() if template is None else template
    # The capture adapter checks the exact whole body around its loop, including
    # all binding/owner checks and acquire->capture->raw->drop->materialization.
    # The lookup adapter translates the actual materialization/read/visibility.
    for name, marker in COMPONENTS.items():
        module = component(name)
        generated = module.render(source) if name != "guard_ownership" else module.render()
        if source == SOURCE.read_text() and module.OUTPUT.read_text() != generated:
            raise ValueError("stale indexed-slice component: " + name)
        placeholder = "/* " + marker + " */"
        if result.count(placeholder) != 1:
            raise ValueError("missing/duplicate component marker: " + name)
        result = result.replace(placeholder, generated)
    return "// Generated source-bound one-bucket indexed-read/validation composition.\n" + result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit("stale indexed-read composition")
    else:
        OUTPUT.write_text(generated)


if __name__ == "__main__":
    main()
