#!/usr/bin/env python3
"""Compose source-derived storage operations over one shared lookup vocabulary."""
from pathlib import Path
import argparse
import importlib.util
ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / 'slice.rs'
OUTPUT = HERE / 'slice.verus.rs'


def component(name):
    spec = importlib.util.spec_from_file_location('storage_slice_' + name, ROOT / 'verification' / name / 'generate.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render():
    lookup = component('lookup')
    publication = component('row_publication')
    retention = component('row_retention')
    lifecycle = component('lifecycle')
    initialization = component('row_initialization')
    return ('// Generated native storage/publication/retention composition.\n'
        + 'pub mod lookup {\n' + lookup.render() + '\n}\n'
        + 'pub mod publication {\n' + publication.render_module() + '\n}\n'
        + 'pub mod retention {\n' + retention.render_module() + '\n}\n'
        + 'pub mod lifecycle {\n' + lifecycle.render(lifecycle.SOURCE.read_text()) + '\n}\n'
        + 'pub mod initialization {\n' + initialization.render_module() + '\n}\n'
        + TEMPLATE.read_text())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    result = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != result:
            raise SystemExit('stale storage slice')
    else:
        OUTPUT.write_text(result)

if __name__ == '__main__':
    main()
