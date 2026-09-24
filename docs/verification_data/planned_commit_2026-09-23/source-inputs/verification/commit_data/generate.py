#!/usr/bin/env python3
"""Compose exact existing native posting and row-publication modules."""
from pathlib import Path
import argparse
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
CONTRACTS=HERE/'contracts.rs'
OUTPUT=HERE/'data.verus.rs'
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
_spec=importlib.util.spec_from_file_location('commit_data_ordinary',HERE/'ordinary_generate.py')
ordinary=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(ordinary)

def component(name):
    spec=importlib.util.spec_from_file_location('commit_data_'+name,ROOT/'verification'/name/'generate.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def render_module(source=None):
    source=SOURCE.read_text() if source is None else source
    spec=importlib.util.spec_from_file_location('commit_data_segment',HERE/'commit_segment.py')
    segment=importlib.util.module_from_spec(spec);spec.loader.exec_module(segment)
    template=CONTRACTS.read_text();marker='/* NATIVE_ORDINARY_COMMIT_DATA */'
    if template.count(marker)!=1:raise ValueError('native commit data marker changed')
    return template.replace(marker,segment.render(source))

def render():
    lookup=component('lookup');postings=component('postings');publication=component('row_publication')
    source=SOURCE.read_text()
    return ('// Generated single-index posting/row data composition.\n'
        +'pub mod lookup {\n'+lookup.render(source)+'\n}\n'
        +'pub mod postings {\n'+postings.render(source)+'\n}\n'
        +'pub mod publication {\n'+publication.render_module(source)+'\n}\n'
        +'pub mod ordinary {\n'+ordinary.render(source)+'\n}\n'
        +render_module())

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    expected=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=expected:raise SystemExit('stale commit data composition')
    else:OUTPUT.write_text(expected)
if __name__=='__main__':main()
