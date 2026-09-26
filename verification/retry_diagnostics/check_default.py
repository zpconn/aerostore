#!/usr/bin/env python3
"""Pin diagnostic call placement and default-build native token equivalence.

The reviewed baseline is a committed source revision recorded in the manifest;
checking never reads mutable HEAD or treats this lexical check as a Rust proof.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path(__file__).with_name('default_sources.json')
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
normalize = load('retry_default_normalizer', Path(__file__).with_name('normalize.py'))
lexer = load('retry_default_lexer', ROOT / 'verification/lookup_native/check_production_equivalence.py')


def digest(tokens):
    return hashlib.sha256(json.dumps(tokens, separators=(',', ':')).encode()).hexdigest()


def describe(text):
    tokens, sites = normalize.erase(lexer.rust_tokens(text))
    for site in sites:
        pos = site['position']
        site['before'] = tokens[max(0, pos - 16):pos]
        site['after'] = tokens[pos:pos + 16]
    return {'default_tokens_sha256': digest(tokens), 'sites': sites}


def check(root=ROOT, manifest=None):
    manifest = json.loads(MANIFEST.read_text()) if manifest is None else manifest
    results = {}
    for path, expected in manifest['files'].items():
        actual = describe((root / path).read_text())
        if actual != expected['default_projection']:
            raise ValueError('default native tokens or reviewed diagnostic placement changed: ' + path)
        results[path] = {'default_tokens_sha256': actual['default_tokens_sha256'], 'sites': len(actual['sites'])}
    causes = [site['cause'] for value in manifest['files'].values() for site in value['default_projection']['sites']]
    if sorted(causes) != sorted(normalize.ARGS):
        raise ValueError('diagnostic origin inventory omits or duplicates a reviewed cause')
    return {'passed': True, 'baseline_commit': manifest['baseline_commit'], 'proof_feature_scope': 'retry-diagnostics disabled only', 'files': results}


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--output', type=Path); args = parser.parse_args()
    result = check()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
