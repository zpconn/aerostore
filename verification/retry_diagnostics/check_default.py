#!/usr/bin/env python3
"""Pin diagnostic call placement and default-build native token equivalence.

The reviewed baseline is a committed source revision recorded in the manifest.
A later semantic change may have an explicit, exact token transition from an
archived immutable source; it is never erased as diagnostic instrumentation.
Checking never reads mutable HEAD or treats this lexical check as a Rust proof.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path(__file__).with_name('default_sources.json')
CAPTURE_REFERENCE = '94ad54bef275dd4db0ce382569bc239e931ada87'
LEGACY_REFERENCE = '4da551bbc2a17614ae11795cfee7764cb252d56a'
LEGACY_MANIFEST_SHA256 = '90b4952ea89a54740cc8666f15673ef4ddc9947a307a9da746b50bd0e48473b4'
OCC = 'aerostore_core/src/occ_partitioned.rs'
WAL = 'aerostore_core/src/wal_writer.rs'
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
normalize = load('retry_default_normalizer', Path(__file__).with_name('normalize.py'))
lexer = load('retry_default_lexer', ROOT / 'verification/lookup_native/check_production_equivalence.py')


def digest(tokens):
    return hashlib.sha256(json.dumps(tokens, separators=(',', ':')).encode()).hexdigest()


def describe_tokens(source_tokens):
    tokens, sites = normalize.erase(source_tokens)
    for site in sites:
        pos = site['position']
        site['before'] = tokens[max(0, pos - 16):pos]
        site['after'] = tokens[pos:pos + 16]
    return {'default_tokens_sha256': digest(tokens), 'sites': sites}


def describe(text):
    return describe_tokens(lexer.rust_tokens(text))


def replace_once(tokens, before, after):
    before, after = lexer.rust_tokens(before), lexer.rust_tokens(after)
    positions = [i for i in range(len(tokens)) if tokens[i:i + len(before)] == before]
    if len(positions) != 1:
        raise ValueError('reviewed capture transition is missing or ambiguous')
    i = positions[0]
    return tokens[:i] + after + tokens[i + len(before):]


def capture_transition_tokens(reference):
    """Exactly two native edits and one test-only module; never a normalizer."""
    tokens = lexer.rust_tokens(reference)
    tokens = replace_once(tokens,
        'for bucket in &buckets { let stamp = index.transactional_stamp(*bucket)?;',
        'let prior_read_len = tx.index_reads.len(); '
        'for bucket in &buckets { let stamp = index.transactional_stamp(*bucket)?;')
    tokens = replace_once(tokens,
        'if let Some(previous) = tx.index_reads.iter().find('
        '|read| read.index_offset == index.header_offset() && read.bucket == *bucket) {',
        'let previous = if tx.index_reads.len() == prior_read_len {'
        'tx.index_reads.iter().find('
        '|read| read.index_offset == index.header_offset() && read.bucket == *bucket)'
        '} else { tx.index_reads[..prior_read_len].iter().find('
        '|read| read.index_offset == index.header_offset() && read.bucket == *bucket) } ;'
        'if let Some(previous) = previous {')
    tokens += lexer.rust_tokens('#[cfg(test)] mod capture_prefix_tests;')
    return tokens


def check_diagnostic_contexts(old_sites, new_sites):
    """Allow only the two exact window changes caused by hybrid search syntax."""
    reviewed = {
        ('LookupPostSnapshotStamp', 'after'): (
            'return Err(Error::SerializationFailure); } if let Some(previous) =',
            'return Err(Error::SerializationFailure); } let previous = if tx.index_reads'),
        ('LookupChangedCapturedStamp', 'before'): (
            'bucket) { if previous.stamp != stamp { tx.index_conflict = true;',
            '= previous { if previous.stamp != stamp { tx.index_conflict = true;'),
    }
    if len(old_sites) != len(new_sites):
        raise ValueError('capture transition changed the diagnostic site inventory')
    seen = set()
    for old, new in zip(old_sites, new_sites):
        expected = {k: v for k, v in old.items() if k != 'position'}
        for field in ('before', 'after'):
            key = (old['cause'], field)
            if key in reviewed:
                before, after = reviewed[key]
                if old[field] != lexer.rust_tokens(before):
                    raise ValueError('reviewed diagnostic reference context differs')
                expected[field] = lexer.rust_tokens(after)
                seen.add(key)
        if expected != {k: v for k, v in new.items() if k != 'position'}:
            raise ValueError('capture transition changed an unreviewed diagnostic context')
    if seen != set(reviewed):
        raise ValueError('reviewed diagnostic context site missing')
    return [{'cause': cause, 'field': field} for cause, field in sorted(seen)]


def check_transition(manifest, artifact_root=None):
    """Validate the new expectation against preserved source, not current HEAD."""
    artifact_root = MANIFEST.parent if artifact_root is None else artifact_root
    historical_bytes = (artifact_root / 'default_sources_4da551b.json').read_bytes()
    if hashlib.sha256(historical_bytes).hexdigest() != LEGACY_MANIFEST_SHA256:
        raise ValueError('historical diagnostic baseline changed')
    historical = json.loads(historical_bytes)
    transition = manifest.get('reviewed_transition')
    if transition is None:
        if manifest.get('baseline_commit') == LEGACY_REFERENCE:
            if manifest != historical:
                raise ValueError('legacy baseline must be the exact preserved historical manifest')
            return None
        raise ValueError('reviewed transition is required for this diagnostic baseline')
    if (not isinstance(transition, dict)
            or transition.get('kind') != 'prior_prefix_capture_hybrid_v2'
            or manifest.get('baseline_commit') != CAPTURE_REFERENCE):
        raise ValueError('unknown reviewed diagnostic baseline transition')
    if (transition.get('reference_artifact') != 'occ_partitioned_94ad54b.rs'
            or transition.get('historical_manifest') != 'default_sources_4da551b.json'):
        raise ValueError('reviewed capture artifacts have unexpected identities')
    if set(manifest['files']) != {OCC, WAL}:
        raise ValueError('reviewed capture baseline must cover exactly OCC and WAL')
    reference_path = artifact_root / 'occ_partitioned_94ad54b.rs'
    reference = reference_path.read_bytes()
    if hashlib.sha256(reference).hexdigest() != transition['reference_raw_sha256']:
        raise ValueError('immutable capture reference source changed')
    if hashlib.sha256(historical_bytes).hexdigest() != transition['historical_manifest_sha256']:
        raise ValueError('historical diagnostic baseline changed')
    if hashlib.sha256(Path(__file__).with_name('normalize.py').read_bytes()).hexdigest() != transition['normalizer_sha256']:
        raise ValueError('capture rebaseline must not change the diagnostic normalizer')
    if historical['baseline_commit'] != LEGACY_REFERENCE:
        raise ValueError('historical diagnostic baseline has the wrong immutable revision')
    old_occ = describe(reference.decode())
    if old_occ != historical['files'][OCC]['default_projection']:
        raise ValueError('immutable reference no longer preserves the original diagnostic baseline')
    expected_occ = describe_tokens(capture_transition_tokens(reference.decode()))
    if expected_occ != manifest['files'][OCC]['default_projection']:
        raise ValueError('OCC expectation is not the exact reviewed prior-prefix transition')
    if manifest['files'][OCC]['baseline_raw_sha256'] != transition['reference_raw_sha256']:
        raise ValueError('OCC immutable reference identity differs')
    if (manifest['files'][OCC].get('baseline_commit') != CAPTURE_REFERENCE
            or manifest['files'][WAL].get('baseline_commit') != historical['baseline_commit']):
        raise ValueError('per-file immutable baseline revisions differ')
    if manifest['files'][WAL]['default_projection'] != historical['files'][WAL]['default_projection']:
        raise ValueError('capture transition changed the WAL default projection or diagnostic site')
    # Rejecting branches/arguments stay exact; two neighboring search-syntax
    # windows change explicitly. This allowance never erases branch tokens.
    old_sites, new_sites = old_occ['sites'], expected_occ['sites']
    context_changes = check_diagnostic_contexts(old_sites, new_sites)
    return {'kind': transition['kind'], 'reference_commit': CAPTURE_REFERENCE,
            'reference_raw_sha256': transition['reference_raw_sha256'],
            'historical_baseline_commit': historical['baseline_commit'],
            'normalizer_unchanged': True, 'wal_projection_unchanged': True,
            'diagnostic_contexts_unchanged': False,
            'diagnostic_contexts_reviewed': True,
            'reviewed_diagnostic_context_changes': context_changes,
            'scope': 'Exact reviewed token transition; optimization remains in the default program. Not a semantic proof.'}


def check(root=ROOT, manifest=None):
    manifest_from_file = manifest is None
    manifest = json.loads(MANIFEST.read_text()) if manifest_from_file else manifest
    transition = check_transition(manifest)
    results = {}
    for path, expected in manifest['files'].items():
        actual = describe((root / path).read_text())
        if actual != expected['default_projection']:
            raise ValueError('default native tokens or reviewed diagnostic placement changed: ' + path)
        results[path] = {'default_tokens_sha256': actual['default_tokens_sha256'], 'sites': len(actual['sites'])}
    causes = [site['cause'] for value in manifest['files'].values() for site in value['default_projection']['sites']]
    if sorted(causes) != sorted(normalize.ARGS):
        raise ValueError('diagnostic origin inventory omits or duplicates a reviewed cause')
    inputs = {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in manifest['files']}
    dependencies = [Path(__file__), Path(normalize.__file__), Path(lexer.__file__),
                    MANIFEST.with_name('default_sources_4da551b.json')]
    if manifest_from_file:
        dependencies.append(MANIFEST)
    if transition is not None:
        dependencies.append(MANIFEST.with_name('occ_partitioned_94ad54b.rs'))
    inputs.update({str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in dependencies})
    result = {'passed': True, 'baseline_commit': manifest['baseline_commit'],
              'proof_feature_scope': 'retry-diagnostics disabled only', 'files': results,
              'input_sha256': inputs,
              'manifest_value_sha256': hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    if transition is not None:
        result['reviewed_transition'] = transition
    return result


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--output', type=Path); args = parser.parse_args()
    result = check()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
