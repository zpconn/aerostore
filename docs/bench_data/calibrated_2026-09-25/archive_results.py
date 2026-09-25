"""Archive completed, bounded test evidence without modifying original reports."""
import datetime
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil

ROOT = Path('/home/zpconn/code/aerostore')
SOURCE = ROOT / 'target/calibrated-validation'
DESTINATION = ROOT / 'docs/bench_data/calibrated_2026-09-25'
DESTINATION.mkdir(parents=True, exist_ok=True)
spec = importlib.util.spec_from_file_location('gate', ROOT / 'scripts/qualify_hyperfeed.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
provenance = json.loads((SOURCE / 'build-provenance.json').read_text())
assert gate.snapshot_sources() == provenance['source_after']
assert gate.sha256(Path(provenance['binary'])) == provenance['binary_sha256']

for name in ['accelerated-full', 'accelerated-metrics', 'cadence-native', 'cadence-postgres']:
    campaign = json.loads((SOURCE / name / 'campaign.json').read_text())
    assert campaign['completed'] and campaign['passed'] and campaign['source_stable'], name
    assert campaign['source_before'] == campaign['source_after'] == provenance['source_after']
    assert campaign['binary_before_sha256'] == campaign['binary_after_sha256'] == provenance['binary_sha256']
    assert all(t['assessment']['execution_valid'] for t in campaign['trials'])
assert json.loads((SOURCE / 'loopback/orchestration.json').read_text())['passed']
assert json.loads((SOURCE / 'preserved-stress/report.json').read_text())['passed']

entries = []
def archive(path, relative):
    original_size = path.stat().st_size
    original_digest = gate.sha256(path)
    compressed = path.suffix in {'.json', '.jsonl', '.log'} and original_size > 16_384 and path.name != 'final-review.json'
    destination = DESTINATION / (str(relative) + ('.gz' if compressed else ''))
    assert not destination.exists(), destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    if compressed:
        with path.open('rb') as source, destination.open('wb') as output:
            with gzip.GzipFile(fileobj=output, mode='wb', compresslevel=6, mtime=0, filename='') as zipped:
                shutil.copyfileobj(source, zipped, 1 << 20)
    else:
        shutil.copyfile(path, destination)
    assert gate.sha256(path) == original_digest, f'input changed during archive: {path}'
    entries.append({'source': str(path.relative_to(ROOT)),
                    'archive': str(destination.relative_to(DESTINATION)),
                    'original_sha256': original_digest,
                    'archive_sha256': gate.sha256(destination),
                    'original_bytes': original_size, 'archive_bytes': destination.stat().st_size,
                    'compression': 'gzip' if compressed else None})

top = ['build-provenance.json', 'source.tar.gz', 'build-bound.jsonl',
       'boundary-check.json', 'boundary-review.json', 'boundary-update.json',
       'previous-boundary-check.json', 'previous-frozen-boundary.json',
       'preserved-engine-and-extended.json', 'real-cadence-execution.json',
       'cadence-native.log', 'cadence-postgres.log',
       'integration-final-command.json', 'integration-final.log',
       'pause-tests.log', 'model-and-measurement-tests.log',
       'qualification-tests.log', 'remote-tests.log', 'python-unit-status.json',
       'final-review.json', 'cadence-review.json', 'preservation-review.json',
       'final_review.py', 'review_cadence.py', 'archive_results.py', 'postgres-restart.json', 'postgres-cleanup.json']
for name in top:
    archive(SOURCE / name, Path(name))

for name in ['accelerated-full', 'accelerated-metrics', 'cadence-native', 'cadence-postgres',
             'loopback', 'final-integration', 'fault-tests', 'preserved-stress']:
    directory = SOURCE / name
    for path in sorted(directory.rglob('*')):
        if not path.is_file() or path.suffix not in {'.json', '.jsonl', '.log', '.c'}:
            continue
        if path.name == 'private-config.json' or re.fullmatch(r'worker-\d+\.json', path.name):
            continue
        # Execution binaries, mappings, WAL files, credentials and PG data are
        # excluded. Per-run histories and expected failure evidence are retained.
        archive(path, path.relative_to(SOURCE))

manifest = {'completed': True, 'archived_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'scope': 'Raw completed benchmark/test evidence. Includes deliberate negative controls; metrics histories remain unverified. Guardrails have a separate manifest.',
            'source_sha256': provenance['source_after']['sha256'],
            'binary_sha256': provenance['binary_sha256'], 'files': entries,
            'omitted': ['binaries', 'mmap arenas', 'WAL files', 'private worker configurations',
                        'PostgreSQL data', 'earlier development attempts']}
(DESTINATION / 'artifact-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
for entry in entries:
    path = DESTINATION / entry['archive']
    assert gate.sha256(path) == entry['archive_sha256']
    digest = hashlib.sha256()
    with (gzip.open(path, 'rb') if entry['compression'] else path.open('rb')) as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            digest.update(block)
    assert digest.hexdigest() == entry['original_sha256']
print(json.dumps({'archived_files': len(entries), 'compressed_bytes': sum(e['archive_bytes'] for e in entries),
                  'all_original_and_archived_hashes_verified': True}))
