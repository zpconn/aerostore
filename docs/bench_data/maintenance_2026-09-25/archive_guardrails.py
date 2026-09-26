#!/usr/bin/env python3
"""Archive the completed maintenance milestone's existing component pilot."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tomllib

ROOT = Path.cwd()
LIVE = ROOT / 'target/maintenance-final-validation/guardrails'
DEST = ROOT / 'docs/bench_data/maintenance_2026-09-25/guardrails'
sys.path.insert(0, str(ROOT / 'scripts'))
from qualify_hyperfeed import snapshot_sources
import verify_formal


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


report = json.loads((LIVE / 'report.json').read_text())
execution = json.loads((LIVE / 'execution-receipt.json').read_text())
build = json.loads((ROOT / 'target/maintenance-final-validation/build-provenance.json').read_text())
assert report['completed'] and report['passed'] and report['source_stable']
assert len(report['checks']) == 71 and all(check['passed'] for check in report['checks'])
assert report['source_before'] == report['source_after']
assert execution['completed'] and execution['passed'] and execution['source_stable']
assert execution['source_before'] == execution['source_after'] == build['source_before'] == snapshot_sources(ROOT)
assert build['source_before'] == build['source_after']
assert execution['source_before']['sha256'] == '1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
assert verify_formal.source_fingerprint(ROOT) == report['source_after']
assert not report['full_P1_complete'] and not report['whole_engine_verified'] and not report['promotion_eligible']
main_sources = build['source_before']['files']
extras = sorted(report['source_before'].keys() - main_sources.keys())
assert len(main_sources) == 484 and len(extras) == 41
assert len(report['source_before']) == len(main_sources) + len(extras)
for relative in report['source_before'].keys() & main_sources.keys():
    assert report['source_before'][relative] == main_sources[relative]

lean = json.loads((LIVE / 'lean.json').read_text())
lean_capture = json.loads((LIVE / 'lean-generated/capture.json').read_text())
assert lean_capture['stage_removed_by_runner']
assert not execution['lean_capture_errors']
captured = set(lean_capture['captured_files'].values())
for relative, expected in lean_capture['captured_files'].items():
    assert digest(LIVE / 'lean-generated' / relative) == expected
expected_mutations = {value for item in lean['mutation_checks'] for name, value in item.items()
                      if name.endswith('_sha256')}
missing = expected_mutations - captured
save(LIVE / 'lean-generated/hash-validation.json', {
    'passed': not missing,
    'mutation_source_hashes': len(expected_mutations),
    'captured_hashes': len(captured),
    'missing_declared_mutation_hashes': sorted(missing),
    'scope': 'Captured Lean/Rust/extracted mutation source bytes match final runner receipt; compiled proof objects remain omitted.'})
assert not missing, missing
claims = tomllib.loads((ROOT / 'verification/claims.toml').read_text())['claims']
strict = verify_formal.collect_claim_evidence(claims, report['checks'], LIVE)
save(LIVE / 'strict-revalidation.json', {
    'passed': True, 'claim_evidence': strict,
    'scope': 'Current full live receipts validated before binary/cache omission; existing component proof scope only.'})

DEST.mkdir(parents=True, exist_ok=True)
assert not (DEST / 'manifest.json').exists(), 'Do not overwrite a completed archive'
for name in ('report.json', 'execution-receipt.json', 'pilot.log', 'strict-revalidation.json'):
    shutil.copy2(LIVE / name, DEST / name)
for source, name in (('boundary-review.json', 'boundary-review.json'),
                     ('boundary-check.json', 'boundary-check.json'),
                     ('previous-boundary-check.json', 'previous-boundary-rejection.json')):
    shutil.copy2(ROOT / 'target/maintenance-final-validation' / source, DEST / name)
save(DEST / 'source-fingerprint.json', {
    'before': report['source_before'], 'after': report['source_after'], 'source_stable': True,
    'main_source_archive': '../source.tar.gz', 'main_source_sha256': build['source_before']['sha256']})

allowed = {'.rs', '.log', '.stderr', '.stdout', '.toml', '.cfg', '.json', '.lock', '.tcl', '.patch',
           '.py', '.tar', '.gz', '.lean', '.tla', '.sh', '.txt', '.md', '.yml', '.yaml', '.csv', '.llbc'}
files, omitted = {}, []
for current, directories, names in os.walk(LIVE):
    directories[:] = sorted(directory for directory in directories
                            if directory not in {'target', 'cargo-target', '.lake', 'states', '__pycache__'}
                            and not directory.startswith(('production-build-', 'mutant-build-')))
    for name in sorted(names):
        path = Path(current) / name
        if path.is_symlink() or path.suffix not in allowed:
            omitted.append(str(path.relative_to(ROOT)))
            continue
        files['evidence/' + str(path.relative_to(LIVE))] = path
for relative in extras:
    path = ROOT / relative
    assert digest(path) == report['source_before'][relative]
    files['proof-inputs-extra/' + relative] = path
for name in ('boundary-review.json', 'boundary-check.json', 'previous-boundary-check.json', 'previous-frozen-boundary.json'):
    files['boundary/' + name] = ROOT / 'target/maintenance-final-validation' / name

archive = DEST / 'formal-evidence.tar.gz'
pathmap, first, expected = [], {}, {}
hardlinks = 0
with tarfile.open(archive, 'w:gz', compresslevel=6) as tar:
    for member, path in sorted(files.items()):
        sha, size = digest(path), path.stat().st_size
        expected[member] = {'sha256': sha, 'bytes': size}
        pathmap.append({'original': str(path.relative_to(ROOT)), 'archive_member': member,
                        'sha256': sha, 'bytes': size})
        info = tar.gettarinfo(str(path), arcname=member)
        info.uid = info.gid = 0
        info.uname = info.gname = ''
        key = sha, size
        if key in first:
            info.type, info.linkname, info.size = tarfile.LNKTYPE, first[key], 0
            tar.addfile(info)
            hardlinks += 1
        else:
            first[key] = member
            with path.open('rb') as stream:
                tar.addfile(info, stream)
verified = {}
with tarfile.open(archive, 'r|gz') as tar:
    for member in tar:
        if member.islnk():
            assert member.linkname in verified
            actual = verified[member.linkname]
        else:
            assert member.isfile()
            h, size = hashlib.sha256(), 0
            stream = tar.extractfile(member)
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                h.update(block)
                size += len(block)
            actual = {'sha256': h.hexdigest(), 'bytes': size}
        assert actual == expected[member.name], member.name
        verified[member.name] = actual
assert verified == expected
sha = digest(archive)
save(DEST / 'archive-path-map.json', {
    'format_version': 1, 'archive': archive.name, 'archive_sha256': sha,
    'original_artifact_root': str(LIVE.relative_to(ROOT)), 'files': pathmap,
    'source_bindings': {
        'main_archive': '../source.tar.gz',
        'main_archive_sha256': digest(ROOT / 'target/maintenance-final-validation/source.tar.gz'),
        'main_source_snapshot_sha256': build['source_before']['sha256'],
        'main_source_files': len(main_sources), 'extra_inputs': extras,
        'extra_archive_prefix': 'proof-inputs-extra/', 'full_proof_inputs': len(report['source_before'])}})
save(DEST / 'manifest.json', {
    'passed': True, 'scope': report['scope'], 'checks': len(report['checks']), 'source_stable': True,
    'source_inputs': len(report['source_before']), 'main_source_snapshot_sha256': build['source_before']['sha256'],
    'p0_complete': report['p0_complete'], 'full_P1_complete': False, 'whole_engine_verified': False,
    'promotion_eligible': False, 'anchoring': report['anchoring'],
    'archive_sha256': sha, 'archive_bytes': archive.stat().st_size,
    'archived_file_count': len(files), 'verified_archive_members': len(verified),
    'tar_hardlink_count': hardlinks, 'unique_content_file_count': len(first),
    'artifact_sha256': {key: value['sha256'] for key, value in expected.items()},
    'input_sha256': report['source_before'],
    'omissions': 'Compiled binaries, Cargo targets, TLC state storage and Lean build caches omitted. Complete live proof/native receipt validation passed before archival. Reduced archive cannot rerun validators that require omitted binaries. Raw logs, receipts, generated proof/mutation source and native source snapshots retained.',
    'other_excluded_paths': omitted,
    'standalone_sha256': {path.name: digest(path) for path in DEST.iterdir()
                          if path.is_file() and path.name not in {'manifest.json', 'README.md', 'formal-evidence.tar.gz'}}})
assert verify_formal.source_fingerprint(ROOT) == report['source_after']

(DEST / 'README.md').write_text(f'''The current-source component pilot completed **{len(report['checks'])}/{len(report['checks'])} checks**. The [raw report](report.json) records identical before/after hashes for **{len(report['source_before'])} proof, test and gate inputs**. The separate [execution receipt](execution-receipt.json) anchors the same frozen {len(main_sources)}-file benchmark/source snapshot `{build['source_before']['sha256']}` before and after the run. P0 contract coverage is complete; full P1, whole-engine verification and architecture promotion remain false.

This preserves the existing component proofs, finite TLA models, native mutation controls, regressions and all three deterministic Extended Crucible feature configurations while complete batched maintenance sweeps are added. It introduces no new formal claims and does not formally prove the new sweep scheduler, complete HyperFeed compatibility, worker-failure availability or a speedup. New maintenance model/process tests and observed serial histories are separate evidence in the [maintenance campaign](../README.md). Functional campaigns ran concurrently with this pilot, so their latency is not a controlled performance comparison.

The [boundary review](boundary-review.json) identifies benchmark/model/measurement changes and tests, retaining the production engine and original Extended Crucible. The [previous boundary rejection](previous-boundary-rejection.json) is preserved; an explicitly reviewed local boundary refresh now [passes](boundary-check.json). The report remains **local_bootstrap_only**, not acceptance by an independent baseline checker.

The [compressed evidence](formal-evidence.tar.gz) retains raw receipts, logs, generated proof and mutation sources, native source snapshots and boundary snapshots. The [manifest](manifest.json) and [path map](archive-path-map.json) give every logical member's SHA-256 and original path. Identical bytes use relative tar hardlinks; every logical member was verified after creation. All {len(report['source_before'])} source bindings can be reconstructed from the root [{len(main_sources)}-file source bundle](../source.tar.gz), the {len(extras)} additional proof inputs in this archive, and the [full source fingerprint](source-fingerprint.json).

The Lean runner normally deletes temporary extraction/mutation sources. This run additionally captured {len(lean_capture['captured_files'])} source/translation files before deletion: all {len(expected_mutations)} mutation source/extraction hashes declared by its final receipt match the captured bytes. Capture and hash-validation receipts are under `evidence/lean-generated/` in the archive. Lean's {len(lean['mutation_checks'])} negative controls, including the extraction-dependent controls, retain their exact reported scope.

[Strict receipt validation](strict-revalidation.json) ran against complete live evidence before archiving. Compiled native/test/tool binaries, Cargo targets, TLC states and Lean build caches are omitted. The reduced archive cannot rerun validators requiring those binaries; tool identities, source bindings, outcomes and diagnostic counterexamples remain recorded.

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot \\
  --output target/maintenance-final-validation/guardrails/report.json
```

Use a fresh output directory when repeating the command so this evidence is preserved. The [pilot log](pilot.log) records the exact component commands and outcomes.
''')
print(json.dumps({'passed': True, 'archive': str(archive.relative_to(ROOT)), 'files': len(files),
                  'hardlinks': hardlinks, 'bytes': archive.stat().st_size, 'source_stable': True}))
