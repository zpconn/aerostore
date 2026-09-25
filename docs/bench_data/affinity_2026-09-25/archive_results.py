"""Archive source-bound affinity checks; omit binaries and disposable storage."""
import datetime,gzip,hashlib,importlib.util,json,re,shutil
from pathlib import Path
ROOT=Path.cwd();SOURCE=ROOT/'target/affinity-final-validation';OLD=ROOT/'target/affinity-validation';DEST=ROOT/'docs/bench_data/affinity_2026-09-25';DEST.mkdir(parents=True,exist_ok=True)
spec=importlib.util.spec_from_file_location('gate',ROOT/'scripts/qualify_hyperfeed.py');gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
build=json.loads((SOURCE/'build-provenance.json').read_text());assert gate.snapshot_sources()==build['source_after'];assert gate.sha256(Path(build['binary']))==build['binary_sha256']
for name in ['identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics']:
 c=json.loads((SOURCE/name/'campaign.json').read_text());assert c['completed'] and c['passed'] and c['source_stable'];assert c['source_before']==c['source_after']==build['source_after'];assert c['binary_before_sha256']==c['binary_after_sha256']==build['binary_sha256']
for name in ['affinity-tests-execution.json','affinity-tests-review.json','unit-execution.json','campaign-review.json','implementation-review.json']:
 assert json.loads((SOURCE/name).read_text())['passed'],name
entries=[]
def archive(path,relative):
 size=path.stat().st_size;digest=gate.sha256(path);compressed=path.suffix in {'.json','.jsonl','.log'} and size>16384
 dest=DEST/(str(relative)+('.gz' if compressed else ''));assert not dest.exists(),dest;dest.parent.mkdir(parents=True,exist_ok=True)
 if compressed:
  with path.open('rb') as inp,dest.open('wb') as out:
   with gzip.GzipFile(fileobj=out,mode='wb',compresslevel=6,mtime=0,filename='') as gz:shutil.copyfileobj(inp,gz,1<<20)
 else:shutil.copyfile(path,dest)
 assert gate.sha256(path)==digest
 entries.append({'source':str(path.relative_to(ROOT)),'archive':str(dest.relative_to(DEST)),'original_sha256':digest,'archive_sha256':gate.sha256(dest),'original_bytes':size,'archive_bytes':dest.stat().st_size,'compression':'gzip' if compressed else None})
def tree(base,prefix):
 for path in sorted(base.rglob('*')):
  if not path.is_file() or path.suffix not in {'.json','.jsonl','.log','.c'}:continue
  if path.name=='private-config.json' or re.fullmatch(r'worker-\d+\.json',path.name):continue
  archive(path,prefix/path.relative_to(base))
for name in ['source.tar.gz','build-provenance.json','build-bound.jsonl','build.py','boundary-check.json','boundary-review.json','boundary-update.log','previous-boundary-check.json','previous-frozen-boundary.json','unit-execution.json','unit_checks.py','model-and-measurement-tests.log','affinity-tests-execution.json','affinity-tests-review.json','affinity-tests.log','implementation-review.json','campaign-execution.json','campaign-review.json','run_campaigns.py','review_campaigns.py','postgres-cleanup.json','cleanup_postgres.py','preservation-review.json','preserve_baseline.py','next-milestone-review.md','archive_results.py']:
 archive(SOURCE/name,Path(name))
for name in ['identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics','integration','python-checks','loopback']:
 tree(SOURCE/name,Path(name))
for path in sorted(SOURCE.glob('*.log')):
 if not any(e['source']==str(path.relative_to(ROOT)) for e in entries):archive(path,Path(path.name))
# Higher-load sensitivity is diagnostic: preserve failures and scoped review.
for name in ['sensitivity-identity-full','sensitivity-affinity-full']:
 if (SOURCE/name).exists():tree(SOURCE/name,Path(name))
for path in sorted(SOURCE.glob('*sensitivity*')):
 if path.is_file() and path.suffix in {'.json','.py'}:archive(path,Path(path.name))
# Preserve the failed compatibility assertion and authentic pre-fix regression evidence.
for name in ['build-provenance.json','source.tar.gz','affinity-tests-execution.json','affinity-tests.log','preserved-tests-execution.json','preserved-integration.log','preserved-pause.log','postgres-restart.json']:
 archive(OLD/name,Path('development')/name)
for name in ['integration','preserved-integration','preserved-pause']:
 tree(OLD/name,Path('development')/name)
manifest={'completed':True,'archived_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Bounded functional evidence, including deliberate negative controls and preserved development failure. Metrics histories remain unverified. Higher-load sensitivity is not capacity or a speed comparison. Guardrails have a separate manifest.','source_sha256':build['source_after']['sha256'],'binary_sha256':build['binary_sha256'],'files':entries,'omitted':['binaries','mmap arenas','WAL files','private worker configurations','PostgreSQL data','redundant pre-test-fix campaigns (final rerun archived)']}
(DEST/'artifact-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for entry in entries:
 path=DEST/entry['archive'];assert gate.sha256(path)==entry['archive_sha256'];digest=hashlib.sha256()
 with (gzip.open(path,'rb') if entry['compression'] else path.open('rb')) as stream:
  for block in iter(lambda:stream.read(1<<20),b''):digest.update(block)
 assert digest.hexdigest()==entry['original_sha256']
print(json.dumps({'archived_files':len(entries),'archive_bytes':sum(e['archive_bytes'] for e in entries),'all_original_and_archived_hashes_verified':True}))
