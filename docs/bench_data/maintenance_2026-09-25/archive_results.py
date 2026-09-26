"""Preserve source-bound maintenance evidence, excluding disposable binaries/storage."""
from pathlib import Path
from datetime import datetime,timezone
import gzip,hashlib,json,re,shutil,sys
root=Path.cwd();live=root/'target/maintenance-final-validation';dev=root/'target/maintenance-development';dest=root/'docs/bench_data/maintenance_2026-09-25';dest.mkdir(parents=True,exist_ok=True);sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources,sha256
build=json.loads((live/'build-provenance.json').read_text());assert snapshot_sources()==build['source_before']==build['source_after'];assert sha256(Path(build['binary']))==build['binary_sha256']
for name in ['campaign-review.json','cadence-review.json','implementation-review.json','unit-tests-execution.json','postgres-cleanup.json']:assert json.loads((live/name).read_text())['passed'],name
entries=[]
def archive(path,relative):
 size=path.stat().st_size;digest=sha256(path);compressed=path.suffix in {'.json','.jsonl','.log','.txt'} and size>16384;target=dest/(str(relative)+('.gz' if compressed else ''));assert not target.exists(),target;target.parent.mkdir(parents=True,exist_ok=True)
 if compressed:
  with path.open('rb') as inp,target.open('wb') as out:
   with gzip.GzipFile(fileobj=out,mode='wb',compresslevel=6,mtime=0,filename='') as stream:shutil.copyfileobj(inp,stream,1<<20)
 else:shutil.copyfile(path,target)
 assert sha256(path)==digest
 entries.append({'source':str(path.relative_to(root)),'archive':str(target.relative_to(dest)),'original_sha256':digest,'archive_sha256':sha256(target),'original_bytes':size,'archive_bytes':target.stat().st_size,'compression':'gzip' if compressed else None})
def tree(base,prefix):
 for path in sorted(base.rglob('*')):
  if not path.is_file() or path.is_symlink() or '__pycache__' in path.parts or path.suffix not in {'.json','.jsonl','.log','.c','.py','.md','.gz'}:continue
  if path.name=='private-config.json' or re.fullmatch(r'worker-\d+\.json',path.name):continue
  archive(path,prefix/path.relative_to(base))
# Explicit trees prevent accidental archival of tool caches or live server data.
for path in sorted(live.iterdir()):
 if path.is_file() and path.suffix in {'.json','.jsonl','.log','.py','.md','.gz'}:archive(path,Path(path.name))
for name in ['identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics','sensitivity-p1-h16-full','sensitivity-p16-h64-full','loopback','cadence-native','cadence-postgres','unit-tests','sensitivity-identity-full','sensitivity-affinity-full']:
 if (live/name).exists():tree(live/name,Path(name))
# Real-process checks ran on identical executable bytes before the reviewed lock refresh.
for name in ['source.tar.gz','build-provenance.json','build-bound.jsonl','build.py','maintenance-tests-execution.json','maintenance-tests.log']:
 archive(dev/name,Path('development')/name)
for name in ['integration','preserved-tests']:tree(dev/name,Path('development')/name)
manifest={'completed':True,'archived_at':datetime.now(timezone.utc).isoformat(),'scope':'Complete maintenance job functional evidence; deliberate negative controls and all higher-load failures retained. Real-process regression source differs final only in reviewed boundary lock, with identical executable bytes. Metrics histories remain unverified. Shared-host runs do not qualify capacity/performance; component guardrails have their own manifest.','source_sha256':build['source_after']['sha256'],'binary_sha256':build['binary_sha256'],'files':entries,'omitted':['compiled binaries','mmap arenas','WAL files','private worker configurations','PostgreSQL data','tool caches']}
(dest/'artifact-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for entry in entries:
 path=dest/entry['archive'];assert sha256(path)==entry['archive_sha256'];digest=hashlib.sha256()
 with (gzip.open(path,'rb') if entry['compression'] else path.open('rb')) as stream:
  for block in iter(lambda:stream.read(1<<20),b''):digest.update(block)
 assert digest.hexdigest()==entry['original_sha256']
print(json.dumps({'passed':True,'files':len(entries),'original_bytes':sum(e['original_bytes'] for e in entries),'archive_bytes':sum(e['archive_bytes'] for e in entries)}))
