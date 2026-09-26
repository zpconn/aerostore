"""Preserve source-bound retry/index evidence, excluding disposable binaries/storage."""
from pathlib import Path
from datetime import datetime,timezone
import gzip,hashlib,json,re,shutil,sys
root=Path.cwd();live=root/'target/retry-final-validation';dev=live/'development';dest=root/'docs/bench_data/retry_2026-09-26';dest.mkdir(parents=True,exist_ok=True);sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources,sha256
build=json.loads((live/'build-provenance.json').read_text());assert snapshot_sources()==build['source_before']==build['source_after'];assert sha256(Path(build['binary']))==build['binary_sha256']
for name in ['process-review.json','unit-tests-execution.json','development-to-final.json','postgres-cleanup.json','controlled-review.json']:assert json.loads((live/name).read_text())['passed'],name
assert json.loads((live/'implementation-review.json').read_text())['review_passed']
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
for name in ['controlled-campaign','process-regressions','tcp-regression','unit-tests','development']:
 if (live/name).exists():tree(live/name,Path(name))
manifest={'completed':True,'archived_at':datetime.now(timezone.utc).isoformat(),'scope':'Optional retry observations and controlled native expiry policy experiments. All predeclared failures and functional negative controls retained. Pre-refresh regression sources differ only in proof receipt checker, its negative tests and boundary lock; default/feature executable bytes identical. Sequential trials run without owned build/proof workloads, with ordinary host interference and no CPU isolation. Metrics histories remain unverified; capacity/10x qualification remains false; default-feature component guardrails have their own manifest.','source_sha256':build['source_after']['sha256'],'binary_sha256':build['binary_sha256'],'files':entries,'omitted':['compiled binaries','mmap arenas','WAL files','private worker configurations','PostgreSQL data','tool caches']}
(dest/'artifact-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for entry in entries:
 path=dest/entry['archive'];assert sha256(path)==entry['archive_sha256'];digest=hashlib.sha256()
 with (gzip.open(path,'rb') if entry['compression'] else path.open('rb')) as stream:
  for block in iter(lambda:stream.read(1<<20),b''):digest.update(block)
 assert digest.hexdigest()==entry['original_sha256']
print(json.dumps({'passed':True,'files':len(entries),'original_bytes':sum(e['original_bytes'] for e in entries),'archive_bytes':sum(e['archive_bytes'] for e in entries)}))
