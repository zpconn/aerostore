from pathlib import Path
import sys,json,subprocess,time,datetime,hashlib,threading
root=Path.cwd(); sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources
out=root/'target/retry-final-validation/guardrails'; out.mkdir(parents=True,exist_ok=True)
assert not (out/'report.json').exists(),'Do not overwrite an existing pilot'
build=json.loads((root/'target/retry-final-validation/build-provenance.json').read_text())
before=snapshot_sources(root); expected=sys.argv[1]
assert before==build['source_before'] and before['sha256']==expected
command=[sys.executable,'scripts/verify_formal.py','--profile','pilot','--output',str(out/'report.json')]
receipt={'command':command,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_before':before,'expected_source':expected,'completed':False,'passed':False}
(out/'execution-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
stop=threading.Event(); capture_errors=[]; capture_start=time.time()
def capture_lean():
    destination=out/'lean-generated'; stages={}; seen={}
    try:
        while not stop.is_set():
            for stage in (root/'target').glob('aerostore-lean-*'):
                if stage.is_dir() and (stage in stages or stage.stat().st_ctime>=capture_start):
                    stages[stage]=True
                    for path in stage.rglob('*'):
                        if path.suffix not in {'.rs','.lean','.llbc','.json'} or not path.is_file(): continue
                        try:
                            stat=path.stat(); key=(stat.st_mtime_ns,stat.st_size); rel=str(path.relative_to(stage))
                            if seen.get(rel)==key: continue
                            data=path.read_bytes(); dest=destination/rel
                            dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(data); seen[rel]=key
                        except FileNotFoundError: pass
            if stages and all(not stage.exists() for stage in stages): break
            stop.wait(.2)
        if stages:
            files={str(p.relative_to(destination)):hashlib.sha256(p.read_bytes()).hexdigest() for p in destination.rglob('*') if p.is_file()}
            capture={'original_temporary_roots':[str(p.relative_to(root)) for p in stages],'capture_scope':'Source/translation bytes preserved while unchanged Lean runner used its temporary scratch directory; compiled objects omitted. Compare source hashes with final Lean receipt.','stage_removed_by_runner':all(not p.exists() for p in stages),'captured_files':files}
            (destination/'capture.json').write_text(json.dumps(capture,indent=2)+'\n')
    except Exception as error: capture_errors.append(repr(error))
thread=threading.Thread(target=capture_lean,daemon=True); thread.start(); start=time.monotonic()
with (out/'pilot.log').open('w') as log:
    process=subprocess.run(command,cwd=root,stdout=log,stderr=subprocess.STDOUT)
stop.set(); thread.join(timeout=5)
after=snapshot_sources(root)
receipt.update({'completed':True,'returncode':process.returncode,'elapsed_seconds':time.monotonic()-start,'finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_after':after,'source_stable':before==after,'log_sha256':hashlib.sha256((out/'pilot.log').read_bytes()).hexdigest(),'lean_capture_errors':capture_errors,'passed':process.returncode==0 and before==after})
(out/'execution-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({k:v for k,v in receipt.items() if not k.startswith('source_')},indent=2))
raise SystemExit(0 if receipt['passed'] else 1)
