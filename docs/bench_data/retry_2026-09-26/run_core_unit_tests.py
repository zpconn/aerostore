from pathlib import Path
import datetime, hashlib, json, os, subprocess, sys, time
root=Path.cwd();sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources
out=root/'target/retry-final-validation/core-unit-tests';out.mkdir(parents=True,exist_ok=True)
assert not (out/'receipt.json').exists(),'Do not overwrite evidence'
commands=[
 ('core-default',['cargo','test','--offline','--locked','-p','aerostore_core','--test','contention_diagnostics']),
 ('core-feature',['cargo','test','--offline','--locked','-p','aerostore_core','--features','retry-diagnostics','--test','contention_diagnostics']),
 ('module-default',['cargo','test','--offline','--locked','-p','aerostore_core','--lib','retry_diagnostics']),
 ('module-feature',['cargo','test','--offline','--locked','-p','aerostore_core','--features','retry-diagnostics','--lib','retry_diagnostics']),
 ('default-projection',[sys.executable,'verification/retry_diagnostics/check_default.py']),
 ('normalizer',[sys.executable,'-m','unittest','discover','-s','verification/retry_diagnostics','-p','test_*.py']),
 ('concurrent-adapter',[sys.executable,'-m','unittest','discover','-s','verification/concurrent','-p','test_*.py']),
 ('p0',[sys.executable,'scripts/check_p0_contracts.py','--require-complete']),
 ('p0-tests',[sys.executable,'scripts/test_p0_contracts.py']),
 ('formal-gate-tests',[sys.executable,'scripts/test_formal_gate.py']),
]
receipt={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'completed':False,'passed':False,'source_before':snapshot_sources(root),'checks':[], 'scope':'Native default and optional diagnostic tests plus default-feature source normalization/API/gate tests. No feature-enabled formal proof or performance noninterference claim.'}
env=os.environ.copy();env.update(RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1',RUSTUP_NO_UPDATE_CHECK='1')
path=out/'receipt.json'
def save():path.write_text(json.dumps(receipt,indent=2)+'\n')
save()
for name,command in commands:
 start=time.monotonic();log=out/(name+'.log')
 with log.open('w') as stream:r=subprocess.run(command,cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT)
 check={'name':name,'command':command,'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-start,'passed':r.returncode==0,'log':str(log.relative_to(root)),'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
 receipt['checks'].append(check);save();print(name,r.returncode,flush=True)
 if r.returncode:break
receipt.update(completed=len(receipt['checks'])==len(commands),source_after=snapshot_sources(root))
receipt['source_stable']=receipt['source_before']==receipt['source_after']
receipt['passed']=receipt['completed'] and receipt['source_stable'] and all(c['passed'] for c in receipt['checks'])
receipt['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print(json.dumps({k:v for k,v in receipt.items() if not k.startswith('source_')},indent=2))
raise SystemExit(0 if receipt['passed'] else 1)
