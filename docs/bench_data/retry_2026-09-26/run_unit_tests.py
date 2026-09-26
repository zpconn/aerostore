from pathlib import Path
import datetime,subprocess,json,sys,time,os
root=Path.cwd();out=root/'target/retry-final-validation/unit-tests';out.mkdir(exist_ok=True);sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources
before=snapshot_sources();steps=[]
commands=[['cargo','test','--offline','--locked','--release','-p','aerostore_core','--features','retry-diagnostics','--test','contention_expiry_policy','--test','contention_retry_diagnostics','--test','contention_diagnostics','--test','calibrated_contention_model','--test','contention_measurement','--test','contention_crucible_model','--','--test-threads=1'],['cargo','test','--offline','--locked','--release','-p','aerostore_core','--features','retry-diagnostics','--lib','retry_diagnostics','--','--test-threads=1']]
commands += [[sys.executable,'-m','unittest','discover','-s',directory,'-p',pattern] for directory,pattern in [('scripts','test_hyperfeed_qualification.py'),('scripts','test_run_remote_contention.py'),('scripts','test_formal_gate.py'),('verification/retry_diagnostics','test_normalize.py')]]
for number,command in enumerate(commands):
 start=time.monotonic();log=out/f'{number}.log'
 with log.open('w') as stream:r=subprocess.run(command,stdout=stream,stderr=subprocess.STDOUT,env={**os.environ,'RUST_MIN_STACK':'16777216'})
 steps.append({'command':command,'returncode':r.returncode,'elapsed_seconds':time.monotonic()-start,'log':str(log.relative_to(root))});print(number,r.returncode,flush=True)
after=snapshot_sources();receipt={'completed':True,'passed':before==after and all(s['returncode']==0 for s in steps),'source_before':before,'source_after':after,'steps':steps,'scope':'Feature-enabled focused native/fixture/worker/service/model tests and qualification negative controls; larger test-thread stack for imported large debug fixture, release build used.'}
(out.parent/'unit-tests-execution.json').write_text(json.dumps(receipt,indent=2)+'\n');raise SystemExit(0 if receipt['passed'] else 1)
