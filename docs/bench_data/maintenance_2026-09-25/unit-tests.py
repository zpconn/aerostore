from pathlib import Path
from datetime import datetime, timezone
import hashlib, importlib.util, json, os, subprocess, sys
root=Path.cwd(); base=root/'target/maintenance-final-validation'; out=base/'unit-tests'; out.mkdir(exist_ok=True)
spec=importlib.util.spec_from_file_location('gate',root/'scripts/qualify_hyperfeed.py');gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
source=gate.snapshot_sources();binary=base/'benchmark-final'
assert source['sha256']=='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
assert gate.sha256(binary)=='c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28'
receipt={'started_at':datetime.now(timezone.utc).isoformat(),'source_before':source,'binary_before_sha256':gate.sha256(binary),'compiler':subprocess.check_output(['rustc','-Vv'],text=True),'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),'steps':[],'scope':'Final-source unit and model checks. These tests do not establish throughput, physical MMHF deployment or complete production verification.'}
commands=[('rust-model-measurement',['cargo','test','--offline','--locked','-p','aerostore_core','--test','calibrated_contention_model','--test','contention_measurement']),('python-qualification',[sys.executable,'scripts/test_hyperfeed_qualification.py']),('python-remote',[sys.executable,'scripts/test_run_remote_contention.py'])]
for label,command in commands:
 step={'name':label,'command':command,'started_at':datetime.now(timezone.utc).isoformat(),'log':str(out/(label+'.log'))}
 with Path(step['log']).open('w') as log:
  process=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
 step.update(exit_code=process.returncode,finished_at=datetime.now(timezone.utc).isoformat(),log_sha256=gate.sha256(Path(step['log'])))
 receipt['steps'].append(step)
 (base/'unit-tests-execution.json').write_text(json.dumps(receipt,indent=2)+'\n')
 print(label,process.returncode,flush=True)
receipt['source_after']=gate.snapshot_sources();receipt['binary_after_sha256']=gate.sha256(binary)
receipt['source_stable']=receipt['source_before']==receipt['source_after'];receipt['passed']=receipt['source_stable'] and receipt['binary_before_sha256']==receipt['binary_after_sha256'] and all(s['exit_code']==0 for s in receipt['steps']);receipt['finished_at']=datetime.now(timezone.utc).isoformat()
(base/'unit-tests-execution.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'passed':receipt['passed'],'source_stable':receipt['source_stable']}),flush=True)
sys.exit(0 if receipt['passed'] else 1)
