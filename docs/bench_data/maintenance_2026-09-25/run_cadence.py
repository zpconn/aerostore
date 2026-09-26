from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,os,sys,hashlib
root=Path.cwd();out=root/'target/maintenance-final-validation';sys.path.insert(0,str(root/'scripts'))
from qualify_hyperfeed import snapshot_sources,sha256
before=snapshot_sources();build=json.loads((out/'build-provenance.json').read_text());assert before==build['source_before']
receipt={'started_at':datetime.now(timezone.utc).isoformat(),'scope':'Concurrent same-host functional validation, two real 300-second intervals, complete maintenance sweeps with identity routing as ordering control. Other validation may overlap. No capacity, comparative speed, retention plateau or physical MMHF claim.','source_before':before,'binary_sha256':sha256(out/'benchmark-final'),'commands':[],'exit_codes':{}}
processes=[]
for label,engine in [('cadence-native','aerostore'),('cadence-postgres','postgres')]:
 command=[sys.executable,'scripts/qualify_hyperfeed.py','--binary',str(out/'benchmark-final'),'--output',str(out/label),'--engines',engine,'--workload','calibrated','--families','16','--hot-percent','0','--workers','4','--rates','32','--seeds','20260925','--seconds','601','--projection-interval-seconds','300','--housekeeping-interval-seconds','300','--maintenance-mode','sweep','--slo-ms','50','--evidence','full','--max-messages','20000']
 receipt['commands'].append(command);log=(out/(label+'.log')).open('w');p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT);processes.append((label,p,log));print('START',label,p.pid,flush=True)
(out/'real-cadence-execution.json').write_text(json.dumps(receipt,indent=2)+'\n')
for label,p,log in processes:receipt['exit_codes'][label]=p.wait();log.close();print('DONE',label,receipt['exit_codes'][label],flush=True)
receipt.update(finished_at=datetime.now(timezone.utc).isoformat(),source_after=snapshot_sources());receipt['source_stable']=before==receipt['source_after'];receipt['passed']=receipt['source_stable'] and all(code==0 for code in receipt['exit_codes'].values());(out/'real-cadence-execution.json').write_text(json.dumps(receipt,indent=2)+'\n');assert receipt['passed'],receipt['exit_codes']
