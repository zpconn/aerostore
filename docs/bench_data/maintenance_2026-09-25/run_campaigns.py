#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,os,signal,subprocess,sys
base=Path(__file__).resolve().parent;binary=base/'benchmark-final'
receipt={'started_at':datetime.now(timezone.utc).isoformat(),'driver_pid':os.getpid(),'driver_start_ticks':Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'scope':'Sequential functional campaigns on shared host with concurrent formal/tests; accelerated maintenance, fixed population and complete batched sweeps. No comparative speed/capacity claim.','steps':[]}
sys.path.insert(0,str(Path.cwd()/'scripts'))
import qualify_hyperfeed as gate
receipt['binary_before_sha256']=gate.sha256(binary)
receipt['source_before']=gate.snapshot_sources()
assert receipt['binary_before_sha256']=='c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28'
assert receipt['source_before']['sha256']=='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
path=base/'campaign-execution.json'
def save():
 temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(receipt,indent=2)+'\n');temporary.replace(path)
def run(command,output,log_path):
 step={'command':command,'output':str(output),'started_at':datetime.now(timezone.utc).isoformat()};receipt['steps'].append(step);save()
 print('START_STEP',output.name,flush=True)
 with log_path.open('w') as log:
  process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,start_new_session=True)
  step['process_pid']=process.pid;save()
  try:
   for line in process.stdout:log.write(line);log.flush();print(line,end='',flush=True)
   step['exit_code']=process.wait()
  except KeyboardInterrupt:
   process.send_signal(signal.SIGINT)
   step['exit_code']=process.wait(timeout=60)
   step['interrupted']=True;save();raise
 step['finished_at']=datetime.now(timezone.utc).isoformat();save();print('STEP_EXIT',step['exit_code'],flush=True)
try:
 for dispatch,ttl in [('identity',0),('signature-affinity',600)]:
  label='identity-mixed' if dispatch=='identity' else 'affinity-mixed'
  for evidence in ['full','metrics']:
   output=base/(label+'-'+evidence)
   command=[sys.executable,'scripts/qualify_hyperfeed.py','--binary',str(binary),'--output',str(output),'--engines','aerostore,service-unix,postgres','--workload','calibrated','--maintenance-mode','sweep','--dispatch',dispatch,'--affinity-ttl-ms',str(ttl),'--signature-pattern','mixed','--rates','64','--workers','4','--families','16','--hot-percent','0','--seeds','20260924,20260925,20260926','--seconds','5','--projection-interval-seconds','1','--housekeeping-interval-seconds','2','--slo-ms','50','--evidence',evidence]
   if evidence=='metrics':command+=['--correctness-report',str(base/(label+'-full')/'campaign.json')]
   run(command,output,base/(label+'-'+evidence+'.log'))
 for projection,housekeeping in [(1,16),(16,64)]:
  label=f'sensitivity-p{projection}-h{housekeeping}-full'
  output=base/label
  command=[sys.executable,'scripts/qualify_hyperfeed.py','--binary',str(binary),'--output',str(output),'--engines','aerostore,service-unix,postgres','--workload','calibrated','--maintenance-mode','sweep','--projection-batch-size',str(projection),'--housekeeping-batch-size',str(housekeeping),'--dispatch','identity','--affinity-ttl-ms','0','--signature-pattern','mixed','--rates','64','--workers','4','--families','16','--hot-percent','0','--seeds','20260925','--seconds','5','--projection-interval-seconds','1','--housekeeping-interval-seconds','2','--slo-ms','50','--evidence','full']
  run(command,output,base/(label+'.log'))
 command=[sys.executable,'scripts/run_remote_contention.py','--binary',str(binary),'--output-dir',str(base/'loopback'),'--workload','calibrated','--maintenance-mode','sweep','--dispatch','signature-affinity','--affinity-ttl-ms','600','--signature-pattern','mixed','--seconds','3','--arrival-rate','64','--workers','4','--families','16','--hot-percent','0','--seed','20260925','--projection-interval-seconds','1','--housekeeping-interval-seconds','1','--evidence','full']
 run(command,base/'loopback',base/'loopback.log')
 receipt['completed']=True;receipt['passed']=all(s['exit_code']==0 for s in receipt['steps'])
except KeyboardInterrupt:
 receipt.update(completed=False,passed=False,interrupted=True)
finally:
 receipt['source_after']=gate.snapshot_sources();receipt['binary_after_sha256']=gate.sha256(binary)
 receipt['source_stable']=receipt['source_before']==receipt['source_after']
 receipt['passed']=receipt.get('passed',False) and receipt['source_stable'] and receipt['binary_after_sha256']==receipt['binary_before_sha256']
 receipt['finished_at']=datetime.now(timezone.utc).isoformat();save()
print('ALL_DONE',json.dumps({'passed':receipt['passed'],'steps':[(s['output'],s.get('exit_code')) for s in receipt['steps']]}),flush=True)
sys.exit(0 if receipt['passed'] else 1)
