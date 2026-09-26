#!/usr/bin/env python3
"""Source-bound loopback check of diagnostic and partial-index peer agreement."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
ROOT=Path(__file__).resolve().parents[2]
BASE=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'scripts'))
import qualify_hyperfeed as gate
out=BASE/'tcp-regression'
out.mkdir(exist_ok=False)
binary=BASE/'benchmark-final'
source=gate.snapshot_sources()
build=json.loads((BASE/'build-provenance.json').read_text())
assert source==build['source_before']==build['source_after']
assert gate.sha256(binary)==build['binary_sha256']
command=[sys.executable,str(ROOT/'scripts/run_remote_contention.py'),'--binary',str(binary),'--output-dir',str(out/'case'),
 '--workload','calibrated','--maintenance-mode','sweep','--expiry-index','housekeeping','--retry-diagnostics','on',
 '--dispatch','signature-affinity','--affinity-ttl-ms','600','--signature-pattern','mixed',
 '--seconds','3','--workers','4','--families','16','--arrival-rate','64','--hot-percent','0',
 '--projection-interval-seconds','1','--housekeeping-interval-seconds','1','--evidence','full',
 '--max-messages','1000','--max-backlog','1000','--shm-mib','256','--timeout','45']
receipt={'command':command,'created_at':datetime.now(timezone.utc).isoformat(),'source_before':source,
 'binary_before_sha256':gate.sha256(binary),'passed':False,
 'scope':'Functional local TCP loopback, same host; no physical network or performance claim.'}
start=time.monotonic()
try:
 with (out/'run.log').open('w') as log:
  result=subprocess.run(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,timeout=60)
 receipt['exit_code']=result.returncode
 manifest=json.loads((out/'case/orchestration.json').read_text())
 report=json.loads((out/'case/client-report.json').read_text())
 run=report['runs'][0]
 checks={'process_succeeded':result.returncode==0,'helper_passed':manifest.get('passed') is True,
  'loopback_only':manifest['topology']=='tcp_loopback','clean_drain':manifest.get('server_resources_cleanly_drained') is True,
  'full_history_verified':run.get('correctness_history_verified') is True,
  'complete_sweep':run.get('global_maintenance_sweep_complete') is True,
  'job_count':run.get('completed_messages')==196,
  'transaction_count':run['completed_transactions']==run['store_metrics']['commits'],
  'expiry_selection':report['config']['expiry_index_policy']=='housekeeping' and run['effective_expiry_index_policy']=='housekeeping',
  'diagnostic_selection':report['config']['retry_diagnostics'] is True,
  'retry_trace_validation':not gate.experiment_report_errors(run,report['config'])}
 receipt.update(checks=checks,completed=True,passed=all(checks.values()),
                completed_jobs=run['completed_messages'],completed_transactions=run['completed_transactions'],
                retries=run['retries'],trace_errors=gate.experiment_report_errors(run,report['config']))
except Exception as error:
 receipt['error']=repr(error)
finally:
 receipt.update(source_after=gate.snapshot_sources(),binary_after_sha256=gate.sha256(binary),elapsed_seconds=time.monotonic()-start)
 receipt['source_stable']=receipt['source_before']==receipt['source_after'] and receipt['binary_before_sha256']==receipt['binary_after_sha256']
 receipt['passed']=receipt['passed'] and receipt['source_stable']
 (out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({key:receipt.get(key) for key in ['passed','source_stable','checks','error']}),flush=True)
raise SystemExit(0 if receipt['passed'] else 1)
