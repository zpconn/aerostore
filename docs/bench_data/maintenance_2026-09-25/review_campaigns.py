#!/usr/bin/env python3
"""Independent receipt accounting for final functional sweep campaigns.

Checks terminal query shape/cutoffs against raw history, not serializability:
the full Rust history oracle is the serializability checker. Metrics rows have
no operations and therefore do not gain a history proof from these checks.
"""
from collections import defaultdict, Counter
from datetime import datetime, timezone
import hashlib, json
from pathlib import Path
import sys
sys.path.insert(0,str(Path.cwd()/'scripts'))
import qualify_hyperfeed as gate
BASE=Path(__file__).resolve().parent
SOURCE='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
BINARY='c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28'
LABELS=['identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics','sensitivity-p1-h16-full','sensitivity-p16-h64-full']
review={'created_at':datetime.now(timezone.utc).isoformat(),'scope':'Shared-host functional evidence with concurrent guardrail/cadence runs. No speed, saturation, sustainable capacity, 10x or physical MMHF claim. Complete sweep means the terminal transaction observed its fixed-cutoff complete query empty, not atomic sweep isolation or absence of later eligible writes.', 'source_sha256':SOURCE,'binary_sha256':BINARY,'errors':[], 'campaigns':[], 'trials':[]}
errors=review['errors'];seeds=defaultdict(list);foregrounds=defaultdict(list)
def check(condition,message):
 if not condition:errors.append(message)
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def outcome(raw):
 return {name:(len(raw['outputs']) if name=='outputs' else int(raw['duplicate']) if name=='duplicate_messages' else int(raw[name])) for name in gate.OUTCOME_FIELDS}
def inspect_trial(label,t,policy,companions=frozenset()):
 name=f"{label}/{t['config']['engine']}/s{t['config']['seed']}"
 c=t['config'];r=t['report']['runs'][0]
 assessment=gate.assess_trial(t,policy,companions)
 check(assessment['execution_valid'],f'{name}: invalid execution {assessment["reasons"]}')
 check(not assessment['qualified_capacity_trial'] and not assessment['performance_passed'],f'{name}: incorrectly qualifies capacity')
 full=c['evidence']=='full'
 check(assessment['history_verified']==full,f'{name}: history scope wrong')
 check(assessment['correctness_companion_verified'],f'{name}: missing own/full companion correctness')
 check(not r['population_turnover_tested'],f'{name}: unsupported population turnover claim')
 case=Path(r['evidence_directory'])
 rows=[json.loads(line) for line in (case/'history.jsonl').read_text().splitlines()]
 initial=json.loads((case/'initial.json').read_text())
 initial_sha=digest(sorted(initial,key=lambda row:row['id']))
 seeds[c['seed']].append({'trial':name,'sha256':initial_sha,'rows':len(initial)})
 check(len(rows)==r['completed_transactions']==r['store_metrics']['commits'],f'{name}: raw rows/transactions/commits disagree')
 check(len({row['receipt']['message']['id'] for row in rows})==len(rows),f'{name}: duplicate committed transaction IDs')
 raw_counts=Counter(row['workload_class'] for row in rows)
 fg=[row for row in rows if row['workload_class']=='foreground']
 corpus_sha=digest(sorted([row['receipt']['message'] for row in fg],key=lambda message:message['id']))
 foregrounds[c['seed']].append({'trial':name,'sha256':corpus_sha,'messages':len(fg)})
 check(len(fg)==c['arrival_rate']*c['seconds'],f'{name}: incomplete foreground history')
 check(sum(row['job_completed'] for row in rows)==r['completed_messages'],f'{name}: finished jobs inflated/missing')
 grouped=defaultdict(list)
 for row in rows:
  body=row['receipt']['body']
  check(bool(body['operations'])==full,f'{name}: metrics/full operation retention mismatch')
  if row['workload_class']!='foreground':grouped[row['job_id']].append(row)
 check(set(grouped)=={j['job_id'] for j in r['maintenance_jobs']},f'{name}: raw/summarized sweep IDs differ')
 detailed=[]
 for job in r['maintenance_jobs']:
  jobname=f"{name}/job{job['job_id']}"
  batches=sorted(grouped[job['job_id']],key=lambda row:row['batch_index'])
  projection=job['class']=='projection'
  limit=c['projection_batch_size' if projection else 'housekeeping_batch_size']
  interval=c['projection_interval_seconds' if projection else 'housekeeping_interval_seconds']
  at=1700000000000000000+(job['job_ordinal']+1)*interval*1000000000
  cutoff=at if projection else at-3600*1000000000
  kind='GlobalProject' if projection else 'GlobalHousekeeping'
  field='at' if projection else 'before'
  query_name='GlobalDue' if projection else 'GlobalExpired'
  expected_query={query_name:{field:cutoff}}
  check([row['batch_index'] for row in batches]==list(range(job['batches'])),jobname+': missing/duplicate batch ordinal')
  totals=Counter();count=0
  for index,row in enumerate(batches):
   message=row['receipt']['message'];body=row['receipt']['body'];effects=outcome(body['outcome'])
   selected=effects['claimed_events' if projection else 'expired_records'];terminal=index==len(batches)-1
   check(message['kind']=={kind:{field:cutoff,'limit':limit}} and message['event_time']==at,jobname+': cutoff/limit changed during sweep')
   check(message['id']==8000000000+(job['job_id']-4000000000)*4096+index,jobname+': incorrect transaction ID')
   check(row['maintenance_terminal']==terminal==row['job_completed'],jobname+': terminal/completed flag mismatch')
   check(selected==0 if terminal else 1<=selected<=limit,jobname+': invalid selected row count')
   check(row['scheduled_ns']==job['scheduled_ns'] and row['job_ordinal']==job['job_ordinal'] and row['worker']==job['worker'],jobname+': job receipt identity changed')
   if full:
    operations=body['operations'];first=operations[0].get('Query',{})
    check(first.get('query')==expected_query,jobname+': incorrect complete query predicate')
    if terminal:check(operations==[{'Query':{'query':expected_query,'rows':[]}}],jobname+': terminal lacks exact complete empty query')
    else:check(selected==min(len(first.get('rows',[])),limit),jobname+': batch selected count differs from complete query result/limit')
   if terminal:check(all(value==0 for value in effects.values()),jobname+': empty terminal has effects')
   totals.update(effects);count+=selected
  check(count==job['processed_rows'] and dict(totals)==job['outcomes'],jobname+': raw effects differ from summary')
  check(sum(row['retries'] for row in batches)==job['retries'],jobname+': retry count mismatch')
  check(batches[0]['message_started_ns']==job['started_ns'] and batches[-1]['receipt']['finished']==job['finished_ns'] and batches[-1]['received_ns']==job['received_ns'],jobname+': timestamps do not cover entire job')
  detailed.append({'job_id':job['job_id'],'class':job['class'],'cutoff':cutoff,'batches':len(batches),'processed_rows':count,'terminal_empty_query_observed':full})
 summary={'name':name,'engine':c['engine'],'seed':c['seed'],'dispatch':c['dispatch'],'evidence':c['evidence'],'projection_batch_size':c['projection_batch_size'],'housekeeping_batch_size':c['housekeeping_batch_size'],'execution_valid':assessment['execution_valid'],'history_verified':assessment['history_verified'],'correctness_companion_verified':assessment['correctness_companion_verified'],'useful_work_passed':assessment.get('useful_work_passed'), 'foreground_stale_view_update_fraction':assessment.get('foreground_stale_view_update_fraction'),'completed_jobs':r['completed_messages'],'completed_transactions':len(rows),'committed_batches':sum(j['batches'] for j in r['maintenance_jobs']),'retries':r['retries'],'retry_causes':r['retry_causes'],'maintenance_jobs':detailed,'initial_sha256':initial_sha,'foreground_corpus_sha256':corpus_sha}
 review['trials'].append(summary)
for label in LABELS:
 p=BASE/label/'campaign.json';campaign=json.loads(p.read_text())
 check(campaign['passed'] and campaign['completed'] and campaign['source_stable'],label+': campaign incomplete/failed/changed')
 check(campaign['source_before']['sha256']==SOURCE==campaign['source_after']['sha256'],label+': source mismatch')
 check(campaign['binary_before_sha256']==BINARY==campaign['binary_after_sha256'],label+': binary mismatch')
 companions=set()
 if label.endswith('metrics'):
  own=json.loads((BASE/label.replace('metrics','full')/'campaign.json').read_text())
  companions={gate.key(t['config']) for t in own['trials'] if gate.assess_trial(t,campaign['policy'])['history_verified']}
 for t in campaign['trials']:
  check(t['source_before_sha256']==SOURCE==t['source_after_sha256'] and t['binary_before_sha256']==BINARY==t['binary_after_sha256'],label+': individual trial binding mismatch')
  inspect_trial(label,t,campaign['policy'],companions)
 review['campaigns'].append({'label':label,'trials':len(campaign['trials']),'passed':campaign['passed'],'source_stable':campaign['source_stable']})
check(len(review['trials'])==42,'expected 42 matrix/sensitivity trials')
review['initial_seed_groups']=dict(seeds)
review['foreground_seed_groups']=dict(foregrounds)
for seed,group in seeds.items():check(len({t['sha256'] for t in group})==1,f'seed{seed}: initial fixture differs across engines/dispatch/evidence/batch sizes')
for seed,group in foregrounds.items():check(len({t['sha256'] for t in group})==1,f'seed{seed}: offered foreground corpus differs across engines/dispatch/evidence/batch sizes')
# The TCP run uses a shorter corpus/housekeeping interval and is reviewed apart.
p=BASE/'loopback';m=json.loads((p/'orchestration.json').read_text());report=json.loads((p/'client-report.json').read_text())
check(m['passed'] and m['completed'] and m['server_resources_cleanly_drained'],'TCP loopback failed or not drained')
check(m['binary_sha256']==BINARY,'TCP binary differs')
check(not m['physical_hosts_independently_verified'] and m['topology']=='tcp_loopback','TCP loopback misstates physical host evidence')
r=report['runs'][0];jobs=r['maintenance_jobs'];hist=[json.loads(line) for line in (Path(r['evidence_directory'])/'history.jsonl').read_text().splitlines()]
check(report['passed'] and r['oracle_status']=='Valid','TCP full oracle failed')
check(r['completed_messages']==196 and len(hist)==r['completed_transactions']==r['store_metrics']['commits'],'TCP jobs/transactions/commits differ')
terminals=[row for row in hist if row.get('maintenance_terminal')]
check(len(jobs)==len(terminals)==4 and all(row['receipt']['body']['operations'][0]['Query']['rows']==[] for row in terminals),'TCP terminal query evidence incomplete')
review['loopback']={'passed':m['passed'],'oracle_status':r['oracle_status'],'completed_jobs':r['completed_messages'],'completed_transactions':len(hist),'terminal_empty_queries':len(terminals),'physical_mm_hf_evidence':False}
review['source_after_sha256']=gate.snapshot_sources()['sha256'];review['binary_after_sha256']=gate.sha256(BASE/'benchmark-final')
check(review['source_after_sha256']==SOURCE and review['binary_after_sha256']==BINARY,'final review source/binary binding changed')
review['passed']=not errors
review['history_review_scope']='Raw per-job transaction IDs/cutoffs/query shape/terminal and count/effect/retry/timing reconciliation, plus recorded Rust full-history oracle results. Does not independently reproduce the serial-history proof; metrics operations absent and history_verified remains false.'
gate.atomic_json(BASE/'campaign-review.json',review)
print(json.dumps({'passed':review['passed'],'trials':len(review['trials']),'errors':errors,'transactions':sum(t['completed_transactions'] for t in review['trials'])}))
sys.exit(0 if review['passed'] else 1)
