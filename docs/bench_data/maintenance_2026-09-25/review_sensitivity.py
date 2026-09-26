#!/usr/bin/env python3
"""Failure-aware review of all twelve higher-load sweep diagnostic cells."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, re, sys
sys.path.insert(0,str(Path.cwd()/'scripts'))
import qualify_hyperfeed as gate
BASE=Path(__file__).resolve().parent
SOURCE='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
BINARY='c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28'
errors=[];results=[];observed_inputs={};campaigns=[]
def check(condition,message):
 if not condition:errors.append(message)
def sha(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
for label in ('sensitivity-identity-full','sensitivity-affinity-full'):
 cp=BASE/label/'campaign.json';campaign=json.loads(cp.read_text())
 check(campaign['completed'] and campaign['source_stable'],label+': completion/source changed')
 check(campaign['source_before']['sha256']==SOURCE==campaign['source_after']['sha256'],label+': source mismatch')
 check(campaign['binary_before_sha256']==BINARY==campaign['binary_after_sha256'],label+': binary mismatch')
 check(len(campaign['trials'])==6,label+': expected six trials')
 campaigns.append({'label':label,'completed':campaign['completed'],'passed':campaign['passed'],'sha256':gate.sha256(cp)})
 for t in campaign['trials']:
  c=t['config'];r=t['report']['runs'][0];a=gate.assess_trial(t,campaign['policy']);name=f"{c['dispatch']}/{c['engine']}/r{c['arrival_rate']}"
  check(t['source_before_sha256']==SOURCE==t['source_after_sha256'] and t['binary_before_sha256']==BINARY==t['binary_after_sha256'],name+': individual binding changed')
  check(not a['qualified_capacity_trial'] and not a['capacity_failure'],name+': unsupported capacity claim')
  root=Path(r['evidence_directory']);offered=json.loads((root/'offered-schedule.json').read_text());corpus=gate.calibrated_corpus(c)
  check(offered['offered_messages']==corpus['total'] and offered['offered_by_worker']==corpus['worker_counts'],name+': offered corpus mismatch')
  committed_ids=set();groups=defaultdict(list);class_counts=defaultdict(Counter);job_by_worker=[0]*6;transactions_by_worker=[0]*6
  observed_fg={};flight_intervals=defaultdict(list);hashobj=hashlib.sha256()
  with (root/'history.jsonl').open('rb') as stream:
   for raw in stream:
    hashobj.update(raw);row=json.loads(raw);receipt=row['receipt'];message=receipt['message'];body=receipt['body'];outcome=body['outcome'];mid=message['id'];cls=row['workload_class']
    check(mid not in committed_ids,name+': duplicate transaction');committed_ids.add(mid)
    check(bool(body['operations']),name+': full diagnostic omitted operations')
    transactions_by_worker[row['worker']]+=1;job_by_worker[row['worker']]+=int(row['job_completed'])
    class_counts[cls].update({'received_transactions':1,'received_successful_transaction_retries':row['retries'],'completed_jobs':int(row['job_completed']),'ignored_stale_views':outcome['ignored_stale'],'updated_views':outcome['updated_views'],'missing_family':int(outcome['missing_family']),'allocation_deferred':int(outcome['allocation_deferred'])})
    if cls=='foreground':
     q=mid-1000000;identity=q%12;ordinal=q//12;alias=ordinal//2%3
     check(0<=q<c['arrival_rate']*5,name+': foreground ID outside offered input')
     check(row['scheduled_ns']-offered['admission_started_ns']==q*1000000000//c['arrival_rate'],name+': foreground arrival changed')
     check((message['callsign'],message['tail'])==(0 if alias==2 else 100+identity//4,0 if alias==1 else 10000+identity),name+': foreground alias changed')
     check(row['job_completed'] is True,name+': foreground completion missing')
     observed_fg[mid]=message
     flight_intervals[identity].append((ordinal,row['message_started_ns'],receipt['finished']))
    else:
     jobid=row['job_id'];projection=cls=='projection';bit=0 if projection else 1;ordinal=(jobid-4000000000-bit)//2
     check(jobid==4000000000+ordinal*2+bit and 0<=ordinal<corpus[cls],name+': maintenance job ID outside offered ticks')
     check(row['worker']==c['workers']+bit and row['job_ordinal']==ordinal,name+': maintenance worker/ordinal changed')
     limit=c['projection_batch_size' if projection else 'housekeeping_batch_size'];period=c[cls+'_interval_seconds'];at=1700000000000000000+(ordinal+1)*period*1000000000
     cutoff=at if projection else at-3600*1000000000;field='at' if projection else 'before';kind='GlobalProject' if projection else 'GlobalHousekeeping';query={'GlobalDue' if projection else 'GlobalExpired':{field:cutoff}}
     check(message['kind']=={kind:{field:cutoff,'limit':limit}} and message['event_time']==at,name+': maintenance cutoff/limit changed')
     check(row['scheduled_ns']==offered['admission_started_ns']+(ordinal+1)*period*1000000000,name+': maintenance admission changed')
     check(mid==8000000000+(jobid-4000000000)*4096+row['batch_index'],name+': maintenance batch ID changed')
     terminal=row['maintenance_terminal'];selected=outcome['claimed_events' if projection else 'expired_records'];ops=body['operations'];first=ops[0].get('Query',{})
     check(first.get('query')==query and selected==min(limit,len(first.get('rows',[]))),name+': selection differs from fixed-cutoff complete query result')
     check(row['job_completed']==terminal,name+': committed batch incorrectly counted as job')
     if terminal:check(selected==0 and ops==[{'Query':{'query':query,'rows':[]}}],name+': completed job lacks terminal empty query')
     else:check(1<=selected<=limit,name+': nonterminal batch empty/oversize')
     groups[jobid].append({'index':row['batch_index'],'terminal':terminal,'selected':selected})
     class_counts[cls].update({'processed_rows':selected,'terminal_empty_queries':int(terminal),'nonempty_batches':int(not terminal)})
  for jobid,batches in groups.items():
   batches.sort(key=lambda b:b['index'])
   check([b['index'] for b in batches]==list(range(len(batches))),name+': received job batch history is not contiguous prefix')
   check(all(not b['terminal'] for b in batches[:-1]),name+': received batches continue after terminal')
  overlaps=reorders=0
  for lane in flight_intervals.values():
   lane.sort();maxfinish=0
   for ordinal,start,finish in lane:
    reorders+=finish<maxfinish;maxfinish=max(maxfinish,finish)
    overlaps+=any(other!=ordinal and otherstart<finish and start<otherfinish for other,otherstart,otherfinish in lane)
  reference=observed_inputs.setdefault(c['arrival_rate'],{})
  for mid,message in observed_fg.items():
   check(mid not in reference or reference[mid]==message,name+': shared observed foreground input differs across engines/dispatch')
   reference[mid]=message
  valid=a['execution_valid'];fault=r.get('error');cause='none';failed_job=None
  if fault and 'offered backlog' in fault:cause='admitted_backlog_bound_exceeded'
  elif fault and 'after 128 retries' in fault:
   cause='transaction_retry_budget_exhausted';match=re.search(r'message (\d+) after (\d+) retries',fault)
   if match:
    mid=int(match[1])
    if mid>=8000000000:
     offset,index=divmod(mid-8000000000,4096);ordinal,bit=divmod(offset,2);kind='housekeeping' if bit else 'projection'
     failed_job={'transaction_id':mid,'job_id':4000000000+offset,'batch_index':index,'class':kind,'job_ordinal':ordinal,'scheduled_offset_seconds':(ordinal+1)*c[kind+'_interval_seconds'],'retry_limit':int(match[2])}
    else:failed_job={'transaction_id':mid,'class':'foreground','retry_limit':int(match[2])}
  elif not valid:cause='other_execution_or_evidence_failure'
  if failed_job and failed_job.get('class') in {'projection','housekeeping'}:
   prior=groups.get(failed_job['job_id'],[])
   failed_job.update(received_prior_committed_batches=len(prior),
                     received_prior_processed_rows=sum(batch['selected'] for batch in prior),
                     terminal_received_for_failed_job=any(batch['terminal'] for batch in prior),
                     failure_position='first_batch' if failed_job['batch_index']==0 else 'after_prior_committed_batches')
  if valid:
   check(r['oracle_status']=='Valid' and sum(job_by_worker)==corpus['total'],name+': complete valid corpus mismatch')
   check(len(committed_ids)==r['completed_transactions']==r['store_metrics']['commits'],name+': complete transaction count mismatch')
   check(job_by_worker==r['completed_by_worker'],name+': complete job-by-worker count mismatch')
  else:
   check(r.get('oracle_status') is None and not a['history_verified'],name+': incomplete history incorrectly claims verdict')
  progresspath=root/'progress.json';progress=json.loads(progresspath.read_text()) if progresspath.exists() else None
  paths=[cp,Path(t['directory'])/'report.json',Path(t['directory'])/'run.log',root/'offered-schedule.json']
  if progresspath.exists():paths.append(progresspath)
  hashes={str(p.relative_to(BASE)):gate.sha256(p) for p in paths};hashes[str((root/'history.jsonl').relative_to(BASE))]=hashobj.hexdigest()
  results.append({'name':name,'dispatch':c['dispatch'],'engine':c['engine'],'arrival_rate':c['arrival_rate'],'seed':c['seed'],'execution_valid':valid,'exit_code':t['exit_code'],'timed_out':t['timed_out'],'oracle_status':r.get('oracle_status'),'history_verified':a['history_verified'],'primary_cause':cause,'primary_error':fault,'failed_job':failed_job,'full_offered_jobs':corpus['total'],'recorded_committed_transactions':len(committed_ids),'recorded_completed_jobs':sum(job_by_worker),'recorded_completed_jobs_by_worker':job_by_worker,'recorded_transactions_by_worker':transactions_by_worker,'recorded_classes':dict(class_counts),'recorded_same_flight_overlap_messages':overlaps,'recorded_same_flight_out_of_order_completions':reorders,'received_foreground_inputs':len(observed_fg),'received_foreground_corpus_sha256':sha(sorted(observed_fg.values(),key=lambda m:m['id'])),'recorded_scope':'Complete committed-and-received history' if valid else 'Incomplete committed-and-received subset only; no oracle verdict. Effects/retries count received committed transactions, not all attempts or transactions the coordinator never received.','last_periodic_progress':{k:v for k,v in progress.items() if k!='retention_samples'} if progress else None,'progress_scope':'Periodic snapshot can lag retained history; pending job counters and reported retries are partial.','stage_retry_causes':r.get('retry_causes'),'retry_attribution_scope':'Final worker metrics unavailable for aborted runs; no SQLSTATE or native validation-stage cause inferred for aborted transaction.','useful_work_passed':a.get('useful_work_passed'),'qualified_capacity_trial':False,'artifacts_sha256':hashes})
check(len(results)==12,'expected twelve diagnostics')
check(gate.snapshot_sources()['sha256']==SOURCE and gate.sha256(BASE/'benchmark-final')==BINARY,'final binding changed')
counts=Counter(r['primary_cause'] for r in results)
review={'created_at':datetime.now(timezone.utc).isoformat(),'review_passed':not errors,'review_errors':errors,'all_cells_succeeded':all(r['execution_valid'] for r in results),'source_sha256':SOURCE,'binary_sha256':BINARY,'requested_cells':12,'reviewed_cells':len(results),'execution_valid_cells':sum(r['execution_valid'] for r in results),'oracle_invalid_cells':sum(r['oracle_status']=='Invalid' for r in results),'oracle_inconclusive_cells':sum(r['oracle_status']=='Inconclusive' for r in results),'oracle_unavailable_cells':sum(r['oracle_status'] is None for r in results),'primary_causes':dict(counts),'scope':'One seed, short accelerated full-history sweep jobs under shared-host concurrency. Failed jobs are progress/evidence limits, not invalid-serial-history findings. Prior affinity baseline used single batches rather than sweeps; neither workload nor timing conditions support a before/after speed ratio. No capacity/10x claim.','raw_history_review_scope':'Exact fixed cutoffs, batch IDs/ordinals, terminal empty query shape and count reconciliation. Completeness of query results is checked by the recorded Rust serial-history oracle only for successful full runs; this reviewer does not independently reproduce that oracle.','shared_observed_foreground_inputs_agree':True,'observed_foreground_union_sizes':{rate:len(messages) for rate,messages in observed_inputs.items()},'campaigns':campaigns,'cells':results}
gate.atomic_json(BASE/'sensitivity-review.json',review)
print(json.dumps({'review_passed':not errors,'valid':review['execution_valid_cells'],'causes':dict(counts),'errors':errors}))
sys.exit(bool(errors))
