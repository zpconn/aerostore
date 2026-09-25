#!/usr/bin/env python3
"""Review all completed and failed sensitivity cells without rerunning them."""
from pathlib import Path
from datetime import datetime,timezone
import collections,hashlib,json,re,sys
sys.path.insert(0,str(Path('scripts').resolve()))
import qualify_hyperfeed as gate
base=Path(__file__).resolve().parent
source='fef4c866ad708e8d49335f6432a0ce0e24d288b4232e87efece565d22ad3c32e'
binary='f01f19d86061fb4fe6f87cf384fc60eb6b4f3f44126b2ecd94d388808004f48d'
errors=[];results=[];references={};observed_corpora=[]
def check(value,message):
 if not value:errors.append(message)
def expected_owners(config):
 cache={};rotor=0;owners=[]
 for q in range(config['arrival_rate']*config['seconds']):
  identity,ordinal=q%12,q//12;alias=ordinal//2%3;signature=(0 if alias==2 else 100+identity//4,0 if alias==1 else 10000+identity)
  if config['dispatch']=='identity':owner=identity%4
  else:
   now=q*10**9//config['arrival_rate'];entry=cache.get(signature)
   if entry and now<entry[1]:owner=entry[0]
   else:owner=rotor;rotor=(rotor+1)%4
   cache[signature]=(owner,now+config['affinity_ttl_ms']*10**6)
  owners.append(owner)
 return owners
for label in ('sensitivity-identity-full','sensitivity-affinity-full'):
 campaign_path=base/label/'campaign.json';campaign=json.loads(campaign_path.read_text())
 check(campaign['completed'] and campaign['source_stable'],label+':completion/source stability')
 check(campaign['source_before']==campaign['source_after'] and campaign['source_before']['sha256']==source,label+':source fingerprint')
 check(campaign['binary_before_sha256']==campaign['binary_after_sha256']==binary,label+':binary fingerprint')
 check(len(campaign['trials'])==6,label+':six requested cells')
 for trial in campaign['trials']:
  c=trial['config'];assessment=trial['assessment'];run=trial['report']['runs'][0];name=f'{c["dispatch"]}/{c["engine"]}/{c["arrival_rate"]}'
  valid=assessment['execution_valid'];root=next(Path(trial['directory']).glob('contention-crucible-*/*/history.jsonl')).parent
  offered=json.loads((root/'offered-schedule.json').read_text());expected=gate.calibrated_corpus(c)
  check(offered['offered_messages']==expected['total'] and offered['offered_by_worker']==expected['worker_counts'],name+':offered schedule')
  check(trial['source_before_sha256']==trial['source_after_sha256']==source and trial['binary_before_sha256']==trial['binary_after_sha256']==binary,name+':trial fingerprints')
  check(not assessment['qualified_capacity_trial'] and not assessment['capacity_failure'],name+':capacity limits')
  planned=expected_owners(c);by_flight=collections.defaultdict(list);classes=collections.defaultdict(lambda:collections.Counter());messages={};rawhash=hashlib.sha256();by_worker=[0]*6;route_errors=0
  with (root/'history.jsonl').open('rb') as stream:
   for raw in stream:
    rawhash.update(raw);row=json.loads(raw);receipt=row['receipt'];m=receipt['message'];outcome=receipt['body']['outcome'];mid=m['id'];cls=row['workload_class']
    check(mid not in messages,name+':duplicate recorded message')
    messages[mid]={'message':m,'offset_ns':row['scheduled_ns']-offered['admission_started_ns']}
    by_worker[row['worker']]+=1
    positive=bool(outcome['outputs']) or any(outcome[k]>0 for k in ('created_views','updated_views','claimed_events','cancelled_events','rescheduled_events','expired_records','expired_families'))
    classes[cls].update({'received_messages':1,'received_message_retries':row['retries'],'positive_jobs':int(positive),'ignored_stale_views':outcome['ignored_stale'],'updated_views':outcome['updated_views'],'missing_family':int(outcome['missing_family']),'allocation_deferred':int(outcome['allocation_deferred'])})
    if cls=='foreground':
     q=mid-1000000;identity,ordinal=q%12,q//12;alias=ordinal//2%3
     check(0<=q<len(planned) and row['worker']==planned[q],name+':recorded routing')
     check(messages[mid]['offset_ns']==q*10**9//c['arrival_rate'],name+':arrival offset')
     check((m['callsign'],m['tail'])==(0 if alias==2 else 100+identity//4,0 if alias==1 else 10000+identity),name+':alias fields')
     by_flight[identity].append((ordinal,row['message_started_ns'],receipt['finished']))
  overlapping=reordered=0
  for lane in by_flight.values():
   lane.sort();maxfinish=0
   for ordinal,start,finish in lane:
    reordered+=finish<maxfinish;maxfinish=max(maxfinish,finish)
    overlapping+=any(other!=ordinal and otherstart<finish and otherfinish>start for other,otherstart,otherfinish in lane)
  fault=run.get('error');cause='none';failed_job=None
  if fault and 'offered backlog' in fault:cause='admitted_backlog_bound_exceeded'
  elif fault and 'after 128 retries' in fault:
   cause='transaction_retry_budget_exhausted';match=re.search(r'message (\d+) after (\d+) retries',fault)
   if match:
    mid=int(match[1]);bit=(mid-4000000000)%2;ordinal=(mid-4000000000-bit)//2;period=c['housekeeping_interval_seconds'] if bit else c['projection_interval_seconds']
    failed_job={'message_id':mid,'class':'housekeeping' if bit else 'projection','scheduled_offset_seconds':(ordinal+1)*period,'retry_limit':int(match[2])}
  elif not valid:cause='other_execution_or_evidence_failure'
  progress_path=root/'progress.json';progress=json.loads(progress_path.read_text()) if progress_path.exists() else None
  if valid:
   check(run['oracle_status']=='Valid' and len(messages)==expected['total'],name+':complete valid history')
   check(not gate.continuous_timing_errors(run,expected['total']),name+':continuous drain')
   check(not gate.calibrated_report_errors(run,c,expected['worker_counts']),name+':dispatch/class report')
   order=run['per_flight_order'];check(order['passed']==(overlapping==0 and reordered==0),name+':order summary')
   if c['dispatch']=='signature-affinity':check(order['overlapping_messages']==overlapping and order['out_of_order_completions']==reordered,name+':affinity order diagnostics')
   references.setdefault(c['arrival_rate'],messages)
  else:
   check(run.get('oracle_status') is None,name+':failure incorrectly claims oracle verdict')
  observed_corpora.append((name,c['arrival_rate'],messages))
  inputs={str(p.relative_to(base)):gate.sha256(p) for p in (campaign_path,Path(trial['directory'])/'report.json',Path(trial['directory'])/'run.log',root/'offered-schedule.json')}
  if progress_path.exists():inputs[str(progress_path.relative_to(base))]=gate.sha256(progress_path)
  inputs[str((root/'history.jsonl').relative_to(base))]=rawhash.hexdigest()
  results.append({'dispatch':c['dispatch'],'engine':c['engine'],'arrival_rate':c['arrival_rate'],'seed':c['seed'],'execution_valid':valid,'exit_code':trial['exit_code'],'timed_out':trial['timed_out'],'source_stable':trial['source_stable'],'oracle_status':run.get('oracle_status'),'primary_cause':cause,'primary_error':fault,'failed_maintenance_job':failed_job,'full_offered_messages':expected['total'],'recorded_received_messages':len(messages),'recorded_received_by_worker':by_worker,'recorded_classes':dict(classes),'recorded_same_flight_overlap_messages':overlapping,'recorded_same_flight_out_of_order_completions':reordered,'recorded_scope':'Complete committed-and-received history' if valid else 'Incomplete committed-and-received subset only; overlap/stale counts are lower bounds and no serial-history verdict is available','last_periodic_progress':{k:v for k,v in progress.items() if k!='retention_samples'} if progress else None,'progress_scope':'Periodic snapshot can lag the retained history; its retries cover received successful messages, not all attempts or the failed message','stage_retry_causes':run.get('retry_causes'),'stage_retry_scope':'Final worker Done metrics are unavailable for aborted cases; no SQLSTATE or native validation-stage attribution inferred from total retries','useful_work_passed':assessment.get('useful_work_passed'),'qualified_capacity_trial':assessment['qualified_capacity_trial'],'artifacts_sha256':inputs})
for name,rate,messages in observed_corpora:
 reference=references.get(rate);check(reference is not None,name+':reference input corpus')
 if reference is not None:check(all(reference.get(mid)==message for mid,message in messages.items()),name+':observed business inputs differ across policy/engine')
counts=collections.Counter(r['primary_cause'] for r in results)
receipt={'created_at':datetime.now(timezone.utc).isoformat(),'review_passed':not errors,'review_errors':errors,'all_cells_succeeded':all(r['execution_valid'] for r in results),'source_sha256':source,'binary_sha256':binary,'requested_cells':12,'reviewed_cells':len(results),'execution_valid_cells':sum(r['execution_valid'] for r in results),'oracle_invalid_cells':sum(r['oracle_status']=='Invalid' for r in results),'oracle_inconclusive_cells':sum(r['oracle_status']=='Inconclusive' for r in results),'oracle_unavailable_cells':sum(r['oracle_status'] is None for r in results),'primary_causes':dict(counts),'scope':'One seed, short accelerated maintenance, full-history instrumentation and concurrent shared-host validation. Failures are progress/evidence limits, not evidence of invalid committed transaction histories. No saturation bracket, speed ranking, replacement or capacity claim. Stored partial histories/progress/logs retained.','observed_business_inputs_match_complete_reference_per_rate':True,'cells':results}
(base/'sensitivity-review.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'review_passed':not errors,'errors':errors,'valid':receipt['execution_valid_cells'],'causes':dict(counts),'partial_affinity':[{k:r[k] for k in ('engine','arrival_rate','recorded_received_messages','recorded_same_flight_overlap_messages','recorded_same_flight_out_of_order_completions','recorded_classes')} for r in results if r['dispatch']=='signature-affinity']},indent=2))
raise SystemExit(bool(errors))
