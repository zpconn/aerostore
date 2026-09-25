#!/usr/bin/env python3
"""Audit saved reports and full-history inputs; starts no database or workload."""
from pathlib import Path
from datetime import datetime,timezone
import collections,hashlib,json,sys
sys.path.insert(0,str(Path('scripts').resolve()))
import qualify_hyperfeed as gate
base=Path(sys.argv[1] if len(sys.argv)>1 else 'target/affinity-validation')
expected_source=sys.argv[2] if len(sys.argv)>2 else '3f94a0c3098bd6ac079553dfeb4b638be2686a5ba2ecff92bf4e2a3a7304cd72'
expected_binary=sys.argv[3] if len(sys.argv)>3 else 'f01f19d86061fb4fe6f87cf384fc60eb6b4f3f44126b2ecd94d388808004f48d'
errors=[]; reviews=[]; corpus_by_seed=collections.defaultdict(set)
def check(ok,reason):
 if not ok:errors.append(reason)
def history_review(path,run,config):
 h=hashlib.sha256();messages={};routes={};flight=collections.defaultdict(list);positive=stale=0
 with path.open('rb') as stream:
  for raw in stream:
   h.update(raw);r=json.loads(raw);m=r['receipt']['message'];mid=m['id']
   check(mid not in messages,str(path)+':duplicate ID')
   messages[mid]={'message':m,'offset_ns':r['scheduled_ns']-run['admission_started_ns']}
   if r['workload_class']=='foreground':
    sequence=mid-1000000;routes[sequence]=r['worker'];identity=sequence%12;ordinal=sequence//12
    alias=ordinal//2%3
    check((m['callsign'],m['tail'])==(0 if alias==2 else 100+identity//4,0 if alias==1 else 10000+identity),str(path)+':mixed signature')
    check(r['logical_identity']==identity and r['foreground_ordinal']==ordinal,str(path)+':identity metadata')
    check(messages[mid]['offset_ns']==sequence*10**9//64,str(path)+':foreground offset')
    flight[identity].append((ordinal,r['message_started_ns'],r['receipt']['finished']))
    o=r['receipt']['body']['outcome'];stale+=o['ignored_stale']
    positive+=bool(o['outputs']) or any(o[k]>0 for k in ('created_views','updated_views','claimed_events','cancelled_events','rescheduled_events','expired_records','expired_families'))
 check(set(routes)==set(range(config['seconds']*64)),str(path)+':complete foreground IDs')
 expected_ids=set(range(1000000,1000000+config['seconds']*64))
 for bit,period in ((0,config['projection_interval_seconds']),(1,config['housekeeping_interval_seconds'])):
  for ordinal,tick in enumerate(range(period,config['seconds'],period)):
   mid=4000000000+2*ordinal+bit;expected_ids.add(mid)
   check(messages.get(mid,{}).get('offset_ns')==tick*10**9,str(path)+':maintenance offset')
 check(set(messages)==expected_ids,str(path)+':complete input corpus')
 fnv=0xcbf29ce484222325
 for sequence,owner in sorted(routes.items()):
  for byte in sequence.to_bytes(8,'little')+owner.to_bytes(8,'little'):fnv=((fnv^byte)*0x100000001b3)&((1<<64)-1)
 expected=gate.calibrated_dispatch(config)
 check(f'{fnv:016x}'==expected['assignment_fingerprint']==run['dispatch_audit']['assignment_fingerprint'],str(path)+':actual assignment digest')
 overlaps=reordered=0
 for lane in flight.values():
  lane.sort();maximum_finish=0
  for ordinal,start,finish in lane:
   reordered+=finish<maximum_finish;maximum_finish=max(maximum_finish,finish)
   overlaps+=any(other!=ordinal and a<finish and b>start for other,a,b in lane)
 order=run['per_flight_order']
 check(order['passed']==(overlaps==0 and reordered==0),str(path)+':observed ordering')
 if config.get('dispatch')=='signature-affinity':
  check(order['overlapping_messages']==overlaps and order['out_of_order_completions']==reordered,str(path)+':order diagnostic counts')
 check(run['workload_classes']['foreground']['positive_effect_jobs']==positive,str(path)+':positive job count')
 check(sum(v['outcomes']['ignored_stale'] for k,v in run['per_kind'].items() if k in ('plan','position'))==stale,str(path)+':stale views')
 encoded=json.dumps([messages[k] for k in sorted(messages)],sort_keys=True,separators=(',',':')).encode()
 return {'observations':len(messages),'raw_history_sha256':h.hexdigest(),'input_corpus_sha256':hashlib.sha256(encoded).hexdigest(),'observed_assignment_fingerprint':f'{fnv:016x}','foreground_positive_jobs':positive,'ignored_stale_views':stale,'overlapping_same_flight_messages':overlaps,'out_of_order_completions':reordered}
for label in ['identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics']:
 p=base/label/'campaign.json';c=json.loads(p.read_text())
 check(c['completed'] and c['passed'] and c['source_stable'],label+':campaign completion')
 check(c['source_before']==c['source_after'] and c['source_before']['sha256']==expected_source,label+':source')
 check(c['binary_before_sha256']==c['binary_after_sha256']==expected_binary,label+':binary')
 check(len(c['trials'])==9,label+':all cells')
 for t in c['trials']:
  cfg=t['config'];r=t['report']['runs'][0];a=t['assessment'];name=f'{label}/{cfg["engine"]}/{cfg["seed"]}'
  check(t['source_before_sha256']==t['source_after_sha256']==expected_source and t['binary_before_sha256']==t['binary_after_sha256']==expected_binary,name+':trial provenance')
  check(a['execution_valid'] and a['correctness_companion_verified'] and r['completed_messages']==326,name+':execution/companion/count')
  check(not gate.continuous_timing_errors(r,326),name+':continuous clock')
  check(not a['qualified_capacity_trial'] and not a['capacity_failure'] and not a['representative_cadence_coverage_passed'],name+':claim limits')
  check(not gate.calibrated_report_errors(r,cfg,gate.calibrated_corpus(cfg)['worker_counts']),name+':dispatch and scheduling report')
  check(a['history_verified']==(cfg['evidence']=='full'),name+':history scope')
  entry={'campaign':label,'engine':cfg['engine'],'seed':cfg['seed'],'execution_valid':a['execution_valid'],'history_verified':a['history_verified'],'companion_verified':a['correctness_companion_verified'],'useful_work_passed':a['useful_work_passed'],'foreground_positive_job_fraction':a['foreground_positive_job_fraction'],'foreground_stale_view_update_fraction':a['foreground_stale_view_update_fraction'],'ordering_passed':a['foreground_ordering_passed'],'dispatch_audit':r['dispatch_audit'],'completed_messages':r['completed_messages'],'retries':r['retries']}
  if cfg['evidence']=='full':
   hp=Path(r['evidence_directory'])/'history.jsonl'
   entry['history_review']=history_review(hp,r,cfg);corpus_by_seed[cfg['seed']].add(entry['history_review']['input_corpus_sha256'])
  reviews.append(entry)
for seed,hashes in corpus_by_seed.items():check(len(hashes)==1,f'seed{seed}:identical inputs across engines and dispatch controls')
lp=base/'loopback'/'client-report.json';ld=json.loads(lp.read_text());lr=ld['runs'][0];lc=ld['config']
check(ld['passed'] and lr['oracle_status']=='Valid' and lr['completed_messages']==196,'loopback:execution/history/count')
check(not gate.continuous_timing_errors(lr,196),'loopback:continuous clock')
check(not gate.calibrated_report_errors(lr,lc,gate.calibrated_corpus(lc)['worker_counts']),'loopback:dispatch and scheduling report')
lh=history_review(Path(lr['evidence_directory'])/'history.jsonl',lr,lc)
receipt={'created_at':datetime.now(timezone.utc).isoformat(),'passed':not errors,'errors':errors,'source_sha256':expected_source,'binary_sha256':expected_binary,'scope':'Accelerated shared-host functional validation with concurrent formal/tests, not a speed comparison. Identity and signature-affinity use identical mixed-signature business inputs per seed. Full histories were streamed for corpus/route/order/effect accounting; semantic oracle Valid results checked, not rerun. Metrics histories remain unverified despite exact full companions.','campaigns':4,'trials':len(reviews),'full_history_trials':18,'metrics_companion_trials':18,'same_business_input_corpus_by_seed':{str(k):sorted(v) for k,v in corpus_by_seed.items()},'all_capacity_claims_false':True,'trials_review':reviews,'loopback':lh}
(base/'campaign-review.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'passed':not errors,'errors':errors,'trials':len(reviews),'input_corpora':receipt['same_business_input_corpus_by_seed'],'loopback_observations':lh['observations']}))
raise SystemExit(bool(errors))
