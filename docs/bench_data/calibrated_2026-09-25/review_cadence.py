#!/usr/bin/env python3
"""Read-only independent schedule/accounting/timing audit; no oracle re-execution."""
import bisect, collections, hashlib, json, math, sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path('scripts').resolve()))
import qualify_hyperfeed as gate
BASE=Path('target/calibrated-validation')
SOURCE='29f53f412ab151ce026c189a95e67c477015ffa0489a59c8bdc5e254f7451add'
BINARY='8842cd00f64fd6964536dadd058b928c7d27574b53a80d7b513c7255ef93ddb5'
errors=[]
def check(ok, message):
    if not ok: errors.append(message)
def digest(path):
    return gate.sha256(path)
def q99(values):
    return sorted(values)[math.ceil(len(values)*.99)-1]/1000 if values else None
reviews=[]
for label in ('cadence-native','cadence-postgres'):
    prefix=label+': '
    campaign_path=BASE/label/'campaign.json'
    campaign=json.loads(campaign_path.read_text())
    trial=campaign['trials'][0]; config=trial['config']; run=trial['report']['runs'][0]
    root=next((BASE/label).glob('*/contention-crucible-*/*/history.jsonl')).parent
    check(campaign['completed'] and campaign['passed'] and campaign['source_stable'],prefix+'campaign validity')
    check(campaign['source_before']==campaign['source_after'] and campaign['source_before']['sha256']==SOURCE,prefix+'source provenance')
    check(campaign['binary_before_sha256']==campaign['binary_after_sha256']==BINARY,prefix+'binary provenance')
    check(trial['source_before_sha256']==trial['source_after_sha256']==SOURCE and trial['binary_before_sha256']==trial['binary_after_sha256']==BINARY,prefix+'trial provenance')
    check(config['seconds']==601 and config['arrival_rate']==32 and config['families']==16 and config['workers']==4 and config['seed']==20260925 and config['projection_interval_seconds']==config['housekeeping_interval_seconds']==300,prefix+'expected configuration')
    offered=json.loads((root/'offered-schedule.json').read_text())
    admission=run['admission_started_ns']; end=admission+601*10**9
    check(offered['admission_started_ns']==admission and offered['admission_finished_ns']==end,prefix+'offered schedule admission')
    check(offered['offered_messages']==19236 and offered['offered_by_worker']==[4808]*4+[2,2],prefix+'offered schedule counts')
    classes={name: {'count':0,'retries':0,'positive':0,'latency':[],'service':[],'queue':[]} for name in ('foreground','projection','housekeeping')}
    worker_counts=[0]*6; worker_retries=[0]*6; worker_busy=[0]*6
    ids=set(); fg_sequences=set(); by_flight=collections.defaultdict(list); intervals={k:[] for k in classes}
    kinds=collections.Counter(); outcomes=collections.defaultdict(collections.Counter); ticks=[]; successful=[]
    h=hashlib.sha256()
    with (root/'history.jsonl').open('rb') as stream:
        for raw in stream:
            h.update(raw); row=json.loads(raw); receipt=row['receipt']; message=receipt['message']; outcome=receipt['body']['outcome']
            mid=message['id']; cls=row['workload_class']; worker=row['worker']; kind=message['kind']
            kind_name=kind if isinstance(kind,str) else next(iter(kind))
            canonical={'Plan':'plan','Position':'position','GlobalProject':'global_projection','GlobalHousekeeping':'global_housekeeping'}[kind_name]
            check(mid not in ids,prefix+'duplicate message ID'); ids.add(mid)
            first=row['message_started_ns']; start=receipt['started']; finish=receipt['finished']; received=row['received_ns']; scheduled=row['scheduled_ns']
            check(admission<=scheduled<=first<=start<=finish<=received,prefix+'message timestamps')
            check(row['end_to_end_latency_ns']==received-scheduled and row['service_latency_ns']==finish-first,prefix+'message latency arithmetic')
            check(message['event_time_units_per_second']==10**9 and message['event_time']==1700000000000000000+scheduled-admission,prefix+'event clock')
            if cls=='foreground':
                sequence=mid-1000000; identity=sequence%12; ordinal=sequence//12
                fg_sequences.add(sequence)
                check(0<=sequence<19232 and worker==identity%4 and row['logical_identity']==identity and row['foreground_ordinal']==ordinal,prefix+'foreground routing metadata')
                check(scheduled==admission+sequence*10**9//32 and kind_name==('Plan' if ordinal%16==0 else 'Position') and message['source']==1<<(ordinal%3),prefix+'foreground schedule/kind/source')
                check(message['creation']=='ExistingOnly',prefix+'foreground creation policy')
                by_flight[identity].append((ordinal,first,finish))
                check(outcome['updated_views']>0 and not outcome['missing_family'] and not outcome['allocation_deferred'] and not outcome['duplicate'] and outcome['ignored_stale']==0,prefix+'useful foreground')
            else:
                bit=cls=='housekeeping'; ordinal=(mid-4000000000-int(bit))//2
                check(worker==4+int(bit) and mid==4000000000+ordinal*2+int(bit) and ordinal in (0,1),prefix+'maintenance routing/ID')
                check(row['logical_identity'] is None and row['foreground_ordinal'] is None and scheduled==admission+(ordinal+1)*300*10**9,prefix+'maintenance schedule')
                check(kind_name==('GlobalHousekeeping' if bit else 'GlobalProject'),prefix+'maintenance kind')
                payload=kind[kind_name]
                check(payload['limit']==(32 if bit else 4),prefix+'bounded maintenance limit')
                expected_query='GlobalExpired' if bit else 'GlobalDue'
                global_queries=[op['Query'] for op in receipt['body']['operations'] if 'Query' in op and expected_query in op['Query']['query']]
                check(len(global_queries)==1,prefix+'maintenance global query')
                ticks.append({'class':cls,'id':mid,'scheduled_offset_seconds':(scheduled-admission)/1e9,'first_attempt_delay_us':(first-scheduled)/1000,'retries':row['retries'],'global_query_rows':len(global_queries[0]['rows']) if global_queries else None,'claimed_events':outcome['claimed_events'],'expired_records':outcome['expired_records'],'outputs':len(outcome['outputs'])})
            positive=bool(outcome['outputs']) or any(outcome[k]>0 for k in ('created_views','updated_views','claimed_events','cancelled_events','rescheduled_events','expired_records','expired_families'))
            c=classes[cls]; c['count']+=1; c['retries']+=row['retries']; c['positive']+=positive
            c['latency'].append(received-scheduled); c['service'].append(finish-first); c['queue'].append(first-scheduled)
            worker_counts[worker]+=1; worker_retries[worker]+=row['retries']; worker_busy[worker]+=finish-first
            kinds[canonical]+=1
            for key,value in outcome.items():
                if key=='family': continue
                key='duplicate_messages' if key=='duplicate' else key
                outcomes[canonical][key]+=len(value) if key=='outputs' else int(value)
            intervals[cls].append((first,finish)); successful.append((mid,start,finish))
    check(fg_sequences==set(range(19232)) and len(ids)==19236,prefix+'complete distinct admitted corpus')
    check(worker_counts==[4808]*4+[2,2]==run['completed_by_worker'],prefix+'completed routing counts')
    check(kinds=={'plan':1212,'position':18020,'global_projection':2,'global_housekeeping':2}==run['message_kinds'],prefix+'kind counts')
    for identity,lane in by_flight.items():
        lane.sort()
        check([v[0] for v in lane]==list(range(len(lane))),prefix+'contiguous same-flight ordinals')
        check(all(previous[2]<=following[1] for previous,following in zip(lane,lane[1:])),prefix+'nonoverlapping same-flight execution')
    check(len(by_flight)==12 and run['per_flight_order']['passed'],prefix+'identity/order scope')
    classes['maintenance']={key:classes['projection'][key]+classes['housekeeping'][key] for key in classes['projection']}
    class_summary={}
    for name,computed in classes.items():
        reported=run['workload_classes'][name]
        check(reported['offered']==reported['completed']==computed['count'] and reported['retries']==computed['retries'] and reported['positive_effect_jobs']==computed['positive'],prefix+'class accounting '+name)
        for field,series in [('p99_us_including_retries','latency'),('service_latency_p99_us_including_retries','service'),('arrival_queue_delay_p99_us','queue')]:
            check(reported[field]==q99(computed[series]),prefix+'class percentile '+name+'/'+field)
        class_summary[name]={k:computed[k] for k in ('count','retries','positive')}
    for name,counts in outcomes.items():
        check(dict(counts)==run['per_kind'][name]['outcomes'],prefix+'per-kind outcomes '+name)
    for worker,reported in enumerate(run['worker_activity']):
        check(reported['busy_ns']==worker_busy[worker] and reported['retries']==worker_retries[worker],prefix+'worker activity')
        check(math.isclose(reported['utilization'],worker_busy[worker]/(run['drain_confirmed_ns']-admission),rel_tol=1e-12),prefix+'worker utilization')
    elapsed=(run['drain_confirmed_ns']-admission)/1e9
    check(admission<end<=run['workload_completed_ns']<=run['workers_stopped_ns']<=run['drain_confirmed_ns'],prefix+'continuous drain ordering')
    check(run['elapsed_seconds_including_drain']==elapsed and math.isclose(run['completed_messages_per_second_including_drain'],19236/elapsed,rel_tol=1e-12),prefix+'continuous drain arithmetic')
    population={}
    for snapshot in ('initial','final'):
        rows=json.loads((root/(snapshot+'.json')).read_text())
        families={r['family'] for r in rows if r['active'] and r['kind']==1}
        population[snapshot]=len(families)
        check(len(families)==16 and run[snapshot+'_fleet']['live_families']==16,prefix+'population '+snapshot)
    witness=json.loads((root/'serial-witness.json').read_text())
    order=witness['order']; position={mid:index for index,mid in enumerate(order)}
    check(witness['status']=='Valid' and len(order)==len(ids) and set(order)==ids,prefix+'complete serial witness IDs')
    # An independent check that its successful-attempt order respects completed-before-start edges.
    finished=sorted(successful,key=lambda v:v[2]); cursor=0; maximum_rank=-1
    for mid,start,finish in sorted(successful,key=lambda v:v[1]):
        while cursor<len(finished) and finished[cursor][2]<=start:
            maximum_rank=max(maximum_rank,position[finished[cursor][0]]); cursor+=1
        check(maximum_rank<position[mid],prefix+'serial witness real-time precedence')
    check(run['oracle_status']=='Valid' and run['history_checked'] and run['correctness_history_verified'],prefix+'oracle declaration')
    check(trial['assessment']['representative_cadence_coverage_passed'] and trial['assessment']['foreground_effect_coverage_passed'],prefix+'qualified cadence and effects')
    check(not trial['assessment']['qualified_capacity_trial'] and not run['performance_comparison_eligible'] and not run['population_turnover_tested'] and not run['global_maintenance_sweep_complete'],prefix+'limited claim scope')
    paths=['initial.json','final.json','offered-schedule.json','serial-witness.json']
    reviews.append({'engine':config['engine'],'campaign':str(campaign_path),'campaign_sha256':digest(campaign_path),'history':str(root/'history.jsonl'),'history_sha256':h.hexdigest(),'supporting_sha256':{p:digest(root/p) for p in paths},'oracle_status':run['oracle_status'],'observations':len(ids),'foreground_identities':len(by_flight),'worker_counts':worker_counts,'kind_counts':dict(kinds),'classes':class_summary,'maintenance_ticks':sorted(ticks,key=lambda t:(t['scheduled_offset_seconds'],t['class'])),'population':population,'continuous_elapsed_seconds':elapsed,'worker_shutdown_seconds':(run['workers_stopped_ns']-run['workload_completed_ns'])/1e9,'post_worker_drain_confirmation_seconds':(run['drain_confirmed_ns']-run['workers_stopped_ns'])/1e9,'foreground_useful_and_ordered':True,'representative_cadence_coverage':True,'capacity_qualified':False})
check(gate.snapshot_sources()['sha256']==SOURCE,'current frozen source fingerprint')
check(digest(BASE/'benchmark-final')==BINARY,'current frozen binary fingerprint')
receipt={'created_at':datetime.now(timezone.utc).isoformat(),'passed':not errors,'errors':errors,'source_sha256':SOURCE,'binary_sha256':BINARY,'review_method':'Streamed every history observation independently; reconstructed foreground/timer schedule, IDs, routing, useful outcomes, class counts/percentiles, worker busy time, per-flight order, population snapshots, serial-witness ID coverage and real-time precedence, continuous drain arithmetic, and source/binary hashes. Existing semantic serial oracle result checked, not independently re-executed.','scope':'Concurrent same-host functional validation, 601 seconds and one seed per engine. Two positive jobs per timer at 300 and 600 seconds. No capacity/ranking/10x, full global sweep, turnover, memory plateau or remote-worker claim.','runs':reviews}
(BASE/'cadence-review.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'passed':receipt['passed'],'errors':errors,'runs':[{'engine':r['engine'],'observations':r['observations'],'maintenance_ticks':r['maintenance_ticks']} for r in reviews]},indent=2))
raise SystemExit(bool(errors))
