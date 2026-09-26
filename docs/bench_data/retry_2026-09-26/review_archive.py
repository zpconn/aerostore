#!/usr/bin/env python3
"""Read-only archive integrity, provenance, trial-retention and documentation audit."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import tarfile
import time
from urllib.parse import unquote, urlsplit

ROOT=Path(__file__).resolve().parents[2]
BASE=Path(__file__).resolve().parent

def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as stream:
  for block in iter(lambda:stream.read(1<<20),b''):h.update(block)
 return h.hexdigest()

def json_data(raw):return json.loads(raw)

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--archive',type=Path,default=ROOT/'docs/bench_data/retry_2026-09-26')
 p.add_argument('--output',type=Path,default=BASE/'archive-review.json')
 args=p.parse_args(); archive=args.archive.resolve(strict=True); started=time.monotonic(); errors=[]
 manifest_path=archive/'artifact-manifest.json'; manifest_hash=digest(manifest_path); manifest=json.loads(manifest_path.read_text()); entries=manifest['files']
 bysource={e['source']:e for e in entries}; byarchive={e['archive']:e for e in entries}
 if len(bysource)!=len(entries) or len(byarchive)!=len(entries):errors.append('duplicate source/archive manifest entries')
 def check(condition,message):
  if not condition:errors.append(message)
 def entry_for(path):
  path=Path(path)
  source=str(path.relative_to(ROOT)) if path.is_absolute() else str(path)
  if source not in bysource:raise ValueError('missing retained source '+source)
  return bysource[source]
 def stream_entry(entry):return gzip.open(archive/entry['archive'],'rb') if entry['compression']=='gzip' else (archive/entry['archive']).open('rb')
 def raw_entry(entry):
  with stream_entry(entry) as stream:return stream.read()
 def load_source(path):return json.loads(raw_entry(entry_for(path)))
 def load_name(name):return json.loads(raw_entry(byarchive[name]))
 integrity=[]
 for index,entry in enumerate(entries):
  path=(archive/entry['archive']).resolve(strict=True)
  check(path.is_relative_to(archive) and not (archive/entry['archive']).is_symlink(),f"unsafe archive path {entry['archive']}")
  check(entry['compression'] in (None,'gzip'),f"unknown compression {entry['archive']}")
  compressed_sha=digest(path); actual_size=path.stat().st_size
  h=hashlib.sha256(); size=0
  with stream_entry(entry) as stream:
   for block in iter(lambda:stream.read(1<<20),b''):h.update(block);size+=len(block)
  check(compressed_sha==entry['archive_sha256'] and actual_size==entry['archive_bytes'],f"stored hash/size differs: {entry['archive']}")
  check(h.hexdigest()==entry['original_sha256'] and size==entry['original_bytes'],f"decompressed hash/size differs: {entry['archive']}")
  integrity.append({'archive':entry['archive'],'archive_sha256':compressed_sha,'original_sha256':h.hexdigest(),'original_bytes':size})
  if (index+1)%250==0:print(f'checked {index+1}/{len(entries)} archive artifacts',flush=True)
 bundles=[]; snapshots={}; builds={}
 for prefix in ('','development/'):
  for stem in ('','default-'):
   label=prefix+stem; build=load_name(label+'build-provenance.json.gz'); builds[label]=build
   expected=build['source_before']; check(expected==build['source_after'],label+'build source unstable')
   check(hashlib.sha256(json.dumps(expected['files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()==expected['sha256'],label+'source digest differs')
   actual={}; members=[]
   with tarfile.open(archive/(label+'source.tar.gz'),'r:gz') as tar:
    for member in tar:
     check(member.isfile() and not Path(member.name).is_absolute() and '..' not in Path(member.name).parts,label+'unsafe tar member '+member.name)
     check(member.name not in actual,label+'duplicate tar member '+member.name)
     if not member.isfile():continue
     stream=tar.extractfile(member);h=hashlib.sha256()
     for block in iter(lambda:stream.read(1<<20),b''):h.update(block)
     actual[member.name]=h.hexdigest();members.append(member.name)
   check(actual==expected['files'],label+'source bundle differs from complete build source map')
   check(len(actual)==493,label+'source file count differs from declared493')
   snapshots[label]=actual;bundles.append({'bundle':label+'source.tar.gz','members':len(actual),'source_sha256':expected['sha256'],'binary_sha256':build['binary_sha256']})
 check(snapshots['']==snapshots['default-'],'default and feature final sources differ')
 check(snapshots['development/']==snapshots['development/default-'],'default and feature development sources differ')
 transition=load_name('development-to-final.json'); changed=sorted(k for k in snapshots[''] if snapshots[''][k]!=snapshots['development/'].get(k))
 check(changed==sorted(transition['changed_inputs']),'development-to-final source delta differs')
 check(builds['']['binary_sha256']==builds['development/']['binary_sha256'],'feature executable changed across proof-only refresh')
 check(builds['default-']['binary_sha256']==builds['development/default-']['binary_sha256'],'default executable changed across proof-only refresh')
 check(manifest['source_sha256']==builds['']['source_before']['sha256'],'manifest source binding differs')
 check(manifest['binary_sha256']==builds['']['binary_sha256'],'manifest binary binding differs')
 execution=load_name('controlled-campaign/execution.json.gz'); review=load_name('controlled-review.json.gz')
 check(execution['completed'] and execution['passed'] and execution['source_stable'],'campaign incomplete/unstable')
 check(review['passed'] and not review['errors'],'independent controlled review failed')
 check(len(execution['steps'])==len(review['cells'])==90 and not execution['unrun_steps'],'not all90predeclaredcells retained')
 check(review['source_sha256']==manifest['source_sha256'] and review['binary_sha256']==manifest['binary_sha256'],'review binding differs')
 check(review['execution_sha256']==byarchive['controlled-campaign/execution.json.gz']['original_sha256'],'review execution hash differs')
 for path,sha in review['artifact_hashes'].items():check(entry_for(path)['original_sha256']==sha,'review-bound artifact missing/changed '+path)
 cells=[]; failure_files=[]
 for step,cell in zip(execution['steps'],review['cells']):
  check(step['label']==cell['label'] and step['index']==cell['index'],'review cell ordering differs')
  campaign_entry=entry_for(step['campaign_path']);check(campaign_entry['original_sha256']==step['campaign_sha256'],step['label']+'campaign receipt hash differs')
  campaign=json.loads(raw_entry(campaign_entry));check(len(campaign['trials'])==1,step['label']+'trial cardinality differs')
  trial=campaign['trials'][0];directory=Path(trial['directory'])
  check(load_source(directory/'trial.json')==trial,step['label']+'raw trial differs')
  check(load_source(directory/'report.json')==trial['report'],step['label']+'raw report differs')
  check(trial['assessment']==step['assessment']==cell['assessment'],step['label']+'assessment differs')
  entry_for(directory/'run.log')
  run=trial['report']['runs'][0];evidence=Path(run['evidence_directory'])
  for name in ('offered-schedule.json','history.jsonl','result.json'):entry_for(evidence/name)
  history=entry_for(evidence/'history.jsonl'); lines=0
  with stream_entry(history) as stream:
   for line in stream:
    if line.strip():
     lines+=1
     if step['evidence']=='metrics':check(json.loads(line)['receipt']['body']['operations']==[],step['label']+'metrics receipt unexpectedly records serial operations')
  valid=cell['assessment']['execution_valid']
  if valid:
   for name in ('initial.json','final.json','serial-witness.json'):entry_for(evidence/name)
   check(lines==run['completed_transactions'],step['label']+'receipt transaction coverage differs')
  else:
   check(len(cell['failure_evidence'])==1,step['label']+'missing unique failure progress')
   for failure in cell['failure_evidence']:
    ent=entry_for(failure['path']);check(ent['original_sha256']==failure['sha256'],step['label']+'failure progress hash differs')
    raw=json.loads(raw_entry(ent));check(raw['failure_evidence']==failure['failed_worker_cumulative'],step['label']+'failure cumulative snapshot differs')
    check(raw['completed_worker_metrics_snapshots']==failure['completed_peer_snapshots'],step['label']+'failure completed-peer snapshots differ')
    check(lines==raw['completed_transactions'],step['label']+'partial receipt count differs')
    failure_files.append(ent['archive'])
  cells.append({'index':step['index'],'label':step['label'],'execution_valid':valid,'history_lines':lines,'failure_progress_count':len(cell['failure_evidence'])})
 check(len(failure_files)==25,'not all25failedcells retain failure progress')
 check(sum(c['execution_valid'] for c in cells)==65,'execution-valid count differs')
 check(sum(c['numeric_comparison_eligible'] for c in review['cells'])==60,'useful numeric-comparison count differs')
 # Audit stated data-derived counts; this is not a natural-language entailment checker.
 low=[c for c in review['cells'] if c['rate']<2048];native=[c for c in review['cells'] if c['rate']==2048 and c['engine']=='aerostore'];pg=[c for c in review['cells'] if c['rate']==2048 and c['engine']=='postgres']
 failed=[c for c in native if not c['assessment']['execution_valid']];traced=[c for c in failed if c['retry_diagnostics']=='on'];completed=[c for c in native if c['assessment']['execution_valid']]
 tail=Counter()
 for c in traced:
  f=c['failure_evidence'][0];tail.update(f['failed_worker_tail']['primary_origins'])
  check(f['worker']==4 and 'after128retries' in f['error'].replace(' ',''),c['label']+'failure is not projection retry exhaustion')
  samples=f['failed_worker_tail']['terminal_samples'];check(len(samples)==1 and samples[0]['attempt_index']==128 and samples[0]['diagnostics_delta'].get('conflict_origin:commit:predicate_validation_stamp:due')==1,c['label']+'terminal cause differs')
 counts={'low_rate_useful_trials':len(low),'overload_native_trials':len(native),'overload_native_failed':len(failed),'traced_native_failures':len(traced),'retained_tail_samples':sum(tail.values()),'tail_due_commit':tail['conflict_origin:commit:predicate_validation_stamp:due'],'tail_due_lookup':tail['conflict_origin:global_due_lookup:lookup_post_snapshot_stamp:due'],'overload_native_completed':len(completed),'overload_full_completed':sum(c['evidence']=='full' for c in completed),'overload_metrics_completed':sum(c['evidence']=='metrics' for c in completed),'postgres_overload_failures':sum(not c['assessment']['execution_valid'] for c in pg)}
 check(list(counts.values())==[60,24,19,10,320,295,25,5,3,2,6],'documented core result counts differ')
 check(all(c['numeric_comparison_eligible'] for c in low),'low-rate useful coverage differs')
 check(all(not c['numeric_comparison_eligible'] for c in native+pg),'overload numeric qualification unexpectedly accepted')
 check(all('backlog' in c['benchmark_error'] for c in pg),'PostgreSQL overload cause differs')
 check(all(c['assessment']['foreground_outcomes']['ignored_stale']>0 for c in completed),'completed native overload lacks stated stale work')
 readme=(archive/'README.md').read_text(); compact=re.sub(r'\s+',' ',readme.replace('**',''))
 for fragment in ['All 60 trials at 128 and 512','19 of24native'.replace('of24native','of 24 native'),'All ten diagnostic-enabled','320 retained tail samples','295 at commit and25during'.replace('and25during','and 25 during'),'Five native overload trials','Three have Valid full histories; two are metrics-only histories','All six PostgreSQL overload trials','493-file source bundle']:
  check(fragment in compact,'README count statement missing/changed: '+fragment)
 checks=[]
 for collection,field in [('expiry_policy_pairs','retry_diagnostics'),('runtime_diagnostic_pairs','expiry_index_policy')]:
  fixed='off' if collection=='expiry_policy_pairs' else 'housekeeping'
  g=next(g for g in review[collection]['groups'] if g['rate']==512 and g['evidence']=='metrics' and g[field]==fixed);s=g['changes']['foreground_p99_ms']['percent_change'];checks.append(s)
 check(round(checks[0]['median'],2)==-2.50 and round(checks[0]['min'],2)==-12.91 and round(checks[0]['max'],2)==-0.76,'README filter effect differs')
 check(round(checks[1]['median'],2)==6.62 and checks[1]['min']>0,'README tracing effect differs')
 unit=load_name('unit-tests-execution.json.gz');rust_count=0
 for step in unit['steps']:
  if step['command'][0]=='cargo':rust_count+=sum(map(int,re.findall(rb'test result: ok\. (\d+) passed;',raw_entry(entry_for(step['log'])))))
 check(rust_count==190,'README190Rusttest executions differ')
 process=load_name('process-review.json');check(process['tests']==23 and sum(s['tests'] for s in process['suites'])==23,'README23process tests differ')
 tcp=load_name('tcp-regression/case/client-report.json.gz')['runs'][0];check(tcp['completed_messages']==196 and tcp['completed_transactions']==209 and tcp['oracle_status']=='Valid','READMETCPresult differs')
 doc_paths=[ROOT/'README.md',archive/'README.md',archive/'controlled-findings.md',ROOT/'docs/hyperfeed_retry_diagnostics.md'];links=[];docs={};deferred=[]
 for doc in doc_paths:
  docs[str(doc.relative_to(ROOT))]=digest(doc)
  for raw in re.findall(r'\[[^\]]*\]\(([^)]+)\)',doc.read_text()):
   target=raw.strip().split(' "',1)[0].strip('<>');parts=urlsplit(target)
   if parts.scheme or parts.netloc or not parts.path:continue
   resolved=(doc.parent/unquote(parts.path)).resolve()
   if resolved.is_relative_to(archive/'guardrails'):
    deferred.append({'document':str(doc.relative_to(ROOT)),'link':target});continue
   exists=resolved.exists();links.append({'document':str(doc.relative_to(ROOT)),'link':target,'exists':exists})
   check(exists,f'broken relative link {doc.relative_to(ROOT)}: {target}')
 check(digest(manifest_path)==manifest_hash,'manifest changed during review')
 unmanifested=[]
 for path in archive.rglob('*'):
  if path.is_file() and not path.is_relative_to(archive/'guardrails') and str(path.relative_to(archive)) not in byarchive:unmanifested.append(str(path.relative_to(archive)))
 unexpected=set(unmanifested)-{'README.md','artifact-manifest.json','archive-review.json','review_archive.py','archive-review.log','archive-review-initial.log','archive-review-scope-check.log','archive-review-scope-check.json','audit-manifest.json'}
 check(not unexpected,'unexpected unmanifested files: '+str(sorted(unexpected)))
 result={'passed':not errors,'errors':errors,'created_at':datetime.now(timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-started,'scope':'Read-only archive byte/decompression/source-bundle and trial-retention audit; no benchmark, oracle or proof rerun. Main manifest binds1393artifacts; this audit and review helper are separately retained to avoid a circular manifest. Planned guardrail archive excluded until complete.','archive':str(archive),'manifest_sha256':manifest_hash,'review_script_sha256':digest(Path(__file__)),'artifact_count':len(entries),'archive_bytes':sum(e['archive_bytes'] for e in entries),'original_bytes':sum(e['original_bytes'] for e in entries),'integrity':integrity,'source_bundles':bundles,'development_to_final_changed_inputs':changed,'trial_cells':cells,'failure_progress_artifacts':failure_files,'documented_counts':counts,'rust_test_executions':rust_count,'process_tests':process['tests'],'document_sha256':docs,'relative_links':links,'deferred_guardrail_links':deferred,'unmanifested_files':unmanifested}
 args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k] for k in ('passed','errors','elapsed_seconds','artifact_count','archive_bytes','original_bytes','documented_counts')},indent=2));return 0 if result['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
