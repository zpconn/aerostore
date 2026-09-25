#!/usr/bin/env python3
"""Independent immutable archive byte, decompression and evidence-binding audit."""
import collections,gzip,hashlib,json,re,tarfile
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path.cwd();BASE=ROOT/'docs/bench_data/affinity_2026-09-25';OUT=ROOT/'target/affinity-final-validation'
SOURCE='fef4c866ad708e8d49335f6432a0ce0e24d288b4232e87efece565d22ad3c32e';BINARY='f01f19d86061fb4fe6f87cf384fc60eb6b4f3f44126b2ecd94d388808004f48d'
errors=[];checks=[]
def check(ok,msg):
 if not ok:errors.append(msg)
def stream_hash(stream):
 h=hashlib.sha256();size=0
 for block in iter(lambda:stream.read(1<<20),b''):h.update(block);size+=len(block)
 return h.hexdigest(),size
def file_hash(path):
 with path.open('rb') as stream:return stream_hash(stream)[0]
def read(name):
 p=BASE/name
 with (gzip.open(p,'rt') if p.suffix=='.gz' else p.open()) as stream:return json.load(stream)
def same_source(data):
 return data['source_before']==data['source_after'] and data['source_before']['sha256']==SOURCE
manifest=read('artifact-manifest.json');files=manifest['files'];index={};archive_names=set();compressed=0;archive_bytes=0;original_bytes=0;original_checked=0
check(manifest['completed'] and manifest['source_sha256']==SOURCE and manifest['binary_sha256']==BINARY,'manifest frozen binding')
for entry in files:
 name=entry['archive'];relative=Path(name)
 check(not relative.is_absolute() and '..' not in relative.parts,'unsafe archive path '+name)
 check(name not in archive_names,'duplicate archive path '+name);archive_names.add(name)
 path=BASE/name;check(path.is_file() and not path.is_symlink(),'missing/symlink artifact '+name)
 if not path.is_file():continue
 rawhash=file_hash(path);size=path.stat().st_size
 check(rawhash==entry['archive_sha256'] and size==entry['archive_bytes'],'archive bytes differ '+name)
 archive_bytes+=size;original_bytes+=entry['original_bytes']
 if entry['compression']=='gzip':
  compressed+=1
  with gzip.open(path,'rb') as stream:originalhash,decodedbytes=stream_hash(stream)
 else:
  check(entry['compression'] is None,'unknown compression '+name);originalhash,decodedbytes=rawhash,size
  if path.suffix=='.gz':
   with gzip.open(path,'rb') as stream:stream_hash(stream)
 check(originalhash==entry['original_sha256'] and decodedbytes==entry['original_bytes'],'decoded original differs '+name)
 sourcepath=(ROOT/entry['source']).resolve();index[str(sourcepath)]=entry
 if sourcepath.is_file():
  original_checked+=1;check(sourcepath.stat().st_size==entry['original_bytes'] and file_hash(sourcepath)==entry['original_sha256'],'source file changed after archive '+name)
check(len(files)==708,'expected708 manifested main artifacts')
checks.append('All708 archive byte hashes/sizes and original decompressed hashes/sizes checked; gzip CRC/trailers read to EOF.')
build=read('build-provenance.json.gz');check(build['exit_code']==0 and same_source(build) and build['binary_sha256']==BINARY,'build provenance binding')
def tar_inventory(path):
 hashes={}
 with tarfile.open(path,'r:gz') as archive:
  for member in archive:
   check(member.isfile() and not Path(member.name).is_absolute() and '..' not in Path(member.name).parts,'unsafe/nonfile source tar member '+member.name)
   check(member.name not in hashes,'duplicate source member '+member.name)
   if member.isfile():
    with archive.extractfile(member) as stream:hashes[member.name]=stream_hash(stream)[0]
 return hashes
inventory=tar_inventory(BASE/'source.tar.gz')
check(len(inventory)==482 and inventory==build['source_before']['files'],'source tar inventory differs from frozen482 files')
canonical=hashlib.sha256(json.dumps(inventory,sort_keys=True,separators=(',',':')).encode()).hexdigest();check(canonical==SOURCE,'source inventory digest')
development=tar_inventory(BASE/'development/source.tar.gz');changed=sorted(set(inventory)^set(development)|{p for p in inventory.keys()&development.keys() if inventory[p]!=development[p]})
check(changed==['scripts/test_affinity_dispatch.py','verification/frozen_boundary.json'],'development correction scope')
checks.append('Final482 source members independently match build inventory/digest; development differs only in the optional-field test and frozen-boundary hash.')
for label in ('identity-mixed-full','identity-mixed-metrics','affinity-mixed-full','affinity-mixed-metrics','sensitivity-identity-full','sensitivity-affinity-full'):
 c=read(label+'/campaign.json.gz')
 check(c['completed'] and c['source_stable'] and same_source(c) and c['binary_before_sha256']==c['binary_after_sha256']==BINARY,label+':campaign binding')
 check(len(c['trials'])==(6 if label.startswith('sensitivity') else 9),label+':trial count')
 for t in c['trials']:
  check(t['source_before_sha256']==t['source_after_sha256']==SOURCE and t['binary_before_sha256']==t['binary_after_sha256']==BINARY,label+':trial binding')
  check(not t['assessment']['qualified_capacity_trial'] and not t['assessment']['capacity_failure'],label+':capacity scope')
  if not label.startswith('sensitivity'):
   check(t['assessment']['execution_valid'] and t['assessment']['correctness_companion_verified'] and t['report']['runs'][0]['completed_messages']==326,label+':functional completeness')
   check(t['assessment']['history_verified']==label.endswith('-full'),label+':metrics/full history scope')
review=read('campaign-review.json.gz');check(review['passed'] and not review['errors'] and review['source_sha256']==SOURCE and review['binary_sha256']==BINARY and review['trials']==36,'main independent review')
check(all(len(v)==1 for v in review['same_business_input_corpus_by_seed'].values()) and len(review['same_business_input_corpus_by_seed'])==3,'matched input corpora')
check(review['loopback']['observations']==196,'loopback corpus')
sensitivity=read('sensitivity-review.json.gz');check(sensitivity['review_passed'] and not sensitivity['review_errors'] and not sensitivity['all_cells_succeeded'] and sensitivity['execution_valid_cells']==6 and sensitivity['oracle_unavailable_cells']==6 and sensitivity['oracle_invalid_cells']==0 and sensitivity['oracle_inconclusive_cells']==0,'sensitivity verdict scope')
check(sensitivity['primary_causes']=={'none':6,'admitted_backlog_bound_exceeded':2,'transaction_retry_budget_exhausted':4},'sensitivity primary causes')
partial=[]
for engine,expected_stale,expected_count in [('postgres',3341,2446),('service-unix',2726,3285)]:
 cell=next(c for c in sensitivity['cells'] if c['dispatch']=='signature-affinity' and c['arrival_rate']==2048 and c['engine']==engine)
 check(not cell['execution_valid'] and cell['oracle_status'] is None,'partial history verdict '+engine)
 history_relative=next(p for p in cell['artifacts_sha256'] if p.endswith('/history.jsonl'))
 archived=index[str((OUT/history_relative).resolve())]['archive'];counter=stale=0
 with gzip.open(BASE/archived,'rt') as stream:
  for line in stream:
   row=json.loads(line);counter+=1
   if row['workload_class']=='foreground':stale+=row['receipt']['body']['outcome']['ignored_stale']
 check(stale==expected_stale==cell['recorded_classes']['foreground']['ignored_stale_views'] and counter==expected_count==cell['recorded_received_messages'],'partial rawtrace claims '+engine)
 partial.append({'engine':engine,'recorded_messages':counter,'ignored_stale_views':stale,'oracle_status':None})
checks.append('All48 trial frozen bindings/capacity flags checked; main36 pass, sensitivity6 valid+6 progress failures. Partial archived PG/service traces independently reproduce3341/2726 stale views.')
fault_execution=read('affinity-tests-execution.json.gz');check(fault_execution['completed'] and fault_execution['passed'] and fault_execution['source_stable'] and same_source(fault_execution) and fault_execution['binary_before_sha256']==fault_execution['binary_after_sha256']==BINARY,'fault-test execution binding')
fault_review=read('affinity-tests-review.json.gz');check(fault_review['passed'] and fault_review['source_sha256']==SOURCE and fault_review['binary_sha256']==BINARY,'fault review binding')
fault=next(r for r in fault_review['runs'] if r['completed_messages']==18);a=fault['assessment'];check(a['execution_valid'] and a['history_verified'] and not a['foreground_ordering_passed'] and a['foreground_positive_job_fraction']==1 and a['foreground_outcomes']['ignored_stale']==6 and not a['useful_work_passed'] and not a['performance_passed'],'fault test accounting/scope')
report_source=str((ROOT/fault['path']).resolve());report_entry=index[report_source];check(report_entry['original_sha256']==fault['report_sha256'],'fault report byte binding')
fault_report=read(report_entry['archive']);evidence=Path(fault_report['runs'][0]['evidence_directory']);history_entry=index[str((evidence/'history.jsonl').resolve())];count=stale=positive=0
with gzip.open(BASE/history_entry['archive'],'rt') as stream:
 for line in stream:
  row=json.loads(line);o=row['receipt']['body']['outcome'];count+=1;stale+=o['ignored_stale'];positive+=bool(o['outputs']) or any(o[k]>0 for k in ['updated_views','created_views','expired_records','expired_families','claimed_events','cancelled_events','rescheduled_events'])
check((count,stale,positive)==(18,6,18),'fault rawtrace accounting')
checks.append('Final real-process fault18-message trace independently reproduces6 stale view updates and18 positive jobs; Valid serial history and failed usefulness remain separate.')
preservation=read('preservation-review.json');check(preservation['passed'] and preservation['unchanged_files']==39 and preservation['baseline']=='c0d6556','preservation claim')
implementation=read('implementation-review.json');reuse=implementation['development_evidence_reuse'];check(implementation['passed'] and not implementation['findings'] and reuse['total_preserved_tests']==13 and reuse['benchmark_bytes_identical'] and reuse['final_source_sha256']==SOURCE and reuse['benchmark_binary_sha256']==BINARY,'reused13 tests scope')
unit=read('unit-execution.json.gz');check(unit['passed'] and unit['source_stable'] and same_source(unit),'unit execution binding')
python=read('python-checks/receipt.json');check(python['passed'] and python['source_sha256']==SOURCE,'Python unit source binding')
for name,test_count in [('test_hyperfeed_qualification.py',44),('test_run_remote_contention.py',10)]:
 check(re.search(r'Ran '+str(test_count)+r' tests',(BASE/'python-checks'/(name+'.log')).read_text()) is not None,'Python unit count '+name)
text=(BASE/'model-and-measurement-tests.log').read_text();check('39 passed; 0 failed' in text and '7 passed; 0 failed' in text,'Rust unit counts')
check('Ran 2 tests' in (BASE/'affinity-tests.log').read_text(),'fault unit count')
readme=OUT/'README.pending.md';draft=readme.read_text();pending_links=[]
for link in re.findall(r'\]\(([^)]+)\)',draft):
 if '://' not in link and not (BASE/link).resolve().exists():pending_links.append(link)
check(all(link=='guardrails/README.md' for link in pending_links),'unexpected missing README links: '+str(pending_links))
for claim in ['36/36 trials','18 full histories','18 metrics histories','196 messages','All 18 messages','six fork updates','3,341 and 2,726','39 model tests','seven measurement tests','44 qualification tests','ten remote-helper tests','two real affinity tests','482-file']:
 check(claim in draft,'reviewed README claim changed/missing '+claim)
checks.append('Draft README tables/counts match reviews and logs. Guardrail completion wording/link is contingent on separately finishing/archiving the final pilot; this audit does not certify that pilot.')
receipt={'created_at':datetime.now(timezone.utc).isoformat(),'passed':not errors,'errors':errors,'manifest_sha256':file_hash(BASE/'artifact-manifest.json'),'readme_draft_sha256':file_hash(readme),'auditor_script_sha256':file_hash(Path(__file__)),'archive_files_checked':len(files),'gzip_wrapped_files_checked':compressed,'archive_bytes':archive_bytes,'original_bytes':original_bytes,'original_source_files_also_checked':original_checked,'source_sha256':SOURCE,'binary_sha256':BINARY,'source_tar_files':len(inventory),'development_changed_paths':changed,'checks':checks,'partial_trace_recheck':partial,'fault_trace_recheck':{'recorded_messages':count,'ignored_stale_views':stale,'positive_jobs':positive},'pending_readme_links':pending_links,'scope':'Independent read-only archive/content review. No benchmark, proof, semantic serial oracle or database action rerun. Main manifest excludes its own hash, pending README/root checksums, standalone audit and separately manifested guardrails. No capacity, speed ranking, whole-engine correctness or MMHF availability certification.'}
(OUT/'archive-audit.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'passed':not errors,'errors':errors,'files':len(files),'gzip':compressed,'archive_bytes':archive_bytes,'original_bytes':original_bytes,'pending_links':pending_links}))
raise SystemExit(bool(errors))
