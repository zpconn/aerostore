#!/usr/bin/env python3
"""Read-only audit of completed maintenance artifacts and component proof archive.

Writes only its audit receipt under target. Does not rerun benchmarks/proofs,
extract files, alter the archive or infer semantics from matching hashes.
"""
from pathlib import Path
from datetime import datetime, timezone
import gzip, hashlib, json, re, tarfile
ROOT=Path.cwd();OUT=ROOT/'target/maintenance-final-validation';BASE=ROOT/'docs/bench_data/maintenance_2026-09-25'
SOURCE='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
DEVELOPMENT='c7ce01d396edbc3d93a7987cb284afa7ba1b834d78c439c7d55539ef8ebf4e44'
BINARY='c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28'
errors=[];checks=[];source_index={};archive_index={}
def check(condition,message):
 if not condition:errors.append(message)
def stream_hash(stream):
 h=hashlib.sha256();size=0
 for block in iter(lambda:stream.read(1<<20),b''):h.update(block);size+=len(block)
 return h.hexdigest(),size
def hash_file(path):
 with path.open('rb') as stream:return stream_hash(stream)[0]
def safe(name):return not Path(name).is_absolute() and '..' not in Path(name).parts
def archived_path(name):
 path=BASE/name
 return path if path.exists() else Path(str(path)+'.gz')
def read(name):
 path=archived_path(name)
 with (gzip.open(path,'rt') if path.suffix=='.gz' else path.open()) as stream:return json.load(stream)
def text(name):
 path=archived_path(name)
 with (gzip.open(path,'rt') if path.suffix=='.gz' else path.open()) as stream:return stream.read()
def original(name):
 path=str(Path(name).resolve());entry=source_index.get(path)
 check(entry is not None,'missing original artifact '+path)
 return None if entry is None else BASE/entry['archive']
def same_source(data,expected=SOURCE):return data['source_before']==data['source_after'] and data['source_before']['sha256']==expected
manifest=read('artifact-manifest.json');entries=manifest['files']
check(manifest['completed'] and manifest['source_sha256']==SOURCE and manifest['binary_sha256']==BINARY,'main manifest binding')
compressed=0;original_bytes=0;archive_bytes=0
for entry in entries:
 name=entry['archive'];check(safe(name),'unsafe archive path '+name);check(name not in archive_index,'duplicate archive path '+name)
 path=BASE/name;archive_index[name]=entry;source_index[str((ROOT/entry['source']).resolve())]=entry
 check(path.is_file() and not path.is_symlink(),'missing/symlink artifact '+name)
 if not path.is_file():continue
 h=hash_file(path);size=path.stat().st_size
 check(h==entry['archive_sha256'] and size==entry['archive_bytes'],'archive bytes changed '+name)
 if entry['compression']=='gzip':
  compressed+=1
  with gzip.open(path,'rb') as stream:decoded,decoded_size=stream_hash(stream)
 else:
  check(entry['compression'] is None,'unknown compression '+name);decoded,decoded_size=h,size
  if path.suffix=='.gz':
   with gzip.open(path,'rb') as stream:stream_hash(stream)
 check(decoded==entry['original_sha256'] and decoded_size==entry['original_bytes'],'decompressed original mismatch '+name)
 original_bytes+=decoded_size;archive_bytes+=size
check(len(source_index)==len(entries),'duplicate original source mapping')
checks.append('Every main manifest entry independently hashed, decompressed and counted; compressed trailers checked to EOF.')
def tar_inventory(path,hardlinks=False):
 values={}
 with tarfile.open(path,'r|gz') as archive:
  for member in archive:
   check(safe(member.name) and member.name not in values,'unsafe/duplicate tar member '+member.name)
   if member.islnk():
    check(hardlinks and safe(member.linkname) and member.linkname in values,'unsafe/unresolved hardlink '+member.name)
    if member.linkname not in values:continue
    value=values[member.linkname]
   else:
    check(member.isfile(),'non-file tar member '+member.name)
    if not member.isfile():continue
    with archive.extractfile(member) as stream:h,n=stream_hash(stream)
    value={'sha256':h,'bytes':n}
   values[member.name]=value
 return values
build=read('build-provenance.json');check(build['exit_code']==0 and same_source(build) and build['binary_sha256']==BINARY,'final build binding')
final_inventory=tar_inventory(BASE/'source.tar.gz');final_hashes={p:v['sha256'] for p,v in final_inventory.items()}
check(len(final_hashes)==484 and final_hashes==build['source_before']['files'],'484-member final source inventory')
check(hashlib.sha256(json.dumps(final_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()==SOURCE,'final canonical source hash')
dev_inventory=tar_inventory(BASE/'development/source.tar.gz');dev_hashes={p:v['sha256'] for p,v in dev_inventory.items()}
changed=sorted(set(final_hashes)^set(dev_hashes)|{p for p in final_hashes.keys()&dev_hashes.keys() if final_hashes[p]!=dev_hashes[p]})
check(changed==['verification/frozen_boundary.json'],'development differs outside reviewed boundary lock')
check(len(dev_hashes)==484 and hashlib.sha256(json.dumps(dev_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()==DEVELOPMENT,'development canonical source hash')
devbuild=read('development/build-provenance.json');check(devbuild['exit_code']==0 and same_source(devbuild,DEVELOPMENT) and devbuild['source_before']['files']==dev_hashes and devbuild['binary_sha256']==BINARY,'development source/build identity')
reuse=read('process-evidence-reuse.json');check(reuse['passed'] and reuse['changed_paths']==changed and reuse['final_source_sha256']==SOURCE and reuse['development_source_sha256']==DEVELOPMENT and reuse['identical_binary_sha256']==BINARY,'process evidence reuse scope')
for source,expected in reuse['process_receipts'].items():
 entry=source_index.get(str((ROOT/source).resolve()));check(entry is not None and entry['original_sha256']==expected,'reused process receipt hash '+source)
checks.append('Final and development source archives contain exactly484 inputs; only reviewed boundary lock differs and build receipts identify identical executable bytes.')
labels={'identity-mixed-full':9,'identity-mixed-metrics':9,'affinity-mixed-full':9,'affinity-mixed-metrics':9,'sensitivity-p1-h16-full':3,'sensitivity-p16-h64-full':3,'sensitivity-identity-full':6,'sensitivity-affinity-full':6,'cadence-native':1,'cadence-postgres':1}
trial_count=valid_count=fail_count=metrics_count=0
for label,count in labels.items():
 campaign=read(label+'/campaign.json');check(campaign['completed'] and campaign['source_stable'] and same_source(campaign),'campaign completion/source '+label)
 check(campaign['binary_before_sha256']==campaign['binary_after_sha256']==BINARY and len(campaign['trials'])==count,'campaign binary/count '+label)
 stress=label in ('sensitivity-identity-full','sensitivity-affinity-full')
 for trial in campaign['trials']:
  trial_count+=1;assessment=trial['assessment'];valid=assessment['execution_valid'];valid_count+=valid;fail_count+=not valid
  check(trial['source_before_sha256']==trial['source_after_sha256']==SOURCE and trial['binary_before_sha256']==trial['binary_after_sha256']==BINARY,'individual trial binding '+label)
  check(not assessment['qualified_capacity_trial'] and not assessment['capacity_failure'],'capacity scope '+label)
  if not stress:check(valid and assessment['correctness_companion_verified'],'functional/cadence completion '+label)
  report=trial['report'];run=report['runs'][0];case=Path(run['evidence_directory'])
  for name in ('report.json','run.log','trial.json'):original(Path(trial['directory'])/name)
  for name in ('history.jsonl','offered-schedule.json'):original(case/name)
  if valid:
   for name in ('initial.json','final.json','serial-witness.json'):original(case/name)
   full=trial['config']['evidence']=='full';metrics_count+=not full
   check(assessment['history_verified']==full,'full/metrics history scope '+label)
   check(run['store_metrics']['commits']==run['completed_transactions'],'commits/transactions '+label)
  else:check(run.get('oracle_status') is None and not assessment['history_verified'],'failed case incorrectly verified '+label)
check((trial_count,valid_count,fail_count,metrics_count)==(56,48,8,18),'42 functional+12stress+2cadence completeness')
functional=read('campaign-review.json');check(functional['passed'] and not functional['errors'] and len(functional['trials'])==42,'independent42 functional review')
check(functional['source_sha256']==SOURCE and functional['binary_sha256']==BINARY,'functional review binding')
for field in ('initial_seed_groups','foreground_seed_groups'):
 check(len(functional[field])==3 and all(len({v['sha256'] for v in group})==1 for group in functional[field].values()),'seed/corpus grouping '+field)
stress=read('sensitivity-review.json');check(stress['review_passed'] and not stress['review_errors'] and not stress['all_cells_succeeded'],'failure-aware diagnostic review')
check(stress['reviewed_cells']==12 and stress['execution_valid_cells']==4 and stress['oracle_unavailable_cells']==8 and stress['oracle_invalid_cells']==stress['oracle_inconclusive_cells']==0,'stress verdict counts')
check(stress['primary_causes']=={'none':4,'transaction_retry_budget_exhausted':6,'admitted_backlog_bound_exceeded':2},'stress failure counts')
partial=next(c for c in stress['cells'] if c['dispatch']=='signature-affinity' and c['engine']=='aerostore' and c['arrival_rate']==2048)
job=partial['failed_job'];check(job['batch_index']==3 and job['received_prior_committed_batches']==3 and job['received_prior_processed_rows']==12 and not job['terminal_received_for_failed_job'],'partial committed batches not completed job')
for cell in stress['cells']:
 for relative,expected in cell['artifacts_sha256'].items():
  entry=source_index.get(str((OUT/relative).resolve()));check(entry is not None and entry['original_sha256']==expected,'stress retained artifact hash '+relative)
cadence=read('cadence-review.json');check(cadence['passed'] and cadence['source_sha256']==SOURCE and cadence['binary_sha256']==BINARY and len(cadence['runs'])==2,'cadence review binding')
for run in cadence['runs']:
 check(run['completed_jobs']==19236 and run['oracle_status']=='Valid' and run['representative_cadence_coverage'] and not run['capacity_qualified'],'cadence verdict')
 check(len(run['maintenance_ticks'])==4 and all(t['processed_rows']>0 and t['offset_seconds'] in (300,600) for t in run['maintenance_ticks']),'cadence positive complete timers')
 for name,expected in run['artifacts'].items():
  entry=source_index.get(str((Path(run['evidence_directory'])/name).resolve()));check(entry is not None and entry['original_sha256']==expected,'cadence retained artifact hash '+name)
loop=read('loopback/orchestration.json');check(loop['passed'] and loop['completed'] and loop['server_resources_cleanly_drained'] and loop['binary_sha256']==BINARY and loop['topology']=='tcp_loopback' and not loop['physical_hosts_independently_verified'],'TCP scope/completion')
check(functional['loopback']['completed_jobs']==196 and functional['loopback']['terminal_empty_queries']==4,'TCP complete jobs')
checks.append('All56 matrix/cadence trials and TCP retained, including all8diagnostic failures;18metrics histories remain unverified. Complete job/transaction scope and real300/600s positive sweeps checked.')
unit=read('unit-tests-execution.json');check(unit['passed'] and unit['source_stable'] and same_source(unit) and unit['binary_before_sha256']==unit['binary_after_sha256']==BINARY,'final unit binding')
for step in unit['steps']:
 entry=source_index.get(str(Path(step['log']).resolve()));check(step['exit_code']==0 and entry is not None and entry['original_sha256']==step['log_sha256'],'unit log receipt '+step['name'])
check('51 passed; 0 failed' in text('unit-tests/rust-model-measurement.log') and '7 passed; 0 failed' in text('unit-tests/rust-model-measurement.log'),'Rust model/measurement58')
check('Ran 51 tests' in text('unit-tests/python-qualification.log') and 'Ran 12 tests' in text('unit-tests/python-remote.log'),'Python63')
process=read('development/maintenance-tests-execution.json');check(process['passed'] and process['tests']==4 and process['expected_source_sha256']==DEVELOPMENT and process['binary_sha256']==BINARY,'four new real-process tests')
check('Ran 4 tests' in text('development/maintenance-tests.log'),'new process log count')
preserved=read('development/preserved-tests/execution.json');check(preserved['passed'] and preserved['source_stable'] and same_source(preserved,DEVELOPMENT) and preserved['binary_before_sha256']==preserved['binary_after_sha256']==BINARY,'15preserved process binding')
for name,n in [('integration',11),('pause',2),('affinity',2)]:check('Ran '+str(n)+' tests' in text('development/preserved-tests/'+name+'.log'),'preserved process count '+name)
for name in ('batch-cap-after-commit','paused-projection-drain','sweep-full','bounded-batch-control','companion-full','companion-metrics'):
 check(any('/'+name+'/report.json' in entry['source'] for entry in entries),'omitted maintenance negative/control case '+name)
checks.append('Final121 unit checks and reused19 process regressions have matching archived receipts/logs; deliberate cap failure and pause evidence retained.')
formal=read('guardrails/manifest.json');report=read('guardrails/report.json');execution=read('guardrails/execution-receipt.json');pathmap=read('guardrails/archive-path-map.json');fingerprint=read('guardrails/source-fingerprint.json')
check(formal['passed'] and formal['source_stable'] and formal['checks']==71 and formal['source_inputs']==525 and formal['main_source_snapshot_sha256']==SOURCE,'formal manifest scope/binding')
check(report['completed'] and report['passed'] and report['source_stable'] and len(report['checks'])==71 and all(c['passed'] for c in report['checks']),'71component checks')
check(report['source_before']==report['source_after']==formal['input_sha256']==fingerprint['before']==fingerprint['after'],'formal source fingerprints')
check(report['p0_complete'] and not report['full_P1_complete'] and not report['whole_engine_verified'] and not report['promotion_eligible'] and report['anchoring']=='local_bootstrap_only','formal component-only declaration')
check(execution['completed'] and execution['passed'] and execution['source_stable'] and same_source(execution),'formal execution final source')
proof_tar=BASE/'guardrails/formal-evidence.tar.gz';check(hash_file(proof_tar)==formal['archive_sha256']==pathmap['archive_sha256'],'formal archive SHA')
proof_inventory=tar_inventory(proof_tar,hardlinks=True)
check(len(proof_inventory)==formal['archived_file_count']==formal['verified_archive_members'] and {p:v['sha256'] for p,v in proof_inventory.items()}==formal['artifact_sha256'],'formal logical member hashes')
expected_map={row['archive_member']:{'sha256':row['sha256'],'bytes':row['bytes']} for row in pathmap['files']};check(proof_inventory==expected_map,'formal pathmap sizes/hashes')
for name,expected in formal['standalone_sha256'].items():check(hash_file(BASE/'guardrails'/name)==expected,'formal standalone hash '+name)
bindings=pathmap['source_bindings'];extras=bindings['extra_inputs'];check(len(extras)==41 and bindings['full_proof_inputs']==525 and bindings['main_source_files']==484 and bindings['main_source_snapshot_sha256']==SOURCE,'formal484+41input partition')
check(bindings['main_archive_sha256']==hash_file(BASE/'source.tar.gz'),'formal main source archive hash')
reconstructed=final_hashes.copy()
for name in extras:
 member='proof-inputs-extra/'+name
 check(name not in reconstructed and member in proof_inventory,'invalid/missing extra proof input '+name)
 if member in proof_inventory:reconstructed[name]=proof_inventory[member]['sha256']
check(reconstructed==formal['input_sha256'] and len(reconstructed)==525,'reconstructed525 proof input binding')
checks.append('Every formal logical tar member/hardlink, standalone receipt and525inputs independently reconstructed from484source+41extras;71checks/P0/local-bootstrap scope confirmed, fullP1 and whole-engine false.')
# Local markdown links are checked after root adds final cadence/formal paragraphs.
missing=[]
for doc in [ROOT/'README.md',ROOT/'docs/hyperfeed_maintenance.md',BASE/'README.md',BASE/'guardrails/README.md']:
 for link in re.findall(r'\]\(([^)]+)\)',doc.read_text()):
  target=link.split('#',1)[0]
  if not target or '://' in target or target.startswith('mailto:'):continue
  candidate=Path(target) if target.startswith('/') else doc.parent/target
  if not candidate.exists():missing.append(str(doc.relative_to(ROOT))+': '+link)
check(not missing,'missing documentation links: '+str(missing))
receipt={'created_at':datetime.now(timezone.utc).isoformat(),'passed':not errors,'errors':errors,'auditor_script_sha256':hash_file(Path(__file__)),'main_manifest_sha256':hash_file(BASE/'artifact-manifest.json'),'formal_manifest_sha256':hash_file(BASE/'guardrails/manifest.json'),'source_sha256':SOURCE,'binary_sha256':BINARY,'archive_files_checked':len(entries),'gzip_wrapped_files_checked':compressed,'archive_bytes':archive_bytes,'original_bytes':original_bytes,'final_source_files':len(final_hashes),'development_changed_paths':changed,'trials_checked':trial_count,'successful_trials':valid_count,'retained_failed_trials':fail_count,'metrics_histories_unverified':metrics_count,'formal_members_checked':len(proof_inventory),'formal_inputs_reconstructed':len(reconstructed),'checks':checks,'documentation_files_sha256':{str(p.relative_to(ROOT)):hash_file(p) for p in [ROOT/'README.md',ROOT/'docs/hyperfeed_maintenance.md',BASE/'README.md',BASE/'guardrails/README.md']},'scope':'Read-only byte/inventory/receipt/documentation audit. No runtime or proof source changed; no benchmarks/proofs/semantic oracle rerun. Archive omissions remain explicit. Confirms retained reported evidence scope, not whole-engine correctness, speed, capacity, survivor continuity or physical MMHF.'}
(OUT/'archive-audit.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'passed':not errors,'errors':errors,'main_files':len(entries),'trials':trial_count,'formal_members':len(proof_inventory),'formal_inputs':len(reconstructed)}))
raise SystemExit(bool(errors))
