import pathlib,json,hashlib,shutil,gzip,statistics
root=pathlib.Path('/home/zpconn/code/aerostore');src=root/'target/contention-validation';dst=root/'docs/bench_data/contention_2026-09-24';dst.mkdir(parents=True,exist_ok=True)
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
manifest={'format':1,'artifacts':[],'omitted_large_histories':[],'meaning':'Raw reports remain unchanged; their absolute evidence paths are mapped below. Large successful histories remain locally under target; all bounded/failure histories are archived.'}
for name in ['campaign.json','sensitivity.json','baseline-audit.json','regression-validation.json','extended-baseline.json','extended-baseline.log','model-tests.log','postgres-tests.log','postgres-external-settings.json','postgres-tuned-settings.json','build.log']:
 p=src/name
 if p.exists():
  shutil.copyfile(p,dst/name);manifest['artifacts'].append({'source':str(p.relative_to(root)),'archive':name,'sha256':digest(p)})
trials=[]
for campaign in ['campaign.json','sensitivity.json']:
 for run in json.loads((src/campaign).read_text())['runs']:
  p=root/run['report'];report=json.loads(p.read_text());name=run['name'];trials.append((name,report));shutil.copyfile(p,dst/p.name)
  manifest['artifacts'].append({'source':str(p.relative_to(root)),'archive':p.name,'sha256':digest(p)})
  log=src/(name+'.log')
  if log.exists():shutil.copyfile(log,dst/log.name)
  for i,c in enumerate(report['runs']):
   directory=pathlib.Path(c['evidence_directory']);case=dst/'cases'/name/f'{i}-{c["engine"]}';case.mkdir(parents=True,exist_ok=True)
   for artifact in sorted(directory.iterdir()):
    if artifact.name not in ['history.jsonl.gz','initial.json','final.json','serial-witness.json','progress.json','result.json']:continue
    item={'source':str(artifact.relative_to(root)),'sha256':digest(artifact),'bytes':artifact.stat().st_size}
    if artifact.name=='history.jsonl.gz' and artifact.stat().st_size>10*1024*1024:
     item['archived']=False;manifest['omitted_large_histories'].append(item);continue
    target=case/(artifact.name+'.gz' if artifact.suffix=='.json' else artifact.name)
    if artifact.suffix=='.json':
     with artifact.open('rb') as r,gzip.open(target,'wb',compresslevel=9) as w:shutil.copyfileobj(r,w)
    else:shutil.copyfile(artifact,target)
    item.update({'archive':str(target.relative_to(dst)),'archive_sha256':digest(target),'archived':True});manifest['artifacts'].append(item)
summary={'architecture_promotion_eligible':False,'whole_engine_verified':False,'required_worker_failure_availability_satisfied':False,'groups':{},'failures':[]}
groups={}
for name,r in trials:
 for c in r['runs']:
  if not c['passed']:summary['failures'].append({'trial':name,**c});continue
  if name.startswith('load-'):group='unpaced-family-'+c['engine']
  elif name.startswith('broad-'):group='unpaced-global-time-aerostore'
  elif name.startswith('pg-10ms'):group='unpaced-family-postgres-deadlock-10ms'
  elif name.startswith('retention-'):group=name
  elif name.startswith('fixed-'):group=name
  else:continue
  groups.setdefault(group,[]).append(c)
for name,cs in groups.items():
 summary['groups'][name]={'successful_runs':len(cs),'completed_messages':[c['completed_messages'] for c in cs], 'median_messages_per_second':statistics.median(c['completed_messages_per_second'] for c in cs),'median_p99_ms_including_retries':statistics.median(c['message_latency_p99_us_including_retries']/1000 for c in cs),'median_retries_per_message':statistics.median(c['retries']/c['completed_messages'] for c in cs)}
(dst/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');(dst/'archive-path-map.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(summary['groups'],indent=2));print('archive MB',sum(p.stat().st_size for p in dst.rglob('*') if p.is_file())/1048576,'omitted',len(manifest['omitted_large_histories']))
