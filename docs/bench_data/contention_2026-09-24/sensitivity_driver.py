import pathlib,subprocess,json,time,hashlib,gzip,shutil,os
root=pathlib.Path('/home/zpconn/code/aerostore');out=root/'target/contention-validation';base=json.loads((out/'campaign.json').read_text());binary=root/'target/release/deps/hyperfeed_contention_crucible-a7be51afa41635bf';url=os.environ['AEROSTORE_CONTENTION_PG_URL'];r={'completed':False,'passed':False,'architecture_promotion_eligible':False,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_sha256':base['source_after'],'runs':[]}
def save():(out/'sensitivity.json').write_text(json.dumps(r,indent=2)+'\n')
save()
trials=[(f'pg-10ms-{seed}','postgres','family',seed,5,100000) for seed in [20260924,20260925,20260926]]
trials += [(f'fixed-{plan}','aerostore',plan,20260924,60,1000) for plan in ['family','global-time']]
for name,engine,plan,seed,seconds,cap in trials:
 output=out/(name+'.json');cmd=[str(binary),'--engine',engine,'--mode','sustained','--seconds',str(seconds),'--max-messages',str(cap),'--families','16','--workers','4','--shm-mib','256','--seed',str(seed),'--query-plan',plan,'--output',str(output)];public=cmd.copy()
 if engine=='postgres':cmd+=['--pg-url',url+" options='-c deadlock_timeout=10ms'"];public+=['--pg-url',"<disposable local PostgreSQL URL> options='-c deadlock_timeout=10ms'"]
 print('START',name,flush=True);start=time.monotonic()
 with (out/(name+'.log')).open('w') as log:
  code=subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT,timeout=seconds+420).returncode
 report=json.loads(output.read_text());r['runs'].append({'name':name,'command':public,'exit_code':code,'wall_seconds':time.monotonic()-start,'report':str(output.relative_to(root)),'passed':code==0 and report.get('passed') is True,'explicit_pg_session_option':'deadlock_timeout=10ms' if engine=='postgres' else None});save();print('DONE',name,code,[(c.get('completed_messages'),c.get('retries'),c.get('message_latency_p99_us_including_retries'),c.get('error')) for c in report['runs']],flush=True)
 for c in report['runs']:
  p=pathlib.Path(c['evidence_directory'])/'history.jsonl'
  if p.exists():
   with p.open('rb') as src,gzip.open(str(p)+'.gz','wb',compresslevel=1) as dst:shutil.copyfileobj(src,dst)
   p.unlink()
r['source_stable']=all(hashlib.sha256((root/p).read_bytes()).hexdigest()==h for p,h in r['source_sha256'].items());r['completed']=True;r['passed']=r['source_stable'] and all(x['passed'] for x in r['runs']);save();print('SENSITIVITY',r['passed'],flush=True)
