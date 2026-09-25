import hashlib,json,pathlib,subprocess,time,os,sys,gzip,shutil
root=pathlib.Path('/home/zpconn/code/aerostore'); out=root/'target/contention-validation';out.mkdir(parents=True,exist_ok=True)
def hashes():
 paths=[root/'Cargo.toml',root/'Cargo.lock']
 for d in ['aerostore_core','aerostore_verified','aerostore_macros','aerostore_tcl']:
  paths += [p for p in (root/d).rglob('*') if p.is_file() and 'target' not in p.relative_to(root).parts and (p.suffix=='.rs' or p.name=='Cargo.toml')]
 return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
r={'completed':False,'passed':False,'source_before':hashes(),'commands':[],'runs':[],'architecture_promotion_eligible':False}
def save(): (out/'campaign.json').write_text(json.dumps(r,indent=2)+'\n')
save()
cmd=['cargo','bench','--offline','-p','aerostore_core','--bench','hyperfeed_contention_crucible','--no-run'];r['commands'].append(cmd)
with (out/'build.log').open('w') as log:
 p=subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT,timeout=300)
if p.returncode: save();sys.exit(p.returncode)
binary=root/'target/release/deps/hyperfeed_contention_crucible-a7be51afa41635bf'
r['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest();r['rustc']=subprocess.check_output(['rustc','-Vv'],text=True);r['git_revision']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip();save()
url=os.environ['AEROSTORE_CONTENTION_PG_URL']
trials=[('scenarios','both','family',20260924,2,0)]
for seed in [20260924,20260925,20260926]:
 engines=['aerostore','postgres'] if seed%2==0 else ['postgres','aerostore']
 trials.extend((f'load-{seed}-{engine}',engine,'family',seed,5,0) for engine in engines)
 trials.append((f'broad-{seed}','aerostore','global-time',seed,5,0))
trials += [('retention-aerostore','aerostore','family',20260924,120,10000),('retention-postgres','postgres','family',20260924,120,10000)]
for name,engine,plan,seed,seconds,interval in trials:
 output=out/(name+'.json')
 cmd=[str(binary),'--engine',engine,'--mode','scenarios' if name=='scenarios' else 'sustained','--workers','4','--families','16','--seconds',str(seconds),'--max-messages','100000','--shm-mib','256','--seed',str(seed),'--query-plan',plan,'--message-interval-us',str(interval),'--output',str(output)]
 public=cmd.copy()
 if engine!='aerostore':cmd+=['--pg-url',url];public+=['--pg-url','<disposable local PostgreSQL URL>']
 print('START',name,flush=True);r['commands'].append(public);save();start=time.monotonic()
 with (out/(name+'.log')).open('w') as log:
  try:code=subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT,timeout=seconds+420).returncode
  except subprocess.TimeoutExpired:code=-999
 report=json.loads(output.read_text()) if output.exists() else {}
 r['runs'].append({'name':name,'exit_code':code,'wall_seconds':time.monotonic()-start,'report':str(output.relative_to(root)),'passed':code==0 and report.get('passed') is True});save()
 print('DONE',name,code,[(c.get('completed_messages'),c.get('retries'),c.get('message_latency_p99_us_including_retries'),c.get('error')) for c in report.get('runs',[])],flush=True)
 # Full transaction receipts are retained compressed; compression is outside all timed phases.
 for c in report.get('runs',[]):
  path=pathlib.Path(c['evidence_directory'])/'history.jsonl'
  if path.exists():
   with path.open('rb') as src, gzip.open(str(path)+'.gz','wb',compresslevel=1) as dst:shutil.copyfileobj(src,dst)
   path.unlink()
r['source_after']=hashes();r['source_stable']=r['source_before']==r['source_after'];r['completed']=True;r['passed']=r['source_stable'] and all(x['passed'] for x in r['runs']);save();print('CAMPAIGN',r['passed'],flush=True)
