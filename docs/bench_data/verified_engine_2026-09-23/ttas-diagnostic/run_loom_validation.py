from pathlib import Path
import hashlib,json,os,re,subprocess,time
root=Path('/home/zpconn/code/aerostore');work=root/'target/verification-next/candidate-ttas';receipt=work/'validation/receipt.json';state=json.loads(receipt.read_text());state['status']='running';receipt.write_text(json.dumps(state,indent=2)+'\n')
digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
env=dict(os.environ);env.update(RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1',RUSTFLAGS='--cfg aerostore_loom',RUSTUP_NO_UPDATE_CHECK='1')
env.pop('CARGO_ENCODED_RUSTFLAGS',None)
state['environment']={k:env[k] for k in ['RUSTUP_HOME','CARGO_HOME','RUSTUP_TOOLCHAIN','RUSTFLAGS']}
state['runner_sha256']=digest(Path(__file__))
try:
 for name,source,filter_name,expected in [('candidate',work/'source',None,0),('relaxed_acquire_mutant',work/'mutant-source','contended_handoff_publishes_protected_non_atomic_value',101)]:
  cmd=['cargo','test','--offline','--locked','-p','aerostore_core','--release','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(work/('loom-build-'+name+'-final')),'--message-format=json','--test','shm_mutation_model']
  if filter_name:cmd.append(filter_name)
  cmd+=['--','--nocapture']
  assert not (work/('loom-build-'+name+'-final')).exists(), 'fresh validation requires a new target'
  started=time.monotonic();log=work/'validation'/f'{name}.log'
  with log.open('w') as out:
   result=subprocess.run(cmd,cwd=source,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=600)
  body=log.read_text();artifacts=[]
  for line in body.splitlines():
   try: event=json.loads(line)
   except ValueError: continue
   if event.get('reason')=='compiler-artifact' and event.get('target',{}).get('name')=='shm_mutation_model' and event.get('executable'): artifacts.append(event)
  assert len(artifacts)==1 and not artifacts[0]['fresh'], ('test binary was not freshly compiled',artifacts)
  artifact=artifacts[0]; assert Path(artifact['target']['src_path']).resolve()==(source/'aerostore_core/tests/shm_mutation_model.rs').resolve()
  state['checks'].append({'name':name,'command':cmd,'cwd':str(source),'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-started,'log_sha256':digest(log),'source_sha256':digest(source/'aerostore_core/src/shm_lock.rs'),'expected_exit_code':expected,'fresh_test_binary':True,'executable':artifact['executable'],'executable_sha256':digest(Path(artifact['executable']))})
  receipt.write_text(json.dumps(state,indent=2)+'\n')
  assert result.returncode==expected,(name,result.returncode)
  if name=='candidate':assert 'test result: ok. 7 passed; 0 failed' in body,body[-2000:]
  else:assert 'Causality violation' in body or 'Concurrent read' in body or 'Concurrent write' in body,body[-2000:]
 for name,expected in state['main_source_sha256'].items():assert digest(root/name)==expected,('main source changed',name)
 assert digest(work/'source/aerostore_core/src/shm_lock.rs')==state['candidate_shm_lock_sha256']
 assert digest(work/'mutant-source/aerostore_core/src/shm_lock.rs')==state['mutant_shm_lock_sha256']
 state.update(passed=True,status='passed',main_source_unchanged=True,source_stable=True)
except Exception as error:state.update(passed=False,status='failed',error=str(error))
receipt.write_text(json.dumps(state,indent=2)+'\n');print(json.dumps({'passed':state['passed'],'status':state['status'],'error':state.get('error'),'receipt':str(receipt)}));raise SystemExit(0 if state['passed'] else 1)
