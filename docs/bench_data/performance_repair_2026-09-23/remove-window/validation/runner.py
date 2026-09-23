from pathlib import Path
import hashlib, json, os, re, subprocess, time, sys
root=Path('/home/zpconn/code/aerostore')
out=root/'target/performance-repair/remove-window'
validation=out/'validation';validation.mkdir(exist_ok=True)
source=out/'source';baseline=out/'baseline-test-source';mutant=out/'mutant-source'
case='shm_skiplist::tests::removal_window_detaches_every_lane_before_pinned_retirement_and_shorter_reuse'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def inputs(folder):
 paths=[folder/'Cargo.toml',folder/'Cargo.lock']
 for member in ['aerostore_core','aerostore_macros','aerostore_verified','aerostore_tcl']:
  paths += [p for p in (folder/member).rglob('*') if p.is_file() and not any(x in {'target','__pycache__'} for x in p.relative_to(folder).parts)]
 return {str(p.relative_to(folder)):sha(p) for p in sorted(paths)}
r={'passed':False,'status':'running','parent_commit':'9676fa9ff602565d5a628af41b6d40ba1576a8b9','scope':'native removal regression and bounded actual-lock models; no full native refinement','checks':[],'source_before':inputs(source),'main_before':inputs(root),'sources':{str(p):inputs(p) for p in [baseline,mutant]},'patch_sha256':sha(out/'candidate.patch'),'runner_sha256':sha(Path(__file__))}
def persist(): (validation/'receipt.json').write_text(json.dumps(r,indent=2)+'\n')
persist()
env=dict(os.environ)
env.update(RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1')
sysroot=Path(subprocess.check_output(['rustc','--print','sysroot'],env=env,text=True).strip())
cargo=sysroot/'bin/cargo';rustc=sysroot/'bin/rustc'
env['RUSTC']=str(rustc)
r['rustc']=subprocess.check_output([str(rustc),'-Vv'],text=True);r['tool_sha256']={str(p):sha(p) for p in [rustc,cargo]}
r['environment']={k:env[k] for k in ['RUSTUP_HOME','CARGO_HOME','RUSTUP_TOOLCHAIN','RUSTC']}
def run(name,command,cwd,expected=0,timeout=600):
 log=validation/(name+'.log');start=time.monotonic()
 with log.open('w') as stream: result=subprocess.run(command,cwd=cwd,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=timeout)
 c={'name':name,'command':command,'cwd':str(cwd),'exit_code':result.returncode,'expected_exit_code':expected,'elapsed_seconds':time.monotonic()-start,'log':str(log),'log_sha256':sha(log)}
 r['checks'].append(c);persist()
 assert result.returncode==expected,(name,log)
 return c,log.read_text()
def build(name,folder):
 target=out/('build-'+name);assert not target.exists()
 c,text=run(name+'-build',[str(cargo),'test','--offline','--locked','--release','--manifest-path',str(folder/'Cargo.toml'),'--target-dir',str(target),'-p','aerostore_core','--lib','--no-run','--message-format=json'],folder)
 artifacts=[]
 for line in text.splitlines():
  try:event=json.loads(line)
  except ValueError:continue
  if event.get('reason')=='compiler-artifact' and event.get('target',{}).get('name')=='aerostore_core' and event.get('executable'):artifacts.append(event)
 assert len(artifacts)==1
 a=artifacts[0];assert a['fresh'] is False
 assert Path(a['target']['src_path'])==folder/'aerostore_core/src/lib.rs'
 exe=Path(a['executable']);assert exe.is_relative_to(target)
 c.update(executable=str(exe),executable_sha256=sha(exe),fresh_test_binary=True);persist()
 return exe,target
try:
 baseline_exe,_=build('baseline-tests',baseline)
 _,text=run('baseline-new-regression',[str(baseline_exe),'--exact',case,'--nocapture','--test-threads=1'],baseline)
 assert '1 passed; 0 failed; 0 ignored' in text and 'running 1 test' in text
 candidate_exe,target=build('candidate',source)
 _,text=run('candidate-skiplist-suite',[str(candidate_exe),'shm_skiplist::tests','--nocapture','--test-threads=1'],source)
 assert re.search(r'test result: ok\. [1-9][0-9]* passed; 0 failed;',text) and case in text
 tests=['index_relink_gc_consistency','shm_index_gc_horizon','shm_index_bounds','shm_index_churn','shm_index_contention','shm_index_fork','shm_recycle_visibility','occ_transactional_index','wal_protocol_regressions']
 command=[str(cargo),'test','--offline','--locked','--release','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(target),'-p','aerostore_core']
 for test in tests:command += ['--test',test]
 command += ['--','--test-threads=1']
 _,text=run('candidate-native-regressions',command,source,timeout=600)
 summaries=re.findall(r'test result: ok\. ([0-9]+) passed; 0 failed;',text)
 assert len(summaries)==len(tests) and all(int(x)>0 for x in summaries)
 mutant_exe,_=build('upper-lane-mutant',mutant)
 assert sha(mutant_exe)!=sha(candidate_exe)
 _,text=run('upper-lane-mutant-rejected',[str(mutant_exe),'--exact',case,'--nocapture','--test-threads=1'],mutant,expected=101)
 assert 'running 1 test' in text and '0 passed; 1 failed; 0 ignored' in text and 'removed node remains reachable in lane 1' in text
 # Reuse the mandatory fresh-artifact runner, with its production environment root.
 del env['RUSTC']
 _,text=run('actual-lock-models',['python3',str(source/'scripts/check_lock_models.py'),'--root',str(source),'--environment-root',str(root),'--output',str(source/'target/lock-models')],source,timeout=600)
 lock=json.loads((source/'target/lock-models/receipt.json').read_text())
 assert lock['passed'] and lock['source_stable'] and len(lock['checks'])==8
 r['lock_receipt_sha256']=sha(source/'target/lock-models/receipt.json')
 r['source_after']=inputs(source);r['main_after']=inputs(root)
 assert r['source_before']==r['source_after'] and r['main_before']==r['main_after']
 assert all(inputs(Path(path))==before for path,before in r['sources'].items())
 assert all(sha(Path(path))==value for path,value in r['tool_sha256'].items())
 r.update(passed=True,status='passed',source_stable=True,main_source_stable=True)
except BaseException as error:
 r.update(status='failed',error=repr(error));persist();raise
finally:persist()
print(json.dumps({'passed':r['passed'],'receipt':str(validation/'receipt.json'),'checks':len(r['checks'])}))
