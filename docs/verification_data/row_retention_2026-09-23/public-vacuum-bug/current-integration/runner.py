from pathlib import Path
import hashlib,json,os,re,subprocess,time
root=Path('/home/zpconn/code/aerostore');out=root/'target/verification-storage/public-api-integration';out.mkdir(parents=True,exist_ok=True)
env=dict(os.environ,RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1',RUSTUP_NO_UPDATE_CHECK='1')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files=sorted(json.loads(Path('/tmp/aerostore-vacuum-api-iw1_el02/before-receipt.json').read_text())['source_sha256'])
fingerprint=lambda:{name:sha(root/name) for name in files}
rustc=subprocess.check_output(['rustc','-Vv'],cwd=root,env=env,text=True)
assert 'release: 1.93.1\n' in rustc
rustc_path=Path(subprocess.check_output(['rustup','which','rustc'],cwd=root,env=env,text=True).strip())
report={'scope':'focused current-native public vacuum horizon and recycling regressions','passed':False,'completed':False,'rustc':rustc,'rustc_sha256':sha(rustc_path),'input_sha256':fingerprint(),'checks':[],'runner_sha256':sha(Path(__file__))}
path=out/'receipt.json';path.write_text(json.dumps(report,indent=2)+'\n')
for name,suite,test in [('public-horizon','occ_transactional_index','public_vacuum_clamps_caller_horizon_to_retained_snapshot'),('single-thread-recycling','occ_recycle_invariants_single_thread','single_thread_recycle_invariants_hold_under_update_reclaim_cycle')]:
 command=['cargo','test','--offline','--locked','--release','-p','aerostore_core','--test',suite,test,'--','--exact','--test-threads=1','--nocapture']
 start=time.monotonic(); result=subprocess.run(command,cwd=root,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 log=out/(name+'.log');log.write_text(result.stdout)
 binaries=[Path(s) for s in re.findall(r'Running[^\n]*\(([^)]+)\)',result.stdout)]
 binaries=[p if p.is_absolute() else root/p for p in binaries]
 passed=result.returncode==0 and '1 passed; 0 failed' in result.stdout and len(binaries)==1 and binaries[0].is_file()
 report['checks'].append({'name':name,'command':command,'exit_code':result.returncode,'passed':passed,'elapsed_seconds':time.monotonic()-start,'binary':str(binaries[0]) if binaries else None,'binary_sha256':sha(binaries[0]) if binaries else None,'log':str(log.relative_to(root)),'log_sha256':sha(log)})
 path.write_text(json.dumps(report,indent=2)+'\n')
 if not passed:raise RuntimeError(name+' did not pass intended one-test run')
report['final_input_sha256']=fingerprint();report['source_stable']=report['final_input_sha256']==report['input_sha256'];report['completed']=True;report['passed']=report['source_stable'] and all(c['passed'] for c in report['checks']);path.write_text(json.dumps(report,indent=2)+'\n')
(out/'runner.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps({'passed':report['passed'],'receipt':str(path),'checks':len(report['checks']),'source_stable':report['source_stable']}))
