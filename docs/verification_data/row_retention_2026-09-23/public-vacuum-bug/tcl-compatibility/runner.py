from pathlib import Path
import os,subprocess,hashlib,json,re,time
root=Path('/home/zpconn/code/aerostore');out=root/'target/verification-storage/public-api-tcl'
env=dict(os.environ,RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1',RUSTUP_NO_UPDATE_CHECK='1')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files=sorted(json.loads((root/'target/verification-storage/public-api-integration/receipt.json').read_text())['input_sha256'])
fingerprint=lambda:{n:sha(root/n) for n in files}
command=['cargo','test','--offline','--locked','--release','-p','aerostore_tcl','--lib','vacuum_index_cleanup_tests','--','--test-threads=1','--nocapture']
report={'scope':'additional Tcl public-vacuum caller compatibility tests','completed':False,'passed':False,'command':command,'input_sha256':fingerprint(),'rustc':subprocess.check_output(['rustc','-Vv'],env=env,cwd=root,text=True),'runner_sha256':sha(Path(__file__))}
path=out/'receipt.json';path.write_text(json.dumps(report,indent=2)+'\n')
start=time.monotonic();r=subprocess.run(command,cwd=root,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
log=out/'vacuum_index_cleanup_tests.log';log.write_text(r.stdout)
binaries=[Path(s) for s in re.findall(r'Running[^\n]*\(([^)]+)\)',r.stdout)];binaries=[p if p.is_absolute() else root/p for p in binaries]
report.update(exit_code=r.returncode,elapsed_seconds=time.monotonic()-start,log=str(log.relative_to(root)),log_sha256=sha(log),completed=True)
report['final_input_sha256']=fingerprint();report['source_stable']=report['final_input_sha256']==report['input_sha256']
report['test_names']=re.findall(r'^test (vacuum_index_cleanup_tests::\w+) \.\.\. ok$',r.stdout,re.M)
report['passed']=r.returncode==0 and len(report['test_names'])==4 and '4 passed; 0 failed' in r.stdout and report['source_stable']
report['binary_sha256']={str(p):sha(p) for p in binaries if p.is_file()}
if not report['passed']:report['failure_tail']=r.stdout[-6000:]
path.write_text(json.dumps(report,indent=2)+'\n');(out/'runner.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps({'passed':report['passed'],'receipt':str(path),'test_names':report['test_names'],'failure_tail':report.get('failure_tail')}))
raise SystemExit(0 if report['passed'] else 1)
