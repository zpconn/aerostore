from pathlib import Path
import hashlib,json,os,subprocess,sys
root=Path(__file__).resolve().parent;source=root/'source'
cargo=Path(subprocess.check_output(['rustup','which','cargo'],text=True).strip())
env=os.environ.copy()
for k in list(env):
    if k.startswith('AEROSTORE_') or k in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC','RUSTDOC','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','CARGO_BUILD_RUSTFLAGS','CARGO_BUILD_RUSTC_WRAPPER','CARGO_BUILD_TARGET') or k.startswith('CARGO_PROFILE_') or (k.startswith('CARGO_TARGET_') and k.endswith('_RUSTFLAGS')):
        env.pop(k)
env['RUSTC']=str(cargo.with_name('rustc'));env['RUSTUP_TOOLCHAIN']='stable-x86_64-unknown-linux-gnu';env['CARGO_ENCODED_RUSTFLAGS']=''
files=sorted(p for p in source.rglob('*') if p.is_file() and (p.suffix=='.rs' or p.name in ('Cargo.toml','Cargo.lock','build.rs')))
hashes=lambda:{str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
start=hashes();reports=[]
base=[str(cargo),'--offline','--locked','--target-dir',str(root/'cargo-target'),'-p','aerostore_core']
commands=[('unit',[base[0],'test',*base[1:],'--test','crucible_seed','--','--nocapture']),('build',[base[0],'build',*base[1:],'--release','--bench','hyperfeed_crucible','--message-format=json'])]
for name,command in commands:
    log=root/(name+'.log')
    with log.open('w') as f:r=subprocess.run(command,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
    report={'name':name,'command':command,'exit_code':r.returncode,'log':log.name};reports.append(report);print(json.dumps(report),flush=True)
    if r.returncode: break
if len(reports)==2 and all(r['exit_code']==0 for r in reports):
    exes=[]
    for line in (root/'build.log').read_text().splitlines():
        try:message=json.loads(line)
        except ValueError:continue
        if message.get('reason')=='compiler-artifact' and message.get('target',{}).get('name')=='hyperfeed_crucible' and message.get('executable'):exes.append(message['executable'])
    assert len(exes)==1,exes
    benchmark=exes[0]
    run_env={**env,'AEROSTORE_CRUCIBLE_AEROSTORE_ONLY':'1','AEROSTORE_CRUCIBLE_SHM_MIB':'128','AEROSTORE_CRUCIBLE_DURATION_SECS':'1'}
    cases=[('zero','0',True),('max',str((1<<64)-1),True),('absent',None,True),('invalid','18446744073709551616',False)]
    for name,seed,valid in cases:
        child_env=run_env.copy()
        if seed is not None:child_env['AEROSTORE_CRUCIBLE_SEED']=seed
        log=root/f'smoke-{name}.log';command=[benchmark,'--noplot']
        with log.open('w') as f:r=subprocess.run(command,cwd=source,env=child_env,stdout=f,stderr=subprocess.STDOUT,timeout=90)
        content=log.read_text();markers=[line for line in content.splitlines() if line.startswith('hyperfeed_crucible_seed:')]
        expected=f'hyperfeed_crucible_seed: mode=fixed seed={seed} algorithm=worker_add_xorshift64_v1' if seed is not None else 'hyperfeed_crucible_seed: mode=entropy seed=none algorithm=pid_time_xorshift64_v1'
        ok=(r.returncode==0 and markers==[expected]) if valid else (r.returncode!=0 and not markers and 'AEROSTORE_CRUCIBLE_SEED:' in content)
        report={'name':f'smoke-{name}','command':command,'seed':seed,'exit_code':r.returncode,'expected_success':valid,'passed':ok,'markers':markers,'log':log.name};reports.append(report);print(json.dumps(report),flush=True)
report={'parent_commit':'9676fa9ff602565d5a628af41b6d40ba1576a8b9','source':str(source),'compiler':subprocess.check_output([env['RUSTC'],'--version','--verbose'],text=True),'source_sha256':start,'source_stable':start==hashes(),'checks':reports,'performance_measurement':False,'passed':len(reports)==6 and all(r.get('passed',r['exit_code']==0) for r in reports)}
(root/'validation.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k] for k in ('source_stable','passed')}),flush=True)
sys.exit(0 if report['passed'] else 1)
