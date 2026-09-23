from pathlib import Path
import hashlib,json,os,subprocess,sys,time
root=Path(__file__).resolve().parent
source=root/'source'
cargo=Path(subprocess.check_output(['rustup','which','cargo'],text=True).strip())
env=os.environ.copy()
for k in list(env):
    if k.startswith('AEROSTORE_') or k in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC','RUSTDOC','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','CARGO_BUILD_RUSTFLAGS','CARGO_BUILD_RUSTC_WRAPPER','CARGO_BUILD_TARGET') or k.startswith('CARGO_PROFILE_') or (k.startswith('CARGO_TARGET_') and k.endswith('_RUSTFLAGS')):
        env.pop(k)
env['RUSTC']=str(cargo.with_name('rustc'));env['RUSTUP_TOOLCHAIN']='stable-x86_64-unknown-linux-gnu';env['CARGO_ENCODED_RUSTFLAGS']=''
files=sorted(p for p in source.rglob('*') if p.is_file() and (p.suffix=='.rs' or p.name in ('Cargo.toml','Cargo.lock','build.rs')))
hashes=lambda:{str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
start=hashes()
base=[str(cargo),'test','--offline','--locked','--target-dir',str(root/'cargo-target'),'-p','aerostore_core']
cases=[('serializer',['--lib','wal_ring::reusable_buffer_tests']),('buffer-owner',['--lib','wal_writer::buffer_reuse_regressions']),('poison',['--lib','wal_writer::poison_regressions']),('protocol',['--test','wal_protocol_regressions']),('allocation',['--test','wal_serializer_allocations'])]
reports=[]
for name,args in cases:
    command=base+args+['--','--nocapture']
    log=root/(name+'.log')
    with log.open('w') as f:
        result=subprocess.run(command,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
    reports.append({'name':name,'command':command,'exit_code':result.returncode,'log':str(log.relative_to(root))})
    print(json.dumps(reports[-1]),flush=True)
    if result.returncode: break
report={'parent_commit':'9676fa9ff602565d5a628af41b6d40ba1576a8b9','source':str(source),'cargo_target':str(root/'cargo-target'),'compiler':subprocess.check_output([env['RUSTC'],'--version','--verbose'],text=True),'source_sha256':start,'source_stable':start==hashes(),'tests':reports,'passed':len(reports)==len(cases) and all(r['exit_code']==0 for r in reports)}
(root/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ('source_stable','passed')}),flush=True)
sys.exit(0 if report['passed'] else 1)
