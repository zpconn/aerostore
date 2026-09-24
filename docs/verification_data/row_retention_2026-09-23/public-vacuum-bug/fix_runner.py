from pathlib import Path
import os,subprocess,hashlib,json
root=Path('/home/zpconn/code/aerostore');p=Path('/tmp/aerostore-vacuum-api-repro-path').read_text();p=Path(p);clone=p/'patched-workspace'
env=dict(os.environ,RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1',RUSTUP_NO_UPDATE_CHECK='1',CARGO_TARGET_DIR='/tmp/aerostore-vacuum-api-target')
cmd=['cargo','test','--offline','--locked','--release','--manifest-path',str(clone/'Cargo.toml'),'-p','aerostore_core','--test','occ_transactional_index','public_vacuum_clamps_caller_horizon_to_retained_snapshot','--','--exact','--nocapture']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
checks=[]; source=clone/'aerostore_core/src/occ_partitioned.rs'; original=source.read_text()
def run(name,negative):
 r=subprocess.run(cmd,cwd=root,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 log=p/(name+'.log');log.write_text(r.stdout)
 binary=Path('/tmp/aerostore-vacuum-api-target/release/deps/occ_transactional_index-80a51d590d615acb')
 check={'name':name,'negative':negative,'command':cmd,'exit_code':r.returncode,'source_sha256':{str(f.relative_to(clone)):sha(f) for f in [source,clone/'aerostore_core/src/vacuum.rs',clone/'aerostore_core/tests/occ_transactional_index.rs']},'binary_sha256':sha(binary),'log':str(log),'log_sha256':sha(log)}
 checks.append(check)
 if negative: assert r.returncode==101 and 'a caller must not advance the retained horizon' in r.stdout and '1 failed' in r.stdout
 else: assert r.returncode==0 and '1 passed; 0 failed' in r.stdout
try:
 run('patched-positive',False)
 old='self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))'; assert original.count(old)==1
 source.write_text(original.replace(old,'self.vacuum_reclaim_before(requested_xmin)',1))
 run('omitted-clamp-negative',True)
finally:
 source.write_text(original)
run('restored-positive',False)
receipt={'passed':True,'scope':'isolated native public horizon regression and missing-clamp control','checks':checks,'proposed_patch':str(p/'proposed-fix.patch'),'proposed_patch_sha256':sha(p/'proposed-fix.patch'),'runner_sha256':sha(Path(__file__))}
(p/'fix-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'passed':True,'receipt':str(p/'fix-receipt.json')}))
