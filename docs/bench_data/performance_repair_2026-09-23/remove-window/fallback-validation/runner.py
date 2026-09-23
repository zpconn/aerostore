from pathlib import Path
import hashlib,json,os,shutil,subprocess,difflib
root=Path('/home/zpconn/code/aerostore');out=root/'target/performance-repair/remove-window';parent=out/'source';folder=out/'fallback-validation';folder.mkdir()
rel='aerostore_core/src/shm_skiplist.rs';sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
env=dict(os.environ);env.update(RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1')
sysroot=Path(subprocess.check_output(['rustc','--print','sysroot'],env=env,text=True).strip());cargo=sysroot/'bin/cargo';env['RUSTC']=str(sysroot/'bin/rustc')
r={'passed':False,'status':'running','scope':'test-only local-window fault probes; no shared protocol metadata injection','candidate_source_sha256':sha(parent/rel),'checks':[]}
def persist():(folder/'receipt.json').write_text(json.dumps(r,indent=2)+'\n')
persist()
try:
 for name,injection in [('successor-mismatch','succs[1] = NULL_OFFSET;'),('cas-mismatch','preds[1] = node_offset;')]:
  source=out/('fallback-'+name);source.mkdir()
  for member in ['aerostore_core','aerostore_macros','aerostore_verified','aerostore_tcl']:shutil.copytree(parent/member,source/member,ignore=shutil.ignore_patterns('target','__pycache__'))
  for f in ['Cargo.toml','Cargo.lock']:shutil.copyfile(parent/f,source/f)
  original=(source/rel).read_text();call='''                    self.unlink_node(key, node_offset, &mut preds, &mut succs)?;'''
  assert original.count(call)==1
  changed=original.replace(call,'                    #[cfg(test)]\n                    { '+injection+' }\n'+call)
  marker='''        while !detached_all {
            let _ = self.find(key, preds, succs)?;'''
  assert changed.count(marker)==1
  changed=changed.replace(marker,'''        while !detached_all {
            #[cfg(test)]
            eprintln!("remove_window_fallback_entered");
            let _ = self.find(key, preds, succs)?;''')
  (source/rel).write_text(changed)
  patch=folder/(name+'.patch');patch.write_text(''.join(difflib.unified_diff(original.splitlines(True),changed.splitlines(True),fromfile='a/'+rel,tofile='b/'+rel)))
  target=out/('build-fallback-'+name);assert not target.exists()
  cmd=[str(cargo),'test','--offline','--locked','--release','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(target),'-p','aerostore_core','--lib','--no-run','--message-format=json'];log=folder/(name+'-build.log')
  with log.open('w') as stream:p=subprocess.run(cmd,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=600)
  c={'name':name,'source_sha256':sha(source/rel),'patch_sha256':sha(patch),'build_command':cmd,'build_log_sha256':sha(log),'build_exit_code':p.returncode};r['checks'].append(c);persist();assert p.returncode==0
  artifacts=[]
  for line in log.read_text().splitlines():
   try:event=json.loads(line)
   except ValueError:continue
   if event.get('reason')=='compiler-artifact' and event.get('target',{}).get('name')=='aerostore_core' and event.get('executable'):artifacts.append(event)
  assert len(artifacts)==1 and artifacts[0]['fresh'] is False
  a=artifacts[0];assert Path(a['target']['src_path'])==source/'aerostore_core/src/lib.rs'
  exe=Path(a['executable']);assert exe.is_relative_to(target)
  c.update(executable=str(exe),executable_sha256=sha(exe),fresh_test_binary=True)
  cmd=[str(exe),'--exact','shm_skiplist::tests::removal_window_detaches_every_lane_before_pinned_retirement_and_shorter_reuse','--nocapture','--test-threads=1'];log=folder/(name+'-test.log')
  with log.open('w') as stream:p=subprocess.run(cmd,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=120)
  text=log.read_text();c.update(test_command=cmd,test_exit_code=p.returncode,test_log_sha256=sha(log),fallback_entries=text.count('remove_window_fallback_entered'));persist()
  assert p.returncode==0 and 'running 1 test' in text and '1 passed; 0 failed; 0 ignored' in text and c['fallback_entries']>0
  assert sha(source/rel)==c['source_sha256'] and sha(parent/rel)==r['candidate_source_sha256']
 r.update(passed=True,status='passed',source_stable=True)
except BaseException as error:r.update(status='failed',error=repr(error));persist();raise
finally:persist()
print(json.dumps({'passed':r['passed'],'receipt':str(folder/'receipt.json')}))
