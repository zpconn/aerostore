from pathlib import Path
import hashlib,json,os,shutil,subprocess,time,difflib
root=Path('/home/zpconn/code/aerostore');out=root/'target/performance-repair/remove-window';folder=out/'search-count-validation';folder.mkdir()
rel='aerostore_core/src/shm_skiplist.rs'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
env=dict(os.environ);env.update(RUSTUP_HOME=str(root/'target/verification-tools/production-rustup'),CARGO_HOME=str(root/'target/verification-tools/production-cargo'),RUSTUP_TOOLCHAIN='1.93.1')
sysroot=Path(subprocess.check_output(['rustc','--print','sysroot'],env=env,text=True).strip());cargo=sysroot/'bin/cargo';env['RUSTC']=str(sysroot/'bin/rustc')
r={'passed':False,'status':'running','scope':'cfg-test-only exact call count; not throughput evidence','checks':[],'candidate_source_before':sha(out/'source'/rel)}
def persist():(folder/'receipt.json').write_text(json.dumps(r,indent=2)+'\n')
persist()
try:
 for name,parent,expected in [('baseline',out/'baseline-test-source',2),('candidate',out/'source',1)]:
  source=out/('search-count-'+name);source.mkdir()
  for member in ['aerostore_core','aerostore_macros','aerostore_verified','aerostore_tcl']:
   shutil.copytree(parent/member,source/member,ignore=shutil.ignore_patterns('target','__pycache__'))
  for name2 in ['Cargo.toml','Cargo.lock']:shutil.copyfile(parent/name2,source/name2)
  original=(source/rel).read_text();modified=original
  marker='''    ) -> Result<Option<u32>, ShmSkipListError> {
        'retry: loop {'''
  assert modified.count(marker)==1
  modified=modified.replace(marker,'''    ) -> Result<Option<u32>, ShmSkipListError> {
        #[cfg(test)]
        tests::REMOVAL_FIND_CALLS.with(|calls| calls.set(calls.get() + 1));
        'retry: loop {''')
  marker='''    #[test]
    fn removal_window_detaches_every_lane_before_pinned_retirement_and_shorter_reuse() {'''
  test='''    thread_local! {
        pub(super) static REMOVAL_FIND_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    #[test]
    fn normal_last_posting_removal_search_count_probe() {
        let list = make_list();
        insert_with_height(&list, 10, 10, MAX_HEIGHT);
        insert_with_height(&list, 20, 20, MAX_HEIGHT);
        insert_with_height(&list, 30, 30, MAX_HEIGHT);
        REMOVAL_FIND_CALLS.with(|calls| calls.set(0));
        list.remove_payload(&TestKey(20), 4, &20_u32.to_le_bytes()).unwrap();
        let calls = REMOVAL_FIND_CALLS.with(|calls| calls.get());
        println!("normal_last_posting_removal_find_calls={calls}");
        assert_eq!(calls, EXPECTED);
        list.audit_allocations().unwrap();
    }

'''.replace('EXPECTED',str(expected))
  assert modified.count(marker)==1
  modified=modified.replace(marker,test+marker);(source/rel).write_text(modified)
  patch=folder/(name+'-instrumentation.patch');patch.write_text(''.join(difflib.unified_diff(original.splitlines(True),modified.splitlines(True),fromfile='a/'+rel,tofile='b/'+rel)))
  target=out/('build-search-count-'+name);assert not target.exists()
  log=folder/(name+'-build.log');cmd=[str(cargo),'test','--offline','--locked','--release','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(target),'-p','aerostore_core','--lib','--no-run','--message-format=json']
  with log.open('w') as stream:proc=subprocess.run(cmd,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=600)
  c={'name':name,'command':cmd,'source_sha256':sha(source/rel),'base_source_sha256':sha(parent/rel),'patch_sha256':sha(patch),'build_log':str(log),'build_log_sha256':sha(log),'build_exit_code':proc.returncode};r['checks'].append(c);persist();assert proc.returncode==0
  artifacts=[]
  for line in log.read_text().splitlines():
   try:event=json.loads(line)
   except ValueError:continue
   if event.get('reason')=='compiler-artifact' and event.get('target',{}).get('name')=='aerostore_core' and event.get('executable'):artifacts.append(event)
  assert len(artifacts)==1 and artifacts[0]['fresh'] is False
  a=artifacts[0];assert Path(a['target']['src_path'])==source/'aerostore_core/src/lib.rs'
  exe=Path(a['executable']);assert exe.is_relative_to(target)
  c.update(executable=str(exe),executable_sha256=sha(exe),fresh_test_binary=True)
  cmd=[str(exe),'--exact','shm_skiplist::tests::normal_last_posting_removal_search_count_probe','--nocapture','--test-threads=1'];log=folder/(name+'-count.log')
  with log.open('w') as stream:proc=subprocess.run(cmd,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=120)
  c.update(test_command=cmd,test_exit_code=proc.returncode,test_log=str(log),test_log_sha256=sha(log),expected_search_calls=expected);persist()
  assert proc.returncode==0 and 'running 1 test' in log.read_text() and '1 passed; 0 failed; 0 ignored' in log.read_text()
  assert 'normal_last_posting_removal_find_calls='+str(expected) in log.read_text()
  assert sha(source/rel)==c['source_sha256'] and sha(parent/rel)==c['base_source_sha256']
 r['candidate_source_after']=sha(out/'source'/rel);assert r['candidate_source_after']==r['candidate_source_before']
 r.update(passed=True,status='passed',source_stable=True)
except BaseException as error:r.update(status='failed',error=repr(error));persist();raise
finally:persist()
print(json.dumps({'passed':r['passed'],'receipt':str(folder/'receipt.json')}))
