from pathlib import Path
import os,subprocess,json,hashlib,sys,tarfile,io
root=Path('/home/zpconn/code/aerostore');out=Path(__file__).resolve().parent
cargo=Path(subprocess.check_output(['rustup','which','cargo'],text=True).strip())
env=os.environ.copy()
for key in list(env):
    if key.startswith('AEROSTORE_') or key in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC','RUSTDOC','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','CARGO_BUILD_RUSTFLAGS','CARGO_BUILD_RUSTC_WRAPPER','CARGO_BUILD_TARGET') or key.startswith('CARGO_PROFILE_') or (key.startswith('CARGO_TARGET_') and key.endswith('_RUSTFLAGS')):env.pop(key)
env['RUSTC']=str(cargo.with_name('rustc'));env['RUSTUP_TOOLCHAIN']='stable-x86_64-unknown-linux-gnu';env['CARGO_ENCODED_RUSTFLAGS']=''
sourcepath='aerostore_core/src/occ_partitioned.rs';original=(root/sourcepath).read_bytes();original_sha=hashlib.sha256(original).hexdigest()
records=[]
def run(name,source,target,args,expected):
    command=[str(cargo),'test','--offline','--locked','--target-dir',str(target),'-p','aerostore_core',*args,'--','--nocapture']
    log=out/(name+'.log')
    with log.open('w') as stream:r=subprocess.run(command,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT)
    content=log.read_text();passed=(r.returncode==0) if expected=='pass' else (r.returncode==101 and 'test result: FAILED.' in content and 'panicked at' in content and 'error[E' not in content)
    entry={'name':name,'command':command,'source':str(source),'occ_source_sha256':hashlib.sha256((source/sourcepath).read_bytes()).hexdigest(),'exit_code':r.returncode,'expected':expected,'passed':passed,'log':log.name};records.append(entry);print(json.dumps(entry),flush=True);return passed
ok=run('new-tests',root,out/'native-target',['--lib','predicate_completion_tests'],'pass')
if ok:ok=run('existing-index',root,out/'native-target',['--test','occ_transactional_index'],'pass')
if ok and '--mutants' in sys.argv:
    archive=subprocess.check_output(['git','archive','HEAD','Cargo.toml','Cargo.lock','aerostore_core','aerostore_verified','aerostore_macros','aerostore_tcl'])
    (out/'parent-crates.tar').write_bytes(archive)
    text=original.decode()
    def scoped(function,end,old,new):
        start=text.index(function);stop=text.index(end,start);body=text[start:stop]
        assert body.count(old)==1,(function,old,body.count(old))
        return text[:start]+body.replace(old,new)+text[stop:]
    variants=[
        ('skip-predicate-revalidation',scoped('    fn index_read_conflict(', '    // Destination insertion', 'if stamp != read.stamp || !aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid) {','if false {'),'empty_capture_then_create_or_key_move_rejects_without_concrete_row_dependency'),
        ('collapse-index-identity',scoped('    fn index_read_conflict(', '    // Destination insertion','.find(|bound| bound.index.header_offset() == read.index_offset)','.find(|_| true)'),'equal_bucket_numbers_in_different_indexes_keep_independent_dependencies'),
        ('omit-own-write-candidates',text.replace('        candidates.extend(tx.write_set.iter().map(|write| write.row_id));','        // mutant: omit pending writes from predicate candidates'),'colliding_predicates_filter_final_own_writes_and_revalidate_an_unread_creation'),
        ('omit-query-locks',scoped('    fn index_lock_keys(', '    fn acquire_index_locks(','            keys.insert((binding, read.bucket));','            let _ = (binding, read.bucket);'),'crossed_empty_predicates_cannot_both_publish_through_disjoint_write_buckets'),
        ('omit-old-key-stamps',scoped('    fn publish_index_stamps(', '    /// Locks stable, shared-memory row slots','[change.before.as_ref(), change.after.as_ref()]','[None, change.after.as_ref()]'),'actual_publication_stamps_all_old_and_new_buckets_once_and_leaves_others_unchanged'),
    ]
    for name,mutated,test in variants:
        source=out/name/'source';source.mkdir(parents=True,exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(source,filter='data')
        (source/sourcepath).write_text(mutated)
        ok=run(name,source,out/name/'cargo-target',['--lib',test],'assertion failure') and ok
report={'parent_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'rustc':subprocess.check_output([env['RUSTC'],'--version','--verbose'],text=True),'main_occ_source_sha256':original_sha,'main_source_stable':hashlib.sha256((root/sourcepath).read_bytes()).hexdigest()==original_sha,'results':records,'passed':ok}
(out/'report.json').write_text(json.dumps(report,indent=2)+'\n');sys.exit(0 if ok else 1)
