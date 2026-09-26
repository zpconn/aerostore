import datetime,importlib.util,json,shutil,subprocess,tarfile
from pathlib import Path
root=Path.cwd();out=root/'target/maintenance-final-validation'
spec=importlib.util.spec_from_file_location('gate',root/'scripts/qualify_hyperfeed.py');gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
before=gate.snapshot_sources()
cmd=['cargo','build','--offline','--locked','--release','-p','aerostore_core','--bench','hyperfeed_contention_crucible','--message-format=json-render-diagnostics']
with (out/'build-bound.jsonl').open('w') as log:
 r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
assert r.returncode==0, 'build failed; inspect build-bound.jsonl'
artifacts=[]
for line in (out/'build-bound.jsonl').read_text().splitlines():
 try: row=json.loads(line)
 except json.JSONDecodeError: continue
 if row.get('reason')=='compiler-artifact' and row.get('target',{}).get('name')=='hyperfeed_contention_crucible' and row.get('executable'):artifacts.append(row)
assert len(artifacts)==1
after=gate.snapshot_sources();assert before==after
binary=out/'benchmark-final';shutil.copyfile(artifacts[0]['executable'],binary);binary.chmod(0o755)
with tarfile.open(out/'source.tar.gz','w:gz') as tar:
 for path,digest in sorted(after['files'].items()):
  assert gate.sha256(root/path)==digest
  tar.add(root/path,arcname=path,recursive=False)
assert gate.snapshot_sources()==after
receipt={'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary':str(binary),'binary_sha256':gate.sha256(binary),'cargo_artifact':artifacts[0],'source_before':before,'source_after':after,'command':cmd,'compiler':subprocess.check_output(['rustc','-Vv'],text=True),'git_revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'exit_code':r.returncode}
(out/'build-provenance.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'binary':str(binary),'binary_sha256':receipt['binary_sha256'],'source_sha256':after['sha256'],'files':len(after['files'])}),flush=True)
