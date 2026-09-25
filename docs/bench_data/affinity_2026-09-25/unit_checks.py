from pathlib import Path
import importlib.util,subprocess,json,datetime
root=Path.cwd();out=root/'target/affinity-final-validation'
spec=importlib.util.spec_from_file_location('gate',root/'scripts/qualify_hyperfeed.py');gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
build=json.loads((out/'build-provenance.json').read_text());before=gate.snapshot_sources();assert before==build['source_after']
cmd=['cargo','test','--offline','--locked','-p','aerostore_core','--test','calibrated_contention_model','--test','contention_measurement']
with (out/'model-and-measurement-tests.log').open('w') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
after=gate.snapshot_sources();receipt={'command':cmd,'exit_code':r.returncode,'source_before':before,'source_after':after,'source_stable':before==after,'passed':r.returncode==0 and before==after,'completed_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(out/'unit-execution.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({'passed':receipt['passed'],'source_sha256':after['sha256']}));raise SystemExit(0 if receipt['passed'] else 1)
