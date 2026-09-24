from pathlib import Path
import hashlib,json,subprocess
ROOT=Path('/home/zpconn/code/aerostore')
BASE='f82788073224f833873f57c25243a553b4c4d511'
ROOTS=['Cargo.toml','Cargo.lock','.cargo','aerostore_core','aerostore_verified','aerostore_macros','aerostore_tcl']
def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT)
def digest(data):return hashlib.sha256(data).hexdigest()
def selected(n):return n.endswith(('.rs','.toml')) or n=='Cargo.lock'
before=sorted(n for n in git('ls-tree','-r','--name-only',BASE,'--',*ROOTS).decode().splitlines() if selected(n))
after=sorted(set(n for n in git('ls-files','--cached','--others','--exclude-standard','--',*ROOTS).decode().splitlines() if selected(n)))
assert before==after
files={}
for name in before:
 old=git('show',BASE+':'+name);new=(ROOT/name).read_bytes()
 files[name]={'baseline_sha256':digest(old),'current_sha256':digest(new),'byte_identical':old==new}
changed=[n for n,v in files.items() if not v['byte_identical']]
assert changed==['aerostore_core/tests/occ_transactional_index.rs'],changed
assert all(v['byte_identical'] for n,v in files.items() if '/tests/' not in n)
assert all(digest((ROOT/n).read_bytes())==v['current_sha256'] for n,v in files.items())
report={'passed':True,'baseline_commit':BASE,'checked_files':len(files),'files':files,
 'changed_inputs':changed,'production_runtime_changed':False,'production_byte_identical':True,
 'source_stable':True,'formal_semantic_equivalence_proved':False,
 'scope':'Exact bytes of all native Rust/Cargo inputs except the reviewed Cargo integration-test file; no semantic equivalence theorem.',
 'runner_sha256':digest(Path(__file__).read_bytes())}
out=ROOT/'target/verification-planning/runtime-audit.json'
out.parent.mkdir(parents=True,exist_ok=True)
out.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='files'}))
