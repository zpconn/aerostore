from pathlib import Path
import subprocess,json,hashlib
root=Path.cwd();paths=[]
for prefix in ['aerostore_core/src','aerostore_core/benches/extended_crucible']:
 paths+=subprocess.check_output(['git','ls-tree','-r','--name-only','c0d6556','--',prefix],text=True).splitlines()
rows=[]
for name in paths:
 old=subprocess.check_output(['git','show','c0d6556:'+name]);current=(root/name).read_bytes()
 assert old==current,name
 rows.append({'path':name,'sha256':hashlib.sha256(current).hexdigest()})
value={'passed':True,'baseline':'c0d6556','unchanged_files':len(rows),'files':rows,'scope':'All tracked production core source and original Extended Crucible implementation files are byte-identical to the pushed baseline. Benchmark scaffolding and tests changed; this is preservation evidence, not a new engine proof or performance acceptance.'}
(root/'target/affinity-final-validation/preservation-review.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({'passed':True,'unchanged_files':len(rows)}))
