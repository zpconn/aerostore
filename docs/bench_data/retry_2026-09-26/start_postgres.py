from pathlib import Path
import subprocess,json,datetime
root=Path.cwd();out=root/'target/retry-final-validation';base=root/'target/contention-tuned-validation/native-postgres';data=base/'data'
assert not (data/'postmaster.pid').exists(), 'Refuse to adopt an existing PostgreSQL process'
command=[str(base/'install/bin/pg_ctl'),'-D',str(data),'-l',str(out/'postgres.log'),'-w','-t','30','start']
r=subprocess.run(command,capture_output=True,text=True)
assert r.returncode==0,(r.stdout,r.stderr)
lines=(data/'postmaster.pid').read_text().splitlines();proc=Path('/proc')/lines[0]
receipt={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Previously authorized disposable PostgreSQL fixture for controlled retry/index comparison; persistent data retained.','command':command,'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'pid_file':lines,'proc_start_ticks':(proc/'stat').read_text().rsplit(') ',1)[1].split()[19],'executable':str((proc/'exe').resolve())}
assert receipt['executable']==str((base/'install/bin/postgres').resolve())
(out/'postgres-restart.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
