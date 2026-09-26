from pathlib import Path
import subprocess,json,datetime,os
root=Path.cwd();out=root/'target/retry-final-validation';base=root/'target/contention-tuned-validation/native-postgres';data=base/'data';restart=json.loads((root/'target/retry-final-validation/postgres-restart.json').read_text());pid=int(restart['pid_file'][0]);proc=Path('/proc')/str(pid)
assert (data/'postmaster.pid').read_text().splitlines()[:3]==restart['pid_file'][:3]
stat=(proc/'stat').read_text();start=stat[stat.rfind(')')+2:].split()[19];assert start==restart['proc_start_ticks']
exe=(proc/'exe').resolve();assert exe==(base/'install/bin/postgres').resolve()
args=(proc/'cmdline').read_bytes().split(b'\0');assert str(data).encode() in args
sql="SELECT current_setting('data_directory'), current_setting('port'), (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid())"
cmd=[str(base/'install/bin/psql'),'host=/tmp/aerostore-tuned-pg-20260925 port=55432 user=postgres dbname=contention_test connect_timeout=5','-X','-A','-t','-c',sql]
settings=subprocess.check_output(cmd,text=True).strip().split('|');assert settings==[str(data),'55432','0'],settings
command=[str(base/'install/bin/pg_ctl'),'-D',str(data),'-m','fast','-w','-t','30','stop'];r=subprocess.run(command,capture_output=True,text=True);assert r.returncode==0,(r.stdout,r.stderr)
s=subprocess.run([str(base/'install/bin/pg_ctl'),'-D',str(data),'status'],capture_output=True,text=True)
receipt={'completed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'passed':r.returncode==0 and s.returncode==3 and not (data/'postmaster.pid').exists() and (data/'PG_VERSION').exists(),'scope':'Stopped only the previously authorized disposable fixture after all maintenance campaigns completed. Database files retained; no unrelated process signalled.','verified_pid':pid,'verified_proc_start_ticks':start,'verified_executable':str(exe),'sql_data_directory_port_other_clients':settings,'command':command,'exit_code':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'final_status_exit_code':s.returncode,'pid_file_absent':not (data/'postmaster.pid').exists(),'data_retained':(data/'PG_VERSION').exists()}
(out/'postgres-cleanup.json').write_text(json.dumps(receipt,indent=2)+'\n');assert receipt['passed'];print(json.dumps(receipt,indent=2))
