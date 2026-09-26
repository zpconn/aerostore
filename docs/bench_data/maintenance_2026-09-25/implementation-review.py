from pathlib import Path
from datetime import datetime, timezone
import hashlib, importlib.util, json, subprocess
root=Path.cwd();base=root/'target/maintenance-final-validation'
spec=importlib.util.spec_from_file_location('gate',root/'scripts/qualify_hyperfeed.py');gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
source=gate.snapshot_sources(); assert source['sha256']=='1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
tracked=subprocess.check_output(['git','ls-files'],text=True).splitlines()
groups={
 'production_core':[p for p in tracked if p.startswith('aerostore_core/src/')],
 'other_library_crate_sources_including_colocated_tests':[p for p in tracked if p.startswith(('aerostore_macros/src/','aerostore_tcl/src/','aerostore_verified/src/'))],
 'original_extended_crucible':[p for p in tracked if p.startswith('aerostore_core/benches/extended_crucible/') or p=='aerostore_core/benches/hyperfeed_extended_crucible.rs'],
 'unchanged_contention_execution_dependencies':['aerostore_core/benches/contention_crucible/'+p for p in ['aerostore.rs','postgres.rs','service.rs','model.rs','storage.rs','oracle.rs','measurement.rs']],
}
preservation={}
for name,paths in groups.items():
 files=[]
 for path in paths:
  before=subprocess.check_output(['git','show',f'{head}:{path}']);after=(root/path).read_bytes()
  files.append({'path':path,'head_sha256':hashlib.sha256(before).hexdigest(),'working_sha256':hashlib.sha256(after).hexdigest(),'unchanged':before==after})
 preservation[name]={'files':files,'count':len(files),'passed':all(row['unchanged'] for row in files)}
reviewed=[
 'aerostore_core/benches/contention_crucible/'+p for p in ['calibrated.rs','maintenance.rs','workers.rs','runner.rs','remote.rs','model.rs','oracle.rs','measurement.rs']
]+['scripts/qualify_hyperfeed.py','scripts/run_remote_contention.py','scripts/test_hyperfeed_qualification.py','scripts/test_run_remote_contention.py','scripts/test_maintenance_sweeps.py','aerostore_core/tests/calibrated_contention_model.rs']
checks=[
 {'area':'Fixed job input and IDs','finding':'Coordinator reconstructs each offered timer job and expected next batch. Derived transaction IDs encode the logical job and batch ordinal; all fields except ID retain the job cutoff. Retries reuse the same transaction message. Missing, repeated or changed batches are rejected.'},
 {'area':'Terminal and incomplete jobs','finding':'With positive handler limits, processed==0 identifies the terminal batch; full receipts additionally require exactly the matching empty global query and an empty outcome. Batch cap includes this transaction. Worker errors retain preceding successful receipts and pending jobs; Done requires every offered job completed and no pending batch.'},
 {'area':'History coverage and commit accounting','finding':'Every successful transaction, including empty terminal probes, enters the history and commit-count invariant. The serial oracle validates complete histories; job counts advance only at terminal. Metrics mode does not claim a recorded serial history.'},
 {'area':'Latency and FIFO','finding':'Foreground samples remain one input each; maintenance samples span first attempted batch to terminal completion, with queueing from the original timer deadline and summed retries. Per-worker transaction and job intervals are checked for FIFO. Extra transactions do not increase logical message throughput.'},
 {'area':'Independent qualification audit','finding':'Python reconstructs timer job IDs/deadlines and checks expected job coverage, transaction-ID ranges, positive batch bounds, terminal counts, per-kind effects, retry sums, occupied time, class p99, aggregate transaction counts and native commits. Legacy default reports retain their original one-transaction-per-job interpretation.'},
 {'area':'Companions','finding':'Mode, both batch sizes and cap are part of full/metrics configuration matching alongside dispatch, timers, population and source/binary binding. Discovered batch counts need not match across valid executions. A companion does not certify the metrics execution history.'},
 {'area':'Local and remote population','finding':'Sweep seed cohorts depend on housekeeping cadence but not duration/rate/workers/dispatch. Remote owner can initialize with short dummy admission duration; client verifies matching explicit options and exact initial rows before executing.'},
 {'area':'Preservation','finding':'Production core, other library crate source files, original Extended Crucible, and unchanged contention transaction handlers/oracle/adapters are byte-compared against the current HEAD below.'},
]
limits=[
 'This is a source review and test record, not a formal proof of the new job coordinator or qualification driver.',
 'Terminal completion is one serial observation; a sweep is not an atomic snapshot, and later eligible insertions belong to later jobs.',
 'Up to three finite expiry cohorts provide early useful housekeeping work without lifecycle turnover. Batch control retains different historical ages; mode comparison is not isolated loop-cost measurement.',
 'All queries remain complete before application batch selection. Repeated whole-result queries may revisit many rows; this change does not establish an optimal query/index design.',
 'Failure histories are partial diagnostic evidence and are not labeled complete successful serial histories. Ambiguous service commits remain failures rather than transparently replayed work.',
 'The Python audit checks source-bound report accounting, not independent re-execution of the SQL transaction history; full history correctness still uses the Rust oracle.',
 'No additional throughput, 10x capacity, 100-300-worker support, physical multi-host result, crash durability equivalence or direct-access worker-death isolation is established here.',
 'No blocking implementation defect was found in this bounded review; this is not a claim that every possible defect has been excluded.'
]
unit=json.loads((base/'unit-tests-execution.json').read_text())
report={'created_at':datetime.now(timezone.utc).isoformat(),'review_type':'Independent read-only implementation and accounting review plus final-source unit execution','reviewer':'maintenance_model subagent (model/helper author; independent reviewer of root integration and Python qualification)','head':head,'source_sha256':source['sha256'],'source_file_count':len(source['files']),'binary_sha256':gate.sha256(base/'benchmark-final'),'reviewed_files':{p:gate.sha256(root/p) for p in reviewed},'checks':checks,'blocking_findings':[],'limits':limits,'preservation':preservation,'unit_tests':{'receipt':'unit-tests-execution.json','passed':unit['passed'],'calibrated_model':51,'measurement':7,'qualification':51,'remote_helper':12},'source_stable_after_review':gate.snapshot_sources()==source}
report['passed']=report['source_stable_after_review'] and unit['passed'] and all(group['passed'] for group in preservation.values())
(base/'implementation-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'preservation':{k:v['count'] for k,v in preservation.items()},'blocking_findings':report['blocking_findings']}))
assert report['passed']
