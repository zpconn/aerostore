#!/usr/bin/env python3
"""Source-bound vacuum horizon callers and one acquired native row iteration."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
VACUUM_SOURCE=ROOT/'aerostore_core/src/vacuum.rs'
HORIZON=HERE/'horizon.rs'
CONTRACTS=HERE/'contracts.rs'
OUTPUT=HERE/'retention.verus.rs'
spec=importlib.util.spec_from_file_location('retention_lookup_adapter',ROOT/'verification/lookup/generate.py')
lookup=importlib.util.module_from_spec(spec);spec.loader.exec_module(lookup)
adapter=lookup.adapter
adapter.TOKEN=re.compile(adapter.TOKEN.pattern.replace('<=|>=|::|->','<==>|==>|=~=|<=|>=|::|->') + r'|@')

def render_module(source=None,template=None):
    source=SOURCE.read_text() if source is None else source
    template=CONTRACTS.read_text() if template is None else template
    if len(re.findall(r'\bpub\s*\(\s*crate\s*\)\s+fn\s+vacuum_reclaim_before\s*\(',source))!=1:
        raise ValueError('unchecked vacuum kernel must remain crate-private')
    body=lookup.method(source,'vacuum_reclaim_before','''fn vacuum_reclaim_before(
        &self, global_xmin: TxId,
    ) -> Result<Vec<VacuumReclaimedRow<T>>, Error>''')
    start=adapter.tokenize('let mut reclaimed = Vec::new(); for row_id in 0..self.capacity() {')
    if body[:len(start)]!=start: raise ValueError('changed native vacuum outer scan')
    end=adapter.balanced_end(body,len(start)-1)
    if body[end:]!=adapter.tokenize('Ok(reclaimed)'): raise ValueError('changed native vacuum return')
    body=body[len(start):end-1]
    lock=adapter.tokenize('let _lock = self.acquire_row_lock(row_id);')
    if body[:len(lock)]!=lock: raise ValueError('missing first row partition acquisition')
    body=body[len(lock):]
    changes=[
      ('let slot = self.slot_ref(row_id)?; let head_offset = slot.head.load(Ordering::Acquire);','let head_offset = driver.head(row_id)?;',1),
      ('if head_offset == EMPTY_PTR { continue; }','''if head_offset == 0 {
        proof { assert(driver.state().recycled.difference(initial.recycled) =~= Set::<u32>::empty()); }
        return Ok(reclaimed);
    }''',1),
      ('RelPtr::<OccRow<T>>::from_offset','',3),
      ('self.resolve_row_ptr(&head_ptr)','driver.resolve(head_ptr)',1),
      ('self.resolve_row_ptr(&curr_ptr)','driver.resolve(curr_ptr)',1),
      ('self.resolve_row_ptr(&prev_ptr)','driver.resolve(prev_ptr)',1),
      ('head_row.next.load(Ordering::Acquire)','head_row.next',1),
      ('curr_row.next.load(Ordering::Acquire)','curr_row.next',1),
      ('curr_row.xmax.load(Ordering::Acquire)','curr_row.xmax',1),
      ('curr_row.is_locked.load(Ordering::Acquire)','curr_row.locked',1),
      ('prev_row.next.store(next_offset, Ordering::Release);','driver.store_next(prev_ptr,next_offset,row_id);',1),
      ('self.recycle_row_ptr(row_id, &curr_ptr)','driver.recycle(row_id,curr_ptr)',1),
    ]
    for old,new,n in changes: body=adapter.replace(body,old,new,n)
    body=['Reclaimed' if t=='VacuumReclaimedRow' else ('0' if t=='EMPTY_PTR' else t) for t in body]
    forbidden={'self','unsafe','load','store','RelPtr','OccRow','Ordering','acquire_row_lock'}
    if forbidden.intersection(body): raise ValueError('unadapted native vacuum operation')
    body=adapter.replace(body,'while curr_offset != 0 {','''
    let ghost mut kept = Set::empty().insert(head_offset);
    let ghost mut removed = Set::<u32>::empty();
    proof { reachable_unroll(driver.state().image,head_offset); }
    while curr_offset != 0
        invariant initial==old(driver).state(), driver.no_errors()==old(driver).no_errors(), lookup::image_valid(initial.image), driver.partition_held(row_id), row_id < driver.state().image.capacity,
            lookup::image_valid(driver.state().image),
            admissible(driver.state().image,tx,global_xmin),
            safe_result(initial,driver.state(),row_id,tx),
            prefix_ineligible(initial.image,row_id,tx,global_xmin) ==> prefix_preserved(initial,driver.state(),row_id,tx),
            head_offset == initial.image.heads[row_id], head_offset != 0,
            live_head_value == initial.image.rows[head_offset].value,
            driver.state().image.rows.contains_key(prev_offset),
            reachable(driver.state().image,head_offset).contains(prev_offset),
            driver.state().image.rows[prev_offset].next == curr_offset,
            reachable(driver.state().image,head_offset) == kept.union(reachable(driver.state().image,curr_offset)),
            kept.disjoint(reachable(driver.state().image,curr_offset)),
            kept.contains(prev_offset), kept.contains(head_offset),
            forall|p:u32| kept.contains(p) && p!=head_offset ==> !eligible(driver.state().image.rows[p],global_xmin),
            reachable(driver.state().image,head_offset) == reachable(initial.image,head_offset).difference(removed),
            driver.state().recycled == initial.recycled.union(removed),
            removed.disjoint(initial.recycled),
            forall|p:u32| removed.contains(p) ==> p!=head_offset && eligible(initial.image.rows[p],global_xmin),
            reclaimed.len() == removed.len(),
            report_provenance(initial.image,removed,reclaimed@,row_id),
        decreases if curr_offset == 0 { 0 } else { driver.state().image.rank[curr_offset] + 1 },
    {''',1)
    body=adapter.replace(body,'let curr_ptr = (curr_offset);','''proof {
        reachable_unroll(driver.state().image,curr_offset);
        assert(reachable(driver.state().image,curr_offset).contains(curr_offset));
        assert(reachable(driver.state().image,head_offset).contains(curr_offset));
        assert(reachable(initial.image,head_offset).contains(curr_offset));
        assert(!kept.contains(curr_offset));
    }
    let curr_ptr = (curr_offset);''',1)
    body=adapter.replace(body,'driver.store_next(prev_ptr,next_offset,row_id);','''
    let ghost before = driver.state();
    proof {
        splice_valid(before.image,prev_ptr,curr_ptr);
        splice_reachable(before.image,prev_ptr,curr_ptr,head_offset);
        below_cut_unchanged(before.image,prev_ptr,curr_ptr,next_offset);
        eligible_is_invisible(before.image,curr_ptr,tx,global_xmin);
        splice_first_visible(before.image,prev_ptr,curr_ptr,head_offset,tx);
        selection_is_visible(before.image,head_offset,tx);
        assert(lookup::first_visible(initial.image,head_offset,tx) != Some(curr_ptr));
        assert(!before.recycled.contains(curr_ptr));
        assert(reachable(initial.image,head_offset).contains(curr_ptr));
        assert(!initial.image.rows[curr_ptr].locked);
        if prefix_ineligible(initial.image,row_id,tx,global_xmin) {
            assert(!prefix(initial.image,head_offset,tx).contains(curr_ptr));
            if prefix(initial.image,head_offset,tx).contains(prev_ptr)
                && lookup::first_visible(initial.image,head_offset,tx) != Some(prev_ptr) {
                prefix_edges(initial.image,head_offset,tx,prev_ptr);
                assert(initial.image.rows[prev_ptr].next==curr_ptr);
                assert(false);
            }
        }
    }
    driver.store_next(prev_ptr,next_offset,row_id);
    proof {
        assert(metadata_same(initial.image,driver.state().image));
        assert(safe_result(initial,driver.state(),row_id,tx));
        if prefix_ineligible(initial.image,row_id,tx,global_xmin) {
            assert forall|p:u32| prefix(initial.image,head_offset,tx).contains(p) implies
                driver.state().image.rows.contains_key(p) && data_same(initial.image.rows[p],driver.state().image.rows[p])
                && (lookup::first_visible(initial.image,head_offset,tx)!=Some(p) ==>
                    initial.image.rows[p].next==driver.state().image.rows[p].next) by {
                prefix_edges(initial.image,head_offset,tx,p);
                if lookup::first_visible(initial.image,head_offset,tx)!=Some(p) { assert(p!=prev_ptr); }
            }
        }
        assert(prefix_ineligible(initial.image,row_id,tx,global_xmin) ==> prefix_preserved(initial,driver.state(),row_id,tx));
    }''',1)
    body=adapter.replace(body,'driver.recycle(row_id,curr_ptr)?;','''driver.recycle(row_id,curr_ptr)?;
    proof {
        assert(!removed.contains(curr_ptr));
        assert(removed.insert(curr_ptr).len()==removed.len()+1);
        removed=removed.insert(curr_ptr);
        assert(driver.state().recycled =~= initial.recycled.union(removed));
        assert(reachable(driver.state().image,head_offset) =~= reachable(initial.image,head_offset).difference(removed));
        assert forall|p:u32| driver.state().recycled.contains(p) && !initial.recycled.contains(p) implies
            reachable(initial.image,head_offset).contains(p) && !initial.image.rows[p].locked
            && lookup::first_visible(initial.image,head_offset,tx) != Some(p) by {
            if p != curr_ptr { assert(before.recycled.contains(p)); }
        }
        assert(safe_result(initial,driver.state(),row_id,tx));
        assert(prefix_ineligible(initial.image,row_id,tx,global_xmin) ==> prefix_preserved(initial,driver.state(),row_id,tx));
        assert(reachable(driver.state().image,head_offset) =~= kept.union(reachable(driver.state().image,next_offset)));
    }''',1)
    body=adapter.replace(body,'prev_offset = curr_offset;','''proof {
        kept=kept.insert(curr_offset);
        assert(reachable(driver.state().image,head_offset) =~= kept.union(reachable(driver.state().image,next_offset)));
    }
    prev_offset = curr_offset;''',1)
    body=adapter.replace(body,'reclaimed.push(Reclaimed {','let ghost old_reports=reclaimed@; reclaimed.push(Reclaimed {',1)
    body=adapter.replace(body,'curr_offset = next_offset; continue;','''proof {
        assert(initial.image.rows[curr_ptr].value==reclaimed[old_reports.len() as int].reclaimed_value);
        assert forall|i:int| 0<=i<reclaimed.len() implies reclaimed[i].row_id==row_id
            && reclaimed[i].live_head_value==Some(initial.image.rows[head_offset].value)
            && (exists|p:u32| removed.contains(p) && initial.image.rows.contains_key(p)
                && initial.image.rows[p].value==reclaimed[i].reclaimed_value) by {
            if i==old_reports.len() { assert(removed.contains(curr_ptr)); }
        }
    }
    curr_offset = next_offset; continue;''',1)
    generated='''let ghost initial=driver.state();
    let mut reclaimed=Vec::new();
    proof {
        safe_reflexive(initial,row_id,tx);
        if prefix_ineligible(initial.image,row_id,tx,global_xmin) { prefix_reflexive(initial,row_id,tx); }
    }
    '''+adapter.show_tokens(body)+'''
    proof {
        assert(reachable(driver.state().image,head_offset) =~= kept);
        assert(driver.state().recycled.difference(initial.recycled) =~= removed);
        assert forall|p:u32| driver.state().recycled.contains(p) && !initial.recycled.contains(p)
            <==> reachable(initial.image,head_offset).contains(p) && p!=head_offset && eligible(initial.image.rows[p],global_xmin) by {
            if reachable(initial.image,head_offset).contains(p) && p!=head_offset && eligible(initial.image.rows[p],global_xmin)
                && !removed.contains(p) {
                assert(kept.contains(p));
                reachable_rank(driver.state().image,head_offset,p);
            }
        }
    }
    Ok(reclaimed)
    '''
    marker='/* NATIVE_VACUUM_ROW */'
    if template.count(marker)!=1: raise ValueError('vacuum placeholder changed')
    return template.replace(marker,generated)+'\n'+render_horizon(source,VACUUM_SOURCE.read_text())

def render_horizon(source,vacuum):
    arena=lookup.method(source,'shared_arena','fn shared_arena(&self)->&Arc<ShmArena>')
    if arena!=adapter.tokenize('&self.shm'):raise ValueError('changed table arena accessor')
    public=lookup.method(source,'vacuum_reclaim_once','fn vacuum_reclaim_once(&self,requested_xmin:TxId)->Result<Vec<VacuumReclaimedRow<T>>,Error>')
    expected='let retained_xmin = crate::vacuum::compute_global_xmin(self.shm.as_ref()); self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))'
    if public!=adapter.tokenize(expected):raise ValueError('changed public vacuum dispatch')
    public=adapter.replace(public,'crate::vacuum::compute_global_xmin(self.shm.as_ref())','compute_global_xmin(driver)',1)
    public=adapter.replace(public,'self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))','driver.reclaim_before(min_horizon(requested_xmin,retained_xmin))',1)
    compute=lookup.method(vacuum,'compute_global_xmin','fn compute_global_xmin(shm:&ShmArena)->TxId')
    if compute!=adapter.tokenize('shm.proc_array().oldest_snapshot_xmin(shm.global_txid())'):raise ValueError('changed same-arena horizon computation')
    compute_code='''let result=lifecycle::oldest_snapshot_xmin(driver);
    proof {
        assert forall|i:int| 0<=i<driver.state().slots.len() && driver.state().slots[i].txid!=0 implies
            result<=driver.state().slots[i].snapshot_xmin by {
            lifecycle::retention_covers_active_snapshot(driver.state(),i,lifecycle::PROCARRAY_SLOTS as int);
        }
    }
    // The native call returns after dropping its scoped lifecycle guard.
    driver.release_lifecycle();
    result'''
    marker='pub fn run_vacuum_pass<T>'
    if vacuum.count(marker)!=1:raise ValueError('missing/duplicate collector')
    start=vacuum.index(marker);opening=vacuum.index('{',start)
    header=vacuum[start:opening]
    expected_header="pub fn run_vacuum_pass<T>(table: &OccTable<T>) -> Result<Vec<VacuumReclaimedRow<T>>, VacuumError> where T: Copy + Send + Sync + 'static,"
    if re.sub(r'\s+','',header)!=re.sub(r'\s+','',expected_header):raise ValueError('changed collector signature')
    end=opening+1;depth=1
    while depth:
        if vacuum[end]=='{':depth+=1
        elif vacuum[end]=='}':depth-=1
        end+=1
    body=adapter.tokenize(vacuum[opening:end])[1:-1]
    expected='let global_xmin = compute_global_xmin(table.shared_arena().as_ref()); table.vacuum_reclaim_before(global_xmin).map_err(VacuumError::Occ)'
    if body!=adapter.tokenize(expected):raise ValueError('changed single-computation collector dispatch')
    body=adapter.replace(body,'compute_global_xmin(table.shared_arena().as_ref())','compute_global_xmin(driver)',1)
    body=adapter.replace(body,'table.vacuum_reclaim_before(global_xmin).map_err(VacuumError::Occ)','match driver.reclaim_before(global_xmin) { Ok(rows)=>Ok(rows), Err(error)=>Err(VacuumError::Occ(error)) }',1)
    result=HORIZON.read_text()
    for marker,code in [('NATIVE_COMPUTE_HORIZON',compute_code),('NATIVE_PUBLIC_VACUUM',adapter.show_tokens(public)),('NATIVE_COLLECTOR_DISPATCH',adapter.show_tokens(body))]:
        needle='/* '+marker+' */'
        if result.count(needle)!=1:raise ValueError('changed horizon placeholder')
        result=result.replace(needle,code)
    return result

def render(source=None,template=None):
    source=SOURCE.read_text() if source is None else source
    common=lookup.render(source)
    if source==SOURCE.read_text() and lookup.OUTPUT.read_text()!=common: raise ValueError('stale common lookup module')
    module=render_module(source,template)
    if not module.startswith('use crate::lookup;\n'): raise ValueError('changed shared lookup import')
    lspec=importlib.util.spec_from_file_location('retention_lifecycle_adapter',ROOT/'verification/lifecycle/generate.py')
    lifecycle=importlib.util.module_from_spec(lspec);lspec.loader.exec_module(lifecycle)
    lproof=lifecycle.render(lifecycle.SOURCE.read_text())
    if lifecycle.OUTPUT.read_text()!=lproof:raise ValueError('stale embedded lifecycle proof')
    return '// Generated actual native acquired-row vacuum proof.\npub mod lookup {\n'+common+'\n}\npub mod lifecycle {\n'+lproof+'\n}\n'+module.removeprefix('use crate::lookup;\n').replace('use crate::lifecycle;\n','')

def main():
    p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');a=p.parse_args();out=render()
    if a.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=out: raise SystemExit('stale row retention proof')
    else: OUTPUT.write_text(out)
if __name__=='__main__':main()
