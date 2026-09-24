#!/usr/bin/env python3
"""Restricted translation of the native prepared row publication loop."""
from pathlib import Path
import argparse
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
CONTRACTS=HERE/'contracts.rs'
OUTPUT=HERE/'publication.verus.rs'
spec=importlib.util.spec_from_file_location('row_publication_lookup_adapter',ROOT/'verification/lookup/generate.py')
lookup=importlib.util.module_from_spec(spec);spec.loader.exec_module(lookup)

def render_module(source=None):
    source=SOURCE.read_text() if source is None else source
    body=lookup.method(source,'publish_prepared_write_set',
        'fn publish_prepared_write_set(&self,record:&OccCommitRecord<T>)->Result<(),Error>')
    for published in ('false','true'):
        hook=lookup.tokenize('#[cfg(test)] ROW_PUBLICATION_STEP_HOOK.with(|hook| { if let Some(hook) = hook.borrow_mut().as_mut() { hook(write.row_id, '+published+'); } });')
        count=sum(body[i:i+len(hook)]==hook for i in range(len(body)))
        if count>1: raise ValueError('duplicate publication test hook')
        if count==1: body=lookup.replace(body,lookup.show(hook),'',1)
    banned={'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}
    if banned.intersection(body): raise ValueError('unsupported publication syntax or bypass')
    body=lookup.replace(body,'self.slot_ref(write.row_id)?','driver.slot_ref(write.row_id)?',1)
    body=lookup.replace(body,'EMPTY_PTR','0',1)
    body=lookup.replace(body,'self.resolve_row_ptr(&RelPtr::from_offset(write.base_offset))?',
        'driver.resolve(write.base_offset)?',1)
    body=lookup.replace(body,'base_row.xmax.compare_exchange(0,record.txid,Ordering::AcqRel,Ordering::Acquire)',
        'driver.compare_xmax(base_row,0,record.txid,Ghost(write.row_id),Ghost(write.new_offset))',1)
    body=lookup.replace(body,'self.resolve_row_ptr(&RelPtr::from_offset(write.new_offset))?',
        'driver.resolve(write.new_offset)?',1)
    body=lookup.replace(body,'new_row.next.store(write.base_offset,Ordering::Release)',
        'driver.store_next(new_row,write.base_offset,Ghost(write.row_id))',1)
    body=lookup.replace(body,'slot.head.compare_exchange(write.base_offset,write.new_offset,Ordering::AcqRel,Ordering::Acquire,)',
        'driver.compare_head(slot,write.base_offset,write.new_offset)',1)
    body=lookup.loop(body,'for write in &record.writes','''
    let ghost initial=driver.image();
    let ghost initially_infallible=driver.infallible();
    let mut i=0;
    while i<record.writes.len()
        invariant initial==old(driver).image(), initially_infallible==old(driver).infallible(),
            record.writes.len()==1,i<=record.writes.len(),
            prepared(initial,record.writes[0],record.txid),
            driver.infallible()==initially_infallible,
            driver.authorized(record.writes[0].row_id,record.writes[0].new_offset),
            driver.image()==publication_prefix(initial,record.writes[0],record.txid,if i==0 {0} else {3}),
        decreases record.writes.len()-i,
    {
        let write=&record.writes[i];
    ''','''
        proof {assert(driver.image()==publication_prefix(initial,record.writes[0],record.txid,3));}
        i+=1;
    }
    ''')
    result=CONTRACTS.read_text()
    marker='/* NATIVE_PUBLICATION */'
    if result.count(marker)!=1: raise ValueError('missing/duplicate publication marker')
    return result.replace(marker,lookup.show(body))

def render(source=None):
    source=SOURCE.read_text() if source is None else source
    embedded=lookup.render(source)
    if source==SOURCE.read_text() and lookup.OUTPUT.read_text()!=embedded: raise ValueError('stale lookup component')
    return '// Generated: exact native prepared row-publication loop.\n'+'pub mod lookup {\n'+embedded+'\n}\n'+render_module(source).replace('use crate::lookup;\n','',1)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    expected=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=expected: raise SystemExit('stale row-publication proof')
    else: OUTPUT.write_text(expected)
if __name__=='__main__':main()
