#!/usr/bin/env python3
"""Restricted source adapter for last-write selection and index key planning."""
from pathlib import Path
import argparse
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
OUTPUT=HERE/'planning.verus.rs'
CONTRACTS=HERE/'contracts.rs'

def component(name):
    spec=importlib.util.spec_from_file_location('write_plan_'+name,ROOT/'verification'/name/'generate.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

adapter=component('lookup')
data=component('commit_data')

def render_module(source=None):
    source=SOURCE.read_text() if source is None else source
    final=adapter.method(source,'final_write_indices','fn final_write_indices(&self,tx:&OccTransaction<T>)->Vec<usize>')
    changes=adapter.method(source,'index_changes','fn index_changes(&self,tx:&OccTransaction<T>,final_writes:&[usize])->Result<Vec<IndexChange>,Error>')
    banned={'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}
    if any(banned.intersection(body) for body in (final,changes)):
        raise ValueError('unsupported native planning syntax or proof bypass')
    final=adapter.replace(final,'BTreeMap::<usize,usize>::new()','M::new()',1)
    final=adapter.loop(final,'for (idx,write) in tx.write_set.iter().enumerate()', '''
        let mut idx=0;
        while idx<tx.write_set.len()
            invariant idx<=tx.write_set.len(),prefix_map(tx.write_set@,idx as int,by_row.map()),
            decreases tx.write_set.len()-idx,
        {
            let write=&tx.write_set[idx];
            let ghost previous=by_row.map();
    ''','''
            proof {prefix_insert(tx.write_set@,idx as int,previous);}
            idx+=1;
        }
    ''')
    final=adapter.replace(final,'by_row.into_values().collect()', '''
        let ghost map=by_row.map();let values=by_row.into_values();
        proof {map_yields_selection(*tx,map,values.view());} values
    ''',1)
    changes=adapter.replace(changes,'final_writes','indices',1)
    for old,new,count in [
        ('&self.resolve_row_ptr(&write.base_ptr)?.value','&driver.load_value(driver.resolve(write.base_offset)?)',1),
        ('&self.resolve_row_ptr(&write.new_ptr)?.value','&driver.load_value(driver.resolve(write.new_offset)?)',1),
        ('(bound.key)(before)','driver.key(binding,before)',1),
        ('(bound.key)(after)','driver.key(binding,after)',1),
        ('bound.index.transactional_prevalidate(key,&write.row_id)?','driver.prevalidate(binding,key,&write.row_id)?',2),
        ('IndexChange','postings::IndexChange',1),
        ('continue;','binding=binding+1;continue;',1),
    ]: changes=adapter.replace(changes,old,new,count)
    changes=adapter.loop(changes,'for (binding,bound) in self.indexes.iter().enumerate()', '''
        let mut binding=0;
        while binding<driver.binding_count()
            invariant binding<=1,driver.bindings()==1,i==0,
                admissible(driver.image(),*tx,indices@),
                before==&driver.image().rows[write.base_offset].value,
                after==&driver.image().rows[write.new_offset].value,
                write==&tx.write_set[indices[0] as int],
                binding==0 ==> changes.len()==0,
                binding==1 ==> extracted(driver.image(),*tx,indices@,changes@),
                checked(*driver,changes@),
            decreases 1-binding,
        {
    ''','''
            binding+=1;
        }
    ''')
    changes=adapter.loop(changes,'for write_idx in indices','''
        let mut i=0;
        while i<indices.len()
            invariant i<=1,admissible(driver.image(),*tx,indices@),driver.bindings()==1,
                i==0 ==> changes.len()==0,
                i==1 ==> extracted(driver.image(),*tx,indices@,changes@),
                checked(*driver,changes@),
            decreases indices.len()-i,
        {
            let write_idx=&indices[i];
    ''','''
            i+=1;
        }
    ''')
    if 'self' in final+changes:raise ValueError('unadapted planning receiver')
    template=CONTRACTS.read_text()
    for marker,body in [('NATIVE_FINAL_WRITES',final),('NATIVE_INDEX_CHANGES',changes)]:
        marker='/* '+marker+' */'
        if template.count(marker)!=1:raise ValueError('planning marker changed')
        template=template.replace(marker,adapter.show(body))
    return template

def render():
    return data.render()+'\npub mod planning {\n'+render_module()+'\n}\n'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    expected=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=expected:raise SystemExit('stale write planning source')
    else:OUTPUT.write_text(expected)
if __name__=='__main__':main()
