#!/usr/bin/env python3
"""Lower the actual guarded write-base validator, preserving every branch."""
from pathlib import Path
import argparse
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
TEMPLATE=HERE/'admission.rs'
OUTPUT=HERE/'admission.verus.rs'

def component(name):
    spec=importlib.util.spec_from_file_location('admission_'+name,ROOT/'verification'/name/'generate.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def render_module(source=None):
    source=SOURCE.read_text() if source is None else source
    adapter=component('lookup')
    declarations=adapter.tokenize(source.split('#[cfg(test)]',1)[0])
    positions=[i for i in range(len(declarations)) if declarations[i:i+2]==['const','EMPTY_PTR']]
    if (len(positions)!=1 or declarations[positions[0]:declarations.index(';',positions[0])+1]
            !=adapter.tokenize('const EMPTY_PTR:u32=0;')):
        raise ValueError('native empty-pointer declaration changed')
    body=adapter.method(source,'has_write_base_conflict',
        'fn has_write_base_conflict(&self,tx:&OccTransaction<T>,final_write_indices:&[usize],)->Result<bool,Error>')
    if {'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}.intersection(body):
        raise ValueError('unsupported write admission syntax or proof bypass')
    replacements=[
        ('final_write_indices','indices',1),
        ('self.slot_ref(write.row_id)?','driver.slot_ref(write.row_id)?',1),
        ('slot.head.load(Ordering::Acquire)','driver.load_head(slot)',1),
        ('write.base_ptr.load(Ordering::Acquire)','write.base_offset',1),
        ('EMPTY_PTR','0',1),
        ('self.resolve_row_ptr(&write.base_ptr)?','driver.resolve(write.base_offset)?',1),
        ('base_row.xmax.load(Ordering::Acquire)','driver.load_xmax(base_row)',1),
    ]
    for old,new,count in replacements:body=adapter.replace(body,old,new,count)
    body=adapter.loop(body,'for idx in indices','''
        let mut i=0;
        while i<indices.len()
            invariant i<=indices.len(), selection_input(driver,*tx,indices@),
                forall|j:int| 0<=j<i ==> base_valid(driver.image(),tx.write_set[indices[j] as int]),
            decreases indices.len()-i,
        {
            let idx=&indices[i];
    ''','''
            proof {assert(base_valid(driver.image(),*write));}
            i+=1;
        }
    ''')
    if {'self','Ordering','base_ptr','head','xmax'}.intersection(body):
        raise ValueError('unadapted write admission primitive')
    template=TEMPLATE.read_text();marker='/* NATIVE_WRITE_BASE_CONFLICT */'
    if template.count(marker)!=1:raise ValueError('write admission marker changed')
    return template.replace(marker,adapter.show(body))

def render():
    return component('commit_data').render()+'\npub mod admission {\n'+render_module()+'\n}\n'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    expected=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=expected:raise SystemExit('stale write admission proof')
    else:OUTPUT.write_text(expected)
if __name__=='__main__':main()
