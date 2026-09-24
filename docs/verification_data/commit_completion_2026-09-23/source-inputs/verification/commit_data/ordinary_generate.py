#!/usr/bin/env python3
"""Restrict and lower actual ordinary native row publication into shared fields."""
from pathlib import Path
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
_spec=importlib.util.spec_from_file_location('commit_data_ordinary_lookup',ROOT/'verification/lookup/generate.py')
lookup=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(lookup)
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
CONTRACTS=HERE/'ordinary.rs'

def render(source=None):
    source=SOURCE.read_text() if source is None else source
    body=lookup.method(source,'publish_write_set','''fn publish_write_set(&self,tx:&OccTransaction<T>,final_write_indices:&[usize],)->Result<Vec<OccCommittedWrite<T>>,Error>''')
    for phase in ('false','true'):
        hook=lookup.tokenize('#[cfg(test)] ROW_PUBLICATION_STEP_HOOK.with(|hook| { if let Some(hook) = hook.borrow_mut().as_mut() { hook(write.row_id, '+phase+'); } });')
        occurrences=sum(body[i:i+len(hook)]==hook for i in range(len(body)))
        if occurrences!=1:raise ValueError('ordinary publication hook changed')
        body=lookup.replace(body,lookup.show(hook),'',1)
    if {'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}.intersection(body):
        raise ValueError('unsupported ordinary publication syntax or bypass')
    substitutions=[
        ('final_write_indices','indices',2),
        ('self.slot_ref(write.row_id)?','driver.slot_ref(write.row_id)?',1),
        ('write.base_ptr.load(Ordering::Acquire)','write.base_offset',1),
        ('write.new_ptr.load(Ordering::Acquire)','write.new_offset',1),
        ('EMPTY_PTR','0',1),
        ('self.resolve_row_ptr(&write.base_ptr)?','driver.resolve(write.base_offset)?',1),
        ('self.resolve_row_ptr(&write.new_ptr)?','driver.resolve(write.new_offset)?',2),
        ('base_row.xmax.compare_exchange(0,tx.txid,Ordering::AcqRel,Ordering::Acquire)',
         'driver.compare_xmax(base_row,0,tx.txid,Ghost(write.row_id),Ghost(write.new_offset))',1),
        ('base_row.value','driver.load_value(base_row)',1),
        ('new_row.value','driver.load_value(new_row)',2),
        ('new_row.next.store(base_offset,Ordering::Release)','driver.store_next(new_row,base_offset,Ghost(write.row_id))',1),
        ('slot.head.compare_exchange(base_offset,new_offset,Ordering::AcqRel,Ordering::Acquire)',
         'driver.compare_head(slot,base_offset,new_offset)',1),
        ('OccCommittedWrite','PublishedWrite',1),
        ('Error::SerializationFailure','lookup::Error::SerializationFailure',2),
    ]
    for old,new,count in substitutions:body=lookup.replace(body,old,new,count)
    body=lookup.loop(body,'for idx in indices','''
        let ghost initial=driver.image();
        let ghost initially_infallible=driver.infallible();
        let mut i=0;
        while i<indices.len()
            invariant i<=indices.len(), admitted(initial,*tx,indices@),initial==old(driver).image(),
                driver.infallible()==initially_infallible,initially_infallible==old(driver).infallible(),
                driver.authorized(tx.write_set[indices[0] as int].row_id,tx.write_set[indices[0] as int].new_offset),
                driver.image()==publication::publication_prefix(initial,row_write(tx.write_set[indices[0] as int]),tx.txid,if i==0 {0} else {3}),
                published.len()==i,
                i==1 ==> published[0]==report(initial,tx.write_set[indices[0] as int]),
            decreases indices.len()-i,
        {
            let idx=&indices[i];
    ''','''
            proof {assert(published[0]==report(initial,tx.write_set[indices[0] as int]));}
            i+=1;
        }
    ''')
    if 'self' in body:raise ValueError('unadapted ordinary publication receiver')
    template=CONTRACTS.read_text();marker='/* NATIVE_ORDINARY_PUBLICATION */'
    if template.count(marker)!=1:raise ValueError('ordinary marker changed')
    return template.replace(marker,lookup.show(body))
