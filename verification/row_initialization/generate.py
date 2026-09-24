#!/usr/bin/env python3
"""Source-bind all constructor fields and the native exclusive ptr::write call."""
from pathlib import Path
import argparse
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
SOURCE=ROOT/'aerostore_core/src/occ_partitioned.rs'
CONTRACTS=HERE/'contracts.rs'
OUTPUT=HERE/'initialization.verus.rs'
ROOTS=['new_row','initialized_preserves_protected','initialized_image_valid','initialize_row']
MUTATIONS=[
    ('constructor_wrong_xmin','constructor','            xmin,','            xmin: 0,'),
    ('constructor_stale_xmax','constructor','xmax: AtomicU64::new(0)','xmax: AtomicU64::new(1)'),
    ('constructor_locked','constructor','is_locked: AtomicBool::new(false)','is_locked: AtomicBool::new(true)'),
    ('constructor_stale_owner','constructor','lock_owner_txid: AtomicU64::new(0)','lock_owner_txid: AtomicU64::new(1)'),
    ('constructor_wrong_next','constructor','next: AtomicU32::new(next)','next: AtomicU32::new(0)'),
    ('constructor_stale_recycle_next','constructor','recycle_next: AtomicU32::new(EMPTY_PTR)','recycle_next: AtomicU32::new(next)'),
    ('initialize_wrong_xmin','initializer','OccRow::new(value, xmin, next)','OccRow::new(value, 0, next)'),
    ('initialize_wrong_next','initializer','OccRow::new(value, xmin, next)','OccRow::new(value, xmin, 0)'),
]
spec=importlib.util.spec_from_file_location('row_initialization_lookup_adapter',ROOT/'verification/lookup/generate.py')
lookup=importlib.util.module_from_spec(spec);spec.loader.exec_module(lookup)


def constructor(source):
    marker='impl<T: Copy> OccRow<T> {'
    if source.count(marker)!=1: raise ValueError('native OccRow implementation changed')
    start=source.index(marker);opening=source.index('{',start);depth=1;stop=opening+1
    while depth:
        if source[stop]=='{': depth+=1
        elif source[stop]=='}': depth-=1
        stop+=1
    tokens=lookup.tokenize(source[start:stop])
    prefix=lookup.tokenize('impl<T:Copy> OccRow<T> {')
    found=[i for i in range(len(tokens)) if tokens[i:i+len(prefix)]==prefix]
    if len(found)!=1: raise ValueError('native OccRow implementation changed')
    begin=found[0]+len(prefix)-1;end=lookup.adapter.balanced_end(tokens,begin)
    body=lookup.method(lookup.show(tokens[begin:end]),'new','fn new(value:T,xmin:TxId,next:u32)->Self')
    if body[:2]!=['Self','{'] or lookup.adapter.balanced_end(body,1)!=len(body):
        raise ValueError('native constructor must be one complete struct expression')
    fields={};pos=2
    while pos<len(body)-1:
        name=body[pos];pos+=1
        if name in fields: raise ValueError('duplicate constructor field')
        if body[pos] in (',','}'):
            value=[name]
        elif body[pos]==':':
            pos+=1;start=pos
            while pos<len(body)-1 and body[pos]!=',':
                if body[pos] in ('(', '[', '{'):
                    pos=lookup.adapter.balanced_end(body,pos)
                else: pos+=1
            value=body[start:pos]
        else: raise ValueError('unsupported constructor field')
        fields[name]=value
        if body[pos]==',': pos+=1
    expected={'xmin','xmax','is_locked','lock_owner_txid','next','recycle_next','value'}
    if set(fields)!=expected: raise ValueError('native constructor field set changed')
    atomics={'xmax':'AtomicU64','is_locked':'AtomicBool','lock_owner_txid':'AtomicU64',
             'next':'AtomicU32','recycle_next':'AtomicU32'}
    for field,atomic in atomics.items():
        value=fields[field]
        prefix=[atomic,'::','new','(']
        if value[:4]!=prefix or lookup.adapter.balanced_end(value,3)!=len(value):
            raise ValueError('native atomic constructor changed: '+field)
        fields[field]=value[4:-1]
    for field,value in fields.items():
        if {'unsafe','assume','admit','external_body','#','proof','ghost','tracked'}.intersection(value):
            raise ValueError('unsupported constructor syntax')
        fields[field]=lookup.show(['0' if t=='EMPTY_PTR' else t for t in value])
    return ('Cell { row: Row { xmin: '+fields['xmin']+', xmax: '+fields['xmax']+
        ', next: '+fields['next']+', value: '+fields['value']+', locked: '+fields['is_locked']+
        ', owner: '+fields['lock_owner_txid']+' }, recycle_next: '+fields['recycle_next']+' }')


def render_module(source=None):
    source=SOURCE.read_text() if source is None else source
    declarations=lookup.tokenize(source.split('#[cfg(test)]',1)[0])
    constant=lookup.tokenize('const EMPTY_PTR:u32=0;')
    if sum(declarations[i:i+len(constant)]==constant for i in range(len(declarations)))!=1:
        raise ValueError('native empty pointer constant changed')
    body=lookup.method(source,'initialize_row',
        'fn initialize_row(&self,row_ptr:&RelPtr<OccRow<T>>,value:T,xmin:TxId,next:u32)->Result<(),Error>')
    # Only the exact single ptr::write expression may cross the native unsafe
    # boundary; unrelated unsafe code is never discarded by this adaptation.
    where=[i for i,t in enumerate(body) if t=='unsafe']
    if len(where)!=1 or body[where[0]+1]!='{': raise ValueError('initializer unsafe block changed')
    at=where[0];end=lookup.adapter.balanced_end(body,at+1)
    inside=body[at+2:end-1]
    prefix=lookup.tokenize('std::ptr::write(')
    if inside[:len(prefix)]!=prefix or lookup.adapter.balanced_end(inside,len(prefix)-1)!=len(inside)-1 or inside[-1]!=';':
        raise ValueError('initializer unsafe body is not one ptr::write')
    body=body[:at]+inside+body[end:]
    if {'unsafe','assume','admit','external_body','#','proof','ghost','tracked'}.intersection(body):
        raise ValueError('unsupported initializer syntax')
    body=lookup.replace(body,'row_ptr.load(Ordering::Acquire)','row_ptr',1)
    body=lookup.replace(body,'self.resolve_row_ptr_raw(offset)?','driver.resolve_row_ptr_raw(offset)?',1)
    body=lookup.replace(body,'std::ptr::write','driver.write_cell',1)
    body=lookup.replace(body,'OccRow::new','new_row',1)
    result=CONTRACTS.read_text()
    for marker,replacement in [('/* NATIVE_CONSTRUCTOR */',constructor(source)),('/* NATIVE_INITIALIZE */',lookup.show(body))]:
        if result.count(marker)!=1: raise ValueError('missing/duplicate initializer marker')
        result=result.replace(marker,replacement)
    return result


def mutation_source(item,source=None):
    source=SOURCE.read_text() if source is None else source
    name,target,old,new=item
    if target=='constructor':
        start=source.index('impl<T: Copy> OccRow<T> {')
        end=source.index('\npub struct RowLockGuard',start)
    elif target=='initializer':
        start=source.index('    fn initialize_row(')
        end=source.index('\n    fn resolve_row_ptr_raw(',start)
    else: raise ValueError('unknown initialization mutation target')
    body=source[start:end]
    if body.count(old)!=1: raise ValueError('missing/ambiguous mutation anchor: '+name)
    return source[:start]+body.replace(old,new,1)+source[end:]


def render(source=None):
    source=SOURCE.read_text() if source is None else source
    return '// Generated native row construction and initialization.\n'+'pub mod lookup {\n'+lookup.render(source)+'\n}\n'+render_module(source).replace('use crate::lookup;\n','',1)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    result=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=result: raise SystemExit('stale row-initialization proof')
    else: OUTPUT.write_text(result)
if __name__=='__main__':main()
