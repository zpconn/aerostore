#!/usr/bin/env python3
"""Restricted native visibility/read/materialization adapter; see README."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / 'aerostore_core/src/occ_partitioned.rs'
CONTRACTS = HERE / 'contracts.rs'
HISTORY = HERE / 'history.rs'
OUTPUT = HERE / 'lookup.verus.rs'
TRAVERSAL_HOOK = '''#[cfg(test)] ROW_TRAVERSAL_STEP_HOOK.with(|hook| {
    if let Some(hook) = hook.borrow_mut().take() {
        hook(row_ptr.load(Ordering::Acquire), head_offset);
    }
});'''
spec = importlib.util.spec_from_file_location('lookup_token_adapter', ROOT / 'verification/concurrent/generate.py')
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
adapter.TOKEN = re.compile(adapter.TOKEN.pattern.replace('::|->','<=|>=|::|->'))
tokenize,replace,show = adapter.tokenize,adapter.replace,adapter.show_tokens


def method(source, name, signature):
    matches = list(re.finditer(r'\bfn\s+'+name+r'\s*\(',source))
    if len(matches) != 1:
        raise ValueError('expected one native method: '+name)
    start = matches[0].start()
    opening = source.index('{',start)
    def signature_tokens(text):
        tokens=tokenize(text)
        return [v for i,v in enumerate(tokens) if not (v==',' and i+1<len(tokens) and tokens[i+1] in (')','>'))]
    if signature_tokens(source[start:opening]) != signature_tokens(signature):
        raise ValueError('unsupported native signature: '+name)
    # Tokenize only the balanced source body, which contains no string braces
    # in the selected routines. Token balancing then independently validates it.
    depth=1; end=opening+1
    while depth:
        if source[end]=='{': depth+=1
        elif source[end]=='}': depth-=1
        end+=1
    tokens=tokenize(source[opening:end])
    if adapter.balanced_end(tokens,0)!=len(tokens):
        raise ValueError('unbalanced native method')
    return tokens[1:-1]


def loop(tokens, header, prefix, suffix):
    needle=tokenize(header+' {')
    positions=[i for i in range(len(tokens)) if tokens[i:i+len(needle)]==needle]
    if len(positions)!=1: raise ValueError('missing/duplicate loop: '+header)
    pos=positions[0];end=adapter.balanced_end(tokens,pos+len(needle)-1)
    return tokens[:pos]+[prefix]+tokens[pos+len(needle):end-1]+[suffix]+tokens[end:]


def render(source=None):
    source=SOURCE.read_text() if source is None else source
    # Read actual declarations, ignoring comments; a commented-out old value
    # must not satisfy source freshness after the executable constant changes.
    declarations=tokenize(source.split('#[cfg(test)]',1)[0])
    for name,expected in [('MAX_VISIBLE_CHAIN_STEPS','const MAX_VISIBLE_CHAIN_STEPS: u32 = 262_144;'),
                          ('EMPTY_PTR','const EMPTY_PTR: u32 = 0;')]:
        positions=[i for i in range(len(declarations)) if declarations[i:i+2]==['const',name]]
        if len(positions)!=1 or declarations[positions[0]:declarations.index(';',positions[0])+1]!=tokenize(expected):
            raise ValueError('native declaration changed: '+name)
    visible=method(source,'is_visible','fn is_visible(&self, row: &OccRow<T>, tx: &OccTransaction<T>) -> bool')
    locked=method(source,'row_locked_by_other_tx','fn row_locked_by_other_tx(&self, row: &OccRow<T>, txid: TxId) -> bool')
    find=method(source,'find_visible_row_ptr','fn find_visible_row_ptr(&self, tx: &OccTransaction<T>, row_id: usize) -> Result<Option<RelPtr<OccRow<T>>>, Error>')
    # Strip only the reviewed deterministic cursor cut at its exact native site.
    # Unknown cfg(test) blocks and moved/modified calls remain rejected.
    find=replace(find,'head_offset = row.next.load(Ordering::Acquire); '+TRAVERSAL_HOOK,
        'head_offset = row.next.load(Ordering::Acquire);',1)
    read=method(source,'read','fn read(&self, tx: &mut OccTransaction<T>, row_id: usize) -> Result<Option<T>, Error>')
    record=method(source,'record_read','fn record_read(&self,tx:&mut OccTransaction<T>,row_id:usize,row_ptr:RelPtr<OccRow<T>>,xmin:TxId)')
    validate=method(source,'has_serialization_conflict','fn has_serialization_conflict(&self,tx:&OccTransaction<T>) -> Result<bool,Error>')
    lookup=method(source,'index_lookup','fn index_lookup(&self, tx: &mut OccTransaction<T>, index: &SecondaryIndex<usize>, predicate: &IndexCompare) -> Result<Vec<usize>, Error>')
    start=tokenize('let mut candidates: BTreeSet<usize> = candidates.into_iter().collect();')
    positions=[i for i in range(len(lookup)) if lookup[i:i+len(start)]==start]
    if len(positions)!=1: raise ValueError('native materialization boundary changed')
    materialize=lookup[positions[0]:]
    # Validate the existing capture/guard/raw-lookup prefix with its reviewed
    # adapter; no source preceding materialization silently escapes freshness.
    capspec=importlib.util.spec_from_file_location('lookup_capture_adapter',ROOT/'verification/predicate_capture/generate.py')
    capture=importlib.util.module_from_spec(capspec);capspec.loader.exec_module(capture)
    capture.render(source)
    banned={'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}
    if any(banned.intersection(body) for body in (visible,locked,find,read,record,validate,materialize)):
        raise ValueError('unsupported native syntax or proof bypass')
    visible=replace(visible,'row.xmax.load(Ordering::Acquire)','row.xmax',1)
    visible=replace(visible,'tx.snapshot_active.contains(&row.xmin)','active_contains(&tx.snapshot_active,row.xmin)',1)
    visible=replace(visible,'tx.snapshot_active.contains(&xmax)','active_contains(&tx.snapshot_active,xmax)',1)
    locked=replace(locked,'row.is_locked.load(Ordering::Acquire)','row.locked',1)
    locked=replace(locked,'row.lock_owner_txid.load(Ordering::Acquire)','row.owner',1)
    find=replace(find,'let slot = self.slot_ref(row_id)?; let mut head_offset = slot.head.load(Ordering::Acquire);',
        'let mut head_offset = driver.head(row_id)?;',1)
    find=replace(find,'let row_ptr = RelPtr::from_offset(head_offset);','let row_ptr = head_offset;',1)
    find=replace(find,'self.resolve_row_ptr(&row_ptr)?','driver.resolve(row_ptr)?',1)
    find=replace(find,'self.is_visible(row,tx)','is_visible(&row,tx)',1)
    find=replace(find,'row.next.load(Ordering::Acquire)','row.next',1)
    find=replace(find,'std::thread::yield_now()','driver.yield_now()',1)
    find=replace(find,'steps.wrapping_add(1)','steps + 1',1)
    find=replace(find,'let mut steps = 0_u32;','let mut steps: u32 = 0;',1)
    find=loop(find,'while head_offset != EMPTY_PTR','''
    while head_offset != EMPTY_PTR
        invariant image_valid(driver.image()), row_id < driver.image().capacity,
            head_offset == 0 || driver.image().rows.contains_key(head_offset),
            steps <= MAX_VISIBLE_CHAIN_STEPS,
            first_visible(driver.image(),head_offset,snapshot(*tx))
                == first_visible(driver.image(),driver.image().heads[row_id],snapshot(*tx)),
        decreases MAX_VISIBLE_CHAIN_STEPS + 1 - steps,
    {
    ''','}')
    read=replace(read,'self.ensure_open(tx)?','driver.ensure_open(tx)?',1)
    read=replace(read,'self.capacity()','driver.capacity()',2)
    read=replace(read,'tx.write_set.iter().rev().find(|entry| entry.row_id == row_id)',
        'latest_pending(&tx.write_set,row_id)',1)
    read=replace(read,'self.resolve_row_ptr(&pending.new_ptr)?','driver.resolve(pending.new_ptr)?',1)
    read=replace(read,'self.find_visible_row_ptr(tx,row_id)?','find_visible_row_ptr(driver,tx,row_id)?',1)
    read=replace(read,'self.resolve_row_ptr(&row_ptr)?','driver.resolve(row_ptr)?',1)
    read=replace(read,'self.row_locked_by_other_tx(row,tx.txid)','row_locked_by_other_tx(&row,tx.txid)',1)
    read=replace(read,'std::thread::yield_now()','driver.yield_now()',1)
    read=replace(read,'self.record_read(tx,row_id,row_ptr,observed_xmin)','record_read(tx,row_id,row_ptr,observed_xmin)',1)
    record=replace(record,'let row_offset = row_ptr.load(Ordering::Acquire);','let row_offset = row_ptr;',1)
    record=replace(record,'tx.read_set.iter().any(|entry| entry.row_ptr.load(Ordering::Acquire) == row_offset)',
        'read_already_recorded(&tx.read_set,row_offset)',1)
    record=replace(record,'ReadSetEntry','ReadRecord',1)
    record=replace(record,'observed_xmin:','xmin:',1)
    validate=replace(validate,'self.resolve_row_ptr(&read.row_ptr)?','driver.resolve(read.row_ptr)?',1)
    validate=replace(validate,'read.observed_xmin','read.xmin',1)
    validate=replace(validate,'row.xmax.load(Ordering::Acquire)','row.xmax',1)
    validate=replace(validate,'tx.snapshot_active.contains(&xmax)','active_contains(&tx.snapshot_active,xmax)',1)
    # Continue advances the native for iterator; retain that advance explicitly.
    validate=replace(validate,'continue;','ri = ri + 1; continue;',1)
    validate=loop(validate,'for read in &tx.read_set','''
    let mut ri=0;
    while ri<tx.read_set.len()
        invariant ri<=tx.read_set.len(),
            forall|i:int| 0<=i<tx.read_set.len() ==> driver.image().rows.contains_key(tx.read_set[i].row_ptr),
            forall|i:int| 0<=i<ri ==> !read_conflict(driver.image().rows[tx.read_set[i].row_ptr],tx.read_set[i],*tx),
        decreases tx.read_set.len()-ri,
    {
        let read=&tx.read_set[ri];
    ''','''
        ri += 1;
    }
    ''')
    materialize=replace(materialize,'let mut candidates: BTreeSet<usize> = candidates.into_iter().collect();',
        'let mut candidates = C::from_vec(candidates);',1)
    materialize=replace(materialize,'candidates.extend(tx.write_set.iter().map(|write| write.row_id));','''
        let mut wi = 0;
    ''',1)
    extend='''
    let ghost raw_ids = candidates.contents();
    while wi < tx.write_set.len()
        invariant wi <= tx.write_set.len(), initial == *old(tx), transaction_view_same(initial,*tx),
            tx.read_set@ == initial.read_set@,
            raw_ids == candidates_input@.to_set(),
            forall|id:usize| matches_row(driver.image(),initial,*predicate,id) ==>
                raw_ids.contains(id) || own_ids(initial.write_set@,initial.write_set.len() as int).contains(id),
            candidates.contents() == raw_ids.union(own_ids(tx.write_set@,wi as int)),
        decreases tx.write_set.len() - wi,
    {
        candidates.insert(tx.write_set[wi].row_id);
        wi += 1;
    }
    let ordered = candidates.into_vec();
    proof {
        assert forall|id:usize| matches_row(driver.image(),initial,*predicate,id)
            implies ordered@.contains(id) by {
            assert(raw_ids.contains(id) || own_ids(initial.write_set@,initial.write_set.len() as int).contains(id));
            assert(ordered@.to_set().contains(id));
        }
    }
    '''
    materialize=replace(materialize,'let mut wi = 0;','let mut wi = 0;',1)
    # Insert ghost specification and iterator loop without changing native work.
    # locate exact declaration end instead of relying on formatted output.
    decl=tokenize('let mut wi = 0;'); pos=next(i for i in range(len(materialize)) if materialize[i:i+len(decl)]==decl)
    materialize=materialize[:pos+len(decl)]+[extend]+materialize[pos+len(decl):]
    materialize=replace(materialize,'self.read(tx,row_id)?','read(driver,tx,row_id)?',1)
    materialize=replace(materialize,'(binding.key)(&value).as_ref().is_some_and(|key| index_predicate_matches(predicate,key))',
        'predicate.evaluate(&value)',1)
    push=tokenize('result.push(row_id);');pos=next(i for i in range(len(materialize)) if materialize[i:i+len(push)]==push)
    materialize=materialize[:pos]+['''proof {
        assert forall|i:int| 0 <= i < result.len() implies result[i] < row_id by {
            assert(result@.contains(result[i]));
            assert(ordered@.take(ci as int).contains(result[i]));
            prefix_less(ordered@,ci as int,result[i]);
        }
    }''']+materialize[pos:]
    materialize=loop(materialize,'for row_id in candidates','''
    let mut ci = 0;
    while ci < ordered.len()
        invariant ci <= ordered.len(), initial == *old(tx), transaction_view_same(initial,*tx),
            records_extend(initial.read_set@,tx.read_set@),
            records_have_provenance(driver.image(),initial,initial.read_set@,tx.read_set@),
            transaction_valid(driver.image(),*tx),
            ordered@.to_set() == raw_ids.union(own_ids(initial.write_set@,initial.write_set.len() as int)),
            forall|id:usize| matches_row(driver.image(),initial,*predicate,id) ==> ordered@.contains(id),
            forall|id:usize| result@.contains(id) <==>
                ordered@.take(ci as int).contains(id) && matches_row(driver.image(),initial,*predicate,id),
            forall|i:int,j:int| 0 <= i < j < ordered.len() ==> ordered[i] < ordered[j],
            forall|i:int,j:int| 0 <= i < j < result.len() ==> result[i] < result[j],
        decreases ordered.len() - ci,
    {
        let row_id = ordered[ci];
    ''','''
        proof { assert(ordered@.take(ci as int+1) =~= ordered@.take(ci as int).push(row_id)); }
        ci += 1;
    }
    ''')
    result=CONTRACTS.read_text()
    for marker,tokens in [('VISIBLE',visible),('ROW_LOCK',locked),('FIND_VISIBLE',find),('READ',read),('RECORD_READ',record),('ROW_VALIDATION',validate),('MATERIALIZE',materialize)]:
        if 'self' in tokens: raise ValueError('unadapted native receiver: '+marker)
        code=show(tokens)
        if marker=='MATERIALIZE': code='let ghost initial = *tx;\nlet ghost candidates_input = candidates;\n'+code
        if marker=='RECORD_READ': code+='\nproof { assert(tx.read_set[tx.read_set.len() as int-1].row_ptr == row_ptr); }\n'
        result=result.replace('/* NATIVE_'+marker+' */',code)
    return '// Generated from native visibility, read, and index_lookup materialization.\n'+result+'\n'+HISTORY.read_text()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
    generated=render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text()!=generated: raise SystemExit('stale lookup proof')
    else: OUTPUT.write_text(generated)


if __name__=='__main__': main()
