#!/usr/bin/env python3
"""Selected data projection of the actual ordinary commit publication interval.

Every omitted prefix/suffix statement is checked exactly. Their effects and
entry invariants are handled outside this conditional data-refinement slice.
"""
from pathlib import Path
import importlib.util
ROOT=Path(__file__).resolve().parents[2]
_spec=importlib.util.spec_from_file_location('commit_data_native_driver',ROOT/'verification/concurrent/generate.py')
adapter=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(adapter)
EXPECTED_HEADER='    fn commit_with_record_impl<const WRITE_AHEAD: bool, E, P, F>(\n        &self,\n        tx: &mut OccTransaction<T>,\n        prepare: P,\n    ) -> Result<OccCommitRecord<T>, E>\n    where\n        E: From<Error>,\n        P: FnOnce(&OccCommitRecord<T>) -> Result<F, E>,\n        F: FnOnce(&OccCommitRecord<T>) -> Result<(), E>,\n    '
EXPECTED_PREFIX='self . ensure_open ( tx ) ? ;\nlet final_write_indices = self . final_write_indices ( tx ) ;\nlet index_changes = self . index_changes ( tx , & final_write_indices ) ? ;\nlet index_keys = self . index_lock_keys ( tx , & index_changes ) ? ;\nlet prepared = if WRITE_AHEAD {\n    Some ( self . prepare_before_publish ( tx , & final_write_indices , prepare ) ? )\n}\nelse {\n    None\n}\n;\nlet index_locks = match self . acquire_index_locks ( & index_keys ) {\n    Ok ( locks ) => locks , Err ( Error :: SerializationFailure ) => {\n        self . abort_for_serialization_failure ( tx ) ;\n        return Err ( Error :: SerializationFailure . into ( ) ) ;\n    }\n    Err ( err ) => return Err ( err . into ( ) ) ,\n}\n;\nlet locks = match self . acquire_partition_locks ( tx ) {\n    Ok ( locks ) => locks , Err ( Error :: SerializationFailure ) => {\n        drop ( index_locks ) ;\n        self . abort_for_serialization_failure ( tx ) ;\n        return Err ( Error :: SerializationFailure . into ( ) ) ;\n    }\n    Err ( err ) => return Err ( err . into ( ) ) ,\n}\n;\nif ! WRITE_AHEAD && ! final_write_indices . is_empty ( ) {\n    self . ensure_unlogged_write_allowed ( ) ? ;\n}\nif let Err ( err ) = self . check_not_poisoned ( ) {\n    drop ( locks ) ;\n    drop ( index_locks ) ;\n    self . abort_preparation ( tx ) ? ;\n    return Err ( err . into ( ) ) ;\n}\nif self . index_read_conflict ( tx ) ? || self . has_row_lock_conflict ( tx ) ? || self . has_serialization_conflict ( tx ) ? || self . has_write_base_conflict ( tx , & final_write_indices ) ? {\n    drop ( locks ) ;\n    drop ( index_locks ) ;\n    self . abort_for_serialization_failure ( tx ) ;\n    return Err ( Error :: SerializationFailure . into ( ) ) ;\n}'
EXPECTED_SUFFIX='# [ cfg ( test ) ] INDEX_PUBLICATION_HOOK . with ( | hook | {\n    if let Some ( hook ) = hook . borrow_mut ( ) . take ( ) {\n        hook ( ) ;\n    }\n}\n) ;\nlet commit_record = OccCommitRecord {\n    txid : tx . txid , writes ,\n}\n;\nif let Err ( err ) = self . recycle_non_final_writes ( tx , & final_write_indices ) {\n    self . poison_indexes ( ) ;\n    tx . write_set . clear ( ) ;\n    let _ = self . finish_transaction ( tx ) ;\n    return Err ( err . into ( ) ) ;\n}\ntx . read_set . clear ( ) ;\ntx . index_reads . clear ( ) ;\ntx . index_conflict = false ;\ntx . write_set . clear ( ) ;\ntx . savepoints . clear ( ) ;\nlet _ = self . shm . flush_local_recycle_caches ( ) ;\nlet finish = match self . finish_transaction ( tx ) {\n    Ok ( ( ) ) => self . publish_index_stamps ( & index_changes ) , Err ( err ) => Err ( err ) ,\n}\n;\nif let Err ( err ) = finish {\n    self . poison_indexes ( ) ;\n    return Err ( err . into ( ) ) ;\n}\ndrop ( locks ) ;\ndrop ( index_locks ) ;\nOk ( commit_record )'
PREPARED_MATCH='''let prepared = match prepared {
    Some((record,before_publish)) => {
        self.invoke_before_publish(tx,&index_changes,&inserted,&record,before_publish)?;
        Some(record)
    }
    None=>None,
};'''
PUBLICATION_MATCH='''let publication = match prepared {
    Some(record)=>self.publish_prepared_write_set(&record).map(|()|record.writes),
    None=>self.publish_write_set(tx,&final_write_indices),
};'''
OMITTED_ERROR_CLEANUP='''tx.write_set.clear();tx.read_set.clear();tx.index_reads.clear();
    let _=self.finish_transaction(tx);'''

def render(source):
    marker='    fn commit_with_record_impl<'
    if source.count(marker)!=1:raise ValueError('native commit signature missing or ambiguous')
    begin=source.index(marker);opening=source.index('{',begin)
    if adapter.tokenize(source[begin:opening])!=adapter.tokenize(EXPECTED_HEADER):
        raise ValueError('native commit signature/policy parameter binding changed')
    body=adapter.method_body(source,'    fn commit_with_record_impl<','\n    fn prepare_before_publish<')
    wrapper=adapter.method_body(source,'    pub fn commit_with_record(','\n    pub(crate) fn commit_with_record_before_publish<')
    if wrapper!=adapter.tokenize('self.commit_with_record_impl::<false, Error, _, _>(tx, |_| { Ok(|_: &OccCommitRecord<T>| Ok(())) })'):
        raise ValueError('ordinary policy is no longer WRITE_AHEAD=false')
    prefix=adapter.tokenize(EXPECTED_PREFIX);suffix=adapter.tokenize(EXPECTED_SUFFIX)
    if body[:len(prefix)]!=prefix or body[-len(suffix):]!=suffix:
        raise ValueError('native commit entry/exit context changed; review omitted effects')
    segment=body[len(prefix):-len(suffix)]
    if {'unsafe','assume','admit','external_body','#','proof','ghost','tracked','invariant','decreases'}.intersection(segment):
        raise ValueError('unsupported commit data syntax or bypass')
    # WRITE_AHEAD=false gives None at the checked prefix's sole construction of
    # prepared, so the checked Some callback branch is unreachable in this slice.
    substitutions=[
        (PREPARED_MATCH,'',1),
        (PUBLICATION_MATCH,'let publication=publish_rows_ordinary(storage,plan,ordinary_plan,Ghost(before));',1),
        ('self.prepare_index_destinations(&index_changes)?',
         'match prepare_destinations(storage,plan) { Ok(value)=>value, Err(err)=>return Err(DataError::Posting(err)), }',1),
        ('self.remove_index_sources(&index_changes)?',
         'match remove_sources(storage,plan,Ghost(before)) { Ok(value)=>value, Err(err)=>return Err(DataError::Posting(err)), }',1),
        ('self.poison_indexes()','poison(storage)',1),
        (OMITTED_ERROR_CLEANUP,'',1),
        ('return Err(Error::Index(format!("row publication failed after index preparation ({err}); table poisoned")).into());',
         'return Err(DataError::Publication(err));',1),
    ]
    for old,new,count in substitutions:segment=adapter.replace(segment,old,new,count)
    if 'self' in segment or 'tx' in segment or 'prepared' in segment:raise ValueError('unadapted data segment state')
    return 'let ghost before=storage.view();\n'+adapter.show_tokens(segment)+'\nOk(writes)'
