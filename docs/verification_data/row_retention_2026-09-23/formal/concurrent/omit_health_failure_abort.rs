// Generated from commit_with_record. See generate.py for every abstraction.
// Conditional event contracts for the existing commit orchestration. These are
// assumptions on native primitives, NOT implementations of those primitives.
// No property of postings, MVCC rows, mmap, atomics, or allocation is assumed
// beyond the named orchestration events recorded here.
use vstd::prelude::*;
verus! {

#[derive(PartialEq, Eq)]
pub enum Error { SerializationFailure, Index, Other }
pub struct OccTransaction<T> {
    pub txid: u64,
    pub write_set: Vec<T>,
    pub read_set: Vec<usize>,
    pub index_reads: Vec<usize>,
    pub index_conflict: bool,
    pub savepoints: Vec<usize>,
}
pub struct OccCommitRecord<T> { pub txid: u64, pub writes: Vec<T> }
pub struct IndexGuards {}
pub struct PartitionGuards {}

pub struct Events {
    pub index_locked: bool,
    pub partition_locked: bool,
    pub index_validated: bool,
    pub row_lock_validated: bool,
    pub serial_validated: bool,
    pub base_validated: bool,
    pub write_ahead: bool,
    pub durably_bound: bool,
    pub has_writes: bool,
    pub unlogged_write_checked: bool,
    pub health_checked: bool,
    pub health_cleanup_required: bool,
    pub record_prepared: bool,
    pub payload_prepared: bool,
    pub preparation_cleanup_required: bool,
    pub destinations_prepared: bool,
    pub callback_complete: bool,
    pub prepared: bool,
    pub rows_published: bool,
    pub deregistered: bool,
    pub stamped: bool,
    pub poisoned: bool,
}
pub open spec fn initial(s: Events) -> bool {
    !s.index_locked && !s.partition_locked && !s.index_validated
    && !s.row_lock_validated && !s.serial_validated && !s.base_validated
    && !s.unlogged_write_checked && !s.health_checked && !s.health_cleanup_required
    && !s.record_prepared && !s.payload_prepared && !s.preparation_cleanup_required
    && !s.destinations_prepared && !s.callback_complete
    && !s.prepared && !s.rows_published && !s.deregistered && !s.stamped && !s.poisoned
}
pub open spec fn held(s: Events) -> bool { s.index_locked && s.partition_locked }
pub open spec fn validated(s: Events) -> bool {
    s.health_checked && !s.health_cleanup_required
    && s.index_validated && s.row_lock_validated && s.serial_validated && s.base_validated
}
pub open spec fn safe_release(s: Events) -> bool {
    ((!s.destinations_prepared && !s.prepared) || s.poisoned || (s.rows_published && s.deregistered && s.stamped))
    && (!s.preparation_cleanup_required || s.poisoned)
}
pub open spec fn completed(s: Events) -> bool {
    validated(s) && s.prepared && s.rows_published && s.deregistered && s.stamped
    && !s.index_locked && !s.partition_locked && !s.poisoned
    && (s.write_ahead ==> (s.record_prepared && s.payload_prepared && s.callback_complete))
    && (!s.write_ahead && s.has_writes ==> (s.unlogged_write_checked && !s.durably_bound))
}

// A failed WAL callback can poison the native table when rollback of an
// indeterminate append is itself unsuccessful. Poison may only grow. Success
// preserves it; all other phase observations remain unchanged on failure.
pub open spec fn callback_transition(before: Events, after: Events, ok: bool) -> bool {
    after == (Events {
        callback_complete: if ok { true } else { before.callback_complete },
        poisoned: after.poisoned,
        ..before
    })
    && (before.poisoned ==> after.poisoned)
    && (ok ==> after.poisoned == before.poisoned)
}

// A cheap non-vacuity check of this particular primitive contract: success,
// unchanged failure, and a failure that poisons are all admitted for any state.
// This does not construct a native database or prove every primitive inhabited.
pub proof fn callback_contract_has_witnesses(before: Events)
    ensures
        callback_transition(before, (Events { callback_complete: true, ..before }), true),
        callback_transition(before, before, false),
        callback_transition(before, (Events { poisoned: true, ..before }), false),
{}

// The theorem is universally quantified over any implementation of this
// interface. No native implementation of the interface is asserted verified.
pub trait CommitPrimitives<T> {
    spec fn events(&self) -> Events;

    fn ensure_open(&mut self, tx: &OccTransaction<T>) -> (r: Result<(), Error>)
        ensures final(self).events() == old(self).events();
    fn final_write_indices(&mut self, tx: &OccTransaction<T>) -> (r: Vec<usize>)
        ensures final(self).events() == old(self).events(),
            (r.len() > 0) == old(self).events().has_writes;
    fn index_changes(&mut self, tx: &OccTransaction<T>, indices: &Vec<usize>) -> (r: Result<Vec<usize>, Error>)
        ensures final(self).events() == old(self).events();
    fn index_lock_keys(&mut self, tx: &OccTransaction<T>, changes: &Vec<usize>) -> (r: Result<Vec<usize>, Error>)
        ensures final(self).events() == old(self).events();

    fn acquire_index_locks(&mut self, keys: &Vec<usize>) -> (r: Result<IndexGuards, Error>)
        requires !old(self).events().index_locked,
        ensures final(self).events() == (Events { index_locked: r.is_ok(), ..old(self).events() });
    fn acquire_partition_locks(&mut self, tx: &OccTransaction<T>) -> (r: Result<PartitionGuards, Error>)
        requires old(self).events().index_locked, !old(self).events().partition_locked,
        ensures final(self).events() == (Events { partition_locked: r.is_ok(), ..old(self).events() });
    fn ensure_unlogged_write_allowed(&mut self) -> (r: Result<(), Error>)
        requires held(old(self).events()),
        ensures final(self).events() == (Events { unlogged_write_checked: true, ..old(self).events() }),
            r.is_ok() ==> !old(self).events().durably_bound;
    // This records one guarded health observation, not perpetual exclusion of
    // failures in other already-running writers. Native global poison/atomics
    // and the synchronous file-lock admission check remain unproved primitives.
    fn check_not_poisoned(&mut self) -> (r: Result<(), Error>)
        requires held(old(self).events()), !old(self).events().destinations_prepared,
        ensures final(self).events() == (Events {
            health_checked: r.is_ok(), health_cleanup_required: r.is_err(),
            poisoned: final(self).events().poisoned, ..old(self).events()
        }), old(self).events().poisoned ==> final(self).events().poisoned,
            r.is_ok() ==> !final(self).events().poisoned;
    fn abort_for_serialization_failure(&mut self, tx: &mut OccTransaction<T>)
        requires !old(self).events().prepared,
        ensures final(self).events() == old(self).events();

    fn index_read_conflict(&mut self, tx: &OccTransaction<T>) -> (r: Result<bool, Error>)
        requires held(old(self).events()),
        ensures final(self).events() == (Events { index_validated: r == Ok(false), ..old(self).events() });
    fn has_row_lock_conflict(&mut self, tx: &OccTransaction<T>) -> (r: Result<bool, Error>)
        requires held(old(self).events()),
        ensures final(self).events() == (Events { row_lock_validated: r == Ok(false), ..old(self).events() });
    fn has_serialization_conflict(&mut self, tx: &OccTransaction<T>) -> (r: Result<bool, Error>)
        requires held(old(self).events()),
        ensures final(self).events() == (Events { serial_validated: r == Ok(false), ..old(self).events() });
    fn has_write_base_conflict(&mut self, tx: &OccTransaction<T>, indices: &Vec<usize>) -> (r: Result<bool, Error>)
        requires held(old(self).events()),
        ensures final(self).events() == (Events { base_validated: r == Ok(false), ..old(self).events() });

    fn prepare_commit_record(&mut self, tx: &OccTransaction<T>, indices: &Vec<usize>) -> (r: Result<OccCommitRecord<T>, Error>)
        requires initial(old(self).events()),
        ensures final(self).events() == (Events {
            record_prepared: r.is_ok(), preparation_cleanup_required: r.is_err(), ..old(self).events()
        });
    // The pre-lock codec is a primitive boundary. It may construct a payload
    // and returned closure, but must not accept WAL or mutate table phases.
    fn run_prepare<P, F>(&mut self, record: &OccCommitRecord<T>, prepare: P) -> (r: Result<Result<F, Error>, usize>)
        requires !old(self).events().index_locked, !old(self).events().partition_locked,
            old(self).events().record_prepared, !old(self).events().preparation_cleanup_required,
        ensures final(self).events() == (Events {
            payload_prepared: match r { Ok(Ok(_)) => true, _ => false },
            preparation_cleanup_required: match r { Ok(Ok(_)) => false, _ => true },
            ..old(self).events()
        });
    fn prepare_before_publish<P, F>(&mut self, tx: &mut OccTransaction<T>, final_write_indices: &Vec<usize>, prepare: P) -> (r: Result<(OccCommitRecord<T>, F), Error>)
        requires initial(old(self).events()),
        ensures safe_release(final(self).events()),
            final(self).events() == (Events {
                record_prepared: final(self).events().record_prepared,
                payload_prepared: final(self).events().payload_prepared,
                preparation_cleanup_required: final(self).events().preparation_cleanup_required,
                poisoned: final(self).events().poisoned, ..old(self).events()
            }),
            r.is_ok() ==> final(self).events() == (Events {
                record_prepared: true, payload_prepared: true, ..old(self).events()
            }),
    { let record = match self . prepare_commit_record ( tx , final_write_indices ) {
    Ok ( record ) => record , Err ( err ) => {
        self . abort_preparation ( tx ) ? ;
        return Err ( err . into ( ) ) ;
    }
}
;
match self . run_prepare ( & record , prepare ) {
    Ok ( Ok ( before_publish ) ) => Ok ( ( record , before_publish ) ) , Ok ( Err ( err ) ) => {
        self . abort_preparation ( tx ) ? ;
        Err ( err )
    }
    Err ( panic ) => {
        let _ = self . abort_preparation ( tx ) ;
        self . resume_unwind ( panic )
    }
} }
    fn abort_preparation(&mut self, tx: &mut OccTransaction<T>) -> (r: Result<(), Error>)
        requires !old(self).events().index_locked, !old(self).events().partition_locked,
            !old(self).events().destinations_prepared, !old(self).events().prepared,
        ensures safe_release(final(self).events()),
            final(self).events() == (Events {
                preparation_cleanup_required: final(self).events().preparation_cleanup_required,
                health_cleanup_required: false,
                poisoned: final(self).events().poisoned, ..old(self).events()
            }),
    { let abort = self . abort ( tx ) ;
if abort . is_err ( ) {
    self . poison_indexes ( ) ;
}
abort }
    // Err after destination allocation must have undone all additions or
    // poisoned the table; this is an open native failure-atomicity obligation.
    fn prepare_index_destinations(&mut self, changes: &Vec<usize>) -> (r: Result<Vec<usize>, Error>)
        requires held(old(self).events()), validated(old(self).events()),
            !old(self).events().write_ahead && old(self).events().has_writes
                ==> (old(self).events().unlogged_write_checked && !old(self).events().durably_bound),
        ensures
            r.is_ok() ==> final(self).events() == (Events { destinations_prepared: true, ..old(self).events() }),
            r.is_err() ==> (final(self).events() == old(self).events()
                || final(self).events() == (Events { poisoned: true, ..old(self).events() }));
    // Application callback invocation/catch-unwind is a primitive boundary.
    // The following two method bodies are extracted from the native code.
    fn run_before_publish<F>(&mut self, record: &OccCommitRecord<T>, before_publish: F) -> (r: Result<Result<(), Error>, usize>)
        requires held(old(self).events()), validated(old(self).events()), old(self).events().record_prepared,
            old(self).events().payload_prepared, !old(self).events().preparation_cleanup_required,
            old(self).events().destinations_prepared, !old(self).events().prepared,
        ensures callback_transition(old(self).events(), final(self).events(),
                    match r { Ok(Ok(())) => true, _ => false });
    fn resume_unwind(&mut self, panic: usize) -> !
        requires safe_release(old(self).events());
    fn rollback_index_destinations(&mut self, changes: &Vec<usize>, inserted: &Vec<usize>) -> (r: Result<(), Error>)
        requires held(old(self).events()), !old(self).events().prepared,
        ensures
            r.is_ok() ==> final(self).events() == (Events { destinations_prepared: false, ..old(self).events() }),
            r.is_err() ==> final(self).events() == (Events { poisoned: true, ..old(self).events() });
    fn abort(&mut self, tx: &mut OccTransaction<T>) -> (r: Result<(), Error>)
        requires !old(self).events().prepared,
        ensures final(self).events() == (Events {
            preparation_cleanup_required: !r.is_ok() && old(self).events().preparation_cleanup_required,
            health_cleanup_required: false,
            ..old(self).events()
        });

    fn rollback_prepared_commit(&mut self, tx: &mut OccTransaction<T>, changes: &Vec<usize>, inserted: &Vec<usize>) -> (r: Result<(), Error>)
        requires held(old(self).events()), !old(self).events().prepared,
            !old(self).events().preparation_cleanup_required, !old(self).events().health_cleanup_required,
        ensures safe_release(final(self).events()),
            r.is_ok() ==> final(self).events() == (Events { destinations_prepared: false, ..old(self).events() }),
            r.is_err() ==> final(self).events().poisoned,
            final(self).events() == (Events { destinations_prepared: final(self).events().destinations_prepared,
                poisoned: final(self).events().poisoned, ..old(self).events() }),
    { let rollback = self . rollback_index_destinations ( changes , inserted ) ;
let abort = self . abort ( tx ) ;
if abort . is_err ( ) {
    self . poison_indexes ( ) ;
}
rollback ? ;
abort }

    fn invoke_before_publish<F>(&mut self, tx: &mut OccTransaction<T>, changes: &Vec<usize>, inserted: &Vec<usize>, record: &OccCommitRecord<T>, before_publish: F) -> (r: Result<(), Error>)
        requires held(old(self).events()), validated(old(self).events()), old(self).events().record_prepared,
            old(self).events().payload_prepared, !old(self).events().preparation_cleanup_required,
            old(self).events().destinations_prepared, !old(self).events().prepared,
        ensures
            !final(self).events().health_cleanup_required,
            r.is_ok() ==> final(self).events() == (Events { callback_complete: true, ..old(self).events() }),
            r.is_err() ==> (final(self).events() == (Events { destinations_prepared: false, ..old(self).events() })
                || (final(self).events().poisoned && safe_release(final(self).events()))),
    { match self . run_before_publish ( record , before_publish ) {
    Ok ( Ok ( ( ) ) ) => Ok ( ( ) ) , Ok ( Err ( err ) ) => {
        self . rollback_prepared_commit ( tx , changes , inserted ) ? ;
        Err ( err )
    }
    Err ( panic ) => {
        let _ = self . rollback_prepared_commit ( tx , changes , inserted ) ;
        self . resume_unwind ( panic )
    }
} }
    fn remove_index_sources(&mut self, changes: &Vec<usize>) -> (r: Result<(), Error>)
        requires held(old(self).events()), validated(old(self).events()), old(self).events().destinations_prepared,
            old(self).events().write_ahead ==> old(self).events().callback_complete,
        ensures
            r.is_ok() ==> final(self).events() == (Events { prepared: true, ..old(self).events() }),
            r.is_err() ==> final(self).events() == (Events { poisoned: true, ..old(self).events() });
    fn publish_prepared_write_set(&mut self, record: &OccCommitRecord<T>) -> (r: Result<(), Error>)
        requires held(old(self).events()), old(self).events().prepared, validated(old(self).events()),
            old(self).events().record_prepared, old(self).events().callback_complete,
        ensures final(self).events() == (Events { rows_published: true, ..old(self).events() });
    fn publish_write_set(&mut self, tx: &OccTransaction<T>, indices: &Vec<usize>) -> (r: Result<Vec<T>, Error>)
        requires held(old(self).events()), old(self).events().prepared, validated(old(self).events()),
        ensures final(self).events() == (Events { rows_published: true, ..old(self).events() });
    fn poison_indexes(&mut self)
        ensures final(self).events() == (Events { poisoned: true, ..old(self).events() });
    fn recycle_non_final_writes(&mut self, tx: &mut OccTransaction<T>, indices: &Vec<usize>) -> (r: Result<(), Error>)
        requires held(old(self).events()), old(self).events().rows_published,
        ensures final(self).events() == old(self).events();
    fn flush_local_recycle_caches(&mut self) -> (r: Result<(), Error>)
        ensures final(self).events() == old(self).events();
    fn finish_transaction(&mut self, tx: &mut OccTransaction<T>) -> (r: Result<(), Error>)
        ensures final(self).events() == (Events { deregistered: r.is_ok() || old(self).events().deregistered, ..old(self).events() });
    fn publish_index_stamps(&mut self, changes: &Vec<usize>) -> (r: Result<(), Error>)
        requires held(old(self).events()), old(self).events().rows_published, old(self).events().deregistered,
        ensures final(self).events() == (Events { stamped: r.is_ok(), ..old(self).events() });

    // These two calls are the adapter's interpretation of explicit drop(guards).
    // Implicit cleanup on return is outside Verus's Drop model. safe_release
    // must already hold at every return, independently of that cleanup.
    fn release_partition_locks(&mut self, guards: PartitionGuards)
        requires old(self).events().partition_locked, safe_release(old(self).events()),
        ensures final(self).events() == (Events { partition_locked: false, ..old(self).events() });
    fn release_index_locks(&mut self, guards: IndexGuards)
        requires old(self).events().index_locked, safe_release(old(self).events()),
        ensures final(self).events() == (Events { index_locked: false, ..old(self).events() });
}
}

verus! {
pub fn commit_with_record_impl<T, D: CommitPrimitives<T>, const WRITE_AHEAD: bool, P, F>(driver: &mut D, tx: &mut OccTransaction<T>, prepare: P)
 -> (result: Result<OccCommitRecord<T>, Error>)
 requires initial(old(driver).events()), old(driver).events().write_ahead == WRITE_AHEAD,
 ensures safe_release(final(driver).events()),
   !final(driver).events().health_cleanup_required,
   result.is_ok() ==> completed(final(driver).events()),
   result.is_err() && final(driver).events().rows_published ==> final(driver).events().poisoned,
{
driver . ensure_open ( tx ) ? ;
let final_write_indices = driver . final_write_indices ( tx ) ;
let index_changes = driver . index_changes ( tx , & final_write_indices ) ? ;
let index_keys = driver . index_lock_keys ( tx , & index_changes ) ? ;
let prepared = if WRITE_AHEAD {
    Some ( driver . prepare_before_publish :: < P , F > ( tx , & final_write_indices , prepare ) ? )
}
else {
    None
}
;
let index_locks = match driver . acquire_index_locks ( & index_keys ) {
    Ok ( locks ) => locks , Err ( Error :: SerializationFailure ) => {
        driver . abort_for_serialization_failure ( tx ) ;
        return Err ( Error :: SerializationFailure . into ( ) ) ;
    }
    Err ( err ) => return Err ( err . into ( ) ) ,
}
;
let locks = match driver . acquire_partition_locks ( tx ) {
    Ok ( locks ) => locks , Err ( Error :: SerializationFailure ) => {
        driver . release_index_locks ( index_locks ) ;
        driver . abort_for_serialization_failure ( tx ) ;
        return Err ( Error :: SerializationFailure . into ( ) ) ;
    }
    Err ( err ) => return Err ( err . into ( ) ) ,
}
;
if ! WRITE_AHEAD && ! final_write_indices . is_empty ( ) {
    driver . ensure_unlogged_write_allowed ( ) ? ;
}
if let Err ( err ) = driver . check_not_poisoned ( ) {
    driver . release_partition_locks ( locks ) ;
    driver . release_index_locks ( index_locks ) ;
    return Err ( err . into ( ) ) ;
}
if driver . index_read_conflict ( tx ) ? || driver . has_row_lock_conflict ( tx ) ? || driver . has_serialization_conflict ( tx ) ? || driver . has_write_base_conflict ( tx , & final_write_indices ) ? {
    driver . release_partition_locks ( locks ) ;
    driver . release_index_locks ( index_locks ) ;
    driver . abort_for_serialization_failure ( tx ) ;
    return Err ( Error :: SerializationFailure . into ( ) ) ;
}
let inserted = driver . prepare_index_destinations ( & index_changes ) ? ;
let prepared = match prepared {
    Some ( ( record , before_publish ) ) => {
        driver . invoke_before_publish ( tx , & index_changes , & inserted , & record , before_publish ) ? ;
        Some ( record )
    }
    None => None ,
}
;
driver . remove_index_sources ( & index_changes ) ? ;
let publication = match prepared {
    Some ( record ) => driver . publish_prepared_write_set ( & record ) . map ( | ( ) | record . writes ) , None => driver . publish_write_set ( tx , & final_write_indices ) ,
}
;
let writes = match publication {
    Ok ( writes ) => writes , Err ( err ) => {
        driver . poison_indexes ( ) ;
        tx . write_set . clear ( ) ;
        tx . read_set . clear ( ) ;
        tx . index_reads . clear ( ) ;
        let _ = driver . finish_transaction ( tx ) ;
        return Err ( Error :: Index . into ( ) ) ;
    }
}
;
let commit_record = OccCommitRecord {
    txid : tx . txid , writes ,
}
;
if let Err ( err ) = driver . recycle_non_final_writes ( tx , & final_write_indices ) {
    driver . poison_indexes ( ) ;
    tx . write_set . clear ( ) ;
    let _ = driver . finish_transaction ( tx ) ;
    return Err ( err . into ( ) ) ;
}
tx . read_set . clear ( ) ;
tx . index_reads . clear ( ) ;
tx . index_conflict = false ;
tx . write_set . clear ( ) ;
tx . savepoints . clear ( ) ;
let _ = driver . flush_local_recycle_caches ( ) ;
let finish = match driver . finish_transaction ( tx ) {
    Ok ( ( ) ) => driver . publish_index_stamps ( & index_changes ) , Err ( err ) => Err ( err ) ,
}
;
if let Err ( err ) = finish {
    driver . poison_indexes ( ) ;
    return Err ( err . into ( ) ) ;
}
driver . release_partition_locks ( locks ) ;
driver . release_index_locks ( index_locks ) ;
Ok ( commit_record )
}
}
