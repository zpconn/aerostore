// Checked joins between source-bound native operations. This is a proof-only
// harness, not a second engine. Native memory/atomic/RAII correspondence remains
// a declared boundary; no Lean theorem is imported into Verus.
mod predicate { /* PREDICATE_MODULE */ }
mod guards { /* GUARDS_MODULE */ }
mod lifecycle { /* LIFECYCLE_MODULE */ }
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub struct Locked { pub keys: Vec<(usize, usize)>, pub guards: Vec<guards::Guard> }
#[derive(PartialEq, Eq, Debug)]
pub enum Error { Keys, Lock }

// The binding ordinal is local to the immutable sorted registry. It must map
// back to the very same shared index header, within one physical arena.
pub open spec fn registry_agrees(s: predicate::State, registry: Seq<usize>) -> bool {
    registry.len() == s.binding_count
    && (forall|i: int| 0 <= i < registry.len() ==>
        s.bindings.contains_key(registry[i]) && s.bindings[registry[i]] == i)
    && (forall|offset: usize| s.bindings.contains_key(offset) ==>
        s.bindings[offset] < registry.len() && registry[s.bindings[offset] as int] == offset)
}

pub proof fn registry_agreement_is_injective(s: predicate::State, registry: Seq<usize>)
    requires registry_agrees(s, registry),
    ensures forall|i: int, j: int| 0 <= i < j < registry.len() ==> registry[i] != registry[j],
{
    assert forall|i: int, j: int| 0 <= i < j < registry.len() implies registry[i] != registry[j] by {
        assert(s.bindings[registry[i]] == i);
        assert(s.bindings[registry[j]] == j);
    }
}

pub open spec fn needed(s: predicate::State, tx: predicate::OccTransaction, changes: Seq<predicate::IndexChange>)
    -> Set<predicate::Pair>
{
    predicate::read_keys(s, tx.index_reads@, tx.index_reads.len() as int)
        .union(predicate::change_keys(s, changes, changes.len() as int))
}

pub proof fn read_key_binding_valid(s: predicate::State, reads: Seq<predicate::IndexRead>, n: int, key: predicate::Pair)
    requires 0 <= n <= reads.len(), predicate::all_reads_bound(s, reads, n),
        predicate::read_keys(s, reads, n).contains(key),
    ensures key.0 < s.binding_count,
    decreases n,
{
    if n > 0 && predicate::read_keys(s, reads, n - 1).contains(key) {
        read_key_binding_valid(s, reads, n - 1, key);
    }
}

pub proof fn native_keys_are_valid(s: predicate::State, tx: predicate::OccTransaction,
    changes: Seq<predicate::IndexChange>, registry: Seq<usize>, keys: Seq<guards::Pair>)
    requires predicate::changes_valid(s, changes),
        predicate::all_reads_bound(s, tx.index_reads@, tx.index_reads.len() as int),
        predicate::canonical(keys, needed(s, tx, changes)), registry.len() == s.binding_count,
    ensures guards::keys_valid(registry, keys),
{
    assert forall|i: int| 0 <= i < keys.len() implies keys[i].0 < registry.len() by {
        assert(keys.to_set().contains(keys[i]));
        if predicate::read_keys(s, tx.index_reads@, tx.index_reads.len() as int).contains(keys[i]) {
            read_key_binding_valid(s, tx.index_reads@, tx.index_reads.len() as int, keys[i]);
        } else {
            predicate::changed_binding_in_range(s, changes, changes.len() as int, keys[i]);
        }
    }
}

pub open spec fn covers(registry: Seq<usize>, guards: Seq<guards::Guard>, keys: Set<guards::Pair>) -> bool {
    forall|key: guards::Pair| keys.contains(key) ==>
        guards::guard_members(guards).contains(guards::physical(registry, key))
}

// This supplies the guard-set premise from actual lock-key generation followed
// by actual acquisition, rather than asking the caller to invent a held set.
// Registry/arena identity and the primitive meaning of owning each returned
// native mutex guard remain explicit assumptions.
pub fn acquire_required_guards<P: predicate::Primitives, C: predicate::PairSet, G: guards::Primitives>(
    driver: &P, locks: &G, tx: &predicate::OccTransaction, changes: &Vec<predicate::IndexChange>)
    -> (result: Result<Locked, Error>)
    requires predicate::changes_valid(driver.state(), changes@),
        registry_agrees(driver.state(), locks.registry()), locks.arena() == driver.state().arena,
    ensures result.is_ok() ==> predicate::canonical(result.unwrap().keys@, needed(driver.state(), *tx, changes@)),
        result.is_ok() ==> predicate::all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
        result.is_ok() ==> guards::guards_match(locks.registry(), result.unwrap().keys@, result.unwrap().guards@),
        result.is_ok() ==> guards::all_guards_in(result.unwrap().guards@, driver.state().arena),
        result.is_ok() ==> covers(locks.registry(), result.unwrap().guards@, needed(driver.state(), *tx, changes@)),
{
    let keys = match predicate::index_lock_keys::<P, C>(driver, tx, changes) {
        Ok(keys) => keys,
        Err(_) => return Err(Error::Keys),
    };
    proof { native_keys_are_valid(driver.state(), *tx, changes@, locks.registry(), keys@); }
    let acquired = match guards::acquire_index_locks(locks, &keys) {
        Ok(acquired) => acquired,
        Err(_) => return Err(Error::Lock),
    };
    proof {
        guards::acquired_guards_cover_keys(locks.registry(), keys@, acquired@);
        assert forall|key: guards::Pair| needed(driver.state(), *tx, changes@).contains(key)
            implies guards::guard_members(acquired@).contains(guards::physical(locks.registry(), key)) by {
            assert(keys@.contains(key));
            let i = choose|i: int| 0 <= i < keys.len() && keys[i] == key;
            let physical_keys = keys@.map(|j: int, k: guards::Pair| guards::physical(locks.registry(), k));
            assert(physical_keys[i] == guards::physical(locks.registry(), key));
            assert(physical_keys.to_set().contains(physical_keys[i]));
        }
    }
    Ok(Locked { keys, guards: acquired })
}

// Native guarded Acquire observations are represented only for the buckets
// actually owned by these guards. The relation is the still-unproved
// mutex/RAII/atomic primitive correspondence, not a supplied complete held set.
pub open spec fn guarded_observation(s: predicate::State, registry: Seq<usize>, owned: Seq<guards::Guard>) -> bool {
    registry_agrees(s, registry) && guards::all_guards_in(owned, s.arena)
    && forall|key: guards::Pair| key.0 < registry.len()
        && guards::guard_members(owned).contains(guards::physical(registry, key)) ==>
            s.held.contains(key)
}

pub proof fn guard_coverage_supplies_validation_permission(s: predicate::State, registry: Seq<usize>,
    owned: Seq<guards::Guard>, reads: Seq<predicate::IndexRead>)
    requires guarded_observation(s, registry, owned),
        predicate::all_reads_bound(s, reads, reads.len() as int),
        covers(registry, owned, predicate::read_keys(s, reads, reads.len() as int)),
    ensures predicate::read_keys(s, reads, reads.len() as int).subset_of(s.held),
{
    assert forall|key: guards::Pair| predicate::read_keys(s, reads, reads.len() as int).contains(key)
        implies s.held.contains(key) by {
        read_key_binding_valid(s, reads, reads.len() as int, key);
        assert(guards::guard_members(owned).contains(guards::physical(registry, key)));
    }
}

// This join consumes the exact relations exported by the native lifecycle
// reservation and publication algorithms. A reader's reservation precedes
// this publication in the shared atomic history; no numeric freshness premise
// is given. It covers an old writer as well as a new one.
pub proof fn lifecycle_publication_invalidates_read(clock_before: lifecycle::State, clock_after: lifecycle::State,
    before: predicate::State, after: predicate::State, reads: Seq<predicate::IndexRead>,
    reader: u64, touched: Set<predicate::Pair>, stamp: u64, read_index: int)
    requires lifecycle::well_formed(clock_before), lifecycle::reservation(clock_before, clock_after, stamp),
        clock_before.reservations.contains(reader),
        before.bindings == after.bindings, before.arena == after.arena, 0 <= read_index < reads.len(),
        touched.contains(predicate::read_pair(before, reads[read_index])),
        predicate::stamp_relation(before.stamps, after.stamps, touched, stamp),
    ensures !predicate::reads_good(after, reads, reader, reads.len() as int),
{
    lifecycle::reservation_after_reader(clock_before, clock_after, stamp, reader);
    predicate::publication_invalidates_dependency(before, after, reads, reader, touched, stamp, read_index);
}

pub fn validate_after_lifecycle_publication<P: predicate::Primitives>(driver: &P,
    tx: &predicate::OccTransaction, owned: &Vec<guards::Guard>,
    Ghost(registry): Ghost<Seq<usize>>, Ghost(clock_before): Ghost<lifecycle::State>,
    Ghost(clock_after): Ghost<lifecycle::State>, Ghost(before): Ghost<predicate::State>,
    Ghost(touched): Ghost<Set<predicate::Pair>>, stamp: u64, read_index: usize)
    -> (result: Result<bool, predicate::Error>)
    requires lifecycle::well_formed(clock_before), lifecycle::reservation(clock_before, clock_after, stamp),
        clock_before.reservations.contains(tx.txid),
        before.bindings == driver.state().bindings, before.arena == driver.state().arena,
        read_index < tx.index_reads.len(),
        touched.contains(predicate::read_pair(before, tx.index_reads@[read_index as int])),
        predicate::stamp_relation(before.stamps, driver.state().stamps, touched, stamp),
        predicate::all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
        covers(registry, owned@, predicate::read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)),
        guarded_observation(driver.state(), registry, owned@),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {
        guard_coverage_supplies_validation_permission(driver.state(), registry, owned@, tx.index_reads@);
        lifecycle_publication_invalidates_read(clock_before, clock_after, before, driver.state(),
            tx.index_reads@, tx.txid, touched, stamp, read_index as int);
    }
    predicate::index_read_conflict(driver, tx)
}
}
