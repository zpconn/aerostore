// Generated composition of exact source-bound proof modules.
// Contract composition over two represented states. This harness is proof-only;
// it does not implement a production transaction or establish a concurrent trace.
mod capture {
    // Generated from native index_lookup; see generate.py for the boundary.
// Conditional proof of the native predicate dependency-capture loop.
// Primitive stamp visibility under held bucket guards remains an assumption.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

#[derive(PartialEq, Eq, Debug)]
pub enum Error { SerializationFailure, Index }
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
pub struct Transaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }

pub open spec fn same_key(a: IndexRead, b: IndexRead) -> bool {
    a.index_offset == b.index_offset && a.bucket == b.bucket
}
pub open spec fn unique(reads: Seq<IndexRead>) -> bool {
    forall|i: int, j: int| 0 <= i < j < reads.len() ==> !same_key(reads[i], reads[j])
}
pub open spec fn captured(reads: Seq<IndexRead>, offset: usize, bucket: usize, stamp: u64) -> bool {
    reads.contains(IndexRead { index_offset: offset, bucket, stamp })
}
pub open spec fn extends(before: Seq<IndexRead>, after: Seq<IndexRead>) -> bool {
    before.len() <= after.len()
        && forall|i: int| 0 <= i < before.len() ==> before[i] == after[i]
}
pub open spec fn permitted_additions(before: Seq<IndexRead>, after: Seq<IndexRead>,
    offset: usize, buckets: Seq<usize>, stamps: Map<usize, u64>) -> bool {
    forall|r: IndexRead| after.contains(r) ==> before.contains(r)
        || (r.index_offset == offset && buckets.contains(r.bucket)
            && stamps.contains_key(r.bucket) && r.stamp == stamps[r.bucket])
}

// Abstracts only the standard iterator's first matching element, not native
// validation. The lowering is executable and itself verified here.
pub fn find_read(reads: &Vec<IndexRead>, offset: usize, bucket: usize)
    -> (r: Option<IndexRead>)
    ensures
        r.is_some() ==> reads@.contains(r.unwrap())
            && r.unwrap().index_offset == offset && r.unwrap().bucket == bucket,
        r.is_none() ==> forall|i: int| 0 <= i < reads.len() ==>
            reads[i].index_offset != offset || reads[i].bucket != bucket,
{
    let mut i = 0;
    while i < reads.len()
        invariant i <= reads.len(),
            forall|j: int| 0 <= j < i ==> reads[j].index_offset != offset || reads[j].bucket != bucket,
        decreases reads.len() - i,
    {
        let read = &reads[i];
        if read.index_offset == offset && read.bucket == bucket { return Some(*read); }
        i += 1;
    }
    None
}

pub trait CaptureIndex {
    spec fn offset(&self) -> usize;
    spec fn stamps(&self) -> Map<usize, u64>;
    spec fn held(&self) -> Set<usize>;
    fn header_offset(&self) -> (r: usize) ensures r == self.offset();
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, Error>)
        requires self.held().contains(bucket), self.stamps().contains_key(bucket),
        ensures r.is_ok() ==> r.unwrap() == self.stamps()[bucket],
            r.is_err() ==> r == Err(Error::Index);
}

pub proof fn appended_read_preserves_unique(reads: Seq<IndexRead>, next: IndexRead)
    requires unique(reads),
        forall|i: int| 0 <= i < reads.len() ==> !same_key(reads[i], next),
    ensures unique(reads.push(next)),
{
    assert forall|i: int, j: int| 0 <= i < j < reads.push(next).len()
        implies !same_key(reads.push(next)[i], reads.push(next)[j]) by {
        if j < reads.len() { assert(!same_key(reads[i], reads[j])); }
    }
}

pub fn capture_dependencies<I: CaptureIndex>(index: &I, tx: &mut Transaction, buckets: &Vec<usize>)
    -> (result: Result<(), Error>)
    requires unique(old(tx).index_reads@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i]),
    ensures
        final(tx).txid == old(tx).txid,
        old(tx).index_conflict ==> final(tx).index_conflict,
        unique(final(tx).index_reads@), extends(old(tx).index_reads@, final(tx).index_reads@),
        permitted_additions(old(tx).index_reads@, final(tx).index_reads@, index.offset(), buckets@, index.stamps()),
        final(tx).index_reads.len() <= old(tx).index_reads.len() + buckets.len(),
        result == Err(Error::SerializationFailure) ==> final(tx).index_conflict,
        result.is_ok() ==> forall|i: int| 0 <= i < buckets.len() ==>
            captured(final(tx).index_reads@, index.offset(), buckets[i], index.stamps()[buckets[i]])
                && index.stamps()[buckets[i]] < final(tx).txid,
{
    let ghost initial_reads = tx.index_reads@;
    let mut bucket_pos = 0;
    while bucket_pos < buckets.len()
        invariant
            bucket_pos <= buckets.len(), unique(tx.index_reads@),
            initial_reads == old(tx).index_reads@, extends(initial_reads, tx.index_reads@),
            permitted_additions(initial_reads, tx.index_reads@, index.offset(), buckets@, index.stamps()),
            tx.index_reads.len() <= initial_reads.len() + bucket_pos,
            tx.txid == old(tx).txid,
            old(tx).index_conflict ==> tx.index_conflict,
            forall|j: int| 0 <= j < buckets.len() ==>
                index.held().contains(buckets[j]) && index.stamps().contains_key(buckets[j]),
            forall|j: int| 0 <= j < bucket_pos ==>
                captured(tx.index_reads@, index.offset(), buckets[j], index.stamps()[buckets[j]])
                    && index.stamps()[buckets[j]] < tx.txid,
        decreases buckets.len() - bucket_pos,
    {
        let bucket = &buckets[bucket_pos];
let stamp = index . transactional_stamp ( * bucket ) ? ;
if ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    tx . index_conflict = true ;
    return Err ( Error :: SerializationFailure ) ;
}
if let Some ( previous ) = find_read ( & tx . index_reads , index . header_offset ( ) , * bucket ) {
    if previous . stamp != stamp {
        tx . index_conflict = true ;
        return Err ( Error :: SerializationFailure ) ;
    }
}
else {
    proof {
        appended_read_preserves_unique(tx.index_reads@, IndexRead {
            index_offset: index.offset(), bucket: *bucket, stamp });
    }
    tx . index_reads . push ( IndexRead {
        index_offset : index . header_offset ( ) , bucket : * bucket , stamp ,
    }
    ) ;
}
        bucket_pos += 1;
    }
    Ok(())
}
}
verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (r: bool)
ensures r == (stamp < transaction_id),
{ stamp < transaction_id } }

}
mod predicate {
    // Generated from native indexed transaction operations; see generate.py.
// The traits below are explicitly unproved primitive boundaries. The three
// native algorithms are inserted from production source by generate.py.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub type Pair = (usize, usize);
#[derive(PartialEq, Eq, Debug)]
pub enum Error { IndexBindingsIncomplete, Other }
#[derive(Clone, Copy)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
#[derive(Clone, Copy)]
pub struct IndexChange { pub binding: usize, pub before: Option<usize>, pub after: Option<usize> }
pub struct OccTransaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }
pub struct State {
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<Pair, usize>,
    // The operation's guarded stamp projection, not a snapshot claiming that
    // every bucket in the concurrently changing database is frozen.
    pub stamps: Map<Pair, u64>,
    pub held: Set<Pair>,
    // Logical state at the reservation's linearization point. The +1 result
    // does not assert a physical global-counter delta across concurrent calls.
    pub clock: u64,
    pub deregistered: bool,
}
// An arbitrary fixed key-to-bucket function; no hash injectivity is assumed.
pub open spec fn key_bucket(s: State, binding: usize, key: usize) -> usize { s.key_buckets[(binding, key)] }
pub open spec fn pair_less(a: Pair, b: Pair) -> bool {
    a.0 < b.0 || (a.0 == b.0 && a.1 < b.1)
}
pub open spec fn canonical(v: Seq<Pair>, members: Set<Pair>) -> bool {
    v.to_set() == members
    && forall|i: int, j: int| 0 <= i < j < v.len() ==> pair_less(v[i], v[j])
}
pub open spec fn read_pair(s: State, r: IndexRead) -> Pair { (s.bindings[r.index_offset], r.bucket) }
pub open spec fn read_keys(s: State, reads: Seq<IndexRead>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { read_keys(s, reads, n - 1).insert(read_pair(s, reads[n - 1])) }
}
pub open spec fn option_keys(s: State, binding: usize, key: Option<usize>) -> Set<Pair> {
    match key { Some(k) => Set::empty().insert((binding, key_bucket(s, binding, k))), None => Set::empty() }
}
pub open spec fn change_keys_one(s: State, c: IndexChange) -> Set<Pair> {
    option_keys(s, c.binding, c.before).union(option_keys(s, c.binding, c.after))
}
pub open spec fn change_keys(s: State, changes: Seq<IndexChange>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { change_keys(s, changes, n - 1).union(change_keys_one(s, changes[n - 1])) }
}
pub open spec fn all_reads_bound(s: State, reads: Seq<IndexRead>, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> s.bindings.contains_key(reads[i].index_offset)
        && s.bindings[reads[i].index_offset] < s.binding_count
}
pub open spec fn changes_valid(s: State, changes: Seq<IndexChange>) -> bool {
    forall|i: int| 0 <= i < changes.len() ==> changes[i].binding < s.binding_count
}
pub open spec fn read_good(s: State, r: IndexRead, txid: u64) -> bool {
    s.bindings.contains_key(r.index_offset)
    && s.stamps.contains_key(read_pair(s, r))
    && s.stamps[read_pair(s, r)] == r.stamp
    && s.stamps[read_pair(s, r)] < txid
}
pub open spec fn reads_good(s: State, reads: Seq<IndexRead>, txid: u64, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> read_good(s, reads[i], txid)
}
pub open spec fn stamp_relation(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, stamp: u64) -> bool {
    after.dom() == before.dom().union(done)
    && forall|p: Pair| after.contains_key(p) ==> after[p] == if done.contains(p) { stamp } else { before[p] }
}

// BTreeSet's ordinary extensional and canonical-iteration contracts. Its
// implementation and Rust allocation/panic behavior are not proved here.
pub trait PairSet {
    spec fn contents(&self) -> Set<Pair>;
    fn new() -> (out: Self) where Self: Sized
        ensures out.contents() == Set::<Pair>::empty();
    fn insert(&mut self, pair: Pair)
        ensures final(self).contents() == old(self).contents().insert(pair);
    fn into_vec(self) -> (out: Vec<Pair>) where Self: Sized
        ensures canonical(out@, self.contents());
}

// Immutable registry lookup, key encoding/hash, guarded Acquire loads, and
// Release stores remain assumptions. A mutable proof receiver models one
// operation's state; it does not assert exclusive access to the real database.
pub trait Primitives {
    spec fn state(&self) -> State;
    fn find_binding(&self, offset: usize) -> (r: Result<usize, Error>)
        ensures r.is_ok() ==> self.state().bindings.contains_key(offset)
            && r.unwrap() == self.state().bindings[offset]
            && r.unwrap() < self.state().binding_count;
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, Error>)
        requires binding < self.state().binding_count,
        ensures r.is_ok() ==> r.unwrap() == key_bucket(self.state(), binding, *key);
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, Error>)
        requires self.state().held.contains((binding, bucket)), binding < self.state().binding_count,
        ensures r.is_ok() ==> self.state().stamps.contains_key((binding, bucket))
            && r.unwrap() == self.state().stamps[(binding, bucket)];
    fn reserve_stamp(&mut self) -> (stamp: u64)
        requires old(self).state().deregistered, old(self).state().clock < u64::MAX,
        ensures stamp == old(self).state().clock,
            final(self).state() == (State { clock: (old(self).state().clock + 1) as u64, ..old(self).state() });
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64) -> (r: Result<(), Error>)
        requires old(self).state().held.contains((binding, bucket)), old(self).state().deregistered,
            binding < old(self).state().binding_count,
        ensures final(self).state() == (State {
            stamps: if r.is_ok() { old(self).state().stamps.insert((binding, bucket), stamp) } else { old(self).state().stamps },
            ..old(self).state()
        });
}

pub proof fn stamp_unchanged(before: Map<Pair, u64>, stamp: u64)
    ensures stamp_relation(before, before, Set::empty(), stamp),
{
    assert(before.dom() =~= before.dom().union(Set::empty()));
}

pub proof fn stamp_update(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, p: Pair, stamp: u64)
    requires stamp_relation(before, after, done, stamp),
    ensures stamp_relation(before, after.insert(p, stamp), done.insert(p), stamp),
{
    assert(after.insert(p, stamp).dom() =~= before.dom().union(done.insert(p)));
}

pub proof fn read_key_member(s: State, reads: Seq<IndexRead>, i: int, n: int)
    requires 0 <= i < n <= reads.len(),
    ensures read_keys(s, reads, n).contains(read_pair(s, reads[i])),
    decreases n,
{
    if i < n - 1 { read_key_member(s, reads, i, n - 1); }
}

pub proof fn changed_binding_in_range(s: State, changes: Seq<IndexChange>, n: int, pair: Pair)
    requires 0 <= n <= changes.len(), changes_valid(s, changes), change_keys(s, changes, n).contains(pair),
    ensures pair.0 < s.binding_count,
    decreases n,
{
    if n > 0 && change_keys(s, changes, n - 1).contains(pair) {
        changed_binding_in_range(s, changes, n - 1, pair);
    }
}

// Composition uses the stamp actually written by the native publication
// theorem. Establishing stamp >= reader.txid from the shared allocator and
// snapshot/ProcArray execution is a separate temporal refinement obligation.
// In particular, the publishing writer's own (possibly older) txid is absent.
pub proof fn publication_invalidates_dependency(before: State, after: State,
    reads: Seq<IndexRead>, txid: u64, touched: Set<Pair>, stamp: u64, i: int)
    requires 0 <= i < reads.len(), before.bindings == after.bindings,
        touched.contains(read_pair(before, reads[i])), stamp >= txid,
        stamp_relation(before.stamps, after.stamps, touched, stamp),
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(after.stamps.contains_key(read_pair(after, reads[i])));
    assert(after.stamps[read_pair(after, reads[i])] == stamp);
    assert(!read_good(after, reads[i], txid));
}

// A changed captured stamp also invalidates the read regardless of txid.
pub proof fn changed_stamp_invalidates_dependency(after: State,
    reads: Seq<IndexRead>, txid: u64, i: int, stamp: u64)
    requires 0 <= i < reads.len(),
        after.stamps.contains_key(read_pair(after, reads[i])),
        after.stamps[read_pair(after, reads[i])] == stamp, stamp != reads[i].stamp,
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(!read_good(after, reads[i], txid));
}

pub fn index_lock_keys<D: Primitives, C: PairSet>(driver: &D, tx: &OccTransaction, changes: &Vec<IndexChange>)
    -> (result: Result<Vec<Pair>, Error>)
    requires changes_valid(driver.state(), changes@),
    ensures result.is_ok() ==> canonical(result->Ok_0@,
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
            .union(change_keys(driver.state(), changes@, changes.len() as int))),
        result.is_ok() ==> all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
{
    let mut keys = C :: new ( ) ;

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, ri as int),
                all_reads_bound(driver.state(), tx.index_reads@, ri as int),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
     let binding = driver . find_binding ( read . index_offset ) ? ;
keys . insert ( ( binding , read . bucket ) ) ;

            proof { reveal_with_fuel(read_keys, 2); }
            ri = ri + 1;
        }
     
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), changes_valid(driver.state(), changes@),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
                    .union(change_keys(driver.state(), changes@, ci as int)),
                all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     let index = change . binding ;
if let Some ( key ) = & change . before {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}
if let Some ( key ) = & change . after {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     Ok ( keys . into_vec ( ) )
}

pub fn index_read_conflict<D: Primitives>(driver: &D, tx: &OccTransaction)
    -> (result: Result<bool, Error>)
    requires read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap() ==
        (tx.index_conflict || !reads_good(driver.state(), tx.index_reads@, tx.txid, tx.index_reads.len() as int)),
{
    if tx . index_conflict {
    return Ok ( true ) ;
}

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(), !tx.index_conflict,
                reads_good(driver.state(), tx.index_reads@, tx.txid, ri as int),
                read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
            proof { read_key_member(driver.state(), tx.index_reads@, ri as int, tx.index_reads.len() as int); }
     let bound = driver . find_binding ( read . index_offset ) ? ;
let stamp = driver . transactional_stamp ( bound , read . bucket ) ? ;
if stamp != read . stamp || ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    return Ok ( true ) ;
}

            ri = ri + 1;
        }
     Ok ( false )
}

pub fn publish_index_stamps<D: Primitives, C: PairSet>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<(), Error>)
    requires old(driver).state().deregistered, old(driver).state().clock < u64::MAX,
        changes_valid(old(driver).state(), changes@),
        change_keys(old(driver).state(), changes@, changes.len() as int).subset_of(old(driver).state().held),
    ensures final(driver).state().bindings == old(driver).state().bindings,
        final(driver).state().binding_count == old(driver).state().binding_count,
        final(driver).state().key_buckets == old(driver).state().key_buckets,
        final(driver).state().held == old(driver).state().held,
        final(driver).state().deregistered == old(driver).state().deregistered,
        final(driver).state().clock == if changes.len() == 0 { old(driver).state().clock as int } else { old(driver).state().clock + 1 },
        changes.len() == 0 ==> final(driver).state() == old(driver).state(),
        result.is_ok() ==> stamp_relation(old(driver).state().stamps, final(driver).state().stamps,
            change_keys(old(driver).state(), changes@, changes.len() as int), old(driver).state().clock),
        // Failures may have published a prefix; every changed stamp has the
        // reserved value, and unrelated buckets are never modified.
        forall|p: Pair| !change_keys(old(driver).state(), changes@, changes.len() as int).contains(p) ==>
            final(driver).state().stamps.contains_key(p) == old(driver).state().stamps.contains_key(p)
            && (old(driver).state().stamps.contains_key(p) ==> final(driver).state().stamps[p] == old(driver).state().stamps[p]),
        forall|p: Pair| final(driver).state().stamps.contains_key(p)
            && (!old(driver).state().stamps.contains_key(p) || final(driver).state().stamps[p] != old(driver).state().stamps[p]) ==>
                change_keys(old(driver).state(), changes@, changes.len() as int).contains(p)
                && final(driver).state().stamps[p] == old(driver).state().clock,
{
    let ghost initial = driver.state(); proof { stamp_unchanged(initial.stamps, initial.clock); } if changes . is_empty ( ) {
    return Ok ( ( ) ) ;
}
let stamp = driver . reserve_stamp ( ) ;
let mut touched = C :: new ( ) ;

        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state() == (State { clock: (initial.clock + 1) as u64, ..initial }),
                initial.deregistered, initial.clock < u64::MAX, stamp == initial.clock,
                touched.contents() == change_keys(initial, changes@, ci as int),
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                changes.len() > 0,
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     if let Some ( key ) = & change . before {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}
if let Some ( key ) = & change . after {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     
        proof { stamp_unchanged(initial.stamps, stamp); }
        let ordered = touched.into_vec();
        let mut pi: usize = 0;
        proof { assert(ordered@.take(0).to_set() =~= Set::<Pair>::empty()); }
        while pi < ordered.len()
            invariant pi <= ordered.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state().binding_count == initial.binding_count,
                canonical(ordered@, change_keys(initial, changes@, changes.len() as int)),
                driver.state().bindings == initial.bindings, driver.state().held == initial.held,
                driver.state().key_buckets == initial.key_buckets,
                driver.state().deregistered == initial.deregistered,
                driver.state().clock == initial.clock + 1,
                initial.deregistered, initial.clock < u64::MAX, stamp == initial.clock,
                changes.len() > 0,
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                stamp_relation(initial.stamps, driver.state().stamps, ordered@.take(pi as int).to_set(), stamp),
                forall|p: Pair| !change_keys(initial, changes@, changes.len() as int).contains(p) ==>
                    driver.state().stamps.contains_key(p) == initial.stamps.contains_key(p)
                    && (initial.stamps.contains_key(p) ==> driver.state().stamps[p] == initial.stamps[p]),
            decreases ordered.len() - pi,
        {
            let (binding, bucket) = ordered[pi];
            let ghost previous_stamps = driver.state().stamps;
            proof {
                assert(ordered@.contains((binding, bucket)));
                changed_binding_in_range(initial, changes@, changes.len() as int, (binding, bucket));
            }
     driver . transactional_publish_stamp ( binding , bucket , stamp ) ? ;

            proof {
                stamp_update(initial.stamps, previous_stamps, ordered@.take(pi as int).to_set(), (binding, bucket), stamp);
                assert(ordered@.take(pi as int + 1).to_set() =~= ordered@.take(pi as int).to_set().insert((binding, bucket)));
            }
            pi = pi + 1;
        }
     Ok ( ( ) )
}

// A proof harness composing the mathematical dependency lemma with the
// extracted native validator. This harness is never compiled into AeroStore.
pub fn validate_after_late_publication<D: Primitives>(driver: &D, tx: &OccTransaction,
    Ghost(before): Ghost<State>, Ghost(touched): Ghost<Set<Pair>>,
    publication_stamp: u64, read_index: usize) -> (result: Result<bool, Error>)
    requires read_index < tx.index_reads.len(), before.bindings == driver.state().bindings,
        touched.contains(read_pair(before, tx.index_reads@[read_index as int])),
        publication_stamp >= tx.txid,
        stamp_relation(before.stamps, driver.state().stamps, touched, publication_stamp),
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {
        publication_invalidates_dependency(before, driver.state(), tx.index_reads@,
            tx.txid, touched, publication_stamp, read_index as int);
    }
    index_read_conflict(driver, tx)
}
}

verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (result: bool)
ensures result == (stamp < transaction_id),
{
stamp < transaction_id
} }

}
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

#[derive(PartialEq, Eq)]
pub enum ReadResult { CaptureError, ValidationError, Checked(bool) }

pub open spec fn translated(read: capture::IndexRead) -> predicate::IndexRead {
    predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp }
}
pub open spec fn translated_reads(before: Seq<capture::IndexRead>, after: Seq<predicate::IndexRead>) -> bool {
    before.len() == after.len()
    && forall|i: int| 0 <= i < before.len() ==> after[i] == translated(before[i])
}
pub open spec fn reads_held(s: predicate::State, reads: Seq<capture::IndexRead>) -> bool {
    forall|i: int| 0 <= i < reads.len() ==>
        s.bindings.contains_key(reads[i].index_offset)
        && s.held.contains((s.bindings[reads[i].index_offset], reads[i].bucket))
}
pub open spec fn has_late_publication(s: predicate::State, offset: usize, buckets: Seq<usize>, start: u64) -> bool {
    exists|i: int| 0 <= i < buckets.len()
        && s.stamps.contains_key((s.bindings[offset], buckets[i]))
        && s.stamps[(s.bindings[offset], buckets[i])] >= start
}

// The native publication theorem exports stamp_relation. This bridge turns
// that exact data relation into the hypothesis consumed by the capture/check
// composition. The temporal stamp >= start premise is still explicit.
pub proof fn publication_supplies_late_bucket(before: predicate::State, after: predicate::State,
    offset: usize, buckets: Seq<usize>, bucket: usize, touched: Set<predicate::Pair>,
    stamp: u64, start: u64)
    requires before.bindings == after.bindings, before.bindings.contains_key(offset),
        buckets.contains(bucket), touched.contains((before.bindings[offset], bucket)),
        predicate::stamp_relation(before.stamps, after.stamps, touched, stamp), stamp >= start,
    ensures has_late_publication(after, offset, buckets, start),
{
    let i = choose|i: int| 0 <= i < buckets.len() && buckets[i] == bucket;
    assert(after.stamps.contains_key((after.bindings[offset], buckets[i])));
    assert(after.stamps[(after.bindings[offset], buckets[i])] >= start);
}

pub proof fn read_keys_are_held(s: predicate::State, reads: Seq<predicate::IndexRead>, n: int)
    requires 0 <= n <= reads.len(),
        forall|i: int| 0 <= i < n ==> s.held.contains(predicate::read_pair(s, reads[i])),
    ensures predicate::read_keys(s, reads, n).subset_of(s.held),
    decreases n,
{
    if n > 0 { read_keys_are_held(s, reads, n - 1); }
}

pub proof fn capture_preserves_guard_coverage(s: predicate::State, before: Seq<capture::IndexRead>,
    after: Seq<capture::IndexRead>, offset: usize, buckets: Seq<usize>, stamps: Map<usize, u64>)
    requires reads_held(s, before), s.bindings.contains_key(offset),
        capture::permitted_additions(before, after, offset, buckets, stamps),
        forall|i: int| 0 <= i < buckets.len() ==> s.held.contains((s.bindings[offset], buckets[i])),
    ensures reads_held(s, after),
{
    assert forall|j: int| 0 <= j < after.len() implies
        s.bindings.contains_key(after[j].index_offset)
        && s.held.contains((s.bindings[after[j].index_offset], after[j].bucket)) by {
        assert(after.contains(after[j]));
        if before.contains(after[j]) {
            let i = choose|i: int| 0 <= i < before.len() && before[i] == after[j];
            assert(s.held.contains((s.bindings[before[i].index_offset], before[i].bucket)));
        } else {
            assert(buckets.contains(after[j].bucket));
            let i = choose|i: int| 0 <= i < buckets.len() && buckets[i] == after[j].bucket;
            assert(s.held.contains((s.bindings[offset], buckets[i])));
        }
    }
}

pub proof fn captured_read_survives_translation(before: Seq<capture::IndexRead>, after: Seq<predicate::IndexRead>,
    offset: usize, bucket: usize, stamp: u64)
    requires translated_reads(before, after), capture::captured(before, offset, bucket, stamp),
    ensures exists|i: int| 0 <= i < after.len() && after[i].index_offset == offset
        && after[i].bucket == bucket && after[i].stamp == stamp,
{
    let i = choose|i: int| 0 <= i < before.len()
        && before[i] == (capture::IndexRead { index_offset: offset, bucket, stamp });
    assert(after[i] == translated(before[i]));
}

// The current native capture operation runs against `index`; the current native
// validation runs against `later`. The caller must justify that these interfaces
// represent the actual two states, including guard lifetime and registry mapping.
// No assumption states the desired conflict conclusion.
pub fn capture_then_validate<I: capture::CaptureIndex, D: predicate::Primitives>(
    index: &I, later: &D, tx: &mut capture::Transaction, buckets: &Vec<usize>) -> (result: ReadResult)
    requires capture::unique(old(tx).index_reads@),
        later.state().bindings.contains_key(index.offset()),
        reads_held(later.state(), old(tx).index_reads@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i])
            && later.state().held.contains((later.state().bindings[index.offset()], buckets[i])),
    ensures result == ReadResult::Checked(false) ==>
        !has_late_publication(later.state(), index.offset(), buckets@, old(tx).txid),
{
    let ghost initial_reads = tx.index_reads@;
    match capture::capture_dependencies(index, tx, buckets) {
        Err(_) => return ReadResult::CaptureError,
        Ok(()) => {},
    }
    proof {
        capture_preserves_guard_coverage(later.state(), initial_reads, tx.index_reads@,
            index.offset(), buckets@, index.stamps());
    }
    let mut reads = Vec::new();
    let mut i: usize = 0;
    while i < tx.index_reads.len()
        invariant i <= tx.index_reads.len(), reads.len() == i,
            forall|j: int| 0 <= j < i ==> reads[j] == translated(tx.index_reads[j]),
            reads_held(later.state(), initial_reads), reads_held(later.state(), tx.index_reads@),
            capture::extends(initial_reads, tx.index_reads@),
            later.state().bindings.contains_key(index.offset()),
            forall|j: int| 0 <= j < buckets.len() ==>
                later.state().held.contains((later.state().bindings[index.offset()], buckets[j])),
        decreases tx.index_reads.len() - i,
    {
        let read = &tx.index_reads[i];
        reads.push(predicate::IndexRead { index_offset: read.index_offset, bucket: 0, stamp: read.stamp });
        i += 1;
    }
    let validated = predicate::OccTransaction { txid: tx.txid, index_conflict: tx.index_conflict, index_reads: reads };
    proof {
        assert(translated_reads(tx.index_reads@, validated.index_reads@));
        // Provenance from the capture contract restricts additions to the
        // guarded buckets, preserving guard coverage during translation.
        assert(reads_held(later.state(), tx.index_reads@));
        assert forall|j: int| 0 <= j < validated.index_reads.len()
            implies later.state().held.contains(predicate::read_pair(later.state(), validated.index_reads[j])) by {
            assert(validated.index_reads[j] == translated(tx.index_reads[j]));
        }
        read_keys_are_held(later.state(), validated.index_reads@, validated.index_reads.len() as int);
    }
    let conflict = match predicate::index_read_conflict(later, &validated) {
        Err(_) => return ReadResult::ValidationError,
        Ok(conflict) => conflict,
    };
    proof {
        if !conflict && has_late_publication(later.state(), index.offset(), buckets@, tx.txid) {
            let b = choose|j: int| 0 <= j < buckets.len()
                && later.state().stamps.contains_key((later.state().bindings[index.offset()], buckets[j]))
                && later.state().stamps[(later.state().bindings[index.offset()], buckets[j])] >= tx.txid;
            captured_read_survives_translation(tx.index_reads@, validated.index_reads@,
                index.offset(), buckets[b], index.stamps()[buckets[b]]);
            let r = choose|j: int| 0 <= j < validated.index_reads.len()
                && validated.index_reads[j].index_offset == index.offset()
                && validated.index_reads[j].bucket == buckets[b]
                && validated.index_reads[j].stamp == index.stamps()[buckets[b]];
            assert(!predicate::read_good(later.state(), validated.index_reads[r], tx.txid));
            assert(false);
        }
    }
    ReadResult::Checked(conflict)
}
}
