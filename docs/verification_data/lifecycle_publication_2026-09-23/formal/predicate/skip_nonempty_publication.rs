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
    // Physical arena identity, not a process-local virtual base address.
    pub arena: usize,
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<Pair, usize>,
    // The operation's guarded stamp projection, not a snapshot claiming that
    // every bucket in the concurrently changing database is frozen.
    pub stamps: Map<Pair, u64>,
    pub held: Set<Pair>,
    // Last represented clock observation. Interfering reservations may advance
    // the actual allocator before this operation's fetch_add linearizes.
    pub clock: u64,
    pub reserved_stamp: u64,
    pub reservations: Seq<u64>,
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
        ensures old(self).state().clock <= stamp < u64::MAX,
            final(self).state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                reservations: old(self).state().reservations.push(stamp), ..old(self).state() });
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
        final(driver).state().arena == old(driver).state().arena,
        final(driver).state().binding_count == old(driver).state().binding_count,
        final(driver).state().key_buckets == old(driver).state().key_buckets,
        final(driver).state().held == old(driver).state().held,
        final(driver).state().deregistered == old(driver).state().deregistered,
        changes.len() > 0 ==> old(driver).state().clock <= final(driver).state().reserved_stamp < u64::MAX,
        changes.len() > 0 ==> final(driver).state().clock == final(driver).state().reserved_stamp + 1,
        changes.len() > 0 ==> final(driver).state().reservations == old(driver).state().reservations.push(final(driver).state().reserved_stamp),
        changes.len() == 0 ==> final(driver).state() == old(driver).state(),
        result.is_ok() ==> stamp_relation(old(driver).state().stamps, final(driver).state().stamps,
            change_keys(old(driver).state(), changes@, changes.len() as int), final(driver).state().reserved_stamp),
        // Failures may have published a prefix; every changed stamp has the
        // reserved value, and unrelated buckets are never modified.
        forall|p: Pair| !change_keys(old(driver).state(), changes@, changes.len() as int).contains(p) ==>
            final(driver).state().stamps.contains_key(p) == old(driver).state().stamps.contains_key(p)
            && (old(driver).state().stamps.contains_key(p) ==> final(driver).state().stamps[p] == old(driver).state().stamps[p]),
        forall|p: Pair| final(driver).state().stamps.contains_key(p)
            && (!old(driver).state().stamps.contains_key(p) || final(driver).state().stamps[p] != old(driver).state().stamps[p]) ==>
                change_keys(old(driver).state(), changes@, changes.len() as int).contains(p)
                && final(driver).state().stamps[p] == final(driver).state().reserved_stamp,
{
    let ghost initial = driver.state(); proof { stamp_unchanged(initial.stamps, initial.reserved_stamp); } if true {
    return Ok ( ( ) ) ;
}
let stamp = driver . reserve_stamp ( ) ;
let mut touched = C :: new ( ) ;

        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                    reservations: initial.reservations.push(stamp), ..initial }),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
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
                driver.state().arena == initial.arena,
                canonical(ordered@, change_keys(initial, changes@, changes.len() as int)),
                driver.state().bindings == initial.bindings, driver.state().held == initial.held,
                driver.state().key_buckets == initial.key_buckets,
                driver.state().deregistered == initial.deregistered,
                driver.state().clock == stamp + 1, driver.state().reserved_stamp == stamp,
                driver.state().reservations == initial.reservations.push(stamp),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
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
