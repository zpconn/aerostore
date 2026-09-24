// Generated checked native publication-slice composition.
// Checked joins between source-bound native operations. This is a proof-only
// harness, not a second engine. Native memory/atomic/RAII correspondence remains
// a declared boundary; no Lean theorem is imported into Verus.
mod predicate { // Generated from native indexed transaction operations; see generate.py.
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
    let ghost initial = driver.state(); proof { stamp_unchanged(initial.stamps, initial.reserved_stamp); } if changes . is_empty ( ) {
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
 }
mod guards { // Generated from native guard acquisition; see generate.py.
// Guard identity refinement. These are operation-local observations, not an
// assertion that the shared database is frozen. Actual mutex exclusion, guard
// ownership/Drop, arena resolution and Acquire/Release visibility remain explicit
// primitive obligations. No object here is linked into the production engine.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub type Pair = (usize, usize);
#[derive(PartialEq, Eq, Debug)]
pub enum Error { Poisoned, InvalidBucket(usize), SerializationFailure, Other }
pub struct Guard { pub arena: usize, pub physical: Pair }

pub trait Primitives {
    spec fn arena(&self) -> usize;
    spec fn registry(&self) -> Seq<usize>;
    spec fn bucket_count(&self) -> usize;
    fn publication_header(&self, binding: usize) -> (r: Result<usize, Error>)
        requires binding < self.registry().len(),
        ensures r.is_ok() ==> r.unwrap() == self.registry()[binding as int];
    // This is an actual health observation, not a promise of perpetual health.
    fn poisoned(&self, header: usize) -> bool;
    fn bucket(&self, header: usize, bucket: usize) -> (r: Option<Pair>)
        ensures r.is_some() <==> bucket < self.bucket_count(),
            r.is_some() ==> r.unwrap() == (header, bucket);
    // A successful primitive returns ownership of this exact physical mutex.
    // This contract must eventually be discharged by ShmMutex/RAII refinement.
    fn try_lock(&self, slot: Pair) -> (r: Option<Guard>)
        ensures r.is_some() ==> r.unwrap().physical == slot && r.unwrap().arena == self.arena();
    fn yield_now(&self);
    fn spin_loop(&self);
}
pub open spec fn physical(registry: Seq<usize>, key: Pair) -> Pair {
    (registry[key.0 as int], key.1)
}
pub open spec fn keys_valid(registry: Seq<usize>, keys: Seq<Pair>) -> bool {
    forall|i: int| 0 <= i < keys.len() ==> keys[i].0 < registry.len()
}
pub open spec fn guards_match(registry: Seq<usize>, keys: Seq<Pair>, guards: Seq<Guard>) -> bool {
    guards.len() == keys.len()
    && forall|i: int| 0 <= i < keys.len() ==> guards[i].physical == physical(registry, keys[i])
}
pub open spec fn all_guards_in(guards: Seq<Guard>, arena: usize) -> bool {
    forall|i: int| 0 <= i < guards.len() ==> guards[i].arena == arena
}
pub open spec fn physical_members(registry: Seq<usize>, keys: Seq<Pair>) -> Set<Pair> {
    keys.map(|i: int, key: Pair| physical(registry, key)).to_set()
}
pub open spec fn guard_members(guards: Seq<Guard>) -> Set<Pair> {
    guards.map(|i: int, guard: Guard| guard.physical).to_set()
}

pub fn transactional_try_lock_bucket<D: Primitives>(driver: &D, binding: usize, bucket: usize)
    -> (result: Result<Option<Guard>, Error>)
    requires binding < driver.registry().len(),
    ensures result.is_ok() && result.unwrap().is_some() ==>
        result.unwrap().unwrap().physical == physical(driver.registry(), (binding, bucket))
            && result.unwrap().unwrap().arena == driver.arena() && bucket < driver.bucket_count(),
{
    let header = driver . publication_header ( binding ) ? ;
if driver . poisoned ( header ) {
    return Err ( Error :: Poisoned ) ;
}
let bucket = driver . bucket ( header , bucket ) . ok_or ( Error :: InvalidBucket ( bucket ) ) ? ;
Ok ( driver . try_lock ( bucket ) )
}

pub fn acquire_index_bucket<D: Primitives>(driver: &D, binding: usize, bucket: usize)
    -> (result: Result<Guard, Error>)
    requires binding < driver.registry().len(),
    ensures result.is_ok() ==>
        result.unwrap().physical == physical(driver.registry(), (binding, bucket))
            && result.unwrap().arena == driver.arena() && bucket < driver.bucket_count(),
{
    let mut attempt: u32 = 0;
while attempt < 4096
invariant attempt <= 4096, binding < driver.registry().len(),
decreases 4096 - attempt,
{
if let Some ( guard ) = transactional_try_lock_bucket ( driver , binding , bucket ) ? {
    return Ok ( guard ) ;
}
if attempt & 0x3f == 0x3f {
    driver . yield_now ( ) ;
}
driver . spin_loop ( ) ;
attempt += 1;
}
Err ( Error :: SerializationFailure )
}

pub fn acquire_index_locks<D: Primitives>(driver: &D, keys: &Vec<Pair>)
    -> (result: Result<Vec<Guard>, Error>)
    requires keys_valid(driver.registry(), keys@),
    ensures result.is_ok() ==> guards_match(driver.registry(), keys@, result.unwrap()@),
        result.is_ok() ==> all_guards_in(result.unwrap()@, driver.arena()),
        result.is_ok() ==> forall|i: int| 0 <= i < keys.len() ==> keys[i].1 < driver.bucket_count(),
{
    let mut guards: Vec<Guard> = Vec::with_capacity(keys.len());
    let mut pos: usize = 0;
    while pos < keys.len()
        invariant pos <= keys.len(), guards.len() == pos,
            keys_valid(driver.registry(), keys@),
            forall|i: int| 0 <= i < pos ==> guards[i].physical == physical(driver.registry(), keys[i]),
            forall|i: int| 0 <= i < pos ==> guards[i].arena == driver.arena(),
            forall|i: int| 0 <= i < pos ==> keys[i].1 < driver.bucket_count(),
        decreases keys.len() - pos,
    {
        let (binding, bucket) = &keys[pos];
guards . push ( acquire_index_bucket ( driver , * binding , * bucket ) ? ) ;
pos += 1;
}
Ok ( guards )
}

pub proof fn acquired_guards_cover_keys(registry: Seq<usize>, keys: Seq<Pair>, guards: Seq<Guard>)
    requires guards_match(registry, keys, guards),
    ensures guard_members(guards) == physical_members(registry, keys),
{
    assert(guards.map(|i: int, guard: Guard| guard.physical)
        =~= keys.map(|i: int, key: Pair| physical(registry, key)));
}

pub proof fn separate_indexes_have_separate_guards(registry: Seq<usize>, left: Pair, right: Pair)
    requires left.0 < registry.len(), right.0 < registry.len(), left != right,
        forall|i: int, j: int| 0 <= i < j < registry.len() ==> registry[i] != registry[j],
    ensures physical(registry, left) != physical(registry, right),
{
    if left.0 < right.0 { assert(registry[left.0 as int] != registry[right.0 as int]); }
    if right.0 < left.0 { assert(registry[right.0 as int] != registry[left.0 as int]); }
}
}
 }
mod lifecycle { // Generated from actual ProcArray lifecycle operations; see generate.py.
// Native ProcArray algorithms, conditional on mutex exclusion, acquisition
// framing, slot atomic projection, shared-clock linearization, allocation, and
// scoped guard release. Acquisition framing is a separate unproved assumption:
// a real blocking mutex does not preserve metadata from native API entry.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub const PROCARRAY_SLOTS: usize = 256;
pub const EMPTY_SLOT: u64 = 0;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProcArrayRegistration { pub slot_idx: u16, pub txid: u64 }
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProcArrayError {
    NoFreeSlot { txid: u64 }, InvalidSlot { slot_idx: u16 },
    SlotOwnershipMismatch { slot_idx: u16, expected_txid: u64, observed_txid: u64 },
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Slot { pub txid: u64, pub snapshot_xmin: u64 }
pub struct ProcSnapshot { pub xmin: u64, pub xmax: u64, pub in_flight: Vec<u64>, pub in_flight_len: u16 }
// Operation-local input metadata is the view at lifecycle-lock acquisition,
// not an arbitrary real API-entry view. The current driver gives acquisition
// an exact frame. Composing released calls therefore excludes unmodeled slot
// transitions before/during acquisition; shared-clock advances can still be
// represented at the explicit observation/reservation events below. A native
// interference/ownership refinement relating API entry to acquired state is
// not established by this model or by mutex exclusion alone.
pub struct State {
    pub slots: Seq<Slot>,
    // Latest observation of the shared atomic clock; external allocations may
    // advance it at the next modeled observation/reservation. It is not frozen
    // physically while the ProcArray mutex is held.
    pub clock: u64,
    // The last atomic load may lag the current counter, including a Relaxed
    // snapshot load. Native xmax adjustment must cover all active txids.
    pub sampled_clock: u64,
    // Logical history of reservations made through this shared-clock interface.
    // Interfering allocations may skip values; no synthetic consecutive IDs.
    pub reservations: Seq<u64>,
    pub lifecycle_held: bool,
}
pub open spec fn ordered(ids: Seq<u64>) -> bool {
    forall|i: int, j: int| 0 <= i < j < ids.len() ==> ids[i] < ids[j]
}
pub open spec fn well_formed(s: State) -> bool {
    s.slots.len() == PROCARRAY_SLOTS && 0 < s.sampled_clock <= s.clock
    && ordered(s.reservations)
    && (forall|i: int| 0 <= i < s.reservations.len() ==> 0 < #[trigger] s.reservations[i] < s.clock)
    && (forall|i: int| 0 <= i < s.slots.len() ==>
        if #[trigger] s.slots[i].txid == 0 { s.slots[i].snapshot_xmin == 0 }
        else { 0 < s.slots[i].snapshot_xmin <= s.slots[i].txid < s.clock
            && s.reservations.contains(s.slots[i].txid) })
    && (forall|i: int, j: int| 0 <= i < j < s.slots.len() && s.slots[i].txid != 0 ==>
        s.slots[i].txid != s.slots[j].txid)
}
pub open spec fn active(slots: Seq<Slot>, n: int) -> Seq<u64>
    decreases n,
{
    if n <= 0 { Seq::empty() }
    else if slots[n - 1].txid == 0 { active(slots, n - 1) }
    else { active(slots, n - 1).push(slots[n - 1].txid) }
}
pub open spec fn minimum_active(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_active(slots, n - 1, ceiling) }
    else { if slots[n - 1].txid < minimum_active(slots, n - 1, ceiling) {
        slots[n - 1].txid
    } else { minimum_active(slots, n - 1, ceiling) } }
}
pub open spec fn maximum_active(slots: Seq<Slot>, n: int) -> u64
    decreases n,
{
    if n <= 0 { 0 }
    else { if slots[n - 1].txid > maximum_active(slots, n - 1) {
        slots[n - 1].txid
    } else { maximum_active(slots, n - 1) } }
}
pub open spec fn minimum_retention(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_retention(slots, n - 1, ceiling) }
    else { if slots[n - 1].snapshot_xmin < minimum_retention(slots, n - 1, ceiling) {
        slots[n - 1].snapshot_xmin
    } else { minimum_retention(slots, n - 1, ceiling) } }
}
pub open spec fn coherent_snapshot(s: State, snapshot: ProcSnapshot) -> bool {
    snapshot.in_flight@ == active(s.slots, s.slots.len() as int)
    && snapshot.in_flight_len == snapshot.in_flight.len()
    && snapshot.xmax == if s.sampled_clock > maximum_active(s.slots, s.slots.len() as int) + 1 {
        s.sampled_clock as int
    } else { maximum_active(s.slots, s.slots.len() as int) + 1 }
    && snapshot.xmin == minimum_active(s.slots, s.slots.len() as int, s.sampled_clock)
}
pub open spec fn observation(before: State, after: State) -> bool {
    after.slots == before.slots && after.reservations == before.reservations
    && after.lifecycle_held == before.lifecycle_held && after.clock >= before.clock
    && 0 < after.sampled_clock <= after.clock
}
pub open spec fn reservation(before: State, after: State, id: u64) -> bool {
    id >= before.clock && id < u64::MAX && after.clock == id + 1
    && after.reservations == before.reservations.push(id)
    && after.slots == before.slots && after.lifecycle_held == before.lifecycle_held
    && after.sampled_clock == before.sampled_clock
}

pub trait LifecyclePrimitives {
    spec fn state(&self) -> State;
    // EXTRA ASSUMPTION: this preserves the operation-local acquired-input view.
    // Native ShmMutex::lock may block while other threads change slots/history;
    // that interference is not represented here. The algorithm's old(state)
    // postconditions must not be read as frames from arbitrary native API entry.
    fn lock_lifecycle(&mut self)
        requires !old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: true, ..old(self).state() });
    fn release_lifecycle(&mut self)
        requires old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: false, ..old(self).state() });
    // The lock precondition is an obligation of the registration protocol,
    // not a property assumed of all AtomicU64::fetch_add calls.
    fn reserve_registration(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held, 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn reserve_publication(&mut self) -> (id: u64)
        requires 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn load_clock(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held,
        ensures observation(old(self).state(), final(self).state()), id == final(self).state().sampled_clock;
    fn slot_txid(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].txid;
    fn slot_xmin(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].snapshot_xmin;
    fn compare_empty_register(&mut self, slot: usize, txid: u64) -> (ok: bool)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures ok == (old(self).state().slots[slot as int].txid == EMPTY_SLOT),
            final(self).state() == (State { slots: if ok {
                old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] })
            } else { old(self).state().slots }, ..old(self).state() });
    fn store_xmin(&mut self, slot: usize, xmin: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { snapshot_xmin: xmin, ..old(self).state().slots[slot as int] }), ..old(self).state() });
    fn store_txid(&mut self, slot: usize, txid: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] }), ..old(self).state() });
}

pub fn checked_slot(slot: u16) -> (result: Result<usize, ProcArrayError>)
    ensures result.is_ok() ==> result.unwrap() == slot as usize && slot < PROCARRAY_SLOTS,
        result.is_err() ==> slot >= PROCARRAY_SLOTS,
{
    if (slot as usize) < PROCARRAY_SLOTS { Ok(slot as usize) }
    else { Err(ProcArrayError::InvalidSlot { slot_idx: slot }) }
}

pub proof fn reservation_preserves_clock_history(before: State, after: State, id: u64)
    requires well_formed(before), reservation(before, after, id),
    ensures well_formed(after), after.reservations.contains(id),
        forall|i: int| 0 <= i < before.reservations.len() ==> before.reservations[i] < id,
{}
pub proof fn observation_preserves_state(before: State, after: State)
    requires well_formed(before), observation(before, after),
    ensures well_formed(after),
{}

pub proof fn snapshot_scan_bounds(s: State, n: int)
    requires well_formed(s), 0 <= n <= s.slots.len(),
    ensures active(s.slots, n).len() <= n,
        0 < minimum_active(s.slots, n, s.sampled_clock) <= s.sampled_clock,
        maximum_active(s.slots, n) < s.clock,
        active(s.slots, n).len() == 0 ==> minimum_active(s.slots, n, s.sampled_clock) == s.sampled_clock,
        active(s.slots, n).len() == 0 ==> maximum_active(s.slots, n) == 0,
    decreases n,
{
    if n > 0 { snapshot_scan_bounds(s, n - 1); }
}

pub proof fn snapshot_minimum_covers_slot(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures minimum_active(s.slots, n, s.sampled_clock) <= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { snapshot_minimum_covers_slot(s, i, n - 1); }
}

pub proof fn snapshot_fields_unchanged(before: Seq<Slot>, after: Seq<Slot>, n: int, ceiling: u64)
    requires before.len() == after.len(), 0 <= n <= before.len(),
        forall|i: int| 0 <= i < before.len() ==> before[i].txid == after[i].txid,
    ensures active(before, n) == active(after, n),
        minimum_active(before, n, ceiling) == minimum_active(after, n, ceiling),
        maximum_active(before, n) == maximum_active(after, n),
    decreases n,
{
    if n > 0 { snapshot_fields_unchanged(before, after, n - 1, ceiling); }
}

pub proof fn reservation_after_reader(before: State, after: State, stamp: u64, reader: u64)
    requires well_formed(before), reservation(before, after, stamp), before.reservations.contains(reader),
    ensures stamp > reader,
{
    let i = choose|i: int| 0 <= i < before.reservations.len() && before.reservations[i] == reader;
    assert(reader < before.clock);
}

pub proof fn active_slot_membership(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures active(s.slots, n).contains(s.slots[i].txid), maximum_active(s.slots, n) >= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { active_slot_membership(s, i, n - 1); }
}

pub proof fn snapshot_covers_active_writer(s: State, snapshot: ProcSnapshot, writer_slot: int)
    requires well_formed(s), coherent_snapshot(s, snapshot),
        0 <= writer_slot < s.slots.len(), s.slots[writer_slot].txid != 0,
    ensures snapshot.in_flight@.contains(s.slots[writer_slot].txid),
        0 < snapshot.xmin <= s.slots[writer_slot].txid < snapshot.xmax <= s.clock,
{
    snapshot_scan_bounds(s, s.slots.len() as int);
    snapshot_minimum_covers_slot(s, writer_slot, s.slots.len() as int);
    active_slot_membership(s, writer_slot, s.slots.len() as int);
}

pub proof fn retention_covers_active_snapshot(s: State, slot: int, n: int)
    requires well_formed(s), 0 <= slot < n <= s.slots.len(), s.slots[slot].txid != 0,
    ensures minimum_retention(s.slots, n, s.sampled_clock) <= s.slots[slot].snapshot_xmin,
    decreases n,
{
    if slot < n - 1 { retention_covers_active_snapshot(s, slot, n - 1); }
}

pub proof fn reservation_history_strict_order(s: State, earlier: int, later: int)
    requires well_formed(s), 0 <= earlier < later < s.reservations.len(),
    ensures s.reservations[earlier] < s.reservations[later],
{}

// Non-vacuity witnesses include an older retained writer and a stale sampled
// clock below both active IDs. The native xmax compensation is necessary for
// this permitted observation; a latest-value load is not assumed.
pub proof fn lifecycle_contract_has_live_and_stale_witnesses() {
    let empty = Seq::new(PROCARRAY_SLOTS as nat, |i: int| Slot { txid: 0, snapshot_xmin: 0 });
    let initial = State { slots: empty, clock: 1, sampled_clock: 1,
        reservations: Seq::empty(), lifecycle_held: false };
    assert(well_formed(initial));
    let live = State { slots: empty.update(0, Slot { txid: 5, snapshot_xmin: 2 })
        .update(1, Slot { txid: 10, snapshot_xmin: 5 }), clock: 11, sampled_clock: 1,
        reservations: seq![2u64, 5u64, 10u64], lifecycle_held: true };
    assert(well_formed(live));
    assert(live.sampled_clock < live.slots[0].txid < live.slots[1].txid);
    let later = State { clock: 12, reservations: live.reservations.push(11), ..live };
    assert(reservation(live, later, 11));
    reservation_after_reader(live, later, 11, 10);
    assert(11u64 > 10u64);
}

pub fn begin_transaction<D: LifecyclePrimitives>(driver: &mut D) -> (result: Result<ProcArrayRegistration, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock > old(driver).state().clock,
        final(driver).state().reservations.len() == old(driver).state().reservations.len() + 1,
        result.is_ok() ==> {
            let r = result.unwrap();
            r.slot_idx < PROCARRAY_SLOTS && r.txid >= old(driver).state().clock
            && final(driver).state().reservations == old(driver).state().reservations.push(r.txid)
            && old(driver).state().slots[r.slot_idx as int].txid == 0
            && final(driver).state().slots == old(driver).state().slots.update(r.slot_idx as int,
                Slot { txid: r.txid, snapshot_xmin: r.txid })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots
            && (forall|i: int| 0 <= i < old(driver).state().slots.len() ==> old(driver).state().slots[i].txid != 0),
        match result {
            Err(ProcArrayError::NoFreeSlot { txid }) => txid >= old(driver).state().clock
                && final(driver).state().reservations == old(driver).state().reservations.push(txid),
            Err(_) => false,
            Ok(_) => true,
        },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let ghost before_reservation = driver.state(); let txid = driver . reserve_registration ( ) ;
proof { reservation_preserves_clock_history(before_reservation, driver.state(), txid); } let ghost reserved = driver.state(); 
        let mut slot_idx: usize = 0;
        while slot_idx < PROCARRAY_SLOTS
            invariant slot_idx <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(initial), well_formed(reserved), driver.state() == reserved,
                reserved.slots == initial.slots, reserved.lifecycle_held,
                reserved.clock > initial.clock, txid >= initial.clock, txid < reserved.clock,
                reserved.reservations == initial.reservations.push(txid),
                forall|i: int| 0 <= i < slot_idx ==> initial.slots[i].txid != 0,
            decreases PROCARRAY_SLOTS - slot_idx,
        {
     if driver . compare_empty_register ( slot_idx , txid ) {
    driver . store_xmin ( slot_idx , txid ) ;
    return Ok ( ProcArrayRegistration {
        slot_idx : slot_idx as u16 , txid ,
    }
    ) ;
}

            slot_idx += 1;
        }
     Err ( ProcArrayError :: NoFreeSlot {
    txid
}
)
}

pub fn end_transaction<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<(), ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock == old(driver).state().clock,
        final(driver).state().sampled_clock == old(driver).state().sampled_clock,
        final(driver).state().reservations == old(driver).state().reservations,
        result.is_ok() ==> registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { txid: 0, snapshot_xmin: 0 }),
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        result == if registration.slot_idx >= PROCARRAY_SLOTS {
            Err(ProcArrayError::InvalidSlot { slot_idx: registration.slot_idx })
        } else if old(driver).state().slots[registration.slot_idx as int].txid != registration.txid {
            Err(ProcArrayError::SlotOwnershipMismatch { slot_idx: registration.slot_idx,
                expected_txid: registration.txid,
                observed_txid: old(driver).state().slots[registration.slot_idx as int].txid })
        } else { Ok(()) },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot_idx = registration . slot_idx as usize ;
if slot_idx >= PROCARRAY_SLOTS {
    return Err ( ProcArrayError :: InvalidSlot {
        slot_idx : registration . slot_idx ,
    }
    ) ;
}
let slot = slot_idx ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
driver . store_xmin ( slot , EMPTY_SLOT ) ;
driver . store_txid ( slot , EMPTY_SLOT ) ;
Ok ( ( ) )
}

pub fn snapshot_locked<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), observation(old(driver).state(), final(driver).state()),
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); let mut xmax = driver . load_clock ( ) ;
let mut xmin = xmax ;
let mut max_in_flight = 0_u64 ;
let mut in_flight = Vec :: new ( ) ;
let mut in_flight_len = 0_u16 ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(initial, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, observation(initial, loaded),
                loaded.lifecycle_held, xmax == loaded.sampled_clock,
                in_flight@ == active(loaded.slots, si as int),
                in_flight_len == in_flight.len(), in_flight_len <= si,
                xmin == minimum_active(loaded.slots, si as int, loaded.sampled_clock),
                max_in_flight == maximum_active(loaded.slots, si as int),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof {
                reveal_with_fuel(active, 2); reveal_with_fuel(minimum_active, 2); reveal_with_fuel(maximum_active, 2);
            }
     let txid = driver . slot_txid ( slot ) ;
if txid == EMPTY_SLOT {
    continue ;
}
proof { assert((in_flight_len as usize) < PROCARRAY_SLOTS); } in_flight . push ( txid ) ;
in_flight_len += 1 ;
xmin = xmin . min ( txid ) ;
max_in_flight = max_in_flight . max ( txid ) ;

        }
        proof { snapshot_scan_bounds(loaded, PROCARRAY_SLOTS as int); }
     if in_flight_len == 0 {
    xmin = xmax ;
}
else {
    xmax = xmax . max ( max_in_flight . saturating_add ( 1 ) ) ;
}
ProcSnapshot {
    xmin , xmax , in_flight , in_flight_len ,
}
}

pub fn create_snapshot<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
snapshot_locked ( driver )
}

pub fn create_transaction_snapshot<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<ProcSnapshot, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held, registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result.is_ok() ==> {
            let snap = result->Ok_0;
            registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && coherent_snapshot(final(driver).state(), snap)
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { snapshot_xmin: snap.xmin, ..old(driver).state().slots[registration.slot_idx as int] })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        (registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid) == result.is_ok(),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot = checked_slot ( registration . slot_idx ) ? ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
let snapshot = snapshot_locked ( driver ) ;
let ghost snapshot_state = driver.state(); proof { snapshot_scan_bounds(snapshot_state, PROCARRAY_SLOTS as int); snapshot_minimum_covers_slot(snapshot_state, slot as int, PROCARRAY_SLOTS as int); } driver . store_xmin ( slot , snapshot . xmin ) ;
proof { snapshot_fields_unchanged(snapshot_state.slots, driver.state().slots, PROCARRAY_SLOTS as int, snapshot_state.sampled_clock); } Ok ( snapshot )
}

pub fn oldest_snapshot_xmin<D: LifecyclePrimitives>(driver: &mut D) -> (result: u64)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result == minimum_retention(final(driver).state().slots, PROCARRAY_SLOTS as int, final(driver).state().sampled_clock),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let mut xmin = driver . load_clock ( ) ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(State { lifecycle_held: true, ..initial }, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, loaded.lifecycle_held,
                loaded.slots == initial.slots, loaded.reservations == initial.reservations,
                loaded.clock >= initial.clock,
                xmin == minimum_retention(loaded.slots, si as int, loaded.sampled_clock),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof { reveal_with_fuel(minimum_retention, 2); }
     if driver . slot_txid ( slot ) != EMPTY_SLOT {
    xmin = xmin . min ( driver . slot_xmin ( slot ) ) ;
}

        }
     xmin
}

// This expression is extracted from publish_index_stamps, where the existing
// commit driver invokes it after finish_transaction. Its atomic operation is
// the same shared allocator used by native begin_transaction.
pub fn reserve_publication_clock<D: LifecyclePrimitives>(driver: &mut D) -> (stamp: u64)
    requires well_formed(old(driver).state()), old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), reservation(old(driver).state(), final(driver).state(), stamp),
{
    let ghost before = driver.state();
    let stamp = driver . reserve_publication ( );
    proof { reservation_preserves_clock_history(before, driver.state(), stamp); }
    stamp
}

// Proof harness: the actual end operation, its explicit scoped-guard boundary,
// and the actual publication-reservation expression. No extra reservation is
// inserted into AeroStore; composition must identify this event with the same
// fetch_add used by publish_index_stamps, never perform a second reservation.
pub fn deregister_then_reserve<D: LifecyclePrimitives>(driver: &mut D,
    registration: ProcArrayRegistration, reader: u64) -> (result: Result<u64, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0, old(driver).state().clock < u64::MAX,
        old(driver).state().reservations.contains(reader),
    ensures well_formed(final(driver).state()), !final(driver).state().lifecycle_held,
        result.is_ok() ==> result.unwrap() > reader,
        result.is_ok() ==> final(driver).state().reservations == old(driver).state().reservations.push(result.unwrap()),
        result.is_ok() ==> (forall|i: int| 0 <= i < final(driver).state().slots.len() ==>
            final(driver).state().slots[i].txid != registration.txid),
        result.is_err() ==> final(driver).state().reservations == old(driver).state().reservations,
{
    let result = end_transaction(driver, registration);
    driver.release_lifecycle();
    match result {
        Err(error) => Err(error),
        Ok(()) => {
            let ghost before = driver.state();
            let stamp = reserve_publication_clock(driver);
            proof { reservation_after_reader(before, driver.state(), stamp, reader); }
            Ok(stamp)
        }
    }
}
}
 }
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
    let acquired = match Ok::<Vec<guards::Guard>, guards::Error>(Vec::new()) {
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
