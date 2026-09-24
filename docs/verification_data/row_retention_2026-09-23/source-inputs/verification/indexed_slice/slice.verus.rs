// Generated source-bound one-bucket indexed-read/validation composition.
// Checked composition of exact native dependency capture, owned guard release,
// history-based candidate coverage, MVCC materialization and validation.
mod ownership { // Generated from native ShmMutex CAS/Drop and index selection; see generate.py.
// Source-bound lock ownership. Raw atomic linearization and one authority per
// physical shared mutex remain explicit assumptions; no runtime code is added.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub type Key = (usize, usize, usize); // physical arena, header, bucket
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum Ordering { Acquire, Relaxed, Release }
pub struct Cell { pub bit: u32, pub owner: Option<nat> }
pub tracked struct Authority {
    ghost cells: Map<Key, Cell>,
    ghost next: nat,
}
pub tracked struct LeaseToken { ghost key: Key, ghost serial: nat }
// No public constructor, Clone or Copy. Native ShmMutexGuard likewise has private
// fields and a borrowed mutex pointer; the adapter checks that representation.
pub struct ReadLease { physical: Key, token: Tracked<LeaseToken> }

pub closed spec fn lease_key(lease: &ReadLease) -> Key { lease.physical }
pub closed spec fn lease_serial(lease: &ReadLease) -> nat { lease.token@.serial }
pub closed spec fn cells(authority: &Authority) -> Map<Key, Cell> { authority.cells }
pub closed spec fn next_serial(authority: &Authority) -> nat { authority.next }
pub closed spec fn valid(authority: &Authority) -> bool {
    forall|key: Key| authority.cells.contains_key(key) ==>
        (authority.cells[key].bit == 0 || authority.cells[key].bit == 1)
        && (authority.cells[key].owner.is_some() <==> authority.cells[key].bit == 1)
        && (authority.cells[key].owner.is_some() ==> authority.cells[key].owner.unwrap() < authority.next)
}
pub closed spec fn authorized(authority: &Authority, lease: &ReadLease) -> bool {
    lease.physical == lease.token@.key && authority.cells.contains_key(lease.physical)
        && authority.cells[lease.physical].bit == 1
        && authority.cells[lease.physical].owner == Some(lease.token@.serial)
}
pub closed spec fn framed(before: &Authority, after: &Authority, key: Key) -> bool {
    forall|other: Key| before.cells.contains_key(other) == after.cells.contains_key(other)
        && (other != key && before.cells.contains_key(other) ==> before.cells[other] == after.cells[other])
}

// Atomic protocol events. A successful native CAS and native Drop each refine
// one event; read-only observations/failed CAS refine Observe. Ghost grant and
// revoke are folded into the atomic linearization event, not interleavable work.
pub enum Step { Acquire { key: Key, serial: nat }, Release { key: Key, serial: nat }, Observe }
pub open spec fn step(before: &Authority, after: &Authority, event: Step) -> bool {
    match event {
        Step::Acquire { key, serial } => cells(before).contains_key(key)
            && cells(before)[key].bit == 0 && cells(before)[key].owner.is_none()
            && serial == next_serial(before) && next_serial(after) == next_serial(before) + 1
            && cells(after) == cells(before).insert(key, Cell { bit: 1, owner: Some(serial) }),
        Step::Release { key, serial } => cells(before).contains_key(key)
            && cells(before)[key].bit == 1 && cells(before)[key].owner == Some(serial)
            && next_serial(after) == next_serial(before)
            && cells(after) == cells(before).insert(key, Cell { bit: 0, owner: None }),
        Step::Observe => cells(after) == cells(before) && next_serial(after) == next_serial(before),
    }
}
pub open spec fn releases(event: Step, key: Key, serial: nat) -> bool {
    match event { Step::Release { key: k, serial: s } => k == key && s == serial, _ => false }
}

// Each call denotes one actual atomic event on the uniquely resolved physical
// location. The ghost Authority is globally unique for this modeled arena;
// native pointer/arena resolution and coupling to the shared atomic are not
// established by the Rust-source transformation.
pub trait Atomics {
    fn priority_waiters(&self, key: Key, ordering: Ordering) -> u32
        requires ordering == Ordering::Acquire;
    fn compare_exchange(&self, key: Key, expected: u32, next: u32,
        success: Ordering, failure: Ordering, Tracked(authority): Tracked<&mut Authority>)
        -> (result: Result<u32, u32>)
        requires valid(old(authority)), cells(old(authority)).contains_key(key), expected == 0, next == 1,
            success == Ordering::Acquire, failure == Ordering::Relaxed,
        ensures result.is_ok() <==> cells(old(authority))[key].bit == expected,
            result.is_ok() ==> result.unwrap() == expected,
            result.is_err() ==> result.unwrap_err() == cells(old(authority))[key].bit,
            cells(final(authority)) == cells(old(authority)).insert(key, Cell {
                bit: if result.is_ok() { next } else { cells(old(authority))[key].bit },
                owner: cells(old(authority))[key].owner,
            }), next_serial(final(authority)) == next_serial(old(authority));
    fn store(&self, key: Key, value: u32, ordering: Ordering, Tracked(authority): Tracked<&mut Authority>)
        requires cells(old(authority)).contains_key(key), cells(old(authority))[key].bit == 1,
            cells(old(authority))[key].owner.is_none(), ordering == Ordering::Release,
        ensures cells(final(authority)) == cells(old(authority)).insert(key, Cell {
            bit: value, owner: cells(old(authority))[key].owner,
        }), next_serial(final(authority)) == next_serial(old(authority));
}

// These ghost updates belong to the successful CAS / releasing store events;
// they add no runtime step or window between the native atomic and ownership.
proof fn grant(tracked authority: &mut Authority, key: Key) -> (tracked token: LeaseToken)
    requires cells(old(authority)).contains_key(key), cells(old(authority))[key].bit == 1,
        cells(old(authority))[key].owner.is_none(),
    ensures token.key == key, token.serial == next_serial(old(authority)),
        cells(final(authority)) == cells(old(authority)).insert(key, Cell { bit: 1, owner: Some(token.serial) }),
        next_serial(final(authority)) == next_serial(old(authority)) + 1,
{
    let serial = authority.next;
    authority.cells = authority.cells.insert(key, Cell { bit: 1, owner: Some(serial) });
    authority.next = serial + 1;
    LeaseToken { key, serial }
}
proof fn revoke(tracked authority: &mut Authority, tracked token: LeaseToken)
    requires cells(old(authority)).contains_key(token.key), cells(old(authority))[token.key].bit == 1,
        cells(old(authority))[token.key].owner == Some(token.serial),
    ensures cells(final(authority)) == cells(old(authority)).insert(token.key, Cell { bit: 1, owner: None }),
        next_serial(final(authority)) == next_serial(old(authority)),
{
    authority.cells = authority.cells.insert(token.key, Cell { bit: 1, owner: None });
}

pub fn try_acquire<D: Atomics>(driver: &D, key: Key, Tracked(authority): Tracked<&mut Authority>)
    -> (result: Option<ReadLease>)
    requires valid(old(authority)), cells(old(authority)).contains_key(key),
    ensures valid(final(authority)), framed(old(authority), final(authority), key),
        result.is_some() ==> lease_key(&result.unwrap()) == key && authorized(final(authority), &result.unwrap()),
        result.is_none() ==> cells(final(authority)) == cells(old(authority)) && next_serial(final(authority)) == next_serial(old(authority)),
        cells(old(authority))[key].bit == 1 ==> result.is_none(),
        result.is_some() ==> cells(old(authority))[key].bit == 0
            && lease_serial(&result.unwrap()) == next_serial(old(authority)),
        cells(old(authority))[key].bit == 0 ==> result.is_some(),
        next_serial(final(authority)) >= next_serial(old(authority)),
        result.is_some() ==> step(old(authority), final(authority), Step::Acquire {
            key, serial: lease_serial(&result.unwrap()) }),
        result.is_none() ==> step(old(authority), final(authority), Step::Observe),
{
    proof { reveal(step); }
    let ghost before = *authority;
    match driver.compare_exchange(key, 0 , 1 , Ordering :: Acquire , Ordering :: Relaxed, Tracked(&mut *authority)) {
        Ok(_) => {
            let tracked token = grant(&mut *authority, key);
            proof {
                assert(cells(&before)[key].bit == 0);
                assert(cells(&before)[key].owner.is_none());
                assert(cells(authority) =~= cells(&before).insert(key, Cell { bit: 1, owner: Some(token.serial) }));
                assert(step(&before, &*authority, Step::Acquire { key, serial: token.serial }));
            }
            Some(ReadLease { physical: key, token: Tracked(token) })
        }
        Err(_) => {
            proof { assert(cells(authority) =~= cells(&before)); }
            None
        },
    }
}

pub fn drop_lease<D: Atomics>(driver: &D, lease: ReadLease, Tracked(authority): Tracked<&mut Authority>)
    requires valid(old(authority)), authorized(old(authority), &lease),
    ensures valid(final(authority)), framed(old(authority), final(authority), lease_key(&lease)),
        cells(final(authority))[lease_key(&lease)] == (Cell { bit: 0, owner: None }),
        next_serial(final(authority)) == next_serial(old(authority)),
        step(old(authority), final(authority), Step::Release { key: lease_key(&lease), serial: lease_serial(&lease) }),
{
    proof { reveal(step); }
    let ghost before = *authority;
    let ghost old_key = lease_key(&lease);
    let ghost old_serial = lease_serial(&lease);
    let ReadLease { physical, token: Tracked(token) } = lease;
    proof { revoke(&mut *authority, token); }
    driver.store(physical, 0 , Ordering :: Release, Tracked(&mut *authority));
    proof {
        assert(cells(authority) =~= cells(&before).insert(old_key, Cell { bit: 0, owner: None }));
        assert(step(&before, &*authority, Step::Release { key: old_key, serial: old_serial }));
    }
}

pub type LeaseId = (Key, nat);
pub open spec fn protected_valid(authority: &Authority, protected: Set<LeaseId>) -> bool {
    forall|id: LeaseId| protected.contains(id) ==> cells(authority).contains_key(id.0)
        && cells(authority)[id.0].bit == 1 && cells(authority)[id.0].owner == Some(id.1)
}
pub open spec fn held_ids(leases: Seq<ReadLease>) -> Set<LeaseId> {
    leases.map(|i: int, lease: ReadLease| (lease_key(&lease), lease_serial(&lease))).to_set()
}
pub open spec fn leases_authorized(authority: &Authority, leases: Seq<ReadLease>) -> bool {
    forall|i: int| 0 <= i < leases.len() ==> authorized(authority, &leases[i])
}
pub open spec fn environment_trace(before: &Authority, after: &Authority, protected: Set<LeaseId>,
    w: (Seq<Authority>, Seq<Step>)) -> bool {
    w.0.len() == w.1.len() + 1
        && w.0[0] == *before && w.0[w.0.len() - 1] == *after
        && (forall|i: int| 0 <= i < w.1.len() ==> step(&w.0[i], &w.0[i + 1], w.1[i]))
        && (forall|i: int, id: LeaseId| 0 <= i < w.1.len() && protected.contains(id) ==>
            !releases(w.1[i], id.0, id.1))
}
pub open spec fn legal_environment(before: &Authority, after: &Authority, protected: Set<LeaseId>) -> bool {
    exists|w: (Seq<Authority>, Seq<Step>)| environment_trace(before, after, protected, w)
}

// A rely trace represents other actors' completed atomic protocol events at a
// native observation boundary. It may release/acquire any foreign-held target;
// it cannot consume a caller's actual borrowed lease. That ownership routing is
// explicit, rather than assuming all metadata stayed fixed while waiting.
pub trait Scheduler: Atomics {
    fn interfere(&self, Ghost(protected): Ghost<Set<LeaseId>>, Tracked(authority): Tracked<&mut Authority>)
        requires valid(old(authority)), protected_valid(old(authority), protected),
        ensures legal_environment(old(authority), final(authority), protected);
}

pub open spec fn no_resurrection(before: &Authority, after: &Authority) -> bool {
    next_serial(after) >= next_serial(before) && cells(after).dom() == cells(before).dom()
        && (forall|key: Key, serial: nat| cells(before).contains_key(key) && serial < next_serial(before)
            && cells(before)[key].owner != Some(serial) ==> cells(after)[key].owner != Some(serial))
}
pub proof fn step_does_not_resurrect(before: &Authority, after: &Authority, event: Step)
    requires step(before, after, event),
    ensures no_resurrection(before, after),
{
    assert(cells(after).dom() =~= cells(before).dom());
    assert forall|key: Key, serial: nat| cells(before).contains_key(key) && serial < next_serial(before)
        && cells(before)[key].owner != Some(serial) implies cells(after)[key].owner != Some(serial) by {
        match event {
            Step::Acquire { key: acquired, serial: fresh } => {
                if key == acquired { assert(fresh > serial); }
            }
            Step::Release { key: released, serial: old } => {},
            Step::Observe => {},
        }
    }
}
pub proof fn no_resurrection_transitive(before: &Authority, middle: &Authority, after: &Authority)
    requires no_resurrection(before, middle), no_resurrection(middle, after),
    ensures no_resurrection(before, after),
{}

pub proof fn step_preserves_protected(before: &Authority, after: &Authority,
    protected: Set<LeaseId>, event: Step)
    requires valid(before), protected_valid(before, protected), step(before, after, event),
        forall|id: LeaseId| protected.contains(id) ==> !releases(event, id.0, id.1),
    ensures valid(after), protected_valid(after, protected), cells(after).dom() == cells(before).dom(),
        no_resurrection(before, after),
{
    assert forall|id: LeaseId| protected.contains(id) implies cells(after).contains_key(id.0)
        && cells(after)[id.0].bit == 1 && cells(after)[id.0].owner == Some(id.1) by {
        match event {
            Step::Acquire { key, serial } => { if key == id.0 { assert(false); } }
            Step::Release { key, serial } => { if key == id.0 { assert(serial == id.1); assert(false); } }
            Step::Observe => {},
        }
    }
    assert(cells(after).dom() =~= cells(before).dom());
    step_does_not_resurrect(before, after, event);
}

pub proof fn trace_preserves_protected(states: Seq<Authority>, events: Seq<Step>, protected: Set<LeaseId>)
    requires states.len() == events.len() + 1, valid(&states[0]), protected_valid(&states[0], protected),
        forall|i: int| 0 <= i < events.len() ==> step(&states[i], &states[i + 1], events[i]),
        forall|i: int, id: LeaseId| 0 <= i < events.len() && protected.contains(id) ==> !releases(events[i], id.0, id.1),
    ensures valid(&states[states.len() - 1]), protected_valid(&states[states.len() - 1], protected),
        cells(&states[states.len() - 1]).dom() == cells(&states[0]).dom(),
        no_resurrection(&states[0], &states[states.len() - 1]),
    decreases events.len(),
{
    if events.len() > 0 {
        step_preserves_protected(&states[0], &states[1], protected, events[0]);
        trace_preserves_protected(states.drop_first(), events.drop_first(), protected);
        no_resurrection_transitive(&states[0], &states[1], &states[states.len() - 1]);
    }
}

pub fn environment<D: Scheduler>(driver: &D, Ghost(protected): Ghost<Set<LeaseId>>,
    Tracked(authority): Tracked<&mut Authority>)
    requires valid(old(authority)), protected_valid(old(authority), protected),
    ensures valid(final(authority)), protected_valid(final(authority), protected),
        cells(final(authority)).dom() == cells(old(authority)).dom(), no_resurrection(old(authority), final(authority)),
{
    let ghost before = *authority;
    driver.interfere(Ghost(protected), Tracked(&mut *authority));
    proof {
        let (states, events) = choose|w: (Seq<Authority>, Seq<Step>)|
            environment_trace(&before, &*authority, protected, w);
        trace_preserves_protected(states, events, protected);
    }
}

pub proof fn borrowed_leases_define_protection(authority: &Authority, leases: Seq<ReadLease>)
    requires leases_authorized(authority, leases),
    ensures protected_valid(authority, held_ids(leases)),
{
    let ids = leases.map(|i: int, lease: ReadLease| (lease_key(&lease), lease_serial(&lease)));
    assert forall|id: LeaseId| held_ids(leases).contains(id) implies cells(authority).contains_key(id.0)
        && cells(authority)[id.0].bit == 1 && cells(authority)[id.0].owner == Some(id.1) by {
        let i = choose|i: int| 0 <= i < ids.len() && ids[i] == id;
        assert(authorized(authority, &leases[i]));
    }
}
pub proof fn protected_borrows_remain_authorized(before: &Authority, after: &Authority, leases: Seq<ReadLease>)
    requires leases_authorized(before, leases), protected_valid(after, held_ids(leases)),
    ensures leases_authorized(after, leases),
{
    let ids = leases.map(|i: int, lease: ReadLease| (lease_key(&lease), lease_serial(&lease)));
    assert forall|i: int| 0 <= i < leases.len() implies authorized(after, &leases[i]) by {
        assert(ids[i] == (lease_key(&leases[i]), lease_serial(&leases[i])));
        assert(held_ids(leases).contains(ids[i]));
    }
}

pub proof fn protection_can_shrink(authority: &Authority, larger: Set<LeaseId>, smaller: Set<LeaseId>)
    requires protected_valid(authority, larger), smaller.subset_of(larger),
    ensures protected_valid(authority, smaller),
{}
pub proof fn protected_key_authorizes(before: &Authority, after: &Authority, lease: &ReadLease, protected: Set<LeaseId>)
    requires authorized(before, lease), protected_valid(after, protected),
        protected.contains((lease_key(lease), lease_serial(lease))),
    ensures authorized(after, lease),
{}

pub fn acquire_preserving_borrows<D: Atomics>(driver: &D, key: Key, held: &Vec<ReadLease>,
    Tracked(authority): Tracked<&mut Authority>) -> (result: Option<ReadLease>)
    requires valid(old(authority)), cells(old(authority)).contains_key(key), leases_authorized(old(authority), held@),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        protected_valid(final(authority), held_ids(held@)),
        cells(final(authority)).dom() == cells(old(authority)).dom(),
        cells(old(authority))[key].bit == 0 ==> result.is_some(),
        result.is_some() ==> lease_serial(&result.unwrap()) == next_serial(old(authority)),
        result.is_some() ==> authorized(final(authority), &result.unwrap()) && lease_key(&result.unwrap()) == key
            && !held_ids(held@).contains((key, lease_serial(&result.unwrap()))),
{
    let ghost before = *authority;
    proof { borrowed_leases_define_protection(&before, held@); }
    let result = try_acquire(driver, key, Tracked(&mut *authority));
    proof {
        let event = if result.is_some() { Step::Acquire { key, serial: lease_serial(&result.unwrap()) } }
            else { Step::Observe };
        step_preserves_protected(&before, &*authority, held_ids(held@), event);
        protected_borrows_remain_authorized(&before, &*authority, held@);
        if result.is_some() && held_ids(held@).contains((key, lease_serial(&result.unwrap()))) {
            assert(cells(&before)[key].bit == 1);
            assert(false);
        }
    }
    result
}

pub fn drop_preserving_borrows<D: Atomics>(driver: &D, lease: ReadLease, held: &Vec<ReadLease>,
    Tracked(authority): Tracked<&mut Authority>)
    requires valid(old(authority)), authorized(old(authority), &lease), leases_authorized(old(authority), held@),
        !held_ids(held@).contains((lease_key(&lease), lease_serial(&lease))),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        protected_valid(final(authority), held_ids(held@)),
        cells(final(authority)).dom() == cells(old(authority)).dom(),
        cells(final(authority))[lease_key(&lease)].bit == 0,
        next_serial(final(authority)) == next_serial(old(authority)),
{
    let ghost before = *authority;
    let ghost event = Step::Release { key: lease_key(&lease), serial: lease_serial(&lease) };
    proof { borrowed_leases_define_protection(&before, held@); }
    drop_lease(driver, lease, Tracked(&mut *authority));
    proof {
        step_preserves_protected(&before, &*authority, held_ids(held@), event);
        protected_borrows_remain_authorized(&before, &*authority, held@);
    }
}

pub fn try_lock<D: Scheduler>(driver: &D, key: Key, held: &Vec<ReadLease>,
    Tracked(authority): Tracked<&mut Authority>) -> (result: Option<ReadLease>)
    requires valid(old(authority)), cells(old(authority)).contains_key(key), leases_authorized(old(authority), held@),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        cells(final(authority)).dom() == cells(old(authority)).dom(),
        result.is_some() ==> lease_key(&result.unwrap()) == key && authorized(final(authority), &result.unwrap()),
{
    let ghost entry = *authority;
    let ghost protected = held_ids(held@);
    proof { borrowed_leases_define_protection(&*authority, held@); }
    environment(driver, Ghost(protected), Tracked(&mut *authority));
if driver . priority_waiters ( key , Ordering :: Acquire ) != 0 {
    proof {
        protected_borrows_remain_authorized ( & entry , & * authority , held @ ) ;
    }
    return None ;
}
environment ( driver , Ghost ( protected ) , Tracked ( & mut * authority ) ) ;
proof {
    protected_borrows_remain_authorized ( & entry , & * authority , held @ ) ;
}
let guard = acquire_preserving_borrows ( driver , key , held , Tracked ( & mut * authority ) ) ? ;
let ghost with_local = protected . insert ( ( lease_key ( & guard ) , lease_serial ( & guard ) ) ) ;
let ghost acquired = * authority ;
let ghost local_serial = lease_serial ( & guard ) ;
environment ( driver , Ghost ( with_local ) , Tracked ( & mut * authority ) ) ;
proof {
    protection_can_shrink ( & * authority , with_local , protected ) ;
    protected_borrows_remain_authorized ( & entry , & * authority , held @ ) ;
    protected_key_authorizes ( & acquired , & * authority , & guard , with_local ) ;
}
if driver . priority_waiters ( key , Ordering :: Acquire ) != 0 {
    drop_preserving_borrows ( driver , guard , held , Tracked ( & mut * authority ) ) ;
    proof {
        assert ( cells ( authority ) [ key ] . owner != Some ( local_serial ) ) ;
    }
    proof {
        protected_borrows_remain_authorized ( & entry , & * authority , held @ ) ;
    }
    return None ;
}
Some ( guard )
}

// A borrowed permission can justify a protected observation. Moving the lease
// to drop_lease makes this API unavailable afterwards under Rust/Verus moves.
pub fn read_permission(lease: &ReadLease, Tracked(authority): Tracked<&Authority>) -> (key: Key)
    requires valid(authority), authorized(authority, lease),
    ensures key == lease_key(lease), cells(authority)[key].bit == 1,
{
    lease.physical
}

// Same physical location cannot produce another live lease while the first is
// borrowed. Failed native CAS preserves the first lease's authorization.
pub fn competing_acquire_is_excluded<D: Atomics>(driver: &D, first: &ReadLease,
    Tracked(authority): Tracked<&mut Authority>) -> (rejected: bool)
    requires valid(old(authority)), authorized(old(authority), first),
    ensures rejected, valid(final(authority)), authorized(final(authority), first),
        cells(final(authority)) == cells(old(authority)),
{
    let key = read_permission(first, Tracked(&*authority));
    let second = try_acquire(driver, key, Tracked(&mut *authority));
    second.is_none()
}

pub fn release_then_handoff<D: Atomics>(driver: &D, first: ReadLease,
    Tracked(authority): Tracked<&mut Authority>) -> (next: ReadLease)
    requires valid(old(authority)), authorized(old(authority), &first),
    ensures valid(final(authority)), authorized(final(authority), &next),
        lease_key(&next) == lease_key(&first), lease_serial(&next) > lease_serial(&first),
{
    let key = read_permission(&first, Tracked(&*authority));
    drop_lease(driver, first, Tracked(&mut *authority));
    let next = try_acquire(driver, key, Tracked(&mut *authority));
    next.unwrap()
}

pub fn other_mutex_preserves_leases<D: Scheduler>(driver: &D, held: &Vec<ReadLease>, key: Key,
    Tracked(authority): Tracked<&mut Authority>) -> (other: Option<ReadLease>)
    requires valid(old(authority)), leases_authorized(old(authority), held@), cells(old(authority)).contains_key(key),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        other.is_some() ==> authorized(final(authority), &other.unwrap()) && lease_key(&other.unwrap()) == key,
{
    try_lock(driver, key, held, Tracked(&mut *authority))
}

// Private consistency-witness initializer. Clients cannot mint a second
// authority for an existing native arena. The unique initial authority is an
// explicit raw-arena initialization/coupling obligation, not a public safe API
// and not a proof of physical shared mapping uniqueness.
proof fn initialize_authority(keys: Set<Key>) -> (tracked authority: Authority)
    ensures valid(&authority), cells(&authority).dom() == keys,
        forall|key: Key| keys.contains(key) ==> cells(&authority)[key].bit == 0,
{
    Authority { cells: Map::new(keys, |key: Key| Cell { bit: 0, owner: None }), next: 0 }
}

pub proof fn physical_keys_are_distinct(left: Key, right: Key)
    requires left.0 != right.0 || left.1 != right.1 || left.2 != right.2,
    ensures left != right,
{}

#[derive(PartialEq, Eq, Debug)]
pub enum IndexError { Poisoned, InvalidBucket(usize), SerializationFailure, Other }

// This primitive resolves actual shared-memory locations; uniqueness/correctness
// of the arena/header projection and initialization of the authority are explicit
// native obligations. The selected bucket's try_lock is no longer an ownership
// assumption: it invokes the extracted CAS/guard/Drop operations above.
pub trait IndexAtomics: Scheduler {
    spec fn arena(&self) -> usize;
    spec fn registry(&self) -> Seq<usize>;
    spec fn bucket_count(&self) -> usize;
    fn publication_header(&self, binding: usize) -> (result: Result<usize, IndexError>)
        requires binding < self.registry().len(),
        ensures result.is_ok() ==> result.unwrap() == self.registry()[binding as int];
    fn poisoned(&self, header: usize) -> bool;
    fn yield_now(&self);
    fn spin_loop(&self);
    fn bucket(&self, header: usize, bucket: usize, Tracked(authority): Tracked<&Authority>)
        -> (result: Option<Key>)
        requires bucket_request_valid(self.arena(), self.bucket_count(), header, bucket, authority),
        ensures bucket_reply(self.arena(), self.bucket_count(), header, bucket, authority, result);
}

// The registered physical domain is supplied by native arena/header resolution;
// bucket() cannot conjure membership in an unrelated or empty authority.
pub open spec fn registered_domain(arena: usize, registry: Seq<usize>, count: usize,
    authority: &Authority) -> bool {
    forall|binding: int, bucket: usize| 0 <= binding < registry.len() && bucket < count ==>
        cells(authority).contains_key((arena, registry[binding], bucket))
}
pub open spec fn physical_domain<D: IndexAtomics>(driver: &D, authority: &Authority) -> bool {
    registered_domain(driver.arena(), driver.registry(), driver.bucket_count(), authority)
}
pub open spec fn bucket_request_valid(arena: usize, count: usize, header: usize, bucket: usize,
    authority: &Authority) -> bool {
    bucket >= count || cells(authority).contains_key((arena, header, bucket))
}
pub open spec fn bucket_reply(arena: usize, count: usize, header: usize, bucket: usize,
    authority: &Authority, result: Option<Key>) -> bool {
    (result.is_some() <==> bucket < count) && (result.is_some() ==>
        result.unwrap() == (arena, header, bucket) && cells(authority).contains_key(result.unwrap()))
}
pub proof fn domain_survives_interference<D: IndexAtomics>(driver: &D, before: &Authority, after: &Authority)
    requires physical_domain(driver, before), cells(after).dom() == cells(before).dom(),
    ensures physical_domain(driver, after),
{}

// Totality of the bucket primitive's allowed response for every admitted input,
// including out-of-bounds inputs and authorities belonging to another arena.
pub proof fn bucket_reply_has_witness(arena: usize, count: usize, header: usize, bucket: usize,
    authority: &Authority)
    requires bucket_request_valid(arena, count, header, bucket, authority),
    ensures exists|reply: Option<Key>| bucket_reply(arena, count, header, bucket, authority, reply),
{
    let reply = if bucket < count { Some((arena, header, bucket)) } else { None };
    assert(bucket_reply(arena, count, header, bucket, authority, reply));
}

pub proof fn physical_domain_has_live_witness() {
    let key: Key = (1, 2, 0);
    let tracked mut authority = initialize_authority(Set::empty().insert(key));
    assert(registered_domain(1, seq![2usize], 1, &authority));
    assert(bucket_request_valid(1, 1, 2, 0, &authority));
    assert(bucket_reply(1, 1, 2, 0, &authority, Some(key)));
    assert(bucket_reply(1, 1, 2, 1, &authority, None));
    // An admitted resolved bucket also permits the successful native CAS reply
    // and ownership grant; this is not a constructed native heap or scheduler.
    authority.cells = authority.cells.insert(key, Cell { bit: 1, owner: None });
    let tracked token = grant(&mut authority, key);
    assert(valid(&authority));
    assert(cells(&authority)[key].owner == Some(token.serial));
}

pub fn transactional_try_lock_bucket<D: IndexAtomics>(driver: &D, binding: usize, bucket: usize,
    held: &Vec<ReadLease>, Tracked(authority): Tracked<&mut Authority>) -> (result: Result<Option<ReadLease>, IndexError>)
    requires valid(old(authority)), binding < driver.registry().len(), leases_authorized(old(authority), held@),
        physical_domain(driver, old(authority)),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        cells(final(authority)).dom() == cells(old(authority)).dom(),
        result.is_ok() && result.unwrap().is_some() ==>
            authorized(final(authority), &result.unwrap().unwrap())
            && lease_key(&result.unwrap().unwrap()) == (driver.arena(), driver.registry()[binding as int], bucket),
{
    let header = driver . publication_header ( binding ) ? ;
if driver . poisoned ( header ) {
    return Err ( IndexError :: Poisoned ) ;
}
let bucket = driver . bucket ( header , bucket , Tracked ( & * authority ) ) . ok_or ( IndexError :: InvalidBucket ( bucket ) ) ? ;
Ok ( try_lock ( driver , bucket , held , Tracked ( & mut * authority ) ) )
}

pub fn acquire_index_bucket<D: IndexAtomics>(driver: &D, binding: usize, bucket: usize,
    held: &Vec<ReadLease>, Tracked(authority): Tracked<&mut Authority>) -> (result: Result<ReadLease, IndexError>)
    requires valid(old(authority)), binding < driver.registry().len(), leases_authorized(old(authority), held@),
        physical_domain(driver, old(authority)),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        cells(final(authority)).dom() == cells(old(authority)).dom(),
        result.is_ok() ==> authorized(final(authority), &result.unwrap())
            && lease_key(&result.unwrap()) == (driver.arena(), driver.registry()[binding as int], bucket),
{
    let ghost initial = *authority;
    let mut attempt: u32 = 0;
    while attempt < 4096
        invariant attempt <= 4096, binding < driver.registry().len(),
            valid(authority), leases_authorized(authority, held@), physical_domain(driver, authority),
            cells(authority).dom() == cells(&initial).dom(),
            cells(&initial).dom() == cells(old(authority)).dom(),
        decreases 4096 - attempt,
    {
if let Some ( guard ) = transactional_try_lock_bucket ( driver , binding , bucket , held , Tracked ( & mut * authority ) ) ? {
    return Ok ( guard ) ;
}
if attempt & 0x3f == 0x3f {
    environment_for_borrows ( driver , held , Tracked ( & mut * authority ) ) ;
    driver . yield_now ( ) ;
}
environment_for_borrows ( driver , held , Tracked ( & mut * authority ) ) ;
driver . spin_loop ( ) ;
attempt += 1;
}
Err ( IndexError :: SerializationFailure )
}

pub proof fn authorized_survives_other_key(before: &Authority, after: &Authority, lease: &ReadLease, key: Key)
    requires authorized(before, lease), framed(before, after, key), key != lease_key(lease),
    ensures authorized(after, lease),
{}

// Explicit verified lowering of native Vec<ShmMutexGuard> destruction. Native
// Rust move/Vec/RAII correspondence remains a frontend obligation; callers must
// source-check the release site before using this helper to model implicit Drop.
pub fn release_all<D: Scheduler>(driver: &D, leases: Vec<ReadLease>, Tracked(authority): Tracked<&mut Authority>)
    requires valid(old(authority)),
        forall|i: int| 0 <= i < leases.len() ==> authorized(old(authority), &leases[i]),
        forall|i: int, j: int| 0 <= i < j < leases.len() ==> lease_key(&leases[i]) != lease_key(&leases[j]),
    ensures valid(final(authority)), cells(final(authority)).dom() == cells(old(authority)).dom(),
        forall|i: int| 0 <= i < leases.len() ==> !authorized(final(authority), &leases[i]),
{
    let ghost initial = leases@;
    let mut leases = leases;
    while leases.len() > 0
        invariant valid(authority), leases.len() <= initial.len(),
            cells(authority).dom() == cells(old(authority)).dom(),
            forall|i: int| 0 <= i < leases.len() ==> leases[i] == initial[i],
            forall|i: int| 0 <= i < leases.len() ==> authorized(authority, &leases[i]),
            forall|i: int| 0 <= i < initial.len() ==> cells(authority).contains_key(lease_key(&initial[i])),
            forall|i: int| 0 <= i < initial.len() ==> lease_serial(&initial[i]) < next_serial(authority),
            forall|i: int, j: int| 0 <= i < j < initial.len() ==> lease_key(&initial[i]) != lease_key(&initial[j]),
            forall|i: int| leases.len() <= i < initial.len() ==>
                cells(authority)[lease_key(&initial[i])].owner != Some(lease_serial(&initial[i])),
        decreases leases.len(),
    {
        let ghost before = *authority;
        let ghost last = leases.len() as int - 1;
        proof { assert(0 <= last < leases.len()); assert(authorized(authority, &leases[last])); assert(leases[last] == initial[last]); }
        let lease = leases.pop().unwrap();
        proof { assert(lease == initial[last]); }
        let ghost key = lease_key(&lease);
        let ghost serial = lease_serial(&lease);
        drop_lease(driver, lease, Tracked(&mut *authority));
        proof {
            step_does_not_resurrect(&before, &*authority, Step::Release { key, serial });
            assert forall|i: int| 0 <= i < leases.len() implies authorized(authority, &leases[i]) by {
                assert(leases[i] == initial[i]);
                assert(lease_key(&initial[i]) != lease_key(&initial[last]));
                authorized_survives_other_key(&before, &*authority, &leases[i], key);
            }
            assert forall|i: int| leases.len() <= i < initial.len()
                implies cells(authority)[lease_key(&initial[i])].owner != Some(lease_serial(&initial[i])) by {
                if i == last { assert(lease_key(&initial[i]) == key); }
                else {
                    assert(last < i);
                    assert(cells(&before)[lease_key(&initial[i])].owner != Some(lease_serial(&initial[i])));
                }
            }
        }
        let ghost released = *authority;
        environment_for_borrows(driver, &leases, Tracked(&mut *authority));
        proof {
            assert forall|i: int| leases.len() <= i < initial.len()
                implies cells(authority)[lease_key(&initial[i])].owner != Some(lease_serial(&initial[i])) by {
                assert(lease_serial(&initial[i]) < next_serial(&released));
                assert(cells(&released)[lease_key(&initial[i])].owner != Some(lease_serial(&initial[i])));
            }
        }
    }
}

pub proof fn ownership_has_live_handoff_witness() {
    let key: Key = (1, 2, 3);
    let tracked mut authority = initialize_authority(Set::empty().insert(key));
    // Success replies of the actual raw 0->1 CAS and releasing store contracts.
    authority.cells = authority.cells.insert(key, Cell { bit: 1, owner: None });
    let tracked first = grant(&mut authority, key);
    assert(valid(&authority));
    assert(authority.cells[key].owner == Some(first.serial));
    let first_serial = first.serial;
    revoke(&mut authority, first);
    authority.cells = authority.cells.insert(key, Cell { bit: 0, owner: None });
    assert(valid(&authority));
    authority.cells = authority.cells.insert(key, Cell { bit: 1, owner: None });
    let tracked next = grant(&mut authority, key);
    assert(valid(&authority));
    assert(next.serial > first_serial);
    assert(authority.cells[key].owner == Some(next.serial));
}

// An interleaved actor cannot acquire a held mutex. Releasing it requires that
// exact owner's lease; Rust/Verus moves prevent another actor from consuming a
// borrowed lease. This lemma checks all three event kinds, not just a schedule.
pub proof fn interleaved_step_preserves_lease(before: &Authority, after: &Authority,
    lease: &ReadLease, event: Step)
    requires valid(before), authorized(before, lease), step(before, after, event),
        !releases(event, lease_key(lease), lease_serial(lease)),
    ensures authorized(after, lease), valid(after),
{
    match event {
        Step::Acquire { key, serial } => {
            if key == lease_key(lease) { assert(false); }
        }
        Step::Release { key, serial } => {
            if key == lease_key(lease) { assert(serial == lease_serial(lease)); assert(false); }
        }
        Step::Observe => {},
    }
}

// Induction over arbitrary finite legal atomic interleavings. It does not assert
// that an arbitrary native program/unsafe store follows this protocol; that
// correspondence and weak-memory publication remain explicit obligations.
pub proof fn interleaved_trace_preserves_lease(states: Seq<Authority>, events: Seq<Step>, lease: &ReadLease)
    requires states.len() == events.len() + 1, valid(&states[0]), authorized(&states[0], lease),
        forall|i: int| 0 <= i < events.len() ==> step(&states[i], &states[i + 1], events[i])
            && !releases(events[i], lease_key(lease), lease_serial(lease)),
    ensures authorized(&states[states.len() - 1], lease), valid(&states[states.len() - 1]),
    decreases events.len(),
{
    if events.len() > 0 {
        interleaved_step_preserves_lease(&states[0], &states[1], lease, events[0]);
        interleaved_trace_preserves_lease(states.drop_first(), events.drop_first(), lease);
    }
}
pub fn environment_for_borrows<D: Scheduler>(driver: &D, held: &Vec<ReadLease>,
    Tracked(authority): Tracked<&mut Authority>)
    requires valid(old(authority)), leases_authorized(old(authority), held@),
    ensures valid(final(authority)), leases_authorized(final(authority), held@),
        cells(final(authority)).dom() == cells(old(authority)).dom(), no_resurrection(old(authority), final(authority)),
{
    let ghost before = *authority;
    proof { borrowed_leases_define_protection(&before, held@); }
    environment(driver, Ghost(held_ids(held@)), Tracked(&mut *authority));
    proof { protected_borrows_remain_authorized(&before, &*authority, held@); }
}

// Actual source-bound failed CAS, another owner's consuming native Drop, then
// successful source-bound CAS. This is a positive interleaving witness, without
// assuming that every scheduler must choose this handoff.
pub fn foreign_release_between_attempts<D: Atomics>(driver: &D, foreign: ReadLease,
    held: &Vec<ReadLease>, Tracked(authority): Tracked<&mut Authority>) -> (acquired: ReadLease)
    requires valid(old(authority)), authorized(old(authority), &foreign), leases_authorized(old(authority), held@),
        !held_ids(held@).contains((lease_key(&foreign), lease_serial(&foreign))),
    ensures valid(final(authority)), authorized(final(authority), &acquired), leases_authorized(final(authority), held@),
        lease_key(&acquired) == lease_key(&foreign), lease_serial(&acquired) > lease_serial(&foreign),
{
    let key = read_permission(&foreign, Tracked(&*authority));
    let rejected = try_acquire(driver, key, Tracked(&mut *authority));
    assert(rejected.is_none());
    drop_preserving_borrows(driver, foreign, held, Tracked(&mut *authority));
    let acquired = acquire_preserving_borrows(driver, key, held, Tracked(&mut *authority));
    acquired.unwrap()
}

pub proof fn rely_allows_foreign_release_and_retains_local_owner() {
    let foreign: Key = (1, 2, 3);
    let local: Key = (1, 2, 4);
    let before = Authority { cells: Map::empty()
        .insert(foreign, Cell { bit: 1, owner: Some(0nat) })
        .insert(local, Cell { bit: 1, owner: Some(1nat) }), next: 2 };
    let after = Authority { cells: before.cells.insert(foreign, Cell { bit: 0, owner: None }), next: 2 };
    let protected = Set::empty().insert((local, 1nat));
    let states = seq![before, after];
    let events = seq![Step::Release { key: foreign, serial: 0 }];
    assert(valid(&before));
    assert(protected_valid(&before, protected));
    assert(step(&before, &after, events[0]));
    assert(legal_environment(&before, &after, protected)) by {
        assert(states.len() == events.len() + 1);
        assert(forall|i: int| 0 <= i < events.len() ==> step(&states[i], &states[i + 1], events[i]));
        assert(forall|i: int, id: LeaseId| 0 <= i < events.len() && protected.contains(id) ==>
            !releases(events[i], id.0, id.1));
        assert(environment_trace(&before, &after, protected, (states, events)));
    }
    trace_preserves_protected(states, events, protected);
    assert(cells(&after)[foreign].bit == 0);
    assert(cells(&after)[local].owner == Some(1nat));
    assert(cells(&before) != cells(&after));
}
pub proof fn rely_allows_new_owner_after_release_without_resurrecting_old_lease() {
    let key: Key = (7, 8, 9);
    let before = Authority { cells: Map::empty().insert(key, Cell { bit: 0, owner: None }), next: 2 };
    let after = Authority { cells: before.cells.insert(key, Cell { bit: 1, owner: Some(2nat) }), next: 3 };
    let states = seq![before, after];
    let events = seq![Step::Acquire { key, serial: 2 }];
    assert(valid(&before));
    assert(environment_trace(&before, &after, Set::empty(), (states, events)));
    assert(legal_environment(&before, &after, Set::empty()));
    trace_preserves_protected(states, events, Set::empty());
    assert(cells(&after)[key].bit == 1);
    assert(cells(&after)[key].owner != Some(1nat));
    assert(no_resurrection(&before, &after));
}
}
 }
mod lookup { // Generated from native visibility, read, and index_lookup materialization.
// Source-bound MVCC selection and indexed candidate materialization.
// Heap validity, retained version projections, raw posting enumeration, and
// key extraction remain explicit storage obligations. No runtime additions.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub const EMPTY_PTR: u32 = 0;
pub const MAX_VISIBLE_CHAIN_STEPS: u32 = 262_144;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error { SerializationFailure, Storage, RowOutOfBounds { row_id: usize, capacity: usize } }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Row { pub xmin: u64, pub xmax: u64, pub next: u32, pub value: Option<usize>, pub locked: bool, pub owner: u64 }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PendingWrite { pub row_id: usize, pub new_ptr: u32 }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ReadRecord { pub row_id: usize, pub row_ptr: u32, pub xmin: u64 }
pub struct Transaction {
    pub txid: u64, pub snapshot_xmin: u64, pub snapshot_xmax: u64,
    pub snapshot_active: Vec<u64>, pub write_set: Vec<PendingWrite>, pub read_set: Vec<ReadRecord>,
}
pub struct Snapshot { pub txid:u64, pub xmin:u64, pub xmax:u64, pub active:Seq<u64> }
pub open spec fn snapshot(tx:Transaction) -> Snapshot {
    Snapshot {txid:tx.txid,xmin:tx.snapshot_xmin,xmax:tx.snapshot_xmax,active:tx.snapshot_active@}
}
pub struct Image {
    pub heads: Map<usize, u32>, pub rows: Map<u32, Row>, pub rank: Map<u32, nat>, pub capacity: usize,
}
pub open spec fn image_valid(s: Image) -> bool {
    (forall|id: usize| #![trigger s.heads.contains_key(id)] #![trigger s.heads[id]]
        id < s.capacity ==> s.heads.contains_key(id)
        && (s.heads[id] == 0 || s.rows.contains_key(s.heads[id])))
    && (forall|p: u32| #[trigger] s.rows.contains_key(p) ==> p != 0 && s.rank.contains_key(p)
        && (s.rows[p].next == 0 || s.rows.contains_key(s.rows[p].next)
            && s.rank[s.rows[p].next] < s.rank[p]))
}
pub open spec fn creator_visible(xmin: u64, tx: Snapshot) -> bool {
    xmin == tx.txid || xmin < tx.xmin
        || (xmin < tx.xmax && !tx.active.contains(xmin))
}
pub open spec fn visible(row: Row, tx: Snapshot) -> bool {
    row.xmin == tx.txid || (creator_visible(row.xmin, tx)
        && (row.xmax == 0 || (row.xmax != tx.txid
            && (row.xmax >= tx.xmax || tx.active.contains(row.xmax)))))
}
pub open spec fn first_visible(s: Image, p: u32, tx: Snapshot) -> Option<u32>
    recommends image_valid(s), p == 0 || s.rows.contains_key(p),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if !image_valid(s) || p == 0 || !s.rows.contains_key(p) { None }
    else if visible(s.rows[p], tx) { Some(p) }
    else { first_visible(s, s.rows[p].next, tx) }
}
pub open spec fn last_write(writes: Seq<PendingWrite>, id: usize, n: int) -> Option<u32>
    decreases n,
{
    if n <= 0 { None }
    else if writes[n-1].row_id == id { Some(writes[n-1].new_ptr) }
    else { last_write(writes, id, n-1) }
}
pub open spec fn transaction_valid(s: Image, tx: Transaction) -> bool {
    image_valid(s) && 0 < tx.txid < tx.snapshot_xmax
    && 0 < tx.snapshot_xmin <= tx.txid
    && forall|i: int| 0 <= i < tx.write_set.len() ==>
        tx.write_set[i].row_id < s.capacity && s.rows.contains_key(tx.write_set[i].new_ptr)
}
pub open spec fn selected_pointer(s: Image, tx: Transaction, id: usize) -> Option<u32> {
    let own = last_write(tx.write_set@, id, tx.write_set.len() as int);
    if own.is_some() { own } else { first_visible(s, s.heads[id], snapshot(tx)) }
}
pub open spec fn selected_value(s: Image, tx: Transaction, id: usize) -> Option<Option<usize>> {
    let p = selected_pointer(s, tx, id);
    if p.is_some() { Some(s.rows[p.unwrap()].value) } else { None }
}
pub open spec fn stored_snapshot_key(s:Image,tx:Transaction,id:usize) -> Option<usize> {
    let p=first_visible(s,s.heads[id],snapshot(tx));
    if p.is_some() {s.rows[p.unwrap()].value} else {None}
}
pub open spec fn transaction_view_same(a: Transaction, b: Transaction) -> bool {
    a.txid == b.txid && a.snapshot_xmin == b.snapshot_xmin && a.snapshot_xmax == b.snapshot_xmax
        && a.snapshot_active@ == b.snapshot_active@ && a.write_set@ == b.write_set@
}
pub open spec fn records_extend(before: Seq<ReadRecord>, after: Seq<ReadRecord>) -> bool {
    before.len() <= after.len()
        && forall|i:int| 0 <= i < before.len() ==> before[i] == after[i]
}
pub open spec fn recording(before: Seq<ReadRecord>, row_id: usize, row_ptr: u32, xmin: u64) -> Seq<ReadRecord> {
    if exists|i:int| 0 <= i < before.len() && before[i].row_ptr == row_ptr {
        before
    } else {
        before.push(ReadRecord { row_id, row_ptr, xmin })
    }
}
pub open spec fn observed_record(image: Image, tx: Transaction, entry: ReadRecord) -> bool {
    entry.row_id < image.capacity && image.rows.contains_key(entry.row_ptr)
        && first_visible(image, image.heads[entry.row_id], snapshot(tx)) == Some(entry.row_ptr)
        && entry.xmin == image.rows[entry.row_ptr].xmin
}
pub open spec fn records_have_provenance(image: Image, tx: Transaction,
    original: Seq<ReadRecord>, current: Seq<ReadRecord>) -> bool {
    forall|entry:ReadRecord| current.contains(entry) ==>
        original.contains(entry) || observed_record(image,tx,entry)
}

pub trait Storage {
    // A retained per-operation version-chain image. This does NOT assert that
    // the entire physical heap is frozen while a reader materializes candidates.
    // Native load-to-image correspondence and reclamation ownership stay open.
    spec fn image(&self) -> Image;
    fn ensure_open(&self, tx: &Transaction) -> Result<(), Error>;
    fn capacity(&self) -> (n: usize) ensures n == self.image().capacity;
    fn head(&self, id: usize) -> (r: Result<u32, Error>)
        requires id < self.image().capacity,
        ensures r.is_ok() ==> r.unwrap() == self.image().heads[id];
    fn resolve(&self, ptr: u32) -> (r: Result<Row, Error>)
        requires self.image().rows.contains_key(ptr),
        ensures r.is_ok() ==> r.unwrap() == self.image().rows[ptr];
    fn yield_now(&self);
}

pub fn active_contains(active: &Vec<u64>, id: u64) -> (r: bool)
    ensures r == active@.contains(id),
{
    let mut i = 0;
    while i < active.len()
        invariant i <= active.len(), forall|j:int| 0 <= j < i ==> active[j] != id,
        decreases active.len() - i,
    {
        if active[i] == id { return true; }
        i += 1;
    }
    false
}
pub fn is_visible(row: &Row, tx: &Transaction) -> (r: bool)
    ensures r == visible(*row, snapshot(*tx)),
{
    if row . xmin == tx . txid {
    return true ;
}
if row . xmin < tx . snapshot_xmin {
}
else if row . xmin >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , row . xmin ) {
    return false ;
}
let xmax = row . xmax ;
if xmax == 0 {
    return true ;
}
if xmax == tx . txid {
    return false ;
}
xmax >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , xmax )
}
pub fn row_locked_by_other_tx(row: &Row, txid: u64) -> (r: bool)
    ensures r == (row.locked && row.owner != txid),
{
    if ! row . locked {
    return false ;
}
let owner = row . owner ;
owner != txid
}
pub proof fn selected_pointer_valid(s: Image, tx: Snapshot, p: u32)
    requires image_valid(s), p == 0 || s.rows.contains_key(p),
    ensures first_visible(s,p,tx).is_some() ==> s.rows.contains_key(first_visible(s,p,tx).unwrap()),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if p != 0 && !visible(s.rows[p],tx) { selected_pointer_valid(s,tx,s.rows[p].next); }
}
pub fn find_visible_row_ptr<D: Storage>(driver: &D, tx: &Transaction, row_id: usize)
    -> (r: Result<Option<u32>, Error>)
    requires image_valid(driver.image()), row_id < driver.image().capacity,
    ensures r.is_ok() ==> r.unwrap() == first_visible(driver.image(), driver.image().heads[row_id], snapshot(*tx)),
        r.is_ok() && r.unwrap().is_some() ==> driver.image().rows.contains_key(r.unwrap().unwrap()),
{
    let mut head_offset = driver . head ( row_id ) ? ;
let mut steps : u32 = 0 ;

    while head_offset != EMPTY_PTR
        invariant image_valid(driver.image()), row_id < driver.image().capacity,
            head_offset == 0 || driver.image().rows.contains_key(head_offset),
            steps <= MAX_VISIBLE_CHAIN_STEPS,
            first_visible(driver.image(),head_offset,snapshot(*tx))
                == first_visible(driver.image(),driver.image().heads[row_id],snapshot(*tx)),
        decreases MAX_VISIBLE_CHAIN_STEPS + 1 - steps,
    {
     steps = steps + 1 ;
if steps > MAX_VISIBLE_CHAIN_STEPS {
    driver . yield_now ( ) ;
    return Err ( Error :: SerializationFailure ) ;
}
let row_ptr = head_offset ;
let row = driver . resolve ( row_ptr ) ? ;
if is_visible ( & row , tx ) {
    return Ok ( Some ( row_ptr ) ) ;
}
head_offset = row . next ;
}
Ok ( None )
}
pub fn latest_pending(writes: &Vec<PendingWrite>, row_id: usize) -> (r: Option<PendingWrite>)
    ensures r.is_some() ==> r.unwrap().row_id == row_id && writes@.contains(r.unwrap()),
        r.is_some() <==> last_write(writes@,row_id,writes.len() as int).is_some(),
        r.is_some() ==> r.unwrap().new_ptr == last_write(writes@,row_id,writes.len() as int).unwrap(),
{
    let mut i = writes.len();
    while i > 0
        invariant i <= writes.len(),
            last_write(writes@,row_id,writes.len() as int) == last_write(writes@,row_id,i as int),
        decreases i,
    {
        i -= 1;
        if writes[i].row_id == row_id { return Some(writes[i]); }
    }
    None
}
pub fn record_read(tx: &mut Transaction, row_id: usize, row_ptr: u32, xmin: u64)
    ensures transaction_view_same(*old(tx),*final(tx)),
        final(tx).read_set@ == recording(old(tx).read_set@,row_id,row_ptr,xmin),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        final(tx).read_set.len() <= old(tx).read_set.len() + 1,
        exists|i:int| 0 <= i < final(tx).read_set.len() && final(tx).read_set[i].row_ptr == row_ptr,
        forall|r:ReadRecord| old(tx).read_set@.contains(r) ==> final(tx).read_set@.contains(r),
{
    let row_offset = row_ptr ;
if read_already_recorded ( & tx . read_set , row_offset ) {
    return ;
}
tx . read_set . push ( ReadRecord {
    row_id , row_ptr , xmin : xmin ,
}
) ;
proof { assert(tx.read_set[tx.read_set.len() as int-1].row_ptr == row_ptr); }

}
pub fn read_already_recorded(reads:&Vec<ReadRecord>,ptr:u32) -> (found:bool)
    ensures found <==> exists|i:int| 0 <= i < reads.len() && reads[i].row_ptr == ptr,
{
    let mut i=0;
    while i < reads.len()
        invariant i <= reads.len(), forall|j:int| 0 <= j < i ==> reads[j].row_ptr != ptr,
        decreases reads.len()-i,
    {
        if reads[i].row_ptr == ptr {return true;}
        i+=1;
    }
    false
}
pub fn read<D: Storage>(driver: &D, tx: &mut Transaction, row_id: usize)
    -> (r: Result<Option<Option<usize>>, Error>)
    requires transaction_valid(driver.image(),*old(tx)),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        final(tx).read_set.len() <= old(tx).read_set.len() + 1,
        r.is_ok() ==> row_id < driver.image().capacity
            && r.unwrap() == selected_value(driver.image(),*old(tx),row_id),
        r.is_ok() && r.unwrap().is_some()
            && last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_none() ==>
            exists|i:int| 0<=i<final(tx).read_set.len()
                && final(tx).read_set[i].row_ptr == first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap(),
        r.is_ok() && r.unwrap().is_some()
            && last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_none() ==>
            final(tx).read_set@ == recording(old(tx).read_set@,row_id,
                first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap(),
                driver.image().rows[first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap()].xmin),
        r.is_err() || (r.is_ok() && (r.unwrap().is_none()
            || last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_some())) ==>
            final(tx).read_set@ == old(tx).read_set@,
        forall|entry:ReadRecord| old(tx).read_set@.contains(entry) ==> final(tx).read_set@.contains(entry),
{
    driver . ensure_open ( tx ) ? ;
if row_id >= driver . capacity ( ) {
    return Err ( Error :: RowOutOfBounds {
        row_id , capacity : driver . capacity ( ) ,
    }
    ) ;
}
if let Some ( pending ) = latest_pending ( & tx . write_set , row_id ) {
    let pending_row = driver . resolve ( pending . new_ptr ) ? ;
    return Ok ( Some ( pending_row . value ) ) ;
}
if let Some ( row_ptr ) = find_visible_row_ptr ( driver , tx , row_id ) ? {
    let row = driver . resolve ( row_ptr ) ? ;
    if row_locked_by_other_tx ( & row , tx . txid ) {
        driver . yield_now ( ) ;
        return Err ( Error :: SerializationFailure ) ;
    }
    let observed_xmin = row . xmin ;
    record_read ( tx , row_id , row_ptr , observed_xmin ) ;
    return Ok ( Some ( row . value ) ) ;
}
Ok ( None )
}
pub open spec fn read_conflict(row:Row,entry:ReadRecord,tx:Transaction) -> bool {
    row.xmin!=entry.xmin || (row.xmax!=0 && row.xmax!=tx.txid
        && (row.xmax>=tx.snapshot_xmax || tx.snapshot_active@.contains(row.xmax)))
}
pub open spec fn reads_conflict(image:Image,tx:Transaction) -> bool {
    exists|i:int| 0<=i<tx.read_set.len() && read_conflict(image.rows[tx.read_set[i].row_ptr],tx.read_set[i],tx)
}
pub fn has_serialization_conflict<D:Storage>(driver:&D,tx:&Transaction) -> (r:Result<bool,Error>)
    requires forall|i:int| 0<=i<tx.read_set.len() ==> driver.image().rows.contains_key(tx.read_set[i].row_ptr),
    ensures r.is_ok() ==> r.unwrap()==reads_conflict(driver.image(),*tx),
{
    
    let mut ri=0;
    while ri<tx.read_set.len()
        invariant ri<=tx.read_set.len(),
            forall|i:int| 0<=i<tx.read_set.len() ==> driver.image().rows.contains_key(tx.read_set[i].row_ptr),
            forall|i:int| 0<=i<ri ==> !read_conflict(driver.image().rows[tx.read_set[i].row_ptr],tx.read_set[i],*tx),
        decreases tx.read_set.len()-ri,
    {
        let read=&tx.read_set[ri];
     let row = driver . resolve ( read . row_ptr ) ? ;
if row . xmin != read . xmin {
    return Ok ( true ) ;
}
let xmax = row . xmax ;
if xmax == 0 || xmax == tx . txid {
    ri = ri + 1 ;
    continue ;
}
let committed_after_snapshot = xmax >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , xmax ) ;
if committed_after_snapshot {
    return Ok ( true ) ;
}

        ri += 1;
    }
     Ok ( false )
}
pub proof fn later_deletion_rejects_recorded_read(image:Image,tx:Transaction,i:int)
    requires 0<=i<tx.read_set.len(), image.rows.contains_key(tx.read_set[i].row_ptr),
        image.rows[tx.read_set[i].row_ptr].xmax!=0, image.rows[tx.read_set[i].row_ptr].xmax!=tx.txid,
        image.rows[tx.read_set[i].row_ptr].xmax>=tx.snapshot_xmax
            || tx.snapshot_active@.contains(image.rows[tx.read_set[i].row_ptr].xmax),
    ensures reads_conflict(image,tx),
{
    assert(read_conflict(image.rows[tx.read_set[i].row_ptr],tx.read_set[i],tx));
}

pub trait CandidateSet: Sized {
    spec fn contents(&self) -> Set<usize>;
    fn from_vec(rows: Vec<usize>) -> (s: Self) ensures s.contents() == rows@.to_set();
    fn insert(&mut self, row: usize) ensures final(self).contents() == old(self).contents().insert(row);
    fn into_vec(self) -> (rows: Vec<usize>)
        ensures rows@.to_set() == self.contents(),
            forall|i:int,j:int| 0 <= i < j < rows.len() ==> rows[i] < rows[j];
}
pub trait KeyPredicate {
    spec fn matches(&self, value: usize) -> bool;
    spec fn bucket(&self, value: usize) -> usize;
    fn evaluate(&self, value: &Option<usize>) -> (r: bool)
        ensures r == (value.is_some() && self.matches(value.unwrap()));
}
pub open spec fn own_ids(writes: Seq<PendingWrite>, n: int) -> Set<usize>
    decreases n,
{
    if n <= 0 { Set::empty() } else { own_ids(writes,n-1).insert(writes[n-1].row_id) }
}
pub open spec fn matches_row<P:KeyPredicate>(image: Image, tx: Transaction, p: P, id: usize) -> bool {
    id < image.capacity && selected_value(image,tx,id).is_some()
        && selected_value(image,tx,id).unwrap().is_some()
        && p.matches(selected_value(image,tx,id).unwrap().unwrap())
}
pub proof fn prefix_less(rows:Seq<usize>, n:int, id:usize)
    requires 0 <= n < rows.len(), rows.take(n).contains(id),
        forall|i:int,j:int| 0 <= i < j < rows.len() ==> rows[i] < rows[j],
    ensures id < rows[n],
{
    let i=choose|i:int| 0 <= i < rows.take(n).len() && rows.take(n)[i]==id;
    assert(rows[i]==id);
}
pub fn materialize<D:Storage,C:CandidateSet,P:KeyPredicate>(driver: &D, tx: &mut Transaction,
    candidates: Vec<usize>, predicate: &P) -> (r: Result<Vec<usize>,Error>)
    requires transaction_valid(driver.image(),*old(tx)),
        forall|id:usize| matches_row(driver.image(),*old(tx),*predicate,id) ==>
            candidates@.contains(id) || own_ids(old(tx).write_set@,old(tx).write_set.len() as int).contains(id),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        r.is_ok() ==> forall|id:usize| r.unwrap()@.contains(id)
            <==> matches_row(driver.image(),*old(tx),*predicate,id),
        r.is_ok() ==> forall|i:int,j:int| 0 <= i < j < r.unwrap().len() ==> r.unwrap()[i] < r.unwrap()[j],
{
    let ghost initial = *tx;
let ghost candidates_input = candidates;
let mut candidates = C :: from_vec ( candidates ) ;
let mut wi = 0 ;

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
     let mut result = Vec :: new ( ) ;

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
     if let Some ( value ) = read ( driver , tx , row_id ) ? {
    if predicate . evaluate ( & value ) {
        proof {
        assert forall|i:int| 0 <= i < result.len() implies result[i] < row_id by {
            assert(result@.contains(result[i]));
            assert(ordered@.take(ci as int).contains(result[i]));
            prefix_less(ordered@,ci as int,result[i]);
        }
    } result . push ( row_id ) ;
    }
}

        proof { assert(ordered@.take(ci as int+1) =~= ordered@.take(ci as int).push(row_id)); }
        ci += 1;
    }
     Ok ( result )
}

// A writer invisible to the retained snapshot may append a version and mark
// the predecessor deleted without changing that predecessor's visibility.
pub proof fn invisible_delete_preserves_visibility(row: Row, tx: Transaction, writer: u64)
    requires row.xmax == 0, writer != tx.txid,
        writer >= tx.snapshot_xmax || tx.snapshot_active@.contains(writer),
    ensures visible(row,snapshot(tx)) == visible(Row {xmax: writer,..row},snapshot(tx)),
{}
pub proof fn first_visible_frame(before:Image,after:Image,tx:Snapshot,ptr:u32)
    requires image_valid(before),image_valid(after),ptr==0 || before.rows.contains_key(ptr),
        forall|p:u32| #[trigger] before.rows.contains_key(p) ==> after.rows.contains_key(p)
            && before.rows[p].next==after.rows[p].next
            && visible(before.rows[p],tx)==visible(after.rows[p],tx),
    ensures first_visible(before,ptr,tx)==first_visible(after,ptr,tx),
    decreases if ptr==0 {0nat} else {before.rank[ptr]+1},
{
    if ptr!=0 && !visible(before.rows[ptr],tx) {
        first_visible_frame(before,after,tx,before.rows[ptr].next);
    }
}
pub proof fn invisible_head_append_preserves_snapshot(before:Image,after:Image,tx:Transaction,
    id:usize,ptr:u32,newrow:Row)
    requires image_valid(before),image_valid(after),id<before.capacity,after.capacity==before.capacity,
        before.heads[id]!=0,ptr!=0,!before.rows.contains_key(ptr),
        before.rows[before.heads[id]].xmax==0,
        newrow.xmin!=tx.txid,!creator_visible(newrow.xmin,snapshot(tx)),
        newrow.xmin>=tx.snapshot_xmax || tx.snapshot_active@.contains(newrow.xmin),
        newrow.next==before.heads[id],
        after.heads==before.heads.insert(id,ptr),
        after.rows==before.rows.insert(before.heads[id],Row{xmax:newrow.xmin,..before.rows[before.heads[id]]}).insert(ptr,newrow),
    ensures stored_snapshot_key(before,tx,id)==stored_snapshot_key(after,tx,id),
{
    let oldhead=before.heads[id];
    invisible_delete_preserves_visibility(before.rows[oldhead],tx,newrow.xmin);
    assert forall|p:u32| before.rows.contains_key(p) implies after.rows.contains_key(p)
        && before.rows[p].next==after.rows[p].next
        && visible(before.rows[p],snapshot(tx))==visible(after.rows[p],snapshot(tx)) by {
        if p==oldhead { assert(visible(before.rows[p],snapshot(tx))==visible(after.rows[p],snapshot(tx))); }
    }
    first_visible_frame(before,after,snapshot(tx),oldhead);
    selected_pointer_valid(before,snapshot(tx),oldhead);
    assert(!visible(newrow,snapshot(tx)));
    assert(first_visible(after,ptr,snapshot(tx))==first_visible(before,oldhead,snapshot(tx)));
    if first_visible(before,oldhead,snapshot(tx)).is_some() {
        let p=first_visible(before,oldhead,snapshot(tx)).unwrap();
        assert(before.rows.contains_key(p));
        assert(after.rows[p].value==before.rows[p].value);
    }
}
}

// Reproved in Verus; no unchecked transfer from Lean or TLC.
verus! {
pub struct Protocol {
    pub clock:u64, pub active:Set<u64>, pub ended:Set<u64>,
    pub published:Map<u64,u64>, pub observed:Map<u64,Set<u64>>,
}
pub enum Action { Start{txid:u64}, End{txid:u64}, Snapshot{reader:u64}, Publish{writer:u64,stamp:u64} }
pub open spec fn protocol_initial(p:Protocol) -> bool {
    p.clock>0 && p.active==Set::<u64>::empty() && p.ended==Set::<u64>::empty()
        && p.published==Map::<u64,u64>::empty() && p.observed==Map::<u64,Set<u64>>::empty()
}
pub open spec fn protocol_valid(p:Protocol) -> bool {
    p.clock>0 && p.active.disjoint(p.ended)
    && (forall|tx:u64| p.active.contains(tx) || p.ended.contains(tx) ==> 0<tx<p.clock)
    && (forall|writer:u64| #[trigger] p.published.contains_key(writer) ==> p.ended.contains(writer)
        && writer<p.published[writer]<p.clock)
    && (forall|reader:u64| p.observed.contains_key(reader) ==> 0<reader<p.clock)
    && (forall|reader:u64,writer:u64| p.observed.contains_key(reader) && p.observed[reader].contains(writer)
        && p.published.contains_key(writer) ==> reader<p.published[writer])
}
pub open spec fn protocol_step(a:Protocol,b:Protocol,action:Action) -> bool {
    match action {
        Action::Start{txid} => a.clock<=txid<u64::MAX
            && b==(Protocol{clock:(txid+1) as u64,active:a.active.insert(txid),..a}),
        Action::End{txid} => a.active.contains(txid)
            && b==(Protocol{active:a.active.remove(txid),ended:a.ended.insert(txid),..a}),
        Action::Snapshot{reader} => a.active.contains(reader)
            && b==(Protocol{observed:a.observed.insert(reader,a.active.remove(reader)),..a}),
        Action::Publish{writer,stamp} => a.ended.contains(writer) && !a.published.contains_key(writer)
            && a.clock<=stamp<u64::MAX
            && b==(Protocol{clock:(stamp+1) as u64,published:a.published.insert(writer,stamp),..a}),
    }
}
pub proof fn protocol_step_preserves(a:Protocol,b:Protocol,action:Action)
    requires protocol_valid(a),protocol_step(a,b,action),
    ensures protocol_valid(b),
{
    match action {
        Action::Start{txid} => {
            assert(!a.ended.contains(txid));
            assert(b.active.disjoint(b.ended));
        },
        Action::Snapshot{reader} => {
            assert forall|who:u64,writer:u64| b.observed.contains_key(who) && b.observed[who].contains(writer)
                && b.published.contains_key(writer) implies who<b.published[writer] by {
                if who==reader {
                    assert(a.active.contains(writer));
                    assert(a.ended.contains(writer));
                }
            }
        },
        _ => {},
    }
}
pub open spec fn protocol_history(states:Seq<Protocol>,actions:Seq<Action>) -> bool {
    states.len()==actions.len()+1 && protocol_initial(states[0])
        && forall|i:int| 0<=i<actions.len() ==> protocol_step(states[i],states[i+1],actions[i])
}
pub proof fn reachable_protocol_valid(states:Seq<Protocol>,actions:Seq<Action>,n:int)
    requires protocol_history(states,actions),0<=n<=actions.len(),
    ensures protocol_valid(states[n]),
    decreases n,
{
    if n>0 {
        reachable_protocol_valid(states,actions,n-1);
        protocol_step_preserves(states[n-1],states[n],actions[n-1]);
    }
}
pub struct Event { pub row:usize, pub before:Option<usize>, pub after:Option<usize>, pub creator:u64, pub stamp:u64 }
pub open spec fn key_matches<P:KeyPredicate>(p:P,key:Option<usize>) -> bool {
    key.is_some() && p.matches(key.unwrap())
}
pub open spec fn affects<P:KeyPredicate>(p:P,e:Event) -> bool {
    key_matches(p,e.before) || key_matches(p,e.after)
}
pub open spec fn event_buckets<P:KeyPredicate>(p:P,e:Event) -> Set<usize> {
    let old = if e.before.is_some() {Set::empty().insert(p.bucket(e.before.unwrap()))} else {Set::empty()};
    if e.after.is_some() {old.insert(p.bucket(e.after.unwrap()))} else {old}
}
pub open spec fn stamp_at<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,b:usize) -> u64
    decreases n,
{
    if n<=0 {initial[b]} else if event_buckets(p,events[n-1]).contains(b) {events[n-1].stamp}
    else {stamp_at(initial,events,n-1,p,b)}
}
pub open spec fn guarded_stamp_history<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P) -> bool {
    forall|i:int,b:usize| 0<=i<events.len() && event_buckets(p,events[i]).contains(b) ==>
        initial.contains_key(b) && stamp_at(initial,events,i,p,b)<=events[i].stamp
}
pub proof fn stamp_history_covers<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,i:int,b:usize)
    requires 0<=i<n<=events.len(),guarded_stamp_history(initial,events,p),event_buckets(p,events[i]).contains(b),
    ensures events[i].stamp<=stamp_at(initial,events,n,p,b),
    decreases n,
{
    if i<n-1 {
        stamp_history_covers(initial,events,n-1,p,i,b);
        if event_buckets(p,events[n-1]).contains(b) {
            assert(stamp_at(initial,events,n-1,p,b)<=events[n-1].stamp);
        }
    }
}
pub proof fn accepted_stamps_force_early_events<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P,
    buckets:Set<usize>,reader:u64)
    requires guarded_stamp_history(initial,events,p),
        forall|value:usize| p.matches(value) ==> buckets.contains(p.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial,events,events.len() as int,p,b)<reader,
    ensures forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<reader,
{
    assert forall|i:int| 0<=i<events.len() && affects(p,events[i]) implies events[i].stamp<reader by {
        let e=events[i];
        let value=if key_matches(p,e.before) {e.before.unwrap()} else {e.after.unwrap()};
        assert(p.matches(value));
        let bucket=p.bucket(value);
        assert(event_buckets(p,e).contains(bucket));
        stamp_history_covers(initial,events,events.len() as int,p,i,bucket);
        assert(buckets.contains(bucket));
        assert(stamp_at(initial,events,events.len() as int,p,bucket)<reader);
    }
}
pub open spec fn live_rows(initial:Map<usize,Option<usize>>, events:Seq<Event>,n:int) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else {live_rows(initial,events,n-1).insert(events[n-1].row,events[n-1].after)}
}
pub open spec fn snapshot_rows(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else if creator_visible(events[n-1].creator,s) {
        snapshot_rows(initial,events,n-1,s).insert(events[n-1].row,events[n-1].after)
    } else {snapshot_rows(initial,events,n-1,s)}
}
pub open spec fn postings(rows:Map<usize,Option<usize>>) -> Set<(usize,usize)> {
    rows.dom().filter(|id:usize| rows[id].is_some()).map(|id:usize| (rows[id].unwrap(),id))
}
pub open spec fn posting_step(before:Set<(usize,usize)>,e:Event) -> Set<(usize,usize)> {
    let removed = if e.before.is_some() {before.remove((e.before.unwrap(),e.row))} else {before};
    if e.after.is_some() {removed.insert((e.after.unwrap(),e.row))} else {removed}
}
pub open spec fn posting_replay(initial:Set<(usize,usize)>,events:Seq<Event>,n:int) -> Set<(usize,usize)>
    decreases n,
{
    if n<=0 {initial} else {posting_step(posting_replay(initial,events,n-1),events[n-1])}
}
pub open spec fn coherent(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int) -> bool {
    forall|i:int| 0<=i<n ==> initial.contains_key(events[i].row)
        && events[i].before==live_rows(initial,events,i)[events[i].row]
}
pub proof fn live_domain(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(), coherent(initial,events,n),
    ensures live_rows(initial,events,n).dom()==initial.dom(),
    decreases n,
{
    if n>0 {
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        assert(live_rows(initial,events,n).dom() =~= initial.dom());
    }
}
pub proof fn posting_update_exact(rows:Map<usize,Option<usize>>,e:Event)
    requires rows.contains_key(e.row),rows[e.row]==e.before,
    ensures posting_step(postings(rows),e)==postings(rows.insert(e.row,e.after)),
{
    assert forall|pair:(usize,usize)| posting_step(postings(rows),e).contains(pair)
        == postings(rows.insert(e.row,e.after)).contains(pair) by {
        posting_membership(rows,pair.0,pair.1);
        posting_membership(rows.insert(e.row,e.after),pair.0,pair.1);
    }
    assert(posting_step(postings(rows),e) =~= postings(rows.insert(e.row,e.after)));
}
pub proof fn posting_membership(rows:Map<usize,Option<usize>>,key:usize,id:usize)
    ensures postings(rows).contains((key,id)) <==> rows.contains_key(id) && rows[id]==Some(key),
{
    if postings(rows).contains((key,id)) {
        let r=choose|r:usize| rows.dom().filter(|r:usize|rows[r].is_some()).contains(r)
            && (rows[r].unwrap(),r)==(key,id);
        assert(r==id);
    } else if rows.contains_key(id) && rows[id]==Some(key) {
        assert(rows.dom().filter(|r:usize|rows[r].is_some()).contains(id));
    }
}
pub proof fn posting_history_exact(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(),coherent(initial,events,n),
    ensures posting_replay(postings(initial),events,n)==postings(live_rows(initial,events,n)),
    decreases n,
{
    if n>0 {
        posting_history_exact(initial,events,n-1);
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        posting_update_exact(live_rows(initial,events,n-1),events[n-1]);
    }
}
// These numeric facts are consequences of shared-clock/lifecycle history in
// the checked composition, not an assumption of historical candidate coverage.
pub open spec fn event_chronology(s:Snapshot,e:Event) -> bool {
    e.creator < e.stamp && (s.active.contains(e.creator) ==> e.stamp > s.txid)
}
pub proof fn event_chronology_from_history(states:Seq<Protocol>,actions:Seq<Action>,s:Snapshot,e:Event)
    requires protocol_history(states,actions),
        states.last().observed.contains_key(s.txid),s.active.to_set()==states.last().observed[s.txid],
        states.last().published.contains_key(e.creator),states.last().published[e.creator]==e.stamp,
    ensures event_chronology(s,e),
{
    reachable_protocol_valid(states,actions,actions.len() as int);
    let p=states.last();
    assert(e.creator<p.published[e.creator]);
    if s.active.contains(e.creator) {
        assert(p.observed[s.txid].contains(e.creator));
        assert(s.txid<p.published[e.creator]);
    }
}
pub proof fn early_publication_is_visible(s:Snapshot,e:Event)
    requires s.txid<s.xmax,event_chronology(s,e),e.stamp<s.txid,
    ensures creator_visible(e.creator,s),
{}
pub proof fn accepted_replay_matches<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot,p:P)
    requires 0<=n<=events.len(),coherent(initial,events,n),s.txid<s.xmax,
        forall|i:int| 0<=i<n ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<n && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) ==>
        key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]),
    decreases n,
{
    if n>0 {
        accepted_replay_matches(initial,events,n-1,s,p);
        let e=events[n-1];
        if affects(p,e) {early_publication_is_visible(s,e);}
        assert forall|id:usize| initial.contains_key(id) implies
            key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]) by {
            if id==e.row && !creator_visible(e.creator,s) {
                assert(!affects(p,e));
                assert(e.before==live_rows(initial,events,n-1)[id]);
            }
        }
    }
}
pub open spec fn raw_candidates<P:KeyPredicate>(p:P,index:Set<(usize,usize)>) -> Set<usize> {
    index.filter(|pair:(usize,usize)|p.matches(pair.0)).map(|pair:(usize,usize)|pair.1)
}
pub proof fn accepted_history_has_candidates<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,s:Snapshot,p:P)
    requires coherent(initial,events,events.len() as int),s.txid<s.xmax,
        forall|i:int| 0<=i<events.len() ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) ==>
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id),
{
    posting_history_exact(initial,events,events.len() as int);
    live_domain(initial,events,events.len() as int);
    accepted_replay_matches(initial,events,events.len() as int,s,p);
    assert forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) implies
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id) by {
        let value=live_rows(initial,events,events.len() as int)[id].unwrap();
        posting_membership(live_rows(initial,events,events.len() as int),value,id);
        assert(postings(live_rows(initial,events,events.len() as int)).contains((value,id)));
        assert(posting_replay(postings(initial),events,events.len() as int)
            .filter(|pair:(usize,usize)|p.matches(pair.0)).contains((value,id)));
    }
}
pub proof fn absent_own_row_has_no_write(writes:Seq<PendingWrite>,id:usize,n:int)
    requires 0<=n<=writes.len(),!own_ids(writes,n).contains(id),
    ensures last_write(writes,id,n).is_none(),
    decreases n,
{
    if n>0 {absent_own_row_has_no_write(writes,id,n-1);}
}
pub proof fn matched_row_comes_from_snapshot_or_own<P:KeyPredicate>(s:Image,tx:Transaction,p:P,id:usize)
    requires matches_row(s,tx,p,id),!own_ids(tx.write_set@,tx.write_set.len() as int).contains(id),
    ensures key_matches(p,stored_snapshot_key(s,tx,id)),
{
    absent_own_row_has_no_write(tx.write_set@,id,tx.write_set.len() as int);
}
// Retained MVCC cells correspond to the visible-event history, while raw lookup
// enumerates current maintained postings. These are distinct storage mappings;
// neither is a premise that historical candidates are already complete.
pub open spec fn history_maps_snapshot(image:Image,tx:Transaction,initial:Map<usize,Option<usize>>,events:Seq<Event>) -> bool {
    forall|id:usize| id<image.capacity ==> initial.contains_key(id)
        && stored_snapshot_key(image,tx,id)==snapshot_rows(initial,events,events.len() as int,snapshot(tx))[id]
}
pub fn materialize_after_checked_history<D:Storage,C:CandidateSet,P:KeyPredicate>(driver:&D,tx:&mut Transaction,
    candidates:Vec<usize>,predicate:&P,
    Ghost(initial):Ghost<Map<usize,Option<usize>>>,Ghost(events):Ghost<Seq<Event>>,
    Ghost(protocol):Ghost<Seq<Protocol>>,Ghost(actions):Ghost<Seq<Action>>,
    Ghost(initial_stamps):Ghost<Map<usize,u64>>,Ghost(buckets):Ghost<Set<usize>>)
    -> (r:Result<Vec<usize>,Error>)
    requires transaction_valid(driver.image(),*old(tx)),
        coherent(initial,events,events.len() as int),history_maps_snapshot(driver.image(),*old(tx),initial,events),
        protocol_history(protocol,actions),protocol.last().observed.contains_key(old(tx).txid),
        old(tx).snapshot_active@.to_set()==protocol.last().observed[old(tx).txid],
        forall|i:int| 0<=i<events.len() ==> protocol.last().published.contains_key(events[i].creator)
            && protocol.last().published[events[i].creator]==events[i].stamp,
        guarded_stamp_history(initial_stamps,events,*predicate),
        forall|value:usize| predicate.matches(value) ==> buckets.contains(predicate.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial_stamps,events,events.len() as int,*predicate,b)<old(tx).txid,
        raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).subset_of(candidates@.to_set()),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        r.is_ok() ==> forall|id:usize| r.unwrap()@.contains(id) <==> matches_row(driver.image(),*old(tx),*predicate,id),
        r.is_ok() ==> forall|i:int,j:int| 0<=i<j<r.unwrap().len() ==> r.unwrap()[i]<r.unwrap()[j],
{
    proof {
        assert forall|i:int| 0<=i<events.len() implies event_chronology(snapshot(*tx),events[i]) by {
            event_chronology_from_history(protocol,actions,snapshot(*tx),events[i]);
        }
        accepted_stamps_force_early_events(initial_stamps,events,*predicate,buckets,tx.txid);
        accepted_history_has_candidates(initial,events,snapshot(*tx),*predicate);
        assert forall|id:usize| matches_row(driver.image(),*tx,*predicate,id) implies
            candidates@.contains(id) || own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) by {
            if !own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) {
                matched_row_comes_from_snapshot_or_own(driver.image(),*tx,*predicate,id);
                assert(initial.contains_key(id));
                assert(key_matches(*predicate,snapshot_rows(initial,events,events.len() as int,snapshot(*tx))[id]));
                assert(raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).contains(id));
                assert(candidates@.to_set().contains(id));
            }
        }
    }
    materialize::<D,C,P>(driver,tx,candidates,predicate)
}
pub struct EqualsKey {pub key:usize}
impl KeyPredicate for EqualsKey {
    open spec fn matches(&self,value:usize) -> bool {value==self.key}
    open spec fn bucket(&self,_value:usize) -> usize {0}
    fn evaluate(&self,value:&Option<usize>) -> (r:bool) {
        match value {Some(key)=>*key==self.key,None=>false}
    }
}
pub proof fn query_contract_has_success_witness() {
    let p0=Protocol{clock:1,active:Set::empty(),ended:Set::empty(),published:Map::empty(),observed:Map::empty()};
    let p1=Protocol{clock:2,active:Set::empty().insert(1),..p0};
    let p2=Protocol{active:Set::empty(),ended:Set::empty().insert(1),..p1};
    let p3=Protocol{clock:3,published:Map::empty().insert(1,2),..p2};
    let p4=Protocol{clock:4,active:Set::empty().insert(3),..p3};
    let p5=Protocol{observed:Map::empty().insert(3,Set::empty()),..p4};
    let states=seq![p0,p1,p2,p3,p4,p5];
    let actions=seq![Action::Start{txid:1},Action::End{txid:1},Action::Publish{writer:1,stamp:2},
        Action::Start{txid:3},Action::Snapshot{reader:3}];
    assert(p1.active.remove(1) =~= Set::<u64>::empty());
    assert(p4.active.remove(3) =~= Set::<u64>::empty());
    assert forall|i:int| 0<=i<5 implies protocol_step(states[i],states[i+1],actions[i]) by {
        if i==0 {} else if i==1 {} else if i==2 {} else if i==3 {} else {assert(i==4);}
    }
    assert(protocol_history(states,actions));
    reachable_protocol_valid(states,actions,5);
    let s=Snapshot{txid:3,xmin:3,xmax:4,active:Seq::empty()};
    assert(s.active.to_set() =~= Set::<u64>::empty());
    let e=Event{row:0,before:Some(10),after:Some(42),creator:1,stamp:2};
    let initial=Map::empty().insert(0,Some(10));
    let events=seq![e];
    let p=EqualsKey{key:42};
    event_chronology_from_history(states,actions,s,e);
    assert(coherent(initial,events,1));
    assert(guarded_stamp_history(Map::empty().insert(0,0),events,p));
    assert(stamp_at(Map::empty().insert(0,0),events,1,p,0)<s.txid);
    accepted_history_has_candidates(initial,events,s,p);
    assert(snapshot_rows(initial,events,1,s)[0]==Some(42));
    assert(raw_candidates(p,posting_replay(postings(initial),events,1)).contains(0));
}
}
 }
mod capture { // Generated from native index_lookup; see generate.py for the boundary.
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
        result.is_ok() ==> final(tx).index_conflict == old(tx).index_conflict,
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
            tx.index_conflict == old(tx).index_conflict,
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
use vstd::prelude::*;
use ownership::IndexAtomics;
use lookup::{Storage, KeyPredicate};
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub struct History {
    pub initial: Map<usize, Option<usize>>,
    pub events: Seq<lookup::Event>,
    pub protocol: Seq<lookup::Protocol>,
    pub actions: Seq<lookup::Action>,
    pub initial_stamps: Map<usize, u64>,
}
pub open spec fn history_valid<P: KeyPredicate>(h: History, image: lookup::Image,
    tx: lookup::Transaction, p: P, bucket: usize) -> bool {
    lookup::transaction_valid(image, tx)
    && lookup::coherent(h.initial, h.events, h.events.len() as int)
    && lookup::history_maps_snapshot(image, tx, h.initial, h.events)
    && lookup::protocol_history(h.protocol, h.actions)
    && h.protocol.last().observed.contains_key(tx.txid)
    && tx.snapshot_active@.to_set() == h.protocol.last().observed[tx.txid]
    && (forall|i: int| 0 <= i < h.events.len() ==>
        h.protocol.last().published.contains_key(h.events[i].creator)
        && h.protocol.last().published[h.events[i].creator] == h.events[i].stamp)
    && lookup::guarded_stamp_history(h.initial_stamps, h.events, p)
    && h.initial_stamps.contains_key(bucket)
    && (forall|value: usize| p.matches(value) ==> p.bucket(value) == bucket)
}
pub open spec fn represented_stamp<P: KeyPredicate>(h: History, p: P, bucket: usize) -> u64 {
    lookup::stamp_at(h.initial_stamps, h.events, h.events.len() as int, p, bucket)
}

// Native storage/history correspondence is deliberately an explicit primitive.
// A projection is selected by the actual acquired lease, not an API-entry view
// frozen across waiting. It contains no historical-candidate-completeness claim.
// The retained image may describe later physical versions; its snapshot keys
// must map to this capture history. Arbitrary native history mapping is open.
pub trait SlicePrimitives<P: KeyPredicate>: ownership::IndexAtomics + lookup::Storage {
    // This adapter represents one concrete transaction snapshot. Without this
    // context, an arbitrary snapshot_active vector with the same txid could
    // demand contradictory observed sets from the same guarded history.
    spec fn operation_snapshot(&self) -> lookup::Snapshot;
    spec fn operation_index_offset(&self) -> usize;
    spec fn key_bucket(&self, header: usize, value: usize) -> usize;
    spec fn history(&self, lease: &ownership::ReadLease) -> History;
    proof fn guarded_correspondence(&self, tx: lookup::Transaction, p: P,
        lease: &ownership::ReadLease, tracked authority: &ownership::Authority)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).0 == self.arena(),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            lookup::transaction_valid(self.image(), tx),
            lookup::snapshot(tx) == self.operation_snapshot(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
            forall|value: usize| p.matches(value) ==> p.bucket(value) == ownership::lease_key(lease).2,
        ensures history_valid(self.history(lease), self.image(), tx, p, ownership::lease_key(lease).2);
    fn guarded_stamp(&self, p: &P, lease: &ownership::ReadLease,
        Tracked(authority): Tracked<&ownership::Authority>) -> (r: Result<u64, capture::Error>)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
        ensures r.is_ok() ==> r.unwrap() == represented_stamp(self.history(lease), *p, ownership::lease_key(lease).2),
            r.is_err() ==> r == Err(capture::Error::Index);
    fn raw_lookup(&self, p: &P, lease: &ownership::ReadLease,
        Tracked(authority): Tracked<&ownership::Authority>) -> (r: Result<Vec<usize>, lookup::Error>)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
        ensures r.is_ok() ==> lookup::raw_candidates(*p, lookup::posting_replay(
            lookup::postings(self.history(lease).initial), self.history(lease).events,
            self.history(lease).events.len() as int)).subset_of(r.unwrap()@.to_set());
}

pub struct BorrowedIndex<'a, D: SlicePrimitives<P>, P: KeyPredicate> {
    pub driver: &'a D,
    pub query: &'a P,
    pub lease: &'a ownership::ReadLease,
    pub authority: Tracked<&'a ownership::Authority>,
    pub offset: usize,
    pub binding: usize,
    pub binding_count: usize,
    pub bucket: usize,
}
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> BorrowedIndex<'a, D, P> {
    pub open spec fn owned(&self) -> bool {
        ownership::valid(self.authority@) && ownership::authorized(self.authority@, self.lease)
            && ownership::lease_key(self.lease) == (self.driver.arena(), self.offset, self.bucket)
            && self.binding < self.driver.registry().len()
            && self.driver.registry().len() == self.binding_count
            && self.driver.registry()[self.binding as int] == self.offset
            && self.offset == self.driver.operation_index_offset()
            && (forall|value:usize| self.query.bucket(value) == self.driver.key_bucket(self.offset, value))
    }
    pub open spec fn stamp(&self) -> u64 {
        represented_stamp(self.driver.history(self.lease), *self.query, self.bucket)
    }
}
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> capture::CaptureIndex for BorrowedIndex<'a, D, P> {
    open spec fn offset(&self) -> usize { self.offset }
    open spec fn stamps(&self) -> Map<usize, u64> { Map::empty().insert(self.bucket, self.stamp()) }
    open spec fn held(&self) -> Set<usize> {
        if self.owned() { Set::empty().insert(self.bucket) } else { Set::empty() }
    }
    fn header_offset(&self) -> (r: usize) { self.offset }
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, capture::Error>) {
        self.driver.guarded_stamp(self.query, self.lease, Tracked(*self.authority.borrow()))
    }
}

// Read-only validator view. Permissions are justified by the borrowed opaque
// lease and authority; the pair fields cannot manufacture a held guard.
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> predicate::Primitives for BorrowedIndex<'a, D, P> {
    open spec fn state(&self) -> predicate::State {
        predicate::State { arena: self.driver.arena(),
            bindings: Map::empty().insert(self.offset, self.binding),
            binding_count: self.binding_count,
            key_buckets: Map::empty(),
            stamps: Map::empty().insert((self.binding, self.bucket), self.stamp()),
            held: if self.owned() { Set::empty().insert((self.binding, self.bucket)) } else { Set::empty() },
            clock: 0, reserved_stamp: 0, reservations: Seq::empty(), deregistered: false }
    }
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>) {
        if offset == self.offset && self.binding < self.binding_count {
            Ok(self.binding)
        } else { Err(predicate::Error::IndexBindingsIncomplete) }
    }
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>) {
        Err(predicate::Error::Other)
    }
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>) {
        match self.driver.guarded_stamp(self.query, self.lease, Tracked(*self.authority.borrow())) {
            Ok(stamp) => Ok(stamp), Err(_) => Err(predicate::Error::Other),
        }
    }
    fn reserve_stamp(&mut self) -> (stamp: u64) {
        proof { assert(false); }
        0
    }
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64)
        -> (r: Result<(), predicate::Error>)
    {
        proof { assert(false); }
        Err(predicate::Error::Other)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum SliceError { Acquire, Capture, RawLookup, Materialize, Validate }
pub struct Outcome {
    pub rows: Vec<usize>, pub conflict: bool,
    pub predicate_conflict: bool, pub row_conflict: bool,
    pub captured_stamp: u64, pub validation_stamp: u64,
}

pub proof fn unique_one_lease(leases: Seq<ownership::ReadLease>)
    requires leases.len() == 1,
    ensures forall|i: int, j: int| 0 <= i < j < leases.len() ==>
        ownership::lease_key(&leases[i]) != ownership::lease_key(&leases[j]),
{}

// A first indexed lookup with one query bucket. Prefix routing/guard/capture/
// raw/drop/materialization order is checked against the whole native body by
// generate.py. Validation additionally represents its later commit phase.
pub fn indexed_read_then_validate<D: SlicePrimitives<P>, V: lookup::Storage, P: KeyPredicate, C: lookup::CandidateSet>(
    driver: &D, validation_driver: &V, tx: &mut lookup::Transaction, query: &P,
    binding: usize, binding_count: usize, offset: usize, bucket: usize,
    Tracked(authority): Tracked<&mut ownership::Authority>) -> (result: Result<Outcome, SliceError>)
    requires ownership::valid(old(authority)), ownership::physical_domain(driver, old(authority)),
        binding < driver.registry().len(),
        driver.registry().len() == binding_count, driver.registry()[binding as int] == offset,
        offset == driver.operation_index_offset(),
        lookup::transaction_valid(driver.image(), *old(tx)),
        lookup::snapshot(*old(tx)) == driver.operation_snapshot(),
        forall|value:usize| query.bucket(value) == driver.key_bucket(offset, value),
        driver.image().rows.dom().subset_of(validation_driver.image().rows.dom()),
        forall|entry:lookup::ReadRecord| old(tx).read_set@.contains(entry) ==>
            validation_driver.image().rows.contains_key(entry.row_ptr),
        forall|value: usize| query.matches(value) ==> query.bucket(value) == bucket,
    ensures ownership::valid(final(authority)), lookup::transaction_view_same(*old(tx), *final(tx)),
        result.is_ok() ==> forall|id: usize| result.unwrap().rows@.contains(id)
            <==> lookup::matches_row(driver.image(), *old(tx), *query, id),
        result.is_ok() ==> result.unwrap().captured_stamp < old(tx).txid,
        result.is_ok() ==> result.unwrap().predicate_conflict ==
            (result.unwrap().validation_stamp != result.unwrap().captured_stamp
                || result.unwrap().validation_stamp >= old(tx).txid),
        result.is_ok() && !result.unwrap().predicate_conflict ==>
            result.unwrap().row_conflict == lookup::reads_conflict(validation_driver.image(), *final(tx)),
        result.is_ok() ==> result.unwrap().conflict ==
            (result.unwrap().predicate_conflict || lookup::reads_conflict(validation_driver.image(), *final(tx))),
{
    let ghost initial_authority = *authority;
    let mut guards = Vec::new();
    let lease = match ownership::acquire_index_bucket(driver, binding, bucket, &guards, Tracked(&mut *authority)) {
        Ok(lease) => lease, Err(_) => return Err(SliceError::Acquire),
    };
    guards.push(lease);
    proof { unique_one_lease(guards@); }
    let ghost captured_history = driver.history(&guards[0]);
    let mut captured = capture::Transaction { txid: tx.txid, index_conflict: false, index_reads: Vec::new() };
    let mut buckets = Vec::new();
    buckets.push(bucket);
    let captured_result;
    let raw_result;
    {
        let index = BorrowedIndex { driver, query, lease: &guards[0], authority: Tracked(&*authority), offset, binding, binding_count, bucket };
        proof {
            driver.guarded_correspondence(*tx, *query, &guards[0], &*authority);
            assert(history_valid(captured_history, driver.image(), *tx, *query, bucket));
        }
        captured_result = capture::capture_dependencies(&index, &mut captured, &buckets);
        if captured_result.is_ok() {
            proof {
                assert(capture::captured(captured.index_reads@, capture::CaptureIndex::offset(&index),
                    buckets[0], capture::CaptureIndex::stamps(&index)[buckets[0]]));
                assert(capture::captured(captured.index_reads@, offset, bucket,
                    represented_stamp(captured_history, *query, bucket)));
                let member = choose|i: int| 0 <= i < captured.index_reads.len()
                    && captured.index_reads[i] == (capture::IndexRead { index_offset: offset, bucket,
                        stamp: represented_stamp(captured_history, *query, bucket) });
                assert(captured.index_reads.len() == 1);
                assert(member == 0);
            }
            raw_result = driver.raw_lookup(query, &guards[0], Tracked(&*authority));
        } else { raw_result = Err(lookup::Error::Storage); }
    }
    ownership::release_all(driver, guards, Tracked(&mut *authority));
    proof { ownership::domain_survives_interference(driver, &initial_authority, authority); }
    match captured_result { Err(_) => return Err(SliceError::Capture), Ok(()) => {}, }
    let candidates = match raw_result { Err(_) => return Err(SliceError::RawLookup), Ok(rows) => rows, };
    proof {
        assert(captured.index_reads.len() == 1);
        assert(captured.index_reads[0] == (capture::IndexRead { index_offset: offset, bucket,
            stamp: represented_stamp(captured_history, *query, bucket) }));
    }
    let captured_stamp = captured.index_reads[0].stamp;
    let rows = match lookup::materialize_after_checked_history::<D, C, P>(driver, tx, candidates, query,
        Ghost(captured_history.initial), Ghost(captured_history.events), Ghost(captured_history.protocol),
        Ghost(captured_history.actions), Ghost(captured_history.initial_stamps), Ghost(Set::empty().insert(bucket))) {
        Ok(rows) => rows, Err(_) => return Err(SliceError::Materialize),
    };
    let mut validation_guards = Vec::new();
    let validation_lease = match ownership::acquire_index_bucket(driver, binding, bucket, &validation_guards, Tracked(&mut *authority)) {
        Ok(lease) => lease, Err(_) => return Err(SliceError::Acquire),
    };
    validation_guards.push(validation_lease);
    proof { unique_one_lease(validation_guards@); }
    let mut reads = Vec::new();
    let read = &captured.index_reads[0];
    reads.push(predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp });
    let validation_tx = predicate::OccTransaction { txid: captured.txid, index_conflict: captured.index_conflict, index_reads: reads };
    let validation;
    let validation_stamp;
    {
        let index = BorrowedIndex { driver, query, lease: &validation_guards[0], authority: Tracked(&*authority), offset, binding, binding_count, bucket };
        validation_stamp = match driver.guarded_stamp(query, &validation_guards[0], Tracked(&*authority)) {
            Ok(stamp) => Some(stamp), Err(_) => None,
        };
        proof {
            reveal_with_fuel(predicate::read_keys, 2);
            reveal(predicate::reads_good);
            assert(predicate::read_keys(predicate::Primitives::state(&index), validation_tx.index_reads@,
                validation_tx.index_reads.len() as int).subset_of(predicate::Primitives::state(&index).held));
        }
        validation = predicate::index_read_conflict(&index, &validation_tx);
    }
    proof {
        assert forall|i:int| 0<=i<tx.read_set.len() implies
            validation_driver.image().rows.contains_key(tx.read_set[i].row_ptr) by {
            let entry = tx.read_set[i];
            assert(tx.read_set@.contains(entry));
            if !old(tx).read_set@.contains(entry) {
                assert(lookup::observed_record(driver.image(), *old(tx), entry));
            }
        }
    }
    let row_validation = match &validation {
        Ok(true) => Ok(false),
        Ok(false) => lookup::has_serialization_conflict(validation_driver, tx),
        Err(_) => Err(lookup::Error::Storage),
    };
    ownership::release_all(driver, validation_guards, Tracked(&mut *authority));
    let stamp = match validation_stamp { Some(stamp) => stamp, None => return Err(SliceError::Validate), };
    let predicate_conflict = match validation { Ok(conflict) => conflict, Err(_) => return Err(SliceError::Validate), };
    let row_conflict = match row_validation {
        Ok(conflict) => conflict, Err(_) => return Err(SliceError::Validate),
    };
    let conflict = predicate_conflict || row_conflict;
    Ok(Outcome { rows, conflict, predicate_conflict, row_conflict, captured_stamp, validation_stamp: stamp })
}

// Satisfiability witness for the newly introduced history/retained-image seam.
// Guard ownership has independent executable handoff witnesses; this lemma
// does not claim a concrete native Storage/Scheduler implementation.
pub proof fn empty_history_has_success_witness(tx: lookup::Transaction)
    requires tx.txid == 1, tx.snapshot_xmin == 1, tx.snapshot_xmax == 2,
        tx.snapshot_active@ == Seq::<u64>::empty(), tx.write_set@ == Seq::<lookup::PendingWrite>::empty(),
    ensures exists|h:History, image:lookup::Image| history_valid(h,image,tx,lookup::EqualsKey{key:42},0)
        && represented_stamp(h,lookup::EqualsKey{key:42},0) < tx.txid,
{
    let p0=lookup::Protocol{clock:1,active:Set::empty(),ended:Set::empty(),published:Map::empty(),observed:Map::empty()};
    let p1=lookup::Protocol{clock:2,active:Set::empty().insert(1),..p0};
    let p2=lookup::Protocol{observed:Map::empty().insert(1,Set::empty()),..p1};
    let states=seq![p0,p1,p2];
    let actions=seq![lookup::Action::Start{txid:1},lookup::Action::Snapshot{reader:1}];
    assert(p1.active.remove(1) =~= Set::<u64>::empty());
    assert forall|i:int| 0<=i<2 implies lookup::protocol_step(states[i],states[i+1],actions[i]) by {
        if i==0 {} else {assert(i==1);}
    }
    assert(tx.snapshot_active@.to_set() =~= Set::<u64>::empty());
    let h=History{initial:Map::empty(),events:Seq::empty(),protocol:states,actions,
        initial_stamps:Map::empty().insert(0,0)};
    let image=lookup::Image{heads:Map::empty(),rows:Map::empty(),rank:Map::empty(),capacity:0};
    assert(history_valid(h,image,tx,lookup::EqualsKey{key:42},0));
    assert(represented_stamp(h,lookup::EqualsKey{key:42},0)==0);
}
}
