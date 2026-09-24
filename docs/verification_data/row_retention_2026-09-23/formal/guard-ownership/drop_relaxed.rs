// Generated from native ShmMutex CAS/Drop and index selection; see generate.py.
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
    driver.store(physical, 0 , Ordering :: Relaxed, Tracked(&mut *authority));
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
