// Generated from native guard acquisition; see generate.py.
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
guards . push ( acquire_index_bucket ( driver , 0 , * bucket ) ? ) ;
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
