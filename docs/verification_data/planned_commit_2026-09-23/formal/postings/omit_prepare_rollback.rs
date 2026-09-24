// Generated from native posting preparation/rollback/removal. See generate.py.
// Conditional posting-set contracts for actual native preparation/rollback/
// source-removal algorithms. The primitive pointer operations remain unproved.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub type Posting = (usize, usize, usize);
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error { Index, Primitive }
#[derive(Clone, Copy)]
pub struct IndexChange { pub binding: usize, pub row_id: usize, pub before: Option<usize>, pub after: Option<usize> }
pub struct State {
    // Operation-local posting projection under the required publication guards.
    pub postings: Set<Posting>,
    pub binding_count: usize,
    pub held: Set<(usize, usize)>,
    pub poisoned: bool,
}
pub open spec fn posting(c: IndexChange, key: usize) -> Posting { (c.binding, key, c.row_id) }
pub open spec fn option_posting(c: IndexChange, key: Option<usize>) -> Set<Posting> {
    match key { Some(k) => Set::empty().insert(posting(c, k)), None => Set::empty() }
}
pub open spec fn destinations(changes: Seq<IndexChange>, n: int) -> Set<Posting>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { destinations(changes, n - 1).union(option_posting(changes[n - 1], changes[n - 1].after)) }
}
pub open spec fn sources(changes: Seq<IndexChange>, n: int) -> Set<Posting>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { sources(changes, n - 1).union(option_posting(changes[n - 1], changes[n - 1].before)) }
}
pub open spec fn recorded(changes: Seq<IndexChange>, indices: Seq<usize>) -> Set<Posting>
    decreases indices.len(),
{
    if indices.len() == 0 { Set::empty() }
    else { recorded(changes, indices.drop_last()).union(
        option_posting(changes[indices.last() as int], changes[indices.last() as int].after)) }
}
pub open spec fn change_valid(s: State, c: IndexChange) -> bool {
    c.binding < s.binding_count
    && (c.before.is_some() ==> s.held.contains((c.binding, c.before.unwrap())))
    && (c.after.is_some() ==> s.held.contains((c.binding, c.after.unwrap())))
}
pub open spec fn valid(s: State, changes: Seq<IndexChange>) -> bool {
    forall|i: int| 0 <= i < changes.len() ==> change_valid(s, changes[i])
}
pub open spec fn unique_destinations(changes: Seq<IndexChange>) -> bool {
    forall|i: int, j: int| 0 <= i < j < changes.len()
        && changes[i].after.is_some() && changes[j].after.is_some() ==>
            posting(changes[i], changes[i].after.unwrap()) != posting(changes[j], changes[j].after.unwrap())
}
pub open spec fn valid_indices(changes: Seq<IndexChange>, indices: Seq<usize>) -> bool {
    indices.no_duplicates()
    && forall|i: int| 0 <= i < indices.len() ==> indices[i] < changes.len()
        && changes[indices[i] as int].after.is_some()
}
pub open spec fn prefix_record(changes: Seq<IndexChange>, indices: Seq<usize>, n: int) -> bool {
    valid_indices(changes, indices)
    && (forall|i: int| 0 <= i < indices.len() ==> indices[i] < n)
    && (forall|j: int| 0 <= j < n ==> indices.contains(j as usize) == changes[j].after.is_some())
    && recorded(changes, indices) == destinations(changes, n)
}
pub open spec fn stable(a: State, b: State) -> bool {
    b.binding_count == a.binding_count && b.held == a.held
    && (a.poisoned ==> b.poisoned)
}
pub open spec fn removed_only(before: Set<Posting>, after: Set<Posting>, owned: Set<Posting>) -> bool {
    after.subset_of(before) && before.difference(owned).subset_of(after)
}

pub trait PostingPrimitives {
    spec fn state(&self) -> State;
    // Ownership is explicit: insertion requires a genuinely absent posting.
    // The primitive's failure frame is an assumption on native index insertion.
    fn transactional_insert(&mut self, binding: usize, key: usize, row: usize) -> (r: Result<(), Error>)
        requires binding < old(self).state().binding_count,
            old(self).state().held.contains((binding, key)),
            !old(self).state().postings.contains((binding, key, row)),
        ensures final(self).state() == (State {
            postings: if r.is_ok() { old(self).state().postings.insert((binding, key, row)) } else { old(self).state().postings },
            ..old(self).state()
        });
    fn transactional_remove(&mut self, binding: usize, key: &usize, row: &usize) -> (r: Result<(), Error>)
        requires binding < old(self).state().binding_count, old(self).state().held.contains((binding, *key)),
        ensures final(self).state().binding_count == old(self).state().binding_count,
            final(self).state().held == old(self).state().held,
            final(self).state().poisoned == old(self).state().poisoned,
            r.is_ok() ==> final(self).state().postings == old(self).state().postings.remove((binding, *key, *row)),
            r.is_err() ==> final(self).state().postings == old(self).state().postings
                || final(self).state().postings == old(self).state().postings.remove((binding, *key, *row));
    fn poison_indexes(&mut self)
        ensures final(self).state() == (State { poisoned: true, ..old(self).state() });
}

pub proof fn destination_member(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= i < n <= changes.len(), changes[i].after.is_some(),
    ensures destinations(changes, n).contains(posting(changes[i], changes[i].after.unwrap())),
    decreases n,
{
    if i < n - 1 { destination_member(changes, i, n - 1); }
}

pub proof fn source_member(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= i < n <= changes.len(), changes[i].before.is_some(),
    ensures sources(changes, n).contains(posting(changes[i], changes[i].before.unwrap())),
    decreases n,
{
    if i < n - 1 { source_member(changes, i, n - 1); }
}

pub proof fn destination_prefix_subset(changes: Seq<IndexChange>, n: int, m: int)
    requires 0 <= n <= m <= changes.len(),
    ensures destinations(changes, n).subset_of(destinations(changes, m)),
    decreases m - n,
{
    if n < m { destination_prefix_subset(changes, n, m - 1); }
}

pub proof fn restoration_frame(base: Set<Posting>, owned: Set<Posting>, after: Set<Posting>)
    requires base.disjoint(owned), removed_only(base.union(owned), after, owned),
    ensures base.subset_of(after), after.subset_of(base.union(owned)),
        after == base.union(owned).difference(owned) ==> after == base,
{
    assert(base.union(owned).difference(owned) =~= base);
}

pub proof fn destination_new(changes: Seq<IndexChange>, n: int)
    requires 0 <= n < changes.len(), unique_destinations(changes), changes[n].after.is_some(),
    ensures !destinations(changes, n).contains(posting(changes[n], changes[n].after.unwrap())),
    decreases n,
{
    if n > 0 {
        destination_not_prior(changes, n, n);
    }
}
pub proof fn destination_not_prior(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= n <= i < changes.len(), unique_destinations(changes), changes[i].after.is_some(),
    ensures !destinations(changes, n).contains(posting(changes[i], changes[i].after.unwrap())),
    decreases n,
{
    if n > 0 { destination_not_prior(changes, i, n - 1); }
}

pub proof fn recorded_push(changes: Seq<IndexChange>, indices: Seq<usize>, idx: usize)
    ensures recorded(changes, indices.push(idx)) == recorded(changes, indices)
        .union(option_posting(changes[idx as int], changes[idx as int].after)),
{
    assert(indices.push(idx).drop_last() == indices);
}

pub proof fn prefix_record_push(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < changes.len(), n <= usize::MAX, prefix_record(changes, indices, n), changes[n].after.is_some(),
    ensures prefix_record(changes, indices.push(n as usize), n + 1),
{
    recorded_push(changes, indices, n as usize);
    reveal_with_fuel(destinations, 2);
    assert forall|i: int, j: int| 0 <= i < j < indices.push(n as usize).len()
        implies #[trigger] indices.push(n as usize)[i] != #[trigger] indices.push(n as usize)[j] by {
        if j < indices.len() { assert(indices[i] != indices[j]); }
    }
    assert(indices.push(n as usize).no_duplicates());
    assert forall|j: int| 0 <= j < n + 1 implies indices.push(n as usize).contains(j as usize) == changes[j].after.is_some() by {
        if j < n { assert(indices.contains(j as usize) == changes[j].after.is_some()); }
    }
}

pub proof fn prefix_record_skip(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < changes.len(), n <= usize::MAX, prefix_record(changes, indices, n), changes[n].after.is_none(),
    ensures prefix_record(changes, indices, n + 1),
{
    reveal_with_fuel(destinations, 2);
    assert(destinations(changes, n + 1) =~= destinations(changes, n));
    assert(!indices.contains(n as usize)) by {
        if indices.contains(n as usize) {
            let i = choose|i: int| 0 <= i < indices.len() && indices[i] == n as usize;
            assert(indices[i] < n);
        }
    }
}

pub proof fn recorded_member(changes: Seq<IndexChange>, indices: Seq<usize>, i: int)
    requires 0 <= i < indices.len(),
    ensures option_posting(changes[indices[i] as int], changes[indices[i] as int].after)
        .subset_of(recorded(changes, indices)),
    decreases indices.len(),
{
    if i < indices.len() - 1 { recorded_member(changes, indices.drop_last(), i); }
}

pub proof fn recorded_suffix(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < indices.len(),
    ensures recorded(changes, indices.subrange(n, indices.len() as int)) ==
        recorded(changes, indices.subrange(n + 1, indices.len() as int))
        .union(option_posting(changes[indices[n] as int], changes[indices[n] as int].after)),
    decreases indices.len() - n,
{
    if n + 1 < indices.len() {
        recorded_suffix(changes, indices.drop_last(), n);
        assert(indices.subrange(n, indices.len() as int).drop_last() == indices.drop_last().subrange(n, indices.len() as int - 1));
        assert(indices.subrange(n + 1, indices.len() as int).drop_last() == indices.drop_last().subrange(n + 1, indices.len() as int - 1));
    }
    reveal_with_fuel(recorded, 2);
    assert(recorded(changes, indices.subrange(n, indices.len() as int)) =~=
        recorded(changes, indices.subrange(n + 1, indices.len() as int))
        .union(option_posting(changes[indices[n] as int], changes[indices[n] as int].after)));
}

pub fn rollback_index_destinations<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>, inserted: &Vec<usize>)
    -> (result: Result<(), Error>)
    requires valid(old(driver).state(), changes@), valid_indices(changes@, inserted@),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.difference(recorded(changes@, inserted@)),
        result.is_err() ==> final(driver).state().poisoned,
        removed_only(old(driver).state().postings, final(driver).state().postings, recorded(changes@, inserted@)),
{
    let ghost initial = driver.state(); let mut rollback_error = None ;

        let mut ri = inserted.len();
        proof { assert(recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int)) =~= Set::<Posting>::empty()); }
        while ri > 0
            invariant ri <= inserted.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                valid_indices(changes@, inserted@),
                removed_only(initial.postings, driver.state().postings, recorded(changes@, inserted@)),
                rollback_error.is_none() ==> driver.state().postings == initial.postings.difference(
                    recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int))),
            decreases ri,
        {
            ri = ri - 1;
            let idx = &inserted[ri];
            proof {
                recorded_suffix(changes@, inserted@, ri as int);
                recorded_member(changes@, inserted@, ri as int);
                assert(recorded(changes@, inserted@).contains(posting(changes@[*idx as int], changes@[*idx as int].after.unwrap())));
            }
     let previous = & changes [ * idx ] ;
if let Err ( undo ) = driver . transactional_remove ( previous . binding , previous . after . as_ref ( ) . expect ( "inserted destination" ) , & previous . row_id , ) {
    rollback_error = Some ( undo ) ;
}

        }
        proof { assert(inserted@.subrange(0, inserted.len() as int) == inserted@); }
     if let Some ( undo ) = rollback_error {
    driver . poison_indexes ( ) ;
    return Err ( Error :: Index ) ;
}
Ok ( ( ) )
}

pub fn prepare_index_destinations<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<Vec<usize>, Error>)
    requires valid(old(driver).state(), changes@), unique_destinations(changes@),
        old(driver).state().postings.disjoint(destinations(changes@, changes.len() as int)),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.union(destinations(changes@, changes.len() as int)),
        result.is_ok() ==> prefix_record(changes@, result->Ok_0@, changes.len() as int),
        result.is_err() ==> final(driver).state().postings == old(driver).state().postings || final(driver).state().poisoned,
        old(driver).state().postings.subset_of(final(driver).state().postings),
        final(driver).state().postings.subset_of(old(driver).state().postings.union(destinations(changes@, changes.len() as int))),
{
    let ghost initial = driver.state(); let mut inserted = Vec :: new ( ) ;

        let mut change_idx: usize = 0;
        proof { assert(recorded(changes@, inserted@) =~= Set::<Posting>::empty()); }
        while change_idx < changes.len()
            invariant change_idx <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                unique_destinations(changes@),
                initial.postings.disjoint(destinations(changes@, changes.len() as int)),
                driver.state().postings == initial.postings.union(destinations(changes@, change_idx as int)),
                prefix_record(changes@, inserted@, change_idx as int),
            decreases changes.len() - change_idx,
        {
            let change = &changes[change_idx];
            let ghost previous_indices = inserted@;
            proof {
                destination_prefix_subset(changes@, change_idx as int, changes.len() as int);
                assert(initial.postings.disjoint(destinations(changes@, change_idx as int)));
                if change.after.is_some() {
                    destination_member(changes@, change_idx as int, changes.len() as int);
                    destination_new(changes@, change_idx as int);
                }
            }
     if let Some ( after ) = & change . after {
    let index = change . binding ;
    if let Err ( err ) = driver . transactional_insert ( index , after . clone ( ) , change . row_id ) {
        if let Err ( undo ) = Ok :: < ( ) , Error > ( ( ) ) {
            proof { restoration_frame(initial.postings, recorded(changes@, inserted@), driver.state().postings); } return Err ( Error :: Index ) ;
        }
        proof { restoration_frame(initial.postings, recorded(changes@, inserted@), driver.state().postings); } return Err ( err ) ;
    }
    proof { recorded_push(changes@, previous_indices, change_idx); } inserted . push ( change_idx ) ;
}

            proof {
                if change.after.is_some() { prefix_record_push(changes@, previous_indices, change_idx as int); }
                else { prefix_record_skip(changes@, previous_indices, change_idx as int); }
                reveal_with_fuel(destinations, 2);
            }
            change_idx = change_idx + 1;
        }
     Ok ( inserted )
}

pub fn remove_index_sources<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<(), Error>)
    requires valid(old(driver).state(), changes@),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.difference(sources(changes@, changes.len() as int)),
        result.is_err() ==> final(driver).state().poisoned,
        removed_only(old(driver).state().postings, final(driver).state().postings, sources(changes@, changes.len() as int)),
{
    let ghost initial = driver.state(); 
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                driver.state().postings == initial.postings.difference(sources(changes@, ci as int)),
                removed_only(initial.postings, driver.state().postings, sources(changes@, changes.len() as int)),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
            proof { if change.before.is_some() { source_member(changes@, ci as int, changes.len() as int); } }
     if let Some ( before ) = & change . before {
    if let Err ( err ) = driver . transactional_remove ( change . binding , before , & change . row_id ) {
        driver . poison_indexes ( ) ;
        return Err ( Error :: Index ) ;
    }
}

            proof { reveal_with_fuel(sources, 2); }
            ci = ci + 1;
        }
     Ok ( ( ) )
}
}
