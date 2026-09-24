use crate::lookup;
use lookup::{Image, Row, Snapshot, Error};
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub open spec fn reachable(s: Image, p: u32) -> Set<u32>
    decreases if p == 0 || !s.rows.contains_key(p) { 0nat } else { s.rank[p] + 1 },
{
    if !lookup::image_valid(s) || p == 0 || !s.rows.contains_key(p) { Set::empty() }
    else { reachable(s, s.rows[p].next).insert(p) }
}
pub open spec fn prefix(s: Image, p: u32, tx: Snapshot) -> Set<u32>
    decreases if p == 0 || !s.rows.contains_key(p) { 0nat } else { s.rank[p] + 1 },
{
    if !lookup::image_valid(s) || p == 0 || !s.rows.contains_key(p) { Set::empty() }
    else if lookup::visible(s.rows[p], tx) { Set::empty().insert(p) }
    else { prefix(s, s.rows[p].next, tx).insert(p) }
}
pub open spec fn eligible(row: Row, horizon: u64) -> bool {
    row.xmax != 0 && row.xmax < horizon && !row.locked
}
pub open spec fn snapshot_well_formed(tx: Snapshot) -> bool {
    0 < tx.xmin <= tx.txid < tx.xmax
    && forall|i: int| 0 <= i < tx.active.len() ==> tx.xmin <= tx.active[i]
}
pub open spec fn admissible(s: Image, tx: Snapshot, horizon: u64) -> bool {
    snapshot_well_formed(tx) && horizon <= tx.xmin
    // A protected reader has not published its own private versions into this
    // public chain. The native own-creator visibility bypass needs this premise.
    && forall|p:u32| s.rows.contains_key(p) ==> s.rows[p].xmin != tx.txid
}
pub open spec fn data_same(a: Row, b: Row) -> bool {
    a.xmin == b.xmin && a.xmax == b.xmax && a.value == b.value
        && a.locked == b.locked && a.owner == b.owner
}
pub open spec fn metadata_same(a: Image, b: Image) -> bool {
    a.heads == b.heads && a.capacity == b.capacity && a.rank == b.rank
    && a.rows.dom() == b.rows.dom()
    && forall|p:u32| a.rows.contains_key(p) ==> data_same(a.rows[p],b.rows[p])
}
pub open spec fn spliced(s: Image, prev: u32, next: u32) -> Image {
    Image { rows: s.rows.insert(prev, Row { next, ..s.rows[prev] }), ..s }
}
pub proof fn reachable_rank(s: Image, p: u32, q: u32)
    requires lookup::image_valid(s), p == 0 || s.rows.contains_key(p), reachable(s,p).contains(q),
    ensures s.rows.contains_key(q), q != 0, s.rank[q] <= s.rank[p],
        q != p ==> s.rank[q] < s.rank[p],
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if q != p { reachable_rank(s,s.rows[p].next,q); }
}
pub proof fn reachable_unroll(s: Image, p: u32)
    requires lookup::image_valid(s), p != 0, s.rows.contains_key(p),
    ensures reachable(s,p) == reachable(s,s.rows[p].next).insert(p),
        !reachable(s,s.rows[p].next).contains(p),
{
    if reachable(s,s.rows[p].next).contains(p) { reachable_rank(s,s.rows[p].next,p); }
}
pub proof fn splice_valid(s: Image, prev: u32, curr: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr,
    ensures lookup::image_valid(spliced(s,prev,s.rows[curr].next)),
        metadata_same(s,spliced(s,prev,s.rows[curr].next)),
{
    let t=spliced(s,prev,s.rows[curr].next);
    reveal(lookup::image_valid);
    assert(t.capacity==s.capacity);
    assert(t.heads==s.heads);
    assert(t.rows.dom() =~= s.rows.dom());
    assert forall|id:usize| id<t.capacity implies t.heads.contains_key(id)
        && (#[trigger] t.heads[id]==0 || t.rows.contains_key(t.heads[id])) by {
        assert(id < s.capacity);
        assert(s.heads[id]==0 || s.rows.contains_key(s.heads[id]));
        assert(s.heads.contains_key(id));
    };
    assert forall|p:u32| s.rows.contains_key(p) implies data_same(s.rows[p],t.rows[p]) by {};
    assert forall|p:u32| #[trigger] t.rows.contains_key(p) implies p != 0 && t.rank.contains_key(p)
        && (t.rows[p].next == 0 || t.rows.contains_key(t.rows[p].next)
            && t.rank[t.rows[p].next] < t.rank[p]) by {
        if p==prev && s.rows[curr].next != 0 { assert(s.rank[s.rows[curr].next] < s.rank[curr] < s.rank[prev]); }
    }
    assert(lookup::image_valid(t));
}
pub proof fn below_cut_unchanged(s: Image, prev: u32, curr: u32, p: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, p == 0 || (s.rows.contains_key(p) && s.rank[p] < s.rank[prev]),
    ensures reachable(spliced(s,prev,s.rows[curr].next),p) == reachable(s,p),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    splice_valid(s,prev,curr);
    if p != 0 { below_cut_unchanged(s,prev,curr,s.rows[p].next); }
}
pub proof fn splice_reachable(s: Image, prev: u32, curr: u32, p: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, p != 0, s.rows.contains_key(p), reachable(s,p).contains(prev),
    ensures reachable(spliced(s,prev,s.rows[curr].next),p) == reachable(s,p).remove(curr),
        !reachable(spliced(s,prev,s.rows[curr].next),p).contains(curr),
    decreases s.rank[p],
{
    let t=spliced(s,prev,s.rows[curr].next);
    splice_valid(s,prev,curr);
    reachable_rank(s,p,prev);
    if p == prev {
        below_cut_unchanged(s,prev,curr,s.rows[curr].next);
        reachable_unroll(s,curr);
        assert(reachable(t,p) =~= reachable(s,p).remove(curr));
    } else {
        reachable_rank(s,s.rows[p].next,prev);
        splice_reachable(s,prev,curr,s.rows[p].next);
        assert(p != curr);
        assert(reachable(t,p) =~= reachable(s,p).remove(curr));
    }
}
pub proof fn eligible_is_invisible(s: Image, p: u32, tx: Snapshot, horizon: u64)
    requires s.rows.contains_key(p), admissible(s,tx,horizon), eligible(s.rows[p],horizon),
    ensures !lookup::visible(s.rows[p],tx),
{
    if tx.active.contains(s.rows[p].xmax) {
        let i=choose|i:int| 0<=i<tx.active.len() && tx.active[i]==s.rows[p].xmax;
    }
}
pub proof fn splice_first_visible(s: Image, prev: u32, curr: u32, p: u32, tx: Snapshot)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, !lookup::visible(s.rows[curr],tx),
        p == 0 || s.rows.contains_key(p),
    ensures lookup::first_visible(spliced(s,prev,s.rows[curr].next),p,tx) == lookup::first_visible(s,p,tx),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    let t=spliced(s,prev,s.rows[curr].next);
    splice_valid(s,prev,curr);
    if p != 0 && !lookup::visible(s.rows[p],tx) {
        if p==prev {
            splice_first_visible(s,prev,curr,s.rows[curr].next,tx);
            reveal_with_fuel(lookup::first_visible,2);
        } else { splice_first_visible(s,prev,curr,s.rows[p].next,tx); }
    }
}
pub proof fn prefix_edges(s: Image, p: u32, tx: Snapshot, q: u32)
    requires lookup::image_valid(s), p == 0 || s.rows.contains_key(p), prefix(s,p,tx).contains(q),
        lookup::first_visible(s,p,tx).is_some(),
    ensures s.rows.contains_key(q), reachable(s,p).contains(q),
        lookup::first_visible(s,p,tx) != Some(q) ==> s.rows[q].next != 0
            && prefix(s,p,tx).contains(s.rows[q].next),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if q != p {
        assert(!lookup::visible(s.rows[p],tx));
        prefix_edges(s,s.rows[p].next,tx,q);
    } else if !lookup::visible(s.rows[p],tx) {
        reveal_with_fuel(lookup::first_visible,2);
        assert(s.rows[p].next != 0);
        assert(prefix(s,s.rows[p].next,tx).contains(s.rows[p].next));
    }
}
pub proof fn selection_is_visible(s:Image,p:u32,tx:Snapshot)
    requires lookup::image_valid(s), p==0 || s.rows.contains_key(p),
    ensures lookup::first_visible(s,p,tx).is_some() ==>
        s.rows.contains_key(lookup::first_visible(s,p,tx).unwrap())
        && lookup::visible(s.rows[lookup::first_visible(s,p,tx).unwrap()],tx),
    decreases if p==0 {0nat} else {s.rank[p]+1},
{
    if p!=0 && !lookup::visible(s.rows[p],tx) { selection_is_visible(s,s.rows[p].next,tx); }
}
pub open spec fn prefix_ineligible(s:Image,row_id:usize,tx:Snapshot,horizon:u64) -> bool {
    lookup::first_visible(s,s.heads[row_id],tx).is_some()
    && forall|p:u32| prefix(s,s.heads[row_id],tx).contains(p) ==> !eligible(s.rows[p],horizon)
}
pub open spec fn prefix_preserved(a:State,b:State,row_id:usize,tx:Snapshot) -> bool {
    b.recycled.disjoint(prefix(a.image,a.image.heads[row_id],tx))
    && forall|p:u32| prefix(a.image,a.image.heads[row_id],tx).contains(p) ==>
        b.image.rows.contains_key(p) && data_same(a.image.rows[p],b.image.rows[p])
        && (lookup::first_visible(a.image,a.image.heads[row_id],tx)!=Some(p) ==>
            a.image.rows[p].next==b.image.rows[p].next)
}
pub proof fn prefix_reflexive(s:State,row_id:usize,tx:Snapshot)
    requires lookup::image_valid(s.image),row_id<s.image.capacity,
        lookup::first_visible(s.image,s.image.heads[row_id],tx).is_some(),
        s.recycled.disjoint(reachable(s.image,s.image.heads[row_id])),
    ensures prefix_preserved(s,s,row_id,tx),
{
    assert forall|p:u32| prefix(s.image,s.image.heads[row_id],tx).contains(p) implies !s.recycled.contains(p) by {
        prefix_edges(s.image,s.image.heads[row_id],tx,p);
    }
    assert forall|p:u32| prefix(s.image,s.image.heads[row_id],tx).contains(p) implies
        s.image.rows.contains_key(p) && data_same(s.image.rows[p],s.image.rows[p]) by {
        prefix_edges(s.image,s.image.heads[row_id],tx,p);
    }
}

pub struct State { pub image: Image, pub recycled: Set<u32> }
pub struct Reclaimed { pub row_id: usize, pub reclaimed_value: Option<usize>, pub live_head_value: Option<Option<usize>> }
pub open spec fn report_provenance(s:Image,removed:Set<u32>,reports:Seq<Reclaimed>,row_id:usize) -> bool {
    forall|i:int| 0<=i<reports.len() ==> reports[i].row_id==row_id
        && reports[i].live_head_value==Some(s.rows[s.heads[row_id]].value)
        && exists|p:u32| removed.contains(p) && s.rows.contains_key(p) && s.rows[p].value==reports[i].reclaimed_value
}
pub trait Vacuum {
    spec fn state(&self) -> State;
    spec fn partition_held(&self,row_id:usize) -> bool;
    spec fn no_errors(&self) -> bool;
    fn head(&self,row_id:usize) -> (r:Result<u32,Error>)
        requires self.partition_held(row_id), row_id<self.state().image.capacity,
        ensures self.no_errors() ==> r.is_ok(), r.is_ok() ==> r.unwrap()==self.state().image.heads[row_id];
    fn resolve(&self,p:u32) -> (r:Result<Row,Error>)
        requires self.state().image.rows.contains_key(p),
        ensures self.no_errors() ==> r.is_ok(), r.is_ok() ==> r.unwrap()==self.state().image.rows[p];
    fn store_next(&mut self,prev:u32,next:u32,row_id:usize)
        requires old(self).partition_held(row_id), old(self).state().image.rows.contains_key(prev),
        ensures final(self).state().image==spliced(old(self).state().image,prev,next),
            final(self).state().recycled==old(self).state().recycled,
            final(self).no_errors()==old(self).no_errors(),
            final(self).partition_held(row_id);
    fn recycle(&mut self,row_id:usize,p:u32) -> (r:Result<(),Error>)
        requires old(self).partition_held(row_id), old(self).state().image.rows.contains_key(p),
            !old(self).state().image.rows[p].locked,
            !reachable(old(self).state().image,old(self).state().image.heads[row_id]).contains(p),
            !old(self).state().recycled.contains(p),
        ensures final(self).state().image==old(self).state().image,
            old(self).no_errors() ==> r.is_ok(), final(self).no_errors()==old(self).no_errors(),
            final(self).state().recycled==if r.is_ok() {old(self).state().recycled.insert(p)} else {old(self).state().recycled},
            final(self).partition_held(row_id);
}
pub open spec fn safe_result(a:State,b:State,row_id:usize,tx:Snapshot) -> bool {
    lookup::image_valid(b.image) && metadata_same(a.image,b.image)
    && lookup::first_visible(a.image,a.image.heads[row_id],tx)==lookup::first_visible(b.image,b.image.heads[row_id],tx)
    && a.recycled.subset_of(b.recycled)
    && b.recycled.disjoint(reachable(b.image,b.image.heads[row_id]))
    && forall|p:u32| b.recycled.contains(p) && !a.recycled.contains(p) ==>
        reachable(a.image,a.image.heads[row_id]).contains(p) && !a.image.rows[p].locked
        && lookup::first_visible(a.image,a.image.heads[row_id],tx) != Some(p)
}
pub proof fn safe_reflexive(s:State,row_id:usize,tx:Snapshot)
    requires lookup::image_valid(s.image),row_id<s.image.capacity,
        s.recycled.disjoint(reachable(s.image,s.image.heads[row_id])),
    ensures safe_result(s,s,row_id,tx),
{}

pub fn vacuum_row_acquired<D:Vacuum>(driver:&mut D,row_id:usize,global_xmin:u64,Ghost(tx):Ghost<Snapshot>)
    -> (result:Result<Vec<Reclaimed>,Error>)
    requires old(driver).partition_held(row_id), row_id<old(driver).state().image.capacity,
        lookup::image_valid(old(driver).state().image), admissible(old(driver).state().image,tx,global_xmin),
        old(driver).state().recycled.disjoint(reachable(old(driver).state().image,old(driver).state().image.heads[row_id])),
    ensures final(driver).partition_held(row_id), safe_result(old(driver).state(),final(driver).state(),row_id,tx),
        final(driver).no_errors()==old(driver).no_errors(), old(driver).no_errors() ==> result.is_ok(),
        prefix_ineligible(old(driver).state().image,row_id,tx,global_xmin) ==>
            prefix_preserved(old(driver).state(),final(driver).state(),row_id,tx),
        result.is_ok() ==> final(driver).state().recycled.difference(old(driver).state().recycled).len()==result.unwrap().len(),
        result.is_ok() ==> report_provenance(old(driver).state().image,
            final(driver).state().recycled.difference(old(driver).state().recycled),result.unwrap()@,row_id),
        result.is_ok() ==> reachable(final(driver).state().image,final(driver).state().image.heads[row_id])
            == reachable(old(driver).state().image,old(driver).state().image.heads[row_id])
                .difference(final(driver).state().recycled.difference(old(driver).state().recycled)),
        result.is_ok() ==> forall|p:u32| final(driver).state().recycled.contains(p) && !old(driver).state().recycled.contains(p)
            <==> reachable(old(driver).state().image,old(driver).state().image.heads[row_id]).contains(p)
                && p!=old(driver).state().image.heads[row_id] && eligible(old(driver).state().image.rows[p],global_xmin),
        result.is_ok() ==> forall|p:u32| reachable(final(driver).state().image,final(driver).state().image.heads[row_id]).contains(p)
            && p != final(driver).state().image.heads[row_id] ==> !eligible(final(driver).state().image.rows[p],global_xmin),
{
    /* NATIVE_VACUUM_ROW */
}

// Executable consistency model for the primitive contracts. It exercises the
// actual extracted loop; it is not the unsafe native allocator implementation.
pub struct Memory {
    pub head:u32, pub one:Row, pub two:Row, pub three:Row, pub returned:Vec<u32>,
}
impl Vacuum for Memory {
    open spec fn state(&self) -> State {
        State { image:Image { capacity:1, heads:Map::empty().insert(0,self.head),
            rows:Map::empty().insert(1,self.one).insert(2,self.two).insert(3,self.three),
            rank:Map::empty().insert(1,1nat).insert(2,2nat).insert(3,3nat) },
            recycled:self.returned@.to_set() }
    }
    open spec fn partition_held(&self,row_id:usize) -> bool { true }
    open spec fn no_errors(&self) -> bool { true }
    fn head(&self,row_id:usize) -> (r:Result<u32,Error>) { Ok(self.head) }
    fn resolve(&self,p:u32) -> (r:Result<Row,Error>) {
        if p==1 { Ok(self.one) } else if p==2 { Ok(self.two) } else { Ok(self.three) }
    }
    fn store_next(&mut self,prev:u32,next:u32,row_id:usize) {
        let ghost before=self.state();
        if prev==1 { self.one.next=next; } else if prev==2 { self.two.next=next; } else { self.three.next=next; }
        proof { assert(self.state().image.rows =~= spliced(before.image,prev,next).rows); }
    }
    fn recycle(&mut self,row_id:usize,p:u32) -> (r:Result<(),Error>) {
        self.returned.push(p); Ok(())
    }
}
pub fn retained_anchor_and_later_reclaim_witness() -> (result:bool)
    ensures result,
{
    let mut memory=Memory { head:3,
        one:Row {xmin:1,xmax:2,next:0,value:Some(10),locked:false,owner:0},
        two:Row {xmin:2,xmax:9,next:1,value:Some(20),locked:false,owner:0},
        three:Row {xmin:9,xmax:0,next:2,value:Some(30),locked:false,owner:0}, returned:Vec::new() };
    let ghost reader=Snapshot {txid:4,xmin:4,xmax:5,active:Seq::empty()};
    proof {
        assert(lookup::image_valid(memory.state().image));
        assert(admissible(memory.state().image,reader,4));
        reveal_with_fuel(lookup::first_visible,4);
        reveal_with_fuel(reachable,4);
        reveal_with_fuel(prefix,4);
        assert(lookup::first_visible(memory.state().image,3,reader)==Some(2));
        assert(prefix_ineligible(memory.state().image,0,reader,4));
    }
    let ghost initial=memory.state();
    proof { assert(initial.recycled =~= Set::<u32>::empty()); }
    let first=vacuum_row_acquired(&mut memory,0,4,Ghost(reader));
    proof {
        reveal_with_fuel(reachable,4);
        assert(memory.state().recycled =~= Set::empty().insert(1));
        assert(memory.two.next==0);
        assert(memory.three.next==2);
        assert(memory.state().recycled.difference(Set::empty()) =~= Set::empty().insert(1u32));
        assert(Set::<u32>::empty().insert(1).len()==1);
        assert(first.is_ok());
        assert(first.unwrap().len()==memory.state().recycled.difference(initial.recycled).len());
        assert(first.unwrap().len()==1);
    }
    let ghost later=Snapshot {txid:12,xmin:12,xmax:13,active:Seq::empty()};
    proof { assert(admissible(memory.state().image,later,12)); }
    let second=vacuum_row_acquired(&mut memory,0,12,Ghost(later));
    proof {
        assert(memory.state().recycled =~= Set::empty().insert(1).insert(2));
        assert(memory.three.next==0);
        assert(first.is_ok() && second.is_ok());
        assert(memory.state().recycled.difference(Set::empty().insert(1u32)) =~= Set::empty().insert(2u32));
        assert(Set::<u32>::empty().insert(2).len()==1);
        assert(first.unwrap().len()==1);
        assert(second.unwrap().len()==1);
    }
    first.is_ok() && second.is_ok() && memory.one.value==Some(10) && memory.two.value==Some(20)
        && memory.three.next==0 && first.unwrap().len()==1 && second.unwrap().len()==1
}

pub fn locked_version_survives_witness() -> (result:bool)
    ensures result,
{
    let mut memory=Memory { head:3,
        one:Row {xmin:1,xmax:2,next:0,value:Some(10),locked:true,owner:7},
        two:Row {xmin:2,xmax:9,next:1,value:Some(20),locked:false,owner:0},
        three:Row {xmin:9,xmax:0,next:2,value:Some(30),locked:false,owner:0}, returned:Vec::new() };
    let ghost reader=Snapshot {txid:4,xmin:4,xmax:5,active:Seq::empty()};
    proof { assert(lookup::image_valid(memory.state().image)); assert(admissible(memory.state().image,reader,4));
        reveal_with_fuel(reachable,4); }
    let ghost initial=memory.state();
    proof { assert(initial.recycled =~= Set::<u32>::empty()); }
    let rows=vacuum_row_acquired(&mut memory,0,4,Ghost(reader));
    proof {
        assert(memory.state().recycled =~= Set::<u32>::empty());
        assert(memory.state().recycled.difference(Set::empty()) =~= Set::<u32>::empty());
        assert(Set::<u32>::empty().len()==0);
        assert(rows.is_ok());
        assert(rows.unwrap().len()==memory.state().recycled.difference(initial.recycled).len());
        assert(rows.is_ok() && rows.unwrap().len()==0);
        reveal_with_fuel(reachable,4);
        assert(reachable(memory.state().image,3) =~= Set::empty().insert(1).insert(2).insert(3));
        assert(memory.two.next==1);
    }
    rows.is_ok() && rows.unwrap().len()==0 && memory.one.locked && memory.two.next==1
}
}
