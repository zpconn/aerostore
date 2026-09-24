use crate::{lookup,ordinary,publication,postings,DataState,Plan,OrdinaryPlan};
use publication::PublicationStorage;
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

// The same PublicationStorage object is passed to validation and publication.
// Holding the row partition makes its image stable across these shared reads.
// Each primitive exposes a single native field; no primitive supplies base_valid.
pub trait GuardedRead:ordinary::OrdinaryStorage {
    fn load_head(&self,id:usize)->(head:u32)
        requires id<self.image().capacity,exists|p:u32| self.authorized(id,p),
        ensures head==self.image().heads[id];
    fn load_xmax(&self,ptr:u32)->(xmax:u64)
        requires self.image().rows.contains_key(ptr),
        ensures xmax==self.image().rows[ptr].xmax;
}
pub open spec fn base_valid(image:lookup::Image,write:ordinary::Write)->bool {
    image.heads[write.row_id]==write.base_offset
    && (write.base_offset==0 || image.rows.contains_key(write.base_offset)
        && image.rows[write.base_offset].xmax==0)
}
pub open spec fn selected_valid(image:lookup::Image,tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    forall|j:int| 0<=j<indices.len() ==> base_valid(image,tx.write_set[indices[j] as int])
}
pub open spec fn selection_input<D:GuardedRead>(driver:&D,tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    lookup::image_valid(driver.image())
    && forall|j:int| 0<=j<indices.len() ==> indices[j]<tx.write_set.len()
        && tx.write_set[indices[j] as int].row_id<driver.image().capacity
        && driver.authorized(tx.write_set[indices[j] as int].row_id,tx.write_set[indices[j] as int].new_offset)
}
pub fn has_write_base_conflict<D:GuardedRead>(driver:&D,tx:&ordinary::Transaction,indices:&Vec<usize>)
    ->(result:Result<bool,lookup::Error>)
    requires selection_input(driver,*tx,indices@),
    ensures result.is_ok() ==> (!result->Ok_0 <==> selected_valid(driver.image(),*tx,indices@)),
        driver.infallible() ==> result.is_ok(),
{
    /* NATIVE_WRITE_BASE_CONFLICT */
}

// Facts about a fresh privately owned allocation. In particular this predicate
// does not assert the selected base is the current head or that its xmax is zero.
// Rank/aliasing/allocator ownership remain low-level native boundary obligations.
pub open spec fn fresh_private(image:lookup::Image,write:ordinary::Write,writer:u64)->bool {
    lookup::image_valid(image) && writer>0 && write.row_id<image.capacity
    && write.new_offset!=0 && image.rows.contains_key(write.new_offset)
    && write.new_offset!=write.base_offset
    && image.rows[write.new_offset].xmin==writer && image.rows[write.new_offset].xmax==0
    && (write.base_offset==0 || image.rows.contains_key(write.base_offset)
        && image.rank[write.base_offset]<image.rank[write.new_offset])
    && (forall|id:usize| id<image.capacity ==> image.heads[id]!=write.new_offset)
    && (forall|p:u32| #[trigger] image.rows.contains_key(p) && p!=write.new_offset
        ==> image.rows[p].next!=write.new_offset)
}
pub proof fn validated_base_prepares_publication(image:lookup::Image,write:ordinary::Write,writer:u64)
    requires fresh_private(image,write,writer),base_valid(image,write),
    ensures publication::prepared(image,ordinary::row_write(write),writer),
{}

// Exact values returned by native index_changes for the selected write. The
// planner proves this relation separately; current-head equality is derived here.
pub open spec fn change_matches(image:lookup::Image,write:ordinary::Write,change:postings::IndexChange)->bool {
    change.row_id==write.row_id
    && change.before==(if write.base_offset==0 {None} else {image.rows[write.base_offset].value})
    && change.after==image.rows[write.new_offset].value
}
pub proof fn validation_establishes_plan(before:DataState,plan:Plan,ordinary_plan:OrdinaryPlan)
    requires ordinary_plan.indices.len()==1,ordinary_plan.indices[0]<ordinary_plan.tx.write_set.len(),
        selected_valid(before.image,ordinary_plan.tx,ordinary_plan.indices@),
        fresh_private(before.image,ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int],ordinary_plan.tx.txid),
        plan.record.txid==ordinary_plan.tx.txid,plan.record.writes.len()==1,
        plan.record.writes[0]==ordinary::row_write(ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        plan.changes.len()==1,
        change_matches(before.image,ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int],plan.changes[0]),
        plan.changes[0].before!=plan.changes[0].after,
        postings::valid(before.postings,plan.changes@),!before.postings.poisoned,
        crate::row_postings_match(before,plan.changes[0].binding,plan.changes[0].row_id),
    ensures crate::planned(before,plan),crate::ordinary_matches(plan,ordinary_plan),
{
    let w=ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int];
    assert(base_valid(before.image,w));
    validated_base_prepares_publication(before.image,w,ordinary_plan.tx.txid);
    assert(plan.changes[0].before==crate::head_key(before.image,w.row_id));
}

impl GuardedRead for publication::Memory {
    fn load_head(&self,id:usize)->(head:u32) {self.head}
    fn load_xmax(&self,ptr:u32)->(xmax:u64) {
        if ptr==1 {self.base.xmax} else {self.fresh.xmax}
    }
}

pub fn validator_has_live_witness()->(result:bool)
    ensures result,
{
    let base=lookup::Row{xmin:1,xmax:0,next:0,value:Some(11),locked:false,owner:0};
    let fresh=lookup::Row{xmin:9,xmax:0,next:1,value:Some(22),locked:false,owner:0};
    let mut memory=publication::Memory{head:1,base,fresh};
    let write=ordinary::Write{row_id:0,base_offset:1,new_offset:2,dirty_columns_bitmask:0};
    let mut writes=Vec::new();writes.push(write);
    let tx=ordinary::Transaction{txid:9,write_set:writes};
    let mut indices=Vec::new();indices.push(0);
    proof {assert(selection_input(&memory,tx,indices@));}
    let valid=has_write_base_conflict(&memory,&tx,&indices);
    assert(valid==Ok(false));
    proof {assert(fresh_private(memory.image(),write,9));
        validated_base_prepares_publication(memory.image(),write,9);}
    memory.base.xmax=7;
    proof {assert(selection_input(&memory,tx,indices@));}
    let deleted=has_write_base_conflict(&memory,&tx,&indices);
    proof {
        assert(!base_valid(memory.image(),tx.write_set[indices[0] as int]));
        assert(!selected_valid(memory.image(),tx,indices@));
    }
    assert(deleted==Ok(true));
    memory.head=0;
    proof {assert(selection_input(&memory,tx,indices@));}
    let stale=has_write_base_conflict(&memory,&tx,&indices);
    proof {
        assert(!base_valid(memory.image(),tx.write_set[indices[0] as int]));
        assert(!selected_valid(memory.image(),tx,indices@));
    }
    assert(stale==Ok(true));
    let create=ordinary::Write{base_offset:0,..write};
    let mut creates=Vec::new();creates.push(create);
    let create_tx=ordinary::Transaction{txid:9,write_set:creates};
    proof {assert(selection_input(&memory,create_tx,indices@));}
    let empty=has_write_base_conflict(&memory,&create_tx,&indices);
    assert(empty==Ok(false));
    true
}
}
