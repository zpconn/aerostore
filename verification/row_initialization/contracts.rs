// Source-bound constructor and exclusive recycled-cell initialization.
use crate::lookup;
use lookup::{Image, Row, Error};
use vstd::prelude::*;
verus! {
pub struct Cell { pub row: Row, pub recycle_next: u32 }
pub struct State {
    pub image: Image,
    pub recycle_next: Map<u32,u32>,
    pub exclusive: Set<u32>,
    pub protected: Set<u32>,
}
pub open spec fn fresh_cell(value:Option<usize>,xmin:u64,next:u32)->Cell {
    Cell{row:Row{xmin,xmax:0,next,value,locked:false,owner:0},recycle_next:0}
}
pub open spec fn stored(s:State,p:u32,cell:Cell)->State {
    State{image:Image{rows:s.image.rows.insert(p,cell.row),..s.image},
        recycle_next:s.recycle_next.insert(p,cell.recycle_next),..s}
}
pub open spec fn initialized(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)->State {
    stored(s,p,fresh_cell(value,xmin,next))
}
pub open spec fn permitted(s:State,p:u32,next:u32)->bool {
    lookup::image_valid(s.image) && s.image.rows.contains_key(p)
    && s.exclusive.contains(p) && !s.protected.contains(p)
    && (next==0 || s.image.rows.contains_key(next) && s.image.rank[next]<s.image.rank[p])
}
// Raw address resolution and one exclusive ptr::write remain primitives. Their
// contracts describe individual operations, not initializer or reader safety.
pub trait Initialization {
    spec fn state(&self)->State;
    fn resolve_row_ptr_raw(&self,offset:u32)->(r:Result<u32,Error>)
        requires self.state().image.rows.contains_key(offset),
        ensures r.is_ok() ==> r.unwrap()==offset;
    fn write_cell(&mut self,p:u32,cell:Cell)
        requires old(self).state().image.rows.contains_key(p),
            old(self).state().exclusive.contains(p),!old(self).state().protected.contains(p),
        ensures final(self).state()==stored(old(self).state(),p,cell);
}
pub fn new_row(value:Option<usize>,xmin:u64,next:u32)->(cell:Cell)
    ensures cell==fresh_cell(value,xmin,next),
{
    /* NATIVE_CONSTRUCTOR */
}
pub proof fn initialized_preserves_protected(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)
    requires !s.protected.contains(p),
    ensures forall|q:u32| s.protected.contains(q) && s.image.rows.contains_key(q) ==>
        initialized(s,p,value,xmin,next).image.rows[q]==s.image.rows[q]
        && initialized(s,p,value,xmin,next).image.rows.contains_key(q),
{}
pub proof fn initialized_image_valid(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)
    requires permitted(s,p,next),
    ensures lookup::image_valid(initialized(s,p,value,xmin,next).image),
{
    let after=initialized(s,p,value,xmin,next).image;
    assert(after.rows.dom() =~= s.image.rows.dom());
    assert forall|id:usize| id<after.capacity implies after.heads.contains_key(id)
        && (after.heads[id]==0 || after.rows.contains_key(after.heads[id])) by {
        let head=s.image.heads[id];
        assert(head==0 || s.image.rows.contains_key(head));
    }
    assert forall|q:u32| #[trigger] after.rows.contains_key(q) implies q!=0 && after.rank.contains_key(q)
        && (after.rows[q].next==0 || after.rows.contains_key(after.rows[q].next)
            && after.rank[after.rows[q].next]<after.rank[q]) by {
        assert(s.image.rows.contains_key(q));
    }
}
pub fn initialize_row<D:Initialization>(driver:&mut D,row_ptr:u32,value:Option<usize>,xmin:u64,next:u32)
    ->(result:Result<(),Error>)
    requires permitted(old(driver).state(),row_ptr,next),
    ensures result.is_ok() ==> final(driver).state()==initialized(old(driver).state(),row_ptr,value,xmin,next),
        result.is_err() ==> final(driver).state()==old(driver).state(),
        lookup::image_valid(final(driver).state().image),
        final(driver).state().protected==old(driver).state().protected,
        forall|q:u32| old(driver).state().protected.contains(q) && old(driver).state().image.rows.contains_key(q) ==>
            final(driver).state().image.rows.contains_key(q)
            && final(driver).state().image.rows[q]==old(driver).state().image.rows[q],
{
    proof {
        initialized_preserves_protected(driver.state(),row_ptr,value,xmin,next);
        initialized_image_valid(driver.state(),row_ptr,value,xmin,next);
    }
    /* NATIVE_INITIALIZE */
}
}
