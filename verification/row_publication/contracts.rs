use crate::lookup;
use lookup::{Image, Row, Snapshot, Error};
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CommittedWrite { pub row_id:usize, pub base_offset:u32, pub new_offset:u32 }
pub struct CommitRecord { pub txid:u64, pub writes:Vec<CommittedWrite> }
pub open spec fn set_xmax(s:Image,p:u32,value:u64) -> Image {
    Image {rows:s.rows.insert(p,Row{xmax:value,..s.rows[p]}),..s}
}
pub open spec fn set_next(s:Image,p:u32,value:u32) -> Image {
    Image {rows:s.rows.insert(p,Row{next:value,..s.rows[p]}),..s}
}
pub open spec fn set_head(s:Image,id:usize,value:u32) -> Image {
    Image {heads:s.heads.insert(id,value),..s}
}
pub open spec fn publication_prefix(s:Image,w:CommittedWrite,writer:u64,phase:nat) -> Image {
    let deleted=if w.base_offset==0 {s} else {set_xmax(s,w.base_offset,writer)};
    let linked=set_next(deleted,w.new_offset,w.base_offset);
    if phase==0 {s} else if phase==1 {deleted} else if phase==2 {linked}
    else {set_head(linked,w.row_id,w.new_offset)}
}
pub open spec fn prepared(s:Image,w:CommittedWrite,writer:u64) -> bool {
    lookup::image_valid(s) && writer>0 && w.row_id<s.capacity
    && s.heads[w.row_id]==w.base_offset
    && w.new_offset!=0 && s.rows.contains_key(w.new_offset)
    && w.new_offset!=w.base_offset && s.rows[w.new_offset].xmin==writer
    && s.rows[w.new_offset].xmax==0
    && (w.base_offset==0 || (s.rows.contains_key(w.base_offset)
        && s.rows[w.base_offset].xmax==0 && s.rank[w.base_offset]<s.rank[w.new_offset]))
    && (forall|id:usize| id<s.capacity ==> s.heads[id]!=w.new_offset)
    && (forall|p:u32| #[trigger] s.rows.contains_key(p) && p!=w.new_offset ==> s.rows[p].next!=w.new_offset)
}
pub open spec fn allowed_prefix(before:Image,after:Image,w:CommittedWrite,writer:u64) -> bool {
    after==publication_prefix(before,w,writer,0) || after==publication_prefix(before,w,writer,1)
        || after==publication_prefix(before,w,writer,2) || after==publication_prefix(before,w,writer,3)
}

// The native row-partition guard excludes writers and vacuum on this projected
// chain; the prepared new allocation is exclusive. Each operation models one
// actual field access. These contracts do not supply a publication result.
pub trait PublicationStorage {
    spec fn image(&self)->Image;
    spec fn infallible(&self)->bool;
    // Exact row partition and exclusive prepared allocation, belonging to this
    // driver's physical arena. Native capability construction remains open.
    spec fn authorized(&self,row_id:usize,new_ptr:u32)->bool;
    fn slot_ref(&self,id:usize)->(r:Result<usize,Error>)
        requires id<self.image().capacity, exists|p:u32| self.authorized(id,p),
        ensures r.is_ok() ==> r.unwrap()==id, self.infallible() ==> r.is_ok();
    fn resolve(&self,p:u32)->(r:Result<u32,Error>)
        requires self.image().rows.contains_key(p),
        ensures r.is_ok() ==> r.unwrap()==p, self.infallible() ==> r.is_ok();
    fn compare_xmax(&mut self,p:u32,expected:u64,value:u64,Ghost(row_id):Ghost<usize>,Ghost(new_ptr):Ghost<u32>)->(r:Result<(),Error>)
        requires old(self).image().rows.contains_key(p), old(self).authorized(row_id,new_ptr),
            row_id<old(self).image().capacity, old(self).image().heads[row_id]==p,
        ensures final(self).infallible()==old(self).infallible(),
            forall|id:usize,q:u32| #[trigger] final(self).authorized(id,q)==old(self).authorized(id,q),
            r.is_ok() <==> old(self).image().rows[p].xmax==expected,
            final(self).image()==if r.is_ok() {set_xmax(old(self).image(),p,value)} else {old(self).image()};
    fn store_next(&mut self,p:u32,value:u32,Ghost(row_id):Ghost<usize>)
        requires old(self).image().rows.contains_key(p), old(self).authorized(row_id,p),
        ensures final(self).infallible()==old(self).infallible(),
            forall|id:usize,q:u32| #[trigger] final(self).authorized(id,q)==old(self).authorized(id,q),
            final(self).image()==set_next(old(self).image(),p,value);
    fn compare_head(&mut self,id:usize,expected:u32,value:u32)->(r:Result<(),Error>)
        requires id<old(self).image().capacity, old(self).authorized(id,value),
        ensures final(self).infallible()==old(self).infallible(),
            forall|row:usize,q:u32| #[trigger] final(self).authorized(row,q)==old(self).authorized(row,q),
            r.is_ok() <==> old(self).image().heads[id]==expected,
            final(self).image()==if r.is_ok() {set_head(old(self).image(),id,value)} else {old(self).image()};
}

pub fn publish_prepared_write_set<D:PublicationStorage>(driver:&mut D,record:&CommitRecord)
    ->(r:Result<(),Error>)
    requires record.writes.len()==1, prepared(old(driver).image(),record.writes[0],record.txid),
        old(driver).authorized(record.writes[0].row_id,record.writes[0].new_offset),
    ensures allowed_prefix(old(driver).image(),final(driver).image(),record.writes[0],record.txid),
        r.is_ok() ==> final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,3),
        r.is_err() ==> final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,0)
            || final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,1),
        old(driver).infallible() ==> r.is_ok(),
        final(driver).infallible()==old(driver).infallible(),
        final(driver).authorized(record.writes[0].row_id,record.writes[0].new_offset),
{
    /* NATIVE_PUBLICATION */
}

pub proof fn image_head_present(s:Image,id:usize)
    requires lookup::image_valid(s),id<s.capacity,
    ensures s.heads.contains_key(id) && (s.heads[id]==0 || s.rows.contains_key(s.heads[id])),
{
    // Materialize the selected head to instantiate the native image's head
    // quantifier (whose trigger is the indexed head, not domain membership).
    let head=s.heads[id];
    assert(head==0 || head>0);
}
pub proof fn publication_prefix_valid(s:Image,w:CommittedWrite,writer:u64,phase:nat)
    requires prepared(s,w,writer), phase<=3,
    ensures lookup::image_valid(publication_prefix(s,w,writer,phase)),
        publication_prefix(s,w,writer,phase).capacity==s.capacity,
        publication_prefix(s,w,writer,phase).rows.dom()==s.rows.dom(),
        forall|p:u32| #[trigger] s.rows.contains_key(p) ==>
            publication_prefix(s,w,writer,phase).rows[p].value==s.rows[p].value,
{
    let after=publication_prefix(s,w,writer,phase);
    reveal(lookup::image_valid);
    assert(lookup::image_valid(s));
    assert(after.capacity==s.capacity);
    assert forall|id:usize| id<after.capacity implies after.heads.contains_key(id)
        && (#[trigger] after.heads[id]==0 || after.rows.contains_key(after.heads[id])) by {
        assert(id<s.capacity);
        image_head_present(s,id);
        assert(s.heads.contains_key(id) && (s.heads[id]==0 || s.rows.contains_key(s.heads[id])));
        if phase==3 && id==w.row_id {assert(after.heads[id]==w.new_offset);}
        else {assert(after.heads[id]==s.heads[id]);}
    }
    assert forall|p:u32| #[trigger] after.rows.contains_key(p) implies p!=0 && after.rank.contains_key(p)
        && (after.rows[p].next==0 || after.rows.contains_key(after.rows[p].next)
            && after.rank[after.rows[p].next]<after.rank[p]) by {
        assert(s.rows.contains_key(p));
        if phase>=2 && p==w.new_offset {assert(after.rows[p].next==w.base_offset);}
        else {assert(after.rows[p].next==s.rows[p].next);}
    }

}

pub proof fn retained_chain_frame(before:Image,after:Image,tx:Snapshot,ptr:u32,new_ptr:u32)
    requires lookup::image_valid(before),lookup::image_valid(after),ptr==0 || before.rows.contains_key(ptr),ptr!=new_ptr,
        forall|p:u32| #[trigger] before.rows.contains_key(p) && p!=new_ptr ==> after.rows.contains_key(p)
            && before.rows[p].next!=new_ptr && before.rows[p].next==after.rows[p].next
            && lookup::visible(before.rows[p],tx)==lookup::visible(after.rows[p],tx),
    ensures lookup::first_visible(before,ptr,tx)==lookup::first_visible(after,ptr,tx),
    decreases if ptr==0 {0nat} else {before.rank[ptr]+1},
{
    if ptr!=0 && !lookup::visible(before.rows[ptr],tx) {
        retained_chain_frame(before,after,tx,before.rows[ptr].next,new_ptr);
    }
}
pub proof fn publication_preserves_old_snapshot(before:Image,w:CommittedWrite,writer:u64,phase:nat,tx:Snapshot)
    requires prepared(before,w,writer),phase<=3,writer!=tx.txid,
        !lookup::creator_visible(writer,tx),writer>=tx.xmax || tx.active.contains(writer),
    ensures lookup::first_visible(before,before.heads[w.row_id],tx)
        ==lookup::first_visible(publication_prefix(before,w,writer,phase),publication_prefix(before,w,writer,phase).heads[w.row_id],tx),
        forall|p:u32| #[trigger] before.rows.contains_key(p) ==>
            publication_prefix(before,w,writer,phase).rows[p].value==before.rows[p].value,
{
    let after=publication_prefix(before,w,writer,phase);
    publication_prefix_valid(before,w,writer,phase);
    assert forall|p:u32| before.rows.contains_key(p) && p!=w.new_offset implies after.rows.contains_key(p)
        && before.rows[p].next!=w.new_offset && before.rows[p].next==after.rows[p].next
        && lookup::visible(before.rows[p],tx)==lookup::visible(after.rows[p],tx) by {
        if p==w.base_offset {assert(before.rows[p].xmax==0);}
    }
    retained_chain_frame(before,after,tx,before.heads[w.row_id],w.new_offset);
    if phase==3 {
        assert(!lookup::visible(after.rows[w.new_offset],tx));
        assert(after.rows[w.new_offset].next==w.base_offset);
    }
}
pub proof fn successful_publication_connects_lookup(before:Image,after:Image,w:CommittedWrite,writer:u64,tx:Snapshot)
    requires prepared(before,w,writer),after==publication_prefix(before,w,writer,3),writer!=tx.txid,
        !lookup::creator_visible(writer,tx),writer>=tx.xmax || tx.active.contains(writer),
    ensures lookup::image_valid(after),after.heads[w.row_id]==w.new_offset,
        after.rows[w.new_offset].next==w.base_offset,
        w.base_offset!=0 ==> after.rows[w.base_offset].xmax==writer,
        lookup::first_visible(before,before.heads[w.row_id],tx)==lookup::first_visible(after,after.heads[w.row_id],tx),
{
    publication_prefix_valid(before,w,writer,3);
    publication_preserves_old_snapshot(before,w,writer,3,tx);
}
pub proof fn publication_has_live_witness()
    ensures exists|s:Image,w:CommittedWrite,tx:Snapshot|
        prepared(s,w,9) && w.base_offset!=0 && !lookup::creator_visible(9,tx)
        && lookup::first_visible(s,s.heads[w.row_id],tx)==Some(w.base_offset)
        && lookup::first_visible(publication_prefix(s,w,9,3),w.new_offset,tx)==Some(w.base_offset),
{
    let base=Row{xmin:1,xmax:0,next:0,value:Some(42),locked:false,owner:0};
    let fresh=Row{xmin:9,xmax:0,next:1,value:Some(77),locked:false,owner:0};
    let s=Image{heads:Map::empty().insert(0,1),rows:Map::empty().insert(1,base).insert(2,fresh),
        rank:Map::empty().insert(1,0).insert(2,1),capacity:1};
    let w=CommittedWrite{row_id:0,base_offset:1,new_offset:2};
    let tx=Snapshot{txid:4,xmin:2,xmax:5,active:Seq::empty()};
    assert(lookup::image_valid(s));
    assert(prepared(s,w,9));
    publication_preserves_old_snapshot(s,w,9,3,tx);
    assert(lookup::first_visible(s,1,tx)==Some(1));
    assert(exists|image:Image,write:CommittedWrite,snap:Snapshot|
        prepared(image,write,9) && write.base_offset!=0 && !lookup::creator_visible(9,snap)
        && lookup::first_visible(image,image.heads[write.row_id],snap)==Some(write.base_offset)
        && lookup::first_visible(publication_prefix(image,write,9,3),write.new_offset,snap)==Some(write.base_offset));
}

// Executable consistency witness for all field contracts, using fixed arena
// offsets 1/2 and one held row partition. This is a model implementation, not
// a claim that the native unsafe allocator already refines these contracts.
pub struct Memory { pub head:u32, pub base:Row, pub fresh:Row }
impl PublicationStorage for Memory {
    open spec fn image(&self)->Image {
        Image{heads:Map::empty().insert(0,self.head),
            rows:Map::empty().insert(1,self.base).insert(2,self.fresh),
            rank:Map::empty().insert(1,0nat).insert(2,1nat),capacity:1}
    }
    open spec fn infallible(&self)->bool { true }
    open spec fn authorized(&self,row_id:usize,new_ptr:u32)->bool { row_id==0 && new_ptr==2 }
    fn slot_ref(&self,id:usize)->(r:Result<usize,Error>) { Ok(id) }
    fn resolve(&self,p:u32)->(r:Result<u32,Error>) { Ok(p) }
    fn compare_xmax(&mut self,p:u32,expected:u64,value:u64,Ghost(row_id):Ghost<usize>,Ghost(new_ptr):Ghost<u32>)->(r:Result<(),Error>) {
        let ghost before=self.image();
        let found=if p==1 {self.base.xmax} else {self.fresh.xmax};
        if found!=expected {return Err(Error::SerializationFailure);}
        if p==1 {self.base.xmax=value;} else {self.fresh.xmax=value;}
        proof {assert(self.image().rows =~= set_xmax(before,p,value).rows);}
        Ok(())
    }
    fn store_next(&mut self,p:u32,value:u32,Ghost(row_id):Ghost<usize>) {
        let ghost before=self.image();
        self.fresh.next=value;
        proof {assert(self.image().rows =~= set_next(before,p,value).rows);}
    }
    fn compare_head(&mut self,id:usize,expected:u32,value:u32)->(r:Result<(),Error>) {
        let ghost before=self.image();
        if self.head!=expected {return Err(Error::SerializationFailure);}
        self.head=value;
        proof {assert(self.image().heads =~= set_head(before,id,value).heads);}
        Ok(())
    }
}
pub fn native_publication_has_executable_witness()->(result:bool)
    ensures result,
{
    let mut driver=Memory{head:1,
        base:Row{xmin:1,xmax:0,next:0,value:Some(42),locked:false,owner:0},
        fresh:Row{xmin:9,xmax:0,next:1,value:Some(77),locked:false,owner:0}};
    let write=CommittedWrite{row_id:0,base_offset:1,new_offset:2};
    let mut writes=Vec::new();writes.push(write);
    let record=CommitRecord{txid:9,writes};
    let ghost before=driver.image();
    let ghost tx=Snapshot{txid:4,xmin:2,xmax:5,active:Seq::empty()};
    proof {assert(prepared(before,write,9));}
    let published=publish_prepared_write_set(&mut driver,&record);
    proof {
        successful_publication_connects_lookup(before,driver.image(),write,9,tx);
        assert(lookup::first_visible(before,1,tx)==Some(1));
        assert(lookup::first_visible(driver.image(),driver.head,tx)==Some(1));
    }
    published.is_ok() && driver.head==2 && driver.base.xmax==9 && driver.fresh.next==1
        && driver.base.value==Some(42) && driver.fresh.value==Some(77)
}
}
