use vstd::prelude::*;
use lookup::{Image,Row,Snapshot,Transaction};
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
#[derive(Clone, Copy)]
pub struct Layout { pub tail:u32, pub anchor:u32, pub fresh:u32 }
pub open spec fn distinct(l:Layout)->bool {
    l.tail!=0 && l.anchor!=0 && l.fresh!=0
        && l.tail!=l.anchor && l.tail!=l.fresh && l.anchor!=l.fresh
}
pub open spec fn seeded(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    Image {capacity:1,heads:Map::empty().insert(0,l.anchor),
        rows:Map::empty()
            .insert(l.tail,Row{xmin:1,xmax:birth,next:0,value:None,locked:false,owner:0})
            .insert(l.anchor,Row{xmin:birth,xmax:0,next:l.tail,value:Some(before),locked:false,owner:0})
            .insert(l.fresh,Row{xmin:writer,xmax:0,next:l.anchor,value:Some(after),locked:false,owner:0}),
        rank:Map::empty().insert(l.tail,0).insert(l.anchor,1).insert(l.fresh,2)}
}
pub open spec fn old_reader(s:Snapshot,birth:u64,writer:u64)->bool {
    0<birth<s.xmin<=s.txid<s.xmax && writer!=s.txid
        && !lookup::creator_visible(writer,s)
        && (writer>=s.xmax || s.active.contains(writer))
        && (forall|id:u64| s.active.contains(id) ==> s.xmin<=id)
}
pub open spec fn write(l:Layout)->publication::CommittedWrite {
    publication::CommittedWrite{row_id:0,base_offset:l.anchor,new_offset:l.fresh}
}
pub open spec fn initial_keys(key:usize)->Map<usize,Option<usize>> {
    Map::empty().insert(0,Some(key))
}
// The prior lifecycle adapter starts from metadata at lock acquisition. This
// derives the numeric vacuum bound from an actual retained slot; ownership of
// that native slot across the complete operation remains a separate boundary.
pub fn native_horizon_covers_reader<D:lifecycle::LifecyclePrimitives>(driver:&mut D,
    Ghost(slot):Ghost<int>,Ghost(tx):Ghost<Transaction>)->(horizon:u64)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        0<=slot<old(driver).state().slots.len(),tx.txid>0,
        old(driver).state().slots[slot].txid==tx.txid,
        old(driver).state().slots[slot].snapshot_xmin==tx.snapshot_xmin,
    ensures horizon<=tx.snapshot_xmin,
{
    let horizon=lifecycle::oldest_snapshot_xmin(driver);
    proof {lifecycle::retention_covers_active_snapshot(driver.state(),slot,lifecycle::PROCARRAY_SLOTS as int);}
    horizon
}
pub proof fn seeded_native_preconditions(l:Layout,birth:u64,writer:u64,before:usize,after:usize)
    requires distinct(l),birth>1,writer>0,
    ensures lookup::image_valid(seeded(l,birth,writer,before,after)),
        publication::prepared(seeded(l,birth,writer,before,after),write(l),writer),
{
    let image=seeded(l,birth,writer,before,after);
    assert forall|p:u32| image.rows.contains_key(p) implies
        p==l.tail || p==l.anchor || p==l.fresh by {}
    assert forall|p:u32| image.rows.contains_key(p) implies p!=0 && image.rank.contains_key(p)
        && (image.rows[p].next==0 || image.rows.contains_key(image.rows[p].next)
            && image.rank[image.rows[p].next]<image.rank[p]) by {
        if p==l.tail {} else if p==l.anchor {} else {assert(p==l.fresh);}
    }
}
pub proof fn publication_derives_snapshot_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,phase:nat)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),phase<=3,
    ensures lookup::history_maps_snapshot(
        publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase),
        tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase),
            publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase).heads[0],
            lookup::snapshot(tx))==Some(l.anchor),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    let initial=seeded(l,birth,writer,before,after);
    publication::publication_preserves_old_snapshot(initial,write(l),writer,phase,lookup::snapshot(tx));
    publication::publication_prefix_valid(initial,write(l),writer,phase);
    assert(lookup::visible(initial.rows[l.anchor],lookup::snapshot(tx)));
    assert(lookup::first_visible(initial,initial.heads[0],lookup::snapshot(tx))==Some(l.anchor));
    let image=publication::publication_prefix(initial,write(l),writer,phase);
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {
        assert(id==0);
    }
}
// A publication prefix is not assumed to implement a logical event. These
// equations derive the new-reader replay from the actual successful head swap.
pub proof fn publication_derives_completed_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,stamp:u64)
    requires distinct(l),birth>1,writer>0,writer<tx.snapshot_xmin,
    ensures lookup::history_maps_snapshot(
        publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3),
        tx,initial_keys(before),seq![lookup::Event{row:0,before:Some(before),after:Some(after),creator:writer,stamp}]),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    let image=publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3);
    publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
    assert(lookup::visible(image.rows[l.fresh],lookup::snapshot(tx)));
    assert(lookup::first_visible(image,l.fresh,lookup::snapshot(tx))==Some(l.fresh));
    let events=seq![lookup::Event{row:0,before:Some(before),after:Some(after),creator:writer,stamp}];
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),events,1,lookup::snapshot(tx))[id] by {
        assert(id==0);
    }
}

// This is the actual source-derived publisher; its field-level postcondition
// supplies the image from which the snapshot/history equation is established.
pub fn native_publication_derives_history<D:publication::PublicationStorage>(driver:&mut D,
    record:&publication::CommitRecord,Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(r:Result<(),lookup::Error>)
    requires distinct(l),birth>1,record.txid>0,record.writes.len()==1,record.writes[0]==write(l),
        old(driver).image()==seeded(l,birth,record.txid,before,after),
        old(driver).authorized(0,l.fresh),
        old_reader(lookup::snapshot(tx),birth,record.txid),
    ensures lookup::history_maps_snapshot(final(driver).image(),tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(final(driver).image(),final(driver).image().heads[0],lookup::snapshot(tx))==Some(l.anchor),
        r.is_ok() ==> final(driver).image()==publication::publication_prefix(
            seeded(l,birth,record.txid,before,after),write(l),record.txid,3),
        old(driver).infallible() ==> r.is_ok(),
{
    proof { seeded_native_preconditions(l,birth,record.txid,before,after); }
    let r=publication::publish_prepared_write_set(driver,record);
    proof {
        let initial=seeded(l,birth,record.txid,before,after);
        let phase=choose|phase:nat| phase<=3 && driver.image()==publication::publication_prefix(initial,write(l),record.txid,phase);
        publication_derives_snapshot_history(l,birth,record.txid,before,after,tx,phase);
    }
    r
}

pub open spec fn published(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3)
}
pub open spec fn pruned(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    retention::spliced(published(l,birth,writer,before,after),l.anchor,0)
}
// The allocator's exclusive ptr::write field effect after an unlinked slot is
// popped. Raw allocation uniqueness and pointer provenance remain a boundary.
pub open spec fn reused(l:Layout,birth:u64,writer:u64,before:usize,after:usize,reuser:u64,key:usize)->Image {
    let p=pruned(l,birth,writer,before,after);
    Image{rows:p.rows.insert(l.tail,Row{xmin:reuser,xmax:0,next:0,value:Some(key),locked:false,owner:0}),..p}
}

pub proof fn publication_derives_retained_prefix(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,horizon:u64)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),horizon<=tx.snapshot_xmin,
    ensures lookup::image_valid(published(l,birth,writer,before,after)),
        retention::admissible(published(l,birth,writer,before,after),lookup::snapshot(tx),horizon),
        retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx))==Set::empty().insert(l.fresh).insert(l.anchor),
        forall|p:u32| retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx)).contains(p)
            ==> !retention::eligible(published(l,birth,writer,before,after).rows[p],horizon),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
    publication_derives_snapshot_history(l,birth,writer,before,after,tx,3);
    let image=published(l,birth,writer,before,after);
    if tx.snapshot_active@.contains(writer) {assert(tx.snapshot_xmin<=writer);}
    assert(writer>=tx.snapshot_xmin);
    assert forall|i:int| 0<=i<tx.snapshot_active.len() implies tx.snapshot_xmin<=tx.snapshot_active[i] by {
        assert(tx.snapshot_active@.contains(tx.snapshot_active[i]));
    }
    assert(retention::snapshot_well_formed(lookup::snapshot(tx)));
    assert forall|p:u32| image.rows.contains_key(p) implies image.rows[p].xmin!=tx.txid by {
        assert(p==l.tail || p==l.anchor || p==l.fresh);
    }
    reveal_with_fuel(retention::prefix,3);
    assert(retention::prefix(image,l.fresh,lookup::snapshot(tx)) =~= Set::empty().insert(l.fresh).insert(l.anchor));
}

pub fn native_vacuum_derives_history<D:retention::Vacuum>(driver:&mut D,horizon:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<Vec<retention::Reclaimed>,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),
        birth<horizon<=tx.snapshot_xmin,old(driver).partition_held(0),
        old(driver).state().image==published(l,birth,writer,before,after),
        old(driver).state().recycled==Set::<u32>::empty(),
    ensures lookup::history_maps_snapshot(final(driver).state().image,tx,initial_keys(before),Seq::empty()),
        !final(driver).state().recycled.contains(l.anchor),!final(driver).state().recycled.contains(l.fresh),
        result.is_ok() ==> final(driver).state().image==pruned(l,birth,writer,before,after)
            && final(driver).state().recycled==Set::empty().insert(l.tail) && result.unwrap().len()==1,
{
    proof {
        publication_derives_retained_prefix(l,birth,writer,before,after,tx,horizon);
        publication_derives_snapshot_history(l,birth,writer,before,after,tx,3);
        assert(driver.state().recycled.disjoint(retention::reachable(driver.state().image,l.fresh)));
    }
    let result=retention::vacuum_row_acquired(driver,0,horizon,Ghost(lookup::snapshot(tx)));
    proof {
        let p=published(l,birth,writer,before,after);
        let image=driver.state().image;
        assert(lookup::first_visible(image,l.fresh,lookup::snapshot(tx))==Some(l.anchor));
        assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
            && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {assert(id==0);}
        if result.is_ok() {
            reveal_with_fuel(retention::reachable,4);
            assert(driver.state().recycled =~= Set::empty().insert(l.tail));
            assert(image.rows[l.fresh].next==l.anchor);
            assert(image.rows[l.tail].next==0);
            assert(image.rows[l.anchor].next==0 || image.rows[l.anchor].next==l.tail);
            if image.rows[l.anchor].next==l.tail {
                assert(retention::reachable(image,l.fresh).contains(l.tail));
                assert(!retention::eligible(image.rows[l.tail],horizon));
                assert(false);
            }
            assert(image.rows =~= pruned(l,birth,writer,before,after).rows);
            assert(image==pruned(l,birth,writer,before,after));
            assert(driver.state().recycled.difference(Set::<u32>::empty()) =~= driver.state().recycled);
            assert(Set::<u32>::empty().insert(l.tail).len()==1);
            assert(result.unwrap().len()==1);
        }
    }
    result
}

pub proof fn pruned_tail_reuse_preserves_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,reuser:u64,key:usize)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),
    ensures lookup::image_valid(reused(l,birth,writer,before,after,reuser,key)),
        lookup::history_maps_snapshot(reused(l,birth,writer,before,after,reuser,key),tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(reused(l,birth,writer,before,after,reuser,key),l.fresh,lookup::snapshot(tx))==Some(l.anchor),
        lookup::first_visible(reused(l,birth,writer,before,after,reuser,key),l.anchor,lookup::snapshot(tx))==Some(l.anchor),
        forall|p:u32| retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx)).contains(p) ==>
            retention::data_same(published(l,birth,writer,before,after).rows[p],reused(l,birth,writer,before,after,reuser,key).rows[p])
            && (p!=l.anchor ==> published(l,birth,writer,before,after).rows[p].next==reused(l,birth,writer,before,after,reuser,key).rows[p].next),
{
    publication_derives_retained_prefix(l,birth,writer,before,after,tx,tx.snapshot_xmin);
    let image=reused(l,birth,writer,before,after,reuser,key);
    assert forall|p:u32| #[trigger] image.rows.contains_key(p) implies p!=0 && image.rank.contains_key(p)
        && (image.rows[p].next==0 || image.rows.contains_key(image.rows[p].next) && image.rank[image.rows[p].next]<image.rank[p]) by {
        assert(p==l.tail || p==l.anchor || p==l.fresh);
    }
    assert(lookup::image_valid(image));
    reveal_with_fuel(lookup::first_visible,3);
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {assert(id==0);}
}

pub fn native_read_after_reuse<D:lookup::Storage>(driver:&D,tx:&mut Transaction,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(reuser):Ghost<u64>,Ghost(key):Ghost<usize>)
    ->(result:Result<Option<Option<usize>>,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(*old(tx)),birth,writer),
        old(tx).write_set.len()==0,old(tx).read_set.len()==0,
        driver.image()==reused(l,birth,writer,before,after,reuser,key),
    ensures lookup::transaction_view_same(*old(tx),*final(tx)),
        result.is_ok() ==> result.unwrap()==Some(Some(before))
            && final(tx).read_set@==seq![lookup::ReadRecord{row_id:0,row_ptr:l.anchor,xmin:birth}],
{
    proof {pruned_tail_reuse_preserves_history(l,birth,writer,before,after,*tx,reuser,key);}
    lookup::read(driver,tx,0)
}

pub fn native_constructor_clears_metadata(value:Option<usize>,writer:u64,next:u32)->(cell:initialization::Cell)
    ensures cell.row.value==value,cell.row.xmin==writer,cell.row.xmax==0,cell.row.next==next,
        !cell.row.locked,cell.row.owner==0,cell.recycle_next==0,
{
    initialization::new_row(value,writer,next)
}

pub fn native_initialize_preserves_reader<D:initialization::Initialization>(driver:&mut D,row_ptr:u32,key:usize,reuser:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<(),lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),row_ptr==l.tail,
        old(driver).state().image==pruned(l,birth,writer,before,after),
        old(driver).state().exclusive.contains(l.tail),
        old(driver).state().protected==Set::empty().insert(l.fresh).insert(l.anchor),
    ensures result.is_ok() ==> final(driver).state().image==reused(l,birth,writer,before,after,reuser,key)
        && lookup::history_maps_snapshot(final(driver).state().image,tx,initial_keys(before),Seq::empty())
        && lookup::first_visible(final(driver).state().image,l.anchor,lookup::snapshot(tx))==Some(l.anchor),
{
    proof {
        seeded_native_preconditions(l,birth,writer,before,after);
        publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
        retention::splice_valid(published(l,birth,writer,before,after),l.anchor,l.tail);
        assert(initialization::permitted(driver.state(),row_ptr,0));
    }
    let result=initialization::initialize_row(driver,row_ptr,Some(key),reuser,0);
    proof {
        if result.is_ok() {
            assert(driver.state().image==reused(l,birth,writer,before,after,reuser,key));
            pruned_tail_reuse_preserves_history(l,birth,writer,before,after,tx,reuser,key);
        }
    }
    result
}

pub fn native_validation_rejects_invisible_writer<D:lookup::Storage>(driver:&D,tx:&Transaction,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(reuser):Ghost<u64>,Ghost(key):Ghost<usize>)
    ->(result:Result<bool,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(*tx),birth,writer),
        tx.read_set@==seq![lookup::ReadRecord{row_id:0,row_ptr:l.anchor,xmin:birth}],
        driver.image()==reused(l,birth,writer,before,after,reuser,key),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {lookup::later_deletion_rejects_recorded_read(driver.image(),*tx,0);}
    lookup::has_serialization_conflict(driver,tx)
}

pub proof fn old_snapshot_cases_have_witnesses()
    ensures exists|l:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(l) && birth>1
        && old_reader(tx,birth,writer) && writer<tx.xmax && tx.active.contains(writer),
        exists|l:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(l) && birth>1
        && old_reader(tx,birth,writer) && writer>=tx.xmax,
{
    let l=Layout{tail:1,anchor:2,fresh:3};
    let active=Snapshot{txid:5,xmin:3,xmax:8,active:seq![7u64]};
    let future=Snapshot{txid:5,xmin:3,xmax:8,active:Seq::empty()};
    assert(distinct(l));
    assert(old_reader(active,2,7));
    assert(old_reader(future,2,9));
    assert(7<active.xmax && active.active.contains(7u64));
    assert(9>=future.xmax);
    assert(exists|layout:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(layout) && birth>1
        && old_reader(tx,birth,writer) && writer<tx.xmax && tx.active.contains(writer));
    assert(exists|layout:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(layout) && birth>1
        && old_reader(tx,birth,writer) && writer>=tx.xmax);
}

pub fn native_vacuum_releases_anchor<D:retention::Vacuum>(driver:&mut D,horizon:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<Vec<retention::Reclaimed>,lookup::Error>)
    requires distinct(l),1<birth<writer<horizon<=tx.snapshot_xmin,
        retention::snapshot_well_formed(lookup::snapshot(tx)),old(driver).partition_held(0),
        old(driver).state().image==pruned(l,birth,writer,before,after),
        old(driver).state().recycled==Set::empty().insert(l.tail),
    ensures result.is_ok() ==> final(driver).state().recycled.contains(l.anchor)
        && !final(driver).state().recycled.contains(l.fresh),
{
    proof {
        seeded_native_preconditions(l,birth,writer,before,after);
        publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
        retention::splice_valid(published(l,birth,writer,before,after),l.anchor,l.tail);
        reveal_with_fuel(retention::reachable,3);
        assert(retention::admissible(driver.state().image,lookup::snapshot(tx),horizon));
        assert(driver.state().recycled.disjoint(retention::reachable(driver.state().image,l.fresh)));
    }
    retention::vacuum_row_acquired(driver,0,horizon,Ghost(lookup::snapshot(tx)))
}
}
