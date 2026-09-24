use vstd::prelude::*;
use crate::postings::PostingPrimitives;
use crate::publication::PublicationStorage;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

// Disjoint posting and row views of one admitted native storage projection.
// Native arena/guard correspondence is an instantiation obligation; neither
// component may alias the other's logical field domain through its primitive.
pub struct Storage<P:PostingPrimitives,R:PublicationStorage> {
    pub postings:P,
    pub rows:R,
}
pub struct DataState { pub postings:postings::State, pub image:lookup::Image }
impl<P:PostingPrimitives,R:PublicationStorage> Storage<P,R> {
    pub open spec fn view(&self)->DataState { DataState{postings:self.postings.state(),image:self.rows.image()} }
}
pub struct Plan { pub changes:Vec<postings::IndexChange>, pub record:publication::CommitRecord }
pub enum DataError { Posting(postings::Error), Publication(lookup::Error) }

pub open spec fn head_key(image:lookup::Image,id:usize)->Option<usize> {
    if image.heads[id]==0 {None} else {image.rows[image.heads[id]].value}
}
pub open spec fn row_postings_match(s:DataState,binding:usize,row:usize)->bool {
    forall|key:usize| #[trigger] s.postings.postings.contains((binding,key,row))
        <==> head_key(s.image,row)==Some(key)
}
pub open spec fn planned(before:DataState,plan:Plan)->bool {
    plan.changes.len()==1 && plan.record.writes.len()==1
    && plan.changes[0].row_id==plan.record.writes[0].row_id
    && plan.changes[0].before!=plan.changes[0].after
    && plan.changes[0].before==head_key(before.image,plan.changes[0].row_id)
    && before.image.rows[plan.record.writes[0].new_offset].value==plan.changes[0].after
    && publication::prepared(before.image,plan.record.writes[0],plan.record.txid)
    && postings::valid(before.postings,plan.changes@)
    && !before.postings.poisoned
    && row_postings_match(before,plan.changes[0].binding,plan.changes[0].row_id)
}
pub open spec fn destinations(plan:Plan)->Set<postings::Posting> {
    postings::destinations(plan.changes@,plan.changes.len() as int)
}
pub open spec fn sources(plan:Plan)->Set<postings::Posting> {
    postings::sources(plan.changes@,plan.changes.len() as int)
}
pub open spec fn installed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==before.image
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan))
}
pub open spec fn removed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==before.image
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan))
}
pub open spec fn committed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3)
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan))
    && row_postings_match(after,plan.changes[0].binding,plan.changes[0].row_id)
}
pub open spec fn unchanged_data(before:DataState,after:DataState)->bool {
    before.image==after.image && before.postings.postings==after.postings.postings
}

pub proof fn planned_derives_destination_ownership(before:DataState,plan:Plan)
    requires planned(before,plan),
    ensures postings::unique_destinations(plan.changes@),
        before.postings.postings.disjoint(destinations(plan)),
        sources(plan).disjoint(destinations(plan)),
{
    reveal_with_fuel(postings::destinations,2);
    reveal_with_fuel(postings::sources,2);
    let c=plan.changes[0];
    assert forall|p:postings::Posting| #[trigger] destinations(plan).contains(p)
        implies !before.postings.postings.contains(p) by {
        assert(c.after.is_some());
        assert(p==(c.binding,c.after.unwrap(),c.row_id));
        assert(head_key(before.image,c.row_id)!=Some(c.after.unwrap()));
    }
    assert(before.postings.postings.disjoint(destinations(plan)));
    assert(sources(plan).disjoint(destinations(plan)));
}

pub proof fn complete_postings_match_published_row(before:DataState,after:DataState,plan:Plan)
    requires planned(before,plan),
        after.image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3),
        after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan)),
    ensures row_postings_match(after,plan.changes[0].binding,plan.changes[0].row_id),
        head_key(after.image,plan.changes[0].row_id)==plan.changes[0].after,
        forall|binding:usize,key:usize,row:usize|
            binding!=plan.changes[0].binding || row!=plan.changes[0].row_id ==>
            after.postings.postings.contains((binding,key,row))==before.postings.postings.contains((binding,key,row)),
{
    reveal_with_fuel(postings::destinations,2);
    reveal_with_fuel(postings::sources,2);
    let c=plan.changes[0];
    assert(head_key(after.image,c.row_id)==c.after);
    assert forall|key:usize| #[trigger] after.postings.postings.contains((c.binding,key,c.row_id))
        <==> head_key(after.image,c.row_id)==Some(key) by {
        assert(before.postings.postings.contains((c.binding,key,c.row_id)) == (c.before==Some(key)));
    }
}

pub fn prepare_destinations<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan)
    ->(result:Result<Vec<usize>,postings::Error>)
    requires planned(old(storage).view(),*plan),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> installed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> postings::prefix_record(plan.changes@,result->Ok_0@,plan.changes.len() as int),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    proof {planned_derives_destination_ownership(old(storage).view(),*plan);}
    postings::prepare_index_destinations(&mut storage.postings,&plan.changes)
}

pub fn rollback_destinations<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    inserted:&Vec<usize>,Ghost(before):Ghost<DataState>)->(result:Result<(),postings::Error>)
    requires planned(before,*plan),installed(before,old(storage).view(),*plan),
        postings::prefix_record(plan.changes@,inserted@,plan.changes.len() as int),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> unchanged_data(before,final(storage).view()),
        result.is_err() ==> final(storage).view().postings.poisoned,
{
    proof {planned_derives_destination_ownership(before,*plan);}
    let result=postings::rollback_index_destinations(&mut storage.postings,&plan.changes,inserted);
    proof {
        if result.is_ok() {
            assert(storage.view().postings.postings =~= before.postings.postings);
        }
    }
    result
}

pub fn remove_sources<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    Ghost(before):Ghost<DataState>)->(result:Result<(),postings::Error>)
    requires planned(before,*plan),installed(before,old(storage).view(),*plan),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> removed(before,final(storage).view(),*plan),
        result.is_err() ==> final(storage).view().postings.poisoned,
        postings::removed_only(old(storage).view().postings.postings,final(storage).view().postings.postings,sources(*plan)),
{
    postings::remove_index_sources(&mut storage.postings,&plan.changes)
}

pub fn publish_rows<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    Ghost(before):Ghost<DataState>)->(result:Result<(),lookup::Error>)
    requires planned(before,*plan),removed(before,old(storage).view(),*plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures final(storage).postings==old(storage).postings,
        result.is_ok() ==> committed(before,final(storage).view(),*plan),
        result.is_err() ==> final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,0)
            || final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,1),
        old(storage).rows.infallible() ==> result.is_ok(),
{
    let result=publication::publish_prepared_write_set(&mut storage.rows,&plan.record);
    proof {if result.is_ok() {complete_postings_match_published_row(before,storage.view(),*plan);}}
    result
}

pub fn poison<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>)
    ensures final(storage).rows==old(storage).rows,
        final(storage).view().postings==(postings::State{poisoned:true,..old(storage).view().postings}),
{
    storage.postings.poison_indexes();
}

// Data-only composition of the commit interval. The native orchestration and
// its actual failure branches are composed separately by the caller campaign.
pub fn publish_indexed_write<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan)
    ->(result:Result<(),DataError>)
    requires planned(old(storage).view(),*plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    let ghost before=storage.view();
    let inserted=match prepare_destinations(storage,plan) {
        Ok(inserted)=>inserted,
        Err(err)=>return Err(DataError::Posting(err)),
    };
    match remove_sources(storage,plan,Ghost(before)) {
        Ok(())=>{},
        Err(err)=>return Err(DataError::Posting(err)),
    }
    match publish_rows(storage,plan,Ghost(before)) {
        Ok(())=>Ok(()),
        Err(err)=>{poison(storage);Err(DataError::Publication(err))},
    }
}

pub struct OrdinaryPlan {pub tx:ordinary::Transaction,pub indices:Vec<usize>}
pub open spec fn ordinary_matches(plan:Plan,ordinary_plan:OrdinaryPlan)->bool {
    ordinary_plan.indices.len()==1 && ordinary_plan.indices[0]<ordinary_plan.tx.write_set.len()
    && plan.record.writes.len()==1 && ordinary_plan.tx.txid==plan.record.txid
    && ordinary::row_write(ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int])==plan.record.writes[0]
}
pub fn publish_rows_ordinary<P:PostingPrimitives,R:ordinary::OrdinaryStorage>(storage:&mut Storage<P,R>,
    plan:&Plan,ordinary_plan:&OrdinaryPlan,Ghost(before):Ghost<DataState>)
    ->(result:Result<Vec<ordinary::PublishedWrite>,lookup::Error>)
    requires planned(before,*plan),removed(before,old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures final(storage).postings==old(storage).postings,
        result.is_ok() ==> committed(before,final(storage).view(),*plan),
        result.is_ok() ==> result->Ok_0.len()==1 && result->Ok_0[0]==ordinary::report(before.image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_err() ==> final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,0)
            || final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,1),
        old(storage).rows.infallible() ==> result.is_ok(),
{
    let result=ordinary::publish_write_set(&mut storage.rows,&ordinary_plan.tx,&ordinary_plan.indices);
    proof {if result.is_ok() {complete_postings_match_published_row(before,storage.view(),*plan);}}
    result
}
pub fn native_ordinary_data_segment<P:PostingPrimitives,R:ordinary::OrdinaryStorage>(storage:&mut Storage<P,R>,
    plan:&Plan,ordinary_plan:&OrdinaryPlan)->(result:Result<Vec<ordinary::PublishedWrite>,DataError>)
    requires planned(old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> result->Ok_0.len()==1 && result->Ok_0[0]==ordinary::report(old(storage).view().image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    /* NATIVE_ORDINARY_COMMIT_DATA */
}


// A consistency implementation of the posting primitives. Its finite logical
// image is ghost state; it is deliberately not offered as a native index.
pub struct PostingMemory {
    pub logical:Ghost<postings::State>,pub fail_insert:bool,pub fail_remove:bool,pub remove_then_fail:bool,
}
impl PostingPrimitives for PostingMemory {
    open spec fn state(&self)->postings::State {self.logical@}
    fn transactional_insert(&mut self,binding:usize,key:usize,row:usize)->(result:Result<(),postings::Error>) {
        if self.fail_insert {return Err(postings::Error::Primitive);}
        proof {self.logical=Ghost(postings::State{postings:self.logical@.postings.insert((binding,key,row)),..self.logical@});}
        Ok(())
    }
    fn transactional_remove(&mut self,binding:usize,key:&usize,row:&usize)->(result:Result<(),postings::Error>) {
        if !self.fail_remove || self.remove_then_fail {
            proof {self.logical=Ghost(postings::State{postings:self.logical@.postings.remove((binding,*key,*row)),..self.logical@});}
        }
        if self.fail_remove {Err(postings::Error::Primitive)} else {Ok(())}
    }
    fn poison_indexes(&mut self) {
        proof {self.logical=Ghost(postings::State{poisoned:true,..self.logical@});}
    }
}
pub fn admitted_input_witness(before_key:Option<usize>,after_key:Option<usize>)
    ->(result:(Storage<PostingMemory,publication::Memory>,Plan,OrdinaryPlan))
    requires before_key!=after_key,
    ensures planned(result.0.view(),result.1),ordinary_matches(result.1,result.2),
        result.0.rows.authorized(result.1.record.writes[0].row_id,result.1.record.writes[0].new_offset),
        result.1.changes[0].before==before_key,result.1.changes[0].after==after_key,
{
    let base=lookup::Row{xmin:1,xmax:0,next:0,value:before_key,locked:false,owner:0};
    let fresh=lookup::Row{xmin:9,xmax:0,next:0,value:after_key,locked:false,owner:0};
    let head=if before_key.is_some() {1u32} else {0u32};
    let rows=publication::Memory{head,base,fresh};
    let ghost before_held=if before_key.is_some() {Set::<(usize,usize)>::empty().insert((0,before_key.unwrap()))}
        else {Set::<(usize,usize)>::empty()};
    let ghost held=if after_key.is_some() {before_held.insert((0,after_key.unwrap()))} else {before_held};
    let ghost initial_posts=if before_key.is_some() {
        Set::<postings::Posting>::empty().insert((0,before_key.unwrap(),0))
    } else {Set::<postings::Posting>::empty()};
    let postings=PostingMemory{logical:Ghost(postings::State{postings:initial_posts,binding_count:1,held,poisoned:false}),
        fail_insert:false,fail_remove:false,remove_then_fail:false};
    let storage=Storage{postings,rows};
    let change=postings::IndexChange{binding:0,row_id:0,before:before_key,after:after_key};
    let mut changes=Vec::new();changes.push(change);
    let write=publication::CommittedWrite{row_id:0,base_offset:head,new_offset:2};
    let mut writes=Vec::new();writes.push(write);
    let plan=Plan{changes,record:publication::CommitRecord{txid:9,writes}};
    let ordinary_write=ordinary::Write{row_id:0,base_offset:head,new_offset:2,dirty_columns_bitmask:37};
    let mut write_set=Vec::new();write_set.push(ordinary_write);
    let tx=ordinary::Transaction{txid:9,write_set};
    let mut indices=Vec::new();indices.push(0usize);
    let ordinary_plan=OrdinaryPlan{tx,indices};
    proof {
        assert(lookup::image_valid(storage.view().image));
        assert(publication::prepared(storage.view().image,write,9));
        assert(row_postings_match(storage.view(),0,0));
        assert(planned(storage.view(),plan));
    }
    (storage,plan,ordinary_plan)
}
pub fn successful_relation_witness(before_key:Option<usize>,after_key:Option<usize>)
    requires before_key!=after_key,
    ensures exists|before:DataState,after:DataState,plan:Plan|
        planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==before_key && plan.changes[0].after==after_key,
{
    let (storage,plan,ordinary_plan)=admitted_input_witness(before_key,after_key);
    let ghost before=storage.view();
    let ghost after=DataState{
        image:publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3),
        postings:postings::State{postings:before.postings.postings.union(destinations(plan)).difference(sources(plan)),..before.postings},
    };
    proof {
        complete_postings_match_published_row(before,after,plan);
        assert(committed(before,after,plan));
        assert(exists|s0:DataState,s1:DataState,p:Plan| planned(s0,p) && committed(s0,s1,p)
            && p.changes[0].before==before_key && p.changes[0].after==after_key);
    }
}
pub fn create_move_delete_have_witnesses()
    ensures (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==None && plan.changes[0].after==Some(42usize)),
        (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==Some(42usize) && plan.changes[0].after==Some(77usize)),
        (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==Some(42usize) && plan.changes[0].after==None),
{
    successful_relation_witness(None,Some(42));
    successful_relation_witness(Some(42),Some(77));
    successful_relation_witness(Some(42),None);
}

}
