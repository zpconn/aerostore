use planning::PlanningPrimitives;
use admission::GuardedRead;
verus! {

pub open spec fn final_pending(tx:ordinary::Transaction)->ordinary::Write {
    tx.write_set[tx.write_set.len()-1]
}
pub open spec fn one_pending_row(tx:ordinary::Transaction)->bool {
    tx.write_set.len()>0
    && forall|i:int| 0<=i<tx.write_set.len() ==> tx.write_set[i].row_id==final_pending(tx).row_id
}
pub open spec fn pending_input(data:DataState,tx:ordinary::Transaction)->bool {
    one_pending_row(tx) && final_pending(tx).base_offset!=0
    && (forall|i:int| 0<=i<tx.write_set.len() ==>
        admission::fresh_private(data.image,tx.write_set[i],tx.txid)
        && tx.write_set[i].base_offset==final_pending(tx).base_offset)
    && (forall|i:int,j:int| 0<=i<j<tx.write_set.len() ==>
        tx.write_set[i].new_offset!=tx.write_set[j].new_offset)
    && data.postings.binding_count==1 && !data.postings.poisoned
    && row_postings_match(data,0,final_pending(tx).row_id)
    && {let c=planning::expected_change(data.image,tx,(tx.write_set.len()-1) as usize);
        c.before!=c.after && postings::change_valid(data.postings,c)}
}
pub open spec fn selected_touches(index:predicate::State,image:lookup::Image,tx:ordinary::Transaction)->Set<predicate::Pair> {
    predicate::change_keys(index,seq![translated_change(planning::expected_change(image,tx,(tx.write_set.len()-1) as usize))],1)
}
pub open spec fn selected_postings(data:DataState,tx:ordinary::Transaction)->Set<postings::Posting> {
    let c=planning::expected_change(data.image,tx,(tx.write_set.len()-1) as usize);
    data.postings.postings.union(postings::option_posting(c,c.after)).difference(postings::option_posting(c,c.before))
}
pub open spec fn planned_context(life:lifecycle::State,index:predicate::State,registration:lifecycle::ProcArrayRegistration,
    data:DataState,tx:ordinary::Transaction)->bool {
    lifecycle::well_formed(life) && !life.lifecycle_held && life.clock<u64::MAX
    && registration.txid==tx.txid && tx.txid>0 && index.binding_count==1
    && selected_touches(index,data.image,tx).subset_of(index.held)
}
#[derive(PartialEq,Eq)]
pub enum PlannedError {Planning,BaseStorage,Conflict,Completion}

pub fn selected_record_write(selected:&ordinary::Write)->(write:publication::CommittedWrite)
    ensures write==ordinary::row_write(*selected),
{
    publication::CommittedWrite{row_id:selected.row_id,base_offset:selected.base_offset,new_offset:selected.new_offset}
}

// The precondition supplies private allocation/guard authority and a coherent
// acquired image. It does not supply complete indices, an extracted key plan,
// current-head/base equality, zero base xmax or the publication result.
// Native pre-lock extraction observes immutable pinned values; its relation
// across acquisition is addressed by the field-frame lemma below, not by
// pretending the whole database remains frozen while locks are acquired.
// Predicate/read/owner validation and native caller error cleanup are omitted
// protocol boundaries. An early result describes this interval before cleanup,
// not the registration state returned by the complete native commit API.
pub fn planned_commit<P:postings::PostingPrimitives,R:PlanningPrimitives+GuardedRead,
    M:planning::RowMapPrimitives,L:scenario::ScenarioClock,I:scenario::IndexPrimitives,C:predicate::PairSet>(
    storage:&mut Storage<P,R>,publisher:&mut scenario::Bridge<L,I>,token:&mut FinishToken,
    tx:ordinary::Transaction)->(result:Result<Completion,PlannedError>)
    requires pending_input(old(storage).view(),tx),old(storage).rows.bindings()==1,
        old(storage).rows.authorized(final_pending(tx).row_id,final_pending(tx).new_offset),
        old(token).registration.is_some(),old(publisher).writer_txid==tx.txid,
        planned_context(old(publisher).lifecycle.state(),old(publisher).state(),old(token).registration.unwrap(),old(storage).view(),tx),
    ensures result.is_ok() ==> final(storage).view().image==publication::publication_prefix(old(storage).view().image,
            ordinary::row_write(final_pending(tx)),tx.txid,3),
        result.is_ok() ==> row_postings_match(final(storage).view(),0,final_pending(tx).row_id),
        result.is_ok() ==> final(storage).view().postings.postings==selected_postings(old(storage).view(),tx)
            && !final(storage).view().postings.poisoned,
        result.is_ok() ==> result->Ok_0.writes.len()==1 && result->Ok_0.writes[0]==ordinary::report(old(storage).view().image,final_pending(tx)),
        result.is_ok() ==> final(token).registration.is_none() && final(publisher).state().deregistered,
        result.is_ok() ==> result->Ok_0.stamp>tx.txid && result->Ok_0.stamp>=old(publisher).state().clock,
        result.is_ok() ==> predicate::stamp_relation(old(publisher).state().stamps,final(publisher).state().stamps,
            selected_touches(old(publisher).state(),old(storage).view().image,tx),result->Ok_0.stamp),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
        result==Err(PlannedError::Planning) || result==Err(PlannedError::BaseStorage) || result==Err(PlannedError::Conflict) ==>
            final(storage).view()==old(storage).view() && final(token).registration==old(token).registration
                && final(publisher).state()==old(publisher).state(),
{
    let indices=planning::final_write_indices::<M>(&tx);
    proof {planning::selected_one_row(tx,indices@);}
    let changes=match planning::index_changes(&storage.rows,&tx,&indices) {
        Ok(changes)=>changes,
        Err(_)=>return Err(PlannedError::Planning),
    };
    proof {
        assert(admission::selection_input(&storage.rows,tx,indices@));
    }
    match admission::has_write_base_conflict(&storage.rows,&tx,&indices) {
        Ok(false)=>{},
        Ok(true)=>return Err(PlannedError::Conflict),
        Err(_)=>return Err(PlannedError::BaseStorage),
    }
    proof {assert(admission::selected_valid(storage.view().image,tx,indices@)) by {};}
    let selected=&tx.write_set[indices[0]];
    let mut writes=Vec::new();
    writes.push(selected_record_write(selected));
    let plan=Plan{changes,record:publication::CommitRecord{txid:tx.txid,writes}};
    let ordinary_plan=OrdinaryPlan{tx,indices};
    proof {
        assert(plan.changes@==seq![planning::expected_change(storage.view().image,ordinary_plan.tx,ordinary_plan.indices[0])]);
        assert(admission::change_matches(storage.view().image,final_pending(ordinary_plan.tx),plan.changes[0]));
        assert(postings::valid(storage.view().postings,plan.changes@));
        admission::validation_establishes_plan(storage.view(),plan,ordinary_plan);
        assert(completion_entry(publisher.lifecycle.state(),publisher.state(),token.registration.unwrap(),storage.view(),plan));
        reveal_with_fuel(postings::destinations,2);
        reveal_with_fuel(postings::sources,2);
        assert(destinations(plan)==postings::option_posting(plan.changes[0],plan.changes[0].after));
        assert(sources(plan)==postings::option_posting(plan.changes[0],plan.changes[0].before));
    }
    match publish_then_complete::<P,R,L,I,C>(storage,publisher,token,&plan,&ordinary_plan) {
        Ok(completion)=>Ok(completion),
        Err(_)=>Err(PlannedError::Completion),
    }
}

// Native extraction precedes acquisition. A head or base xmax may change while
// waiting; only the retained immutable value fields used by extraction must
// agree. Base validation observes the later image and rejects stale geometry.
pub open spec fn key_values_frame(before:lookup::Image,after:lookup::Image,w:ordinary::Write)->bool {
    before.rows.contains_key(w.base_offset) && after.rows.contains_key(w.base_offset)
    && before.rows.contains_key(w.new_offset) && after.rows.contains_key(w.new_offset)
    && before.rows[w.base_offset].value==after.rows[w.base_offset].value
    && before.rows[w.new_offset].value==after.rows[w.new_offset].value
}
pub proof fn extraction_survives_head_interference(before:lookup::Image,after:lookup::Image,
    tx:ordinary::Transaction,indices:Seq<usize>,changes:Seq<postings::IndexChange>)
    requires planning::extracted(before,tx,indices,changes),
        key_values_frame(before,after,tx.write_set[indices[0] as int]),
    ensures planning::extracted(after,tx,indices,changes),
{}

pub proof fn stale_base_cannot_enter_publication(image:lookup::Image,tx:ordinary::Transaction,indices:Seq<usize>)
    requires planning::selected(tx,indices),one_pending_row(tx),
        image.heads[final_pending(tx).row_id]!=final_pending(tx).base_offset
            || image.rows[final_pending(tx).base_offset].xmax!=0 && final_pending(tx).base_offset!=0,
    ensures !admission::selected_valid(image,tx,indices),
{
    planning::selected_one_row(tx,indices);
}

pub fn repeated_input_has_live_witness()->(result:(Ghost<DataState>,ordinary::Transaction))
    ensures pending_input(result.0@,result.1),result.1.write_set.len()==2,
{
    let ghost base=lookup::Row{xmin:1,xmax:0,next:0,value:Some(10),locked:false,owner:0};
    let ghost fresh=lookup::Row{xmin:9,xmax:0,next:1,value:Some(30),locked:false,owner:0};
    let ghost skipped=lookup::Row{xmin:9,xmax:0,next:1,value:Some(20),locked:false,owner:0};
    let ghost image=lookup::Image{heads:Map::empty().insert(0usize,1u32),
        rows:Map::empty().insert(1u32,base).insert(2u32,fresh).insert(3u32,skipped),
        rank:Map::empty().insert(1u32,0nat).insert(3u32,1nat).insert(2u32,2nat),capacity:1};
    let ghost data=DataState{image,postings:postings::State{binding_count:1,
        postings:Set::empty().insert((0usize,10usize,0usize)),
        held:Set::empty().insert((0usize,10usize)).insert((0usize,30usize)),poisoned:false}};
    let mut writes=Vec::new();
    writes.push(ordinary::Write{row_id:0,base_offset:1,new_offset:3,dirty_columns_bitmask:1});
    writes.push(ordinary::Write{row_id:0,base_offset:1,new_offset:2,dirty_columns_bitmask:2});
    let tx=ordinary::Transaction{txid:9,write_set:writes};
    assert(pending_input(data,tx));
    (Ghost(data),tx)
}
}
