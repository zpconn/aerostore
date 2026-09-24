use scenario::{lifecycle, predicate};
use lifecycle::LifecyclePrimitives;
use predicate::Primitives;
verus! {

// The registration field is extracted from the same native transaction that
// supplies the write plan. Its ownership before this cut remains explicit.
pub struct FinishToken {pub registration:Option<lifecycle::ProcArrayRegistration>}

pub fn finish_transaction<L:LifecyclePrimitives>(driver:&mut L,tx:&mut FinishToken)
    ->(result:Result<(),lifecycle::ProcArrayError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        old(tx).registration.is_some() ==> old(tx).registration.unwrap().txid>0,
    ensures lifecycle::well_formed(final(driver).state()),!final(driver).state().lifecycle_held,
        final(tx).registration.is_none(),
        final(driver).state().clock==old(driver).state().clock,
        final(driver).state().reservations==old(driver).state().reservations,
        old(tx).registration.is_none() ==> final(driver).state()==old(driver).state() && result.is_ok(),
        old(tx).registration.is_some() && old(tx).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS
            && old(driver).state().slots[old(tx).registration.unwrap().slot_idx as int].txid==old(tx).registration.unwrap().txid
            ==> result.is_ok(),
        result.is_ok() && old(tx).registration.is_some() ==>
            old(tx).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS
            && old(driver).state().slots[old(tx).registration.unwrap().slot_idx as int].txid==old(tx).registration.unwrap().txid
            && final(driver).state().slots==old(driver).state().slots.update(old(tx).registration.unwrap().slot_idx as int,
                lifecycle::Slot{txid:0,snapshot_xmin:0}),
        result.is_err() ==> final(driver).state().slots==old(driver).state().slots,
{
    /* NATIVE_FINISH */
}

pub open spec fn translated_change(c:postings::IndexChange)->predicate::IndexChange {
    predicate::IndexChange{binding:c.binding,before:c.before,after:c.after}
}
pub fn publication_changes(plan:&Plan)->(changes:Vec<predicate::IndexChange>)
    requires plan.changes.len()==1,
    ensures changes.len()==1 && changes[0]==translated_change(plan.changes[0]),
{
    let c=&plan.changes[0];
    let mut changes=Vec::new();
    changes.push(predicate::IndexChange{binding:c.binding,before:c.before,after:c.after});
    changes
}
pub open spec fn affected(s:predicate::State,plan:Plan)->Set<predicate::Pair> {
    predicate::change_keys(s,seq![translated_change(plan.changes[0])],1)
}
pub open spec fn completion_entry(life:lifecycle::State,index:predicate::State,writer:lifecycle::ProcArrayRegistration,
    data:DataState,plan:Plan)->bool {
    lifecycle::well_formed(life) && !life.lifecycle_held && life.clock<u64::MAX
    && writer.txid>0 && writer.txid==plan.record.txid
    && plan.changes.len()==1 && plan.changes[0].binding<index.binding_count
    && data.postings.binding_count==index.binding_count
    && affected(index,plan).subset_of(index.held)
}
#[derive(PartialEq,Eq)]
pub enum CompletionError {Data,Deregistration,Stamp}
pub struct Completion {pub writes:Vec<ordinary::PublishedWrite>,pub stamp:u64}

// This is a cutpoint composition: the incoming plan has already been validated
// under row/index guards. Index stamp and row/posting views denote the same
// registered native index; physical aliasing and guarded framing are explicit
// primitive obligations. Intervening local-set/recycler cleanup is assumed to
// preserve these published projections; that cleanup is not proved here.
pub fn publish_then_complete<P:postings::PostingPrimitives,R:ordinary::OrdinaryStorage,
    L:scenario::ScenarioClock,I:scenario::IndexPrimitives,C:predicate::PairSet>(
    storage:&mut Storage<P,R>,publisher:&mut scenario::Bridge<L,I>,token:&mut FinishToken,
    plan:&Plan,ordinary_plan:&OrdinaryPlan)->(result:Result<Completion,CompletionError>)
    requires planned(old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
        old(token).registration.is_some(),
        old(publisher).writer_txid==old(token).registration.unwrap().txid,
        completion_entry(old(publisher).lifecycle.state(),old(publisher).state(),old(token).registration.unwrap(),
            old(storage).view(),*plan),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> final(token).registration.is_none() && final(publisher).state().deregistered,
        result.is_ok() ==> result->Ok_0.writes.len()==1 && result->Ok_0.writes[0]==ordinary::report(old(storage).view().image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_ok() ==> result->Ok_0.stamp>=old(publisher).state().clock && result->Ok_0.stamp< u64::MAX,
        result.is_ok() ==> result->Ok_0.stamp>plan.record.txid,
        result.is_ok() ==> predicate::stamp_relation(old(publisher).state().stamps,final(publisher).state().stamps,
            affected(old(publisher).state(),*plan),result->Ok_0.stamp),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
        result==Err(CompletionError::Deregistration) || result==Err(CompletionError::Stamp) ==>
            final(storage).view().postings.poisoned && final(token).registration.is_none(),
{
    let ghost initial_data=storage.view();
    let ghost initial_index=publisher.state();
    let changes=publication_changes(plan);
    let writes=match native_ordinary_data_segment(storage,plan,ordinary_plan) {
        Ok(writes)=>writes,
        Err(_)=>return Err(CompletionError::Data),
    };
    let ghost before_end=publisher.lifecycle.state();
    let writer=token.registration.unwrap();
    proof {assert(committed(initial_data,storage.view(),*plan));}
    match finish_transaction(&mut publisher.lifecycle,token) {
        Err(_)=>{poison(storage);return Err(CompletionError::Deregistration);},
        Ok(())=>{},
    }
    proof {
        scenario::cleared_writer_is_absent(before_end,publisher.lifecycle.state(),writer);
        assert(publisher.state().deregistered);
        scenario::change_keys_ignore_clock(initial_index,publisher.state(),changes@,1);
        assert(changes@==seq![translated_change(plan.changes[0])]);
    }
    match predicate::publish_index_stamps::<scenario::Bridge<L,I>,C>(publisher,&changes) {
        Err(_)=>{poison(storage);return Err(CompletionError::Stamp);},
        Ok(())=>{},
    }
    Ok(Completion{writes,stamp:publisher.reserved_stamp})
}

pub fn owned_registration_finishes<L:LifecyclePrimitives>(driver:&mut L,token:&mut FinishToken)
    ->(result:Result<(),lifecycle::ProcArrayError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        old(token).registration.is_some(),old(token).registration.unwrap().txid>0,
        old(token).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS,
        old(driver).state().slots[old(token).registration.unwrap().slot_idx as int].txid==old(token).registration.unwrap().txid,
    ensures result.is_ok(),final(token).registration.is_none(),
        forall|i:int| 0<=i<final(driver).state().slots.len() ==>
            final(driver).state().slots[i].txid!=old(token).registration.unwrap().txid,
{
    let ghost before=driver.state();
    let registration=token.registration.unwrap();
    let result=finish_transaction(driver,token);
    proof {scenario::cleared_writer_is_absent(before,driver.state(),registration);}
    result
}

pub proof fn completion_entry_has_live_witness(data:DataState,plan:Plan)
    requires planned(data,plan),plan.record.txid<u64::MAX-1,
    ensures exists|life:lifecycle::State,index:predicate::State,writer:lifecycle::ProcArrayRegistration|
        completion_entry(life,index,writer,data,plan)
        && writer.slot_idx<lifecycle::PROCARRAY_SLOTS && life.slots[writer.slot_idx as int].txid==writer.txid,
{
    let id=plan.record.txid;
    let empty=Seq::new(lifecycle::PROCARRAY_SLOTS as nat,|i:int|lifecycle::Slot{txid:0,snapshot_xmin:0});
    let life=lifecycle::State{slots:empty.update(0,lifecycle::Slot{txid:id,snapshot_xmin:id}),
        clock:(id+1) as u64,sampled_clock:(id+1) as u64,reservations:seq![id],lifecycle_held:false};
    let c=plan.changes[0];
    let buckets=Map::empty();
    let buckets=if c.before.is_some() {buckets.insert((c.binding,c.before.unwrap()),0usize)} else {buckets};
    let buckets=if c.after.is_some() {buckets.insert((c.binding,c.after.unwrap()),0usize)} else {buckets};
    let index=predicate::State{arena:1,bindings:Map::empty().insert(7usize,c.binding),
        binding_count:data.postings.binding_count,key_buckets:buckets,
        stamps:Map::empty().insert((c.binding,0usize),0u64),held:Set::empty().insert((c.binding,0usize)),
        clock:(id+1) as u64,reserved_stamp:0,reservations:seq![id],deregistered:false};
    let writer=lifecycle::ProcArrayRegistration{slot_idx:0,txid:id};
    reveal_with_fuel(predicate::change_keys,2);
    reveal(postings::valid);
    assert(postings::change_valid(data.postings,c));
    assert(lifecycle::well_formed(life));
    assert(completion_entry(life,index,writer,data,plan));
}

// A successfully returned writer invalidates a previously captured dependency
// in every affected bucket. The stamp is the actual reservation returned above,
// not a caller-supplied conflict conclusion or synthetic second reservation.
pub proof fn completed_write_invalidates_capture(before:predicate::State,after:predicate::State,
    plan:Plan,read:predicate::IndexRead,reader:u64,stamp:u64)
    requires before.bindings==after.bindings,before.bindings.contains_key(read.index_offset),
        affected(before,plan).contains((before.bindings[read.index_offset],read.bucket)),
        predicate::stamp_relation(before.stamps,after.stamps,affected(before,plan),stamp),
        reader<=before.clock,before.clock<=stamp,
    ensures !predicate::read_good(after,read,reader),
{}
}
