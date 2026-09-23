// Reproved in Verus; no unchecked transfer from Lean or TLC.
verus! {
pub struct Protocol {
    pub clock:u64, pub active:Set<u64>, pub ended:Set<u64>,
    pub published:Map<u64,u64>, pub observed:Map<u64,Set<u64>>,
}
pub enum Action { Start{txid:u64}, End{txid:u64}, Snapshot{reader:u64}, Publish{writer:u64,stamp:u64} }
pub open spec fn protocol_initial(p:Protocol) -> bool {
    p.clock>0 && p.active==Set::<u64>::empty() && p.ended==Set::<u64>::empty()
        && p.published==Map::<u64,u64>::empty() && p.observed==Map::<u64,Set<u64>>::empty()
}
pub open spec fn protocol_valid(p:Protocol) -> bool {
    p.clock>0 && p.active.disjoint(p.ended)
    && (forall|tx:u64| p.active.contains(tx) || p.ended.contains(tx) ==> 0<tx<p.clock)
    && (forall|writer:u64| #[trigger] p.published.contains_key(writer) ==> p.ended.contains(writer)
        && writer<p.published[writer]<p.clock)
    && (forall|reader:u64| p.observed.contains_key(reader) ==> 0<reader<p.clock)
    && (forall|reader:u64,writer:u64| p.observed.contains_key(reader) && p.observed[reader].contains(writer)
        && p.published.contains_key(writer) ==> reader<p.published[writer])
}
pub open spec fn protocol_step(a:Protocol,b:Protocol,action:Action) -> bool {
    match action {
        Action::Start{txid} => a.clock<=txid<u64::MAX
            && b==(Protocol{clock:(txid+1) as u64,active:a.active.insert(txid),..a}),
        Action::End{txid} => a.active.contains(txid)
            && b==(Protocol{active:a.active.remove(txid),ended:a.ended.insert(txid),..a}),
        Action::Snapshot{reader} => a.active.contains(reader)
            && b==(Protocol{observed:a.observed.insert(reader,a.active.remove(reader)),..a}),
        Action::Publish{writer,stamp} => a.ended.contains(writer) && !a.published.contains_key(writer)
            && a.clock<=stamp<u64::MAX
            && b==(Protocol{clock:(stamp+1) as u64,published:a.published.insert(writer,stamp),..a}),
    }
}
pub proof fn protocol_step_preserves(a:Protocol,b:Protocol,action:Action)
    requires protocol_valid(a),protocol_step(a,b,action),
    ensures protocol_valid(b),
{
    match action {
        Action::Start{txid} => {
            assert(!a.ended.contains(txid));
            assert(b.active.disjoint(b.ended));
        },
        Action::Snapshot{reader} => {
            assert forall|who:u64,writer:u64| b.observed.contains_key(who) && b.observed[who].contains(writer)
                && b.published.contains_key(writer) implies who<b.published[writer] by {
                if who==reader {
                    assert(a.active.contains(writer));
                    assert(a.ended.contains(writer));
                }
            }
        },
        _ => {},
    }
}
pub open spec fn protocol_history(states:Seq<Protocol>,actions:Seq<Action>) -> bool {
    states.len()==actions.len()+1 && protocol_initial(states[0])
        && forall|i:int| 0<=i<actions.len() ==> protocol_step(states[i],states[i+1],actions[i])
}
pub proof fn reachable_protocol_valid(states:Seq<Protocol>,actions:Seq<Action>,n:int)
    requires protocol_history(states,actions),0<=n<=actions.len(),
    ensures protocol_valid(states[n]),
    decreases n,
{
    if n>0 {
        reachable_protocol_valid(states,actions,n-1);
        protocol_step_preserves(states[n-1],states[n],actions[n-1]);
    }
}
pub struct Event { pub row:usize, pub before:Option<usize>, pub after:Option<usize>, pub creator:u64, pub stamp:u64 }
pub open spec fn key_matches<P:KeyPredicate>(p:P,key:Option<usize>) -> bool {
    key.is_some() && p.matches(key.unwrap())
}
pub open spec fn affects<P:KeyPredicate>(p:P,e:Event) -> bool {
    key_matches(p,e.before) || key_matches(p,e.after)
}
pub open spec fn event_buckets<P:KeyPredicate>(p:P,e:Event) -> Set<usize> {
    let old = if e.before.is_some() {Set::empty().insert(p.bucket(e.before.unwrap()))} else {Set::empty()};
    if e.after.is_some() {old.insert(p.bucket(e.after.unwrap()))} else {old}
}
pub open spec fn stamp_at<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,b:usize) -> u64
    decreases n,
{
    if n<=0 {initial[b]} else if event_buckets(p,events[n-1]).contains(b) {events[n-1].stamp}
    else {stamp_at(initial,events,n-1,p,b)}
}
pub open spec fn guarded_stamp_history<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P) -> bool {
    forall|i:int,b:usize| 0<=i<events.len() && event_buckets(p,events[i]).contains(b) ==>
        initial.contains_key(b) && stamp_at(initial,events,i,p,b)<=events[i].stamp
}
pub proof fn stamp_history_covers<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,i:int,b:usize)
    requires 0<=i<n<=events.len(),guarded_stamp_history(initial,events,p),event_buckets(p,events[i]).contains(b),
    ensures events[i].stamp<=stamp_at(initial,events,n,p,b),
    decreases n,
{
    if i<n-1 {
        stamp_history_covers(initial,events,n-1,p,i,b);
        if event_buckets(p,events[n-1]).contains(b) {
            assert(stamp_at(initial,events,n-1,p,b)<=events[n-1].stamp);
        }
    }
}
pub proof fn accepted_stamps_force_early_events<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P,
    buckets:Set<usize>,reader:u64)
    requires guarded_stamp_history(initial,events,p),
        forall|value:usize| p.matches(value) ==> buckets.contains(p.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial,events,events.len() as int,p,b)<reader,
    ensures forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<reader,
{
    assert forall|i:int| 0<=i<events.len() && affects(p,events[i]) implies events[i].stamp<reader by {
        let e=events[i];
        let value=if key_matches(p,e.before) {e.before.unwrap()} else {e.after.unwrap()};
        assert(p.matches(value));
        let bucket=p.bucket(value);
        assert(event_buckets(p,e).contains(bucket));
        stamp_history_covers(initial,events,events.len() as int,p,i,bucket);
        assert(buckets.contains(bucket));
        assert(stamp_at(initial,events,events.len() as int,p,bucket)<reader);
    }
}
pub open spec fn live_rows(initial:Map<usize,Option<usize>>, events:Seq<Event>,n:int) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else {live_rows(initial,events,n-1).insert(events[n-1].row,events[n-1].after)}
}
pub open spec fn snapshot_rows(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else if creator_visible(events[n-1].creator,s) {
        snapshot_rows(initial,events,n-1,s).insert(events[n-1].row,events[n-1].after)
    } else {snapshot_rows(initial,events,n-1,s)}
}
pub open spec fn postings(rows:Map<usize,Option<usize>>) -> Set<(usize,usize)> {
    rows.dom().filter(|id:usize| rows[id].is_some()).map(|id:usize| (rows[id].unwrap(),id))
}
pub open spec fn posting_step(before:Set<(usize,usize)>,e:Event) -> Set<(usize,usize)> {
    let removed = if e.before.is_some() {before.remove((e.before.unwrap(),e.row))} else {before};
    if e.after.is_some() {removed.insert((e.after.unwrap(),e.row))} else {removed}
}
pub open spec fn posting_replay(initial:Set<(usize,usize)>,events:Seq<Event>,n:int) -> Set<(usize,usize)>
    decreases n,
{
    if n<=0 {initial} else {posting_step(posting_replay(initial,events,n-1),events[n-1])}
}
pub open spec fn coherent(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int) -> bool {
    forall|i:int| 0<=i<n ==> initial.contains_key(events[i].row)
        && events[i].before==live_rows(initial,events,i)[events[i].row]
}
pub proof fn live_domain(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(), coherent(initial,events,n),
    ensures live_rows(initial,events,n).dom()==initial.dom(),
    decreases n,
{
    if n>0 {
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        assert(live_rows(initial,events,n).dom() =~= initial.dom());
    }
}
pub proof fn posting_update_exact(rows:Map<usize,Option<usize>>,e:Event)
    requires rows.contains_key(e.row),rows[e.row]==e.before,
    ensures posting_step(postings(rows),e)==postings(rows.insert(e.row,e.after)),
{
    assert forall|pair:(usize,usize)| posting_step(postings(rows),e).contains(pair)
        == postings(rows.insert(e.row,e.after)).contains(pair) by {
        posting_membership(rows,pair.0,pair.1);
        posting_membership(rows.insert(e.row,e.after),pair.0,pair.1);
    }
    assert(posting_step(postings(rows),e) =~= postings(rows.insert(e.row,e.after)));
}
pub proof fn posting_membership(rows:Map<usize,Option<usize>>,key:usize,id:usize)
    ensures postings(rows).contains((key,id)) <==> rows.contains_key(id) && rows[id]==Some(key),
{
    if postings(rows).contains((key,id)) {
        let r=choose|r:usize| rows.dom().filter(|r:usize|rows[r].is_some()).contains(r)
            && (rows[r].unwrap(),r)==(key,id);
        assert(r==id);
    } else if rows.contains_key(id) && rows[id]==Some(key) {
        assert(rows.dom().filter(|r:usize|rows[r].is_some()).contains(id));
    }
}
pub proof fn posting_history_exact(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(),coherent(initial,events,n),
    ensures posting_replay(postings(initial),events,n)==postings(live_rows(initial,events,n)),
    decreases n,
{
    if n>0 {
        posting_history_exact(initial,events,n-1);
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        posting_update_exact(live_rows(initial,events,n-1),events[n-1]);
    }
}
// These numeric facts are consequences of shared-clock/lifecycle history in
// the checked composition, not an assumption of historical candidate coverage.
pub open spec fn event_chronology(s:Snapshot,e:Event) -> bool {
    e.creator < e.stamp && (s.active.contains(e.creator) ==> e.stamp > s.txid)
}
pub proof fn event_chronology_from_history(states:Seq<Protocol>,actions:Seq<Action>,s:Snapshot,e:Event)
    requires protocol_history(states,actions),
        states.last().observed.contains_key(s.txid),s.active.to_set()==states.last().observed[s.txid],
        states.last().published.contains_key(e.creator),states.last().published[e.creator]==e.stamp,
    ensures event_chronology(s,e),
{
    reachable_protocol_valid(states,actions,actions.len() as int);
    let p=states.last();
    assert(e.creator<p.published[e.creator]);
    if s.active.contains(e.creator) {
        assert(p.observed[s.txid].contains(e.creator));
        assert(s.txid<p.published[e.creator]);
    }
}
pub proof fn early_publication_is_visible(s:Snapshot,e:Event)
    requires s.txid<s.xmax,event_chronology(s,e),e.stamp<s.txid,
    ensures creator_visible(e.creator,s),
{}
pub proof fn accepted_replay_matches<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot,p:P)
    requires 0<=n<=events.len(),coherent(initial,events,n),s.txid<s.xmax,
        forall|i:int| 0<=i<n ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<n && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) ==>
        key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]),
    decreases n,
{
    if n>0 {
        accepted_replay_matches(initial,events,n-1,s,p);
        let e=events[n-1];
        if affects(p,e) {early_publication_is_visible(s,e);}
        assert forall|id:usize| initial.contains_key(id) implies
            key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]) by {
            if id==e.row && !creator_visible(e.creator,s) {
                assert(!affects(p,e));
                assert(e.before==live_rows(initial,events,n-1)[id]);
            }
        }
    }
}
pub open spec fn raw_candidates<P:KeyPredicate>(p:P,index:Set<(usize,usize)>) -> Set<usize> {
    index.filter(|pair:(usize,usize)|p.matches(pair.0)).map(|pair:(usize,usize)|pair.1)
}
pub proof fn accepted_history_has_candidates<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,s:Snapshot,p:P)
    requires coherent(initial,events,events.len() as int),s.txid<s.xmax,
        forall|i:int| 0<=i<events.len() ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) ==>
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id),
{
    posting_history_exact(initial,events,events.len() as int);
    live_domain(initial,events,events.len() as int);
    accepted_replay_matches(initial,events,events.len() as int,s,p);
    assert forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) implies
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id) by {
        let value=live_rows(initial,events,events.len() as int)[id].unwrap();
        posting_membership(live_rows(initial,events,events.len() as int),value,id);
        assert(postings(live_rows(initial,events,events.len() as int)).contains((value,id)));
        assert(posting_replay(postings(initial),events,events.len() as int)
            .filter(|pair:(usize,usize)|p.matches(pair.0)).contains((value,id)));
    }
}
pub proof fn absent_own_row_has_no_write(writes:Seq<PendingWrite>,id:usize,n:int)
    requires 0<=n<=writes.len(),!own_ids(writes,n).contains(id),
    ensures last_write(writes,id,n).is_none(),
    decreases n,
{
    if n>0 {absent_own_row_has_no_write(writes,id,n-1);}
}
pub proof fn matched_row_comes_from_snapshot_or_own<P:KeyPredicate>(s:Image,tx:Transaction,p:P,id:usize)
    requires matches_row(s,tx,p,id),!own_ids(tx.write_set@,tx.write_set.len() as int).contains(id),
    ensures key_matches(p,stored_snapshot_key(s,tx,id)),
{
    absent_own_row_has_no_write(tx.write_set@,id,tx.write_set.len() as int);
}
// Retained MVCC cells correspond to the visible-event history, while raw lookup
// enumerates current maintained postings. These are distinct storage mappings;
// neither is a premise that historical candidates are already complete.
pub open spec fn history_maps_snapshot(image:Image,tx:Transaction,initial:Map<usize,Option<usize>>,events:Seq<Event>) -> bool {
    forall|id:usize| id<image.capacity ==> initial.contains_key(id)
        && stored_snapshot_key(image,tx,id)==snapshot_rows(initial,events,events.len() as int,snapshot(tx))[id]
}
pub fn materialize_after_checked_history<D:Storage,C:CandidateSet,P:KeyPredicate>(driver:&D,tx:&mut Transaction,
    candidates:Vec<usize>,predicate:&P,
    Ghost(initial):Ghost<Map<usize,Option<usize>>>,Ghost(events):Ghost<Seq<Event>>,
    Ghost(protocol):Ghost<Seq<Protocol>>,Ghost(actions):Ghost<Seq<Action>>,
    Ghost(initial_stamps):Ghost<Map<usize,u64>>,Ghost(buckets):Ghost<Set<usize>>)
    -> (r:Result<Vec<usize>,Error>)
    requires transaction_valid(driver.image(),*old(tx)),
        coherent(initial,events,events.len() as int),history_maps_snapshot(driver.image(),*old(tx),initial,events),
        protocol_history(protocol,actions),protocol.last().observed.contains_key(old(tx).txid),
        old(tx).snapshot_active@.to_set()==protocol.last().observed[old(tx).txid],
        forall|i:int| 0<=i<events.len() ==> protocol.last().published.contains_key(events[i].creator)
            && protocol.last().published[events[i].creator]==events[i].stamp,
        guarded_stamp_history(initial_stamps,events,*predicate),
        forall|value:usize| predicate.matches(value) ==> buckets.contains(predicate.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial_stamps,events,events.len() as int,*predicate,b)<old(tx).txid,
        raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).subset_of(candidates@.to_set()),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        r.is_ok() ==> forall|id:usize| r.unwrap()@.contains(id) <==> matches_row(driver.image(),*old(tx),*predicate,id),
        r.is_ok() ==> forall|i:int,j:int| 0<=i<j<r.unwrap().len() ==> r.unwrap()[i]<r.unwrap()[j],
{
    proof {
        assert forall|i:int| 0<=i<events.len() implies event_chronology(snapshot(*tx),events[i]) by {
            event_chronology_from_history(protocol,actions,snapshot(*tx),events[i]);
        }
        accepted_stamps_force_early_events(initial_stamps,events,*predicate,buckets,tx.txid);
        accepted_history_has_candidates(initial,events,snapshot(*tx),*predicate);
        assert forall|id:usize| matches_row(driver.image(),*tx,*predicate,id) implies
            candidates@.contains(id) || own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) by {
            if !own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) {
                matched_row_comes_from_snapshot_or_own(driver.image(),*tx,*predicate,id);
                assert(initial.contains_key(id));
                assert(key_matches(*predicate,snapshot_rows(initial,events,events.len() as int,snapshot(*tx))[id]));
                assert(raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).contains(id));
                assert(candidates@.to_set().contains(id));
            }
        }
    }
    materialize::<D,C,P>(driver,tx,candidates,predicate)
}
pub struct EqualsKey {pub key:usize}
impl KeyPredicate for EqualsKey {
    open spec fn matches(&self,value:usize) -> bool {value==self.key}
    open spec fn bucket(&self,_value:usize) -> usize {0}
    fn evaluate(&self,value:&Option<usize>) -> (r:bool) {
        match value {Some(key)=>*key==self.key,None=>false}
    }
}
pub proof fn query_contract_has_success_witness() {
    let p0=Protocol{clock:1,active:Set::empty(),ended:Set::empty(),published:Map::empty(),observed:Map::empty()};
    let p1=Protocol{clock:2,active:Set::empty().insert(1),..p0};
    let p2=Protocol{active:Set::empty(),ended:Set::empty().insert(1),..p1};
    let p3=Protocol{clock:3,published:Map::empty().insert(1,2),..p2};
    let p4=Protocol{clock:4,active:Set::empty().insert(3),..p3};
    let p5=Protocol{observed:Map::empty().insert(3,Set::empty()),..p4};
    let states=seq![p0,p1,p2,p3,p4,p5];
    let actions=seq![Action::Start{txid:1},Action::End{txid:1},Action::Publish{writer:1,stamp:2},
        Action::Start{txid:3},Action::Snapshot{reader:3}];
    assert(p1.active.remove(1) =~= Set::<u64>::empty());
    assert(p4.active.remove(3) =~= Set::<u64>::empty());
    assert forall|i:int| 0<=i<5 implies protocol_step(states[i],states[i+1],actions[i]) by {
        if i==0 {} else if i==1 {} else if i==2 {} else if i==3 {} else {assert(i==4);}
    }
    assert(protocol_history(states,actions));
    reachable_protocol_valid(states,actions,5);
    let s=Snapshot{txid:3,xmin:3,xmax:4,active:Seq::empty()};
    assert(s.active.to_set() =~= Set::<u64>::empty());
    let e=Event{row:0,before:Some(10),after:Some(42),creator:1,stamp:2};
    let initial=Map::empty().insert(0,Some(10));
    let events=seq![e];
    let p=EqualsKey{key:42};
    event_chronology_from_history(states,actions,s,e);
    assert(coherent(initial,events,1));
    assert(guarded_stamp_history(Map::empty().insert(0,0),events,p));
    assert(stamp_at(Map::empty().insert(0,0),events,1,p,0)<s.txid);
    accepted_history_has_candidates(initial,events,s,p);
    assert(snapshot_rows(initial,events,1,s)[0]==Some(42));
    assert(raw_candidates(p,posting_replay(postings(initial),events,1)).contains(0));
}
}
