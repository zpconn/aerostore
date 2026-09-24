// Checked composition of exact native dependency capture, owned guard release,
// history-based candidate coverage, MVCC materialization and validation.
mod ownership { /* OWNERSHIP_MODULE */ }
mod lookup { /* LOOKUP_MODULE */ }
mod capture { /* CAPTURE_MODULE */ }
mod predicate { /* PREDICATE_MODULE */ }
use vstd::prelude::*;
use ownership::IndexAtomics;
use lookup::{Storage, KeyPredicate};
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub struct History {
    pub initial: Map<usize, Option<usize>>,
    pub events: Seq<lookup::Event>,
    pub protocol: Seq<lookup::Protocol>,
    pub actions: Seq<lookup::Action>,
    pub initial_stamps: Map<usize, u64>,
}
pub open spec fn history_valid<P: KeyPredicate>(h: History, image: lookup::Image,
    tx: lookup::Transaction, p: P, bucket: usize) -> bool {
    lookup::transaction_valid(image, tx)
    && lookup::coherent(h.initial, h.events, h.events.len() as int)
    && lookup::history_maps_snapshot(image, tx, h.initial, h.events)
    && lookup::protocol_history(h.protocol, h.actions)
    && h.protocol.last().observed.contains_key(tx.txid)
    && tx.snapshot_active@.to_set() == h.protocol.last().observed[tx.txid]
    && (forall|i: int| 0 <= i < h.events.len() ==>
        h.protocol.last().published.contains_key(h.events[i].creator)
        && h.protocol.last().published[h.events[i].creator] == h.events[i].stamp)
    && lookup::guarded_stamp_history(h.initial_stamps, h.events, p)
    && h.initial_stamps.contains_key(bucket)
    && (forall|value: usize| p.matches(value) ==> p.bucket(value) == bucket)
}
pub open spec fn represented_stamp<P: KeyPredicate>(h: History, p: P, bucket: usize) -> u64 {
    lookup::stamp_at(h.initial_stamps, h.events, h.events.len() as int, p, bucket)
}

// Native storage/history correspondence is deliberately an explicit primitive.
// A projection is selected by the actual acquired lease, not an API-entry view
// frozen across waiting. It contains no historical-candidate-completeness claim.
// The retained image may describe later physical versions; its snapshot keys
// must map to this capture history. Arbitrary native history mapping is open.
pub trait SlicePrimitives<P: KeyPredicate>: ownership::IndexAtomics + lookup::Storage {
    // This adapter represents one concrete transaction snapshot. Without this
    // context, an arbitrary snapshot_active vector with the same txid could
    // demand contradictory observed sets from the same guarded history.
    spec fn operation_snapshot(&self) -> lookup::Snapshot;
    spec fn operation_index_offset(&self) -> usize;
    spec fn key_bucket(&self, header: usize, value: usize) -> usize;
    spec fn history(&self, lease: &ownership::ReadLease) -> History;
    proof fn guarded_correspondence(&self, tx: lookup::Transaction, p: P,
        lease: &ownership::ReadLease, tracked authority: &ownership::Authority)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).0 == self.arena(),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            lookup::transaction_valid(self.image(), tx),
            lookup::snapshot(tx) == self.operation_snapshot(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
            forall|value: usize| p.matches(value) ==> p.bucket(value) == ownership::lease_key(lease).2,
        ensures history_valid(self.history(lease), self.image(), tx, p, ownership::lease_key(lease).2);
    fn guarded_stamp(&self, p: &P, lease: &ownership::ReadLease,
        Tracked(authority): Tracked<&ownership::Authority>) -> (r: Result<u64, capture::Error>)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
        ensures r.is_ok() ==> r.unwrap() == represented_stamp(self.history(lease), *p, ownership::lease_key(lease).2),
            r.is_err() ==> r == Err(capture::Error::Index);
    fn raw_lookup(&self, p: &P, lease: &ownership::ReadLease,
        Tracked(authority): Tracked<&ownership::Authority>) -> (r: Result<Vec<usize>, lookup::Error>)
        requires ownership::valid(authority), ownership::authorized(authority, lease),
            ownership::lease_key(lease).1 == self.operation_index_offset(),
            forall|value:usize| p.bucket(value) == self.key_bucket(ownership::lease_key(lease).1, value),
        ensures r.is_ok() ==> lookup::raw_candidates(*p, lookup::posting_replay(
            lookup::postings(self.history(lease).initial), self.history(lease).events,
            self.history(lease).events.len() as int)).subset_of(r.unwrap()@.to_set());
}

pub struct BorrowedIndex<'a, D: SlicePrimitives<P>, P: KeyPredicate> {
    pub driver: &'a D,
    pub query: &'a P,
    pub lease: &'a ownership::ReadLease,
    pub authority: Tracked<&'a ownership::Authority>,
    pub offset: usize,
    pub binding: usize,
    pub binding_count: usize,
    pub bucket: usize,
}
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> BorrowedIndex<'a, D, P> {
    pub open spec fn owned(&self) -> bool {
        ownership::valid(self.authority@) && ownership::authorized(self.authority@, self.lease)
            && ownership::lease_key(self.lease) == (self.driver.arena(), self.offset, self.bucket)
            && self.binding < self.driver.registry().len()
            && self.driver.registry().len() == self.binding_count
            && self.driver.registry()[self.binding as int] == self.offset
            && self.offset == self.driver.operation_index_offset()
            && (forall|value:usize| self.query.bucket(value) == self.driver.key_bucket(self.offset, value))
    }
    pub open spec fn stamp(&self) -> u64 {
        represented_stamp(self.driver.history(self.lease), *self.query, self.bucket)
    }
}
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> capture::CaptureIndex for BorrowedIndex<'a, D, P> {
    open spec fn offset(&self) -> usize { self.offset }
    open spec fn stamps(&self) -> Map<usize, u64> { Map::empty().insert(self.bucket, self.stamp()) }
    open spec fn held(&self) -> Set<usize> {
        if self.owned() { Set::empty().insert(self.bucket) } else { Set::empty() }
    }
    fn header_offset(&self) -> (r: usize) { self.offset }
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, capture::Error>) {
        self.driver.guarded_stamp(self.query, self.lease, Tracked(*self.authority.borrow()))
    }
}

// Read-only validator view. Permissions are justified by the borrowed opaque
// lease and authority; the pair fields cannot manufacture a held guard.
impl<'a, D: SlicePrimitives<P>, P: KeyPredicate> predicate::Primitives for BorrowedIndex<'a, D, P> {
    open spec fn state(&self) -> predicate::State {
        predicate::State { arena: self.driver.arena(),
            bindings: Map::empty().insert(self.offset, self.binding),
            binding_count: self.binding_count,
            key_buckets: Map::empty(),
            stamps: Map::empty().insert((self.binding, self.bucket), self.stamp()),
            held: if self.owned() { Set::empty().insert((self.binding, self.bucket)) } else { Set::empty() },
            clock: 0, reserved_stamp: 0, reservations: Seq::empty(), deregistered: false }
    }
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>) {
        if offset == self.offset && self.binding < self.binding_count {
            Ok(self.binding)
        } else { Err(predicate::Error::IndexBindingsIncomplete) }
    }
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>) {
        Err(predicate::Error::Other)
    }
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>) {
        match self.driver.guarded_stamp(self.query, self.lease, Tracked(*self.authority.borrow())) {
            Ok(stamp) => Ok(stamp), Err(_) => Err(predicate::Error::Other),
        }
    }
    fn reserve_stamp(&mut self) -> (stamp: u64) {
        proof { assert(false); }
        0
    }
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64)
        -> (r: Result<(), predicate::Error>)
    {
        proof { assert(false); }
        Err(predicate::Error::Other)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum SliceError { Acquire, Capture, RawLookup, Materialize, Validate }
pub struct Outcome {
    pub rows: Vec<usize>, pub conflict: bool,
    pub predicate_conflict: bool, pub row_conflict: bool,
    pub captured_stamp: u64, pub validation_stamp: u64,
}

pub proof fn unique_one_lease(leases: Seq<ownership::ReadLease>)
    requires leases.len() == 1,
    ensures forall|i: int, j: int| 0 <= i < j < leases.len() ==>
        ownership::lease_key(&leases[i]) != ownership::lease_key(&leases[j]),
{}

// A first indexed lookup with one query bucket. Prefix routing/guard/capture/
// raw/drop/materialization order is checked against the whole native body by
// generate.py. Validation additionally represents its later commit phase.
pub fn indexed_read_then_validate<D: SlicePrimitives<P>, V: lookup::Storage, P: KeyPredicate, C: lookup::CandidateSet>(
    driver: &D, validation_driver: &V, tx: &mut lookup::Transaction, query: &P,
    binding: usize, binding_count: usize, offset: usize, bucket: usize,
    Tracked(authority): Tracked<&mut ownership::Authority>) -> (result: Result<Outcome, SliceError>)
    requires ownership::valid(old(authority)), ownership::physical_domain(driver, old(authority)),
        binding < driver.registry().len(),
        driver.registry().len() == binding_count, driver.registry()[binding as int] == offset,
        offset == driver.operation_index_offset(),
        lookup::transaction_valid(driver.image(), *old(tx)),
        lookup::snapshot(*old(tx)) == driver.operation_snapshot(),
        forall|value:usize| query.bucket(value) == driver.key_bucket(offset, value),
        driver.image().rows.dom().subset_of(validation_driver.image().rows.dom()),
        forall|entry:lookup::ReadRecord| old(tx).read_set@.contains(entry) ==>
            validation_driver.image().rows.contains_key(entry.row_ptr),
        forall|value: usize| query.matches(value) ==> query.bucket(value) == bucket,
    ensures ownership::valid(final(authority)), lookup::transaction_view_same(*old(tx), *final(tx)),
        result.is_ok() ==> forall|id: usize| result.unwrap().rows@.contains(id)
            <==> lookup::matches_row(driver.image(), *old(tx), *query, id),
        result.is_ok() ==> result.unwrap().captured_stamp < old(tx).txid,
        result.is_ok() ==> result.unwrap().predicate_conflict ==
            (result.unwrap().validation_stamp != result.unwrap().captured_stamp
                || result.unwrap().validation_stamp >= old(tx).txid),
        result.is_ok() && !result.unwrap().predicate_conflict ==>
            result.unwrap().row_conflict == lookup::reads_conflict(validation_driver.image(), *final(tx)),
        result.is_ok() ==> result.unwrap().conflict ==
            (result.unwrap().predicate_conflict || lookup::reads_conflict(validation_driver.image(), *final(tx))),
{
    let ghost initial_authority = *authority;
    let mut guards = Vec::new();
    let lease = match ownership::acquire_index_bucket(driver, binding, bucket, &guards, Tracked(&mut *authority)) {
        Ok(lease) => lease, Err(_) => return Err(SliceError::Acquire),
    };
    guards.push(lease);
    proof { unique_one_lease(guards@); }
    let ghost captured_history = driver.history(&guards[0]);
    let mut captured = capture::Transaction { txid: tx.txid, index_conflict: false, index_reads: Vec::new() };
    let mut buckets = Vec::new();
    buckets.push(bucket);
    let captured_result;
    let raw_result;
    {
        let index = BorrowedIndex { driver, query, lease: &guards[0], authority: Tracked(&*authority), offset, binding, binding_count, bucket };
        proof {
            driver.guarded_correspondence(*tx, *query, &guards[0], &*authority);
            assert(history_valid(captured_history, driver.image(), *tx, *query, bucket));
        }
        captured_result = capture::capture_dependencies(&index, &mut captured, &buckets);
        if captured_result.is_ok() {
            proof {
                assert(capture::captured(captured.index_reads@, capture::CaptureIndex::offset(&index),
                    buckets[0], capture::CaptureIndex::stamps(&index)[buckets[0]]));
                assert(capture::captured(captured.index_reads@, offset, bucket,
                    represented_stamp(captured_history, *query, bucket)));
                let member = choose|i: int| 0 <= i < captured.index_reads.len()
                    && captured.index_reads[i] == (capture::IndexRead { index_offset: offset, bucket,
                        stamp: represented_stamp(captured_history, *query, bucket) });
                assert(captured.index_reads.len() == 1);
                assert(member == 0);
            }
            raw_result = driver.raw_lookup(query, &guards[0], Tracked(&*authority));
        } else { raw_result = Err(lookup::Error::Storage); }
    }
    ownership::release_all(driver, guards, Tracked(&mut *authority));
    proof { ownership::domain_survives_interference(driver, &initial_authority, authority); }
    match captured_result { Err(_) => return Err(SliceError::Capture), Ok(()) => {}, }
    let candidates = match raw_result { Err(_) => return Err(SliceError::RawLookup), Ok(rows) => rows, };
    proof {
        assert(captured.index_reads.len() == 1);
        assert(captured.index_reads[0] == (capture::IndexRead { index_offset: offset, bucket,
            stamp: represented_stamp(captured_history, *query, bucket) }));
    }
    let captured_stamp = captured.index_reads[0].stamp;
    let rows = match lookup::materialize_after_checked_history::<D, C, P>(driver, tx, candidates, query,
        Ghost(captured_history.initial), Ghost(captured_history.events), Ghost(captured_history.protocol),
        Ghost(captured_history.actions), Ghost(captured_history.initial_stamps), Ghost(Set::empty().insert(bucket))) {
        Ok(rows) => rows, Err(_) => return Err(SliceError::Materialize),
    };
    let mut validation_guards = Vec::new();
    let validation_lease = match ownership::acquire_index_bucket(driver, binding, bucket, &validation_guards, Tracked(&mut *authority)) {
        Ok(lease) => lease, Err(_) => return Err(SliceError::Acquire),
    };
    validation_guards.push(validation_lease);
    proof { unique_one_lease(validation_guards@); }
    let mut reads = Vec::new();
    let read = &captured.index_reads[0];
    reads.push(predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp });
    let validation_tx = predicate::OccTransaction { txid: captured.txid, index_conflict: captured.index_conflict, index_reads: reads };
    let validation;
    let validation_stamp;
    {
        let index = BorrowedIndex { driver, query, lease: &validation_guards[0], authority: Tracked(&*authority), offset, binding, binding_count, bucket };
        validation_stamp = match driver.guarded_stamp(query, &validation_guards[0], Tracked(&*authority)) {
            Ok(stamp) => Some(stamp), Err(_) => None,
        };
        proof {
            reveal_with_fuel(predicate::read_keys, 2);
            reveal(predicate::reads_good);
            assert(predicate::read_keys(predicate::Primitives::state(&index), validation_tx.index_reads@,
                validation_tx.index_reads.len() as int).subset_of(predicate::Primitives::state(&index).held));
        }
        validation = predicate::index_read_conflict(&index, &validation_tx);
    }
    proof {
        assert forall|i:int| 0<=i<tx.read_set.len() implies
            validation_driver.image().rows.contains_key(tx.read_set[i].row_ptr) by {
            let entry = tx.read_set[i];
            assert(tx.read_set@.contains(entry));
            if !old(tx).read_set@.contains(entry) {
                assert(lookup::observed_record(driver.image(), *old(tx), entry));
            }
        }
    }
    let row_validation = match &validation {
        Ok(true) => Ok(false),
        Ok(false) => lookup::has_serialization_conflict(validation_driver, tx),
        Err(_) => Err(lookup::Error::Storage),
    };
    ownership::release_all(driver, validation_guards, Tracked(&mut *authority));
    let stamp = match validation_stamp { Some(stamp) => stamp, None => return Err(SliceError::Validate), };
    let predicate_conflict = match validation { Ok(conflict) => conflict, Err(_) => return Err(SliceError::Validate), };
    let row_conflict = match row_validation {
        Ok(conflict) => conflict, Err(_) => return Err(SliceError::Validate),
    };
    let conflict = predicate_conflict || row_conflict;
    Ok(Outcome { rows, conflict, predicate_conflict, row_conflict, captured_stamp, validation_stamp: stamp })
}

// Satisfiability witness for the newly introduced history/retained-image seam.
// Guard ownership has independent executable handoff witnesses; this lemma
// does not claim a concrete native Storage/Scheduler implementation.
pub proof fn empty_history_has_success_witness(tx: lookup::Transaction)
    requires tx.txid == 1, tx.snapshot_xmin == 1, tx.snapshot_xmax == 2,
        tx.snapshot_active@ == Seq::<u64>::empty(), tx.write_set@ == Seq::<lookup::PendingWrite>::empty(),
    ensures exists|h:History, image:lookup::Image| history_valid(h,image,tx,lookup::EqualsKey{key:42},0)
        && represented_stamp(h,lookup::EqualsKey{key:42},0) < tx.txid,
{
    let p0=lookup::Protocol{clock:1,active:Set::empty(),ended:Set::empty(),published:Map::empty(),observed:Map::empty()};
    let p1=lookup::Protocol{clock:2,active:Set::empty().insert(1),..p0};
    let p2=lookup::Protocol{observed:Map::empty().insert(1,Set::empty()),..p1};
    let states=seq![p0,p1,p2];
    let actions=seq![lookup::Action::Start{txid:1},lookup::Action::Snapshot{reader:1}];
    assert(p1.active.remove(1) =~= Set::<u64>::empty());
    assert forall|i:int| 0<=i<2 implies lookup::protocol_step(states[i],states[i+1],actions[i]) by {
        if i==0 {} else {assert(i==1);}
    }
    assert(tx.snapshot_active@.to_set() =~= Set::<u64>::empty());
    let h=History{initial:Map::empty(),events:Seq::empty(),protocol:states,actions,
        initial_stamps:Map::empty().insert(0,0)};
    let image=lookup::Image{heads:Map::empty(),rows:Map::empty(),rank:Map::empty(),capacity:0};
    assert(history_valid(h,image,tx,lookup::EqualsKey{key:42},0));
    assert(represented_stamp(h,lookup::EqualsKey{key:42},0)==0);
}
}
