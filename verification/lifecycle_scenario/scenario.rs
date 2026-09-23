// Checked composition of the exact generated native operations. This harness
// is not linked into AeroStore. Raw index/guard/atomic correspondence is explicit.
mod lifecycle { /* LIFECYCLE_MODULE */ }
mod predicate { /* PREDICATE_MODULE */ }
mod capture { /* CAPTURE_MODULE */ }
use vstd::prelude::*;
use lifecycle::LifecyclePrimitives;
use predicate::Primitives;
use capture::CaptureIndex;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub struct IndexProjection {
    pub arena: usize,
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<predicate::Pair, usize>,
    pub stamps: Map<predicate::Pair, u64>,
    pub held: Set<predicate::Pair>,
}
// The operation-local, guarded index projection. This does not claim that raw
// native index atomics or held guard lifetimes have been refined here.
pub trait IndexPrimitives {
    spec fn projection(&self) -> IndexProjection;
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>)
        ensures r.is_ok() ==> self.projection().bindings.contains_key(offset)
            && r.unwrap() == self.projection().bindings[offset]
            && r.unwrap() < self.projection().binding_count;
    fn key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>)
        requires binding < self.projection().binding_count,
        ensures r.is_ok() ==> r.unwrap() == self.projection().key_buckets[(binding, *key)];
    fn load_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>)
        requires self.projection().held.contains((binding, bucket)), binding < self.projection().binding_count,
        ensures r.is_ok() ==> self.projection().stamps.contains_key((binding, bucket))
            && r.unwrap() == self.projection().stamps[(binding, bucket)];
    fn store_stamp(&mut self, binding: usize, bucket: usize, stamp: u64) -> (r: Result<(), predicate::Error>)
        requires old(self).projection().held.contains((binding, bucket)), binding < old(self).projection().binding_count,
        ensures final(self).projection() == (IndexProjection {
            stamps: if r.is_ok() { old(self).projection().stamps.insert((binding, bucket), stamp) }
                else { old(self).projection().stamps }, ..old(self).projection()
        });
}
// Borrowed view of the same index object used by publication and validation.
pub struct CaptureView<'a, I: IndexPrimitives> { pub index: &'a I, pub offset: usize, pub binding: usize, pub bucket: usize }
impl<'a, I: IndexPrimitives> capture::CaptureIndex for CaptureView<'a, I> {
    open spec fn offset(&self) -> usize { self.offset }
    open spec fn stamps(&self) -> Map<usize, u64> {
        Map::empty().insert(self.bucket, self.index.projection().stamps[(self.binding, self.bucket)])
    }
    open spec fn held(&self) -> Set<usize> {
        if self.binding < self.index.projection().binding_count
            && self.index.projection().held.contains((self.binding, self.bucket))
            && self.index.projection().stamps.contains_key((self.binding, self.bucket)) {
            Set::empty().insert(self.bucket)
        } else { Set::empty() }
    }
    fn header_offset(&self) -> (r: usize) { self.offset }
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, capture::Error>) {
        match self.index.load_stamp(self.binding, bucket) {
            Ok(stamp) => Ok(stamp),
            Err(_) => Err(capture::Error::Index),
        }
    }
}

pub fn capture_one_dependency<I: IndexPrimitives>(index: &I, offset: usize, bucket: usize, txid: u64)
    -> (r: Result<predicate::IndexRead, capture::Error>)
    requires index.projection().bindings.contains_key(offset),
        index.projection().bindings[offset] < index.projection().binding_count,
        index.projection().held.contains((index.projection().bindings[offset], bucket)),
        index.projection().stamps.contains_key((index.projection().bindings[offset], bucket)),
    ensures r.is_ok() ==> r.unwrap().index_offset == offset && r.unwrap().bucket == bucket
        && r.unwrap().stamp == index.projection().stamps[(index.projection().bindings[offset], bucket)]
        && r.unwrap().stamp < txid,
{
    let binding = match index.find_binding(offset) {
        Ok(binding) => binding,
        Err(_) => return Err(capture::Error::Index),
    };
    let view = CaptureView { index, offset, binding, bucket };
    let mut captured = capture::Transaction { txid, index_conflict: false, index_reads: Vec::new() };
    let mut buckets = Vec::new();
    buckets.push(bucket);
    match capture::capture_dependencies(&view, &mut captured, &buckets) {
        Err(error) => return Err(error),
        Ok(()) => {},
    }
    proof {
        assert(capture::captured(captured.index_reads@, view.offset(), buckets[0], view.stamps()[buckets[0]]));
        assert(capture::captured(captured.index_reads@, offset, bucket,
            index.projection().stamps[(binding, bucket)]));
        let member = choose|i: int| 0 <= i < captured.index_reads.len()
            && captured.index_reads[i] == capture::IndexRead { index_offset: offset, bucket,
                stamp: index.projection().stamps[(binding, bucket)] };
        assert(captured.index_reads.len() == 1);
        assert(captured.index_reads[0] == capture::IndexRead { index_offset: offset, bucket,
            stamp: index.projection().stamps[(binding, bucket)] });
    }
    let read = &captured.index_reads[0];
    Ok(predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp })
}

pub trait ScenarioClock: LifecyclePrimitives {
    // Harness case selection only: native AeroStore does not implement this
    // overflow check. External reservations may have exhausted the finite clock
    // during snapshot acquisition even if the initial clock had room.
    fn clock_has_capacity(&self) -> (r: bool)
        ensures r == (self.state().clock < u64::MAX);
}

// One lifecycle object is used for reader registration, snapshot creation,
// writer deregistration AND the publisher's exact reservation expression.
// Arena/clock correspondence is the caller's native projection obligation.
pub struct Bridge<L: ScenarioClock, I: IndexPrimitives> {
    pub lifecycle: L,
    pub index: I,
    pub writer_txid: u64,
    pub reserved_stamp: u64,
}

impl<L: ScenarioClock, I: IndexPrimitives> predicate::Primitives for Bridge<L, I> {
    open spec fn state(&self) -> predicate::State {
        predicate::State {
            arena: self.index.projection().arena,
            bindings: self.index.projection().bindings,
            binding_count: self.index.projection().binding_count,
            key_buckets: self.index.projection().key_buckets,
            stamps: self.index.projection().stamps,
            held: self.index.projection().held,
            clock: self.lifecycle.state().clock,
            reserved_stamp: self.reserved_stamp,
            reservations: self.lifecycle.state().reservations,
            deregistered: lifecycle::well_formed(self.lifecycle.state())
                && !self.lifecycle.state().lifecycle_held
                && (forall|i: int| 0 <= i < self.lifecycle.state().slots.len() ==>
                    self.lifecycle.state().slots[i].txid != self.writer_txid),
        }
    }
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>) {
        self.index.find_binding(offset)
    }
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>) {
        self.index.key_bucket(binding, key)
    }
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>) {
        self.index.load_stamp(binding, bucket)
    }
    fn reserve_stamp(&mut self) -> (stamp: u64) {
        let stamp = lifecycle::reserve_publication_clock(&mut self.lifecycle);
        self.reserved_stamp = stamp;
        stamp
    }
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64)
        -> (r: Result<(), predicate::Error>)
    {
        self.index.store_stamp(binding, bucket, stamp)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ScenarioError { Registration, Snapshot, Capture, Deregistration, ClockExhausted, Publication, Validation }
pub struct Outcome { pub reader_txid: u64, pub publication_stamp: u64, pub conflict: bool,
    pub reader_snapshot: lifecycle::ProcSnapshot }

pub proof fn change_keys_ignore_clock(before: predicate::State, after: predicate::State,
    changes: Seq<predicate::IndexChange>, n: int)
    requires before.key_buckets == after.key_buckets, 0 <= n <= changes.len(),
    ensures predicate::change_keys(before, changes, n) == predicate::change_keys(after, changes, n),
    decreases n,
{
    if n > 0 { change_keys_ignore_clock(before, after, changes, n - 1); }
}

pub proof fn read_keys_ignore_clock(before: predicate::State, after: predicate::State,
    reads: Seq<predicate::IndexRead>, n: int)
    requires before.bindings == after.bindings, 0 <= n <= reads.len(),
    ensures predicate::read_keys(before, reads, n) == predicate::read_keys(after, reads, n),
    decreases n,
{
    if n > 0 { read_keys_ignore_clock(before, after, reads, n - 1); }
}

pub proof fn cleared_writer_is_absent(before: lifecycle::State, after: lifecycle::State,
    registration: lifecycle::ProcArrayRegistration)
    requires lifecycle::well_formed(before), registration.txid > 0,
        registration.slot_idx < before.slots.len(),
        before.slots[registration.slot_idx as int].txid == registration.txid,
        after.slots == before.slots.update(registration.slot_idx as int,
            lifecycle::Slot { txid: 0, snapshot_xmin: 0 }),
    ensures forall|i: int| 0 <= i < after.slots.len() ==> after.slots[i].txid != registration.txid,
{
    assert forall|i: int| 0 <= i < after.slots.len() implies after.slots[i].txid != registration.txid by {
        if i < registration.slot_idx { assert(before.slots[i].txid != registration.txid); }
        if i > registration.slot_idx { assert(before.slots[i].txid != registration.txid); }
    }
}

pub open spec fn scenario_entry(life: lifecycle::State, p: predicate::State,
    writer: lifecycle::ProcArrayRegistration, offset: usize, bucket: usize,
    changes: Seq<predicate::IndexChange>) -> bool {
    lifecycle::well_formed(life)
        && !life.lifecycle_held
        && life.clock < u64::MAX
        && writer.txid > 0
        && writer.slot_idx < lifecycle::PROCARRAY_SLOTS
        && life.slots[writer.slot_idx as int].txid == writer.txid
        && p.bindings.contains_key(offset)
        && p.bindings[offset] < p.binding_count
        && p.stamps.contains_key((p.bindings[offset], bucket))
        && p.held.contains((p.bindings[offset], bucket))
        && predicate::changes_valid(p, changes)
        && predicate::change_keys(p, changes, changes.len() as int)
            .contains((p.bindings[offset], bucket))
        && predicate::change_keys(p, changes, changes.len() as int).subset_of(p.held)
}

// A fixed schedule with an older active writer and a newly registered reader.
// The requested index/bucket and guard permissions are supplied; the reader ID,
// snapshot, dependency, deregistration, history and publication stamp are
// produced by actual generated native functions below. Slot transitions are
// restricted to this schedule, not arbitrary interference while acquiring locks.
pub fn registered_reader_then_writer_publication<L: ScenarioClock, I: IndexPrimitives, C: predicate::PairSet>(
    driver: &mut Bridge<L, I>, writer: lifecycle::ProcArrayRegistration,
    offset: usize, bucket: usize, changes: &Vec<predicate::IndexChange>)
    -> (result: Result<Outcome, ScenarioError>)
    requires scenario_entry(old(driver).lifecycle.state(), old(driver).state(), writer, offset, bucket, changes@),
        writer.txid == old(driver).writer_txid,
    ensures result.is_ok() ==> result.unwrap().conflict
        && result.unwrap().publication_stamp > result.unwrap().reader_txid,
        result.is_ok() ==> result.unwrap().reader_snapshot.in_flight@.contains(writer.txid),
        result.is_ok() ==> final(driver).state().reservations.contains(result.unwrap().reader_txid)
            && final(driver).state().reservations.contains(result.unwrap().publication_stamp),
{
    let ghost initial = driver.lifecycle.state();
    let ghost initial_predicate = driver.state();
    let reader_result = lifecycle::begin_transaction(&mut driver.lifecycle);
    driver.lifecycle.release_lifecycle();
    let reader = match reader_result {
        Ok(reader) => reader,
        Err(_) => return Err(ScenarioError::Registration),
    };
    proof {
        assert(reader.slot_idx != writer.slot_idx);
        assert(driver.lifecycle.state().slots[writer.slot_idx as int].txid == writer.txid);
    }
    let snapshot_result = lifecycle::create_transaction_snapshot(&mut driver.lifecycle, reader);
    driver.lifecycle.release_lifecycle();
    let snapshot = match snapshot_result {
        Ok(snapshot) => snapshot,
        Err(_) => return Err(ScenarioError::Snapshot),
    };
    proof {
        lifecycle::snapshot_covers_active_writer(driver.lifecycle.state(), snapshot, writer.slot_idx as int);
        assert(snapshot.in_flight@.contains(writer.txid));
        assert(driver.lifecycle.state().reservations.contains(reader.txid));
    }
    let read = match capture_one_dependency(&driver.index, offset, bucket, reader.txid) {
        Ok(read) => read,
        Err(_) => return Err(ScenarioError::Capture),
    };
    let mut reads = Vec::new();
    reads.push(read);
    let ghost before_end = driver.lifecycle.state();
    let finish = lifecycle::end_transaction(&mut driver.lifecycle, writer);
    driver.lifecycle.release_lifecycle();
    match finish {
        Err(_) => return Err(ScenarioError::Deregistration),
        Ok(()) => {},
    }
    proof {
        cleared_writer_is_absent(before_end, driver.lifecycle.state(), writer);
        assert(driver.state().deregistered);
    }
    if !driver.lifecycle.clock_has_capacity() {
        return Err(ScenarioError::ClockExhausted);
    }
    let ghost before_publication = driver.state();
    let ghost clock_before_publication = driver.lifecycle.state();
    proof {
        change_keys_ignore_clock(initial_predicate, before_publication, changes@, changes.len() as int);
        read_keys_ignore_clock(initial_predicate, before_publication, reads@, reads.len() as int);
        reveal_with_fuel(predicate::read_keys, 2);
        assert(predicate::read_keys(before_publication, reads@, 1)
            == Set::empty().insert((before_publication.bindings[offset], bucket)));
    }
    let tx = predicate::OccTransaction { txid: reader.txid, index_conflict: false, index_reads: reads };
    let published = predicate::publish_index_stamps::<Bridge<L, I>, C>(driver, changes);
    match published {
        Err(_) => return Err(ScenarioError::Publication),
        Ok(()) => {},
    }
    let publication_stamp = driver.reserved_stamp;
    proof {
        assert(changes.len() > 0);
        read_keys_ignore_clock(before_publication, driver.state(), tx.index_reads@, tx.index_reads.len() as int);
        let member = choose|i: int| 0 <= i < clock_before_publication.reservations.len()
            && clock_before_publication.reservations[i] == reader.txid;
        assert(reader.txid < clock_before_publication.clock);
        assert(publication_stamp > reader.txid);
        predicate::publication_invalidates_dependency(before_publication, driver.state(), tx.index_reads@,
            reader.txid, predicate::change_keys(before_publication, changes@, changes.len() as int),
            publication_stamp, 0);
    }
    let conflict = match predicate::index_read_conflict(driver, &tx) {
        Ok(conflict) => conflict,
        Err(_) => return Err(ScenarioError::Validation),
    };
    Ok(Outcome { reader_txid: reader.txid, publication_stamp, conflict, reader_snapshot: snapshot })
}

// Finite, live entry states for both empty creation and key movement, together
// with a safe old read and a later invalidating publication. This witnesses
// consistency of the scenario premises; it is not a native heap construction.
pub proof fn creation_and_move_have_live_witnesses() {
    let empty = Seq::new(lifecycle::PROCARRAY_SLOTS as nat,
        |i: int| lifecycle::Slot { txid: 0, snapshot_xmin: 0 });
    let slots = empty.update(0, lifecycle::Slot { txid: 2, snapshot_xmin: 2 });
    let life = lifecycle::State { slots, clock: 3, sampled_clock: 3,
        reservations: seq![2u64], lifecycle_held: false };
    let p = predicate::State { arena: 1, bindings: Map::empty().insert(7usize, 0usize),
        binding_count: 1, key_buckets: Map::empty().insert((0usize, 99usize), 1usize)
            .insert((0usize, 100usize), 2usize),
        stamps: Map::empty().insert((0usize, 1usize), 1u64).insert((0usize, 2usize), 1u64),
        held: Set::empty().insert((0usize, 1usize)).insert((0usize, 2usize)),
        clock: 3, reserved_stamp: 0, reservations: seq![2u64], deregistered: false };
    let writer = lifecycle::ProcArrayRegistration { slot_idx: 0, txid: 2 };
    let creation = seq![predicate::IndexChange { binding: 0, before: None, after: Some(100) }];
    let movement = seq![predicate::IndexChange { binding: 0, before: Some(99), after: Some(100) }];
    reveal_with_fuel(predicate::change_keys, 2);
    assert(scenario_entry(life, p, writer, 7, 2, creation));
    assert(scenario_entry(life, p, writer, 7, 2, movement));
    let read = predicate::IndexRead { index_offset: 7, bucket: 2, stamp: 1 };
    assert(predicate::read_good(p, read, 3));
    let allocated = lifecycle::State { clock: 4, reservations: life.reservations.push(3), ..life };
    assert(lifecycle::reservation(life, allocated, 3));
    assert(life.slots[1].txid == 0);
    let registered = lifecycle::State { slots: slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 3 }), clock: 4, sampled_clock: 4,
        reservations: seq![2u64, 3u64], lifecycle_held: false };
    assert(registered.slots == allocated.slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 3 }));
    let snapshotted = lifecycle::State { slots: registered.slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 2 }), ..registered };
    assert(lifecycle::well_formed(snapshotted));
    lifecycle::active_slot_membership(snapshotted, 0, snapshotted.slots.len() as int);
    assert(lifecycle::active(snapshotted.slots, snapshotted.slots.len() as int).contains(2));
    let finished = lifecycle::State { slots: snapshotted.slots.update(0,
        lifecycle::Slot { txid: 0, snapshot_xmin: 0 }), ..snapshotted };
    assert(lifecycle::well_formed(registered));
    assert(lifecycle::well_formed(finished));
    let reserved = lifecycle::State { clock: 5, reservations: finished.reservations.push(4), ..finished };
    assert(lifecycle::reservation(finished, reserved, 4));
    lifecycle::reservation_after_reader(finished, reserved, 4, 3);
    let published = predicate::State { stamps: p.stamps.insert((0usize, 2usize), 4u64),
        clock: 5, reserved_stamp: 4, reservations: reserved.reservations, deregistered: true, ..p };
    assert(!predicate::read_good(published, read, 3));
    assert(4u64 > 3u64);
}
}
