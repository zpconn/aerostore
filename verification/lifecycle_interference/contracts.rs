// Checked API-entry -> interfering lock wait -> native acquired-state suffix.
// Primitive ownership authority, mutex exclusion, native atomics and no-wrap
// remain assumptions. No exact acquisition frame is assumed in this campaign.
mod lifecycle { /* LIFECYCLE_MODULE */ }
use vstd::prelude::*;
use lifecycle::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub tracked struct OwnerToken {
    ghost registration: ProcArrayRegistration,
    ghost arena: usize,
}
impl OwnerToken {
    pub closed spec fn registration(&self) -> ProcArrayRegistration { self.registration }
    pub closed spec fn arena(&self) -> usize { self.arena }
}

pub open spec fn live_owner(s: State, owner: ProcArrayRegistration) -> bool {
    owner.txid > 0 && owner.slot_idx < s.slots.len()
        && s.slots[owner.slot_idx as int].txid == owner.txid
}
pub open spec fn released(s: State) -> State { State { lifecycle_held: false, ..s } }
pub open spec fn extends(earlier: Seq<u64>, later: Seq<u64>) -> bool {
    earlier.len() <= later.len()
        && forall|i: int| 0 <= i < earlier.len() ==> earlier[i] == later[i]
}

#[derive(Clone, Copy)]
pub enum WaitEvent {
    Register { slot: usize, txid: u64 },
    End { slot: usize },
    Horizon { slot: usize, xmin: u64 },
    Reserve { txid: u64 },
    Observe,
}
pub open spec fn allowed(event: WaitEvent, owner: Option<ProcArrayRegistration>) -> bool {
    match owner {
        None => true,
        Some(r) => match event {
            WaitEvent::Register { slot, .. } | WaitEvent::End { slot }
                | WaitEvent::Horizon { slot, .. } => slot != r.slot_idx,
            _ => true,
        },
    }
}
pub open spec fn wait_step(a: State, b: State, event: WaitEvent) -> bool {
    !a.lifecycle_held && !b.lifecycle_held
    && match event {
        WaitEvent::Register { slot, txid } => slot < a.slots.len()
            && a.slots[slot as int].txid == 0 && a.clock <= txid < u64::MAX
            && b == (State { slots: a.slots.update(slot as int, Slot { txid, snapshot_xmin: txid }),
                clock: (txid + 1) as u64, reservations: a.reservations.push(txid), ..a }),
        WaitEvent::End { slot } => slot < a.slots.len() && a.slots[slot as int].txid > 0
            && b == (State { slots: a.slots.update(slot as int, Slot { txid: 0, snapshot_xmin: 0 }), ..a }),
        WaitEvent::Horizon { slot, xmin } => slot < a.slots.len()
            && 0 < xmin <= a.slots[slot as int].txid
            && b.clock >= a.clock && 0 < b.sampled_clock <= b.clock
            && b == (State { slots: a.slots.update(slot as int, Slot { snapshot_xmin: xmin, ..a.slots[slot as int] }),
                clock: b.clock, sampled_clock: b.sampled_clock, ..a }),
        WaitEvent::Reserve { txid } => reservation(a, b, txid),
        WaitEvent::Observe => observation(a, b),
    }
}

pub struct WaitTrace { pub states: Seq<State>, pub events: Seq<WaitEvent> }
pub open spec fn trace_valid(entry: State, acquired: State, owner: Option<ProcArrayRegistration>, trace: WaitTrace) -> bool {
    trace.states.len() == trace.events.len() + 1
        && trace.states[0] == entry
        && trace.states[trace.events.len() as int] == released(acquired)
        && acquired.lifecycle_held
        && (forall|i: int| 0 <= i < trace.events.len() ==>
            wait_step(trace.states[i], trace.states[i + 1], trace.events[i])
                && allowed(trace.events[i], owner))
}

pub proof fn wait_step_preserves(a: State, b: State, event: WaitEvent)
    requires well_formed(a), wait_step(a, b, event),
    ensures well_formed(b), extends(a.reservations, b.reservations), b.clock >= a.clock,
{
    match event {
        WaitEvent::Reserve { txid } => { reservation_preserves_clock_history(a, b, txid); },
        WaitEvent::Observe => { observation_preserves_state(a, b); },
        WaitEvent::Register { slot, txid } => {
            assert forall|i: int, j: int| 0 <= i < j < b.slots.len() && b.slots[i].txid != 0
                implies b.slots[i].txid != b.slots[j].txid by {
                if i == slot { assert(a.slots[j].txid < a.clock); }
                if j == slot { assert(a.slots[i].txid < a.clock); }
            }
        },
        _ => {},
    }
}

pub proof fn wait_step_preserves_owner(a: State, b: State, event: WaitEvent, owner: ProcArrayRegistration)
    requires wait_step(a, b, event), allowed(event, Some(owner)), live_owner(a, owner),
    ensures live_owner(b, owner), b.slots[owner.slot_idx as int] == a.slots[owner.slot_idx as int],
{}

pub proof fn extends_transitive(a: Seq<u64>, b: Seq<u64>, c: Seq<u64>)
    requires extends(a, b), extends(b, c),
    ensures extends(a, c),
{}

pub proof fn trace_prefix_invariant(trace: WaitTrace, owner: Option<ProcArrayRegistration>, n: int)
    requires trace.states.len() == trace.events.len() + 1,
        0 <= n <= trace.events.len(), well_formed(trace.states[0]),
        owner.is_some() ==> live_owner(trace.states[0], owner.unwrap()),
        forall|i: int| 0 <= i < trace.events.len() ==>
            wait_step(trace.states[i], trace.states[i + 1], trace.events[i]) && allowed(trace.events[i], owner),
    ensures well_formed(trace.states[n]),
        trace.states[n].clock >= trace.states[0].clock,
        extends(trace.states[0].reservations, trace.states[n].reservations),
        owner.is_some() ==> live_owner(trace.states[n], owner.unwrap())
            && trace.states[n].slots[owner.unwrap().slot_idx as int] == trace.states[0].slots[owner.unwrap().slot_idx as int],
    decreases n,
{
    if n > 0 {
        trace_prefix_invariant(trace, owner, n - 1);
        wait_step_preserves(trace.states[n - 1], trace.states[n], trace.events[n - 1]);
        extends_transitive(trace.states[0].reservations, trace.states[n - 1].reservations, trace.states[n].reservations);
        if let Some(r) = owner {
            wait_step_preserves_owner(trace.states[n - 1], trace.states[n], trace.events[n - 1], r);
        }
    }
}

pub proof fn acquisition_preserves_owned_history(entry: State, acquired: State,
    owner: Option<ProcArrayRegistration>, trace: WaitTrace)
    requires well_formed(entry), trace_valid(entry, acquired, owner, trace),
        owner.is_some() ==> live_owner(entry, owner.unwrap()),
    ensures well_formed(acquired), acquired.clock >= entry.clock,
        extends(entry.reservations, acquired.reservations),
        owner.is_some() ==> live_owner(acquired, owner.unwrap())
            && acquired.slots[owner.unwrap().slot_idx as int] == entry.slots[owner.unwrap().slot_idx as int],
{
    trace_prefix_invariant(trace, owner, trace.events.len() as int);
}

// Unlike LifecyclePrimitives this interface has NO lock_lifecycle function.
// All operations below require an already acquired native lifecycle guard.
pub trait LockedPrimitives {
    spec fn state(&self) -> State;
    spec fn identity(&self) -> usize;
    fn reserve_registration(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held, 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id), final(self).identity() == old(self).identity();
    fn load_clock(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held,
        ensures observation(old(self).state(), final(self).state()), id == final(self).state().sampled_clock,
            final(self).identity() == old(self).identity();
    fn slot_txid(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].txid;
    fn compare_empty_register(&mut self, slot: usize, txid: u64) -> (ok: bool)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures ok == (old(self).state().slots[slot as int].txid == EMPTY_SLOT), final(self).identity() == old(self).identity(),
            final(self).state() == (State { slots: if ok {
                old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] })
            } else { old(self).state().slots }, ..old(self).state() });
    fn store_xmin(&mut self, slot: usize, xmin: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).identity() == old(self).identity(), final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { snapshot_xmin: xmin, ..old(self).state().slots[slot as int] }), ..old(self).state() });
    fn store_txid(&mut self, slot: usize, txid: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).identity() == old(self).identity(), final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] }), ..old(self).state() });
}

pub open spec fn token_live<D: LockedPrimitives + ?Sized>(d: &D, token: &OwnerToken) -> bool {
    token.arena() == d.identity() && live_owner(d.state(), token.registration())
}

pub trait WaitingPrimitives: LockedPrimitives {
    // Environment no-wrap condition at actual acquisition. This is NOT a
    // production overflow check or a deduction from room at API entry.
    spec fn registration_capacity_on_acquire(&self) -> bool;
    fn acquire(&mut self, Tracked(owner): Tracked<Option<&OwnerToken>>) -> (trace: Ghost<WaitTrace>)
        requires well_formed(old(self).state()), !old(self).state().lifecycle_held,
            owner.is_some() ==> token_live(old(self), owner.unwrap()),
        ensures trace_valid(old(self).state(), final(self).state(),
            match owner { Some(t) => Some(t.registration()), None => None }, trace@),
            final(self).identity() == old(self).identity(),
            old(self).registration_capacity_on_acquire() ==> final(self).state().clock < u64::MAX;
}

/* ACQUIRED_FUNCTIONS */

pub fn begin_after_wait<D: WaitingPrimitives>(driver: &mut D)
    -> (out: (Result<ProcArrayRegistration, ProcArrayError>, Tracked<Option<OwnerToken>>, Ghost<State>))
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        old(driver).registration_capacity_on_acquire(),
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).identity() == old(driver).identity(),
        well_formed(out.2@), out.2@.lifecycle_held,
        extends(old(driver).state().reservations, out.2@.reservations),
        extends(old(driver).state().reservations, final(driver).state().reservations),
        out.0.is_ok() == out.1@.is_some(),
        out.0.is_ok() ==> token_live(final(driver), &out.1@.unwrap())
            && out.1@.unwrap().registration() == out.0.unwrap()
            && out.2@.slots[out.0.unwrap().slot_idx as int].txid == 0
            && out.0.unwrap().txid >= out.2@.clock
            && final(driver).state().reservations == out.2@.reservations.push(out.0.unwrap().txid),
{
    let ghost entry = driver.state();
    let trace = driver.acquire(Tracked(None));
    let ghost acquired = driver.state();
    proof { acquisition_preserves_owned_history(entry, acquired, None, trace@); }
    let result = begin_transaction_acquired(driver);
    proof {
        extends_transitive(entry.reservations, acquired.reservations, driver.state().reservations);
    }
    let tracked token = match result {
        Ok(registration) => Some(OwnerToken { registration, arena: driver.identity() }),
        Err(_) => None,
    };
    (result, Tracked(token), Ghost(acquired))
}

pub fn snapshot_after_wait<D: WaitingPrimitives>(driver: &mut D, registration: ProcArrayRegistration,
    Tracked(owner): Tracked<&OwnerToken>)
    -> (out: (Result<ProcSnapshot, ProcArrayError>, Ghost<State>))
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        token_live(old(driver), owner), owner.registration() == registration,
    ensures out.0.is_ok(), well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        well_formed(out.1@), out.1@.lifecycle_held,
        token_live(final(driver), owner), final(driver).identity() == old(driver).identity(),
        extends(old(driver).state().reservations, final(driver).state().reservations),
        out.1@.slots[registration.slot_idx as int] == old(driver).state().slots[registration.slot_idx as int],
        out.0.is_ok() ==> coherent_snapshot(final(driver).state(), out.0->Ok_0)
            && out.0->Ok_0.in_flight@ == active(out.1@.slots, PROCARRAY_SLOTS as int)
            && final(driver).state().slots == out.1@.slots.update(registration.slot_idx as int,
                Slot { snapshot_xmin: out.0->Ok_0.xmin, ..out.1@.slots[registration.slot_idx as int] }),
{
    let ghost entry = driver.state();
    let trace = driver.acquire(Tracked(Some(owner)));
    let ghost acquired = driver.state();
    proof { acquisition_preserves_owned_history(entry, acquired, Some(registration), trace@); }
    let result = create_transaction_snapshot_acquired(driver, registration);
    proof { if result.is_ok() { snapshot_fields_unchanged(acquired.slots, driver.state().slots,
        PROCARRAY_SLOTS as int, driver.state().sampled_clock); } }
    (result, Ghost(acquired))
}

pub fn end_after_wait<D: WaitingPrimitives>(driver: &mut D, registration: ProcArrayRegistration,
    Tracked(owner): Tracked<OwnerToken>) -> (out: (Result<(), ProcArrayError>, Ghost<State>))
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        token_live(old(driver), &owner), owner.registration() == registration,
    ensures out.0.is_ok(), well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        well_formed(out.1@), out.1@.lifecycle_held,
        final(driver).identity() == old(driver).identity(),
        extends(old(driver).state().reservations, final(driver).state().reservations),
        out.1@.slots[registration.slot_idx as int] == old(driver).state().slots[registration.slot_idx as int],
        final(driver).state().slots == out.1@.slots.update(registration.slot_idx as int, Slot { txid: 0, snapshot_xmin: 0 }),
{
    let ghost entry = driver.state();
    let trace = driver.acquire(Tracked(Some(&owner)));
    let ghost acquired = driver.state();
    proof { acquisition_preserves_owned_history(entry, acquired, Some(registration), trace@); }
    let result = end_transaction_acquired(driver, registration);
    (result, Ghost(acquired))
}

// These wrappers check that the source-derived native suffixes instantiate
// the interference events. Environment traces overapproximate possible native
// schedules; their own-slot exclusion is the remaining ownership assumption.
pub fn native_begin_is_register_event<D: LockedPrimitives>(driver: &mut D)
    -> (result: Result<ProcArrayRegistration, ProcArrayError>)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held,
        old(driver).state().clock < u64::MAX,
    ensures result.is_ok() ==> wait_step(released(old(driver).state()), released(final(driver).state()),
        WaitEvent::Register { slot: result.unwrap().slot_idx as usize, txid: result.unwrap().txid }),
{
    begin_transaction_acquired(driver)
}

pub fn native_end_is_end_event<D: LockedPrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<(), ProcArrayError>)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held, registration.txid > 0,
    ensures result.is_ok() ==> wait_step(released(old(driver).state()), released(final(driver).state()),
        WaitEvent::End { slot: registration.slot_idx as usize }),
{
    end_transaction_acquired(driver, registration)
}

pub fn native_snapshot_is_horizon_event<D: LockedPrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<ProcSnapshot, ProcArrayError>)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held, registration.txid > 0,
    ensures result.is_ok() ==> wait_step(released(old(driver).state()), released(final(driver).state()),
        WaitEvent::Horizon { slot: registration.slot_idx as usize, xmin: result->Ok_0.xmin }),
{
    let result = create_transaction_snapshot_acquired(driver, registration);
    proof {
        if result.is_ok() {
            assert(0 < driver.state().slots[registration.slot_idx as int].snapshot_xmin
                <= driver.state().slots[registration.slot_idx as int].txid);
        }
    }
    result
}

pub proof fn history_reader_survives_wait(entry: State, acquired: State,
    owner: Option<ProcArrayRegistration>, trace: WaitTrace, reader: u64)
    requires well_formed(entry), trace_valid(entry, acquired, owner, trace),
        entry.reservations.contains(reader), owner.is_some() ==> live_owner(entry, owner.unwrap()),
    ensures acquired.reservations.contains(reader), reader < acquired.clock,
{
    acquisition_preserves_owned_history(entry, acquired, owner, trace);
    let i = choose|i: int| 0 <= i < entry.reservations.len() && entry.reservations[i] == reader;
    assert(acquired.reservations[i] == reader);
}

pub proof fn interfering_slot_changes_have_witness()
{
    let empty = Seq::new(PROCARRAY_SLOTS as nat, |i: int| Slot { txid: 0, snapshot_xmin: 0 });
    let slots = empty.update(0, Slot { txid: 2, snapshot_xmin: 2 })
        .update(1, Slot { txid: 5, snapshot_xmin: 1 })
        .update(2, Slot { txid: 7, snapshot_xmin: 7 });
    let entry = State { slots, clock: 8, sampled_clock: 8,
        reservations: seq![2u64, 5u64, 7u64], lifecycle_held: false };
    let ended = State { slots: slots.update(1, Slot { txid: 0, snapshot_xmin: 0 }), ..entry };
    let registered = State { slots: ended.slots.update(1, Slot { txid: 8, snapshot_xmin: 8 }),
        clock: 9, reservations: entry.reservations.push(8), ..ended };
    let horizon = State { slots: registered.slots.update(2, Slot { txid: 7, snapshot_xmin: 2 }), ..registered };
    let acquired = State { lifecycle_held: true, ..horizon };
    let owner = ProcArrayRegistration { slot_idx: 0, txid: 2 };
    let trace = WaitTrace { states: seq![entry, ended, registered, horizon],
        events: seq![WaitEvent::End { slot: 1 }, WaitEvent::Register { slot: 1, txid: 8 },
            WaitEvent::Horizon { slot: 2, xmin: 2 }] };
    assert(well_formed(entry));
    assert(trace_valid(entry, acquired, Some(owner), trace));
    acquisition_preserves_owned_history(entry, acquired, Some(owner), trace);
    assert(acquired.slots[0] == entry.slots[0]);
    assert(acquired.slots[1].txid != entry.slots[1].txid);
    assert(acquired.slots[2].snapshot_xmin != entry.slots[2].snapshot_xmin);
    assert(acquired.reservations.len() > entry.reservations.len());
    assert(acquired.slots[1].txid == 8 && entry.slots[1].txid == 5);
    active_slot_membership(acquired, 1, PROCARRAY_SLOTS as int);
    assert(active(acquired.slots, PROCARRAY_SLOTS as int).contains(8));
    assert(!entry.reservations.contains(8));
    assert(!wait_step(entry, acquired, WaitEvent::End { slot: 0 }));
    assert(!allowed(WaitEvent::End { slot: 0 }, Some(owner)));
    assert(!allowed(WaitEvent::Register { slot: 0, txid: 8 }, Some(owner)));
    assert(!allowed(WaitEvent::Horizon { slot: 0, xmin: 1 }, Some(owner)));
}
}
