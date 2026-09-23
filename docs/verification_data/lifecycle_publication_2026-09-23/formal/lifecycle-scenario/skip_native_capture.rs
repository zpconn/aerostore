// Generated shared lifecycle/clock scenario; see generate.py.
// Checked composition of the exact generated native operations. This harness
// is not linked into AeroStore. Raw index/guard/atomic correspondence is explicit.
mod lifecycle { // Generated from actual ProcArray lifecycle operations; see generate.py.
// Native ProcArray algorithms, conditional on mutex exclusion, acquisition
// framing, slot atomic projection, shared-clock linearization, allocation, and
// scoped guard release. Acquisition framing is a separate unproved assumption:
// a real blocking mutex does not preserve metadata from native API entry.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub const PROCARRAY_SLOTS: usize = 256;
pub const EMPTY_SLOT: u64 = 0;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProcArrayRegistration { pub slot_idx: u16, pub txid: u64 }
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProcArrayError {
    NoFreeSlot { txid: u64 }, InvalidSlot { slot_idx: u16 },
    SlotOwnershipMismatch { slot_idx: u16, expected_txid: u64, observed_txid: u64 },
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Slot { pub txid: u64, pub snapshot_xmin: u64 }
pub struct ProcSnapshot { pub xmin: u64, pub xmax: u64, pub in_flight: Vec<u64>, pub in_flight_len: u16 }
// Operation-local input metadata is the view at lifecycle-lock acquisition,
// not an arbitrary real API-entry view. The current driver gives acquisition
// an exact frame. Composing released calls therefore excludes unmodeled slot
// transitions before/during acquisition; shared-clock advances can still be
// represented at the explicit observation/reservation events below. A native
// interference/ownership refinement relating API entry to acquired state is
// not established by this model or by mutex exclusion alone.
pub struct State {
    pub slots: Seq<Slot>,
    // Latest observation of the shared atomic clock; external allocations may
    // advance it at the next modeled observation/reservation. It is not frozen
    // physically while the ProcArray mutex is held.
    pub clock: u64,
    // The last atomic load may lag the current counter, including a Relaxed
    // snapshot load. Native xmax adjustment must cover all active txids.
    pub sampled_clock: u64,
    // Logical history of reservations made through this shared-clock interface.
    // Interfering allocations may skip values; no synthetic consecutive IDs.
    pub reservations: Seq<u64>,
    pub lifecycle_held: bool,
}
pub open spec fn ordered(ids: Seq<u64>) -> bool {
    forall|i: int, j: int| 0 <= i < j < ids.len() ==> ids[i] < ids[j]
}
pub open spec fn well_formed(s: State) -> bool {
    s.slots.len() == PROCARRAY_SLOTS && 0 < s.sampled_clock <= s.clock
    && ordered(s.reservations)
    && (forall|i: int| 0 <= i < s.reservations.len() ==> 0 < #[trigger] s.reservations[i] < s.clock)
    && (forall|i: int| 0 <= i < s.slots.len() ==>
        if #[trigger] s.slots[i].txid == 0 { s.slots[i].snapshot_xmin == 0 }
        else { 0 < s.slots[i].snapshot_xmin <= s.slots[i].txid < s.clock
            && s.reservations.contains(s.slots[i].txid) })
    && (forall|i: int, j: int| 0 <= i < j < s.slots.len() && s.slots[i].txid != 0 ==>
        s.slots[i].txid != s.slots[j].txid)
}
pub open spec fn active(slots: Seq<Slot>, n: int) -> Seq<u64>
    decreases n,
{
    if n <= 0 { Seq::empty() }
    else if slots[n - 1].txid == 0 { active(slots, n - 1) }
    else { active(slots, n - 1).push(slots[n - 1].txid) }
}
pub open spec fn minimum_active(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_active(slots, n - 1, ceiling) }
    else { if slots[n - 1].txid < minimum_active(slots, n - 1, ceiling) {
        slots[n - 1].txid
    } else { minimum_active(slots, n - 1, ceiling) } }
}
pub open spec fn maximum_active(slots: Seq<Slot>, n: int) -> u64
    decreases n,
{
    if n <= 0 { 0 }
    else { if slots[n - 1].txid > maximum_active(slots, n - 1) {
        slots[n - 1].txid
    } else { maximum_active(slots, n - 1) } }
}
pub open spec fn minimum_retention(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_retention(slots, n - 1, ceiling) }
    else { if slots[n - 1].snapshot_xmin < minimum_retention(slots, n - 1, ceiling) {
        slots[n - 1].snapshot_xmin
    } else { minimum_retention(slots, n - 1, ceiling) } }
}
pub open spec fn coherent_snapshot(s: State, snapshot: ProcSnapshot) -> bool {
    snapshot.in_flight@ == active(s.slots, s.slots.len() as int)
    && snapshot.in_flight_len == snapshot.in_flight.len()
    && snapshot.xmax == if s.sampled_clock > maximum_active(s.slots, s.slots.len() as int) + 1 {
        s.sampled_clock as int
    } else { maximum_active(s.slots, s.slots.len() as int) + 1 }
    && snapshot.xmin == minimum_active(s.slots, s.slots.len() as int, s.sampled_clock)
}
pub open spec fn observation(before: State, after: State) -> bool {
    after.slots == before.slots && after.reservations == before.reservations
    && after.lifecycle_held == before.lifecycle_held && after.clock >= before.clock
    && 0 < after.sampled_clock <= after.clock
}
pub open spec fn reservation(before: State, after: State, id: u64) -> bool {
    id >= before.clock && id < u64::MAX && after.clock == id + 1
    && after.reservations == before.reservations.push(id)
    && after.slots == before.slots && after.lifecycle_held == before.lifecycle_held
    && after.sampled_clock == before.sampled_clock
}

pub trait LifecyclePrimitives {
    spec fn state(&self) -> State;
    // EXTRA ASSUMPTION: this preserves the operation-local acquired-input view.
    // Native ShmMutex::lock may block while other threads change slots/history;
    // that interference is not represented here. The algorithm's old(state)
    // postconditions must not be read as frames from arbitrary native API entry.
    fn lock_lifecycle(&mut self)
        requires !old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: true, ..old(self).state() });
    fn release_lifecycle(&mut self)
        requires old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: false, ..old(self).state() });
    // The lock precondition is an obligation of the registration protocol,
    // not a property assumed of all AtomicU64::fetch_add calls.
    fn reserve_registration(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held, 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn reserve_publication(&mut self) -> (id: u64)
        requires 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn load_clock(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held,
        ensures observation(old(self).state(), final(self).state()), id == final(self).state().sampled_clock;
    fn slot_txid(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].txid;
    fn slot_xmin(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].snapshot_xmin;
    fn compare_empty_register(&mut self, slot: usize, txid: u64) -> (ok: bool)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures ok == (old(self).state().slots[slot as int].txid == EMPTY_SLOT),
            final(self).state() == (State { slots: if ok {
                old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] })
            } else { old(self).state().slots }, ..old(self).state() });
    fn store_xmin(&mut self, slot: usize, xmin: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { snapshot_xmin: xmin, ..old(self).state().slots[slot as int] }), ..old(self).state() });
    fn store_txid(&mut self, slot: usize, txid: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] }), ..old(self).state() });
}

pub fn checked_slot(slot: u16) -> (result: Result<usize, ProcArrayError>)
    ensures result.is_ok() ==> result.unwrap() == slot as usize && slot < PROCARRAY_SLOTS,
        result.is_err() ==> slot >= PROCARRAY_SLOTS,
{
    if (slot as usize) < PROCARRAY_SLOTS { Ok(slot as usize) }
    else { Err(ProcArrayError::InvalidSlot { slot_idx: slot }) }
}

pub proof fn reservation_preserves_clock_history(before: State, after: State, id: u64)
    requires well_formed(before), reservation(before, after, id),
    ensures well_formed(after), after.reservations.contains(id),
        forall|i: int| 0 <= i < before.reservations.len() ==> before.reservations[i] < id,
{}
pub proof fn observation_preserves_state(before: State, after: State)
    requires well_formed(before), observation(before, after),
    ensures well_formed(after),
{}

pub proof fn snapshot_scan_bounds(s: State, n: int)
    requires well_formed(s), 0 <= n <= s.slots.len(),
    ensures active(s.slots, n).len() <= n,
        0 < minimum_active(s.slots, n, s.sampled_clock) <= s.sampled_clock,
        maximum_active(s.slots, n) < s.clock,
        active(s.slots, n).len() == 0 ==> minimum_active(s.slots, n, s.sampled_clock) == s.sampled_clock,
        active(s.slots, n).len() == 0 ==> maximum_active(s.slots, n) == 0,
    decreases n,
{
    if n > 0 { snapshot_scan_bounds(s, n - 1); }
}

pub proof fn snapshot_minimum_covers_slot(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures minimum_active(s.slots, n, s.sampled_clock) <= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { snapshot_minimum_covers_slot(s, i, n - 1); }
}

pub proof fn snapshot_fields_unchanged(before: Seq<Slot>, after: Seq<Slot>, n: int, ceiling: u64)
    requires before.len() == after.len(), 0 <= n <= before.len(),
        forall|i: int| 0 <= i < before.len() ==> before[i].txid == after[i].txid,
    ensures active(before, n) == active(after, n),
        minimum_active(before, n, ceiling) == minimum_active(after, n, ceiling),
        maximum_active(before, n) == maximum_active(after, n),
    decreases n,
{
    if n > 0 { snapshot_fields_unchanged(before, after, n - 1, ceiling); }
}

pub proof fn reservation_after_reader(before: State, after: State, stamp: u64, reader: u64)
    requires well_formed(before), reservation(before, after, stamp), before.reservations.contains(reader),
    ensures stamp > reader,
{
    let i = choose|i: int| 0 <= i < before.reservations.len() && before.reservations[i] == reader;
    assert(reader < before.clock);
}

pub proof fn active_slot_membership(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures active(s.slots, n).contains(s.slots[i].txid), maximum_active(s.slots, n) >= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { active_slot_membership(s, i, n - 1); }
}

pub proof fn snapshot_covers_active_writer(s: State, snapshot: ProcSnapshot, writer_slot: int)
    requires well_formed(s), coherent_snapshot(s, snapshot),
        0 <= writer_slot < s.slots.len(), s.slots[writer_slot].txid != 0,
    ensures snapshot.in_flight@.contains(s.slots[writer_slot].txid),
        0 < snapshot.xmin <= s.slots[writer_slot].txid < snapshot.xmax <= s.clock,
{
    snapshot_scan_bounds(s, s.slots.len() as int);
    snapshot_minimum_covers_slot(s, writer_slot, s.slots.len() as int);
    active_slot_membership(s, writer_slot, s.slots.len() as int);
}

pub proof fn retention_covers_active_snapshot(s: State, slot: int, n: int)
    requires well_formed(s), 0 <= slot < n <= s.slots.len(), s.slots[slot].txid != 0,
    ensures minimum_retention(s.slots, n, s.sampled_clock) <= s.slots[slot].snapshot_xmin,
    decreases n,
{
    if slot < n - 1 { retention_covers_active_snapshot(s, slot, n - 1); }
}

pub proof fn reservation_history_strict_order(s: State, earlier: int, later: int)
    requires well_formed(s), 0 <= earlier < later < s.reservations.len(),
    ensures s.reservations[earlier] < s.reservations[later],
{}

// Non-vacuity witnesses include an older retained writer and a stale sampled
// clock below both active IDs. The native xmax compensation is necessary for
// this permitted observation; a latest-value load is not assumed.
pub proof fn lifecycle_contract_has_live_and_stale_witnesses() {
    let empty = Seq::new(PROCARRAY_SLOTS as nat, |i: int| Slot { txid: 0, snapshot_xmin: 0 });
    let initial = State { slots: empty, clock: 1, sampled_clock: 1,
        reservations: Seq::empty(), lifecycle_held: false };
    assert(well_formed(initial));
    let live = State { slots: empty.update(0, Slot { txid: 5, snapshot_xmin: 2 })
        .update(1, Slot { txid: 10, snapshot_xmin: 5 }), clock: 11, sampled_clock: 1,
        reservations: seq![2u64, 5u64, 10u64], lifecycle_held: true };
    assert(well_formed(live));
    assert(live.sampled_clock < live.slots[0].txid < live.slots[1].txid);
    let later = State { clock: 12, reservations: live.reservations.push(11), ..live };
    assert(reservation(live, later, 11));
    reservation_after_reader(live, later, 11, 10);
    assert(11u64 > 10u64);
}

pub fn begin_transaction<D: LifecyclePrimitives>(driver: &mut D) -> (result: Result<ProcArrayRegistration, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock > old(driver).state().clock,
        final(driver).state().reservations.len() == old(driver).state().reservations.len() + 1,
        result.is_ok() ==> {
            let r = result.unwrap();
            r.slot_idx < PROCARRAY_SLOTS && r.txid >= old(driver).state().clock
            && final(driver).state().reservations == old(driver).state().reservations.push(r.txid)
            && old(driver).state().slots[r.slot_idx as int].txid == 0
            && final(driver).state().slots == old(driver).state().slots.update(r.slot_idx as int,
                Slot { txid: r.txid, snapshot_xmin: r.txid })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots
            && (forall|i: int| 0 <= i < old(driver).state().slots.len() ==> old(driver).state().slots[i].txid != 0),
        match result {
            Err(ProcArrayError::NoFreeSlot { txid }) => txid >= old(driver).state().clock
                && final(driver).state().reservations == old(driver).state().reservations.push(txid),
            Err(_) => false,
            Ok(_) => true,
        },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let ghost before_reservation = driver.state(); let txid = driver . reserve_registration ( ) ;
proof { reservation_preserves_clock_history(before_reservation, driver.state(), txid); } let ghost reserved = driver.state(); 
        let mut slot_idx: usize = 0;
        while slot_idx < PROCARRAY_SLOTS
            invariant slot_idx <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(initial), well_formed(reserved), driver.state() == reserved,
                reserved.slots == initial.slots, reserved.lifecycle_held,
                reserved.clock > initial.clock, txid >= initial.clock, txid < reserved.clock,
                reserved.reservations == initial.reservations.push(txid),
                forall|i: int| 0 <= i < slot_idx ==> initial.slots[i].txid != 0,
            decreases PROCARRAY_SLOTS - slot_idx,
        {
     if driver . compare_empty_register ( slot_idx , txid ) {
    driver . store_xmin ( slot_idx , txid ) ;
    return Ok ( ProcArrayRegistration {
        slot_idx : slot_idx as u16 , txid ,
    }
    ) ;
}

            slot_idx += 1;
        }
     Err ( ProcArrayError :: NoFreeSlot {
    txid
}
)
}

pub fn end_transaction<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<(), ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock == old(driver).state().clock,
        final(driver).state().sampled_clock == old(driver).state().sampled_clock,
        final(driver).state().reservations == old(driver).state().reservations,
        result.is_ok() ==> registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { txid: 0, snapshot_xmin: 0 }),
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        result == if registration.slot_idx >= PROCARRAY_SLOTS {
            Err(ProcArrayError::InvalidSlot { slot_idx: registration.slot_idx })
        } else if old(driver).state().slots[registration.slot_idx as int].txid != registration.txid {
            Err(ProcArrayError::SlotOwnershipMismatch { slot_idx: registration.slot_idx,
                expected_txid: registration.txid,
                observed_txid: old(driver).state().slots[registration.slot_idx as int].txid })
        } else { Ok(()) },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot_idx = registration . slot_idx as usize ;
if slot_idx >= PROCARRAY_SLOTS {
    return Err ( ProcArrayError :: InvalidSlot {
        slot_idx : registration . slot_idx ,
    }
    ) ;
}
let slot = slot_idx ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
driver . store_xmin ( slot , EMPTY_SLOT ) ;
driver . store_txid ( slot , EMPTY_SLOT ) ;
Ok ( ( ) )
}

pub fn snapshot_locked<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), observation(old(driver).state(), final(driver).state()),
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); let mut xmax = driver . load_clock ( ) ;
let mut xmin = xmax ;
let mut max_in_flight = 0_u64 ;
let mut in_flight = Vec :: new ( ) ;
let mut in_flight_len = 0_u16 ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(initial, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, observation(initial, loaded),
                loaded.lifecycle_held, xmax == loaded.sampled_clock,
                in_flight@ == active(loaded.slots, si as int),
                in_flight_len == in_flight.len(), in_flight_len <= si,
                xmin == minimum_active(loaded.slots, si as int, loaded.sampled_clock),
                max_in_flight == maximum_active(loaded.slots, si as int),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof {
                reveal_with_fuel(active, 2); reveal_with_fuel(minimum_active, 2); reveal_with_fuel(maximum_active, 2);
            }
     let txid = driver . slot_txid ( slot ) ;
if txid == EMPTY_SLOT {
    continue ;
}
proof { assert((in_flight_len as usize) < PROCARRAY_SLOTS); } in_flight . push ( txid ) ;
in_flight_len += 1 ;
xmin = xmin . min ( txid ) ;
max_in_flight = max_in_flight . max ( txid ) ;

        }
        proof { snapshot_scan_bounds(loaded, PROCARRAY_SLOTS as int); }
     if in_flight_len == 0 {
    xmin = xmax ;
}
else {
    xmax = xmax . max ( max_in_flight . saturating_add ( 1 ) ) ;
}
ProcSnapshot {
    xmin , xmax , in_flight , in_flight_len ,
}
}

pub fn create_snapshot<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
snapshot_locked ( driver )
}

pub fn create_transaction_snapshot<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<ProcSnapshot, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held, registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result.is_ok() ==> {
            let snap = result->Ok_0;
            registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && coherent_snapshot(final(driver).state(), snap)
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { snapshot_xmin: snap.xmin, ..old(driver).state().slots[registration.slot_idx as int] })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        (registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid) == result.is_ok(),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot = checked_slot ( registration . slot_idx ) ? ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
let snapshot = snapshot_locked ( driver ) ;
let ghost snapshot_state = driver.state(); proof { snapshot_scan_bounds(snapshot_state, PROCARRAY_SLOTS as int); snapshot_minimum_covers_slot(snapshot_state, slot as int, PROCARRAY_SLOTS as int); } driver . store_xmin ( slot , snapshot . xmin ) ;
proof { snapshot_fields_unchanged(snapshot_state.slots, driver.state().slots, PROCARRAY_SLOTS as int, snapshot_state.sampled_clock); } Ok ( snapshot )
}

pub fn oldest_snapshot_xmin<D: LifecyclePrimitives>(driver: &mut D) -> (result: u64)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result == minimum_retention(final(driver).state().slots, PROCARRAY_SLOTS as int, final(driver).state().sampled_clock),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let mut xmin = driver . load_clock ( ) ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(State { lifecycle_held: true, ..initial }, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, loaded.lifecycle_held,
                loaded.slots == initial.slots, loaded.reservations == initial.reservations,
                loaded.clock >= initial.clock,
                xmin == minimum_retention(loaded.slots, si as int, loaded.sampled_clock),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof { reveal_with_fuel(minimum_retention, 2); }
     if driver . slot_txid ( slot ) != EMPTY_SLOT {
    xmin = xmin . min ( driver . slot_xmin ( slot ) ) ;
}

        }
     xmin
}

// This expression is extracted from publish_index_stamps, where the existing
// commit driver invokes it after finish_transaction. Its atomic operation is
// the same shared allocator used by native begin_transaction.
pub fn reserve_publication_clock<D: LifecyclePrimitives>(driver: &mut D) -> (stamp: u64)
    requires well_formed(old(driver).state()), old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), reservation(old(driver).state(), final(driver).state(), stamp),
{
    let ghost before = driver.state();
    let stamp = driver . reserve_publication ( );
    proof { reservation_preserves_clock_history(before, driver.state(), stamp); }
    stamp
}

// Proof harness: the actual end operation, its explicit scoped-guard boundary,
// and the actual publication-reservation expression. No extra reservation is
// inserted into AeroStore; composition must identify this event with the same
// fetch_add used by publish_index_stamps, never perform a second reservation.
pub fn deregister_then_reserve<D: LifecyclePrimitives>(driver: &mut D,
    registration: ProcArrayRegistration, reader: u64) -> (result: Result<u64, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0, old(driver).state().clock < u64::MAX,
        old(driver).state().reservations.contains(reader),
    ensures well_formed(final(driver).state()), !final(driver).state().lifecycle_held,
        result.is_ok() ==> result.unwrap() > reader,
        result.is_ok() ==> final(driver).state().reservations == old(driver).state().reservations.push(result.unwrap()),
        result.is_ok() ==> (forall|i: int| 0 <= i < final(driver).state().slots.len() ==>
            final(driver).state().slots[i].txid != registration.txid),
        result.is_err() ==> final(driver).state().reservations == old(driver).state().reservations,
{
    let result = end_transaction(driver, registration);
    driver.release_lifecycle();
    match result {
        Err(error) => Err(error),
        Ok(()) => {
            let ghost before = driver.state();
            let stamp = reserve_publication_clock(driver);
            proof { reservation_after_reader(before, driver.state(), stamp, reader); }
            Ok(stamp)
        }
    }
}
}
 }
mod predicate { // Generated from native indexed transaction operations; see generate.py.
// The traits below are explicitly unproved primitive boundaries. The three
// native algorithms are inserted from production source by generate.py.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub type Pair = (usize, usize);
#[derive(PartialEq, Eq, Debug)]
pub enum Error { IndexBindingsIncomplete, Other }
#[derive(Clone, Copy)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
#[derive(Clone, Copy)]
pub struct IndexChange { pub binding: usize, pub before: Option<usize>, pub after: Option<usize> }
pub struct OccTransaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }
pub struct State {
    // Physical arena identity, not a process-local virtual base address.
    pub arena: usize,
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<Pair, usize>,
    // The operation's guarded stamp projection, not a snapshot claiming that
    // every bucket in the concurrently changing database is frozen.
    pub stamps: Map<Pair, u64>,
    pub held: Set<Pair>,
    // Last represented clock observation. Interfering reservations may advance
    // the actual allocator before this operation's fetch_add linearizes.
    pub clock: u64,
    pub reserved_stamp: u64,
    pub reservations: Seq<u64>,
    pub deregistered: bool,
}
// An arbitrary fixed key-to-bucket function; no hash injectivity is assumed.
pub open spec fn key_bucket(s: State, binding: usize, key: usize) -> usize { s.key_buckets[(binding, key)] }
pub open spec fn pair_less(a: Pair, b: Pair) -> bool {
    a.0 < b.0 || (a.0 == b.0 && a.1 < b.1)
}
pub open spec fn canonical(v: Seq<Pair>, members: Set<Pair>) -> bool {
    v.to_set() == members
    && forall|i: int, j: int| 0 <= i < j < v.len() ==> pair_less(v[i], v[j])
}
pub open spec fn read_pair(s: State, r: IndexRead) -> Pair { (s.bindings[r.index_offset], r.bucket) }
pub open spec fn read_keys(s: State, reads: Seq<IndexRead>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { read_keys(s, reads, n - 1).insert(read_pair(s, reads[n - 1])) }
}
pub open spec fn option_keys(s: State, binding: usize, key: Option<usize>) -> Set<Pair> {
    match key { Some(k) => Set::empty().insert((binding, key_bucket(s, binding, k))), None => Set::empty() }
}
pub open spec fn change_keys_one(s: State, c: IndexChange) -> Set<Pair> {
    option_keys(s, c.binding, c.before).union(option_keys(s, c.binding, c.after))
}
pub open spec fn change_keys(s: State, changes: Seq<IndexChange>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { change_keys(s, changes, n - 1).union(change_keys_one(s, changes[n - 1])) }
}
pub open spec fn all_reads_bound(s: State, reads: Seq<IndexRead>, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> s.bindings.contains_key(reads[i].index_offset)
        && s.bindings[reads[i].index_offset] < s.binding_count
}
pub open spec fn changes_valid(s: State, changes: Seq<IndexChange>) -> bool {
    forall|i: int| 0 <= i < changes.len() ==> changes[i].binding < s.binding_count
}
pub open spec fn read_good(s: State, r: IndexRead, txid: u64) -> bool {
    s.bindings.contains_key(r.index_offset)
    && s.stamps.contains_key(read_pair(s, r))
    && s.stamps[read_pair(s, r)] == r.stamp
    && s.stamps[read_pair(s, r)] < txid
}
pub open spec fn reads_good(s: State, reads: Seq<IndexRead>, txid: u64, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> read_good(s, reads[i], txid)
}
pub open spec fn stamp_relation(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, stamp: u64) -> bool {
    after.dom() == before.dom().union(done)
    && forall|p: Pair| after.contains_key(p) ==> after[p] == if done.contains(p) { stamp } else { before[p] }
}

// BTreeSet's ordinary extensional and canonical-iteration contracts. Its
// implementation and Rust allocation/panic behavior are not proved here.
pub trait PairSet {
    spec fn contents(&self) -> Set<Pair>;
    fn new() -> (out: Self) where Self: Sized
        ensures out.contents() == Set::<Pair>::empty();
    fn insert(&mut self, pair: Pair)
        ensures final(self).contents() == old(self).contents().insert(pair);
    fn into_vec(self) -> (out: Vec<Pair>) where Self: Sized
        ensures canonical(out@, self.contents());
}

// Immutable registry lookup, key encoding/hash, guarded Acquire loads, and
// Release stores remain assumptions. A mutable proof receiver models one
// operation's state; it does not assert exclusive access to the real database.
pub trait Primitives {
    spec fn state(&self) -> State;
    fn find_binding(&self, offset: usize) -> (r: Result<usize, Error>)
        ensures r.is_ok() ==> self.state().bindings.contains_key(offset)
            && r.unwrap() == self.state().bindings[offset]
            && r.unwrap() < self.state().binding_count;
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, Error>)
        requires binding < self.state().binding_count,
        ensures r.is_ok() ==> r.unwrap() == key_bucket(self.state(), binding, *key);
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, Error>)
        requires self.state().held.contains((binding, bucket)), binding < self.state().binding_count,
        ensures r.is_ok() ==> self.state().stamps.contains_key((binding, bucket))
            && r.unwrap() == self.state().stamps[(binding, bucket)];
    fn reserve_stamp(&mut self) -> (stamp: u64)
        requires old(self).state().deregistered, old(self).state().clock < u64::MAX,
        ensures old(self).state().clock <= stamp < u64::MAX,
            final(self).state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                reservations: old(self).state().reservations.push(stamp), ..old(self).state() });
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64) -> (r: Result<(), Error>)
        requires old(self).state().held.contains((binding, bucket)), old(self).state().deregistered,
            binding < old(self).state().binding_count,
        ensures final(self).state() == (State {
            stamps: if r.is_ok() { old(self).state().stamps.insert((binding, bucket), stamp) } else { old(self).state().stamps },
            ..old(self).state()
        });
}

pub proof fn stamp_unchanged(before: Map<Pair, u64>, stamp: u64)
    ensures stamp_relation(before, before, Set::empty(), stamp),
{
    assert(before.dom() =~= before.dom().union(Set::empty()));
}

pub proof fn stamp_update(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, p: Pair, stamp: u64)
    requires stamp_relation(before, after, done, stamp),
    ensures stamp_relation(before, after.insert(p, stamp), done.insert(p), stamp),
{
    assert(after.insert(p, stamp).dom() =~= before.dom().union(done.insert(p)));
}

pub proof fn read_key_member(s: State, reads: Seq<IndexRead>, i: int, n: int)
    requires 0 <= i < n <= reads.len(),
    ensures read_keys(s, reads, n).contains(read_pair(s, reads[i])),
    decreases n,
{
    if i < n - 1 { read_key_member(s, reads, i, n - 1); }
}

pub proof fn changed_binding_in_range(s: State, changes: Seq<IndexChange>, n: int, pair: Pair)
    requires 0 <= n <= changes.len(), changes_valid(s, changes), change_keys(s, changes, n).contains(pair),
    ensures pair.0 < s.binding_count,
    decreases n,
{
    if n > 0 && change_keys(s, changes, n - 1).contains(pair) {
        changed_binding_in_range(s, changes, n - 1, pair);
    }
}

// Composition uses the stamp actually written by the native publication
// theorem. Establishing stamp >= reader.txid from the shared allocator and
// snapshot/ProcArray execution is a separate temporal refinement obligation.
// In particular, the publishing writer's own (possibly older) txid is absent.
pub proof fn publication_invalidates_dependency(before: State, after: State,
    reads: Seq<IndexRead>, txid: u64, touched: Set<Pair>, stamp: u64, i: int)
    requires 0 <= i < reads.len(), before.bindings == after.bindings,
        touched.contains(read_pair(before, reads[i])), stamp >= txid,
        stamp_relation(before.stamps, after.stamps, touched, stamp),
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(after.stamps.contains_key(read_pair(after, reads[i])));
    assert(after.stamps[read_pair(after, reads[i])] == stamp);
    assert(!read_good(after, reads[i], txid));
}

// A changed captured stamp also invalidates the read regardless of txid.
pub proof fn changed_stamp_invalidates_dependency(after: State,
    reads: Seq<IndexRead>, txid: u64, i: int, stamp: u64)
    requires 0 <= i < reads.len(),
        after.stamps.contains_key(read_pair(after, reads[i])),
        after.stamps[read_pair(after, reads[i])] == stamp, stamp != reads[i].stamp,
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(!read_good(after, reads[i], txid));
}

pub fn index_lock_keys<D: Primitives, C: PairSet>(driver: &D, tx: &OccTransaction, changes: &Vec<IndexChange>)
    -> (result: Result<Vec<Pair>, Error>)
    requires changes_valid(driver.state(), changes@),
    ensures result.is_ok() ==> canonical(result->Ok_0@,
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
            .union(change_keys(driver.state(), changes@, changes.len() as int))),
        result.is_ok() ==> all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
{
    let mut keys = C :: new ( ) ;

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, ri as int),
                all_reads_bound(driver.state(), tx.index_reads@, ri as int),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
     let binding = driver . find_binding ( read . index_offset ) ? ;
keys . insert ( ( binding , read . bucket ) ) ;

            proof { reveal_with_fuel(read_keys, 2); }
            ri = ri + 1;
        }
     
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), changes_valid(driver.state(), changes@),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
                    .union(change_keys(driver.state(), changes@, ci as int)),
                all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     let index = change . binding ;
if let Some ( key ) = & change . before {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}
if let Some ( key ) = & change . after {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     Ok ( keys . into_vec ( ) )
}

pub fn index_read_conflict<D: Primitives>(driver: &D, tx: &OccTransaction)
    -> (result: Result<bool, Error>)
    requires read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap() ==
        (tx.index_conflict || !reads_good(driver.state(), tx.index_reads@, tx.txid, tx.index_reads.len() as int)),
{
    if tx . index_conflict {
    return Ok ( true ) ;
}

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(), !tx.index_conflict,
                reads_good(driver.state(), tx.index_reads@, tx.txid, ri as int),
                read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
            proof { read_key_member(driver.state(), tx.index_reads@, ri as int, tx.index_reads.len() as int); }
     let bound = driver . find_binding ( read . index_offset ) ? ;
let stamp = driver . transactional_stamp ( bound , read . bucket ) ? ;
if stamp != read . stamp || ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    return Ok ( true ) ;
}

            ri = ri + 1;
        }
     Ok ( false )
}

pub fn publish_index_stamps<D: Primitives, C: PairSet>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<(), Error>)
    requires old(driver).state().deregistered, old(driver).state().clock < u64::MAX,
        changes_valid(old(driver).state(), changes@),
        change_keys(old(driver).state(), changes@, changes.len() as int).subset_of(old(driver).state().held),
    ensures final(driver).state().bindings == old(driver).state().bindings,
        final(driver).state().arena == old(driver).state().arena,
        final(driver).state().binding_count == old(driver).state().binding_count,
        final(driver).state().key_buckets == old(driver).state().key_buckets,
        final(driver).state().held == old(driver).state().held,
        final(driver).state().deregistered == old(driver).state().deregistered,
        changes.len() > 0 ==> old(driver).state().clock <= final(driver).state().reserved_stamp < u64::MAX,
        changes.len() > 0 ==> final(driver).state().clock == final(driver).state().reserved_stamp + 1,
        changes.len() > 0 ==> final(driver).state().reservations == old(driver).state().reservations.push(final(driver).state().reserved_stamp),
        changes.len() == 0 ==> final(driver).state() == old(driver).state(),
        result.is_ok() ==> stamp_relation(old(driver).state().stamps, final(driver).state().stamps,
            change_keys(old(driver).state(), changes@, changes.len() as int), final(driver).state().reserved_stamp),
        // Failures may have published a prefix; every changed stamp has the
        // reserved value, and unrelated buckets are never modified.
        forall|p: Pair| !change_keys(old(driver).state(), changes@, changes.len() as int).contains(p) ==>
            final(driver).state().stamps.contains_key(p) == old(driver).state().stamps.contains_key(p)
            && (old(driver).state().stamps.contains_key(p) ==> final(driver).state().stamps[p] == old(driver).state().stamps[p]),
        forall|p: Pair| final(driver).state().stamps.contains_key(p)
            && (!old(driver).state().stamps.contains_key(p) || final(driver).state().stamps[p] != old(driver).state().stamps[p]) ==>
                change_keys(old(driver).state(), changes@, changes.len() as int).contains(p)
                && final(driver).state().stamps[p] == final(driver).state().reserved_stamp,
{
    let ghost initial = driver.state(); proof { stamp_unchanged(initial.stamps, initial.reserved_stamp); } if changes . is_empty ( ) {
    return Ok ( ( ) ) ;
}
let stamp = driver . reserve_stamp ( ) ;
let mut touched = C :: new ( ) ;

        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                    reservations: initial.reservations.push(stamp), ..initial }),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
                touched.contents() == change_keys(initial, changes@, ci as int),
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                changes.len() > 0,
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     if let Some ( key ) = & change . before {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}
if let Some ( key ) = & change . after {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     
        proof { stamp_unchanged(initial.stamps, stamp); }
        let ordered = touched.into_vec();
        let mut pi: usize = 0;
        proof { assert(ordered@.take(0).to_set() =~= Set::<Pair>::empty()); }
        while pi < ordered.len()
            invariant pi <= ordered.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state().binding_count == initial.binding_count,
                driver.state().arena == initial.arena,
                canonical(ordered@, change_keys(initial, changes@, changes.len() as int)),
                driver.state().bindings == initial.bindings, driver.state().held == initial.held,
                driver.state().key_buckets == initial.key_buckets,
                driver.state().deregistered == initial.deregistered,
                driver.state().clock == stamp + 1, driver.state().reserved_stamp == stamp,
                driver.state().reservations == initial.reservations.push(stamp),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
                changes.len() > 0,
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                stamp_relation(initial.stamps, driver.state().stamps, ordered@.take(pi as int).to_set(), stamp),
                forall|p: Pair| !change_keys(initial, changes@, changes.len() as int).contains(p) ==>
                    driver.state().stamps.contains_key(p) == initial.stamps.contains_key(p)
                    && (initial.stamps.contains_key(p) ==> driver.state().stamps[p] == initial.stamps[p]),
            decreases ordered.len() - pi,
        {
            let (binding, bucket) = ordered[pi];
            let ghost previous_stamps = driver.state().stamps;
            proof {
                assert(ordered@.contains((binding, bucket)));
                changed_binding_in_range(initial, changes@, changes.len() as int, (binding, bucket));
            }
     driver . transactional_publish_stamp ( binding , bucket , stamp ) ? ;

            proof {
                stamp_update(initial.stamps, previous_stamps, ordered@.take(pi as int).to_set(), (binding, bucket), stamp);
                assert(ordered@.take(pi as int + 1).to_set() =~= ordered@.take(pi as int).to_set().insert((binding, bucket)));
            }
            pi = pi + 1;
        }
     Ok ( ( ) )
}

// A proof harness composing the mathematical dependency lemma with the
// extracted native validator. This harness is never compiled into AeroStore.
pub fn validate_after_late_publication<D: Primitives>(driver: &D, tx: &OccTransaction,
    Ghost(before): Ghost<State>, Ghost(touched): Ghost<Set<Pair>>,
    publication_stamp: u64, read_index: usize) -> (result: Result<bool, Error>)
    requires read_index < tx.index_reads.len(), before.bindings == driver.state().bindings,
        touched.contains(read_pair(before, tx.index_reads@[read_index as int])),
        publication_stamp >= tx.txid,
        stamp_relation(before.stamps, driver.state().stamps, touched, publication_stamp),
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {
        publication_invalidates_dependency(before, driver.state(), tx.index_reads@,
            tx.txid, touched, publication_stamp, read_index as int);
    }
    index_read_conflict(driver, tx)
}
}

verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (result: bool)
ensures result == (stamp < transaction_id),
{
stamp < transaction_id
} }
 }
mod capture { // Generated from native index_lookup; see generate.py for the boundary.
// Conditional proof of the native predicate dependency-capture loop.
// Primitive stamp visibility under held bucket guards remains an assumption.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

#[derive(PartialEq, Eq, Debug)]
pub enum Error { SerializationFailure, Index }
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
pub struct Transaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }

pub open spec fn same_key(a: IndexRead, b: IndexRead) -> bool {
    a.index_offset == b.index_offset && a.bucket == b.bucket
}
pub open spec fn unique(reads: Seq<IndexRead>) -> bool {
    forall|i: int, j: int| 0 <= i < j < reads.len() ==> !same_key(reads[i], reads[j])
}
pub open spec fn captured(reads: Seq<IndexRead>, offset: usize, bucket: usize, stamp: u64) -> bool {
    reads.contains(IndexRead { index_offset: offset, bucket, stamp })
}
pub open spec fn extends(before: Seq<IndexRead>, after: Seq<IndexRead>) -> bool {
    before.len() <= after.len()
        && forall|i: int| 0 <= i < before.len() ==> before[i] == after[i]
}
pub open spec fn permitted_additions(before: Seq<IndexRead>, after: Seq<IndexRead>,
    offset: usize, buckets: Seq<usize>, stamps: Map<usize, u64>) -> bool {
    forall|r: IndexRead| after.contains(r) ==> before.contains(r)
        || (r.index_offset == offset && buckets.contains(r.bucket)
            && stamps.contains_key(r.bucket) && r.stamp == stamps[r.bucket])
}

// Abstracts only the standard iterator's first matching element, not native
// validation. The lowering is executable and itself verified here.
pub fn find_read(reads: &Vec<IndexRead>, offset: usize, bucket: usize)
    -> (r: Option<IndexRead>)
    ensures
        r.is_some() ==> reads@.contains(r.unwrap())
            && r.unwrap().index_offset == offset && r.unwrap().bucket == bucket,
        r.is_none() ==> forall|i: int| 0 <= i < reads.len() ==>
            reads[i].index_offset != offset || reads[i].bucket != bucket,
{
    let mut i = 0;
    while i < reads.len()
        invariant i <= reads.len(),
            forall|j: int| 0 <= j < i ==> reads[j].index_offset != offset || reads[j].bucket != bucket,
        decreases reads.len() - i,
    {
        let read = &reads[i];
        if read.index_offset == offset && read.bucket == bucket { return Some(*read); }
        i += 1;
    }
    None
}

pub trait CaptureIndex {
    spec fn offset(&self) -> usize;
    spec fn stamps(&self) -> Map<usize, u64>;
    spec fn held(&self) -> Set<usize>;
    fn header_offset(&self) -> (r: usize) ensures r == self.offset();
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, Error>)
        requires self.held().contains(bucket), self.stamps().contains_key(bucket),
        ensures r.is_ok() ==> r.unwrap() == self.stamps()[bucket],
            r.is_err() ==> r == Err(Error::Index);
}

pub proof fn appended_read_preserves_unique(reads: Seq<IndexRead>, next: IndexRead)
    requires unique(reads),
        forall|i: int| 0 <= i < reads.len() ==> !same_key(reads[i], next),
    ensures unique(reads.push(next)),
{
    assert forall|i: int, j: int| 0 <= i < j < reads.push(next).len()
        implies !same_key(reads.push(next)[i], reads.push(next)[j]) by {
        if j < reads.len() { assert(!same_key(reads[i], reads[j])); }
    }
}

pub fn capture_dependencies<I: CaptureIndex>(index: &I, tx: &mut Transaction, buckets: &Vec<usize>)
    -> (result: Result<(), Error>)
    requires unique(old(tx).index_reads@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i]),
    ensures
        final(tx).txid == old(tx).txid,
        old(tx).index_conflict ==> final(tx).index_conflict,
        unique(final(tx).index_reads@), extends(old(tx).index_reads@, final(tx).index_reads@),
        permitted_additions(old(tx).index_reads@, final(tx).index_reads@, index.offset(), buckets@, index.stamps()),
        final(tx).index_reads.len() <= old(tx).index_reads.len() + buckets.len(),
        result == Err(Error::SerializationFailure) ==> final(tx).index_conflict,
        result.is_ok() ==> forall|i: int| 0 <= i < buckets.len() ==>
            captured(final(tx).index_reads@, index.offset(), buckets[i], index.stamps()[buckets[i]])
                && index.stamps()[buckets[i]] < final(tx).txid,
{
    let ghost initial_reads = tx.index_reads@;
    let mut bucket_pos = 0;
    while bucket_pos < buckets.len()
        invariant
            bucket_pos <= buckets.len(), unique(tx.index_reads@),
            initial_reads == old(tx).index_reads@, extends(initial_reads, tx.index_reads@),
            permitted_additions(initial_reads, tx.index_reads@, index.offset(), buckets@, index.stamps()),
            tx.index_reads.len() <= initial_reads.len() + bucket_pos,
            tx.txid == old(tx).txid,
            old(tx).index_conflict ==> tx.index_conflict,
            forall|j: int| 0 <= j < buckets.len() ==>
                index.held().contains(buckets[j]) && index.stamps().contains_key(buckets[j]),
            forall|j: int| 0 <= j < bucket_pos ==>
                captured(tx.index_reads@, index.offset(), buckets[j], index.stamps()[buckets[j]])
                    && index.stamps()[buckets[j]] < tx.txid,
        decreases buckets.len() - bucket_pos,
    {
        let bucket = &buckets[bucket_pos];
let stamp = index . transactional_stamp ( * bucket ) ? ;
if ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    tx . index_conflict = true ;
    return Err ( Error :: SerializationFailure ) ;
}
if let Some ( previous ) = find_read ( & tx . index_reads , index . header_offset ( ) , * bucket ) {
    if previous . stamp != stamp {
        tx . index_conflict = true ;
        return Err ( Error :: SerializationFailure ) ;
    }
}
else {
    proof {
        appended_read_preserves_unique(tx.index_reads@, IndexRead {
            index_offset: index.offset(), bucket: *bucket, stamp });
    }
    tx . index_reads . push ( IndexRead {
        index_offset : index . header_offset ( ) , bucket : * bucket , stamp ,
    }
    ) ;
}
        bucket_pos += 1;
    }
    Ok(())
}
}
verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (r: bool)
ensures r == (stamp < transaction_id),
{ stamp < transaction_id } }
 }
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
    match Ok::<(), capture::Error>(()) {
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
