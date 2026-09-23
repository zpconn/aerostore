// Generated from actual ProcArray lifecycle operations; see generate.py.
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
    let ghost initial = driver.state(); snapshot_locked ( driver )
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
