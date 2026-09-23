#!/usr/bin/env python3
"""Restricted source-bound ProcArray lifecycle and snapshot adaptation."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aerostore_core/src/procarray.rs"
PUBLICATION = ROOT / "aerostore_core/src/occ_partitioned.rs"
SHM = ROOT / "aerostore_core/src/shm.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("lifecycle.verus.rs")
spec = importlib.util.spec_from_file_location("predicate_adapter", ROOT / "verification/predicate/generate.py")
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
adapter._module.TOKEN = re.compile(adapter._module.TOKEN.pattern.replace("[0-9]+", "[0-9][A-Za-z_0-9]*").replace("|==|!=|", "|==|!=|\\+=|-=|"))
tokenize, replace, show_tokens, balanced_end, loop = adapter.tokenize, adapter.replace, adapter.show_tokens, adapter.balanced_end, adapter.loop
SIGNATURES = {
    "begin_transaction": "pub fn begin_transaction(&self, global_txid: &AtomicU64,) -> Result<ProcArrayRegistration, ProcArrayError>",
    "end_transaction": "pub fn end_transaction(&self, registration: ProcArrayRegistration,) -> Result<(), ProcArrayError>",
    "create_snapshot": "pub fn create_snapshot(&self, global_txid: &AtomicU64) -> ProcSnapshot",
    "create_transaction_snapshot": "pub fn create_transaction_snapshot(&self, registration: ProcArrayRegistration, global_txid: &AtomicU64,) -> Result<ProcSnapshot, ProcArrayError>",
    "oldest_snapshot_xmin": "pub fn oldest_snapshot_xmin(&self, global_txid: &AtomicU64) -> u64",
    "snapshot_locked": "fn snapshot_locked(&self, global_txid: &AtomicU64) -> ProcSnapshot",
}
ENDS = {"begin_transaction": "\n    pub fn end_transaction(", "end_transaction": "\n    pub fn create_snapshot(",
    "create_snapshot": "\n    /// Capture and publish", "create_transaction_snapshot": "\n    /// Oldest pinned",
    "oldest_snapshot_xmin": "\n    fn snapshot_locked(", "snapshot_locked": "\n    /// Startup recovery only"}


def optional(tokens, old, new):
    needle = tokenize(old)
    count = sum(tokens[i:i+len(needle)] == needle for i in range(len(tokens)))
    if count > 1:
        raise ValueError("duplicate native operation: " + old)
    return replace(tokens, old, new, count)


def method(source, name):
    marker = "    " + ("pub fn " if name != "snapshot_locked" else "fn ") + name + "("
    start = source.index(marker)
    if tokenize(source[start:source.index("{", start)]) != tokenize(SIGNATURES[name]):
        raise ValueError("native lifecycle signature changed: " + name)
    body = adapter._module.method_body(source, marker, ENDS[name])
    if set(body).intersection({"#", "unsafe", "proof", "ghost", "tracked", "assume", "admit", "external_body", "invariant", "decreases"}):
        raise ValueError("unsupported lifecycle syntax or proof bypass")
    return body


def store_calls(tokens, field, target, index):
    prefix = tokenize("slot." + field + ".store(")
    output, pos = [], 0
    while pos < len(tokens):
        if tokens[pos:pos+len(prefix)] != prefix:
            output.append(tokens[pos]); pos += 1; continue
        opening = pos + len(prefix) - 1
        end = balanced_end(tokens, opening)
        args = tokens[opening+1:end-1]
        order = tokenize(", Ordering::Release")
        if args[-len(order):] != order:
            raise ValueError("native lifecycle store ordering changed")
        output += tokenize("driver." + target + "(" + index + ",") + args[:-len(order)] + [")"]
        pos = end
    return output


def check_shared_arena_routing(shm):
    routing = {
        "proc_array": '&self.header_ref().expect("shared memory header was unexpectedly invalid").proc_array',
        "global_txid": '&self.header_ref().expect("shared memory header was unexpectedly invalid").next_txid',
        "begin_transaction": 'self.proc_array().begin_transaction(self.global_txid())',
        "end_transaction": 'self.proc_array().end_transaction(registration)',
        "create_snapshot": 'self.proc_array().create_snapshot(self.global_txid())',
        "create_transaction_snapshot": 'self.proc_array().create_transaction_snapshot(registration, self.global_txid())',
    }
    for name, expected in routing.items():
        actual = adapter._module.method_body(shm, "    pub fn " + name + "(", "\n    #[inline]")
        if actual != tokenize(expected):
            raise ValueError("shared-arena clock/ProcArray routing changed: " + name)


def render(source, publication=None, shm=None):
    check_shared_arena_routing(SHM.read_text() if shm is None else shm)
    if source.count("pub const PROCARRAY_SLOTS: usize = 256;") != 1 or source.count("const EMPTY_SLOT: u64 = 0;") != 1:
        raise ValueError("review native slot count/sentinel")
    hook = '''#[cfg(test)]
        REGISTRATION_RESERVED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });'''
    if source.count(hook) != 1:
        raise ValueError("native registration test-hook changed")
    source = source.replace(hook, "", 1)
    acquisition_hook = '''#[cfg(test)]
        SNAPSHOT_ACQUIRING_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });'''
    if source.count(acquisition_hook) != 1:
        raise ValueError("native snapshot acquisition test-hook changed")
    source = source.replace(acquisition_hook, "", 1)
    bodies = {name: method(source, name) for name in SIGNATURES}
    for name, body in bodies.items():
        body = optional(body, "let _lifecycle = self.lifecycle.lock();", "driver.lock_lifecycle();")
        body = optional(body, "self.snapshot_locked(global_txid)", "snapshot_locked(driver)")
        body = optional(body, "global_txid.load(Ordering::Acquire)", "driver.load_clock()")
        body = optional(body, "global_txid.load(Ordering::Relaxed)", "driver.load_clock()")
        acquire_load = tokenize("slot.load(Ordering::Acquire)")
        load_count = sum(body[i:i+len(acquire_load)] == acquire_load for i in range(len(body)))
        body = replace(body, "slot.load(Ordering::Acquire)", "driver.slot_txid(slot)", load_count)
        body = optional(body, "slot.load(Ordering::Relaxed)", "driver.slot_txid(slot)")
        body = optional(body, "slot.snapshot_xmin.load(Ordering::Acquire)", "driver.slot_xmin(slot)")
        body = optional(body, "slot.snapshot_xmin.load(Ordering::Relaxed)", "driver.slot_xmin(slot)")
        body = store_calls(body, "snapshot_xmin", "store_xmin", "slot_idx" if name == "begin_transaction" else "slot")
        body = store_calls(body, "txid", "store_txid", "slot")
        bodies[name] = body
    begin = bodies["begin_transaction"]
    begin = replace(begin, "global_txid.fetch_add(1, Ordering::AcqRel)", "driver.reserve_registration()", 1)
    begin = optional(begin, "slot.txid.compare_exchange(EMPTY_SLOT, txid, Ordering::AcqRel, Ordering::Acquire).is_ok()", "driver.compare_empty_register(slot_idx, txid)")
    # Ghost snapshots describe the reservation's state without reading another
    # physical clock value or executing bookkeeping in the database.
    reserve_decl = tokenize("let txid = driver.reserve_registration();")
    at = next(i for i in range(len(begin)) if begin[i:i+len(reserve_decl)] == reserve_decl)
    begin[at:at] = ["let ghost before_reservation = driver.state();"]
    begin[at+1+len(reserve_decl):at+1+len(reserve_decl)] = ["proof { reservation_preserves_clock_history(before_reservation, driver.state(), txid); } let ghost reserved = driver.state();"]
    begin = loop(begin, "for (slot_idx, slot) in self.slots.iter().enumerate()", """
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
    """, """
            slot_idx += 1;
        }
    """)
    bodies["begin_transaction"] = begin
    end = bodies["end_transaction"]
    end = replace(end, "let slot = &self.slots[slot_idx];", "let slot = slot_idx;", 1)
    bodies["end_transaction"] = end
    transaction = bodies["create_transaction_snapshot"]
    transaction = replace(transaction, "self.slots.get(registration.slot_idx as usize).ok_or(ProcArrayError::InvalidSlot { slot_idx: registration.slot_idx, })?", "checked_slot(registration.slot_idx)?", 1)
    snapshot_call = tokenize("let snapshot = snapshot_locked(driver);")
    for i in range(len(transaction)):
        if transaction[i:i+len(snapshot_call)] == snapshot_call:
            transaction[i+len(snapshot_call):i+len(snapshot_call)] = ["let ghost snapshot_state = driver.state(); proof { snapshot_scan_bounds(snapshot_state, PROCARRAY_SLOTS as int); snapshot_minimum_covers_slot(snapshot_state, slot as int, PROCARRAY_SLOTS as int); }"]
            break
    final_ok = tokenize("Ok(snapshot)")
    for i in range(len(transaction)):
        if transaction[i:i+len(final_ok)] == final_ok:
            transaction[i:i] = ["proof { snapshot_fields_unchanged(snapshot_state.slots, driver.state().slots, PROCARRAY_SLOTS as int, snapshot_state.sampled_clock); }"]
            break
    bodies["create_transaction_snapshot"] = transaction
    snapshot = bodies["snapshot_locked"]
    snapshot = replace(snapshot, "let mut in_flight = [const { MaybeUninit::uninit() }; PROCARRAY_SLOTS];", "let mut in_flight = Vec::new();", 1)
    snapshot = optional(snapshot, "in_flight[in_flight_len as usize].write(txid);", "in_flight.push(txid);")
    push = tokenize("in_flight.push(txid);")
    for i in range(len(snapshot)):
        if snapshot[i:i+len(push)] == push:
            snapshot[i:i] = ["proof { assert((in_flight_len as usize) < PROCARRAY_SLOTS); }"]
            break
    snapshot = loop(snapshot, "for slot in self.slots.iter()", """
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
    """, """
        }
        proof { snapshot_scan_bounds(loaded, PROCARRAY_SLOTS as int); }
    """)
    bodies["snapshot_locked"] = snapshot
    oldest = bodies["oldest_snapshot_xmin"]
    oldest = loop(oldest, "for slot in &self.slots", """
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
    """, """
        }
    """)
    bodies["oldest_snapshot_xmin"] = oldest
    result = CONTRACTS.read_text()
    tags = {"begin_transaction": "BEGIN", "end_transaction": "END", "snapshot_locked": "SNAPSHOT",
        "create_snapshot": "CREATE", "create_transaction_snapshot": "TRANSACTION_SNAPSHOT", "oldest_snapshot_xmin": "OLDEST"}
    for name, body in bodies.items():
        if any(t in body for t in ("self", "global_txid", "Ordering", "MaybeUninit")):
            raise ValueError("unadapted native lifecycle operation: " + name)
        body.insert(0, "let ghost initial = driver.state();")
        result = result.replace("/* " + tags[name] + "_BODY */", show_tokens(body))
    publication = PUBLICATION.read_text() if publication is None else publication
    method_tokens = adapter.body(publication, "publish_index_stamps")
    prefix = tokenize("if changes.is_empty() { return Ok(()); } let stamp =")
    if method_tokens[:len(prefix)] != prefix:
        raise ValueError("publication reservation prefix changed")
    finish = method_tokens.index(";", len(prefix))
    reservation = method_tokens[len(prefix):finish]
    reservation = replace(reservation, "self.shm.global_txid().fetch_add(1, Ordering::AcqRel)", "driver.reserve_publication()", 1)
    result = result.replace("/* PUBLICATION_RESERVATION */", "let stamp = " + show_tokens(reservation) + ";")
    return "// Generated from actual ProcArray lifecycle operations; see generate.py.\n" + result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit("stale lifecycle adapter: run verification/lifecycle/generate.py")
    else:
        OUTPUT.write_text(generated)


if __name__ == "__main__": main()
