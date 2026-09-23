# Native lifecycle operations across an interfering lock wait

This campaign removes the earlier **exact acquisition frame** from the
registration, transaction-snapshot, and deregistration composition. API-entry
metadata may change while a caller waits for the lifecycle mutex. The proof
allows an arbitrary finite sequence of other registrations, deregistrations,
retention-horizon changes, shared-clock reservations, and clock observations.
Only a supplied exclusive registration token protects its own slot and identity.

The production implementation is unchanged. The generator checks the real
`ProcArray` signatures and requires mutex acquisition to be the first, unique
statement. It then splits the actual method at that statement: `acquire` models
that same native acquisition; the source-derived suffix executes against the
state acquired after waiting. There is no second native lock and no runtime
proof bookkeeping. The snapshot scan is also source-derived. The original
lifecycle module is embedded byte for byte, but the new suffixes use
`LockedPrimitives`, which has **no exact-framing lock operation**.

## Checked connections

- `wait_step_preserves` and finite-trace induction derive well-formed acquired
  state, monotone clock values, and a reservation-history prefix from individual
  allowed transitions. The acquired state is not assumed equal to API entry.
- `native_begin_is_register_event`, `native_end_is_end_event`, and
  `native_snapshot_is_horizon_event` call the actual acquired suffixes and prove
  their successful results instantiate those interference events. The event
  relation overapproximates possible schedules rather than asserting every
  abstract horizon or clock observation must occur in production.
- `begin_after_wait` runs actual registration against acquired metadata and
  creates a private, tracked `OwnerToken` only after successful registration.
  Its token includes physical-arena identity, slot, and transaction ID. Existing
  tokens are not accepted merely because a numeric slot is in range.
- `snapshot_after_wait` borrows that token. The trace proof preserves the owner's
  complete slot, including its retained horizon, during waiting. The actual
  snapshot then covers **acquired** active IDs and publishes the owner's new
  horizon. The returned ghost witness identifies the acquired state. An unrelated
  writer active at API entry may have completed or been replaced before acquisition.
- `end_after_wait` consumes the token, derives matching ownership after the wait,
  and clears only that owner in the acquired state. Changes made to other slots
  during the wait remain. It does not restore the entry snapshot of the array.
- `history_reader_survives_wait` retains a prior reader reservation through the
  trace even if another actor deregisters that reader; history is not the active
  slot set. A positive witness includes real slot reuse and a separate horizon
  change while protecting an older owner.

These theorems are parameterized over arbitrary finite trace lengths, IDs and
slot contents within the native 256-slot capacity. They do not enumerate a few
fixed schedules. The tracked token cannot be copied or constructed outside this
module; snapshot borrowing and end consumption are checked in the proof wrapper.

## Remaining assumptions

`WaitingPrimitives::acquire` must correspond to one actual mutex acquisition and
an admissible trace of completed metadata operations. It assumes other actors
respect the supplied token's exclusive authority. **That authority has not been
proved for AeroStore's actual copyable registration handles, process lifetime,
raw pointers or native API callers.** Token provenance and consumption are
checked in this wrapper; they do not automatically impose the same discipline
on production Rust. Connecting native ownership and executions to this rely
condition remains open, and the receipt says so.

Entry and trace states are coherent logical metadata projections. They are not
claimed to be simultaneous raw reads while another holder is between its txid
and horizon stores. Relating those intermediate native critical-section steps
to the completed-operation projection is an additional correspondence obligation.

The locked primitive contracts assume stable arena identity, actual exclusion,
correct slot atomics and shared allocator linearization, allocation success,
and the existing initialized-prefix representation. Implicit native RAII guard
release is outside these suffix postconditions: operations finish with the
modeled guard held. A larger harness must connect the real release boundary and
authority transfer. The wait trace normalizes completed intervening operations
to their released states; this is not a proof of Rust `Drop` or process recovery.

`registration_capacity_on_acquire` is an explicit environment condition that
the actual acquired state has room for a nonwrapping reservation. It is **not a
runtime branch**, nor a conclusion from room at API entry. Every intervening
reservation must also be nonwrapping. Snapshot and end wrappers do not require
this capacity condition. Relaxed loads may lag; the native snapshot's active-ID
maximum compensation remains part of the checked scan.

The new result supersedes exact acquired-input framing for these conditional
wrappers. It does not turn the earlier fixed-schedule harness into an arbitrary
native execution proof, establish weak-memory refinement, or finish P1 or full
database verification.

## Reproduce

```sh
python3 verification/lifecycle_interference/generate.py --check
python3 verification/lifecycle_interference/test_generate.py
python3 verification/lifecycle_interference/run.py
```

The runner checks the complete artifact and every named root with pinned Verus,
then requires semantic mutants to fail solver obligations. Controls weaken owner
protection, roll back history, reuse a nonfresh ID, overwrite horizon ownership,
substitute API-entry views for acquired metadata, skip acquisition, mint tokens
with wrong arena/registration identity, end the wrong registration, and corrupt
the native clearing, horizon publication or snapshot copy. Separate adapter tests
reject missing, delayed or duplicated native acquisition and signature/routing
drift. The receipt binds all source adapters, native routing, contracts, generated
artifacts, tool hashes and logs, with source stability checked after the run.
