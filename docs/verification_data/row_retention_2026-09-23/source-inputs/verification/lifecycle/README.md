# Native lifecycle, snapshot, and shared-clock contracts

This campaign extracts and verifies the existing `ProcArray` registration,
deregistration, snapshot, and retention algorithms. It also extracts the
publication reservation expression from `OccTable::publish_index_stamps` and
checks the shared-arena wrappers that route both kinds of reservation to the
same header's `next_txid` field. No production implementation is changed.

The contracts describe an operation-local metadata view at lifecycle-lock
acquisition. They **do not prove a frame from arbitrary native API entry**:
the current lock primitive assumes unchanged input metadata across acquisition,
whereas a real blocking mutex permits other transactions to change slots while
the caller waits. This acquisition-frame assumption is separate from mutex
exclusion and remains unproved. A fixed-schedule composition must exclude
unmodeled metadata transitions between released calls and during acquisition.

```sh
python3 verification/lifecycle/generate.py --check
python3 verification/lifecycle/test_generate.py
python3 verification/lifecycle/run.py
```

The generated operations live in [lifecycle.verus.rs](lifecycle.verus.rs).
[contracts.rs](contracts.rs) defines the data model and remaining primitive
assumptions. The pinned Verus runner checks every required root separately,
requires semantic mutants to fail solver obligations, and binds source, adapter,
contracts, generated code, tool artifacts, and individual logs to a receipt at
`target/verification/lifecycle/receipt.json`. Source changes during a run or
stale generated code fail the campaign.

## Native algorithms covered

- `begin_transaction` acquires the lifecycle guard before reserving and
  advertising a new ID. A successful call registers the fresh ID in an empty
  slot and initializes that slot's retained horizon to its ID. Existing slots
  are unchanged. If every slot is occupied, the reserved ID is still consumed
  and returned in `NoFreeSlot`, with the slots unchanged.
- `end_transaction` validates both the slot bound and its current owner. It
  clears exactly the matching registration and retained horizon, or returns the
  corresponding error without changing slots. The reservation history and clock
  projection survive deregistration.
- `snapshot_locked` copies exactly the nonempty slot IDs in slot order. The
  native initialized prefix, length, minimum, maximum, empty case, and saturating
  maximum adjustment are verified. Every active writer appears in the snapshot
  and is below `xmax`.
- `create_snapshot` acquires the lifecycle guard before that actual copy
  algorithm. `create_transaction_snapshot` first validates the registration,
  calls the same algorithm, and publishes its `xmin` as the owner's retained
  horizon in the same metadata critical section.
- `oldest_snapshot_xmin` computes the minimum of the sampled allocator value and
  all active registrations' retained horizons. It reads retained horizons,
  whereas new snapshots compute their active-ID minimum. An older retained
  writer therefore remains protected without being copied into new readers'
  active sets indefinitely.

Registration arguments supplied to end/transaction-snapshot proofs must have
positive IDs, as IDs returned by successful native registration do. The
algorithm proofs are parameterized over arbitrary well-formed slot contents,
reservation histories, and clock values. They retain the native fixed capacity
of 256 slots, including its `u16` index/length bounds.

## Clock history and stale loads

`State.clock` describes the monotone shared allocator at its modeled atomic
linearization points. `State.reservations` records reservations through that
interface. Interfering allocations may skip values: reserving does not assume
that an ID equals the preceding recorded ID plus one. A reservation returns an
ID at least the preceding counter value, advances that counter past the ID,
and appends the ID to the history. These are primitive properties of the actual
shared `fetch_add(1, AcqRel)`, not an assumed transaction-freshness theorem.

`State.sampled_clock` is separate. A snapshot's atomic load, including its native
Relaxed load, may observe a positive value below the current counter. The proof
does **not** require it to see the latest publication reservation. The actual
native adjustment establishes
`xmax = max(sampled_clock, max_active_id + 1)` and
`xmin = min(sampled_clock, active IDs)`. The resulting bounds cover all active
writers even in that case. Retention may conservatively lag too; no resource
progress or eventual freshness theorem is asserted.

`reservation_after_reader` derives a publication ID strictly greater than a
reader's ID when that reader's reservation is already in the shared history.
It requires no supplied numeric freshness inequality. The publishing writer's
start ID is irrelevant. `deregister_then_reserve` composes the **actual native
end algorithm**, its scoped-guard release boundary, and the **actual publication
reservation expression**. A successful result excludes the old registration
and is newer than the specified earlier reader reservation.

This is ordering of reservation events, not ordering of later bucket stamp
stores. A reader may reserve after the writer's publication reservation but
before its bucket stores; the history lemma does not claim the stamp is newer
than that reader. Other composition campaigns must identify the same native
reservation event, rather than introduce a second reservation to satisfy both
interfaces.

Counter exhaustion is explicitly excluded at each reservation (`clock <
u64::MAX`). A successful final reservation can leave the next counter at MAX;
the proofs do not pretend another nonwrapping reservation is then available.
Likewise, room at the beginning of a multi-operation scenario does not by itself
guarantee room after arbitrary interfering allocations. Native wrap handling
remains an open obligation.

## Restricted adaptation and remaining assumptions

The adapter checks selected native signatures, slot count and sentinel,
atomic-order tokens, and the exact existing test-hook exclusion. Native slot
iteration becomes bounded indexed iteration with ghost invariants. Slot atomic
operations become primitive calls with exact field updates. The native
`MaybeUninit` array becomes a vector representing precisely its initialized
prefix; the proof checks the native index bound before the corresponding push.
The actual counter increment and return fields remain source-derived. Unknown
buffer indexing stays visible to Verus instead of silently becoming a correct
push. Shared-arena wrapper routing is checked exactly.

The source guard acquisition becomes a proof event. Native guard destruction
on scope exit is a named primitive boundary: the extracted operations leave
`lifecycle_held` true in their proof postcondition, and a composing harness calls
`release_lifecycle` to represent the real implicit guard drop. This does not add
an extra lock or release to AeroStore. Mutex exclusion, RAII behavior, mmap
mapping identity, stable arena headers, hardware memory ordering, and primitive
atomic linearization remain unproved native obligations. Ordinary allocation
success and the native initialized-prefix representation are also assumptions
at the Rust/runtime boundary, not unsafe-memory refinement theorems.

In particular, `lock_lifecycle` currently changes only `lifecycle_held` in the
model. Its exact frame is an additional assumption about the input projection,
not a property established for `ShmMutex::lock`. Algorithm postconditions such
as preserving other slots compare against this acquired-input view. Establishing
their relationship to a real concurrent API call requires a future acquired-state
witness and an ownership/interference rule for composing released operations.
Clock availability must likewise be established at the actual reservation; room
at API entry cannot rule out exhaustion after interference while waiting.

The model records one operation's metadata projection and a logical reservation
history. The native implementations of these primitives, admissible interference
between released metadata critical sections, and their full correspondence to
concurrent database histories are not proved here. Checked wrapper routing
prevents a source edit from silently substituting another allocator field; it
does not prove physical header/pointer identity by itself.

Twenty required roots include active-writer coverage, retained-horizon bounds,
history ordering, and non-vacuity witnesses with live registrations and a stale
sampled clock. Seventeen source mutants omit guards, skip registration or
horizon publication, clear the wrong data, bypass ownership, omit an active ID,
compute the wrong minimum, omit the maximum adjustment, confuse active IDs with
retention horizons, or return a stale publication reservation. Every mutant must
reach Verus and fail a precondition, postcondition, invariant, or proof assertion.
Ten adapter tests separately exercise fail-closed source routing and syntax.

These results narrow the lifecycle and clock assumptions used by the predicate
proofs. They do not establish whole-engine correctness, unsafe allocator safety,
or complete native transaction-history refinement. Those receipt claims remain
false, including `acquisition_interference_refinement_proved`.
