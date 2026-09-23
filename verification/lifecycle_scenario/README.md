# Shared lifecycle, capture, and publication scenario

This proof composes the **current extracted native operations**, using one shared
clock driver. It is a checked proof harness, not another database implementation
and not a proof of every transaction history.

`registered_reader_then_writer_publication` performs this controlled schedule:

1. Start with an active older writer; call native `begin_transaction` for a reader.
2. Call native `create_transaction_snapshot` for that reader. The returned outcome
   retains this snapshot and proves that it contains the older writer.
3. Call native `capture_dependencies` for one requested index bucket, through a
   borrowed view of the same index object later used for publication and validation.
   Translate its actual captured fields into the validator's record type.
4. Call native `end_transaction` for the writer.
5. Call native `publish_index_stamps`. Its `Primitives` bridge calls the extracted
   native `reserve_publication_clock` on the **same lifecycle driver** used in steps
   1–4, and projects the same reservation history and clock into predicate state.
6. Call native `index_read_conflict`. If the operations succeed, it returns a
   conflict, and the publication stamp exceeds the reader's actual registered ID.

The caller supplies the requested index/bucket, changes affecting that bucket,
and raw guard permissions. It supplies neither a captured read record nor a
numeric freshness premise, a reservation-history conclusion, or a conflict flag.
The old writer's removal from active slots is derived from the actual deregistration
call and uniqueness of active transaction IDs. Empty creation and key movement are
both admitted; a finite live-state/success-reply witness covers both, an initially
safe read, reader registration, snapshot retention, writer removal, and a later
invalidating publication. This witness establishes consistency of these relations;
it is not a concrete native heap construction or an assertion that fallible
primitives always succeed.

## Boundaries

- **Acquisition interference remains unproved.** Lifecycle state is an operation's
  acquired-input view. Its lock contract frames metadata across acquisition, which
  a real blocking mutex alone does not guarantee. This scenario excludes unmodeled
  slot/history transitions between its calls and while acquiring the lifecycle
  mutex. It does allow interfering clock allocations represented by the existing
  reservation/load contracts; reservations need not be consecutive, and snapshot
  loads may be stale. This is not arbitrary concurrent API-entry refinement.
- `IndexPrimitives` assumes immutable registry/key mapping, correctly guarded
  Acquire stamp observations and Release stores, and the stated frame. The
  `CaptureView` and `Bridge` implementations and their field projections are
  verified; raw pointers, atomics, physical arena identity, and mutex ownership
  are not proved here. Same shared-header clock routing is checked by the lifecycle
  source adapter. Native query guard/capture/raw-lookup order is checked by the
  capture source adapter. Guard acquisition and commit-driver ordering have
  separate proof campaigns; this harness does not prove native guard retention
  throughout a whole transaction.
- Empty query results and row materialization/MVCC are abstract: this proves
  dependency capture and invalidation, not completeness of candidate lookup or
  the serializability of the entire engine. Index bucket collisions are permitted.
- Native RAII release is represented by explicit `release_lifecycle` calls. The
  reader remains registered at the end of this schedule prefix; cleanup, failures,
  panic unwinding, owner death, allocation and BTreeSet implementation are outside
  this scenario's result claim. Errors may return early; no success/liveness
  guarantee is asserted for arbitrary primitive implementations.
- Finite clock room is required at registration. `ScenarioClock::clock_has_capacity`
  selects the nonexhausted publication case after intervening clock observations.
  It is a proof-harness branch, **not a check added to native AeroStore**; wraparound
  handling is still an obligation. It avoids treating initial room as a guarantee
  that later interference cannot exhaust the counter.

## Reproduce

```sh
python3 verification/lifecycle_scenario/generate.py --check
python3 -m unittest discover -s verification/lifecycle_scenario -p test_generate.py
python3 verification/lifecycle_scenario/run.py --output target/verification/lifecycle-scenario
```

`generate.py` embeds the exact freshly regenerated lifecycle, capture, and predicate
modules and rejects stale components. The wrapper is handwritten checked
composition; no alternate native loop is substituted. The pinned verifier runs
with `--no-cheating`. The runner checks the full artifact and every named bridge,
capture, composition and witness root, then requires semantic mutations to fail
solver obligations. Controls cover skipped capture/snapshot/deregistration/
publication/validation, wrong dependency fields, wrong stamp observation/store,
a stale reservation, and disconnected clock/history projections. Parser/type
errors and timeouts do not count as mutation detections. Its receipt records
source hashes before and after, exact commands, verifier pin and log hashes.
