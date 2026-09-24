# Native row vacuum retention

This campaign proves one actual acquired-row iteration of
`OccTable::vacuum_reclaim_before` over an arbitrary finite acyclic version chain,
plus the public and normal-collector callers that select its safe horizon.
It embeds the existing lookup module byte for byte and uses its exact `Image`,
`Row`, `Snapshot`, visibility, and `first_visible` definitions. The root
composition can instead embed `generate.render_module()` against one shared
`crate::lookup`; there is no separately copied visibility semantics.

The adapter checks the native capacity scan and its first partition acquisition,
then selects one row's remaining body. The actual head skip, complete while loop,
strict nonzero `xmax < global_xmin` condition, locked-version exclusion,
predecessor Release store, recycler call, result report and cursor advances
remain source-derived. The native head is retained. This proves the acquired
iteration, not outer-loop acquisition interference or all-table scheduling.

The mutable primitive image changes with each exact predecessor-next store.
The proof derives that each removed version is invisible to the protected
snapshot, that its first visible pointer and value remain unchanged, and that
every newly recycled pointer has first been detached from this row's current
head. A version whose lock bit remains set is never recycled. On success the
recycled set is **exactly** the originally reachable nonhead versions satisfying
the native eligibility predicate; the remaining tail contains none. The number
and row/value/live-head provenance of native reports are checked. Returned
errors preserve retention; an unlink followed by a recycler error may leave a
detached version unqueued, so successful-pass completeness is conditional.

For cursor retention, `prefix_ineligible` names the head-to-first-visible prefix.
If every member is ineligible, the actual loop preserves every prefix cell's
lookup data and excludes it from recycling. Next fields strictly before the
visible anchor are preserved. **The anchor's next may change:** native vacuum
can prune its obsolete tail, and native visible-row selection returns before
reading that next. This conditional prefix premise is separate from ordinary
visible-row retention. The selected publication/retention composition derives
it from its actual publication geometry; this module does not assume a general
concurrent history theorem or claim that first-visible preservation alone makes
an already captured invisible traversal cursor safe.

`Memory` is an executable implementation of the declared primitive contracts,
used only for non-vacuity witnesses. The actual extracted loop retains a visible
anchor under a newer invisible head, recycles an obsolete tail, then reclaims
the anchor after the reader horizon advances. A second witness retains a locked
obsolete version. This is a consistency model, not another database or a proof
of the native unsafe allocator.

## Source-bound horizon admission

The public `vacuum_reclaim_once(requested_xmin)` computes the same arena's
retained horizon and passes `min(requested_xmin, retained_xmin)` to the internal
kernel. The native `compute_global_xmin` body is tied to the exact embedded
ProcArray `oldest_snapshot_xmin` proof. For every represented active slot, the
actual dispatched value is no greater than its retained snapshot xmin. The
public caller also preserves any more conservative requested value.

The normal collector already computes that horizon. Its actual
`run_vacuum_pass` body calls the crate-private kernel directly, with one
metadata scan, so the fix adds no second scan or lock to that production path.
The adapter checks both complete dispatch bodies, the kernel's crate-private
visibility, the table's arena accessor and the same-arena ProcArray/clock
arguments. It rejects routing the collector through the public wrapper.

The dispatch primitive records only the horizon supplied to that exact kernel;
it assumes no successful pruning, retained snapshot or history result. The
separately checked acquired-row loop establishes those retention properties
from the bound and its explicit row/ownership premises. The lifecycle proof
uses its existing acquisition-state projection and modeled scoped guard
release. It does not close arbitrary lock-wait interference, registration
ownership across the whole pass or physical shared-arena correspondence.

## Explicit native boundary

- The selected partition is acquired, and the valid image supplies exact head,
  pointer and field observations for that iteration. Fields other than native
  next stores remain represented consistently. Publication is excluded by the
  partition; scheduling a concurrent owning `RowLockGuard` drop, which may clear
  a lock bit, needs an observation/refinement argument beyond this stable
  lock-state slice. Retention concerns the owning guard whose Drop writes the
  version, not a repeated nonowning handle.
- The caller supplies `global_xmin <= snapshot.xmin`, with active snapshot IDs
  at least `snapshot.xmin` and the usual nonzero xmin/txid/xmax bounds. The public
  wrapper now clamps arbitrary requests, and both supported native callers
  establish the numeric bound for their acquired metadata projection. Keeping
  the reader's actual registration owned and retained until row acquisition
  and through the pass remains a concurrent composition obligation.
- No row in the represented public image is created by the protected reader.
  This explicit admissibility condition handles native own-creator visibility,
  which otherwise bypasses deletion checks. Private writes and committing the
  same reader during this pass are outside the selected slice.
- Pointer resolution, partition/atomic ordering, allocator validity, distinct
  table-chain ownership, and native queue correspondence remain primitive
  obligations. A successful recycler adds exactly its detached pointer; errors
  add nothing. Native recycling changes `recycle_next`, which is outside lookup
  `Row`, so the lookup projection can retain that unreachable cell until reuse.
  Reinitializing an address is a separate operation and must preserve the
  protected reachable prefix. This does not prove general Rust memory safety,
  arbitrary stale-cursor safety, cross-process weak memory, or reclamation bounds.
- `no_errors` optionally states that this admitted primitive instance has no
  returned failures. It is true for the executable witnesses; ordinary callers
  may use false and receive the conditional successful-pass contract. No
  production proof bookkeeping or stronger atomic is introduced. The public
  API adds its necessary horizon query; the normal collector retains its
  existing one-query path.

## Reproduce

```sh
python3 verification/row_retention/generate.py --check
python3 verification/row_retention/test_generate.py
python3 verification/row_retention/run.py --output target/verification/row-retention
```

The pinned no-cheating verifier checks the full shared program, named roots,
and semantic controls that change horizon strictness, live/locked exclusion,
unlinking, target identity, unlink/recycle order, traversal progress and report
data, plus missing or corrupted public/collector horizon dispatch. Each negative
must reach an intended solver failure. Adapter checks reject
unreviewed source shapes or erased acquisition/store-order changes. Receipts
record every input, verifier, generated artifact and log hash and reject source
changes during the campaign.
