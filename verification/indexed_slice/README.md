# Indexed read and validation composition

This proof-only campaign joins the checked native operations for a **first
indexed lookup over one predicate bucket**. Equality is a concrete covered
example; any predicate whose matching keys map to the same bucket has the same
contract. This is a conditional read/validation slice, not the whole commit or
an arbitrary concurrent native history refinement.

`slice.rs` embeds freshly generated guard ownership, dependency capture, lookup
and predicate validator modules. The composition:

1. Acquires the actual checked opaque lease, permitting legal interference while
   acquisition waits. Borrowed lease and authority justify stamp and raw posting
   reads; a numeric bucket pair alone cannot grant permission.
2. Calls the actual dependency-capture loop. Successful capture records the
   exact offset, bucket and stamp, with the stamp below the transaction ID.
3. Enumerates current postings while that lease is borrowed. The coherent
   publication-event replay, global reservation chronology and monotone guarded
   stamp history derive historical candidate coverage from accepted stamps.
   Historical query completeness is **not a raw-index primitive postcondition**.
4. Consumes the capture leases before calling the actual candidate union, private
   write selection, MVCC chain walk, native visibility and key filter. Successful
   rows equal the declared retained snapshot/private-write query result.
5. Reacquires an actual lease and translates the actual captured dependency
   fields into the checked predicate validator. If that validator accepts, calls
   the actual row-read validator against a later retained row image while the
   predicate lease remains held. The combined conflict is exactly a changed or
   too-new predicate stamp, or native xmin/xmax row validation failure.

The wrapper is a proof-only specialization and composition, not another
database. It includes an extra diagnostic stamp observation under the same
borrowed validation lease to state its executable result. Native lookup's exact
guard/capture/raw/drop/materialization order is checked by the capture adapter;
the stage-order regression checks the wrapper order separately. The component
adapters restrict and translate their native bodies rather than importing
unchecked Lean theorems. The publication history argument is reproved in Verus.

## Remaining boundary

`SlicePrimitives::guarded_correspondence` explicitly supplies the relationship
between an **acquired** guard, coherent publication history and the retained
MVCC snapshot image. This projection is indexed by the acquired lease; it does
not freeze an API-entry view across waiting. The driver has an explicit
`operation_snapshot` and correspondence is callable only for that snapshot.
It does not promise one history fits arbitrary active sets sharing a transaction
ID. The driver also fixes `operation_index_offset`; only the selected index
header is admitted to the retained row-key projection. Query bucketing uses
the driver's fixed physical key-to-bucket projection
for the selected index header. The query's bucket function must agree with that
projection for every key; arbitrary remapping of disjoint buckets is excluded.
Stamp reads and raw enumeration require the same selected-header and bucket
context; borrowed validator permissions carry those requirements too.
The authority's physical domain must also cover the registered index buckets.
Raw lookup must enumerate current maintained postings, and selected retained
row values must correspond to the
snapshot-visible event replay. These are storage/history mapping obligations,
not yet proofs of the actual allocator, raw posting implementation, atomic load
interleavings or reclamation horizon. The per-operation retained image can
represent a chain containing later invisible versions; it does not assert that
the physical heap is frozen during materialization.

The validation image is separate and may have different xmin/xmax fields. The
composition requires retained addresses from materialization and preexisting
read records still to resolve there. Pointer validity and reclamation ownership
remain explicit. Snapshot and transaction fields enter through the checked
lookup transaction contract; this campaign does not itself compose native
registration/snapshot acquisition or prove all possible native histories map to
the supplied replay. It also leaves table/index routing, cross-bucket queries,
repeated lookup dependencies, write conflict checks, row publication, WAL,
recovery, weak memory and unbounded progress outside this slice.

The read-only `BorrowedIndex` implements unused writer methods with checked
unreachable bodies: their inherited precondition requires `deregistered`, while
the read-only state projection sets it to false. No admitted theorem or external
body is introduced. The empty-history witness checks satisfiability of the new
history/image seam. It is not a concrete implementation of the storage or
scheduler primitives; guard handoff and changed-history witnesses live in the
component campaigns.

## Checking

```sh
python3 verification/indexed_slice/generate.py --check
python3 -m unittest discover -s verification/indexed_slice -p 'test_*.py'
python3 verification/indexed_slice/run.py
```

The pinned verifier runs the entire embedded program, named composition roots,
semantic controls for skipped capture/raw enumeration/validation and corrupted
dependency translation, and an affine type control for reading through a
consumed guard. Receipts hash every input, generated artifact and log and reject
changes during verification. Semantic controls must fail proof obligations;
the affine control must produce its expected Rust ownership diagnostic.
