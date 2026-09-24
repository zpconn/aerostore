# Native publication, retention and reuse slice

This campaign closes selected storage premises of the earlier indexed-read
proof. It derives a snapshot/history mapping from actual publication, vacuum,
initialization, read and row-validation bodies. All adapters share the **same**
`lookup::Image`, row, snapshot and transaction types. There is no unchecked
Lean/TLA theorem import and no new production bookkeeping.

## What is proved

The symbolic layout has three distinct nonzero offsets: an obsolete tail, a
visible anchor and a prepared replacement. IDs, values and offsets are
parameters. The writer is either active in the reader's captured snapshot or
starts after it. Both cases have explicit non-vacuity witnesses.

1. The native ProcArray minimum-horizon algorithm produces a horizon at or
   below the retained reader's snapshot xmin, conditional on the existing
   lifecycle acquired-state/ownership boundary.
   The embedded retention campaign also checks the actual public vacuum wrapper
   and collector dispatch: the public request is clamped to that same-arena
   bound, while the normal collector computes the horizon once.
2. Actual prepared single-write publication derives each deletion/link/head
   state. Every prefix preserves the old reader's selected anchor and its
   history equation; successful publication also derives the completed event
   seen by a sufficiently new reader. Row and allocation authority is explicit.
3. The published fields establish that every node through the visible anchor
   is ineligible for reclamation. This prefix fact is proved for this layout;
   it is not inferred from visibility alone on arbitrary version chains.
4. The actual acquired-row vacuum loop preserves selection on every returned
   outcome. On success it prunes precisely the eligible tail, queues that
   offset once and reports one reclamation. It preserves the traversed prefix
   and its links **before** the anchor. The anchor's own next link may change:
   the native reader returns before loading that link.
5. The actual row constructor and initializer replace the unlinked tail's
   fields, including clearing deletion, lock-owner and recycler metadata.
   Exclusive allocation authority remains a caller obligation. This operation
   preserves the reader's history equation and the result from either possible
   retained cursor (replacement or anchor).
6. The actual native read then returns the old value and records the anchor's
   offset and creator. Native row validation detects the invisible writer's
   deletion and rejects the transaction. Once the protected reader is gone
   and the horizon advances, a separate branch starting from the pruned
   state (without intervening tail reuse) proves that a successful native
   vacuum pass reclaims the anchor. The native test also exercises release
   after actual tail reuse; retention need not pin the anchor forever.

These are composable theorems at explicit operation cutpoints, plus a prefix
frame between physical states. They are **not** a complete concurrent native
execution proof. In particular, the exact unsafe load-to-projection mapping
for an already running traversal is still a boundary. The deterministic
[native schedules](../retention_native/README.md) exercise that interleaving
with actual pointers, actual publication and actual allocation reuse.

## Remaining boundary

The initial coherent three-node layout is supplied. Arbitrary transaction
histories, partial multirow publication, the complete indexed posting/history
join, and full P1 remain open. The vacuum component proves general finite-chain
properties, but the history and traversal-prefix derivation here is a selected
layout, not induction over every engine history.

The primitive boundary includes pointer provenance, mmap validity, exclusive
allocator/free-list ownership, row-partition guard authority, registered-reader
ownership, native atomic observations and weak memory. The lifecycle call uses
its existing acquired-state projection and does not strengthen API-entry
framing across a blocking wait. Reclamation treats lock bits as a stable
operation projection; concurrent owning `RowLockGuard::drop` needs a separate
observation refinement. Errors from physical resolution/recycling remain
possible; pruning progress is conditional on successful returned operations.

This milestone gives a more precise correctness contract for future storage
experiments. It does **not** automatically approve changing allocator, atomic,
vacuum or publication algorithms. The existing performance sandbox boundary
and promotion gate remain in force, and whole-engine verification stays false.

## Bug exposed by closing the caller boundary

The original public `vacuum_reclaim_once(u64::MAX)` could detach the anchor of
a live reader. A deterministic safe-Rust client then obtained `None` on its
first read and successfully committed. The acquired-row proof alone did not
exclude this call: it required a valid supplied horizon.

The public method now clamps the requested horizon using the table's own arena
retention metadata. The normal collector calls the unchanged internal scanning
kernel with its existing computed horizon, so it does not perform a second
scan or acquire an extra lock. The public clamp is a correctness change;
production equivalence is explicitly **not** claimed for that API. Source-bound
dispatch proofs and a native omitted-clamp control check the repaired boundary.

## Reproduce

```sh
python3 verification/storage_slice/generate.py --check
python3 verification/storage_slice/test_generate.py
python3 verification/row_initialization/test_generate.py
python3 verification/storage_slice/run.py --output target/verification/storage-slice
```

The receipt authenticates the complete composed artifact, explicit root checks,
semantic controls, transitive adapter inputs and pinned verifier. Constructor
and initializer mutations are applied to native Rust source, regenerated into
the embedded component and verified as a full program. All negative controls
must reach failed proof obligations; compiler errors do not count.
