# Native cached skiplist detachment

This is a **conditional, source-bound safety proof** of the two lane-detachment
loops and retirement call in `ShmSkipList::unlink_node`, in
`aerostore_core/src/shm_skiplist.rs`. It does not prove the unsafe heap,
allocator, epoch implementation, all index operations, or whole transaction
histories correct.

The checked native path marks the target, tries the cached predecessor window
from the initial search, and falls back to guarded `find` and retry if that
window is incomplete or a CAS fails. The proof establishes:

- Returning `Ok(())` retires a target only after every lane below its logical
  height is detached. A partial fast scan cannot authorize retirement.
- Every successful expected-target CAS removes that lane and preserves other
  target lanes; failed CAS cannot be mistaken for successful detachment.
- The fallback checks all lanes after refreshing the search window. A failed
  retry cannot authorize retirement.
- Offset errors return without retirement. Throughout the selected operation,
  the abstract target key, posting chain, and successor contents remain intact
  for an epoch-pinned reader. Marking flags and retirement-list metadata are
  deliberately outside that frame.

`generate.py` reads the actual native body, checks the marking/height prefix,
and lowers both descending `for` loops to verified helpers. A native fast-loop
`break` becomes a helper return of that same `detached_all` flag. The outer
fallback condition, its final break condition, the native CAS error branch,
and retirement ordering are preserved. `lane_ref_by_offset`,
`node_next_offset`, and their expected-target CAS are represented by one
fallible `detach_lane` primitive. The actual target successor and memory
orderings must match the recognized native expression. Unsupported source
changes are rejected or remain visible to the verifier; they are not silently
discarded. This restricted adapter is reviewed trusted tooling, not a general
Rust-to-Verus compiler.

The proof is conditional on `GuardedLanes` in `contracts.rs`:

1. The same live mutation guard and epoch pin protect the search window,
   marking, lane operations, fallback search, and retirement. Every structural
   writer and GC operation follows that guard discipline.
2. Node/pred offsets and heights refer to allocated objects of the right type;
   the logical height is in `1..=32`, and no incoming target lane exists above
   it. A predecessor CAS with expected target and the target's successor has
   the stated exact-lane effect. Native pointer validity and atomic refinement
   remain obligations.
3. A successful guarded `find` returns an accurate per-lane target-presence
   window. Its helping may detach marked target lanes, but cannot reattach the
   target or overwrite the target contents protected by the frame.
4. `retire_node` preserves pinned contents and defers reuse according to the
   native epoch policy. This proof checks its detachment precondition; it does
   not establish that the allocator and reclamation code satisfy that policy.

The initial cached window is permitted to be incomplete in the abstraction,
so the fallback is proved as well as the normal fast path. A concrete
two-lane witness proves that the contracts permit attached initial state,
successful cached detachment, failed CAS, and subsequent helping by `find`.
The retry loop is a partial-correctness proof: termination, fairness, and owner
death recovery are not claimed.

Run:

```sh
python3 verification/skiplist_detach/generate.py --check
python3 verification/skiplist_detach/test_generate.py
python3 verification/skiplist_detach/run.py --output target/verification/skiplist-detach
```

The campaign verifies each named root independently and rejects four semantic
native mutations: scanning only lane zero, treating a missing cached successor
as detached, treating failed CAS as detached, and exiting fallback before all
lanes are detached. Negative controls count only solver failures of the named
obligation, never parser or compiler failures. The receipt records exact native,
adapter, contract, generated-source, verifier and log hashes, with stable
before/after input checks.

The complementary native regression
`removal_window_detaches_every_lane_before_pinned_retirement_and_shorter_reuse`
forces a tall node with distinct predecessors, holds a real epoch pin while
detaching, checks all lanes and unchanged pinned contents, then checks
reclamation and reuse by a shorter logical tower. It exercises the actual
unsafe implementation; it does not turn the primitive assumptions above into
a universal proof.
