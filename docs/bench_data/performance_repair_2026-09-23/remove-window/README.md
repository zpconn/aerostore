# Isolated cached removal-window candidate

Parent: `9676fa9ff602565d5a628af41b6d40ba1576a8b9`. Only
`aerostore_core/src/shm_skiplist.rs` differs. Main source was not edited.
`candidate.patch` includes the implementation and deterministic regression.

The original last-posting removal searches for the live node and discards its
predecessor/successor arrays, then searches again after marking the node. This
candidate passes those stack arrays through and attempts every lane's original
AcqRel CAS directly. A missing cached successor or failed CAS enters the original
helping/search loop; retirement still follows complete all-lane detachment.

The validity argument depends on existing native invariants:

1. Both remove and move hold the same global mutation guard continuously across
   search, posting deletion, marking, lane detachment and retirement. All other
   structural writers and collectors acquire this guard.
2. `find` may help remove older marked nodes while constructing its arrays, but
   its returned predecessors remain attached under that guard. The target is
   fully linked and unmarked, so each target lane occurs in that window.
3. Between search and unlink, only the target's posting/count/mark metadata
   changes. No allocation, collection, callback, lock release or key change can
   invalidate a predecessor. Key comparisons retain the existing total-order
   requirement.
4. Every target lane is detached before retirement. The target's own next links,
   key, payload storage and tower remain immutable for pinned readers. Existing
   epoch registration, reclamation horizon, queue ownership and GC priority are
   unchanged.
5. The optimization adds no allocation or lock and keeps all original atomic
   orderings. It does not combine destination insertion with source removal or
   move any action across WAL acceptance.

Validation: 39 native skiplist unit tests and 56 focused native integration tests
passed. The new deterministic test covers all 32 lanes with different per-level
predecessors/successors, multiple postings, a pinned node and its successor links,
blocked reclamation, exact node/tower reuse at height one, and head/tail/middle
removal. It also passes against the original implementation. A separately built
mutant that detaches only level zero fails at the lane-one reachability assertion.
The mandatory seven actual-lock Loom cases and weakened-acquire negative control
also pass with fresh separate artifacts. Those Loom cases model the actual lock,
not this native pointer algorithm.

Separate cfg(test)-only instrumented source copies demonstrate exactly two
`find` calls for ordinary last-posting removal in the parent and one in the
candidate, including a 32-lane node. Two additional cfg(test)-only local-window probes force a missing cached
successor and a failed expected-link CAS after partial upper-lane detachment.
Both enter the original fallback loop and pass the same all-lane/pin/reuse test;
no shared protocol metadata is altered by those probes. Their exact sources,
patches and fresh compiler artifacts are in `fallback-validation`.

The counter never enters the candidate source
or a benchmark binary. Instrumentation patches, compiler artifact identities,
source/binary hashes and exact logs are in `search-count-validation`.

These tests and the invariant argument are not full native refinement, a general
weak-memory/mmap theorem, or unbounded progress proof. The commit event proof is
unchanged and still treats index primitives conditionally. No timings were run;
no performance improvement or adoption is claimed. `validation-summary.json`
binds the source patch and detailed receipts for the later diagnostic capture.
