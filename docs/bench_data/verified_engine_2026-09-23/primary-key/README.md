# Primary-key publication race

The full release test suite exposed an existing cross-process correctness failure: one child returned row ID 203 for `RACE001`, while the final map returned 204. Investigation found the same race in both `ShmPrimaryKeyMap::get_or_insert` and `insert_existing` in [execution.rs](../../../../aerostore_core/src/execution.rs).

Both methods searched for an absent key, allocated a candidate, then loaded a **new** bucket head and used that head as the CAS expectation. Another writer could publish the same key after the absence check but before that new load. The paused writer could then successfully insert a second mapping and return a different row ID. Checking for an existing winner only after a failed CAS did not cover this execution.

The fix retains the exact head whose chain was searched. The first CAS uses that head; a failed CAS supplies the next head to search before retrying. Published map entries are immutable and never removed, so a successful CAS against the searched head preserves the absence check. This source argument still depends on the real shared-memory/atomic primitives; the related finite TLA model is not a Rust refinement proof.

Deterministic tests pause a contender immediately after its absence check and let another writer finish publication. Their hooks are `cfg(test)` scheduling instrumentation only: they do not alter map data or appear in production builds. The four cases cover automatic/automatic, explicit/explicit, and both mixed directions. Two further tests use different keys in one bucket to ensure a retry preserves both mappings and reuses its private candidate.

Evidence retained here:

- [Baseline identity](pk-race-baseline.json): before adding test hooks, `execution.rs` was byte-identical to commit `a382ce3`.
- [Baseline failures](pk-race-before.log), also [reproduced in the separate build target](pk-race-before-isolated.log): all four deterministic same-key cases fail under the original logic.
- [Fixed unit results](pk-race-after.log): nine PK-map tests pass, including all six new schedules and three existing tests.
- [Shared-memory integration results](pk-race-fork-after.log): all six tests pass, including the original cross-process regression.
- [Repeated fork regression receipt](pk-race-repeat.json): 100 successful repetitions, with source/binary hashes and confirmation that the named test ran each time. Repetition is supplementary regression evidence, not exhaustive interleaving coverage.

A candidate that loses to an existing key is never published and is returned to the shared allocator. The same-key tests require the next insertion to reuse that storage without fresh arena allocation. The uncontended path adds no locks or allocations and removes redundant hashing/bucket lookup. This structural observation is not a measured throughput claim; whole-workload measurements are recorded separately.

Reserved automatic row IDs are still not returned when a contender loses or allocation later fails. Those holes can consume row-ID capacity; this change does not implement a reusable ID allocator or prove that capacity tracks only distinct published keys. Explicit row-ID ownership and general allocation/reclamation correctness also remain separate obligations.
