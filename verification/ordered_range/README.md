# Ordered integer publication buckets: experimental boundary

The optional ordered `I64` publication policy changes which existing index
bucket guards and stamps a query depends on. It does not change the skiplist's
key ordering, current postings, transaction snapshots, row visibility, commit
validation, publication order or reclamation protocol. Hashed publication
buckets remain the default policy. The policy, origin and positive bucket width
are immutable shared index metadata; independently attached processes must use
the same persisted mapping.

The shared index header is version 3. Attachments reject version 1 and version
2 headers; there is no in-place conversion or compatible warm attachment to
those layouts. Reusing an older mapping requires an exclusive, quiescent rebuild
through the existing recovery/bootstrap process. This experiment supplies no
online migration or durable-format migration guarantee.

For the ordered policy, integer keys map monotonically to a fixed finite
partition of the signed domain. Underflow and overflow saturate; there is no
wraparound, automatic rebasing or moving window. Non-`I64` keys use a terminal
bucket. `<` and `<=` select a conservative prefix through the bound's bucket;
`>` and `>=` select a conservative suffix, including the terminal bucket.
`Eq` and `In` use the same key mapping as writers. A range bounded by `U64` or
`String` conservatively selects every bucket. Invalid encoded keys still fail
before a query is accepted. The cutoff bucket is deliberately retained for
strict comparisons, avoiding overflow-prone arithmetic on the query bound.

The safety obligation is coverage, not exact bucket selection:

```
matches(predicate, key) => query_buckets(predicate).contains(key_bucket(key))
```

This must hold for absent keys as well as current postings. A writer continues
to guard and stamp both its old and new key buckets, including transitions to
or from an absent key. Any insertion, removal or movement affecting a predicate
therefore intersects its dependency set. A disjoint write may still collide in
the boundary bucket or a saturated tail; the policy does not claim to eliminate
all conservative conflicts.

Historical indexed reads retain their existing complete-or-retry contract.
Narrower buckets cannot make a missing historical posting appear. A key move
that affects the old snapshot's predicate must still change a dependency and
cause retry when its current-posting index cannot provide a complete result.
Empty searches and later competing creation obey the same rule.

## Existing proofs and the remaining gap

The original ordered-range checkpoint left native OCC and WAL control paths
unchanged. The subsequent [prior-prefix capture change](../predicate_capture/README.md)
shortens dependency search while preserving the same selected buckets, stamp
checks and guard lifetime; its reviewed token transition is recorded separately
from the original diagnostic baseline. WAL and diagnostic normalization remain
unchanged. The existing
predicate capture, publication and lookup proofs abstract the mapping from a
key to a bucket and assume query coverage. For example,
`verification/lookup/history.rs::accepted_stamps_force_early_events` explicitly
requires every matching key's bucket to be included. Those conditional proofs
can compose with an ordered mapping satisfying that premise, but they do not
prove the new native arithmetic, persisted policy decoder or attachment path.

Lean's extracted bucket sort/bitmap proofs still establish canonicalization of
the supplied bucket list for the fixed 4096-bucket caller. They do not establish
that the supplied list covers a range predicate. The finite TLA models likewise
do not automatically refine this new native representation.

Focused native coverage and transaction schedules are regression evidence for
this experiment. They must exercise the actual native mapping, mixed key types,
signed extremes, partition boundaries, tail saturation, strict comparisons,
empty results and key movements. Finite tests are not a universal arithmetic
proof. A later small verified kernel should establish monotonicity and coverage
for all permitted origins, positive widths and representable keys, then bind
that kernel to the native caller. No new whole-engine or feature-enabled
transaction proof claim follows from this experiment.

Changing `shm_index.rs` invalidates source-bound receipts that include that
module, even when their generated Verus bodies are unchanged. Fresh receipts
are required; source fingerprints must not be updated to make an old proof run
look current. The P0 contract audit and frozen boundary also require explicit
review of the new policy API and shared layout version.

## Experiment limits

Origin and width use the indexed value's units. Legacy and fleet workloads use
integer seconds; the calibrated workload uses nanoseconds. Record the policy,
origin, width and units in every experiment. A setting suitable for one workload
may collapse another into an overflow bucket. A finite static partition can
also lose selectivity as timestamps advance; saturation is safe but can restore
broad conflict behavior. Do not silently rotate or rebase a live index to avoid
that limit: changing the meaning of captured bucket IDs would require a separate
synchronization and history protocol.

The policy is a candidate to measure against the retained hashed baseline.
Its additional arithmetic and metadata checks may affect equality writes, and
locking a long prefix can remain costly. Passing correctness checks does not
establish a speedup, production suitability, PostgreSQL compatibility or the
project's 10× target.

## Native coverage guardrail

```sh
source target/verification-tools/environment.sh
python3 verification/ordered_range/run.py --output target/verification/ordered-range
```

The pilot invokes this runner as `ordered-range-native`. It selects exactly
`shm_index::tests::ordered_publication_dependency_coverage` in the real core
under the default, verified-sort and verified-bitmap configurations, with retry
diagnostics disabled. A successful command must report exactly that one passing
nonignored test; zero tests, ignored tests and unrelated passing tests fail the
guardrail. The receipt records the named test body's hash, complete before/after
source fingerprints, the pinned production Rust compiler, exact commands and
raw log hashes. The test checks finite exhaustive small domains and selected
partition/extreme-value boundaries; it is not exhaustive over all `i64` keys,
origins and `u64` widths.

The normal gate tests include empty/ignored/wrong-test log negatives and changes
to the named native test body. This verifies evidence selection and freshness;
it does not turn the native coverage assertions into a formal theorem.
