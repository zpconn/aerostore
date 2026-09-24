# Verification and performance experiments

This workspace implements component proofs and model campaigns from the [verification plan](../docs/formal_verification_plan.md). It connects proofs to actual production Rust and gives performance experiments a fixed semantic contract. **It does not yet verify the database or complete the plan's P1 concurrent-slice milestone.**

## What is checked

| Layer | Current coverage | Boundary |
| --- | --- | --- |
| Verus | Both actual bucket canonicalization functions: exact sorted unique membership, bounds, and first-invalid-input semantics; strict stamp comparison; uniqueness of the result contract | Safe Rust functions, successful allocation and pinned collection models; not their concurrent callers |
| Native commit Verus | Actual commit driver and callback cleanup: validation/publication/cleanup order, both policy branches, semantic negative controls | Conditional on explicit primitive event contracts; native data, history refinement and weak memory remain open |
| Native predicate Verus | Actual lock-key union, conflict checks, stamp publication, and lookup dependency capture; data contracts include empty queries, repeat reads, dependency provenance and bounded growth | Restricted source adaptation; registry, hashing, collection semantics, guard visibility and ownership remain primitive contracts |
| Predicate composition | Calls the exact source-bound capture and validation operations, preserving every dependency field; publication's stamp relation implies rejection of overlapping late changes | Composition of represented states; their connection to a real concurrent execution and raw candidate/MVCC completeness remain open |
| Native lifecycle Verus | Actual registration, snapshot/retention scans, horizon publication, deregistration and publication-clock reservation; active-writer coverage and history-derived freshness | Operation-local acquired-input projections; blocking-acquisition interference, native atomic/RAII correspondence and clock exhaustion remain open |
| Native guard identity | Actual bucket selection, retry and collection loops preserve arena/index/bucket identity and requested coverage | Lowest-level ownership, exclusion, Drop and subsequent retained lifetime remain primitive obligations |
| Lifecycle/predicate joins | Actual key generation feeds actual guard acquisition; explicit inverse registry/common-arena correspondence; shared reservation history feeds native validation | Guarded-observation and native history correspondence are explicit, not inferred from numeric guard records |
| Shared-clock scenario | Calls native reader registration, snapshot and dependency capture, writer deregistration, publication and validation through one checked clock adapter | Controlled schedule; no unmodeled metadata changes around acquisition, and no full candidate/MVCC or cross-actor guard-transfer proof |
| Lifecycle interference | Actual acquired-state registration/snapshot/end suffixes after a finite legal wait trace; borrowed registration survives other-slot reuse and horizon changes | Native copyable-registration ownership, coherent metadata cuts, mutex/atomic correspondence and no-wrap enforcement remain assumptions |
| Guard ownership | Actual CAS/Release bodies manage opaque affine leases; borrowed reads, interference during retry and release, nonresurrection of consumed lease identities | Initial physical authority, index domain, frontend Drop, weak memory and mmap correspondence remain native boundaries |
| Native lookup | Actual MVCC visibility/walk, latest private writes, filtering, sorted unique results, exact read provenance and row conflict checks; coherent histories derive candidate completeness | Raw current-posting enumeration and retained-image/history correspondence are explicit; payloads are modeled through index-key projections |
| Indexed-read slice | One-bucket acquire/capture/current-candidates/release/materialize/reacquire/predicate-and-row-validation composition | Conditional storage/history/authority projections; not complete writer commit, arbitrary native heap execution or crash refinement |
| Posting helpers Verus | Actual destination preparation, reverse rollback and source removal: exact posting sets, recorded ownership, unchanged state or poison on failure | Caller-established absent/distinct destinations and guarded insert/remove contracts; unsafe pointer and ownership refinement remain open |
| Skiplist detachment Verus | Actual cached and fallback unlink loops check every lane before retirement; pinned contents preserved under the primitive frame | Guarded lane/CAS/search and epoch contracts; unsafe ownership, actual reuse and termination remain open |
| Lean | Both actual extracted bucket functions, including first-invalid errors; corollaries for the production limit; actual scalar comparison; lifecycle and complete-query history theorems; 34 audited roots and 20 semantic controls | Logical allocation models and explicit production capacity; required roots in [roots.json](lean/roots.json) state exact coverage |
| TLA+/TLC | 136 lifecycle/predicate/publication, primary-key insertion, crash-ordering, checkpoint-cut, retention, collector-admission and live-key-reclamation cases, including intended safety/liveness failures and positive witnesses | Small explicit models with stated fairness; no checked Rust refinement or composed transaction-history theorem |
| Loom | Seven bounded cases importing the actual production lock, including protected-value handoff and registered-priority admission; weakened-Acquire negative control | Preemption bound 2 and 10,000 branches; abstract index protocol, no native mmap refinement or unbounded progress theorem |
| Integration | Kernel differential tests, three transaction/index test suites and extended Crucible under the default and both candidate features | Bounded engine regression evidence, not a replacement for implementation proofs |
| Experiment gate | Fresh extraction/proofs, mandatory negative controls, source hashes, fixed proof roots, frozen engine/contracts/tool configuration, independently anchored comparison | Initial bootstrap is unanchored; repository review and protected CI remain external trust requirements |

The [claim ledger](claims.toml) separates actual implementation proofs, abstract mathematics, bounded model results and open obligations. [Assumptions](assumptions.toml) explicitly include the translators, library models, allocator behavior and compiler/platform. Neither Lean nor TLA+ theorems are imported into Verus. The two actual-Rust proof chains are independent.

Lean acceptance also rebuilds local proof artifacts and replays the complete imported environment through the independent kernel checker. A negative control demonstrates why this matters: an unchecked, axiom-free false declaration can pass ordinary elaboration and an axiom audit, but must fail kernel replay. The replay is intentionally part of every accepted experiment and can take several minutes.

The production index has two opt-in candidate features, `verified-buckets-sort` and `verified-buckets-bitmap`. They cannot be enabled together. The default retains the existing standard sort/dedup implementation. The production stamp decisions now call the proved scalar helper, which preserves the original strict inequality.

## Reproduce

Use x86-64 Linux, production Rust **1.93.1**, Python 3.12 or later, Java 21, rustup and the normal repository build prerequisites. Initial setup requires network access and substantial disk space for the pinned Rust, Lean, mathlib and verifier dependencies. Tools are installed below ignored `target/verification-tools`; the Lean dependency build is below ignored `verification/lean/.lake`.

```sh
python3 scripts/setup_verification.py
source target/verification-tools/environment.sh
cargo fetch --locked
python3 scripts/verify_formal.py --profile pilot
```

The report defaults to `target/verification/report.json`. Logs, exact tool invocations, theorem roots, mutations and model traces accompany it. Missing tools, stale extracted/generated code, absent required roots, disallowed axioms, incomplete model searches, timeouts and source changes during a run fail the relevant gate. Reports start incomplete so interruption cannot leave a stale success.

Profiles are `models`, `proofs`, `pilot` and `full`. `pilot` runs all implemented component checks and the integration matrix. Its extended Crucible fixture uses eight families, one cycle, four local workers, all 27 replay phases and all six native concurrency contracts; these small runs check integration, not sustained performance. **`full` deliberately fails** while the complete implementation obligations remain open. It cannot be made successful by merely editing a descriptive claim status.

Individual tool workflows are described in [Verus](verus/README.md), [native commit](concurrent/README.md), [native predicates](predicate/README.md), [dependency capture](predicate_capture/README.md), [predicate composition](predicate_composition/README.md), [lifecycle](lifecycle/README.md), [guard identity](guards/README.md), [lifecycle joins](publication_slice/README.md), [shared-clock scenario](lifecycle_scenario/README.md), [acquisition interference](lifecycle_interference/README.md), [guard ownership](guard_ownership/README.md), [native lookup](lookup/README.md), [indexed slice](indexed_slice/README.md), [posting helpers](postings/README.md), [skiplist detachment](skiplist_detach/README.md), [Lean/bridge](lean/README.md) and [TLA+](tla/README.md). The TLA directory retains model counterexamples for inspection. The pilot also runs the native durability regressions and the performance comparison runner's integrity tests.

The predicate expansion adds general Lean publication-history and own-write
overlay theorems, six deterministic native predicate regression tests, and
mandatory semantic mutants. No production Rust path changes in this expansion;
the accepted `8d9e9f4` implementation remains the performance baseline. New
proof contracts, adapters and evidence validators form an explicit proposed
verification boundary; they cannot pass the previous boundary by silently
refreshing its hashes. Full P1 remains open until the native storage and
concurrent-history correspondence is composed and justified.

The following lifecycle expansion connects a reader's native registration and
snapshot to native publication/validation through a shared clock, with actual
dependency capture. The stamp's freshness is derived from those operations
rather than supplied as a numeric premise. Its two new native tests exercise
empty creation and key movement immediately before and after deregistration;
five isolated native mutations fail their intended assertions. The native
source changes are test-only. Six new Lean roots and 20 new TLC cases distinguish
atomic label reservation from later stores, including valid disjoint store
reordering. The scenario remains conditional on acquired-input framing and
guard/storage contracts; it is not arbitrary concurrent API-entry refinement.
The immutable transition definitions in `Lifecycle.lean` are frozen with the
contracts. No additional runtime locks, stronger atomics, or proof bookkeeping
are introduced, and no new performance improvement is claimed.

The indexed-read expansion adds acquisition-time interference, opaque guard
ownership, and native MVCC materialization proofs. Its composed one-bucket slice
borrows a live lease for dependency capture and raw candidate enumeration,
consumes guards before materialization, then reacquires for native predicate and
row validation. Exact recorded row/pointer/creator provenance links selection to
validation. Historical candidate completeness is derived; current-posting and
retained-image correspondence remain explicit storage assumptions. New native
regressions exercise slot reuse while waiting and posting deletion/key movement/
reuse after capture, with private writes and attempted vacuum. The production
implementation remains unchanged. P1 and whole-engine verification remain open.

The [production-lock campaign](contracts/lock_models.md) is also mandatory in
`pilot`. It requires all seven named cases and the intended synchronization
counterexample from separate fresh builds, and validates source, binary and
log hashes. Missing evidence, zero-test runs, unrelated mutant failures and
stale compiler artifacts fail the composed gate. These checks add no runtime
lock changes or production instrumentation.

## Make an optimization experiment

The editable production boundary is currently `aerostore_verified/src/lib.rs`. An experiment may also update its proof bodies and regenerated extraction/adaptation artifacts. Public theorem types, required roots, semantic contracts, dependencies, test harnesses, runners, CI configuration and the rest of the engine are frozen. Changing those establishes a new reviewed baseline rather than approving an optimization against the old one.

After changing an implementation and its proof annotations:

```sh
python3 verification/verus/generate.py
python3 scripts/check_lean.py --refresh-generated
python3 scripts/run_verified_experiment.py --baseline-ref <reviewed-commit>
```

The baseline commit must contain the reviewed checker and `frozen_boundary.json`. For a local candidate, first invoke the trusted checker independently of candidate scripts:

```sh
git show <reviewed-commit>:scripts/check_formal_coverage.py > /tmp/aerostore-baseline-check.py
python3 /tmp/aerostore-baseline-check.py --root "$PWD" --baseline-ref <reviewed-commit>
```

Only after that succeeds should you run the candidate's experiment command. The runner also materializes the checker from that commit; changing both a contract and local fingerprints cannot satisfy the trusted comparison. CI obtains its boundary checker from the PR base or previous push commit. A trusted workflow invocation and review of gate changes remain necessary: no repository script can defend against replacing the entire verification workflow with a fabricated success.

For the initial implementation, before any independent Git baseline contains this workspace, use:

```sh
python3 scripts/run_verified_experiment.py --bootstrap
```

That produces explicitly **unanchored bootstrap evidence**, not approval of a later optimization. CI can pass its component-pilot job at bootstrap, while its separate **Reviewed baseline** job deliberately fails until the base commit contains the independently reviewed checker and manifest. Neither mode automatically changes the default or claims whole-engine verification. Rebaselining with `scripts/check_formal_coverage.py --write-boundary` is an explicit review operation, never part of normal verification or CI.

The benchmark checks identical outputs, calibrates each algorithm, rotates measurement order and retains nine batch samples per case. Its medians measure only canonicalization, including its allocations. A separate untimed executable measures allocation/reallocation calls, requested bytes, peak live requested storage and cleanup after dropping the result; its instrumentation does not enter the timed executable. These are allocator-request metrics, not OS resident memory, fragmentation or a reclamation-progress bound.

The standard comparison clones the input to share the candidates' slice interface; the existing engine path already owns its input vector. That difference makes end-to-end measurements essential. These batch medians are not transaction percentiles or HyperFeed throughput. Resource budgets and the automatic performance-promotion gate remain incomplete; whole-workload throughput, tail latency and memory-growth gates are required before promoting a candidate.

For exploratory measurements that cannot pass the formal gate yet:

```sh
cargo bench -p aerostore_core --bench verified_bucket_sets
```

Such measurements are diagnostic only. The fixed bucket contract and first implementation comparison are documented in [bucket_set.md](contracts/bucket_set.md) and [the experiment profile](experiments/profiles/bucket_sets.toml).

## Remaining implementation work

The complete concurrent native predicate/publication operation needs an implementation-refinement theorem connecting actual Rust executions to legal histories. General serializability, unsafe arena/guard ownership, acquire/release/relaxed atomics, process-shared mappings, quantitative reclamation bounds, durability/recovery, and query/application composition remain open. Freezing these files prevents this experiment from silently changing them; it does not prove them correct.

The newest slice models metadata changes during lifecycle acquisition and legal
foreign lock transitions during acquisition and release. Opaque leases justify
borrowed guarded observations in the checked model. Candidate coverage follows
from exact posting replay and accepted stamp history, and native materialization
retains read provenance. These are conditional implementation results: native
registration ownership, physical authority, actual atomic observations and the
raw heap's correspondence to the retained image and coherent history still need
proof. The [native storage slice](storage_slice/README.md) now derives the
snapshot/history mapping for a symbolic three-version path from actual prepared
publication, acquired-row vacuum, row initialization, read and row validation.
It proves a retained traversal prefix and exact successful tail pruning; native
tests cover partial multirow publication and a paused cursor during real reuse.
Generalizing that history/prefix invariant to arbitrary native transactions,
and connecting allocator ownership and physical observations, remain the next
requirements before extending the optimization boundary.
Closing the horizon premise exposed a public API bug: an excessive caller
vacuum horizon could prune a live reader's version, causing its first read to
report absence and successfully commit. The public method now clamps requests
to the same arena's retained horizon. The normal collector still performs one
horizon scan and calls the unchanged internal row loop. Actual caller-dispatch
proofs and deterministic native positive/negative checks cover this repair;
it is a correctness change, not a performance promotion.
The finite-clock no-wrap policy still needs native enforcement; no proof harness
branch repairs production exhaustion.

The durability model led to reproducible failures in the actual engine. The current repair puts WAL acceptance before publication and holds checkpoint exclusion through a cut that records active transaction IDs. The [durability boundary](contracts/durability.md) describes error handling, one-stream enforcement, upgrade requirements and open obligations. `CheckpointCut.tla` models this implemented ordering separately from the original globally drained design. Neither the tests nor the conditional control-flow proof establish full crash/history refinement. Likewise the resource models demonstrate specific retention and starvation mechanisms; they do not establish a general memory bound for a sustained HyperFeed workload.

The [engine comparison runner](../scripts/compare_engine_performance.py) captures binaries with source/compiler identities and runs alternating, matched original/extended Crucible and WAL workloads. Correctness failures, missing evidence, material regressions and inconclusive noisy measurements cannot produce a speed approval. This phase changes the previously frozen engine, so it proposes a new boundary for review; it cannot pass an optimization comparison anchored to the unchanged previous boundary.

The first whole-engine comparison exposed a repeatable extended-workload slowdown
from encoding WAL while holding commit guards. The revised path prepares the
immutable record and complete encoded payload before acquiring guards, then
revalidates and accepts that payload before publication. Its conditional proof
and native tests cover the new preparation/cleanup ordering; performance must
still be measured against the preserved original engine.

The original Crucible's power-of-two histogram also reported bucket lower
bounds as upper bounds. The replacement has 64 subdivisions per octave and
reports inclusive integer nanosecond intervals. Tail-latency acceptance uses
conservative interval ratios; ambiguous intervals or excessive run variation
remain inconclusive. Fixture changes are applied identically to both engines,
with parent-source and exact patch-transition checks, before new binary capture.
The additional histogram storage is benchmark instrumentation, not production
engine memory. Earlier measurements retain their original fixture identity.

Extend the optimization boundary only when those implementation obligations close. The existing plan retains their original exit criteria.
