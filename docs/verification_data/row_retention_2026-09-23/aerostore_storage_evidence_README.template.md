# Native row publication, retention and reuse checkpoint

The complete **$GATE_CHECKS-check component pilot passed** against stable
fingerprints for $INPUT_FILES inputs. The accepted parent is
`$PARENT_COMMIT`. The [summary](summary.json) and
[complete report](formal/report.json) keep `full_P1_complete = false`,
`whole_engine_verified = false` and `promotion_eligible = false`.

This checkpoint connects selected native storage operations to an explicit
snapshot/history model and fixes a reproduced public vacuum API correctness
bug. The public method now bounds a requested horizon by the same arena's actual
retained horizon. The ordinary collector retains its existing single horizon
calculation and calls the unchanged internal kernel. No throughput or p99
improvement is claimed.

## Checked components

| Campaign | Individually checked roots | Rejected semantic mutants | Scope |
| --- | ---: | ---: | --- |
$CAMPAIGN_ROWS

These are $NEW_ROOTS named roots and $NEW_MUTANTS semantic controls across the
three new campaigns. Supporting lemmas, witnesses and composition functions
count as roots; these totals are not a measure of database-wide coverage.
The storage campaign includes $INITIALIZER_MUTANTS native constructor/initializer
controls and checks all seven constructor fields, including recycler metadata.
Every semantic control must reach a failed solver obligation; compilation
failures and timeouts do not count.

Prepared publication is source-bound for one write. The proof retains the
actual deletion CAS, next store, head CAS, fallible resolutions and early
returns. It derives exact successful and partial-error images, and preserves
an older reader's selection at each publication prefix.

The acquired-row vacuum proof covers an arbitrary finite acyclic version chain.
It derives invisibility of eligible versions, detachment before recycling,
locked-version exclusion, exact successful reclamation/reporting, and
preservation of an explicitly ineligible traversal prefix. The visible anchor's
next link may change: native selection returns before loading that link.

The storage composition uses one shared lookup image and a parameterized
three-node layout: an obsolete tail, a retained visible anchor and a prepared
replacement. Actual ProcArray horizon calculation supplies a bound from an
admitted pinned-reader slot. Actual publication derives the history equation
and prefix ineligibility; actual vacuum detaches the obsolete tail; actual
initialization reuses that offset while preserving the prefix. Native read
returns and records the historical anchor, and row validation rejects the
invisible writer's deletion. Both older-active and later-starting writer cases
have witnesses. A separate branch proves anchor reclamation on a successful
vacuum pass after the horizon advances, starting before any intervening tail
reuse.

These are composed operation theorems at explicit cutpoints. They do not prove
that every native concurrent execution maps to the supplied initial geometry
or that an arbitrary running raw traversal is memory safe.

Existing Lean evidence also passed $LEAN_ROOTS roots and $LEAN_MUTANTS semantic
controls, with kernel replay and forged-theorem rejection. TLC passed all
$TLA_CASES existing cases: $TLA_COMPLETE completed finite searches,
$TLA_SAFETY intended safety counterexamples, $TLA_LIVENESS intended liveness
counterexamples and $TLA_WITNESS reachable witnesses. These model campaigns
were not expanded to claim the new native storage refinement. No theorem is
imported unchecked between Lean, TLC and Verus.

## Reproduced bug, native schedules and reviewed runtime change

The [pre-fix reproduction](public-vacuum-bug/before-receipt.json) uses only safe
public APIs, one thread and no storage reuse after vacuum. After a reader starts
and a writer commits a replacement, `vacuum_reclaim_once(u64::MAX)` incorrectly
reclaimed the reader's visible version. The reader's first read returned `None`
and its commit returned `Ok(0)`. A normal collector pass at the actual retained
horizon reclaimed nothing. The source, Cargo lock, tool evidence and raw output
are retained under [public-vacuum-bug/](public-vacuum-bug/).

The fix keeps the public signature, clamps `requested_xmin` to the retained
horizon, and moves the old loop unchanged into a crate-private kernel. The
normal collector calls that kernel after its existing horizon calculation. The
public regression checks historical value retention, rejection of the later
writer, conservative caller horizons and reclamation after the reader ends.
The [isolated fix campaign](public-vacuum-bug/fix-receipt.json) passes, fails at
the intended horizon assertion when the clamp is omitted, and passes again when
restored. The canonical fixed regression and missing-clamp control also run in
the final pilot below. Additional [current-repository integration evidence](public-vacuum-bug/current-integration/receipt.json)
checks the public regression and a 2,000-cycle recycling schedule. Four
[Tcl compatibility tests](public-vacuum-bug/tcl-compatibility/receipt.json) also
pass, exercising existing public-vacuum index-cleanup callers.

The mandatory [native campaign](formal/retention-native/receipt.json) passed
$NATIVE_CURRENT selected tests and $NATIVE_MUTANTS intended assertion failures.
Two new tests pause both ordinary and prepared multirow publication after base
marking or a first head publication, and pause a real traversal after loading
its next pointer. The cursor test runs actual vacuum and allocation reuse while
the reader remains live. The publication test checks retention while the reader
is live, then reclamation and reuse after it ends. Existing tests cover an
owning row guard outliving its
transaction and newer readers not inheriting an obsolete retention horizon.
The cursor schedule covers both older-active and newer creators. These are
deterministic interleaving tests, not a weak-memory or exhaustive history proof.

Negative controls violate horizon retention, unlink-before-reuse ordering,
constructor metadata reset, guard pinning, horizon progress and active-writer
visibility, and omit the public horizon clamp. Each must fail its specified assertion; premature-retention controls
stop before reinitializing incorrectly reclaimed cells. The raw logs and native
receipt remain under [formal/retention-native/](formal/retention-native/).
The [current fixture](native/current-fixture.patch), per-mutant patches,
[source manifest](native/current-source-manifest.json) and
[mutation manifest](native/mutation-manifest.json) reconstruct each tested tree
from the accepted parent's recorded `git archive` command. Build products and
the reproducible parent crate tar are omitted.

The pilot also passed $CORE_TESTS core regression tests, the default/sort/bitmap
transaction/index/query matrix, Extended Crucible smoke checks and the existing
actual-lock Loom campaign. These are correctness checks, not sustained latency
or throughput measurements.

The [runtime-delta audit](runtime-delta.json) and retained
[checker](runtime_delta.py) inspect all $PRODUCTION_FILES Rust/Cargo inputs
against the accepted parent. Only the exact public wrapper, crate-private kernel
rename and collector call rename are allowed production changes. The exact
regression and approved test-only hooks/modules are accounted for separately;
all remaining tokens match the parent. **Production behavior changed to fix the
public API bug.** This is lexical change detection, not a compiler equivalence
theorem. The [pre-fix comparison](pre-fix/production-equivalence.json) is retained
only as historical evidence; it does not describe the fixed implementation.

## Explicit remaining boundary

The initial coherent layout, retained pointer projections, partition authority,
reader ownership and exclusive allocator ownership remain admitted interfaces.
The lifecycle horizon theorem uses an acquired-state projection; it does not
prove arbitrary API-entry wait interference or registration ownership across
the complete vacuum pass.
The proofs do not establish raw mmap validity, native free-list ownership,
load-to-image correspondence for an already running traversal, weak-memory
behavior, arbitrary transaction histories, complete multirow publication or
the full indexed-posting/storage history join. Owning row-guard Drop may clear
a lock bit independently of the partition latch; that concurrent observation
is outside the stable lock-state projection. Repeated nonowning guard handles
are distinct from the owning guard that touches the row on Drop.

The public wrapper's bound is source-bound to native horizon calculation and
its clamp; the internal kernel still requires a valid same-arena horizon from
its crate-private callers. Physical resolution/recycling errors can occur, so
reclamation progress is conditional on successful operations. Full commit,
WAL/recovery and whole-engine verification remain open. This checkpoint does
not authorize arbitrary allocator, atomic, vacuum or publication optimizations.

## Evidence and reproduction

The accepted parent's exact [checker](accepted-boundary-checker.py) and
[frozen boundary](accepted-frozen-boundary.json) correctly
[reject this changed proof boundary](accepted-boundary-rejection.json).
The [proposed local boundary](proposed_boundary.json) records the reviewed new
contracts; refreshing it does not certify an optimization against the old
boundary.

```sh
source target/verification-tools/environment.sh
python3 scripts/verify_formal.py --profile pilot
```

[Source snapshots](source-inputs/) retain all $INPUT_FILES fingerprinted pilot
inputs ($SOURCE_BYTES bytes), with [their hashes](source-input-sha256.json).
[artifact_sha256.json](artifact_sha256.json) hashes every retained artifact
except itself. Original absolute command/log paths are preserved in receipts;
[archive-path-map.json](archive-path-map.json) maps repository paths to retained
copies. Solver/model output, generated mutant sources, test logs, source/tool
fingerprints, current contracts and adapters accompany the report. Substitute
local source/build paths when reproducing native fixtures. Missing roots,
stale sources, missing controls and incomplete searches fail the gate.
