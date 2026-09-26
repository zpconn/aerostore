# Retry attribution and expiry eligibility — 2026-09-26

**The next engine experiment should target due-range contention.** The new
diagnostics reproduced projection retry exhaustion and preserved its terminal
attempts. Restricting expiry-index eligibility did not resolve that failure.
The existing engine and default index policy remain the baseline; this is an
investigation checkpoint, not a qualified 10× result.

The [findings and per-seed comparisons](controlled-findings.md) explain the
result. The [independent evidence review](controlled-review.json.gz) checks all
90 predeclared cells, raw report copies, exact full-history companions, source
bindings and bounded failure evidence. No cell was silently retried or omitted.

## Result

- All **60 trials at 128 and 512 offered foreground messages/s** completed
  useful work. Full histories were checked by the serial oracle; metrics
  trials have exact full companions but do not verify their own histories.
- At 2,048 messages/s, **19 of 24 native trials exhausted projection retries**.
  All ten diagnostic-enabled terminal failures rejected the due predicate at
  commit. Their 320 retained tail samples contain only due-stamp rejections:
  295 at commit and 25 during lookup. These samples overlap the failed workers'
  cumulative counters and must not be added to them.
- Five native overload trials completed, but each skipped stale view updates
  under the permitted temporary-affinity reordering. They fail useful-work
  acceptance. Three have Valid full histories; two are metrics-only histories.
- All six PostgreSQL overload trials exceeded the declared per-worker backlog
  bound. No overload trial supplies a qualified performance comparison.

The native implementation maps a range predicate to **all 4,096 publication
buckets**. Controlled regressions show that an unrelated key move can invalidate
a broad query. The workload counters establish the observed rejection branches;
they do not establish that every individual rejection was unnecessary.
Successful lower-rate traces predominantly hit due/expiry bucket lock budgets,
while the failed projection workers predominantly reject due stamps.

With diagnostics off, filtering improved every paired 512-message/s foreground
p99 in the metrics runs: median change **−2.50%**, range **−12.91% to −0.76%**.
Other comparisons vary in direction, and the filter leaves the due index
unchanged. It remains an explicit experimental option.

Tracing is not assumed free. Filtered-index metrics runs at 512 messages/s had
higher p99 with tracing in all three seeds, with median change **+6.62%**.
Other pairs are noisy. The comparison measures runtime on/off within one
feature-enabled binary, not that binary versus a feature-disabled build.

## Experiment contract

The sequential matrix uses three seeds, four foreground workers plus two
maintenance workers, 16 families, seven-second admissions and complete batched
sweeps. Projection runs every second and housekeeping every two seconds. Both
policies use the same fixture layout and initial rows. Temporary signature
affinity uses a 600 ms sliding TTL with mixed aliases; the affinity behavior is
architect-confirmed, while that TTL and alias distribution are experimental.

Every native policy × diagnostics setting receives an exact full-history run
and a metrics run, with rotated/reversed ordering. PostgreSQL is the unchanged
prepared/buffered SERIALIZABLE control using its existing partial expiry index.
Both acknowledge asynchronous WAL, but equal crash-loss windows and recovery
guarantees are not established. The complete matrix took 556.2 seconds.

Owned compiler, test and proof jobs were stopped before measurement. The formal
pilot started afterward. Ordinary WSL host services remained; CPU isolation was
not enforced. The [execution receipt](controlled-campaign/execution.json.gz)
records this scope and every failed cell.

These short, accelerated, fixed-population trials cannot establish a retention
plateau, realistic five-to-ten-minute cadence, production workload coverage or
sustainable capacity. Each successful cell includes six projection jobs and
three housekeeping jobs, but only one projection job has positive effects.
At fixed offered load, keeping up is not a measurement of maximum throughput.
Physical two-host behavior, production worker counts and durable recovery
equivalence remain unqualified.

## Implementation and validation

The [runbook](../../hyperfeed_retry_diagnostics.md) documents the optional
18-origin native observer, last-32 failed-attempt traces, service counter
freshness and native expiry-policy fixture. The original Extended Crucible
fixture is unchanged. `occ_partitioned.rs` and `wal_writer.rs` become byte-identical to the accepted
baseline when their exact guarded observations are removed.

Focused validation passed 190 Rust test executions, qualification and remote
negative controls, **23 process tests**, and a filtered-index, diagnostics-on
TCP loopback run. The TCP case completed 196 jobs / 209 transactions with a
Valid full history. Deliberately failed maintenance jobs retain committed
batches without claiming completed jobs or verified incomplete histories.
The [implementation review](implementation-review.json) and
[process review](process-review.json) record their exact scope.

All **71/71 [component guardrail checks](guardrails/README.md) passed** on
unchanged source hashes, including Lean, TLA+, native regressions and all three
deterministic Extended Crucible configurations. They retain the existing proof
scope. Default-build proofs do not prove the feature-enabled observer or new
benchmark scheduler. The exact normalization guards, placements and dependency
hash are checked; standalone receipt checks reject a changed or deleted
normalizer. P0 contract coverage is 430 public declarations across 33 modules.
Full P1, whole-engine verification and architecture promotion remain open.

Functional tests used the identical final executable bytes before a proof-only
freshness repair. The [source comparison](development-to-final.json) identifies
the only later changes: the standalone receipt checker, its negative tests and
the boundary lock. Both source bundles and build receipts are retained.

## Reproduction and evidence

- Source snapshot: `a93318d444bd779fcbb6034e11ec51493e59379fbbd9eb7f6481ddbcb147a841`
- Diagnostic-feature binary: `603e528a21620e6cf610d15b923182907ffd7ea2ba317210cefd4696492ce11e`
- Default binary: `a7694868a9c398e1c0a86de32028f3424c52dc1a9db97c358346d4f570c20991`

The [493-file source bundle](source.tar.gz),
[feature build receipt](build-provenance.json.gz),
[default build receipt](default-build-provenance.json.gz),
[campaign driver](run_controlled_campaign.py) and
[reviewer](review_controlled_campaign.py) preserve executable selection and
commands. Scripts were run from the repository root with the pinned toolchain.
Restore helpers to their recorded `target/retry-final-validation/` paths (or
adjust their rooted paths); archived helpers retain that layout assumption.
Use a fresh output directory and an explicitly managed disposable PostgreSQL
fixture when repeating the matrix. The archived database was stopped after
verifying its exact process identity and zero other clients; data files remain
available for reuse.

The [artifact manifest](artifact-manifest.json) maps original paths to compressed
artifacts and records hashes before and after compression. Large JSON/log files
use `.gz`; mmap arenas, WAL, private worker configurations, database files and
compiled binaries are omitted. The [independent archive audit](archive-review.json)
checks all 1,393 artifacts, all four source bundles and every raw trial/failure
record. Its [separate manifest](audit-manifest.json) binds the reviewer and logs.
The proof archive has its own manifest.

Next: prototype narrower due-range dependencies under an explicit comparison
option, beginning with phantom, empty-query, key-movement/removal and old-snapshot
completeness regressions. Repeat this matrix and an identity-routing control
before expanding to realistic cadence, turnover, population and worker counts.
The experiment must preserve atomicity, complete queries and safe reclamation;
increasing retry limits or skipping work would not meet the goal.
