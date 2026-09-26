# HyperFeed retry and expiry-index experiments

The contention Crucible can record native rejection branches and compare two
expiry-index policies. These are investigation tools for the 10× HyperFeed
goal; enabling them does not qualify performance or change a failed run into
a verified history.

Build the optional observations explicitly:

```sh
source target/verification-tools/environment.sh
cargo build --release --locked -p aerostore_core \
  --bench hyperfeed_contention_crucible --features retry-diagnostics
```

Select the executable reported by Cargo, then pass `--retry-diagnostics on`.
The default is `off`; a binary without the feature rejects `on`. The
qualification and remote runners forward this setting and `--expiry-index
all-active|housekeeping`. Correctness companions must match both settings,
as well as the existing source, binary, workload and corpus keys.

## What the counters mean

Existing `retry_causes` count the operation stage that returned a conflict.
With observations enabled, `operation_diagnostics` additionally contains
`conflict_origin:STAGE:BRANCH:INDEX` counters. Eighteen native rejection
branches distinguish predicate-stamp checks, row locks and version checks,
partition/index latch budgets, historical chain bounds and a WAL writer-epoch
change. An unobserved rejection is explicitly `unattributed`. These are
observed decisions, not proof that a conflict was logically necessary.
In particular, a newer predicate stamp alone cannot distinguish a real phantom
from an unrelated key change or a hashed dependency collision.

Query counters include successful and failed attempt durations and attempted
versus completed adapter row materializations after index lookup. Internal
MVCC candidate reads inside the index lookup are not counted separately.
Existing candidate and returned-row
counters retain their original meanings. Failed query time is no longer
silently omitted from the detailed cost evidence.

Each worker retains its last 32 failed attempts. The trace reports the total
failure count and how many samples were dropped. Attempt indices start at
zero: exhaustion after 128 retries includes attempt 128, the 129th failure.
Primary-attempt and additional worker-abort counter deltas are separate.
The primary attempt includes cleanup performed internally by the model or
adapter; the separate delta covers only the worker's subsequent abort call. Error text is
bounded. Disabled mode keeps failure counts but takes no per-attempt map
snapshots and retains no samples.

When the coordinator receives a worker error frame, `failure-progress.json`
includes the failed worker's cumulative metrics and bounded trace, plus already
completed workers' final snapshots. Setup errors can carry no metrics. A killed
worker or broken output stream may leave no final snapshot.
Running peers may have additional unreceived commits or retries. Repeated
cumulative snapshots must not be summed, and a backend commit count does not
prove that the coordinator received a corresponding history receipt.

Service clients use counters returned by existing replies; diagnostic failure
handling does not issue an extra metrics RPC. Sequence numbers identify how
fresh that cache is. A broken connection retains its last known values with
`complete=false`. This does not resolve an in-doubt commit or authorize replay.

## Expiry-index control

The default `all-active` index retains the previous record eligibility.
`housekeeping` includes active position, outbox and deduplication records,
matching PostgreSQL's existing partial expiry index. Flight and scheduled
event rows are excluded. Complete expiry-query results remain identical;
transactional query filtering and overlays still enforce the same semantics.
The separate due index is unchanged.

This choice lives in a contention-benchmark fixture. The original deterministic
Extended Crucible retains its fixture. Both experimental variants use the same
immutable attachment marker and metadata validation, outside timed execution;
workers cannot attach with a different extractor policy. The marker also means
these fixture bytes differ from older benchmark builds. Compare policies within
the same current binary, not by subtracting results from an older archive.

Filtering can remove conflicts from excluded record types. Eligible records
outside a searched range can still invalidate the current broad predicate
stamp. This experiment therefore cannot, by itself, establish that the index
architecture needs replacing or that filtering solves maintenance contention.

## Verification and measurement scope

Default builds compile the native observation statements out. The verification
adapter recognizes only the exact reviewed feature guards and arguments. A
separate check pins all placements and the full remaining native token stream
to the accepted engine. Negative controls reject changed guards, arguments,
placements and executable statements. Generated component proof bodies remain
unchanged. This preserves the default-build component proof scope; it is not
a new formal proof of the feature-enabled observer, scheduler or service.

Native tests exercise the diagnostic decisions, thread isolation and reset
behavior. Fixture tests cover complete queries, own-write overlays, savepoints,
attachment mismatches, empty predicates and excluded/eligible concurrent writes.
Worker and service tests cover exhaustion, failed cleanup and stale counters.
The qualification gate rejects malformed traces and terminal errors disguised
as a completed worker.

Runtime on/off measurements use one feature-enabled binary. They measure the
incremental tracing cost, not the difference between that binary's disabled
TLS path and a build with the feature compiled out. Policy comparisons must
hold diagnostic mode fixed. Full-history and metrics runs remain separate,
and failed or censored trials remain visible. Short accelerated sweeps provide
contention evidence; they cannot establish sustained memory retention,
production cadence, capacity, or the 10× replacement target.
