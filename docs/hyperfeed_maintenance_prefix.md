# Ordered maintenance batches

This experiment asks whether maintenance transactions can avoid reading the
entire eligible population before processing a small batch. It adds an explicit
ordered-prefix query contract to the Contention Crucible, implements it in both
adapters, and retains complete-query runs as controls. The goal is to establish a
useful PostgreSQL comparison before investing in a new native index primitive.
The PostgreSQL prefix treatment completes both 185-second runs at 512 offered
inputs/second; the complete-query control reproduces its housekeeping failure.
The AeroStore service prefix pair also passes. Its foreground p99 is lower at
this operating point, while its whole-housekeeping-job p99 is higher.
The [evidence archive](bench_data/maintenance_prefix_2026-09-28/artifact-manifest.json)
binds the source, executables, histories, reports, and independent reviews.

The [candidate-query investigation](hyperfeed_candidate_queries.md) corrected a
PostgreSQL flight-matching access path, but both query forms still exhausted
128 retries at the first batch of the 100-second housekeeping job. The original
uninstrumented housekeeping worker recorded 282,112 returned rows across 170
predicate queries, versus 4,800 buffered writes. Those are cumulative worker
counters, not measurements of the failing transaction alone. They motivated this
experiment: bounded writes did not imply bounded reads.

## Query contract

`--maintenance-selection complete|prefix` selects the treatment. The default is
`complete`; existing complete queries remain unlimited and ordered by record ID.
The new queries are separate variants in the benchmark's
[storage contract](../aerostore_core/benches/contention_crucible/storage.rs):

| Query | Eligible records | Required result |
| --- | --- | --- |
| `FirstDue { at, limit }` | Active scheduled events with `due <= at` | Up to `limit` (1–16), ordered by `(due, id)` |
| `FirstExpired { before, limit }` | Active position, outbox, or dedup records with `event_time < before` | Up to `limit` (1–64), ordered by `(event_time, id)` |

Each result includes the transaction's own writes and must contain every earlier
eligible record at its serialization position, with ID breaking time ties.
Zero and oversized limits are errors. A short result contains all eligible
records; an empty result with a positive limit proves that none were eligible.
Unordered truncation is insufficient. Changes that could introduce an earlier
record, including creation after an empty search, participate in concurrency
validation.

The selection applies to global projection, cancellation, rescheduling, and
housekeeping messages. A background sweep still consists of independently
committed batches at one immutable cutoff and finishes only after a committed
empty query. It is not one atomic snapshot of the entire sweep. Foreground
messages and their ordering are unchanged. Under a serial execution, complete
and prefix selection choose the same bounded business updates.

## Adapter implementations

PostgreSQL prepares `ORDER BY due, id LIMIT $2` and
`ORDER BY event_time, id LIMIT $2` queries under the existing SERIALIZABLE
transaction contract. Prefix initialization replaces the two time-only indexes
in a freshly created disposable schema with partial composite indexes:

* `due_prefix_idx (due, id) WHERE active AND kind = 3`.
* `expiry_prefix_idx (event_time, id) WHERE active AND kind IN (2, 4, 5)`.

Both fixtures retain nine indexes. The complete control keeps its original
`due_idx` and `event_time_idx`. This is a combined query-and-index treatment;
results cannot isolate the effect of SQL `LIMIT` from the changed index keys.
The composite indexes cover ordering and eligibility access, not every selected
column, and do not promise an index-only scan.

Buffered PostgreSQL writes require more than applying `LIMIT K` to base rows.
If the overlay contains M distinct IDs, the adapter fetches K+M base rows,
removes IDs overridden locally, merges every eligible overlay row, then orders
and selects K. At most M base rows can be displaced by the overlay, so this
refills the result when local writes remove leading records. The SQL predicate
read always occurs, even if the overlay can supply the entire result. Immediate
writes, nested savepoints, rollback, and retry retain the same query contract.

AeroStore initially uses its existing complete indexed OCC read: capture the
whole predicate, materialize candidates, apply eligibility, then sort and select
the prefix. This preserves existing snapshot checks, phantom detection, and
reclamation protection. It changes no core index, publication, or reclamation
algorithm. The core's limited raw-posting lookup does not establish this ordered
prefix contract and is deliberately unused.

For that fallback, `returned_rows` counts only the selected prefix. A smaller
value does **not** demonstrate less index capture or materialization. Native
`candidate_rows` and, when enabled, materialization diagnostics describe that
additional work. PostgreSQL also reports rows after overlay reconciliation;
neither adapter's returned-row count is a physical page-read measurement.

## Correctness and evidence boundaries

The independent serial reference reconstructs eligibility and computes the
time/ID prefix without calling the adapter's sorting helper. Receipts preserve
the adapter's returned order and cardinality, so the oracle can reject missing,
extra, duplicate, or misordered results. Overlapping valid serial orders remain
allowed. The benchmark service protocol is version 2 for the extended query
vocabulary; mismatched peers are rejected. Legacy complete message JSON retains
its existing representation.

Added regression coverage includes ties larger than the maximum batch, signed
time extremes, strict expiration versus inclusive due cutoffs, wrong-kind and
inactive rows, short and empty results, overlay holes and key moves, savepoint
restoration, invalid limits, competing earlier inserts, and empty-result
phantoms. It also exercises independent-oracle rejection, native snapshot and
predicate protection, service transport, and configuration propagation. These
are regression and history-oracle checks; this checkpoint does not add a formal
proof of bounded native index selection.

Reports bind `maintenance_selection_metadata` with requested selection and one
of `complete_query`, `ordered_sql_prefix`, or `complete_read_prefix` as the
effective implementation. Qualification requires exact treatment-specific
correctness companions. PostgreSQL's initial audit records actual index catalog
fields and representative literal query plans, including prefix samples. It is
retained on failure, but is not evidence of the prepared plans actually chosen
during the workload.

## Campaign and status

The implementation campaign uses frozen source fingerprint
`608028c5fe176bcefcbe6415dce68b018a6a7994805e5208516cafe4c834ea8f`.
The benchmark executable is
`cdf5fa277a719b088f42197a7a8123b418ec84b7862fb734da0659184cb90a6c`.
All 433 focused check executions passed: 286 Rust, 141 repository Python, and six
local campaign-helper checks. The independent regression review confirms those
counts, source and executable bindings, and resource checks. All six smoke cells
passed. Both the PostgreSQL and AeroStore service prefix pairs pass independent
review.

The smoke matrix checks full-history and metrics modes for PostgreSQL
complete and prefix selection, plus AeroStore service prefix selection. Longer
runs retain the previous 512-input/second operating point: 185 seconds,
32 configured identities, 16 foreground workers plus two background workers,
five-second projection and housekeeping intervals, batches of four and 32,
40-second retention, and rolling generation reuse. PostgreSQL uses the split
candidate query, prepared buffered writes, and an explicit statistics refresh.
The service retains ordered expiry publication with complete-read prefix
selection. Successful metrics runs need exact full-history companions; failed
controls retain their failed verdicts and partial evidence.

The fresh complete-query PostgreSQL control reproduces the earlier failure:
worker 17, message `8000159744`, exhausts 128 retries (129 attempts) in the first
batch of the 100-second housekeeping job. Its cumulative worker counters record
171 predicate queries returning 282,608 rows, 4,832 attempted buffered writes,
38 commits, and 133 aborts classified as `batch_write:40001`. These include
earlier transactions and failed attempts; they do not isolate the final batch.

The failed-control evidence audit passes while preserving the failed benchmark
verdict. The resource envelope records zero swap and zero OOM events. Partial
progress is retained, but the failed history has not been verified as a complete
run and supplies neither a throughput denominator nor a tested capacity upper
bound.

All four prefix runs complete 185 seconds of admission, 94,648 useful
business messages, 72 retirement controls, and all 36 projection and 36
housekeeping jobs. Each run expires 5,584 records through housekeeping and claims
and reschedules 144 events through projection, producing 144 projection outputs.
Nine housekeeping and six projection jobs perform positive work; the others
still commit the required empty completion probe. Housekeeping uses 214
committed batch transactions and projection uses 72, including terminal probes.

| Prefix run | Foreground p99 | Whole housekeeping job p99 | Total retries | Correctness evidence |
| --- | ---: | ---: | ---: | --- |
| PostgreSQL full | 6.724 ms | 77.103 ms | 79,336 | Complete history passed |
| PostgreSQL metrics | 3.626 ms | 76.081 ms | 27,523 | Exact full-history companion |
| AeroStore service full | 2.502 ms | 261.081 ms | 846 | Complete history passed |
| AeroStore service metrics | 2.319 ms | 255.124 ms | 779 | Exact full-history companion |

Latency includes queueing and retries; foreground latency includes retirement
controls. With 36 jobs per maintenance class, nearest-rank maintenance p99 is the
observed maximum. Each full run passes the complete-history oracle. Each metrics
run has its exact source, executable, treatment, and corpus correctness companion,
rather than an independently verified history. These sequential runs do not
establish that evidence mode caused their latency or retry differences.

PostgreSQL's full run housekeeping and projection workers record 43 and seven
retries; its metrics run records 45 and ten. Foreground work accounts for the
remaining
79,286 and 27,468 retries. Aggregate retry causes are predominantly serialization
failures during buffered batch writes: 77,908 in full mode and 27,291 in metrics
mode. Commit serialization failures account for 1,427 and 230; ordered row-lock
serialization failures account for one and two. Completion and acceptable p99
at this offered rate coexist with substantial retry work, which matters for the
next rate sweep. Projection job p99 is 15.758 ms and 13.606 ms, respectively.

AeroStore's full and metrics runs record 767 and 691 foreground retries,
36 housekeeping retries each, and 43 and 52 projection retries. Commit
serialization failures account for 829 and 766 total retries; the remaining
17 and 13 arise during indexed queries. Projection job p99 is 25.114 ms and
23.420 ms. The metrics run's global expiry queries capture 232,640 candidate IDs
while returning 6,736 prefix rows, including retry work. That distinction
illustrates why smaller returned results do not establish bounded native reads.

Successful reports retain aggregate query, returned-row, and write counts, plus
per-worker job and retry counts; they do not retain the
per-worker query/write snapshots available in failed-run evidence. Those missing
worker counters cannot be used to quantify a before/after read reduction.
Both prefix pairs' resource audits record zero swap and zero OOM events.

At the same 512-input/second offered rate, metrics foreground p99 is about
2.32 ms for AeroStore and 3.63 ms for PostgreSQL. This is a latency comparison
at one operating point, not a throughput ratio or demonstration of the 10× goal.
PostgreSQL now supplies a completed comparison under the explicit prefix contract;
its metrics housekeeping p99 is 76.08 ms versus AeroStore's 255.12 ms. The native
fallback still captures and materializes the full predicate. That makes an
efficient ordered native prefix worth investigating, but these measurements do
not establish the precise cause of the maintenance latency difference. Lower
foreground latency does not mean every workload component is faster.

## Next measurements

Next, test fixed populations of 256, then 1,024
configured identities and sweep offered rates with prefix maintenance and split
PostgreSQL candidate queries. Keep worker count and other settings explicit,
require exact full-history/metrics companions at each point, and compare
completed messages, retry causes, retry-inclusive latency, native candidate
capture and materialization, and retained memory. Keep whole-maintenance-job
completion and latency as guardrails. This should expose population
and contention costs before choosing a native prefix-index optimization.

Then repeat promising points with rolling lifecycle and reclamation checks.
The current rolling generator couples flight lifetime to offered rate:
`cycle_messages × active_identities / inputs_per_second`. With 32 configured
identities, 24 active identities, and a 1,280-input cycle, lifetime is 60 seconds
at 512 inputs/second. Doubling the rate alone shortens it to 30 seconds, below
the 40-second retention plus five-second housekeeping interval. Allocation
deferrals could then be mistaken for database saturation. Preserve lifetime
when changing rate or population, and eventually make lifecycle timing
independent of input rate.

This remains an accelerated contention test at one seed and operating point.
PostgreSQL asynchronous acknowledgment with a final WAL drain is not a matched
crash-durability contract. Larger populations, the reported five-to-ten-minute
HyperFeed maintenance cadence, and at least 100 workers require a further stage;
the current harness limits foreground workers to 32. Sustained capacity brackets,
worker-failure availability, and physical two-host MMHF measurements also remain
necessary for a replacement claim. The project's 10× throughput goal remains
open; a fixed offered-rate result does not establish a capacity ratio.

## Evidence retention and resources

The archive preserves 649 logical files in 754 stored members: 7.36 GB of
original evidence occupies 307.74 MB, with the originals retained at their
recorded paths. Archive creation and an independent stored-byte check both
passed. Failed build/review attempts and the failed PostgreSQL control retain
their original verdicts alongside the successful runs.

After all jobs exited, audited cleanup removed 1,411 disposable compiler-cache
files and reclaimed 410,927,104 bytes (391.89 MiB) inside Linux. All 2,294
retained artifact references and five runtime loader aliases remained valid;
no new missing or changed artifacts were found. Windows C free space changed
by only 65,536 bytes during cleanup, so equivalent VHDX reclamation is not
claimed. Final target growth plus three archive copies budgeted for storage
and Git totals 8.22 GiB against the 20 GiB campaign allowance, with the 30 GiB
reserve maintained on both filesystems.
