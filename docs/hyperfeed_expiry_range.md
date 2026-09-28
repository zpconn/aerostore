# Expiry-range publication experiment

This experiment tests whether narrower expiry dependencies reduce housekeeping
retries in AeroStore's database-owned service. All twelve short diagnostic runs
completed with valid histories. Ordered publication reduced observed
housekeeping retries and whole-job latency in each pair, but foreground latency
needed a separate control investigation. The first long seed completed, with
ordered service passing both evidence modes and failures retained for hashed
service and PostgreSQL. **The second long seed was interrupted during the
reported WSL VM crash.** Its first two attempts finished before the interruption;
the third has only partial progress. Hashed publication remains the default; no
architecture or performance promotion has been made.

The [rolling workload investigation](hyperfeed_rolling_findings.md) found
housekeeping retry exhaustion in a quiet 185-second service run. Instrumented
short runs identified expiry predicate-stamp validation as the largest rejection
branch. Filtering the expiry index to housekeeping record kinds did not improve
either observed two-seed pair. Those observations motivate a dependency-tracking
experiment; they do not establish that all conflicts were avoidable or that the
service protocol caused a particular fraction of the failures.

The treatment changes which publication buckets protect a complete expiry query.
The existing hashed policy captures all 4,096 buckets for a range. The optional
ordered policy groups signed timestamps into intervals, allowing an expiry
query to depend on the intervals up to its cutoff. A write whose old and new
keys are both in later intervals can then avoid invalidating that query. A
write that inserts, deletes, or moves a matching key must still conflict when
required by the transaction's snapshot.

The [benchmark fixture](../aerostore_core/benches/contention_crucible/fixture.rs)
uses the existing
[`IndexPublicationPolicy::OrderedI64`](../aerostore_core/src/shm_index.rs) API.
Only the `event_time` index receives the optional expiry policy. The production
engine, publication algorithm, OCC checks, row values and index postings, query result
filtering, maintenance transaction boundaries, service protocol, and retry
budget are unchanged.

| Option | Default | Meaning |
| --- | --- | --- |
| `--expiry-publication` | `hashed` | Select `hashed` or `ordered` publication dependencies for the native expiry index. |
| `--expiry-index-origin` | `1700000000000000000` | Signed timestamp where the ordered interior intervals begin. |
| `--expiry-index-width` | `1000000000` | Positive unsigned interval width; one second for calibrated nanosecond timestamps. |

These options are separate from `--expiry-index all-active|housekeeping`, which
controls row eligibility. They are also separate from `--due-index`, which
controls projection's due index. The comparison holds expiry eligibility at
`all-active` and due publication at `hashed`. PostgreSQL keeps its existing
partial expiry index and prepared, buffered `SERIALIZABLE` adapter. Its report
labels the effective expiry-publication implementation `postgres`, with no
native origin or width, even though requested options remain recorded.

Reports and correctness-companion keys include the requested publication
policy, origin, and width. Native ordered reports must provide the effective
parameters; hashed and PostgreSQL reports use explicit null effective
parameters. The remote setup handshake checks the same configuration. Omitted
legacy metadata normalizes to the hashed defaults, while Python validation rejects incomplete modern
report or setup metadata. An ordered metrics run needs a
successful full-history companion with that exact ordered configuration; a
hashed companion cannot substitute for it.

The fixture persists both eligibility and publication settings and checks them
against each attached index. Its disposable identity marker advances from
version 2 to version 3. Attachment checks a stable integer prefix before reading
the larger identity and rejects older markers. Existing fixture files need to
be rebuilt when quiescent. This marker change is separate from the engine's
shared-index layout and does not implement a production mapping upgrade.

The ordered mapping has a fixed window. Bucket 0 holds signed keys below the
origin; buckets 1–4,093 hold the interior intervals; bucket 4,094 holds later
signed keys. Other value types share bucket 4,095. At the default width, the
interior covers 4,093 seconds, about 68 minutes. It neither wraps nor advances
with wall time. Strict and inclusive predicates both capture the full boundary
bucket, preserving matching absent-key dependencies at the cost of some false
conflicts.

This creates two limits that measurements must expose. Once timestamps collect
in an endpoint bucket, unrelated updates can conflict again. Inside the window,
contemporary writes can share one interval and contend for its writer guard.
Lower predicate-stamp rejection is insufficient if bucket contention, backlog,
retry exhaustion, or foreground latency becomes worse. A successful short run
inside the window cannot establish an indefinitely useful range design.

The staged comparison uses the [rolling lifecycle profile](hyperfeed_rolling.md)
and retains the [resource limits](hyperfeed_capacity_resources.md):

1. Twelve short diagnostic full-history runs cover direct access and the Unix
   service, hashed and ordered publication, and three seeds. They retain the
   previous ten-second stress configuration: 32 foreground workers, 64
   identities, 256 inputs per second, 16 inputs per generation, and one-second
   retention and maintenance timers. The same diagnostic-feature executable
   serves both policies. Policy order is counterbalanced across paths and seeds;
   three pairs per path cannot exactly balance both directions. These runs can
   test rejection behavior but cannot provide useful projection beyond its
   30-second horizon.
2. An additional eight-run control uses the default executable for direct access,
   with diagnostics disabled, two seeds, both publication policies, and full and
   metrics evidence. It retains the ten-second workload and requires an exact
   full-history companion for each metrics run. This tests sensitivity to the
   combined costs of evidence collection; it cannot isolate serialization or
   establish recurring projection coverage.
3. After review, one six-cell long stage compares service hashed, service ordered,
   and PostgreSQL, each in full and metrics mode. It retains the previous
   185-second configuration: 16 foreground workers, 32 identities, 256 inputs
   per second, 640 inputs per generation, 40-second retention, and five-second
   maintenance timers. Metrics reverses the full-run order. A second seed with
   reversed ordering requires another review. Long runs use the separately
   bound default executable with native diagnostics disabled.
4. After the primary timings, one bounded diagnostic shifts the origin back
   4,093 seconds. Contemporary keys then enter the signed overflow bucket. This
   tests behavior after precision is lost without waiting an hour. It is a
   boundary diagnostic, not a paired capacity estimate.

No stage launches the next automatically. Source and build receipts, executable
hashes, commands, configurations, completed jobs, failures, and raw evidence
remain attached to each attempt. The comparison keeps maintenance batch sizes
at four projection events and 32 housekeeping rows and preserves the terminal
empty transaction for each completed sweep. A committed batch is not counted
as an additional input message or a completed maintenance job.

The first diagnostic stage produced the following paired observations. Values
are hashed → ordered; p99 includes retries and scheduled-arrival queueing. Each
run completed 2,416 useful business messages and nine housekeeping jobs, so
housekeeping p99 is the slowest job in each short run.

| Path | Seed | Foreground p99, ms | Housekeeping p99, ms | Housekeeping retries |
| --- | --- | ---: | ---: | ---: |
| Direct | 20260927 | 2.459 → 13.390 | 123.81 → 108.91 | 85 → 20 |
| Direct | 20260928 | 2.754 → 13.631 | 122.65 → 106.99 | 82 → 18 |
| Direct | 20260929 | 234.765 → 13.857 | 122.61 → 113.53 | 83 → 17 |
| Unix service | 20260927 | 3.043 → 3.104 | 690.71 → 249.62 | 518 → 156 |
| Unix service | 20260928 | 2.891 → 3.025 | 591.31 → 208.21 | 498 → 135 |
| Unix service | 20260929 | 2.846 → 3.339 | 682.45 → 447.22 | 480 → 217 |

These are instrumented observations, not default-build capacity measurements.
The large hashed direct outlier remains in the results: that run recorded 2,867
total retries. Other valid interleavings also produced slightly different
housekeeping expiry counts, from 9,662 to 9,664, without an oracle failure.

The repeated ordered direct foreground p99 prompted the extra evidence-mode
controls. In all three seeds, its slowest one percent of foreground receipts
spent most of their time between worker completion and coordinator receipt,
overlapping housekeeping receipt processing. Worker execution p99 was
227–249 microseconds. Synchronous history serialization is one plausible
contributor; the timing overlap alone does not establish that cause. The
separate default full/metrics controls reproduced the sensitivity:

| Seed | Evidence | Foreground p99, hashed → ordered, ms | Housekeeping p99, hashed → ordered, ms | Housekeeping retries, hashed → ordered |
| --- | --- | ---: | ---: | ---: |
| 20260927 | Full | 2.891 → 13.067 | 122.48 → 108.41 | 81 → 18 |
| 20260927 | Metrics | 1.793 → 0.514 | 65.48 → 28.77 | 79 → 20 |
| 20260928 | Full | 3.047 → 13.041 | 121.11 → 104.97 | 80 → 16 |
| 20260928 | Metrics | 0.824 → 0.460 | 62.26 → 28.50 | 79 → 19 |

All eight controls completed valid executions, with exact full-history
companions for the metrics runs. Ordered foreground p99 fell from about 13 ms
in full mode to 0.46–0.51 ms in metrics mode; the foreground regression against
hashed was absent in those metrics runs. This rules out native diagnostic
instrumentation as a necessary cause of the full-mode observation. The control
changes all evidence-mode costs together, so it does not isolate which part of
receipt transport, processing, or serialization produced the delay. It also
does not supply recurring projection coverage in a ten-second run.

The first 185-second seed (`20260927`) produced four completed executions and
two retry-exhaustion failures:

| Path | Evidence | Outcome | Foreground p99, ms | Housekeeping p99, ms | Housekeeping retries |
| --- | --- | --- | ---: | ---: | ---: |
| Service, hashed | Full | Failed: housekeeping job scheduled at 100 s | — | — | — |
| Service, ordered | Full | Valid history; useful recurring work | 2.760 | 385.04 | 33 |
| PostgreSQL | Full | Valid history; useful recurring work | 3.354 | 401.63 | 180 |
| PostgreSQL | Metrics | Failed: projection job scheduled at 35 s | — | — | — |
| Service, ordered | Metrics | Completed; exact full companion | 2.755 | 291.55 | 33 |
| Service, hashed | Metrics | Completed; missing successful full companion | 2.767 | 2,842.66 | 1,064 |

The last row is descriptive only and is excluded from paired long-run
percentage comparisons. Both failures exhausted the existing limit of 129
attempts, after 128 retries, before acknowledging a first committed batch for
the failing job. Failed worker counters include prior work: the hashed service
snapshot has 149 cumulative failed attempts, while the PostgreSQL projection
snapshot has 129. PostgreSQL reported 125 batch-write and four ordered-lock
serialization failures; the native default build does not provide the more
detailed rejection-origin instrumentation. These branches are not evidence of
the particular conflicting writer or an engine correctness error.

Each completed long run processed 47,288 useful business messages, completed
36 housekeeping sweeps and 36 projection sweeps, expired 5,600 records, and
emitted 144 projection outputs. At 36 jobs, maintenance p99 is again the
slowest completed job. Ordered service is the only treatment with both a
successful full-history run and a completed metrics run in this seed. This is
promising evidence for further testing, not a capacity estimate or a qualified
speed ratio against PostgreSQL.

The second seed (`20260928`) stopped at the following checkpoint:

| Path | Evidence | Status |
| --- | --- | --- |
| PostgreSQL | Full | Finished with retry exhaustion: projection scheduled at 35 s |
| Service, ordered | Full | Finished with valid history and useful recurring work |
| Service, hashed | Full | Interrupted at about 70 s of the planned 185 s; no terminal result |
| Service, hashed | Metrics | Never started |
| Service, ordered | Metrics | Never started |
| PostgreSQL | Metrics | Never started |

The completed ordered run processed 47,288 useful business messages and all
36 sweeps of each maintenance class. Foreground p99 was 2.874 ms; housekeeping
p99 was 368.57 ms with 33 retries. These are full-evidence observations. There
is no second-seed metrics pair. The interrupted hashed run's last progress
snapshot acknowledged 17,981 messages, 17,999 transactions, and 14 sweeps of
each maintenance class. Its partial history is neither a successful correctness
witness nor evidence that AeroStore caused the interruption. The shifted-origin
saturation control never started and supplies no measurement.

Across the checkpoint, 28 attempts finished: 25 valid executions and three
workload retry-exhaustion failures. One additional attempt was interrupted.
The original incomplete campaign remains unchanged, alongside a separate
recovery audit. The audit rechecks source and binary identities, build and test
receipts, completed reports and companion decisions, and recorded raw-file
sizes. It does not turn incomplete evidence into a passing run or establish the
cause of the VM interruption. Resume unfinished cells only as fresh attempts
with new host, boot, and resource receipts. Never append results to the original
interrupted campaign or describe runs spanning the reboot as one uninterrupted
batch. This checkpoint makes no new performance comparison across the reboot.

The implementation checkpoint was committed and pushed as `9cb2a022`; the
measured source bytes still match the pre-interruption build receipts. No
benchmark or PostgreSQL server was restarted after the VM returned. The old
test-server PID file was deliberately retained: its pre-reboot PID identity is
insufficient authority for cleanup in the new boot. The cause of the VM
interruption remains unknown.

The review separates cumulative rejection counters from the bounded tail of
failed attempts. Commit-stamp rejection, busy commit buckets, post-snapshot
lookup rejection, and busy lookup buckets are distinct observations. These
identify rejection branches, not the conflicting writer or whether every retry
was unnecessary. The last retained failed attempt is not automatically the
attempt that caused a later worker error. Failed-run snapshots cover only the
reported workers and acknowledged operations.

Whole-job housekeeping latency includes scheduled-arrival queueing, every retry,
and the final empty transaction. Foreground p99 includes retirement controls;
useful business-message counts exclude those probes. Long-run percentage
comparisons require valid execution, useful work, recurring maintenance and
reuse, continuous timing, and an exact full-history companion. A valid but slow
run remains comparable: missing a latency target is an outcome, not a reason
to discard it. Metrics mode still does not verify its own omitted history.
Failed runs and unpaired metrics observations remain visible without being
promoted into eligible comparisons.

The [expiry fixture contracts](../aerostore_core/tests/contention_expiry_policy.rs)
exercise complete results across record kinds and extreme cutoffs, future-only
insertion and movement, matching insertion/deletion/movement, historical
complete-results-or-retry behavior, own writes, savepoint-retained dependencies,
saturation, and attachment mismatch rejection. A process regression checks
ordered expiry through direct, Unix-service, and TCP worker attachment while
due publication stays hashed. Existing
[ordered-range contracts](../aerostore_core/tests/occ_range_dependencies.rs),
source guards, and P0 checks remain applicable.

Independent review confirmed that production and proof implementation bytes
were unchanged from commit `15128011`. Only the fixture, option wiring,
report/gate metadata, tests, and reviewed workload-boundary fingerprints changed.
This milestone uses focused source-bound checks and the earlier component proof
evidence; it does not claim a newly completed 72-check formal pilot or a formal
proof of the new benchmark integration.

The experiment can establish whether this bounded mapping is worth pursuing
for the observed workload. Production population and lifetime calibration,
five-to-ten-minute maintenance cadence, equal resource and crash-loss contracts,
surviving-worker availability, and physical MMHF remain separate requirements.
Neither a lower p99 nor successful fixed-rate completion establishes the
project's 10× sustained-capacity goal.

Before a capacity comparison, investigate the PostgreSQL control's prepared
query plans and serialization failures. A failed control is evidence to
understand, not an automatic AeroStore performance win. Keep the current
prepared and buffered adapter intact while diagnosing it, and compare any
tuning as a separately recorded treatment.

One subsequent central-service experiment could combine multiple read or
write operations into fewer RPCs while preserving the same atomic transaction,
complete-query, and outcome contracts. That change has not been implemented or
measured here. It should retain exact full-history companions and the prepared,
buffered PostgreSQL control, and should be evaluated independently of this
optional, fixed-window expiry mapping.
