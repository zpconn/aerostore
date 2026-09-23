# Existing-engine performance comparison

This work compares changes to the existing Aerostore engine against commit
`a382ce3d06d4fb690ba09632d7b3a38b8902c65a`. The first campaign exposed repeatable
extended-workload regressions. An optimized follow-up fixed those regressions but
failed the sustained-throughput gate. The baseline
was captured before production edits, from an immutable `git archive`, with the
default bucket implementation and Rust 1.93.1.

[baseline-build.json](baseline-build.json) records the exact build command,
compiler identity, source hashes, archive hash, and copied executable hashes.
[candidate-build.json](candidate-build.json) records the matching candidate capture.
[host.json](host.json) records the host and available CPU affinity. Executables
and archived source are local artifacts under `target/verification-next/baseline`;
rebuild them from the recorded commit to reproduce this capture.

The [initial campaign](campaign/campaign.json) retains every measurement and its
original policy. All eight original-Crucible runs and twelve extended-Crucible runs
passed their correctness checks. The performance comparison did not pass:

| Initial comparison | Candidate / baseline throughput | Candidate / baseline p99 | Result |
| --- | ---: | ---: | --- |
| 120-second churn, three pairs | 0.9774 median | Coarse histogram; unresolved | Throughput within margin; latency inconclusive |
| 240-second churn, one pair | 0.9705 | Coarse histogram; unresolved | Stable allocation/reclamation; no timing acceptance |
| Extended 64 families, 8 workers, three pairs | 0.8614 median | 1.1719 median | Throughput, latency, and retry regression |
| Extended 256 families, 16 workers, three pairs | 0.9399 median | 1.0574 median | Throughput regression |
| Synchronous/asynchronous WAL | Incomplete | Not measured | First baseline test passed; parser rejected libtest's line prefix |

Extended throughput variation was low: 0.46% / 1.09% for baseline / candidate in
the 64-family profile, and 1.06% / 0.39% in the 256-family profile. These results
triggered further engine work; they are not accepted performance evidence for a
later implementation. The initial campaign remains marked incomplete and failed.
Its WAL parsing defect has a regression test in the updated comparison runner.

The historical original-Crucible histogram used floor-log2 bins and reported each
bin's lower endpoint under an incorrectly named upper-bound helper. A reported
131.072 microseconds represents an actual nearest-rank percentile in
[131.072, 262.144) microseconds. Equal bins can hide almost a twofold difference;
crossing a boundary by one nanosecond can display a twofold jump. Those results
cannot establish a 10% p99 bound. Extended Crucible sorts actual message-duration
samples and does not have this quantization defect. The replacement uses
64 subdivisions per octave and reports exact inclusive integer bounds; its
comparator requires the entire candidate/baseline ratio interval to meet the
unchanged threshold. Intersecting the threshold is inconclusive.

The [original runner](campaign/original-runner.py),
[post-run identity checks](campaign/source-integrity-after-stop.json),
[candidate source archive metadata](candidate-source-archive.json), and
[formal source archive metadata](formal-initial-source-archive.json) preserve the
initial implementation and acceptance context. The matching
[formal/regression evidence](verification/README.md) describes the conditional
proof scope and remaining exclusions; it does not establish whole-engine verification.

The [optimized campaign](campaign-optimized/campaign.json) compares the optimized
engine and original baseline with the **identical histogram repair**. Each capture
verified the exact patch transition, unchanged production sources across that
transition, matching fixtures, and Rust 1.93.1. The
[baseline](baseline-fine-build.json), [candidate](candidate-fine-build.json),
[patch](latency_histogram.patch), and
[candidate archive](candidate-fine-source-archive.json) preserve that provenance.
The [optimized formal evidence](verification-optimized/README.md) and
[release validation](validation-optimized.json) cover the matching implementation.

| Optimized comparison | Candidate / baseline throughput | Latency outcome | Result |
| --- | ---: | --- | --- |
| Extended 64 families, 8 workers | 0.9904 median | 1.0204 median p99 ratio | Pass |
| Extended 256 families, 16 workers | 0.9889 median | 1.0011 median p99 ratio | Pass |
| Synchronous WAL | 0.9860 median | Not measured | Inconclusive: baseline throughput range 11.37%, above 10% limit |
| Asynchronous WAL | 1.0036 median | Not measured | Pass |
| 120-second churn, three pairs | **0.9296 median** | Inconclusive: large variation within both executables | **Throughput regression** |
| 240-second churn | Stopped during first baseline run | Not evaluated | Incomplete; no accepted result |

All 24 completed runs passed their correctness/resource checks. The sustained
throughput ratios were 0.9296, 0.9274, and 0.9867; all are retained. Throughput
ranges were 4.89% for baseline and 7.97% for candidate, within the fixed noise
limit. Its median therefore fails the unchanged 0.95 throughput bound. The
workload's aggregate `inconclusive_noise` label describes its noisy latency;
the separate throughput metric remains `regression`. Similarly,
`timing_acceptance: true` enables the policy and does not mean its result passed.

The campaign was deliberately stopped after this rejection, before completing
the longer pair. The [post-stop identity receipt](campaign-optimized/source-integrity-after-stop.json)
confirms matching source, executables, manifests, and runner. It remains incomplete
and failed. These results do not authorize a performance promotion or speed claim.

Final correctness review subsequently reproduced three additional
[poison-admission failures](durability/poison-admission.md), all repaired in the
current source. Commit now rechecks health under its guards; managed synchronous
append checks health and publishes detected indeterminate failure under its
existing file lock. This source is newer than every timed candidate below and
requires a fresh performance capture. The previous throughput rejection remains
unresolved, and no runtime lock experiment was adopted.

The [final verification campaign](verification-final/README.md) passes all 19
checks, including the mandatory actual-lock campaign, with all 231 source hashes
stable. The separate [release receipt](release-final.json) records 411 passing
core/macros/helper tests, two ignored tests, and 23 passing Tcl tests. The
[source archive metadata](final-source-archive.json) identifies the exact final
source. These results remain conditional/component evidence, with full P1,
whole-engine verification and performance promotion explicitly incomplete.

The cause of original-Crucible latency variation remains unresolved. The retained
allocation, retry, and correctness checks do not show an arena-growth collapse.
These runs did not record process scheduling, WAL queue occupancy, or flush
latency, so they cannot attribute the variation to those mechanisms or to WSL.
Any further diagnosis must use separate, identified runs; the original measurements
and margins remain intact.

A [predeclared scratch-only alignment diagnostic](alignment-diagnostic/plan.json)
tested whether immutable WAL identity sharing a cache line with recycler counters
explained the throughput loss. Compiler receipts confirmed the isolated layout
change preserved header size and all preceding field offsets. Three alternating
30-second pairs passed correctness/resources but gained only 0.54% median
throughput, below the declared 3% criterion. The
[result](alignment-diagnostic/result.json) did not support adopting that change;
the production source was left unchanged.

Separate [gprofng diagnostic profiles](profile-diagnostic/result.json) sampled one
30-second run of each preserved executable, following all 17 child processes.
Both passed correctness checks. They concentrate the additional sampled CPU in
shared mutation-lock acquisition: inclusive CPU rose from 11.75 to 15.32
microseconds per operation, with most of the difference attributed to index
source removal. The [analysis and commands](profile-diagnostic/analysis.json)
retain the actual counts and normalized values. These single instrumented runs
identify a place to investigate; their timing is excluded from the performance
gate and does not establish the cause of the sustained regression.

A second [scratch diagnostic](ttas-diagnostic/plan.json) reduced futile contended
mutex compare-and-swap attempts, while preserving the initial attempt, periodic
unconditional retries, acquire/release ordering, priority checks, and backoff.
The [actual-primitive Loom checks](ttas-diagnostic/validation/receipt.json) passed,
and a separately rebuilt weakened-acquire mutant was rejected for a causality
violation. Preliminary exploration exhaustion and invalid shared-target build
reuse were retained and excluded from that accepted test receipt.

This change gained 16.11% median throughput against the rejected candidate, but
p99 was **2.63–3.89 times higher in all three pairs**. The
[resource analysis](ttas-diagnostic/resource-analysis.json) found no allocation
failures, hot memory pressure, GC errors, or sustained-throughput collapse.
The [result](ttas-diagnostic/result.json) supports only its predeclared throughput
hypothesis; `performance_acceptance` remains false. It stays a scratch experiment,
outside the production source and conditional driver proof. There was no full
acceptance campaign for this change.

A final [narrower diagnostic](short-spin-diagnostic/plan.json) restricted the hint
to the initial short-spin phase, restoring the original retry behavior for longer
waits. Before timing it required both throughput improvement and the unchanged
conservative latency/noise bounds. Its
[result](short-spin-diagnostic/result.json) also failed: throughput ratios were
0.9774, 1.0441, and 0.9784, and the median p99 ratio was bounded between 1.6563
and 1.6850. Throughput and tail variation exceeded the declared noise limits.
All six correctness/resource checks and final source/binary identity checks
passed, but the [declared decision conditions](short-spin-diagnostic/decision-analysis.json)
did not. No runtime hint was adopted and no further timing runs were launched.

The existing benchmarks provide complementary coverage:

- Original Crucible runs 16 local worker processes against 50,000 rows with
  sustained updates and scans. It checks exact table/index agreement, allocation
  ownership, reclamation, operation failures, throughput, latency histograms,
  and time-series arena growth.
- Extended Crucible runs complete synthetic transactions, compares every
  phase's state and output against its reference, and reports retries, message
  latency, phase throughput, and arena high-water marks. Native contract probes
  are separate correctness requirements.

Both suites run against Aerostore alone here. PostgreSQL and production HyperFeed
performance are outside this comparison. The extended fixture has phase barriers
and bounded retained histories; its rates do not describe unrestricted production
traffic. Original Crucible supplies the sustained degradation check.

Both Crucibles enable the asynchronous WAL path, including record encoding,
shared-ring enqueue, the writer daemon, and clean shutdown drain. They do not
measure waiting for durable acknowledgement. A separately captured
`wal_ring_benchmark` release executable runs the existing
`benchmark_async_synchronous_commit_modes` test: 10,000 updates through the real
`OccCommitter` in each mode, with `fdatasync` per synchronous commit and final-value
checks. Its existing assertion that asynchronous throughput is at least ten times
synchronous throughput remains enabled. This is a single-row component workload;
it reports throughput, without synchronous latency percentiles or crash recovery.

Matched timing must use the copied baseline and candidate executables, identical
arguments and environment, alternating execution order, and a quiet host. Runs
performed during compilation, proof checking, or other stress tests are diagnostic
only. Every reported run must retain its correctness result, raw output, and
source/binary identity; unsuccessful or noisy measurements cannot establish a
performance pass.

The [comparison runner](../../../scripts/compare_engine_performance.py) fixes the
following campaign before timing:

| Workload | Repetitions per executable | Purpose |
| --- | ---: | --- |
| Original Crucible, 128 MiB, 120 seconds, 16 workers | 3 | Paired sustained throughput, p99, conflicts, resources |
| Original Crucible, 128 MiB, 240 seconds, 16 workers | 1 | Correctness, reclamation, and longer-run degradation; no standalone timing acceptance |
| Extended, 64 families, 128 cycles, 8 workers, seed 8675309 | 3 | 245,760 complete message deliveries per run |
| Extended, 256 families, 32 cycles, 16 workers, seed 20260922 | 3 | Same delivery volume with more families and workers |
| Existing 10,000-update synchronous/asynchronous WAL test | 3 | Separate synchronous acknowledgement throughput |

Pairs alternate baseline/candidate and candidate/baseline order. The operational
screen allows at most a 5% drop in median paired throughput or a 10% rise in median
paired p99; a single pair may not fall more than 10% in throughput or rise more
than 25% in p99. A within-executable metric range exceeding 10% of its median makes
the result inconclusive. These three-pair rules are practical regression margins,
not a statistical significance claim or evidence of zero overhead. Conflicts and
retries have a 20% relative or 0.02-per-operation allowance, whichever is larger;
arena high-water marks have a 5% plus 1 MiB allowance. Every existing correctness
and sustained growth gate must also pass. Missing or mismatched evidence fails.

Capture a candidate only when its source is stable:

```sh
python3 scripts/compare_engine_performance.py capture \
  --source . --source-commit CANDIDATE_BASE_COMMIT \
  --output target/verification-next/candidate
```

The source label records the candidate's base revision; hashes record the actual
working-tree source. Capture fails if source changes during compilation. Compiler
identity and benchmark fixture hashes must match the baseline.
The runner also verifies source hashes before and after timing, so a subsequent
working-tree change requires a fresh capture.

Then, in a coordinated window with builds, tests, and proof solvers stopped:

```sh
python3 scripts/compare_engine_performance.py run \
  --baseline target/verification-next/baseline/manifest.json \
  --candidate target/verification-next/candidate/manifest.json \
  --output docs/bench_data/verified_engine_2026-09-23/campaign \
  --quiet-window-note 'All project builds, tests, and proof solvers stopped'
```

The campaign records raw logs, allocator CSVs, extended reports, commands,
environment snapshots, executable hashes, and separate correctness/comparison
results. The default workload set takes at least 20 minutes plus extended replay
and synchronous WAL time. Background host activity was not comprehensively
measured. An exit code of 2 indicates a failure, observed regression, or
inconclusive timing; inspect the retained reasons before drawing conclusions.

Parser and decision regressions are covered by:

```sh
python3 scripts/test_compare_engine_performance.py
```
