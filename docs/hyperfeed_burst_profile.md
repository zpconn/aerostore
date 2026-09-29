# Service CPU burst investigation

The recurring service-thread CPU bursts show a substantially larger share of
sampled user-space execution in **predicate-lock acquisition**. Sampling the unchanged baseline and inspecting
its machine instructions narrowed the elevated commit samples to the bounded
index-bucket acquisition loop. This finding reproduced in a completed repeat.
It identifies waiting code, but not the contended index or the reason its owner
holds the lock. No engine change is promoted by this experiment.

The accepted sustained comparison remains
[3,072 messages/s versus PostgreSQL's 640/s (4.8×)](hyperfeed_sustained_capacity.md).
This investigation follows the unsuccessful
[frame-writing experiments](hyperfeed_frame_write_experiment.md).

## Experiment and observations

The completed diagnostic admitted 4,032 incoming messages/s for 140 seconds,
with 16 foreground workers, 1,024 retained flight identities (768 receiving
foreground messages and 256 quiet), seven configured forks,
temporary signature affinity of 600 ms, and the existing fixed-population
message mix. Projection and housekeeping retained their 300-second timers;
neither fired during this run. Engine vacuum and index reclamation remained
enabled. Transaction, asynchronous WAL, query and ordering contracts were
unchanged from the captured baseline.

The benchmark retained its 24 logical CPU affinity and 36 GiB memory envelope.
The profiler ran outside that envelope, sharing host CPUs. It sampled only the
owned benchmark processes and their threads at 49 Hz for approximately 120
seconds, starting five seconds into admission. User-space stacks and monotonic
timestamps were joined to one-second thread CPU observations using process and
thread start identities. The service CPU burst threshold, **8.5 cores**, was
declared before the run.

| Completed capture | Ordinary intervals | Burst intervals |
| --- | ---: | ---: |
| Observed wall time | 113.96 s | 6.04 s |
| Mean service CPU, user plus system | 6.69 cores | 9.50 cores |
| Service user-space stack samples | 11,257 | 930 |
| Leaf samples in the index-bucket attempt loop | 106 (0.94%) | 73 (7.85%) |
| All commit-function leaf samples | 191 | 80 |

Thus 73 of the 80 burst commit leaf samples fall inside that acquisition loop.
Disassembly of the exact recorded executable identifies its 4,096-attempt
bound, `pause`, and yield every 64 failed attempts, matching
`OccTable::acquire_index_bucket`. The earlier interrupted capture also showed
the difference: 9.59% of burst service samples versus 0.84% ordinarily.
These percentages describe sampled user-space locations, not percentages of
total CPU time or attainable throughput improvement.

The completed capture's bursts fall near 28–31, 61–63 and 94–96 seconds after
admission. Its highest one-second arrival-cohort p99, 64.10 ms, overlaps the
first burst. Whole-run retry-inclusive p99 was 21.38 ms; cohort percentiles are
not averaged to calculate it. The approximately 33-second spacing remains
unexplained.

All 564,480 incoming messages completed. Of those, 564,456 completed while
arrivals continued: **4,031.83 messages/s** over the full 140-second admission.
The maximum observed end-of-window queue was 84 messages, with 24 remaining
at admission end before successful drain. There were 364,299 retries:
363,692 classified at commit and 607 at candidate lookup. Detailed native
rejection causes were disabled in this unchanged baseline, so these counts
cannot distinguish lock exhaustion from validation conflicts. Successful
acquisition after spinning is not a transaction retry at all.

## What the evidence supports

The next target is **index predicate-lock contention**, including the owner
side of the wait. A concrete hypothesis is that due and expiry publication
buckets make unrelated flights compete: the configured one-second buckets
group updates by time, and foreground updates use current event time and due
time 30 seconds later. Other indexes remain possible. Neither the sampled
lock-field test nor the common-bucket hypothesis establishes the lock owner,
a priority waiter, or the origin of the periodic bursts.

Before changing lock ownership or index coverage, add a bounded diagnostic
that records index/bucket identity, successful contended acquisitions versus
exhausted attempts, attempt distributions, and sampled acquisition/hold times.
Keep observations local to the service thread and emit them outside the lock
scope. That should distinguish expensive waiting from long critical sections.

Then test the smallest justified candidate. Yielding every 16 failed attempts
instead of every 64 is one possible experiment; it preserves the attempt bound
and guard lifetime, but could increase kernel scheduling cost. Use alternating
**90–120-second foreground screens** to observe multiple bursts. A 30-second
screen can miss the phenomenon. Only a promising candidate advances to the
existing maintenance, full-history and repeated sustained-capacity checks.
Reduced spinning alone is insufficient: completed-message throughput and
retry-inclusive latency must improve without violating the other requirements.

The guard and guard-ownership adapters cover the acquisition loop's source and
ownership ordering. An in-memory adapter check accepted the proposed cadence
change, but no engine edit or proof was performed. Those proofs do not establish
fairness or performance; existing receipts cannot be reused after a source
change. Guard release must retain the existing publication, WAL, deregistration
and stamp ordering. Verification policies are unchanged.

## Limits, validation and retention

This is instrumented, foreground-only, metrics-based diagnostic evidence.
There is no new full-history verification or sustained capacity pass. The
completed run's execution, accounting, useful-work, storage and resource audits
passed, but its duration does not exercise maintenance or long-run retention.
It does not overturn the earlier 905-second failures at 4,032/s.

The decoder reported 21,472 valid stacks and no lost or malformed records;
18 samples fell outside the CPU observation interval. Approximately 83% of
service leaf samples resolved to symbols. Unresolved frames remain explicit.
Kernel stacks and blocked-time stacks were unavailable; `/proc` still measures
user and system CPU. The exact instruction mapping uses symbols and disassembly,
not source-line debug information. Correlated CPU and latency observations do
not prove causation.

The first attempt captured its stacks but was interrupted when the launcher
incorrectly rejected perf's scheduled SIGINT exit status. Its evidence remains
unchanged. A successor launcher checks that only its own sufficiently late
stop can accept that status; the repeat completed normally. Neither attempt
swapped or hit a memory limit. The analysis also explicitly records a one-ULP
serialization difference in a derived completion rate; counters and other
accounting fields retain exact comparisons.

**56 focused tests pass**, covering parsing, identity reuse, interval boundaries,
loss reporting, owned-process selection, controlled stopping, configuration
checks and accounting comparison. No Cargo build or engine modification was
needed. Both owned envelopes, profilers and samplers stopped; retained source,
binary and tool hashes were rechecked.

The [compact evidence manifest](bench_data/hyperfeed_burst_profile_2026-09-29/artifact-manifest.json)
and [archive validation](bench_data/hyperfeed_burst_profile_2026-09-29/archive-validation.json)
include analyses, scripts, test receipts, disassembly and compressed decoded
stacks. Raw perf recordings, histories, tools and executables remain at their
original local paths. This is not a portable archive of every runtime dependency.
Local evidence is under `target/hyperfeed-burst-profile-20260929-v1`.

The final pre-archive audit measured 3.13 GiB of growth against the 20 GiB session
budget, approximately 514 GiB guest and 260 GiB Windows free space, and 45 GiB
available RAM. The completed benchmark envelope peaked at 2.76 GiB. The 30 GiB
disk reserves and 4 GiB host memory reserve remained intact. No new Cargo build
trees needed pruning, no evidence was deleted, and no reclaimed Windows space
is claimed.

## Reproduce the analysis or resume

These commands use retained local artifacts and require fresh output paths:

```sh
SESSION=target/hyperfeed-burst-profile-20260929-v1
python3 "$SESSION/analyze_diagnostic_v2.py" \
  --run "$SESSION/diagnostic-02" \
  --output-directory "$SESSION/diagnostic-02-reanalysis"

python3 "$SESSION/instruction-attribution-01/attribute.py" \
  --run "$SESSION/diagnostic-02" \
  --cpu "$SESSION/diagnostic-02-reanalysis/cpu-analysis.json" \
  --analyzer "$SESSION/analyze_burst_stacks.py" \
  --capture target/hyperfeed-frame-write-20260929-v1/baseline/capture/capture.json \
  --output "$SESSION/instruction-attribution-reanalysis"
```

The saved launcher can repeat this exact baseline diagnostic:

```sh
python3 "$SESSION/profile_baseline_v2.py" --output "$SESSION/diagnostic-03"
```

That output must be fresh. The launcher enforces the session resource budget and
the captured workload; it is not a general candidate-comparison runner. Review
the [disk runbook](disk-space.md) and current resources before another run.
The next implementation work is the focused lock diagnostic described above,
followed by the [fast comparison loop](hyperfeed_iteration.md), not an automatic
repeat or queued capacity campaign.
