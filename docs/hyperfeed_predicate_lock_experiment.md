# Predicate-lock diagnostic and yield-cadence experiment

The service CPU bursts concentrate contention in **due-index commit locks**.
Both acquisition and time after all predicate guards are acquired increase.
Other locks, WAL work and scheduling delays within that interval remain
unresolved; this identifies a target, not a cause or throughput improvement.
The accepted sustained comparison remains
[3,072 incoming messages/s versus PostgreSQL's 640/s, or 4.8×](hyperfeed_sustained_capacity.md).

This follows the [baseline CPU profile](hyperfeed_burst_profile.md).
A temporary `predicate-lock-diagnostics` feature recorded acquisition outcomes
by index, physical bucket and lookup/commit context. Timing used a deterministic
mixed-counter sample of approximately one acquisition in 64. Each service
thread drained bounded counters after transaction operations returned, roughly
once per second, with an explicit final drain and a 64 MiB file cap. Serialization
and file writes occurred after native guards were released. Export failures
were fatal to the diagnostic, including failures after a transaction committed.

The frozen diagnostic source digest was `309924344eaea193…`; its executable
digest was `6e42a0170bdefe1d…`. Full hashes, compiler settings, dependencies and
source contents are preserved in the capture and instrumentation manifest below.
After measurement, the temporary instrumentation was restored away. A fresh
default capture reproduced the earlier baseline executable, `6ec1bacc1391938c…`.

The run offered 4,032 incoming messages/s for 140 seconds, using 16 foreground
workers, two maintenance workers, 1,024 retained identities, seven configured
forks, the existing message mix, and 600 ms temporary signature affinity.
Projection and housekeeping retained 300-second timers, so neither fired.
Transaction, ordering, query and asynchronous WAL contracts were unchanged.
CPU and user-stack sampling covered approximately 120 seconds beginning five
seconds into admission. All 564,480 incoming messages completed; this short,
instrumented run is **not a sustained-capacity qualification**.

Service CPU crossed the predeclared 8.5-core threshold near 29–31, 61–63 and
94–96 seconds. It averaged 9.53 cores across those six CPU intervals, versus
6.80 ordinarily. Lock comparisons below use only whole telemetry snapshots
covered entirely by one CPU class: 46 burst snapshots and 1,673 ordinary
snapshots. Another 96 mixed and 335 partially/unobserved snapshots remain
separate; counters were never apportioned across overlapping CPU intervals.

| Due-index commit-lock observation | Ordinary snapshots | Burst snapshots |
| --- | ---: | ---: |
| Acquisitions | 1,108,875 | 24,040 |
| Successful after contention | 10.58% | 34.21% |
| Exhausted bounded acquisition attempts | 0.167% | 12.12% |
| Sampled mean acquisition wait | 2.856 µs | 82.74 µs |
| Acquisition timing samples | 17,430 | 378 |
| Approximate mean hold observation | 30.68 µs | 156.34 µs |
| Hold timing samples | 17,407 | 329 |

A separate [commit-set reduction](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01-commit-set-summary.json)
finds mean duration **after all predicate guards are acquired** rises from
30.04 to 160.31 µs (5.34×; 11,140 versus 215 samples). Acquiring the complete set
rises from 6.72 to 81.57 µs. Later predicate acquisition alone cannot explain this.

Across the entire diagnostic, due commit locks exhausted 10,369 acquisitions,
compared with 547 for callsign commit locks; family and event-time commit locks
had none. The hottest due buckets include 60, 92, 124 and 157. Their roughly
32-bucket spacing is an observation requiring explanation, not proof of an
index defect. Lock acquisitions, retries and fork updates are not incoming
message counts.

Hold observations begin after a successful CAS and end after release of the
whole guard set. They include time spent acquiring later locks and are neither
exact individual hold times nor strict upper bounds. Timing means divide
sampled duration by sampled count; they do not extrapolate total waiting time.
Instrumentation itself changes scheduling and critical-section costs. User
stacks omit kernel and blocked-time stacks; temporal correspondence alone does
not identify a causal mechanism.

All 18 sidecars passed provenance, identity, interval, final-drain, outcome and
histogram checks: 2,150 snapshots, zero entry overflows, dropped hold samples,
instrumentation errors or unwind errors. The final focused run passed 11 core
tests and 38 service/export tests; 31 analyzer tests passed. An earlier test run
exposed temporary-directory permissions incompatible with the export guard;
test fixtures were corrected to explicit mode 0700 and the failure retained.
No new formal proof or full-history oracle result is claimed for this diagnostic.

The diagnostic retained 24 logical CPUs and a 36 GiB memory envelope, with a
4 GiB host-available reserve and 30 GiB guest/Windows disk reserves. Peak owned
memory was 3.08 GiB, with no swap use or OOM event. Sidecars totaled 304.65 MiB.
Resource audits passed and all diagnostic processes stopped. These observations
cover this short run, not long-term memory retention or maintenance completion.

The follow-on experiment changed only index-lock yield cadence from every 64
failed attempts to every 16, preserving the 4,096-attempt bound and guard
lifetimes. Diagnostics were disabled in both builds. Four 120-second admissions
ran at the same 4,032/s offered rate, in baseline/candidate then candidate/baseline
order across two seeds. The complete paired screen took **9m45s**, including
setup, drain, audits and resource control.

| Seed | Baseline p99 | Candidate p99 | Candidate/baseline completed throughput |
| --- | ---: | ---: | ---: |
| 20260929 | 23.82 ms | 20.90 ms | 0.999994 |
| 20260930 | 28.44 ms | 23.60 ms | 1.000004 |

All four cells passed. Each drained all 483,840 offered messages; between
483,814 and 483,817 completed during admission, approximately 4,031.8/s.
No incoming message exhausted its retries, and there was no resource
interruption or screen queue failure.
Maintenance did not fire. The p99 reductions of 12.3% and 17.0% are below the
predeclared 20% paired-selection threshold, so the screen reports **neutral**.
This is a modest latency signal, not a demonstrated capacity increase or proof
that the effect exceeds measurement noise. The candidate remains preserved;
the working engine was restored to baseline, with no promotion or longer
campaign launched for this tweak.

The candidate passed 20 focused native publication, predicate and exhaustion
tests. In-memory guard and ownership adapters mapped exactly the one-line
cadence change. The default-source gate correctly rejected the changed source;
it passed again after restoration. No new formal proof was run. The retained
120-second burst lane passed 44 focused Python checks and leaves the default
30-second lane and sustained-capacity policy unchanged.

The next diagnostic should split the fully held commit interval into partition
lock acquisition, validation, index destination insertion, WAL publication,
source removal, and transaction/stamp completion. Index insertion and removal
can wait for a skiplist mutation lock also used by priority GC. That is a
code-supported possibility, not a measured cause of the bursts. The run showed
no index allocation failures and at most 111 retired due nodes and 333 retired
due postings; the 131,072-entry reclamation batch limits are not message timers.

Changing publication-bucket width needs a separate horizon check. This policy
has 4,093 fixed interior intervals followed by a shared tail bucket: one-second
width covers about 68 minutes, while one-millisecond width covers only about
four seconds. A short precision experiment must not hide later saturation.

The [compact evidence manifest](bench_data/hyperfeed_predicate_locks_2026-09-29/artifact-manifest.json)
and [archive validation](bench_data/hyperfeed_predicate_locks_2026-09-29/archive-validation.json)
preserve summaries, source patches, helpers and test receipts. Raw evidence is
retained locally under `target/hyperfeed-predicate-locks-20260929-v1/`:

- [Capture and exact executable](../target/hyperfeed-predicate-locks-20260929-v1/instrumented/capture/capture.json), [instrumentation manifest](../target/hyperfeed-predicate-locks-20260929-v1/instrumentation-manifest.json), [patch](../target/hyperfeed-predicate-locks-20260929-v1/instrumentation.patch), and [restoration receipt](../target/hyperfeed-predicate-locks-20260929-v1/restoration.json).
- [Completed run/control and resource checks](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01/control.json), [raw sidecars](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01/lock-telemetry/), and [CPU/stack analysis](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01-analysis/summary.json).
- [Compact lock findings](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01-lock-summary.json), [full bound analysis](../target/hyperfeed-predicate-locks-20260929-v1/diagnostic-01-lock-analysis.json), [analyzer](../target/hyperfeed-predicate-locks-20260929-v1/analyze_locks.py), and [31-test receipt](../target/hyperfeed-predicate-locks-20260929-v1/analyzer-tests-02.log).
- [Passing Rust test receipt](../target/hyperfeed-predicate-locks-20260929-v1/focused-checks-02/tests/receipt.json), [earlier failed tests](../target/hyperfeed-predicate-locks-20260929-v1/focused-checks/), and [resource budget](../target/hyperfeed-predicate-locks-20260929-v1/resource-baseline.json).

Raw profiles, sidecars and executables remain local; they are not a large Git
archive. Their manifests and analysis bind the original bytes and paths.
The closeout validated 1,673 referenced files and the captured executables and
sources. All owned jobs stopped. Growth was 5.61 GiB against the 20 GiB session
budget; about 508 GiB guest disk, 252 GiB Windows disk and 45.5 GiB available
RAM remained. Builds reused the normal development cache; no isolated compiler
trees needed pruning, no evidence was deleted, and no reclaimed space is claimed.

To repeat the preserved candidate without rebuilding, choose a fresh output:

```sh
SESSION=target/hyperfeed-predicate-locks-20260929-v1
python3 scripts/iterate_hyperfeed.py screen \
  --baseline "$SESSION/baseline" --candidate "$SESSION/yield16" \
  --allow-change aerostore_core/src/occ_partitioned.rs \
  --lane burst --rate 4032 --output "$SESSION/yield16-burst-repeat"
```

Review the [disk runbook](disk-space.md) first. This is an optional repeat;
the next prioritized work is the commit-phase diagnostic described above.
