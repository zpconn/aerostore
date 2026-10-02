# HyperFeed commit phases and arena-placement experiment

A follow-on control changed only the benchmark arena backing. A volatile
memory-backed arena reduced end-to-end p99 by **39.6% and 52.5%** in two paired
120-second screens, while completed throughput remained at the offered limit
of approximately 4,031.8 incoming messages/s. Large page-fault spikes also
disappeared from the sampled memory-backed runs. This is a reason to qualify
the configuration under sustained load, not a demonstrated capacity increase.

Index source removal, index destination insertion, and row publication account
for **89.6% of the increase in sampled commit time after predicate acquisition**
during service CPU bursts. WAL callback time accounts for about 1.5%. This
narrows the next experiment; it does not identify a cause or demonstrate a
capacity improvement. The accepted sustained comparison remains
[3,072 incoming messages/s versus PostgreSQL's 640/s, or 4.8×](hyperfeed_sustained_capacity.md).

This follows the [predicate-lock diagnostic](hyperfeed_predicate_lock_experiment.md).
A temporary `commit-phase-diagnostics` feature divided native commit into 12
nonoverlapping phases. Approximately one attempt in 64 sampled both monotonic
wall time and thread CPU time. Bounded per-thread counters covered all attempts;
successful, failed and unwinding attempts remained separate. Service sessions
exported counters after transaction operations returned, with a final drain.
The timing path added no allocation or file I/O under native guards. Sampling
and export still perturb execution, so these measurements are diagnostic only.

The frozen instrumented source digest is `392ead301aba5163…`; the executable
digest is `d588a82a2abff5ba…`. The capture preserves full hashes, source contents,
compiler settings and runtime dependencies. After the completed run, the seven
temporary source changes were restored to the recorded baseline. No engine
optimization was promoted by this diagnostic.

The run offered 4,032 incoming messages/s for 140 seconds using 16 foreground
workers and two maintenance workers, 1,024 retained identities, seven configured
forks and 600 ms temporary signature affinity. The calibrated foreground stream
produced four update effects per incoming message. Both maintenance timers
remained at 300 seconds, so no projection or housekeeping job ran. Native
transaction and asynchronous WAL contracts were unchanged. There was no new
PostgreSQL run, and equal acknowledged crash durability is still not claimed.

All 564,480 incoming messages completed, including 564,452 during admission;
the remaining 28 completed during drain. End-to-end foreground p99 was 26.56 ms.
The accounting counts each incoming message once, excluding retry attempts and
fork updates. This short, instrumented run is **not a sustained-capacity
qualification**, a maintenance result, or a new full-history correctness result.

CPU and user-stack sampling covered 120 seconds starting about five seconds
into admission. The predeclared burst threshold was 8.5 service CPU cores. Six
CPU intervals exceeded it, averaging 9.44 service cores, versus 6.57 across the
114 ordinary intervals. Phase comparisons use only whole telemetry snapshots
covered entirely by one CPU class: 46 burst snapshots and 1,674 ordinary
snapshots. The 96 mixed and 332 partially covered or unobserved snapshots remain
separate. Counters were never prorated across CPU intervals.

The table uses **successful attempts only**: 214 sampled successes in burst
snapshots and 7,042 in ordinary snapshots. Times are sampled means, not p99s or
extrapolated totals. The interval after predicate acquisition comprises phases
2–11: partition acquisition through explicit guard release. It starts after all
predicate guards are acquired; its final phase includes releasing those guards.

| Successful commit observation | Ordinary wall | Burst wall | Ordinary thread CPU | Burst thread CPU |
| --- | ---: | ---: | ---: | ---: |
| Whole native commit | 70.72 µs | 289.42 µs | 69.57 µs | 273.73 µs |
| Predicate acquisition | 4.13 µs | 65.99 µs | 3.81 µs | 62.24 µs |
| Phases 2–11 after predicate acquisition | 39.64 µs | 184.43 µs | 39.26 µs | 172.88 µs |
| Index source removal | 13.68 µs | 62.80 µs | 13.63 µs | 62.84 µs |
| Index destination insertion | 12.93 µs | 58.93 µs | 12.62 µs | 50.43 µs |
| Row publication | 1.12 µs | 35.69 µs | 1.07 µs | 35.51 µs |
| WAL callback | 0.68 µs | 2.78 µs | 0.63 µs | 2.54 µs |

Source removal, destination insertion and row publication add 49.13, 46.00 and
34.58 µs respectively, out of the 144.79 µs increase in phases 2–11. These are
differences between sample means, not a causal allocation of latency. The two
clocks are read separately, so CPU can slightly exceed wall time for short
phases; their difference is not an exact measure of scheduling delay. Thread
CPU also includes spinning and kernel work, not just useful database work.

Across the complete run, counters recorded 564,480 successful native commits,
344,497 failed native attempts and zero unwinds. The success count matches the
benchmark's native commit count. In wholly burst snapshots, failed attempts
ended predominantly during predicate acquisition (3,076) and validation (1,691),
with one during partition acquisition. These are attempt counts, not failed
incoming messages. Failed-attempt cleanup belongs to its terminal phase and is
not mixed into the successful-phase means above.

The existing resource-counter reduction — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01-pagefault-analysis.json` (local-only evidence)
adds a temporal clue. All three CPU-burst episodes, near 29–31, 61–63 and
94–96 seconds, overlap large whole-cgroup page-fault increases and nonzero
file-writeback samples. The largest overlapping one-second counter deltas are
329,940, 325,890 and 321,489 faults respectively. The two resource intervals
whose entire read-time bounds lie within burst intervals contain 651,429 faults,
about 310,000–338,000/s, versus about 3,029–3,288/s across 109 wholly ordinary
intervals. Mixed and partial intervals remain separate; no counts are prorated.
The ranges reflect conservative clock/read-time bounds, not confidence intervals.
These counters cover the entire owned cgroup, including workers, WAL and history
activity. They neither identify the faulting mapping nor establish that arena
writeback caused the bursts.

The unexpectedly large increase in row-publication time and the fault
correlation make arena backing a useful next control. In the captured source,
the runner creates `arena.mmap`
inside the trial directory. Despite its name, `map_tmpfs_shared` opens the
supplied path and uses `MAP_SHARED`; it does not require a tmpfs filesystem.
The trial path is on the regular Linux filesystem. Periodic backing-file
writeback is therefore a hypothesis worth testing, **not an established cause**.
User-space stacks do not attribute kernel work, and the phase timing alone
cannot distinguish page faults, lock contention and other sources of CPU cost.
A controlled arena-placement experiment should keep WAL placement, workload,
ordering, workers and resource limits fixed, and explicitly state any changed
arena-lifetime or recovery contract.

All 18 telemetry streams passed identity, lifecycle, interval, outcome,
histogram and final-drain checks: 2,148 snapshots with clean observations.
The independent workload, observer and resource audits passed with no
diagnostic requirement failures. Focused Rust checks passed **26 distinct core
tests** (29 executions because three tests appeared in two filters) and **40
service/export tests**. The analyzer passed **36 tests**. An earlier compilation
failed because a temporary test fixture omitted a generic type argument; its
source, log and failure evidence remain preserved. No new formal proof is
claimed for the temporary instrumentation.

The diagnostic used 24 logical CPUs and a 36 GiB owned-memory limit, with 4 GiB
host-available and 30 GiB guest/Windows disk reserves. Peak owned memory was
2.78 GiB; the resource audit found no swap use or OOM event. Sidecars totaled
25.20 MiB. All processes owned by this diagnostic stopped. These checks do not
establish long-term memory retention or completion of the wider campaign.

Evidence remains under `target/hyperfeed-commit-phases-20260929-v1/`:

- Compact phase summary — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01-phase-summary.json` (local-only evidence) and full phase analysis — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01-phase-analysis.json` (local-only evidence). The compact summary binds its inputs and rechecked all 27 direct input bindings of the full analysis.
- Independent CPU, stack and workload assessment — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01-analysis/summary.json` (local-only evidence), run control — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01/control.json` (local-only evidence), and raw telemetry — `../target/hyperfeed-commit-phases-20260929-v1/diagnostic-01/phase-telemetry/` (local-only evidence).
- Capture — `../target/hyperfeed-commit-phases-20260929-v1/instrumented/capture/capture.json` (local-only evidence), instrumentation manifest — `../target/hyperfeed-commit-phases-20260929-v1/instrumentation-manifest.json` (local-only evidence), source patch — `../target/hyperfeed-commit-phases-20260929-v1/instrumentation.patch` (local-only evidence), and restoration receipt — `../target/hyperfeed-commit-phases-20260929-v1/restoration.json` (local-only evidence).
- Passing Rust test receipt — `../target/hyperfeed-commit-phases-20260929-v1/focused-checks-02/tests/receipt.json` (local-only evidence), earlier compile failure — `../target/hyperfeed-commit-phases-20260929-v1/focused-checks/` (local-only evidence), analyzer — `../target/hyperfeed-commit-phases-20260929-v1/analyze_phases.py` (local-only evidence), and 36-test log — `../target/hyperfeed-commit-phases-20260929-v1/analyze-phases-tests-v2.log` (local-only evidence).

The follow-on storage-placement experiment used one uninstrumented captured
binary in all four cells. The order was file/memfd for seed 20260929, then
memfd/file for seed 20260930. Each admitted 483,840 messages over 120 seconds at
4,032/s. Workload, population, message mix, ordering, workers, maintenance
cadence, WAL placement and resource limits were held fixed. A live observer
verified the control arena and WAL shared the original filesystem, while the
memfd arena had tmpfs backing and the WAL remained on the original filesystem.
The captured executable digest is `29f16a2ff50d9d36…`.

The retained experiment selected its treatment with
`AEROSTORE_CONTENTION_ARENA_BACKING=memfd`, using a benchmark fixture backed by an
owner-held memfd. Current binaries require the explicit
[`--arena-backing memfd` option](hyperfeed_qualification.md#arena-backing) and
reject that former environment override; the captured experiment remains
unchanged. Its compatibility path refers
to that owner's open descriptor. Existing attached mappings survive owner
death, but the original path cannot reopen the arena after owner death. This
differs from a named file's lifetime. No persistent-arena recovery or equal
crash-durability claim follows from these performance results. The normal
benchmark default remains file-backed, and the engine's commit algorithm is
unchanged.

| Seed | File p99 | Memfd p99 | Reduction | Memfd/file completed throughput |
| --- | ---: | ---: | ---: | ---: |
| 20260929 | 22.31 ms | 13.47 ms | 39.6% | 0.999994 |
| 20260930 | 27.60 ms | 13.10 ms | 52.5% | 1.000000 |

All four cells passed their short-screen requirements and drained all admitted
messages. Both pairs exceeded the predeclared 20% and 1 ms latency margin;
neither exceeded the 1% throughput margin. The generic implementation comparator
intentionally retained `control_only` because the binaries are identical. The
separate summary reports the actual, declared storage treatment and does not
reinterpret the generic result as an implementation promotion. Two seeds are
a screening result, not a confidence bound. Maintenance again did not fire.

The independent observer bracketed each CPU/memory-counter read with monotonic
timestamps and joined them to the actual offered-schedule admission bounds.
Each cell provided 119 adjacent intervals wholly inside admission, covering
about 119 seconds. Boundary intervals were excluded without prorating. Whole
owned-cgroup page-fault rates fell from approximately 19,460 to 907/s in the
first pair and 20,470 to 898/s in the second. The largest roughly one-second
fault deltas were 335,267 and 342,580 for file backing, versus 7,870 and 7,866
for memfd. These counters include the driver, workers, WAL and observer; they
do not identify a faulting mapping or a kernel mechanism.

Whole owned-cgroup CPU averaged approximately 12.4–12.7 cores in every cell,
with no consistent mean CPU reduction. These values cannot be compared directly
with the earlier native-service-only CPU means. The largest sampled interval
CPU upper bounds fell from 15.32/15.63 cores for file to 13.97/14.12 for memfd.
Foreground retry rates also showed no consistent reduction: 0.641 to 0.633
and 0.635 to 0.648 retries/message. Coarse causes remained predominantly commit
serialization failures; detailed native retry diagnostics were disabled.

The memfd runs recorded about 69 MiB of shared-memory charge, versus zero for
file backing. File dirty/writeback gauges still cover the unchanged WAL and
other file activity: one memfd run observed a 48.3 MiB writeback sample, so
this is not evidence that all file writeback stopped. The combination of the
controlled placement change, much smaller fault spikes and lower p99 makes
arena placement a stronger optimization lead than a speculative lock change.
It does not yet prove the precise writeback/fault mechanism or raise accepted
capacity above 3,072/s.

The fixture suite passed **41 tests, including five new backing/lifetime
tests**. Three placement-harness tests, 26 final analyzer tests and the existing
default-source verification gate also passed. The analyzer's first version
correctly refused an undeclared one-ULP serialization difference in a derived
retention elapsed-time field. Its helper and failure log remain preserved; the
new version permits that named field within one ULP and records all such
differences. Integer counts, structure and other values remain exact. Resource
audits passed, peak owned memory stayed below 2.46 GiB, and all four owned jobs
stopped. No new formal proof or long-term memory-retention result is claimed.

The [follow-on capacity investigation](hyperfeed_arena_capacity.md) adds explicit
CLI/configuration fields and records requested and observed backing with its
lifetime contract. Its first 4,032/s sustained trial passed operational checks;
6,400/s kept up but missed the 50 ms p99 requirement. Neither is a new qualified
capacity point. The default remains file-backed. The historical environment
treatment still requires its retained experiment harness; current binaries
reject that override and bind storage observations through ordinary qualifier
metadata.

- Storage experiment and four assessments — `../target/hyperfeed-commit-phases-20260929-v1/arena-burst-4032/experiment.json` (local-only evidence), source-bound placement analysis — `../target/hyperfeed-commit-phases-20260929-v1/arena-burst-4032-analysis.json` (local-only evidence), and captured executable/source — `../target/hyperfeed-commit-phases-20260929-v1/arena-config/capture/capture.json` (local-only evidence).
- Placement harness — `../target/hyperfeed-commit-phases-20260929-v1/arena_placement.py` (local-only evidence), final analyzer — `../target/hyperfeed-commit-phases-20260929-v1/analyze_arena_placement_v2.py` (local-only evidence), 26-test log — `../target/hyperfeed-commit-phases-20260929-v1/analyze-arena-placement-tests-v2.log` (local-only evidence), and preserved first-analysis refusal — `../target/hyperfeed-commit-phases-20260929-v1/analyze-arena-placement-first-attempt.log` (local-only evidence).
- Fixture test receipt — `../target/hyperfeed-commit-phases-20260929-v1/arena-tests/tests/receipt.json` (local-only evidence) and default-source gate — `../target/hyperfeed-commit-phases-20260929-v1/final-default-source-gate.log` (local-only evidence).

To repeat the paired storage control without rebuilding, review the
[disk runbook](disk-space.md) and choose a fresh output:

```sh
SESSION=target/hyperfeed-commit-phases-20260929-v1
python3 "$SESSION/arena_placement.py" screen \
  --capture "$SESSION/arena-config" \
  --output "$SESSION/arena-burst-4032-repeat" --rate 4032
```

The harness needs the host permissions used by the systemd memory envelope and
rechecks its disk/memory reserves. This optional repeat is not the next
prioritized step; explicit backing metadata and sustained qualification are.

The final audit — `../target/hyperfeed-commit-phases-20260929-v1/final-audit.json` (local-only evidence)
validated 1,712 referenced files and confirmed that all ten owned resource
envelopes stopped. The [compact evidence manifest](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_commit_phases_2026-09-29/artifact-manifest.json)
and [archive validation](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_commit_phases_2026-09-29/archive-validation.json)
preserve approximately 1.19 MiB of summaries, helpers, source patches and
receipts. Raw profiles, histories, full analyses and executables remain at
their original local paths.

Session growth was 5.34 GiB against the 20 GiB budget; approximately 502.5 GiB
guest disk, 245.7 GiB Windows disk and 45.5 GiB available RAM remained at
closeout. Builds reused the normal Cargo development cache. No isolated compiler
trees were created, no evidence or compiler intermediates were deleted, and
zero reclaimed space is claimed on either Linux or Windows.
