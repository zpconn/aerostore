# Frame writing experiment

Neither tested frame-coalescing implementation earned promotion. The working
engine is restored to the unchanged baseline. Both candidates, their tests,
executables and results remain preserved for further investigation.
The accepted sustained capacity result remains
[3,072 messages/s versus PostgreSQL's 640](hyperfeed_sustained_capacity.md).

## Changes and checks

The native service sends a four-byte length followed by a serialized payload.
The baseline writes those separately and updates the socket timeout before each
attempt. Two candidates tried to send a complete frame in one write:

- **Vectored:** submit header and payload as two slices in one socket operation.
- **Contiguous:** reserve the header in the serialization buffer and use one
  ordinary write. This retains bincode's size prepass and avoids an extra payload
  copy or allocation.

Both preserve transaction operations, wire format, absolute deadlines and
partial-write handling. The workload, message accounting, indexes, workers,
durability settings and resource envelope are unchanged. The changes are
confined to the service implementation and its test module.

The vectored candidate passed 42 service tests; the contiguous candidate passed
44. New tests exercise the actual production writer with partial writes,
interrupted calls, zero writes, permanent errors and expired deadlines. The
contiguous tests also compare bytes with the original encoder and reject empty
or oversized payloads. Real Unix/TCP round trips, killed-client survivor tests
and lost-commit-reply tests pass. Test sources match the respective benchmark
captures, and retained executable/runtime hashes were validated.

Proof-impact reports remain advisory. The socket implementation is outside the
declared refinement components; the existing service protocol model does not
prove framing refinement. No new formal acceptance or cached proof reuse is
claimed, and no existing verification policy changed.

## Short paired measurements

These are four-cell foreground screens: baseline/candidate for seed 20260929,
then candidate/baseline for seed 20260930. Each admits messages for 30 seconds
at a fixed rate. The first natural maintenance tick is after the screen.
They are candidate-selection evidence, not sustained capacity measurements.

| Candidate | Offered messages/s | Baseline p99, seeds 29 / 30 | Candidate p99, seeds 29 / 30 | Screen decision |
| --- | ---: | ---: | ---: | --- |
| Vectored | 3,584 | 31.17 / 27.12 ms | 23.24 / 27.20 ms | Neutral |
| Vectored | 4,032 | 29.86 / 32.83 ms | 37.16 / 49.94 ms | Inconclusive; noisy, worse in both pairs |
| Contiguous | 4,032 | 62.82 / 31.42 ms | 45.73 / 30.78 ms | Inconclusive; noisy, useful difference in only one pair |

All twelve cells produced valid measurements. Eleven met the screen policy;
the first baseline cell in the contiguous comparison failed the 50 ms p99
requirement. Completed-message throughput stayed near the offered ceiling; the
largest candidate/baseline difference in any pair was below 0.2%. No capacity
gain follows from these observations. In particular, the contiguous candidate's
passing cells do not overturn the repeated 905-second baseline failures at
4,032/s.

The three comparisons took about eleven minutes in total. Each candidate
benchmark build took 8.2 seconds; its focused test build and execution took
about 8.5 seconds. This is the intended use of the
[fast loop](hyperfeed_iteration.md): stop an unconvincing experiment before
spending hours on maintenance/full-history/sustained promotion runs. These
results do not prove that frame coalescing can never help another workload.

## Evidence guiding the next step

The earlier 180-second diagnostic at 4,032/s had aggregate p99 of 34.86 ms,
despite individual one-second cohorts exceeding 90 ms. Its ordinary windows
show no monotonic warmup-related deterioration. The failing 905-second runs
also had large latency bursts within their first 30 seconds, followed by
recurring bursts away from maintenance ticks. A short screen can miss a burst;
averaging window percentiles would not reconstruct the run's aggregate p99.

During bursts in the diagnostic, service-session CPU rose from roughly
6.6–7.1 cores to 9.5–9.9, while foreground worker CPU fell and voluntary context
switch rates stayed approximately stable. That makes episodic service-side work
or contention the next profiling target. It does not identify the responsible
code path or establish that socket writes caused the bursts.

The next bounded diagnostic should observe 90–120 seconds and collect
service-session user/kernel stacks alongside per-window message accounting.
The current environment has no installed `perf` executable; this experiment
did not install a profiler or launch that next trial. Obtain stack attribution
before another implementation change. Further framing variants and expensive
proof work on their implementation are deferred.

The [subsequent profiling investigation](hyperfeed_burst_profile.md) installed
a local profiler and collected user-space stacks with unchanged engine bytes.
It narrowed the elevated commit samples to index predicate-lock acquisition;
kernel stacks remain unavailable. That report supersedes the tooling status
and next-step recommendation above.

## Retention and resources

The [validation record](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_frame_write_2026-09-29/validation.json)
contains original report copies, candidate patches, source/binary hashes and
local evidence paths. Full source captures, executables, runtime dependencies,
test logs, resource receipts and workload outputs remain at
`target/hyperfeed-frame-write-20260929-v1`. This compact record is not a portable
archive of every raw artifact. Earlier capacity evidence is unchanged.

The final audit validated all three captures and both test executables, confirmed
all 17 owned envelopes had stopped, and found no active build or screen jobs.
Allocated growth was 4.69 GiB within the shared 20 GiB budget. Linux and Windows
retained their 30 GiB reserves, with no observed envelope swap or memory-limit
event. No evidence or compiler trees were deleted; the ordinary development
cache remains reusable. No space reclaimed from Windows is claimed.
