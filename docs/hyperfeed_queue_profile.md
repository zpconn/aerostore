# HyperFeed capacity, queue and transport investigation

The accepted synthetic capacity lower bounds are now **6,400 offered incoming
messages/s for AeroStore and 704/s for PostgreSQL**, each with **24 foreground
workers on the same 24 logical CPU budget**. Both configurations passed two
905-second trials, normal maintenance, resource checks and their matching
full-history correctness companions. The [bound comparison](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/workers24-accepted-comparison01.json)
gives **9.09× for the tested offered endpoints**. Neither maximum capacity is
established, so this is not a ratio of maximum capacities or the 10× target.

| Configuration | Highest repeatably passing offered/s | Completed incoming messages/s during arrivals, two trials | End-to-end p99 including retries, two trials |
| --- | ---: | ---: | ---: |
| AeroStore, memfd arena and file WAL | 6,400 | 6,399.9724 / 6,399.9680 | 9.014991 / 9.174309 ms |
| PostgreSQL 16.13, current configuration | 704 | 703.9912 / 703.9923 | 41.827283 / 42.431067 ms |

Each incoming foreground message counts once; fork updates, transactions,
retries, maintenance and completions after arrivals stop do not inflate these
rates. All messages completed after drain, and all three jobs of each
maintenance class completed on time with positive effects in every sustained
trial. PostgreSQL's separate 768/s short screen failed the 50 ms p99 requirement;
it is not a repeated sustained upper bound. No higher rate was tested in this
AeroStore configuration. The native improvement came from using 24 workers
with the unchanged executable, not an engine code change.

The workload and transaction boundaries remain matched: query-discovered
updates, serializable transactions, temporary signature affinity, 1,024
identities, seven-fork profile and batched maintenance every 300 seconds.
AeroStore uses a volatile memfd arena and asynchronous file WAL, requesting
`fdatasync` every ten seconds and at drain. PostgreSQL uses `fsync` and
`full_page_writes`, asynchronous commit and a ten-second WAL writer delay.
Acknowledged crash-loss windows and recovery behavior are not proven equivalent.
Transport, index maintenance and conflict logging also differ. This fixed
synthetic workload does not establish production HyperFeed or physical
multi-machine performance, or optimal PostgreSQL tuning.

Two narrowly scoped socket-write candidates did **not** establish a repeatable
performance improvement. Neither was promoted, and the original service
implementation was restored. Their tests, executable captures, patches and
negative or inconclusive measurements remain preserved.

A separate configuration probe with the unchanged executable and 24 foreground
workers passed two short 6,400/s screens at **8.43 ms and 8.39 ms p99**, using
the same 24 logical CPU budget. A separate maintenance screen also passed at
**10.05 ms p99**, with all seven jobs of each class completed. These results
led to the sustained trials above; they do not independently qualify 6,400/s
capacity or a new PostgreSQL speedup ratio.

A 300-second profile at 6,400 offered messages/s found substantial system CPU
usage and frequent execution through the client RPC path. It justified examining
framing, socket calls and deadline handling; it did **not** establish which
mechanism caused the intermittent latency failures in the earlier 905-second
run. Fewer predicted socket calls were a hypothesis, not evidence of a speedup.

Both candidates preserved message handling, queries, transactions and the
existing primitive operation interface. Compound read/write operations and
workload changes were outside this experiment. The accepted historical capacity
comparison for the earlier 16-worker configuration remains **3,072/s native
versus 640/s PostgreSQL, or 4.8×**. See the
[preceding capacity results](hyperfeed_arena_capacity.md) for the unsuccessful
6,400/s sustained trial and its unchanged acceptance requirements.

## Socket candidates and decisions

Each candidate received a separate paired screen against the same preserved
baseline executable. Each cell offered 6,400 messages/s for 120 seconds, with
seeds 20260929 and 20260930 in baseline/candidate/candidate/baseline order.
The workload, 1,024 identities, seven-fork population, 16 foreground workers,
signature affinity, memfd arena, file WAL, primitive requests, transaction
boundaries, 24 logical CPU budget and 50 ms p99 requirement stayed fixed.
The 300-second maintenance timers did not fire during these short screens.

The first candidate attempted an immediate nonblocking send, preparing the
existing timed blocking write only when the socket was full. The second started
afresh from the baseline and sent one frame's existing four-byte prefix and
serialized payload through a single blocking `sendmsg(MSG_NOSIGNAL)`. It did
not combine separate protocol requests or transactions. Both retained the
original frame bytes, absolute deadline and application operations.

| Candidate | Seed | Baseline p99 | Candidate p99 | Baseline completed/s | Candidate completed/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nonblocking attempt | 20260929 | 19.45 ms | **61.55 ms** | 6,399.58 | 6,398.86 |
| Nonblocking attempt | 20260930 | 27.13 ms | 31.73 ms | 6,399.66 | 6,399.64 |
| Vectored frame write | 20260929 | 34.35 ms | 24.85 ms | 6,399.68 | 6,399.61 |
| Vectored frame write | 20260930 | 20.89 ms | 24.85 ms | 6,399.63 | 6,399.63 |

Completed throughput counts logical incoming messages received during admission
after all processing and any retries, excluding drain-only completions. All eight cells
eventually completed all 768,000 offered messages, and their execution, resource
and storage checks passed. The first candidate's 61.55 ms result was a latency
policy failure, not a censored measurement. Offered load capped throughput, so
the near-identical completion rates do not demonstrate increased capacity.

Both automated comparisons were **inconclusive** because the declared noise
threshold was exceeded. The first candidate's p99 was worse in both pairs;
that is enough reason to decline promotion, without claiming a proven stable
regression. The vectored candidate improved one pair and worsened the other.
Its similar p99 across its two trials does not establish an improvement over
the variable baseline. Neither justified launching a sustained qualification
campaign.

The first candidate passed 43 focused service tests; the vectored candidate
passed 42, including partial prefix/payload writes, interruption, deadlines,
real Unix/TCP framing, closed-peer SIGPIPE handling, and existing lost-reply
and killed-client coverage. These are transport and service checks, not a new
formal refinement proof or full-history correctness qualification. The original
source and executable remain the working implementation.

The retained [nonblocking screen](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/write-fast-path-burst/screen.json.gz)
and [decision](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/write-fast-path-decision.json),
[vectored screen](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/vectored-write-burst/screen.json.gz)
and [decision](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/vectored-write-decision.json)
bind the paired results. The
[43-test receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/service-tests/tests/receipt.json.gz)
and [42-test receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/vectored-service-tests/tests/receipt.json.gz)
retain the executed test source, binaries and logs. Candidate source and
executables remain in the respective
[nonblocking capture](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/write-fast-path/capture/capture.json.gz)
and [vectored capture](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/vectored-write/capture/capture.json.gz).

## More workers with the unchanged executable

After restoring the baseline implementation, two separate 120-second probes
increased foreground workers from 16 to 24. They reused the preserved baseline
executable `62a98297d18e68f5…`, unchanged initial records and global message
corpus for each seed, message mix, ordering, signature affinity, primitive
operations, transaction boundaries and memfd/file-WAL configuration. Dispatch
ownership and per-worker message counts changed with the worker count. CPU
affinity remained the same 24 logical CPUs, and the 36 GiB owned memory ceiling
and other resource reserves stayed fixed.

| Seed | Completed messages during admission | Completed/s | End-to-end p99 | Retries per incoming message | Sampled maximum queue |
| --- | ---: | ---: | ---: | ---: | ---: |
| 20260929 | 767,974 | 6,399.78 | 8.43 ms | 0.9576 | 36 |
| 20260930 | 767,972 | 6,399.77 | 8.39 ms | 0.9555 | 35 |

Both probes passed the short-screen operational, resource and storage checks
and completed all 768,000 offered messages after drain. Their p99 includes
queueing and retries, and retries do not increase the message counter. The
roughly 0.96 retries per message remain a measurable cost of this configuration.
The queue observations are sampled once per second, not continuous maxima.
Neither probe exercised maintenance because the first 300-second timer tick
fell after admission ended.

The per-worker hard-abort backlog limit remained 10,000, so its aggregate
allowance increased from 160,000 to 240,000 when worker count changed. The
stricter passing queue-growth, completion and 50 ms end-to-end p99 requirements
were unchanged; the observed sampled queues were far below either hard-abort
allowance.

These are worker-count configuration probes, not database-engine code
improvements or a paired comparison against 16 workers. They motivate the next
maintenance and sustained trials. They do not establish maximum capacity,
long-term memory retention, full-history correctness or a ratio against the
historical 16-worker PostgreSQL baseline. A comparable PostgreSQL configuration
and the existing durability caveats remain necessary for an updated comparison.

The [predeclared plan](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/workers24-probe-plan.json),
[seed 20260929 receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-s29/cell.json.gz)
and [seed 20260930 receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-s30/cell.json.gz)
preserve the fixed settings, changed worker/backlog scope, corpus checks,
per-trial commands and resource evidence.

The subsequent [40-second maintenance screen](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-maintenance-s29/cell.json.gz)
used the same executable and 24-worker configuration, with both maintenance
timers deliberately shortened to five seconds. It completed 255,974 incoming
messages during admission (**6,399.35/s**) and all 256,000 after drain, with
**10.05 ms end-to-end p99**. Housekeeping and projection each completed all
seven offered jobs; three housekeeping jobs and two projection jobs performed
positive work, with no jobs late or unfinished. Resource and storage checks
passed. This checks overlap with frequent maintenance; it is still a short
screen and does not establish sustained capacity or full-history correctness.

The [first 905-second trial, seed 20260929](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-long-s29/cell.json.gz),
then passed its operational requirements with the realistic 300-second
maintenance cadence. It completed **5,791,975 messages during admission**
(**6,399.97/s**) and all 5,792,000 after drain, with **9.01 ms end-to-end p99**.
Housekeeping and projection each completed all three offered jobs on time,
and all six jobs performed positive work. Sampled queue growth passed the
unchanged requirement; the sampled maximum queue was 172 inputs.

Resource checks passed with zero accepted swap and no owned OOM or limit
events. Peak whole-envelope memory was **24,586,358,784 bytes**, including
coordinator records and file cache; this is not database arena usage. Arena
high-water reached **77,860,296 bytes**, with retired index postings drained
to zero. These are observed retention results over this run, not evidence of
indefinitely flat memory use.

The [second 905-second trial, seed 20260930](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-long-s30/cell.json.gz),
also passed: **5,791,971 messages during admission**, **6,399.97/s** and
**9.17 ms p99**, with all 5,792,000 inputs completed after drain. All three
housekeeping and all three projection jobs completed on time and performed
positive work. Resource and storage checks passed; whole-envelope memory
peaked at **24,584,531,968 bytes**, arena high-water at **78,908,664 bytes**,
and retired postings drained to zero.

Both original cell records retain
`operational_pass_pending_companion_and_repeat`, with capacity eligibility
false. Those historical records were not rewritten. The subsequent
[bound acceptance receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-capacity-accepted01/binding.json.gz)
is complete with `capacity_accepted=true`, and the
[unchanged capacity assessor](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-capacity-accepted01/assessment.json.gz)
classifies both trials as passed after binding the matching full-history
companion and independent resource evidence. It establishes the conditional
synthetic lower bound of 6,400/s for this configuration. No higher repeated
failure was tested in this configuration, so neither a maximum nor an upper
bound is established. No PostgreSQL ratio follows from this result alone.

## Correctness companion and its instrumentation cost

The [full-history companion](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/workers24-full6400/guard.json.gz)
passed with the same captured executable, 24 foreground workers, 6,400 offered
messages/s, 40-second admission and five-second maintenance timers. It
completed all **256,000 foreground inputs and 14 scheduled maintenance jobs**.
The oracle returned `Valid` for **257,303 transactions**, finding a serial
witness for complete observations, effects, outcomes and final state that
respects non-overlap. It explored **257,320 states** and took **1,462.17 seconds**
of checking. Resource and cleanup checks passed.

**The trace-enabled companion failed the performance requirements.** Its
foreground accounting records only **231,745 messages received during
admission**, or **5,793.63/s**, with **24,255 remaining at admission end**.
Foreground end-to-end p99 was **3,774.41 ms**. All messages eventually completed;
all seven jobs of each maintenance class completed on time, with three positive
housekeeping jobs and two positive projection jobs. The separate overall
service-latency p99 was **7.58 ms**, while receipt-based end-to-end latency
includes the instrumented collection path. These different distributions
cannot be subtracted to assign the overhead precisely.

The existing policy uses this trace as a **correctness companion only**, and
the repeated low-overhead metrics trials for operational capacity. The trace
is not a passing throughput trial, and its verified serial witness does not
prove the unrecorded histories of the two 905-second runs. No acceptance limit,
checker budget or instrumentation policy was relaxed for this result.

## PostgreSQL comparison and qualification

The historical 640/s PostgreSQL endpoint used 16 workers. This fresh comparison
uses the same captured executable, 24 foreground workers, two maintenance
workers, workload, transaction settings and total resource budget as the
native trials. Prepared statements, serializable transactions, buffered writes
and the split candidate query remain unchanged.

Fresh 24-worker PostgreSQL screens now give these results. Completed rates
count foreground messages received while arrivals continue, excluding retries,
fork updates and maintenance transactions. All offered foreground messages
eventually completed in these screens.

| Screen | Offered/s | Duration | Completed/s during arrivals | Foreground p99 | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Burst | 640 | 120 s | 639.9917 | 32.454213 ms | Passed |
| Burst | 704 | 120 s | 703.9500 | 40.602304 ms | Passed |
| Burst | 768 | 120 s | 767.9250 | 50.785557 ms | Failed 50 ms p99 requirement |
| Maintenance | 704 | 40 s | 703.8000 | 49.552504 ms | Passed |

No maintenance jobs were scheduled in the burst screens. The maintenance
screen completed all seven jobs of each class on time, including three
housekeeping and two projection jobs with positive effects. The 768/s latency
failure is a completed policy failure, not a resource interruption or proof of
a repeatable capacity ceiling. These short screens do not establish a new
capacity comparison or speedup ratio.

The [first PostgreSQL 905-second trial at 704/s](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-long-r704-s29/cell.json.gz)
passed its operational requirements: 637,112 of 637,120 foreground messages
completed during arrivals, or **703.9912/s**, with **41.827283 ms p99**; all
messages completed after drain. All three housekeeping and three projection
jobs completed on time with positive effects. Sampled queue depth peaked at
83. Retry causes were 2,888,518 serialization failures during buffered writes,
131,325 at commit and three during queries. These retries do not increase the
completed-message count. Whole owned memory peaked at 6,604,128,256 bytes,
with no resource failure; table-and-index size grew from 61,202,432 to
132,890,624 bytes.

The [second 905-second trial](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-long-r704-s30/cell.json.gz)
also passed operationally: 637,113 messages completed during arrivals, or
**703.9923/s**, with **42.431067 ms p99**; all 637,120 completed after drain.
All three jobs of each maintenance class again had positive effects and
completed on time. Sampled queue depth peaked at 92; retry causes were
2,901,677 buffered-write serialization failures, 132,164 at commit and three
during queries. Whole owned memory peaked at 6,606,602,240 bytes, and
table-and-index size reached 133,111,808 bytes, with resource checks passing.

PostgreSQL's [matching full-history companion](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-full-r704-s29/cell.json.gz)
passed correctness and resource checks. It completed 28,160 foreground inputs
and 14 maintenance jobs; the oracle found a valid serial witness for all
29,463 transactions, exploring 29,463 states in 168.31 seconds. All seven jobs
of each class completed on time, with three positive housekeeping jobs and two
positive projection jobs. **Its instrumented foreground p99 was 60.860412 ms,
which fails the 50 ms performance requirement.** It received 28,154 foreground
messages during admission, or 703.85/s, and completed all inputs after drain.
As with AeroStore's instrumented companion, this trace qualifies correctness
only; the separate repeated metrics trials establish the operational result.

The [final PostgreSQL binding](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-capacity-accepted02/binding.json.gz)
and [assessment](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-capacity-accepted02/assessment.json.gz)
now accept the repeated 704/s lower bound. The earlier
[failed analysis receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-capacity-accepted01/binding.json.gz)
remains unchanged: an overbroad directory glob included a launch log, so its
inventory check failed before assessment. That was an analysis-tool failure,
not a database failure; the corrected helper used a fresh output directory and
preserved both analysis envelopes. The accepted endpoint ratio is **9.09×**,
with the scope stated above. The older 4.8× result remains specific to the
historical 16-worker configuration.

The [704/s burst](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-burst-r704-s29/cell.json.gz),
[768/s burst](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-burst-r768-s29/cell.json.gz)
and [704/s maintenance screen](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-maintenance-r704-s29/cell.json.gz)
retain their accounting, retry causes, resource results and original verdicts.

The original wrapper incorrectly demanded a completed maintenance sweep even
though this short screen scheduled zero jobs. Its [original incomplete assessment](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/evidence/pg24-burst-r640-s29/cell.json.gz)
remains unchanged; the [replacement assessment](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/pg24-burst-r640-s29-reclassified02.json)
audits the zero-job case, source and launch bindings, resource results and
message accounting before recording the screen pass. An earlier
[reclassification attempt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/pg24-burst-r640-s29-reclassified01.json)
rejected a redacted PostgreSQL command comparison and is also retained.
These were assessment-tool errors, not failed database executions. No workload
was rerun or original result rewritten to obtain the replacement assessment.

The [read-only baseline review](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/pg24-fairness-review.json)
also records limits of the existing PostgreSQL configuration. The fixture has
262,144 reserved record slots, including inactive records, for its 1,024 logical
identities. Historical PostgreSQL table-and-index size grew from 61.2 MB to
129.5–130.7 MB at 640/s and 135.8–136.2 MB at 704/s. Its shared-buffer setting
is 128 MiB (134.2 MB). These sizes do not establish cache residency or the hot
working set; operating-system caching also matters. Buffer misses and confirmed
I/O timing were not measured. The setting therefore cannot be assumed adequate
or assigned as the cause of the observed limit.

One historical 640/s run retained a 2.77 GB PostgreSQL error log and observed
about 11.0 GB of WAL-counter growth. These are investigation leads, not an
attributed performance profile. Fresh trials preserve the current settings so
their baseline remains interpretable. Any resulting ratio describes those
tested configurations; it does not establish a fully tuned PostgreSQL limit,
equivalent crash durability, or production HyperFeed performance.

The [completed-log review](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/pg24-logging-review.json)
found a concrete configuration difference worth testing after these baselines:
PostgreSQL records expected serialization retries with error details and long
SQL statement text, while the native conflict path counts and retries them
without corresponding per-conflict server text logs. The 704/s short trial
retained a 473,310,232-byte PostgreSQL log. That size covers the entire server
lifecycle; it is not measured admission-only I/O or evidence that logging
caused the capacity limit. A separate paired trial suppressing repeated SQL
statement text, while retaining errors and structured retry counters, would
measure this effect without changing transactions or workload. Keep the
current baseline configuration and all original logs intact; any improvement
would belong to a separately qualified PostgreSQL configuration.

## Reducing future qualification cost

The [reference-checker review](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/oracle-cost-review.json)
identifies a source-level explanation for expensive full-history checking:
ordinary messages replay three queries, and each reference query scans all
262,144 reserved rows. A straightforward replay of 256,000 inputs therefore
implies roughly 201 billion row visits, before maintenance and unsuccessful
search branches. This is a complexity estimate, not a measured CPU hotspot.
The completed check explored 257,320 states for 257,303 transactions, a ratio
of about 1.00007. That near-linear count does not suggest extensive backtracking
in this trace; it does not independently measure the CPU cost of row scans.
The [final observation](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/oracle-final-observation.json)
is retained separately from the original source review.

After capacity baselines, a useful verification task would be proving that an
independent faster reference-query implementation returns exactly the same
results as the simple scan, including pending writes, undo, empty matches and
ordered prefixes. Reusing the database's own index implementation would weaken
the independence of the oracle. Partitioning checks by worker or flight also
needs justification because matching and maintenance can cross those boundaries.
For now, short paired screens reject unsuccessful experiments; the expensive
full-history companion is reserved for a finalist and can support both sustained
seeds under the existing matching rules. No checker or proof policy changed in
this experiment.

## What this diagnostic measured

The [findings](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/findings.json) bind the
completed [analysis](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/diagnostic-01/analysis-01/summary.json.gz),
[control receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/diagnostic-01/control.json),
captured source and executable. The source is `a4e898c32e0b0722…`; the executable
is `62a98297d18e68f5…`. These local evidence paths are retained independently of
ordinary compiler intermediates.

The unchanged configuration uses the local Unix service, a memfd arena, 1,024
identities, the seven-fork profile, 16 foreground workers and two maintenance
workers, temporary signature affinity, and the existing ordered indexes and
maintenance selection. Timers are 300 seconds; their endpoint is excluded, so
this run exercised **no maintenance jobs**. Native WAL placement, asynchronous
acknowledgment and final drain are unchanged. This is not an equal-crash-durability
comparison with PostgreSQL. The preceding gain from a file-backed arena to memfd
was a storage-configuration result, not a database-algorithm change. The volatile
arena has different attachment, persistence and recovery requirements; writing
WAL to a file does not establish PostgreSQL-equivalent recovery semantics.

| Measurement | Observed value |
| --- | ---: |
| Foreground inputs offered and completed including drain | 1,920,000 |
| Completions received while arrivals continued | 1,919,960 |
| Completed messages per admission second | 6,399.87 |
| Foreground end-to-end p99, including retries | 23.31 ms |
| Arrival-queue p99 / service p99, separately calculated | 20.64 ms / 6.13 ms |
| Foreground worker occupied time | 92.36–95.21% |
| One-second arrival cohorts with p99 above 50 ms | 7 |

Each incoming message counts once. Fork writes, retries and background
transactions do not increase throughput. Occupied time includes RPC waits,
retries and backoff; it is not CPU utilization. Queue, service and end-to-end
quantiles are separate distributions and cannot be added or subtracted.

Profiling covered 280 seconds beginning approximately five seconds after
admission. It sampled user CPU at 49 Hz and read owned process/thread and
resource counters once per second. PID and thread start times, executable
identity, ownership and clock brackets bind the observations. Of 280 adjacent
counter intervals, 270 lay wholly in cohorts at or below the threshold, two
wholly in slow cohorts, and eight crossed boundaries. Mixed intervals remain
separate. The seven slow cohorts include startup outside profiling; the small
amount of unambiguously slow exposure cannot reliably explain rare bursts.

## What the CPU evidence supports

During the 270 intervals classified as healthy, the owned cgroup averaged
**14.80 CPU cores**, including **10.19 system CPU cores**, approximately **69%**
of its CPU time. Independently matched thread counters give 14.80 total and
10.22 system cores; these are separate measurements and are not added together.
Foreground worker threads averaged 6.53 cores and native service sessions
7.56. The coordinator main thread averaged **0.155 cores**, with its other
threads at 0.423 cores. This does not demonstrate a saturated coordinator main
thread, though brief receipt delays remain possible.

Within healthy foreground workers' **sampled user CPU**, approximately **82%**
of weighted stacks included `Client::call`, and **70%** included `model::put`.
These inclusive stack shares overlap. They exclude system CPU and blocked time
and are not percentages of total workload cost or predictions of obtainable
speedup. Approximately 98.6% of valid samples matched stable thread identities
outside read uncertainty; unresolved symbols and the remaining unmatched
samples stay explicit in the analysis.

The roughly 40-RPC message example discussed for this workload is a feature of
the **synthetic fixture**, not an observed real HyperFeed trace or a universal
per-message count. The fixture's `put` performs a read and write, and its
position, flight, projection and outbox updates produce repeated operations.
Actual HyperFeed made extensive use of prepared PostgreSQL statements. The
mapping from these primitive operations to a practical replacement interface
remains a calibration question; changing that mapping would need its own
comparison and cannot quietly become an engine performance improvement.

## Profile limits and resources

For the separate 300-second diagnostic capture, observer, workload execution,
storage and resource audits passed. That profile used metrics evidence and
did not itself receive full-history oracle verification or sustained capacity
qualification. Native arena high-water reached 73,345,200
bytes, and retired index postings drained to zero. Whole owned memory peaked
at 8,571,969,536 bytes, including workers, coordinator retention, file cache and
checking; it is not native arena retention. Owned swap peaked at zero, with no
owned OOM or memory-limit events.

The kernel reports `sched_schedstats=0`, yet recorded thread wait counters have
positive deltas. Their kernel accounting semantics have not been validated,
so the analyzer conservatively withholds interpreted wait seconds. Neither
absent scheduling delays nor disabled accounting is established. Cgroup
`io.stat` was unavailable in all 281 snapshots. Host faults and pressures cover
the whole guest and cannot be attributed wholesale to the database.

Over the capture, raw-monotonic elapsed time was **1.004702×** monotonic elapsed
time. Bracketed clock readings preserve this discrepancy; they do not provide
independent physical-time calibration. Throughput retains `CLOCK_MONOTONIC`
without correction, and clock behavior is not assigned as the cause of database
stalls. The [clock analysis](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/evidence/diagnostic-01/analysis-01/clock-analysis.json.gz)
retains the bounds and offset changes.

## Next experiments

Neither tested byte-write candidate is retained in the working service. The
accepted comparison supports continuing with the unchanged 24-worker
configuration. First run a short paired PostgreSQL configuration experiment
with `log_min_error_statement=panic`, retaining error records and structured
retry counters, to remove repeated statement-text logging from expected
serialization failures. The logging asymmetry is confirmed; a performance
benefit is not. Qualify any improved PostgreSQL configuration separately before
using it in a new 10× comparison.

Then increase native offered load using the unchanged 24-worker configuration
and profile the demonstrated limit. Its roughly 9 ms sustained p99 suggests
headroom, but no higher passing rate follows from that observation. Prefer
these measurements to speculative index or RPC changes. An independently
implemented, verified reference-query refinement could shorten the roughly
24-minute native correctness check while preserving the oracle's semantics;
it should not copy the production index or weaken the guard.

Any promising result must survive metrics-mode sustained runs, maintenance
coverage, repeated seeds and the matching correctness guard before capacity
promotion. Keep the
existing verification guardrails, 50 ms p99 requirement, 24 logical CPU budget,
36 GiB owned memory ceiling, 4 GiB host available-memory reserve, zero accepted
owned swap, and 30 GiB free-space reserves on both Linux and the Windows volume.
The accepted endpoints do not establish the 10× goal or a physical multi-machine
result. No further long trials are planned in this session; preserve and close
out the completed evidence before starting the next experiment.

## Evidence and resume checkpoint

Both accepted endpoints are complete. No campaign jobs remain running; the
independent closeout covers 31 resource envelopes, including the stopped failed
analysis attempt. The original captures, binaries, runtime dependencies, raw
histories and logs remain at their recorded local paths.

The [profile archive validation](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_queue_profile_2026-09-30/archive-validation.json)
and [results archive validation](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30-v2/archive-validation.json)
record compact-copy integrity. Together with the preserved
[incomplete-archive receipt](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/hyperfeed_socket_write_2026-09-30/archive-incomplete.json),
the committed archive payload is below 16 MiB. The incomplete payload remains
local at its original path and is excluded from Git. Large raw profile artifacts
received the explicitly recorded stat checks, not a fresh full-content hash
audit. No original files were deleted or compiler intermediates pruned; space
reclaimed is zero. No isolated Cargo build trees were created by this campaign.
The final archive check observed 451,548,889,088 free bytes in Linux and
169,348,173,824 on the Windows host volume. These are free-space observations,
not space returned to Windows.

On the originating workspace, recheck the preserved inputs without starting a
trial:

```sh
python3 -B target/hyperfeed-queue-profile-20260930-v1/select_final_archive_extras_v5.py --verify-selection target/hyperfeed-queue-profile-20260930-v1/archive-extra-selection01/selection.json
python3 -B target/hyperfeed-queue-profile-20260930-v1/compare_accepted_workers24.py --output target/hyperfeed-queue-profile-20260930-v1/workers24-accepted-comparison02.json
```

The second command creates a fresh comparison receipt; choose a new filename if
it already exists. Further trials require a fresh experiment directory and
resource admission. Preserve the current PostgreSQL configuration files and
capture: test a logging change through an explicitly recorded startup override,
with a matching control, rather than modifying files bound by these receipts.
Recheck Linux and Windows disk capacity, allocated retained evidence and available
memory before resuming. Keep the 30 GiB disk reserves, 4 GiB host-memory reserve,
36 GiB owned-memory ceiling and zero accepted owned swap. The next experiment is
the paired logging configuration screen described above; nothing is queued.
