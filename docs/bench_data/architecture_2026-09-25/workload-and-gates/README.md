# Workload and qualification checks

This directory records model and harness checks for the architecture investigation. These checks establish properties of the synthetic workload and its evidence collector. They do not establish production HyperFeed compatibility, a verified whole database, or an architectural performance winner.

**Timing correction:** the concentrated stress and `f37b288b…` fleet campaigns below predate continuous drain timing. Their throughput denominator omitted the interval used to flush the receipt file and stop workers before the separately timed drain. Their raw reports and historical gate output are preserved, but their capacity qualification is withdrawn. The corrected gate requires one continuous admission-to-confirmed-drain interval and rejects these older reports for both passing and failing capacity bounds. Their recorded p99, retry counts, outcomes, retention observations, and serial-history witnesses remain evidence of those executions.

The model and gate source files are [model.rs](../../../../aerostore_core/benches/contention_crucible/model.rs), [storage.rs](../../../../aerostore_core/benches/contention_crucible/storage.rs), [oracle.rs](../../../../aerostore_core/benches/contention_crucible/oracle.rs), and [qualify_hyperfeed.py](../../../../scripts/qualify_hyperfeed.py). The existing deterministic Extended Crucible model and storage interface remain separate.

## Workload semantics

The contention storage contract adds two complete cross-family predicates: every due scheduled event and every expired position, output, or deduplication record. Business handlers observe the complete result before selecting a deterministic batch. Global projection, cancellation, and rescheduling select at most 16 events; global housekeeping selects at most 64 records. Whole-family expiry discovers its rows through queries and deletes at most the 128 records reserved for that family. Query results themselves are never truncated to the batch limit.

Five bounded scenarios cover competing creation after empty searches; mixed matching, projection, and housekeeping; indexed key movement; cross-family due-event claim/cancel/reschedule; and creation competing with whole-family expiry. The original three contention scenarios remain included. Every successful recorded transaction includes its complete queries, point reads, writes, and outcome. The unchanged oracle searches for a serial order respecting nonoverlapping successful-attempt intervals. Commit-response order is only a search heuristic. Budget exhaustion remains inconclusive, never a pass.

The `lifecycle` stream supplies a fixed corpus determined by global sequence, seed, family count, and hot-identity percentage. Message content is independent of worker count. Each group of 16 messages offers creation, provenance growth, real positions, global background work, arrivals, and whole-family expiry. Two physical allocations per selected identity are reused. Creation may defer while its reserved allocation is occupied; delayed observations may return a missing-family outcome. These outcomes are counted and must not be treated as equivalent to useful updates.

Retention is an explicit synthetic event-time policy: terminal views become eligible after five seconds, while a family with no view activity for 600 seconds may expire even if it never received a terminal event. The longer cutoff prevents a creation that ran after all of its arrival messages from permanently pinning an allocation. Every view must satisfy the applicable cutoff; one fresh view protects the entire family. Complete transactional family reads protect races with late activity. These retention constants are test assumptions, not a claim about FlightAware's production policy.

Full and record-free modes execute the same business code. Record-free mode omits operation transcript allocation and cloning, while retaining outcomes and query normalization. Its empty operation histories cannot establish a serial-history witness. A full-history companion never retroactively verifies the unrecorded execution.

## Recorded checks

| Check | Recorded result | Evidence |
| --- | --- | --- |
| Pinned Rust model tests | 40 passed | [command and retained result excerpt](model-tests.log) |
| Python qualification gate tests, including scoped timeout cleanup | 19 passed | [command and retained result excerpt](qualification-unit-tests.log) |
| Real-binary arrival/evidence regressions | 4 passed on the repaired frozen binary | [complete test output](contention-integration-tests.log) |

The model tests include independent expected cross-family results, rejection of an omitted candidate outside the selected write batch, rejection of incompatible simultaneous claims, both valid late-update/expiry orders, stale-claim rejection, strict age boundaries, delayed-creation recovery, safe reuse, per-transaction write limits, and full/record-free effect equality. They also include the existing deterministic model's 15 tests.

The four executable regressions check that one arrival per second still waits through the full one-second admission interval; metrics mode explicitly leaves history verification false; full and metrics modes process the same 32-message single-worker corpus with equal outcomes and final rows; a message cap cannot truncate offered inputs successfully; and injected per-operation service delay fails on bounded backlog rather than silently reducing arrivals. The first of these checks also validates scheduled, service, and coordinator-receipt timing relationships. The corpus comparison and cap/backlog checks are separate tests, for four tests total.

The integration suite was rerun after the service reply-deadline repair. The repaired frozen binary SHA-256 is `af39521c16db6244b7a8772a314d8df9a8554128b33a555f336b45f11b784e1f`. Its [build provenance](build-provenance.json) records the locked offline Cargo command, pinned compiler, exact Cargo artifact, and 478 matching source hashes before and after the build; the aggregate source hash is `6e955013e6539022447dd3158843f955e58f4c3bb18b00b77a35ce6005369eb6`. The repaired four-test run completed in 3.978 seconds. The model/unit excerpt files retain observed command/result lines rather than complete raw build logs; the integration file contains the captured repaired test output. The [source note](source-note.json) records the workload and gate files separately.

The [earlier build provenance](pre-reply-deadline-build-provenance.json) and [earlier integration log](pre-reply-deadline-integration-tests.log) remain archived as superseded evidence. They predate the service repair and are not used to qualify the repaired campaign.

The integration command was:

```sh
AEROSTORE_CONTENTION_BINARY=/home/zpconn/code/aerostore/target/release/deps/hyperfeed_contention_crucible-2e74481266f642b5 \
  python3 -m unittest discover -s scripts -p test_contention_integration.py -v
```

It requires permission to create local Unix sockets. These are correctness regressions, separate from the timed comparison campaign.

The [independent full-campaign review](full-campaign-review.json) records all 48 final full-history cells: 42 Valid histories, six service backlog failures, and no Invalid or Inconclusive oracle verdict. The [metrics-campaign review](metrics-campaign-review.json) records another 48 cells on identical source and binary bytes: 42 completed executions explicitly marked `NotCheckedMetricsOnly`, with exact full-history companions, and six retained service backlog failures. There are 42 verified recorded histories across the two campaigns, not 84. Each campaign's historical gate reported 33 capacity-policy passes and nine completed policy failures; that capacity output is superseded by the continuous-timing requirement above.

A Valid history can still fail the performance policy. At 1,000 offered messages/s, four-worker direct native full-history runs have 35.6–36.5% allowed missing/deferred outcomes and substantially higher latency than the one-worker runs. The behavior persists with operation recording disabled:

| Metrics-mode cell, three seeds | End-to-end p99, including queue/retries | Missing/deferred inputs | Retries per 5,000 inputs |
| --- | --- | --- | --- |
| Direct native, one worker, 1,000/s | 4.23–4.38 ms | 0% | 0 |
| Direct native, four workers, 1,000/s | 544.84–980.97 ms | 36.24–37.66% | 10,624–11,173 |
| PostgreSQL, one worker, 1,000/s | 2.10–2.13 ms | 0% | 0 |
| PostgreSQL, four workers, 1,000/s | 3.94–5.11 ms | 4.26–6.36% | 488–821 |

Missing/deferred outcomes are permitted application-model results, not evidence of lost committed rows. Their difference prevents equal-useful-work speed claims between these four-worker cells. One-worker direct native and PostgreSQL completed at the highest tested offered rate, 1,000/s; the interactive service variants completed at 100/s and have retained backlog, latency, or useful-work failures at 1,000/s. These observations do not qualify capacity under the corrected timing gate or isolate the cost of database ownership from the prototype's per-operation serialization/transport costs.

The next contention experiment should distinguish genuine predicate/read-row overlap, provably unrelated invalidation, historical-index rejection, and unresolved causes. Complete global queries can create genuine cross-family conflicts. Controlled false-positive examples and retry-stage counters establish possible mechanisms; they do not establish which mechanism dominates these workload measurements. Global task cadence, allowed message reordering, retention cutoffs, and other domain choices still require HyperFeed calibration. These five-second cells also do not establish long-run memory stability; retention experiments are separate evidence.

## Qualification rules

The driver runs every declared engine/rate/worker/seed cell and retains unsuccessful trials. Each cell starts a fresh benchmark process. Actual working source and binary bytes are hashed before and after the campaign and each trial, including dirty and untracked workload/proof inputs. A change invalidates the gate. Current compiler and host/resource metadata are recorded; a supplied binary's build-to-source/compiler relationship is not attested by the driver.

Capacity trials require a complete fixed lifecycle corpus, exact per-worker and per-kind completion counts, full admission duration, successful structural checks, end-to-end p99 within the declared synthetic SLO, and a declared minimum completion-rate fraction using continuous admission-to-confirmed-drain time. The gate checks ordered admission, workload-completion, worker-stop, and drain-confirmation timestamps on the client monotonic clock, then verifies both reported durations and the throughput denominator. Missing/deferred outcomes are bounded, and creation plus expiry turnover must occur. Exact full-history companions are required for metrics campaigns to qualify tested capacities. Companion identity includes source bytes, binary bytes, engine settings, worker count, seed, offered rate, duration, and workload configuration. The metrics run's own history-verification flag stays false.

At least three declared seeds must all pass for a tested capacity to qualify. Worker counts are selected within the declared grid, preferring fewer workers at the same rate. CPU/resource limits are declarations; the driver does not enforce CPU affinity or prove equal PostgreSQL/native resource allocation. A PostgreSQL saturation point requires valid, useful-work-comparable trials that fail the latency or drained-rate requirement for every declared worker choice and seed. Process errors, progress failures, missing trials, bad histories, and no-op collapse cannot establish that upper point.

Ratios of passing rates are ratios of lower bounds. A separately labeled conditional tested-grid ratio additionally requires a PostgreSQL saturation bracket and comparable useful work, including at the higher-rate endpoint. A strict conditional 10× grid flag requires zero outcome tolerance. Demonstrated synthetic capacity, real-world 10× performance, hardware equivalence, saturation monotonicity, whole-engine verification, replacement qualification, and architecture promotion all remain false. Compatibility, recovery, worker-failure availability, real retention requirements, remote-worker behavior, and broader hardware measurements remain separate evidence obligations.

On timeout the driver terminates the captured owned process tree and verifies no owned process remains live before removing credential-bearing configurations. Cleanup requires the exact token-marked trial directory, removes only `private-config.json` and `worker-[0-9]+.json`, and does not follow directory symlinks. Histories, reports, and similarly named files remain. Public logs/reports redact the supplied PostgreSQL URL and parsed password; archives also need to omit private configuration files defensively.

## Fleet-profile follow-up

The campaign and build hashes above describe the original `lifecycle` stress corpus. A later workload review found that its serial execution creates and expires one family before beginning the next. Its configured identity count therefore does not establish a simultaneously live fleet, and two global background kinds often operate on empty results. Those observations limit its value as a HyperFeed workload comparison; the retained measurements remain contention and lifecycle stress evidence.

The separate `fleet` profile preserves the original lifecycle corpus and interleaves its sixteen phases across at least sixteen configured identities. It begins with the exact serial prefix of sixteen warmup rounds, then continues that same stream. Each round visits every identity once; triangular rotation changes the mapping of phases to workers while keeping message content independent of worker count. This profile requires `--hot-percent 0`. Generator background and expiry windows scale with identity count; ordinary event scheduling retains the shared handler's thirty-second rule.

The expanded model suite passed **45 tests** and the expanded qualification suite passed **22 tests**. The [observed model result excerpt](fleet-model-and-gate-tests.log), [complete qualification test output](fleet-qualification-unit-tests.log), and [post-test source hashes](fleet-source-note.json) are separate from the earlier stress build. A serialization golden over 1,024 lifecycle messages verifies that adding the fleet profile did not change that original corpus. The new tests also compare warmup with the exact preceding stream, check creation and every message kind reach every worker for the tested 4/8/16/32-worker configurations, compare recorded and record-free effects, and reject omission of a global-query candidate outside the selected write batch.

Serial model checks across sixty-four timed rounds measured:

| Configured identities | Minimum live families observed | Largest complete global due query | Families in largest due query | Successful family expirations |
| --- | --- | --- | --- | --- |
| 16 | 11 | 34 rows | 11 | 63 |
| 32 | 21 | 58 rows | 21 | 128 |

All four global background kinds performed mutations, multiple families were affected, physical family allocations were reused with different identities, and every transaction stayed within 128 writes. These observations concern the tested serial prefixes. They do not establish a minimum live population during concurrent runs.

Fleet capacity acceptance additionally requires at least `16 * families` offered messages, initial and final live-family snapshots of at least half the configured identities, and positive effects from all four global kinds: projection, rescheduling, cancellation, and housekeeping. A valid serial history with collapsed population or absent background mutations remains a valid execution but cannot qualify a capacity or establish saturation. Initial/final population snapshots are reported separately from configured identities; they are not runtime minimum measurements. Subsequent fleet benchmark build and campaign provenance must be associated with the new source, rather than the pre-fleet build recorded above.


An earlier fleet binary passed all **six real-binary integration tests** ([pre-final output](fleet-pre-final-integration-tests.log), 7.308 seconds). The added tests require a populated initial/final fleet, positive effects from all four global kinds, a complete global-due result exceeding sixteen rows across at least eight families, and rejection of a silently ignored hotspot setting. This run preceded the final build and does **not** attest the later `f37b288b…` binary. The [final fleet build provenance](fleet-build-provenance.json) separately records source SHA-256 `7b02ad0fd7c2e1a04db8603c4321015c10c4d82ee3bfaa2cfd6adda457558d87` and binary SHA-256 `f37b288b38be0b86a29f1db32079f5819c4fe28220937219f41260c9c8549033`, which identify the timed fleet campaigns. A later rerun against that exact binary supplies the final integration evidence below; the earlier receipt remains distinct rather than being reassigned to a newer build.

The [final paired fleet campaign review](fleet-campaign-review.md) retains all 72 cells. Its full campaign has 33 Valid histories and three backlog failures; its metrics campaign has 31 completed, unverified executions and five retained backlog/progress failures. It distinguishes useful-work changes from invalid histories, records worker-count sensitivity and query amplification, and identifies the unequal expiry-index schemas as a control to measure before choosing an engine change.

The separate fleet retention campaign adds three Valid 120-second histories, each completing 12,000 inputs with identical outcomes and zero retries. The [retention policy review](fleet-retention-campaign-review.json) preserves its historical three individual policy passes and single-seed limitation. Its capacity timing is also superseded; the campaign does not independently qualify capacity or prove long-run memory bounds. The [detailed retention analysis](../fleet-retention/README.md) confirms that these executions had no overlapping messages despite four configured workers.


After all timed campaigns, the exact final `f37b288b…` binary passed the **six integration tests again**, in 7.351 seconds ([complete final log](fleet-final-integration-tests.log), [hash receipt](fleet-final-integration-receipt.json)). The receipt records the matching `7b02ad0f…` source, source stability, binary hash, command, successful exit, and captured log hash. Its log SHA-256 is `b1e951b95cc2ad4b4af8ce215d58713c4ce93bc8ac35e8b054e4b2bea9170ec4`. This exact-binary result is separate from the preserved pre-final six-test run.

## Continuous-timing gate correction

The corrected qualification suite passes **26 tests** ([complete output](continuous-timing-qualification-tests.log), [gate/test source hashes](continuous-timing-gate-source-note.json)). New negative cases reject missing, misordered, or inconsistent timestamp evidence; reject an inflated throughput scalar; preserve history validity for older reports while withdrawing capacity bounds; and show that a five-second worker-shutdown delay belongs in the denominator. A complete three-seed legacy grid can supply neither a passing lower bound nor a saturation upper bound. Corrected runner builds and newly measured campaigns require their own provenance; they do not replace or rewrite the older raw reports.
