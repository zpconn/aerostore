# HyperFeed contention investigation — 2026-09-24

This checkpoint adds a diagnostic workload; it does not promote an architecture or change production engine code. The baseline is `6291200b4383538d960f3b1c73942715193f464b`. The working engine, deterministic Extended Crucible, proof implementations and stable requirements are preserved. The new workload and worker-failure probes are separate additions. One existing asynchronous WAL test was repaired after the regression run exposed a scheduling-dependent durability assumption, as described below.

The required worker-failure behavior is that **other workers keep running**. The [failure study](../../worker_failure_contract.md) demonstrates current violations. Characterization tests passing does not satisfy that requirement.

The [runbook](../../contention_crucible.md) explains scenarios, exact successful-history checking, explicit inconclusive results, asynchronous WAL contracts and measurement limitations. Production API compatibility with proprietary HyperFeed handlers is not established.

## Findings

The family-scoped workload passes the bounded scenarios and short contention trials. The native global-time query variant exposes a substantial contention/progress problem: one trial exhausted a message's 128 retries. That failure is retained, including the partial history. No retry limit or engine behavior was changed to make the campaign pass.

The broad query changes both native predicate coverage and the physical scan. Operation-stage rejection counters cannot identify the exact low-level cause. This evidence warrants isolating predicate breadth, scan/materialization cost and historical-index rejection; it does not by itself justify replacing the index.

PostgreSQL's short trials encounter actual SQLSTATE `40P01` deadlocks. The recorded default deadlock-detection delay matters greatly to tail latency. The adapter issues handler writes as encountered, whereas AeroStore buffers writes and acquires native commit guards. Query plans, server round trips, write organization and crash-loss windows also differ. These results are not a speed ranking or a claim that PostgreSQL cannot support HyperFeed.

The two-minute native retention run is paced at a minimum 10 ms between each worker's message starts. Pacing is outside service latency. It validates a controlled-rate history, not saturation capacity. Its identities and source combinations are preseeded; it does not measure continuous new-flight creation, provenance expansion, or whole-family expiration. Memory counters measure arena allocation and retirement, not physical RSS; PostgreSQL relation bytes measure storage rather than RAM.

## Recorded results

Four worker processes, 16 initial logical identities, 80% hot-identity selection, 256 MiB native arena, three seeds. Workers ran under WSL2 on an Intel Core Ultra 9 285K; PostgreSQL ran in a disposable container through its published localhost port. [Environment details](environment.json) record CPU visibility and limits of the host comparison. Short unpaced trials admit new messages for five seconds; latency includes retries. The table reports medians of successful runs and explicitly retains failed trials.

| Configuration | Successful trials | Messages/s | Message p99 | Retries/message |
| --- | ---: | ---: | ---: | ---: |
| AeroStore, family-scoped queries | 3/3 | 10,835 | 1.91 ms | 0.90 |
| AeroStore, global-time queries | 2/3 | 489 | 115 ms | 5.67 |
| PostgreSQL, default 1 s deadlock detection | 3/3 | 21.6 | 2,038 ms | 2.84 |
| PostgreSQL, session-only 10 ms deadlock detection | 3/3 | 163 | 157 ms | 2.63 |

These are diagnostic measurements, with the instrumentation and transaction-organization limitations above. In particular, the failed global-time trial is not included in its latency/throughput median. The completion gap does not establish a fair architecture speed ratio. PostgreSQL's actual default and tuned settings are recorded in [default settings](postgres-external-settings.json) and [tuned settings](postgres-tuned-settings.json).

The global-time failure in seed `20260926` exhausted 128 retries for worker 0, message 1460. A separate fixed-count attempt (1,000 messages per worker) also failed on that query plan: worker 0, message 1764. The family-scoped fixed-count trial completed all 4,000 messages. Its shorter execution ended at the explicit cap, not the 60-second allowance. Failure histories are partial: killing the remaining process group can leave committed effects without received receipts. They are progress-failure diagnostics, not complete histories rejected by the serial oracle.

All six bounded scenarios (three per engine) had valid complete histories. The two-minute paced runs also had valid histories: 47,559 messages on AeroStore and 3,184 on PostgreSQL. AeroStore's arena high-water allocation went from 2.366 MiB to 2.679 MiB; all transaction registrations and index retirement backlogs were empty after drain. This does not measure RSS or prove indefinite memory stability. PostgreSQL could not keep up with the same maximum offered rate; closed-loop workers do not accumulate an external arrival queue.

The [primary campaign](campaign.json) and [sensitivity campaign](sensitivity.json) both correctly report `passed: false` because of the progress failures. Their source fingerprints stayed stable. The [summary](summary.json) retains separate failed cases instead of averaging them away.

## Evidence interpretation

The campaign records exact source hashes before and after the build/runs, the tested binary digest, compiler, commands, configuration, retry causes, per-kind useful-work counts, whole-message p99 including retries, retention samples and serial witnesses. Every successful transaction's complete queries, point reads, writes and outputs must fit a serial order respecting non-overlap, with exact final state and independent structural invariants. This is observational checking of successful transactions, not a proof of arbitrary histories or opacity of aborted attempts.

The baseline is unmodified production code plus an uncommitted harness. A Git revision alone does not identify the tested harness; use the source fingerprints. `architecture_promotion_eligible` and `whole_engine_verified` remain false. The overall campaign must remain failed when an individual progress check fails.

Full receipt recording, JSON IPC and evidence collection affect throughput. Service p99 includes attempts, retry backoff and recording, excludes pacing and inter-message IPC, and includes the deliberate first-query barrier in bounded creation cases. Timed trials run for five seconds of worker admission, finishing in-flight messages and draining receipts afterward. Completed message counts and per-worker prefixes differ across time-based runs; each committed history is checked independently. Seeds fix generation, not concurrent scheduling.

## Next decisions

1. Preserve this baseline and the negative evidence. Instrument or construct focused schedules that distinguish broad predicate conflicts from historical lookup rejection and scan cost before selecting an index change.
2. Evaluate PostgreSQL deadlock/query/write-order sensitivity before making performance comparisons. Keep durability differences explicit.
3. Prototype worker ownership/recovery against survivor progress, including kills after WAL acceptance and during partial publication. A separate database-owned mutation path is a candidate, not an established improvement. Never reset abandoned guards or registrations while survivors retain snapshots without a recovery protocol.
4. Keep the existing component verification gate and semantic regression checks. Defer expensive proofs of implementation choices this investigation may replace. P0 contract coverage remains complete; full P1 and whole-engine correctness remain open.
5. Before replacement-readiness conclusions, add sustained creation and whole-family churn alongside background work. Bounded competing creation and fixed-population retention cover different failure modes.

## Regression finding: asynchronous WAL loss

The first complete 71-check verification run failed its core-regression check in `async_wal_daemon_restart_does_not_persist_rolled_back_savepoint_intents`. That test killed an asynchronous WAL daemon immediately after three writes, then rewrote only two of those identities after restart. Its recovery assertion nevertheless required the third identity to survive. The declared acknowledgment contract promises enqueueing, so a daemon killed after dequeueing into its private buffer can lose that first wave.

The preserved failure WAL contains exactly three intact second-wave records with the correct committed values, and no rolled-back savepoint values. Independent typed decoding confirmed this. A deterministic negative control then held the WAL file lock, waited for dequeueing, and killed the daemon before append; the original recovery expectation failed at the same assertion.

The repair is confined to that test. It forces this permitted loss window, rewrites every expected identity after daemon replacement, and checks the exact record count, row identities, committed payloads and complete recovered state. The replacement records must recover from an empty row without relying on the lost first wave. The final test passed 21 focused repetitions. Production durability, WAL behavior and proof implementations were not changed.

The [guardrail evidence](guardrails/README.md) preserves the failed run and negative control separately from the post-repair run. The fresh component pilot completed all **71 checks successfully**, with stable source fingerprints, including Lean, Verus, TLA+, native implementation checks, core regressions and the existing Extended Crucible feature variants. Its anchoring remains `local_bootstrap_only`; `promotion_eligible`, `full_P1_complete` and `whole_engine_verified` remain false.

A passing repaired test establishes this particular restart/savepoint case; it does not establish crash consistency for arbitrary dependent asynchronous histories, safety after partial publication, or application-worker availability. Those remain separate obligations. Likewise, the passing component pilot does not override the two workload progress failures or the unmet survivor requirement.

## Archive boundaries

[The path map](archive-path-map.json) records hashes and maps original evidence paths to archived files. Reports are copied without rewriting their original paths. Initial/final images, serial witnesses, progress reports, all bounded histories and both failure histories are retained. Four large successful native histories remain locally in `target/contention-validation/` rather than adding more than 300 MiB to Git; the map records their locations, sizes and SHA-256 digests. Those four full histories cannot be rechecked from this archive alone.

[SHA256SUMS](SHA256SUMS) covers every file in this archive except the checksum file itself, including source bundles, drivers, summary, logs and guardrail evidence. Run `sha256sum --check SHA256SUMS` from this directory to verify the archived bytes.

[Measured harness source](measured-harness-source.tar.gz) contains the exact added/changed harness files used for the timed trials. Apply those files over baseline `6291200` to reconstruct that source; the campaign supplies hashes for all runtime inputs and the binary. The recorded drivers preserve the original checkout path and commands; use the portable commands in the runbook for a new checkout.

After measurement, the runner supervision and pacing helpers were extracted into a standalone module so their integration tests do not import and rerun unrelated legacy adapter fixtures. An initial debug test exposed a stack overflow in one such accidentally imported legacy fixture; the final test target isolates its own checks. No transaction, query, retry or pacing semantics were changed by that extraction. [The final harness source](final-harness-source.tar.gz) and [functional smoke receipt](final-smoke-receipt.json) describe the subsequent eight successful smoke cases, before the separate legacy WAL test repair. That repair changes neither the benchmark harness nor the engine. Verification receipts identify the later source containing the repaired test. The earlier timed measurements remain tied to their archived source rather than being relabeled as measurements of a different binary.

The original [baseline audit](baseline-audit.json) predates the WAL test repair. Use the later reviewed boundary audit in the guardrail archive for the final scope: unchanged runtime and proof/checker files, unchanged deterministic Extended Crucible, the new harness/tests, bench registration, and one repaired legacy test. The local reviewed boundary is not independent acceptance by the previous trusted baseline; the preserved old-baseline gate correctly rejects these changed inputs.
