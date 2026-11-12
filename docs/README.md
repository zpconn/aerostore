# Aerostore documentation

The documents fall into four groups:

- **Guides** describe the engine as it is now.
- **Contracts** state guarantees and their limits.
- **Experiment reports** are dated records of what was tried and what was found.
- **Operations** documents cover CI, evidence and disk usage.

Reports are kept as written. A later report supersedes an earlier one instead of
editing it, so read the newest report on a topic first.

## Start here

| Document | Contents |
| --- | --- |
| [Project README](../README.md) | What Aerostore is, headline results, quick start |
| [Architecture](architecture.md) | Storage, transactions, indexes, durability, reclamation and the service mode; why the design is fast |
| [Bugs found](bugs-found.md) | Defects the tests, models, proofs and benchmark oracles caught, and how each was repaired |
| [Glossary](glossary.md) | Terms used across the code, reports and verification ledger |

## Using the engine

| Document | Contents |
| --- | --- |
| [Transactional indexes](transactional_indexes.md) | Rust API for bound secondary indexes, transactional lookups, the commit protocol and retry rules |
| [Tcl examples](../aerostore_tcl/test.tcl) | `FlightState` ingest and search through the Tcl extension |
| [Durability contract](../verification/contracts/durability.md) | WAL acceptance, checkpoint cuts, upgrade rules and open obligations |
| [Worker failure contract](worker_failure_contract.md) | What happens when a worker dies, measured in direct mode and in the database-owned service |
| [Nightly performance runbook](nightly_perf.md) | Focused and stress suites, Loom checks and Crucible options |

## Benchmarks and workloads

| Document | Contents |
| --- | --- |
| [Queue and worker-count profile](hyperfeed_queue_profile.md) | **Current headline:** 6,400 vs 704 messages/s with 24 workers (9.09×), and the rejected socket-write candidates |
| [Sustained capacity](hyperfeed_sustained_capacity.md) | Earlier accepted result: 3,072 vs 640 messages/s with 16 workers (4.8×) |
| [Architecture qualification](hyperfeed_qualification.md) | The qualification harness: engines, evidence modes, gates and PostgreSQL adapter |
| [Fast iteration](hyperfeed_iteration.md) | Paired baseline/candidate screens used to choose experiments before sustained trials |
| [Extended Crucible](extended_crucible.md) | Message transactions replayed against a serial reference model and PostgreSQL |
| [Contention Crucible](contention_crucible.md) | Query-discovered writes under contention, checked by a serial-witness oracle |
| [Research basis](extended_crucible_research.md) | What the public HyperFeed material establishes and what the simulator assumes |
| [Workload calibration](hyperfeed_workload_calibration.md) | Operating guidance behind the calibrated workload, kept separate from assumptions |
| [Cadence and ordering profile](hyperfeed_calibrated.md) | The `calibrated` workload: per-flight ordering and maintenance timers |
| [Signature affinity](hyperfeed_affinity.md) | Temporary `(callsign, registration)` dispatch affinity |
| [Maintenance sweeps](hyperfeed_maintenance.md) | Projection and housekeeping as successive batch transactions |
| [Rolling lifecycle](hyperfeed_rolling.md) | A population that starts empty and turns over continuously |
| [Two-host commands](hyperfeed_two_host.md) | Preparing a separate worker-host / database-host comparison |
| [Evidence catalog](../evidence/README.md) | Where campaign evidence lives, how to fetch it and how hashes are checked |

## Verification

| Document | Contents |
| --- | --- |
| [Verification workspace](../verification/README.md) | What is proved, model-checked and tested; setup and the experiment gate |
| [Formal verification plan](formal_verification_plan.md) | The full roadmap (P0–P6) and the experiment workflow |
| [Claim ledger](../verification/claims.toml) and [assumptions](../verification/assumptions.toml) | Machine-readable scope of every claim and every trusted assumption |
| [Production-lock models](../verification/contracts/lock_models.md) | The seven Loom cases and their negative control |
| [Sustained churn correctness](sustained_churn_correctness.md) | Structural corruption found under sustained load, and the ownership rules that fixed it |
| [CI and proof-input review](ci.md) | What CI and Verify run, and how proof-input changes are reviewed |

## Experiment reports

Dated records, oldest first. "Superseded" means a later report replaces the
conclusion; the record itself remains accurate for its date.

| Date | Report | Question and finding |
| --- | --- | --- |
| 2026-03-03 | [Crucible 10× attempt log](crucible_10x_attempt_log_2026-03-03.md) | Early 60-second Crucible tuning toward 10×. Historical; later sustained runs measured about 6.1×. |
| 2026-03-03 | [Allocator telemetry](crucible_allocator_telemetry_2026-03-03.md) | Was long-run decay a leak? No: it was allocator reuse failure under churn. |
| 2026-09-26 | [Retry diagnostics](hyperfeed_retry_diagnostics.md) | Attributes native retries and compares expiry-index policies. |
| 2026-09-27 | [Ordered due range](hyperfeed_ordered_range.md) | Do unrelated future schedule updates cause avoidable projection retries? An optional ordered policy improved latency; expiry and stale-work limits remained. |
| 2026-09-27 | [Dependency capture prefix](hyperfeed_capture_prefix.md) | Searching only the pre-query dependency prefix cut fresh broad lookup time by about 97% in microbenchmarks. Adopted, with a proof of the skipped suffix. |
| 2026-09-27 | [Rolling findings](hyperfeed_rolling_findings.md) | A rolling population exposed housekeeping retry exhaustion. Superseded by the expiry-range reports. |
| 2026-09-27 | [Capacity resources](hyperfeed_capacity_resources.md) | Harness and engine limits before scaling worker counts. |
| 2026-09-28 | [Expiry range](hyperfeed_expiry_range.md) | Narrower expiry dependencies reduce housekeeping retries. Superseded by the resumption report. |
| 2026-09-28 | [Expiry resumption](hyperfeed_expiry_resume.md) | Completed the ordered-expiry pairs; an early `ANALYZE` produced a working PostgreSQL control. |
| 2026-09-28 | [PostgreSQL statistics](hyperfeed_statistics.md) | Makes the statistics treatment reproducible; PostgreSQL still exhausts retries at higher load. |
| 2026-09-28 | [PostgreSQL candidate queries](hyperfeed_candidate_queries.md) | Splitting the match query fixes a PostgreSQL access path but not the housekeeping failure. |
| 2026-09-28 | [Ordered maintenance batches](hyperfeed_maintenance_prefix.md) | An ordered-prefix query contract lets both engines complete maintenance at 512 inputs/s. |
| 2026-09-28 | [Population scaling](hyperfeed_population_scaling.md) | Larger synthetic populations and rates, before any native index change. |
| 2026-09-28 | [Paused capacity campaign](sustained_capacity_pause_checkpoint_2026-09-28.md) | Resume checkpoint for an interrupted sustained campaign. |
| 2026-09-29 | [Sustained capacity](hyperfeed_sustained_capacity.md) | First accepted capacity comparison: 3,072 vs 640 messages/s. Superseded for the headline by the queue profile. |
| 2026-09-29 | [Frame writing](hyperfeed_frame_write_experiment.md) | Two frame-coalescing candidates; neither was promoted. |
| 2026-09-29 | [Burst profile](hyperfeed_burst_profile.md) | Recurring service CPU bursts concentrate in predicate-lock acquisition. |
| 2026-09-29 | [Predicate-lock diagnostics](hyperfeed_predicate_lock_experiment.md) | The bursts come from due-index commit locks; a yield-cadence screen is recorded. |
| 2026-09-29 | [Commit phases and arena backing](hyperfeed_commit_phases.md) | A memory-backed arena cut screened p99 by 40–53% at the same throughput. |
| 2026-09-29 | [Arena capacity](hyperfeed_arena_capacity.md) | The first 6,400/s sustained trial missed p99 at 50.92 ms. Superseded by the queue profile. |
| 2026-09-30 | [Queue and worker-count profile](hyperfeed_queue_profile.md) | Socket-write candidates were rejected; 24 workers qualified 6,400 vs 704 messages/s. **Current.** |

## Project operations

| Document | Contents |
| --- | --- |
| [Contributing](../CONTRIBUTING.md) | Build, test, review rules and writing style |
| [Agent operating manual](../AGENTS.md) | Rules every coding agent follows in this repository |
| [Disk-space runbook](disk-space.md) | Budgeting builds and campaigns; what may and may not be cleaned |
| [Overhaul plan](../OVERHAUL_PLAN.md) | The plan for this repository's relaunch, with its execution log |
| [Checkpoint 2](overhaul_checkpoint_2.md) | The approved history rewrite and archive migration |
| [History rewrite tooling](../tools/history/README.md) | The scripted, re-runnable rewrite and its commit map |
| [Previous README (2026-09-22)](archive/README-2026-09-22.md) | The March 2026 design notes and benchmark tables, kept for reference |
