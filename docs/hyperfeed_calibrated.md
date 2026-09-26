# HyperFeed cadence and ordering profile

`--workload calibrated` implements two workload facts supplied by HyperFeed's architect: projection and housekeeping each normally run every 5–10 minutes, and messages for one flight are normally in order. It complements the existing `fleet`, `lifecycle` and deterministic conflict scenarios. This is partial calibration of a synthetic workload, not a reconstruction of proprietary HyperFeed handlers or a production trace.

The subsequent [operator calibration](hyperfeed_workload_calibration.md) adds roughly 100 single-machine workers, 200–300 MMHF workers, 70,000–100,000 flights per day and 3–8 provenance forks per flight. These operating points exceed the present validation scale. The architect confirmed temporary signature affinity was retained in MMHF; this profile's permanent per-flight assignment remains a comparison control. The ledger records the remaining implementation requirements.

## What runs

The foreground stream has a fixed offered rate independent of completion. The default `--dispatch identity` routes each logical flight to one foreground worker, preserving its input and processing order through retries. The optional [temporary signature-affinity dispatcher](hyperfeed_affinity.md) instead uses visible input identifiers and an explicit TTL, allowing aliases and expiry to send the same flight to different workers. Both policies discover reads and writes through queries. Permanent identity routing remains a comparison control, not an inferred production processing guarantee.

`--workers` counts foreground workers. Two additional processes run projection and housekeeping, identically for AeroStore, the interactive service and PostgreSQL. Both timers use real elapsed seconds and admit their first job after one complete interval. A job exactly at the admission endpoint is excluded. Every admitted message and timer job must finish; excess backlog, retry exhaustion or truncated work fails execution. Queued timer jobs are retained instead of coalescing or skipping them.

The defaults are `--projection-interval-seconds 300` and `--housekeeping-interval-seconds 600`. Either can be set within the operator's 300–600-second range. Shorter intervals support fast functional tests and are explicitly accelerated. Changing the foreground offered rate does not change timer deadlines or counts.

The current foreground mix is synthetic: one plan followed by 15 position messages per flight, with three sources cycling across its messages. Roughly a quarter of the configured families are quiet (rounded down, at least one), allowing projections to observe flights whose scheduled events are no longer continually refreshed by incoming updates. All families are populated before admission. Quiet families have seven retained historical positions and one recent position per view. The default batch control keeps its original old positions beyond the synthetic one-hour retention cutoff. Optional sweep mode spreads those historical positions across up to three early expiry deadlines, as described in the [maintenance guide](hyperfeed_maintenance.md). This deliberately gives initial maintenance useful work, rather than measuring only empty queries. Quiet share, message mix, population, ages and history lengths remain assumptions requiring calibration.

Event times and event deadlines use nanoseconds so multiple ordered messages within a second are not discarded as stale; scheduled-flight identity times retain their existing seconds unit. A delayed timer job uses its original scheduled timestamp and cutoff, not its eventual execution time. This fixed-input policy preserves the corpus across engines; it does not assert HyperFeed's production catch-up policy. Existing workload messages retain their previous units and serialized input bytes.

The default `--maintenance-mode batch` executes **one bounded transaction per tick**, observing the complete global query result before selecting up to four projection events or 32 housekeeping records. Optional `--maintenance-mode sweep` instead executes successive batch transactions at the same cutoff until a committed complete query is empty. Batch sizes and the per-job transaction cap are configurable. The [maintenance guide](hyperfeed_maintenance.md) defines completion, partial failure and the different seed ages used in each mode; comparing modes does not isolate only transaction-loop cost. Population remains fixed, and the existing stress profiles continue to exercise lifecycle turnover, competing creation and reordered traffic. A calibrated lifecycle and sustained, resource-matched validation remain prerequisites for claiming representative capacity.

## Inspecting results

Reports separate foreground, projection, housekeeping and combined maintenance counts, retries, queue delay and end-to-end p99. Zero-job classes have null latency, rather than invented zero-latency samples. Foreground offered rate excludes timer jobs; aggregate completed messages include completed jobs once each. Sweep reports separately count committed transactions, including terminal empty probes, so extra batches do not inflate message throughput. Maintenance p99 covers the whole job, including queueing and retries. The continuous throughput denominator still covers admission through worker shutdown and confirmed drain.

The coordinator reconstructs each worker's expected stream. Identity mode requires consecutive per-flight ordinals and nonoverlapping execution intervals; affinity mode records same-flight inversions and overlap as diagnostics while still requiring exact dispatch, worker FIFO and a valid full history. An offered-schedule file remains available when a run fails. Worker activity records occupied wall time, including retries and backoff; it is not CPU usage. Observed execution overlap does not by itself prove particular queries overlapped.

Full evidence checks every successful transaction against the independent serial-history oracle. Metrics evidence omits operation histories and remains explicitly unverified, even when paired with a full run. The qualification driver independently checks expected counts, timer deadlines/configuration, class accounting and ordering evidence. It distinguishes successful execution, useful foreground work, diagnostic latency budgets and observed maintenance cadence. Neither a short smoke nor this fixed-population profile can qualify a capacity bound or a 10× replacement claim. Complete batched sweeps extend coverage without supplying missing population turnover, worker-scale or availability evidence.

## Validation checkpoint

The [2026-09-25 evidence archive](bench_data/calibrated_2026-09-25/README.md) contains nine accelerated full-history trials and nine matching metrics trials across direct AeroStore, the Unix service and PostgreSQL, plus TCP loopback. Two 601-second runs, one per direct engine, each completed 19,232 useful foreground messages and two jobs of each maintenance kind due at 300 and 600 seconds after admission began. Both full histories passed. The timer intervals in these runs were 300 seconds for each maintenance kind; the default 300/600-second configuration is covered by schedule tests, not a separate 1201-second run.

The runs overlapped other local validation, so their timing values are diagnostic rather than a controlled engine comparison. The archive also preserves pause/backlog negative controls, model and gate tests, the existing stress scenarios and verification guardrails. Production engine code remains unchanged. See the archive for exact sources, build hashes, contracts, cleanup and claim limits.

The subsequent [affinity checkpoint](bench_data/affinity_2026-09-25/README.md) validates temporary signature routing, its identity control and stale-update accounting, and retains higher-load progress failures. It extends routing coverage without changing the scope of the earlier cadence evidence. Optional [complete maintenance jobs](hyperfeed_maintenance.md) are the next implementation increment; their validation receipts belong in the separate [maintenance archive](bench_data/maintenance_2026-09-25/README.md).

## Commands

Build and select the exact executable as described in the [two-host runbook](hyperfeed_two_host.md). Set `BENCH_BINARY` to that executable and `AEROSTORE_CONTENTION_PG_URL` to a running, explicitly managed disposable PostgreSQL database. These commands execute sequential local comparisons; use new output paths for each campaign.

```bash
# Accelerated functional companion: not a five-minute cadence measurement.
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/calibrated-full --engines aerostore,service-unix,postgres \
  --workload calibrated --families 16 --hot-percent 0 --workers 4 \
  --rates 64 --seeds 20260924,20260925,20260926 --seconds 5 \
  --projection-interval-seconds 1 --housekeeping-interval-seconds 2 \
  --slo-ms 50 --evidence full
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/calibrated-metrics --engines aerostore,service-unix,postgres \
  --workload calibrated --families 16 --hot-percent 0 --workers 4 \
  --rates 64 --seeds 20260924,20260925,20260926 --seconds 5 \
  --projection-interval-seconds 1 --housekeeping-interval-seconds 2 \
  --slo-ms 50 --evidence metrics \
  --correctness-report target/calibrated-full/campaign.json
```

Fifty milliseconds is a declared experimental budget, not a known HyperFeed SLA. Use whole-message latency including queueing and retries when interpreting it. A fixed, low offered rate tests behavior at that rate; it does not measure either engine's throughput ceiling.

For real timer coverage, use intervals of 300 or 600 seconds and a duration strictly longer than twice the larger interval. For example, a 601-second admission with both intervals at 300 admits two jobs of each kind; a 1201-second admission with the default 300/600 intervals admits four projection jobs and two housekeeping jobs. Positive effects, foreground/background execution overlap and memory retention must be inspected rather than inferred from duration. These commands retain the default batch control. Select sweep mode and its explicit batch policy for complete jobs; both profiles remain diagnostic while calibrated turnover and sustained resource/availability qualification are absent.

The remote helper accepts the same profile and interval options. First validate its TCP loopback path, then use the [matched two-host procedure](hyperfeed_two_host.md) with the same profile, intervals, foreground rate, duration, population and seed on both engines. Its whole-run deadline is capped at 3600 seconds and must exceed admission duration. No physical MMHF result follows from loopback.
