# Rolling flight lifecycle experiment

The calibrated Crucible has an optional rolling population. It starts empty,
creates flights through empty candidate searches, grows their provenance views,
processes positions, marks arrivals, and retires old families through normal
transactions. Global projection and housekeeping continue on independent wall
timers. This supplies ongoing work beyond the fixed profile's finite seeded
expiry cohorts. The original fixed-population profile remains the default.

This is a **synthetic lifecycle stress experiment**, not an empirical HyperFeed
traffic model or a qualified capacity benchmark. In particular, generation
lifetime depends on offered message rate. The remaining resource limits are
reviewed in [the capacity resource note](hyperfeed_capacity_resources.md).

## Configuration and input contract

Use `--workload calibrated --maintenance-mode sweep` together with:

| Option | Disabled default | Enabled range |
| --- | --- | --- |
| `--rolling-cycle-messages` | `0` | 16–1,000,000 input events per active identity per generation |
| `--rolling-retention-seconds` | `0` | 1–3,600 physical seconds for history expiry |

Both options must be enabled together. A quarter of the configured identities
remain reserved; the other identities receive cyclic input. Unlike the fixed
control, all physical slots initially contain inactive empty records, including
the reserved quarter. There are no synthetic historical expiry cohorts.

Each identity's cycle contains:

1. A source-1 plan and position, followed by source-2 and source-4 plan/position
   pairs. These establish and grow provenance views using candidate queries.
2. Positions in three contiguous source blocks, rotating their order each
   generation. Other source-specific views can become quiet and need projection.
3. Three arrival messages, one per source.
4. One retirement probe against the other physical pool for that identity.

Two physical families alternate between generations. Tail identifiers and
scheduled-time identity evidence distinguish successive flights. Only the first
creation message may allocate; subsequent messages require an existing flight.
Creation always carries both signature fields, while later messages may use the
configured alias pattern. Temporary signature affinity retains its normal
scheduled-arrival clock and expiry rules.

Rolling message IDs advance by 1,025 for each identity's next input. This stride
is coprime to the 8-position, 26-deduplication and 32-output slot counts. The
global sequential IDs used by the original control can alias these rings when
the active population shares factors with their sizes; for example, 24 active
identities would repeatedly select one position slot per view. The rolling
mapping exercises the complete rings, remains unique across identities, and is
checked against the original offered sequence by the coordinator. Legacy IDs
and serialized inputs remain unchanged.

Retirement queries the family's current state. Every live view must have arrived
and be older than `rolling_retention_seconds + housekeeping_interval_seconds`.
It then deactivates all five logical record kinds in one transaction. This delay
gives housekeeping an opportunity to discover retained history. A short cycle,
backlogged maintenance, or reordered input can still prevent useful coverage.
The input generator never overwrites a live family to keep the benchmark moving.

Inactive flight records retain generation evidence. A delayed creation for an
equal or older retired generation is rejected. A newer generation encountering
an occupied pool is deferred. Missing, deferred, duplicate, stale, or effectless
business inputs remain visible and fail useful-work qualification even if their
transaction history is serializable. Retirement probes may legitimately be
empty, including the first generation's probe of an unused pool.

There is no inactivity fallback in this experiment. If reordered input leaves a
nonterminal fork, terminal-only retirement retains it. That is an exposed workload
limitation; it does not justify forcibly freeing the family or counting failed
generation creation as useful throughput.

## Timing and measurement

Event timestamps are scheduled offered-wall time in nanoseconds. Projection
reschedules an event 30 physical seconds beyond its job cutoff. Maintenance jobs
retain their admitted timestamps through retries and bounded transactions,
ending only with a committed empty complete query. See the [sweep
contract](hyperfeed_maintenance.md).

For `A` active identities, cycle size `C`, and foreground input rate `R`, a full
generation takes approximately `A*C/R` seconds. Changing `R` while holding `C`
fixed changes the lifecycle speed as well as load. Comparing such cells is a
sensitivity experiment, not an isolated capacity sweep of one wall-time lifecycle
distribution. A source block must also leave an eligible view quiet long enough
for a projection timer to find it before arrival or history expiry.

The lifecycle report reconciles offered phase counts with receipt effects. It
separately counts created generations, successful family retirements, and reuse
of physical families. These are observed aggregates. Full mode additionally
checks all recorded reads, complete query results, writes, outcomes, and final
state against a serial history. Metrics mode does not verify its own omitted
operation history.

Raw foreground counts and latency include the retirement probes. Business-message
throughput excludes those probes, whether they retire rows or find no work.
Maintenance transactions never inflate input-message throughput. Recurrent
coverage requires actual generation reuse and multiple positive maintenance jobs
after the initial retention/projection horizon; projection must emit outputs,
not merely claim an event whose position history is gone.

## Interpreting the next experiment

Compare the direct adapter, database-owned Unix service, and prepared/buffered
PostgreSQL adapter using identical inputs and exact full-history companions.
Keep failures and unqualified cells in the evidence. Inspect retry causes,
queueing-inclusive p99, useful business completions, whole maintenance jobs, and
retention before selecting an implementation change.

This profile does not resolve production active population, flight lifetimes,
fork-count distribution, eight-fork support, matched crash durability, owner
recovery, or physical MMHF performance. Existing proof checks remain guardrails;
they do not convert synthetic workload measurements into a 10× claim.

## Reproduce the exploratory comparison

Build and select the benchmark executable as described in the [two-host
runbook](hyperfeed_two_host.md#select-the-actual-benchmark-executable). With
`bench_binary` set to that executable and a prepared disposable PostgreSQL URL
in `AEROSTORE_CONTENTION_PG_URL`, run from the repository root:

```bash
run_root=$(mktemp -d "$PWD/target/rolling.XXXXXX")
common=(--binary "$bench_binary" --workload calibrated --maintenance-mode sweep
        --rolling-cycle-messages 640 --rolling-retention-seconds 40
        --projection-interval-seconds 5 --housekeeping-interval-seconds 5
        --dispatch signature-affinity --affinity-ttl-ms 600 --signature-pattern both
        --families 32 --hot-percent 0 --workers 16 --rates 256 --seeds 20260927
        --seconds 185 --slo-ms 50 --max-backlog 1000 --max-messages 10000
        --shm-mib 256 --outcome-tolerance 0 --timeout-seconds 600)
full_status=0
python3 scripts/qualify_hyperfeed.py "${common[@]}" \
  --engines aerostore,service-unix,postgres --evidence full --output "$run_root/full" \
  || full_status=$?
metrics_status=0
python3 scripts/qualify_hyperfeed.py "${common[@]}" \
  --engines postgres,service-unix,aerostore --evidence metrics \
  --correctness-report "$run_root/full/campaign.json" --output "$run_root/metrics" \
  || metrics_status=$?
printf 'Full exit: %s; metrics exit: %s\n' "$full_status" "$metrics_status"
```

The qualification command returns zero when sources remain stable and all
trial executions are valid. That exit status does not establish qualified
capacity. Inspect the per-trial history, useful-work, rolling-coverage and
capacity verdicts separately. A failed full trial returns nonzero and cannot
supply a correctness companion for its metrics trial. Keep both campaigns and
their raw evidence; an executed synthetic trial can remain capacity-unqualified.

The first [rolling findings](hyperfeed_rolling_findings.md) and [evidence
archive](bench_data/rolling_2026-09-27/README.md) record the results and the next
bounded experiment.
