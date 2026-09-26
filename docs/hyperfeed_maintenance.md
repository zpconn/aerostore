# Complete maintenance jobs through batch transactions

The calibrated Crucible can run a scheduled projection or housekeeping job until
it observes no eligible work. Select `--maintenance-mode sweep`; the default
`batch` mode retains the earlier one-transaction-per-tick control. Both modes use
the same query-discovered global handlers and keep foreground traffic running.
This extends workload coverage without changing production engine indexing,
transaction ownership or the original Extended Crucible.

HyperFeed's architect recalled committing batches of maintenance updates together,
not necessarily an entire sweep in one transaction. That supports this working
simulation contract. Exact batch sizes, grouping, completion guarantees and any
differences between projection and housekeeping remain unconfirmed. See the
[calibration ledger](hyperfeed_workload_calibration.md).

## Transaction and completion contract

Each timer tick admits one logical job with its original timestamp and cutoff.
The maintenance worker handles jobs in order, preserving every tick even while
an earlier job is slow. A sweep repeatedly starts a new transaction, queries the
complete current eligible result, processes its bounded selection and commits.
Projection moves selected events beyond the fixed cutoff; housekeeping deactivates
selected expired records. Failed transactions retry only their own batch.

A successful transaction that observes the complete eligible query **empty** ends
the job. A short positive batch is insufficient: even an exact or partial final
batch must be followed by the empty transaction. Full-history evidence retains
that terminal query as well as every preceding batch, and the serial-history
oracle checks all successful transactions together.

Completion is an observation at the terminal transaction's position in a valid
serial history. The entire sweep is not an atomic snapshot. A delayed or concurrent
foreground message can create eligible work after that observation; a later sweep
can discover it. The workload does not establish a stronger epoch or watermark
guarantee.

| Option | Default | Bound and meaning |
| --- | --- | --- |
| `--maintenance-mode` | `batch` | `batch` performs one bounded transaction per tick; `sweep` continues to an empty transaction. |
| `--projection-batch-size` | `4` | 1–16 scheduled events processed per transaction, after observing the complete query. |
| `--housekeeping-batch-size` | `32` | 1–64 expired records processed per transaction, after observing the complete query. |
| `--max-maintenance-batches` | `4096` | 1–4096 committed transactions per sweep, including the terminal empty transaction. |

These options apply to the calibrated profile. Sizes are experimental parameters,
not reconstructed historical values. The cap bounds each job independently. A
cap reached without an empty transaction, retry exhaustion, excessive backlog or
an interrupted job fails execution; earlier committed batches remain committed.
Partial history and progress evidence do not become a successful truncated sweep.

Each sweep transaction has a stable `(job_id, batch_ordinal)` identity, distinct from
foreground and logical timer IDs. Retries retain the same identity and cutoff.
The coordinator reconstructs the expected job and batch sequence independently,
rejects skipped or changed batches and refuses a successful drain while an
admitted job is unfinished.

## Finite expiry cohorts and comparison controls

Sweep mode seeds 49 retained historical positions per quiet family across at most
three housekeeping deadlines. Each record is timestamped one nanosecond before
its assigned expiry cutoff. The cohort count is
`min(3, max(1, floor(3599 / housekeeping_interval_seconds)))`, keeping timestamps
before the admission epoch even for long intervals. Reports expose
`housekeeping_seed_cohorts`. These synthetic ages depend on housekeeping cadence,
but not duration, foreground rate, worker count or dispatch policy.

This provides fresh eligible housekeeping records to several early jobs. It does
not generate continuing history turnover. Later jobs can validly be empty, and an
empty completed job does not count as repeated positive maintenance coverage.
Projection also need not find new work at every accelerated tick: a processed
event is rescheduled 30 physical seconds beyond that job's cutoff.

The default batch mode retains its exact earlier seed population. Consequently,
a batch-versus-sweep comparison changes both job execution and retained-record
ages. It is a workload comparison, **not an isolated measurement of the cost of
looping over batches**. To study batch-size sensitivity or routing, keep sweep
mode, timer cadence, population, messages and other assumptions fixed across the
compared runs. Different valid transaction orders can still produce different
batch counts and discovered write sets.

## Reading reports

`completed_messages` counts foreground messages plus completed scheduled jobs.
`completed_transactions`, `transaction_kinds` and store commit counters count all
committed transactions, including terminal probes. Extra batches therefore do not
inflate useful-message throughput. Foreground useful completions and eligible fork
updates remain the relevant quantities for the project's 10× target.

`maintenance_jobs` records each job's original deadline, first attempt, terminal
completion, batch counts, retries and terminal transaction identity.
`processed_rows` counts selected scheduled events for projection and expired
records for housekeeping; it is not a count of every physical row write. `maintenance_job_audit` reconciles those totals. Per-kind and workload
class latency reports use whole-job intervals: queueing-inclusive p99 begins at
the scheduled deadline and ends when the coordinator receives the terminal reply.
Foreground p99 remains separately available. The continuous throughput denominator
covers admission through worker shutdown and confirmed drain.

Raw history contains one entry per transaction, with `job_id`, `job_ordinal`,
`batch_index`, `job_completed` and `maintenance_terminal`. Progress and worker-error
evidence retain pending batches and completed transaction counts. Sampled
`oldest_due_job_age_ns_by_worker` includes an in-flight unfinished job and is null
when that worker has no due unfinished work; it is an instantaneous coordinator
observation, not a continuous queue-age maximum. Worker backlog maxima are also
sampled at scheduling boundaries.

The qualification driver checks logical counts separately from transaction counts
and includes mode, sizes and cap in the configuration matched between full-history
and metrics companions. Companions share fixed foreground input and scheduled job
contracts; their actual batch counts need not match. Metrics evidence still omits
operation histories and does not claim its own serial history was verified.

## Running functional comparisons

Select the exact `BENCH_BINARY` as described in the
[two-host runbook](hyperfeed_two_host.md). Configure an explicitly managed disposable
PostgreSQL server in `AEROSTORE_CONTENTION_PG_URL`. For example:

```bash
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/maintenance-full --engines aerostore,service-unix,postgres \
  --workload calibrated --maintenance-mode sweep \
  --projection-batch-size 4 --housekeeping-batch-size 32 \
  --max-maintenance-batches 4096 \
  --dispatch signature-affinity --affinity-ttl-ms 600 --signature-pattern mixed \
  --families 16 --hot-percent 0 --workers 4 \
  --rates 16 --seeds 20260924,20260925,20260926 --seconds 5 \
  --projection-interval-seconds 1 --housekeeping-interval-seconds 1 \
  --slo-ms 50 --evidence full
```

For a metrics companion, retain every workload option, select a new output path,
use `--evidence metrics`, and pass
`--correctness-report target/maintenance-full/campaign.json`. For a routing control,
use `--dispatch identity` and omit the affinity TTL while retaining the same
signature pattern and maintenance policy. The remote helper forwards maintenance
options to both peers and rejects mismatched setup before client work.

The example is an accelerated functional experiment, not a five-minute cadence,
repeated positive projection coverage or controlled capacity measurement. Use
300–600-second intervals and admission extending beyond the desired timer deadlines
for real cadence experiments. Inspect actual positive effects, foreground overlap,
retry causes and memory retention rather than inferring them from duration.

## Evidence and remaining work

The milestone's [evidence archive](bench_data/maintenance_2026-09-25/README.md)
records 42 passing short functional trials, local TCP validation, real-process
regressions and two 601-second runs with useful complete sweeps at both five-minute
deadlines. It also retains eight higher-load progress failures alongside four
valid histories. These results do not establish a speedup or sustainable capacity. The earlier
[affinity evidence](bench_data/affinity_2026-09-25/README.md) remains unchanged.

Complete batched sweeps remove one workload simplification. Fixed population,
seven-view provenance, synthetic input distribution, existing worker limits,
direct-access worker-failure isolation and physical MMHF measurements remain open.
The goal is still at least 10× sustainable useful foreground throughput under
matched semantics, durability and resources, with surviving workers continuing
when another worker dies. Use workload evidence to select performance changes;
retain verification as a correctness guardrail.
