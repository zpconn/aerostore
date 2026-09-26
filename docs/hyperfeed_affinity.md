# Temporary signature affinity

The calibrated Crucible supports the dispatcher policy confirmed by HyperFeed's
architect for both single-machine HyperFeed and MMHF: round-robin assignment with
temporary affinity for an input's `(callsign, registration)` signature. Select it
with `--dispatch signature-affinity`, alongside the existing `identity` control.
The current implementation is a deterministic workload generator, not a deployed
HyperFeed dispatcher or a measurement of dispatcher throughput.

## Routing and input assumptions

Affinity mode requires an explicit `--affinity-ttl-ms N` in the range 1–3,600,000.
There is no assumed production timeout. A hit retains its assigned worker and
refreshes expiry to scheduled arrival plus TTL. Arrival exactly at expiry is a
miss. New and expired signatures take the next round-robin worker; hits do not
advance that cursor. Sliding refresh and exact expiry behavior are declared
simulation choices requiring further calibration.

Routing uses only the visible callsign and registration, the scheduled arrival
time and dispatcher state. It never uses the model's resolved flight identity,
transaction results or completion times. The finite fixture retains expired cache
entries to count expired misses separately from first sightings; it is not a
production cache eviction policy.

`--signature-pattern both` preserves the previous input corpus. `mixed` cycles
each flight through two messages containing both identifiers, two containing only
the callsign and two containing only the registration. Callsigns are shared by
groups of synthetic flights; matching also uses route evidence. The cycle and
identity distribution are synthetic, not measured FlightAware frequencies.

Both signature patterns work with `--dispatch identity`. Comparing `identity`
and `signature-affinity` using the same pattern preserves the exact messages and
isolates the routing change. Identity mode rejects a nonzero affinity TTL. Other
workload profiles reject nondefault dispatcher options.

The coordinator constructs assignments once before starting admission and sends
each worker only its own compact sequence. Workers validate that sequence before
reporting ready. Queues retain every offered message through retries and backlog;
excess backlog or an incomplete drain fails execution. This preserves fixed
arrivals even when a worker stalls, without making each worker replay the whole
dispatcher during measurement.

## Correctness and useful work

Every worker processes its assigned queue in order. Two signatures can resolve
to the same physical flight on different workers; an affinity entry can also
expire while its previous worker still has unfinished work. Same-flight overlap
and completion inversions are therefore permitted by the workload. The complete
transaction history must still have a valid serial witness.

Reports distinguish three checks:

- `dispatch_audit` records assignment counts, hits, misses, expiry, worker changes
  and an assignment fingerprint. The coordinator checks every actual receipt
  against the offered schedule. The Python qualification driver independently
  reconstructs the routing statistics and fingerprint. FNV-1a here is a portable
  reproducibility check, not a cryptographic commitment.
- `per_flight_order` remains mandatory in identity mode. Under affinity it records
  observed overlap and completion inversions with `required: false`; a false
  observed FIFO result alone does not invalidate execution. These intervals cover
  first attempt through successful completion, including retries and backoff.
- Full evidence checks the serial history. A later update can validly make an
  earlier one stale. Such a run may be correct while failing useful-work policy.
  The gate reports positive-message fraction separately from stale-view fraction
  and refuses to treat discarded fork updates as equivalent business work.

Dispatch policy, TTL and signature pattern are part of the configuration matched
between full-history and metrics companions. Missing legacy options mean
`identity`, zero TTL and `both`; old evidence retains its original meaning.
Metrics histories remain unverified, even with a matching full-history companion.

## Running a comparison

Build and select `BENCH_BINARY` using the [two-host runbook](hyperfeed_two_host.md).
With an explicitly managed disposable PostgreSQL server configured in
`AEROSTORE_CONTENTION_PG_URL`, this starts an accelerated functional campaign:

```bash
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/affinity-full --engines aerostore,service-unix,postgres \
  --workload calibrated --dispatch signature-affinity --affinity-ttl-ms 600 \
  --signature-pattern mixed --families 16 --hot-percent 0 --workers 4 \
  --rates 64 --seeds 20260924,20260925,20260926 --seconds 5 \
  --projection-interval-seconds 1 --housekeeping-interval-seconds 2 \
  --slo-ms 50 --evidence full
```

For the routing control, use a fresh output path, `--dispatch identity` and omit
`--affinity-ttl-ms`; retain every other workload option, including `mixed`.
The 600 ms affinity lifetime and 50 ms latency budget above are experimental
parameters. Five-second admission with accelerated maintenance cannot establish
production cadence, sustained capacity or a speedup. The remote helper forwards
all three dispatcher options to both peers and checks their agreement.

This step preserves the seven-view fixture, bounded maintenance batches,
fixed population and existing worker limits. Full sweeps, lifecycle/population
calibration and reviewed support for 100–300 workers remain necessary. See the
[calibration ledger](hyperfeed_workload_calibration.md) for the operating targets
and known registration/session limits.

## Validation and next milestone

The [affinity checkpoint](bench_data/affinity_2026-09-25/README.md) retains 36 passing
three-engine functional trials, local TCP validation, and the paused-worker
regression that rejects stale fork updates as equivalent useful work. Its higher-load
sensitivity retains six valid histories and six retry/backlog progress failures,
including partial traces with actual same-flight reordering. These runs are
instrumented diagnostics, not a speed ranking.

Next complete scheduled maintenance jobs with explicit transaction boundaries,
job-versus-batch accounting and terminal completion witnesses. The architect's
subsequent recollection supports batches of updates committed together; use
multiple batch transactions as the working sweep contract, with configurable
sizes rather than an assumed historical value. The [calibration ledger](hyperfeed_workload_calibration.md)
records this guidance and the existing PostgreSQL prepared-statement reuse.
Use those results and precise retry attribution to guide performance changes. The archive's
[next-milestone review](bench_data/affinity_2026-09-25/next-milestone-review.md)
also lists the registration, admission, session, WAL and query-size constraints
to resolve before 100/200/300-worker experiments.
