# HyperFeed architecture qualification

AeroStore's target is at least **10× PostgreSQL's sustainable complete-message throughput**, for both local HyperFeed workers and MMHF workers using one central database host. Correctness, useful work, end-to-end latency, durability, worker availability and resource budgets constrain that comparison. A single favorable throughput ratio is insufficient.

This milestone supplies a reproducible investigation harness. It preserves production execution paths, the original deterministic Extended Crucible and existing proof obligations. It does not establish compatibility with unavailable HyperFeed code or qualify a production PostgreSQL replacement.

## Workload and calibration boundary

The [contention model](../aerostore_core/benches/contention_crucible/model.rs) discovers its writes from queries. `--workload fleet --hot-percent 0` is the populated-fleet profile. Its fixed corpus is independent of worker count, with staggered flight lifecycles and rotating assignment of message types to workers. At least 16 configured identities are required. Initialization serially executes 16 rounds outside timing to populate the fleet; the measured stream continues with distinct message IDs. Model tests require sustained live population, complete global query results spanning families, positive background effects and safe slot reuse. Initial/final population snapshots are reported; these are not a measured minimum throughout a concurrent run.

The preserved `--workload lifecycle` profile concentrates one generation into each 16-message cycle. In serial execution it has at most one live family, regardless of configured identity count, and some background searches see only empty results. At four workers its creation messages also fall on the same worker. It remains a useful concentrated turnover stress test, but its results must not be interpreted as a populated HyperFeed fleet. The original `legacy` profile remains available too. Different valid transaction orders can produce different business outcomes in all profiles.

Each 16-message cycle includes plan and position updates, three-source provenance growth, global event projection/rescheduling/cancellation, arrivals and whole-family expiry. Global due/expiry predicates have no family residual. Queries return their complete results; the handler subsequently bounds selected work to fit the native transaction's write capacity. Whole-family expiry requires all views to be terminal and old, or all views to be older than the explicit inactivity cutoff. This permits reclamation after delayed creation while preserving a family with any fresh view.

The concentrated lifecycle fixture uses five synthetic event-time seconds for terminal expiry and 600 for inactivity expiry. The fleet profile scales background/expiry windows by configured identity count; ordinary source and projected reschedule delays remain 30. These are accelerated test parameters, not FlightAware policy. Physical capacity is 128 slots per family, two physical family slots per logical identity, and eight position-history slots per view. Real message mixes, bursts, history-length distributions and flight-population distributions still need calibration against operator knowledge or traces. Fleet traffic is uniform; hot-identity stress remains a separate profile.

Operator calibration, supplied by Zach Conn on 2026-09-25: **global projection and housekeeping each normally run every 5–10 minutes, and messages for one flight are normally in order.** This is the architect's workload guidance, not a measured trace or an absolute ordering guarantee. Cadences of other background message kinds remain uncalibrated. The fleet fixture mixes background transactions into its message stream and allows same-flight messages to race; it remains a contention stress profile.

The [workload calibration ledger](hyperfeed_workload_calibration.md) records the subsequent worker counts, daily flight volume and fork fan-out: about 100 workers locally, 200–300 across MMHF machines against one database host, 70,000–100,000 flights per day and roughly 3–8 forks per flight. Existing limits of 32 workers and 1,024 families require review before this scale is testable. The architect confirmed that MMHF retained the published dispatcher's temporary signature affinity, so permanent per-flight routing is a comparison control. Daily volume does not establish simultaneous population or input message rate.

The separate [`--workload calibrated` profile](hyperfeed_calibrated.md) offers permanent identity routing as an ordering control and optional [temporary signature affinity](hyperfeed_affinity.md). Affinity routes by input identifiers and scheduled arrival time; alias changes or expiry can permit same-flight overlap and completion inversions. Complete histories must still be serializable, and stale fork updates fail useful-work acceptance. Projection and housekeeping have independent wall-clock timers, defaulting to 300 and 600 seconds. Two dedicated maintenance workers are additional to the declared foreground worker count on every engine. Query-dependent discovery remains intact; stable input routing does not predeclare writes. Reports check per-worker order, record same-flight ordering, and distinguish foreground latency, maintenance latency, backlog, occupied worker time and observed execution overlap.

This first calibrated profile intentionally keeps a fixed population with a synthetic quiet quarter. Each timer tick performs one bounded batch after a complete global query, not a full maintenance sweep. Flight lifetimes, turnover, quiet share and source mix are not calibrated. The qualification gate therefore permits functional and diagnostic results but rejects capacity and 10× claims from this profile, including long runs. Explicitly accelerated intervals support fast tests; actual 300–600-second runs are required for observed cadence coverage. The existing profiles retain out-of-order, creation, expiry and heavy-maintenance stress coverage.

The [calibrated validation checkpoint](bench_data/calibrated_2026-09-25/README.md) records accelerated three-engine trials and two full-history 601-second runs with both maintenance intervals set to 300 seconds. Each long run completed 19,232 useful foreground messages plus four useful maintenance jobs. This establishes repeated cadence coverage under the stated synthetic assumptions; the capacity gate remains closed.

The subsequent [temporary-affinity checkpoint](bench_data/affinity_2026-09-25/README.md) adds corpus-matched identity/affinity trials and controlled reordering. At higher offered load it retains both valid histories and maintenance retry/backlog failures, with incomplete histories explicitly unverified. These are short, instrumented sensitivity results; they do not establish sustainable capacity.

`--arrival-rate N` offers exactly `N × seconds` messages on a fixed schedule, independent of completions. In the calibrated profile this rate counts foreground messages; timer jobs are additional and independently admitted. The run waits through the full admission interval and drains all offered work. Per-worker caps, excessive backlog, exhausted retries or incomplete drains fail execution. Queueing remains in p99 instead of disappearing when workers stall. `maximum_backlog` is sampled around message execution; it is not an exact instantaneous maximum during an in-flight transaction.

## Evidence modes and gate

`--evidence full` records every successful attempt's reads, complete query results, effects and outcomes. The independent serial oracle must find a complete witness; `Invalid` and `Inconclusive` fail. `--evidence metrics` runs the same handler with operation recording disabled. It retains outcome counts, retry stages, latency, commit counts and final structural checks, but explicitly reports `correctness_history_verified=false`.

The [qualification driver](../scripts/qualify_hyperfeed.py) retains every cell of a declared engine/rate/worker/seed matrix, including failures. It records dirty source hashes, binary hash, compiler, host, configuration and PostgreSQL settings. Full-history companions must match the measured source, binary, configuration and exact input corpus. They do not prove an unrecorded measurement history.

The gate distinguishes execution validity, useful-work acceptance and performance acceptance. For lifecycle/fleet qualification, a useful-work trial needs actual creation and family expiry, bounded missing/deferred outcomes, and the declared p99 and drained-throughput budgets. Fleet trials also require populated initial/final snapshots, positive effects from each global background kind and a full lifecycle cycle in the offered corpus. Missing/deferred fraction is not a count of all effectless messages: a valid maintenance query may return nothing. At least three seeds must pass to establish a qualified tested rate. Passing rates are lower bounds on capacity: dividing two of them is not a 10× capacity proof. A conditional synthetic comparison additionally requires a repeated PostgreSQL saturation bracket across the declared worker grid and comparable business effects. The calibrated profile has a separate diagnostic assessment and cannot supply those capacity bounds yet. The real-world replacement and architecture-promotion flags remain false.

An illustrative campaign, with an explicitly managed test PostgreSQL server:

```sh
source target/verification-tools/environment.sh
cargo build --release --locked -p aerostore_core --bench hyperfeed_contention_crucible
# Use the exact executable reported by Cargo; do not select a stale glob match.
export AEROSTORE_CONTENTION_PG_URL='host=/path/to/private/socket dbname=bench user=bench'
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/hf-full --engines aerostore,service-unix,postgres --workload fleet \
  --rates 100,1000 --workers 1,4 --seeds 20260924,20260925,20260926 \
  --seconds 5 --families 16 --hot-percent 0 --slo-ms 50 --evidence full
python3 scripts/qualify_hyperfeed.py --binary "$BENCH_BINARY" \
  --output target/hf-metrics --engines aerostore,service-unix,postgres --workload fleet \
  --rates 100,1000 --workers 1,4 --seeds 20260924,20260925,20260926 \
  --seconds 5 --families 16 --hot-percent 0 --slo-ms 50 --evidence metrics \
  --correctness-report target/hf-full/campaign.json
```

Here 50 ms is a declared experimental budget, not a known HyperFeed SLA. Five-second cells explore a capacity grid; separate sustained runs must establish retention and maintenance behavior. CPU budgets are recorded but not enforced; shared-host interference and equal memory/resource allocations remain qualification limits. A source change invalidates companions and requires fresh evidence.

## PostgreSQL comparison

The adapter uses `SERIALIZABLE`, prepared statements, appropriate composite indexes and no predeclared application write sets. Buffered mode is the default: real predicates establish PostgreSQL's dependencies, an overlay implements read-your-writes and savepoints, and commit locks the discovered rows in a stable order before a set-based write batch. Immediate mode remains available as a control.

Reports include statement/row counts, settings, relation retention and sample `EXPLAIN` plans. The plan audit uses literal/custom examples; it does not establish the best possible prepared generic plans. PostgreSQL should run directly on the database host with a local socket for local comparisons, then over the same network topology as the service for MMHF comparisons. Docker remains useful for functional tests, but its transport overhead should not define the local baseline.

Both paths currently acknowledge asynchronous WAL. Runs record a final drain and throughput over the continuous client-clock interval from admission through worker shutdown and confirmed drain. For a remote owner, this also includes delivery of its final snapshot/audit confirmation; server drain duration remains a separate diagnostic. PostgreSQL fsync settings, native writer behavior and dependent-transaction recovery still require an explicit matched crash contract. A successful normal drain does not establish equal durability or recovery.

## Database-owned interactive prototype

The [service](../aerostore_core/benches/contention_crucible/service.rs) executes transaction operations inside the database owner. The external worker still runs the handler and issues begin/read/query/write/savepoint/commit operations. `service-unix` and `service-tcp` measure this interactive path locally. This preserves query-dependent application behavior and exposes round-trip costs.

Server-side sessions own transaction registrations, private writes and native guards. Disconnect cleanup aborts unaccepted transactions; an accepted commit completes even if its caller dies. A bounded volatile outcome cache can resolve a lost commit reply while its entry remains available. `Unknown` is not permission to replay. Session/frame/operation/time limits bound ordinary abandoned clients. A wedged native engine operation can still require terminating the disposable owner: the prototype does not make arbitrary engine failure safe.

The real-native worker-kill suite uses a separate victim process and a surviving worker process whose session was already active. It exercises disconnects after request acceptance, queries, private writes, commit acceptance and commit recording, then checks progress on the same and disjoint rows, registration cleanup and repeated-abort retention. This is worker-failure evidence, not service/host-crash recovery or durable exactly-once delivery. The accompanying [finite protocol model](../verification/service_protocol/README.md) checks accepted-commit ordering, cleanup and conditional survivor progress, with deliberate broken variants. It is not a refinement proof of this Rust implementation.

## Network path

The [remote runner](../scripts/run_remote_contention.py) launches a separate database owner, copies a versioned setup frame to the client, waits for all client sessions to close, requests owner drain and checks a run-bound final snapshot. Client clocks measure complete message latency; server retention samples use their own epoch. No cross-host monotonic-clock comparison is assumed.

```sh
python3 scripts/run_remote_contention.py --binary "$BENCH_BINARY" \
  --output-dir target/hf-loopback --seconds 5 --arrival-rate 100 \
  --workload fleet --families 16 --hot-percent 0 --evidence full
```

The [two-host runbook](hyperfeed_two_host.md) provides paired commands for the remote owner and PostgreSQL on the same database host, plus prerequisites and the existing local validation. The user selected local validation and preparation of those commands; no second host has been measured. The current qualification driver does not include `service-remote` in its automated matrix, so the external-host comparison is manual and exploratory.

The TCP protocol has no authentication or TLS and is for isolated trusted test networks. The PostgreSQL adapter also uses plaintext transport. Loopback execution validates transport behavior, not physical MMHF performance. `--rpc-delay-us` in local service runs is a controlled per-operation delay sensitivity test; it is not a measured network RTT or a substitute for two-host trials.

## What should drive the next implementation change

The [2026-09-25 fleet campaign](bench_data/architecture_2026-09-25/workload-and-gates/fleet-campaign-review.md) reproduces a large native concurrency penalty and retry exhaustion under its synthetic global-background mix. Its p99 and retry observations remain useful, but its initial drained-rate bounds are withdrawn because worker shutdown was omitted from that denominator. Current reports use a continuous admission-to-confirmed-drain client clock, and the gate rejects older reports for capacity. No capacity ceiling or 10× ratio has been established. The operator calibration above further limits extrapolation: these results characterize frequent-maintenance stress and freely reordered transactions, not typical HyperFeed operation.

Use signature affinity and its matched identity control in the cadence/ordering profile to separate normal foreground work from maintenance-related contention, retaining the stress suite and verification checks. Next extend scheduled maintenance into complete bounded transaction batches per job and add calibrated turnover and population/mix parameters. Establish useful work, complete-history correctness companions, latency during maintenance and sustained retention before using the results to choose an architectural change. Prepare physical two-host trials in parallel, but do not infer MMHF performance from loopback.

The current native expiry index includes all active record types, while PostgreSQL's partial expiry index includes only housekeeping-relevant types. Match that schema choice in a controlled benchmark variant before attributing all expiry cost to the engine. Global due queries remain a separate, larger observed stress cost. This schema control and precise retry profiling should follow the calibrated baseline; the stress evidence alone does not establish which optimization will matter most in HyperFeed.

Controlled [native diagnostics](../aerostore_core/tests/contention_diagnostics.rs) separate broad-index stamp rejection after an unrelated completed key move, validation of an already captured predicate, concrete row validation, and historical indexed visibility after a key move. They establish mechanisms under exact schedules. Sustained stage counters alone do not identify each mechanism's fraction of retry cost.

Use repeated workload results to choose the next small experiment: narrower predicate conflicts if broad queries dominate, historical index retention if old indexed reads dominate, or fewer protocol round trips if service latency dominates. Re-run the exact workload and correctness companions after each change. Keep the original direct-access failure evidence visible while evaluating service ownership. Neither an index redesign nor service adoption follows automatically from building this harness.
