# HyperFeed contention Crucible

The next architecture milestone is to measure AeroStore under transactions that discover their writes through queries, with different messages competing for the same flight identities. The `hyperfeed_contention_crucible` benchmark provides that workload and checks complete successful transaction histories against an independent serial store.

The existing [extended Crucible](extended_crucible.md), engine, and [verification checks](../verification/README.md) remain the baseline. This benchmark adds separate adapters and scenarios; it does not establish an architecture winner or justify replacing the engine. The separate database-process design remains an alternative to investigate. There is no measurement here establishing that it would be faster.

## Transaction model

This is a synthetic storage workload informed by HyperFeed, not an implementation of FlightAware's proprietary matching or projection algorithms. It reuses the extended Crucible's record representation, query predicates and 128-slot family layout. Its [handler](../aerostore_core/benches/contention_crucible/model.rs) has a different transaction admission rule: every attempt starts with **no declared write set and no application row prelocks**.

The handler searches by callsign or tail and a scheduling window, then uses route and tail evidence to select a family. The family it finds may differ from the message's allocation hint. Only an empty candidate query permits creation in the hinted physical space. Competing creators have different reserved spaces, so a shared row write conflict cannot conceal a missing empty-search dependency.

After matching, transactions can discover and update up to seven provenance views, position history, scheduled events, output records and deduplication records. New source evidence creates the permitted combinations of views. Position and plan messages reject stale updates; projection reads due events and position history; housekeeping queries expired records. Callsign changes mutate secondary index keys. Outputs are returned only after commit succeeds. Serialization failures retry the whole message with backoff; failed cleanup and ambiguous commit errors stop the run.

Physical space remains bounded: eight position slots per view, 32 output slots and 26 deduplication slots per family. Projection uses an integer surrogate for geographic prediction. The [qualification extension](hyperfeed_qualification.md) adds lifecycle turnover, global background transactions, independent fixed arrivals and a separate TCP service owner. Arbitrary schemas, long production history distributions, calibrated real arrival traces and proprietary handler compatibility remain open. The deterministic baseline continues to cover its savepoint, abort, family expiration and replay cases.

## Workloads

| Scenario | Concurrent work | What it exercises |
| --- | --- | --- |
| `competing-empty-creation` | Four distinct messages with the same identity and different allocation hints | All four first queries must observe absence before any handler proceeds. Successful retries must converge on the winning family. |
| `mixed-matching-projection-housekeeping` | Six messages: two positions, a plan, two projections and housekeeping | Query-discovered writes overlap on a seeded family containing all seven provenance views. Valid outcomes depend on transaction order. |
| `indexed-key-move` | Four messages: two callsign changes, a position and projection | Matching uses callsign evidence with `tail=0`, so a stable tail index cannot hide missing callsign candidates. |
| `global-due-claim-cancel-reschedule` | Five messages claiming, cancelling, rescheduling and projecting global due events | Complete time-range searches cross family boundaries; the selected work is bounded after the query. |
| `creation-versus-whole-family-expiry` | Six messages combining matching, creation and expiry | Whole-family deletion and later physical-slot reuse must preserve identity and complete query observations. |
| `sustained-mixed` | Independent worker processes generate plans, positions, projections, housekeeping and callsign changes | A continuous mixed workload with shared identities, retries, version churn and retention sampling. |

The five bounded scenarios use one worker process per message: four, six, four, five and six processes respectively. The empty-creation scenario has an explicit barrier after the first backend query. Subsequent retries have no barrier. Scenario latency includes this deliberate wait and should not be interpreted as normal service latency.

Sustained mode uses `--workers` processes without transaction or phase barriers. The default `--workload legacy` preserves the original seeded mixed generator described below. `--workload lifecycle` starts with empty reserved space and adds flight creation, source growth, terminal arrivals, whole-family expiry and physical-slot reuse. Its fixed input corpus is independent of worker count, but it completes each generation in 16 messages and has at most one live family in serial execution. At four workers, creation is also assigned to one worker. Treat this as concentrated turnover stress. `--workload fleet --hot-percent 0 --families 16` instead staggers lifecycles across a populated fleet and rotates message types across workers. Its initialization runs outside timing; global background queries and mutations span identities. Fleet traffic is uniform and requires at least 16 identities. `--families` selects the number of logical identities; the physical pool reserves twice that many family slots. Other profiles support `--hot-percent` to bias traffic toward identity zero. Nearby identities also share callsigns and require disambiguation.

`--message-interval-us` optionally spaces sustained message starts within each worker. Its default is `0` (unpaced), and its maximum is `1000000`. This pacing happens outside message latency. A paced trial can exercise retention over a longer interval without producing the receipt volume of an unpaced contention trial. It remains a closed-loop workload: slow transactions and retries can reduce the achieved rate below the requested start rate. Report paced retention observations separately from unpaced contention/throughput measurements.

Message identities and logical timestamps derive from worker sequences and the seed. Worker progress can diverge, so some messages become stale and do less work. Inspect the retained outcomes and actual completed message mix alongside throughput. Identical parameters do not force the two engines to complete identical prefixes during a duration-limited run.

## Run it

Run from the repository root on Linux, with enough local shared-memory storage and disk space for complete receipts. The benchmark uses separate local worker processes and disposable per-case storage.

Run the bounded native cases:

```sh
cargo bench -p aerostore_core --bench hyperfeed_contention_crucible -- \
  --engine aerostore --mode scenarios \
  --output "$PWD/target/contention-native-scenarios.json"
```

Run a sustained native trial:

```sh
cargo bench -p aerostore_core --bench hyperfeed_contention_crucible -- \
  --engine aerostore --mode sustained \
  --workers 4 --families 16 --seconds 60 --max-messages 20000 \
  --seed 20260924 --hot-percent 80 --shm-mib 256 \
  --query-plan family --oracle-budget 2000000 \
  --output "$PWD/target/contention-native-family.json"
```

PostgreSQL runs require an explicit `--pg-url`. Use an existing local test database whose role can create and remove benchmark schemas. The runner creates a uniquely named schema, marks its ownership, and removes that schema after the case. It does not require a particular fixture port or password:

```sh
cargo bench -p aerostore_core --bench hyperfeed_contention_crucible -- \
  --engine both --mode all \
  --pg-url "host=127.0.0.1 dbname=aerostore_bench user=aerostore_bench" \
  --workers 4 --families 16 --seconds 60 --max-messages 20000 \
  --seed 20260924 --hot-percent 80 \
  --output "$PWD/target/contention-both.json"
```

`--max-messages` is a **per-worker** limit. A worker finishes its current message, including retries, before checking the duration again. Check the report's duration and cap fields before treating a trial as a sustained-duration measurement. Increasing history capacity also increases receipt storage and oracle work.

Full successful receipts are approximately 20 KB per message in the current mixed fixture, with size depending on query results and the amount of work. A four-worker run capped at 20000 messages per worker can therefore produce roughly 1.6 GB of raw history. Set duration and message limits explicitly; retaining complete histories is part of the correctness contract. For example, a longer paced retention trial can use:

```sh
cargo bench -p aerostore_core --bench hyperfeed_contention_crucible -- \
  --engine aerostore --mode sustained \
  --workers 4 --families 16 --seconds 300 --max-messages 4000 \
  --message-interval-us 100000 --seed 20260924 --hot-percent 80 \
  --output "$PWD/target/contention-native-paced-retention.json"
```

The configured pace in this example permits at most about ten message starts per second per worker. Actual completion rate may be lower. The wait is excluded from service latency and included in elapsed throughput.

`--query-plan global-time` changes the native access path for due/expired searches from family equality to a time range followed by the same residual filters. PostgreSQL chooses its own physical query plans; this option does not force PostgreSQL to use a corresponding index. Paired native trials can measure sensitivity to broad predicates while preserving business query semantics.

The model/oracle tests run separately:

```sh
cargo test -p aerostore_core --test contention_crucible_model
```

## What a passing history means

Every successful attempt records its message, globally comparable monotonic start/end timestamps, all point reads, complete sorted predicate results, intermediate writes and returned outcome. The coordinator retains these in `history.jsonl`, checks the expected per-worker message sequence, and requires the successful commit count to equal the receipt count. Initial and final row images and `serial-witness.json` accompany the report.

The [oracle](../aerostore_core/benches/contention_crucible/oracle.rs) re-executes each handler against an independent serial map with no engine indexes, allocator or OCC implementation. It searches for an order that reproduces every observation, write and outcome, then requires exact final-state equality. It respects completed-before-started relationships. Overlapping transactions may be ordered either way; commit response order is only a search heuristic.

The search uses reversible row changes and an iterative search stack. A complete witness is `Valid`. Exhausting every possible branch is `Invalid`. Exhausting the configured search budget is `Inconclusive` and does **not** pass. A long run cannot become successful merely because checking it is expensive.

Independent snapshot checks cover physical placement, provenance restrictions, complete view combinations, lineage, orphan records and terminal scheduled events. The contention checker additionally requires consistent immutable identity evidence within each family and prevents the same stable tail/origin/schedule identity from belonging to independent families. Native runs also audit row/index agreement and require worker registrations to be gone after clean shutdown.

Tests deliberately corrupt candidate results, effects, outputs and final rows, and construct two creators that both commit after empty searches. They also check different valid overlapping orders and failed cleanup. These checks establish observational serializability for the supplied successful history. They are not a whole-engine proof, a crash-recovery proof, or a claim about every read made by an aborted attempt.

## Read the measurements

The report distinguishes correctness and operational completion from performance eligibility. Architecture promotion and performance comparison eligibility remain false: a passing diagnostic run does not establish either. Full operation histories are the default. `--evidence metrics` explicitly omits serial-history checking and is restricted to sustained mode; it retains outcomes and final structural checks. See the qualification runbook for paired campaigns and fixed-arrival measurements.

| Measurement | Interpretation and limit |
| --- | --- |
| Completed messages/second | Complete harness throughput, including process/receipt IPC and coordinator work. Open-loop runs must drain their exact offered corpus. Inspect completed message kinds and useful work, including stale ignores. A second rate includes final WAL/maintenance draining. |
| Message p50, p99 and maximum latency | Scheduled arrival in open-loop mode, or first attempt in closed-loop mode, through coordinator receipt; includes queueing, transaction RPC, whole-message retries, backoff and receipt IPC. Service p99 is also reported separately. Bounded scenarios include their controlled barrier wait. |
| Retry counts and stages | Native stages locate where a serialization failure surfaced. They do not distinguish exact predicate-bucket collisions, row validation failures and every newer-version cause. PostgreSQL records retryable SQLSTATEs. |
| Native retention samples | Arena allocation high-water, row reuse/recycling, active registrations and retired/reclaimed index objects. High-water is not live memory or RSS. |
| PostgreSQL retention samples | Relation/table/index sizes and estimated live/dead tuple counts. These are not server RSS, and tuple statistics can lag. |
| After-drain observations | State after normal worker shutdown and native collector/WAL draining. This is not worker-crash recovery. |

All adapters execute the same logical handler. Full observation recording affects their costs differently; metrics mode reduces that recording without claiming the same history guarantee. Oracle checking, initialization and final auditing are outside the timed workload. Use separate memory gauges as retention evidence for each engine; subtracting PostgreSQL relation bytes from AeroStore arena high-water would not measure a memory advantage.

The transaction contract is native optimistic transactions versus PostgreSQL `SERIALIZABLE`, with dynamic writes and no family prelocks. PostgreSQL establishes its snapshot at its first business query. Both runs use asynchronous WAL acknowledgement; the native writer is drained at normal completion. PostgreSQL settings are recorded. Identical acknowledgement terminology does not establish equal crash-loss windows or equivalent recovery behavior.

## Architecture decisions and the next milestone

Start with repeatable correctness witnesses, then run longer paired trials across worker count, identity count, hot-identity percentage and query plan. Keep the same build, hardware, seed, transaction contract and stated durability settings. Record completed work, retries per completed message, p99 including retries and retention over time. Repeated trials and their variability matter more than one favorable throughput number.

If broad time predicates produce a large retry/latency penalty on otherwise independent families, investigate narrower conflict tracking. Native retry-stage totals alone cannot prove that diagnosis. If callsign moves repeatedly force historical readers to retry, measure that cost explicitly before deciding whether index versioning is worthwhile. The original extended Crucible retains a controlled historical-index visibility probe that accepts complete historical results or explicit serialization rejection, and rejects incomplete successful results.

Worker failure is a separate acceptance condition: **surviving workers must keep their mappings and continue processing affected work**. The production direct-access path currently does not satisfy that requirement at several controlled kill points. The separate database-owned prototype passes selected client-kill checks; this is scoped experimental evidence and does not repair direct access or establish host-crash recovery. See the [worker failure contract and evidence](worker_failure_contract.md). Successful contention histories cannot turn that availability result green, and rebuilding the entire mapping after stopping all workers does not satisfy survivor continuity.

These observations should determine whether direct shared-memory mutation can meet the failure contract economically, or whether the database-owned prototype should progress toward production. Neither ownership choice nor an index redesign is assumed to be the winner.

Retain verification as a guardrail for stable requirements: atomic transactions, complete query results, safe reclamation and recovery ordering. Preserve the existing deterministic tests, regression controls and proof checks while exploring workload behavior. Defer expensive implementation-specific proofs of components this investigation may replace. Any eventual optimization still needs its declared refinement checks and performance evidence; this diagnostic harness is not an exception to those gates.

## First investigation

The [2026-09-24 evidence](bench_data/contention_2026-09-24/README.md) records passing complete histories, two native broad-query retry-limit failures, PostgreSQL deadlock-timeout sensitivity, and two-minute controlled-rate retention runs. The overall workload campaign remains failed; component proofs and passing characterization tests do not override its progress or worker-availability failures.

Cargo runs benchmark executables from the package directory. The commands above use absolute output paths so evidence lands in the repository's `target/`; a relative `--output` is relative to the executable's working directory.
