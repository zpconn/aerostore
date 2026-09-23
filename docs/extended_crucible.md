# Extended HyperFeed Crucible

Extended Crucible simulates complete single-machine flight-tracking transactions using the real Aerostore engine, PostgreSQL, and a serial reference store. It supplements the original [Crucible](../aerostore_core/benches/hyperfeed_crucible.rs) storage-churn benchmark. Its domain behavior is synthetic and grounded in the public material summarized in [the research and coverage specification](extended_crucible_research.md).

**A successful bounded replay is not a HyperFeed compatibility pass.** The suite separately tests native storage contracts. The initial Aerostore probes expose three failures: concurrent creation after empty candidate searches, atomic table/index publication, and finding historical rows through changed index keys. `--mode all` must fail while any selected contract fails, even when every replayed message matches the reference.

## Run it

Run commands from the repository root. Aerostore-only runs require the Rust/Linux development environment; PostgreSQL defaults to a disposable PostgreSQL 16 Docker container. Docker must be reachable from the same shell. Dependencies must already be cached when using `--offline`.

Inspect the native contracts without Docker:

```sh
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine aerostore --mode contracts \
  --output target/extended-native-contracts.json
```

Run the full two-engine gate, including the shared input replay:

```sh
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine both --mode all --families 32 --cycles 2 --workers 4 \
  --seed 20260922 --output target/extended-crucible.json
```

The current native failures make those commands exit unsuccessfully after reporting the failed contracts. This is an observed correctness result, not an expected-failure test converted into a green gate.

For storage-adapter diagnosis, isolate the bounded replay:

```sh
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine aerostore --mode replay --families 32 --cycles 4 --workers 4 \
  --output target/extended-replay.json
```

Use an existing local test PostgreSQL instead of Docker by supplying `--pg-url "$EXTENDED_CRUCIBLE_PG_URL"`. The connection can be a PostgreSQL URL or libpq-style connection string. Its role needs permission to create and drop scratch schemas. The runner uses a process-specific schema and removes it after a completed run; interrupted runs can leave a scratch schema. Connections currently use `NoTls`, so this option is intended for the local test database.

| Option | Default | Meaning |
| --- | --- | --- |
| `--engine` | `both` | `both`, `aerostore`, or `postgres` |
| `--mode` | `all` | `all`, `replay`, or `contracts` |
| `--families` | `32` | Synthetic family allocation domains; accepted range 1–4096 |
| `--cycles` | `2` | Repeated 27-phase lifecycles; accepted range 1–1000 |
| `--workers` | `4` | Independent local worker processes; accepted range 1–32 |
| `--seed` | `20260922` | Reproducible input variation shared by all engines |
| `--shm-mib` | `256` | Aerostore shared arena; accepted range 32–3584 MiB |
| `--pg-url` | unset | Existing local PostgreSQL; otherwise use Docker |
| `--output` | `target/extended-crucible.json` | JSON report path |

Higher accepted limits are configuration bounds, not promises that every combination fits the selected arena or completes within worker timeouts. Use `--help` for the executable's current interface.

## Workload coverage

Each cycle progresses through these transaction families. Both engines receive identical messages and return complete candidate records to the same application-level handler.

| Family | Operations exercised | Checks |
| --- | --- | --- |
| Candidate matching | Callsign/tail search, time-window filtering, route decisions, provisional creation | Same callsign can denote different families; missing candidates cannot be rescued through an allocation hint |
| Pedigree expansion | Three synthetic provenance classes, up to seven views, related-row writes | Complete intended view set and source-restricted updates |
| Failed fork | Partial child/output writes inside a savepoint | Rollback removes branch effects while earlier outer work survives |
| Position processing | History append, latest-state updates, duplicate/stale/invalid inputs | Accepted values and all output records match the reference |
| Duplicate contention | Four identical deliveries for each family, assigned across workers | One application and three duplicate outcomes, regardless of which worker wins |
| Whole-message abort | Multirow changes and buffered output followed by abort | No attempted changes or outputs survive |
| Deferred projection | Due-time range searches, history reads, deadline replacement | Real observations stay distinct from projected output; terminal flights cancel scheduled work |
| Lifecycle corrections | Diversion, subsequent positions, arrival or cancellation | Multiple pedigree views change consistently under the synthetic rules |
| Housekeeping | Expire history, output, and dedup records | Retained contents and index entries match committed rows |
| Family expiry and reuse | Atomically deactivate a terminal family and its related records, recreate next cycle | No partial deletion; storage/index reclamation is audited |

The physical fixture reserves 128 slots per family: seven flight views, eight position slots per view, seven scheduled-event slots, 32 output slots, and 26 dedup slots. This bounds retained state while repeated cycles exercise allocation and reclamation. Stored output is a bounded audit journal; per-message emitted output is also compared in full. It is not a production outbox or a crash-delivery guarantee.

Identifiers, source masks, timestamps, rejection rules, projection arithmetic, message mix, and retention thresholds are explicit simulator choices. The fixture does not execute Tcl, FlightAware's parsers, its fuzzy matching algorithms, or production SQL/STAPI calls. Virtual timestamps advance deterministically without real-time pacing; these runs are synthetic catch-up workloads, not measured production traffic.

## Why replay and native contracts are separate

Workers are separate local processes and their assignments rotate between phases. Within one phase, different families run concurrently; each family receives at most one distinct message. The first position phase submits four identical deliveries per family across workers. Its oracle compares a semantic multiset: one successful application and three duplicates, without choosing a winning worker in advance. Phase barriers preserve the order of distinct messages and permit exact state comparison. Concurrent independent-family outcomes commute.

The fixture declares all potentially written family slots. Aerostore tries to acquire its indexed-row guards before beginning the transaction and retries contention; PostgreSQL locks the same reserved rows with `SELECT ... FOR UPDATE` inside a `SERIALIZABLE` transaction. Both adapters then run real candidate queries and real transactions. Aerostore also overlays pending index intents so reads see their own writes and savepoint rollback restores that overlay.

This coordination is part of the measured adapter contract. It tests concurrent duplicate deliveries but does not prove arbitrary predicate protection, dynamic write-set discovery, or unrestricted simultaneous processing of distinct events for one hot family. Native probes deliberately test relevant properties without supplying a compensating predicate lock or serializing all transactions in the harness.

| Native contract | Initial Aerostore observation | Initial PostgreSQL observation |
| --- | --- | --- |
| Empty candidate search followed by competing creation | **FAIL:** both transactions can commit | PASS |
| Committed row and secondary-index visibility | **FAIL:** a reader can miss the new index key during publication | PASS |
| Historical index candidates for a transaction snapshot | **FAIL:** an old snapshot can miss a row after its key moves | PASS |
| Savepoint and whole-message rollback | PASS | PASS |
| Multirow table snapshot visibility | PASS | PASS |
| Concrete row-read dependency/write-skew rejection | PASS | PASS |

PostgreSQL has counterpart probes for all six contracts. Consult the generated JSON for the results of the selected engine and run; this table records the initial findings, not an assertion about every future revision. The [retained validation results](bench_data/extended_crucible_2026-09-22/README.md) include 3,840 deliveries per engine with eight workers and a further 30,720-delivery Aerostore replay in a 128 MiB arena. Both replays passed; the strict combined gate failed on the three native Aerostore contracts. The evidence also records workspace tests, the original Crucible regression, and injected runner failures.

## Reading the report

Before engine execution, the runner atomically replaces the output with an incomplete report containing `completed: false` and `passed: false`. It updates the report at the end, including failures. Infrastructure problems appear in `operational_errors` or the affected replay's `error`; interruption can leave the explicit incomplete report. Invalid CLI arguments or an unwritable output path can prevent initialization of a new report.

The report includes configuration, a trace fingerprint, explicit scope statements, individual contract results, replay results, and the overall `passed` value. `completed` records orchestration completion, not correctness: every selected contract and replay must also pass, with no operational error. PostgreSQL metadata records its server version, `fsync`, WAL writer delay, shared buffers, connection limit, and the adapter's isolation/commit settings. The PostgreSQL connection string is omitted from serialized configuration.

After every phase, the harness compares full physical records and complete per-message outcomes against a serial store implemented with ordinary maps. It also checks structural domain invariants. Separate unit tests contain independently expected provenance coordinates, projection behavior, retained histories, rollback effects, and adversarial missing-candidate behavior; adapter agreement alone could miss an error in their shared handler.

Aerostore's final audit compares raw index postings with committed rows, requires index retirement queues to drain, checks allocation ownership, and verifies that no transactions remain registered. Workers have finite retry and response budgets. The Aerostore replay runs in an isolated coordinator process; the parent terminates its disposable processes if no phase progress occurs for 60 seconds. An unsuccessful worker run invalidates its arena; cleanup does not imply native owner-death recovery, and failure cannot be reported as replay success.

Timing reports include committed messages per second, retry counts, p50/p99 message execution latency, per-phase elapsed time and arena high-water marks, and storage-operation counters. Message latency includes retries and backoff inside the worker. Throughput includes process dispatch and phase barriers; reference-model work and phase-state validation are outside that timing. Successfully committed duplicate/rejected inputs count as committed messages; deliberately aborted inputs do not.

Both backends enable WAL and acknowledge commits asynchronously. The Docker PostgreSQL configuration uses `fsync=on`, `synchronous_commit=off`, and `wal_writer_delay=10s`; existing PostgreSQL is checked for `fsync=on` and its sessions disable synchronous commit. Aerostore drains its WAL writer on successful shutdown. Flush schedules and restart behavior are not established as equivalent by this suite.

Aerostore workers access shared memory directly; PostgreSQL workers use a local client/server connection. Those are the intended single-host paths, with different transport costs. Reported rates describe this bounded fixture and configuration. They are not a production HyperFeed speedup, a replacement-readiness claim, or a crash-recovery validation.

For reproducible validation evidence, retain the JSON report with the tested source revision and exact command. The original sustained-churn gate remains separate; this suite's finite cycles and phase barriers do not replace a long-running degradation test.
