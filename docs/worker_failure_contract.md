# Local worker failure contract

**AeroStore's direct shared-memory worker mode does not currently satisfy the requirement that surviving workers keep processing all affected work after another worker is killed, without restarting the engine.** The probes below preserve the working implementation and measure this limitation. Their Rust tests pass when they reproduce the current behavior; the separate availability result is explicitly **false**. The database-owned prototype tested later in this document has a different ownership boundary.

The evidence is [the survivor report](worker_failure_data/2026-09-24/survivor_availability.json), [test log](worker_failure_data/2026-09-24/test.log), and [source/environment manifest](worker_failure_data/2026-09-24/manifest.json). These observations are against the production sources recorded in that manifest, with a new integration test and no engine changes.

## Required behavior

A killed worker may lose its uncommitted message, and messages may retry. Other workers must keep their existing mappings and continue completing transactions, including work on identities touched by the dead worker. Reclamation must eventually release abandoned retention horizons and private allocations. An indeterminate commit needs an explicit recovery and message-redelivery rule.

Stopping every worker and rebuilding the mapping is a disaster-recovery fallback. It does not meet this availability requirement, regardless of how quickly a small fixture rebuilds.

## Observed behavior

The survivor process attaches before the victim dies, retains the same arena/table/index handles, and submits work after the supervisor confirms `SIGKILL` and reaps the victim. Same-row messages receive up to 16 attempts. A second message updates another logical row and verifies its equality query. A range query whose logical result contains only that disjoint row checks whether broader index protection causes collateral disruption. No registration clearing, lock reset, or mapping replacement occurs while this survivor is alive.

| Controlled kill point | Same-row work | Disjoint update/equality | Disjoint range query | Retention after death |
| --- | --- | --- | --- | --- |
| Attached, idle worker | Completes | Completes | Completes | No registration |
| Open transaction with a private write | Completes | Completes | Completes | One orphan registration remains |
| Holding `lock_for_update` | Fails all 16 attempts | Completes | Completes | One orphan registration remains |
| Holding optional `lock_indexed_rows` guard | Guard unavailable on all 16 attempts | Completes | Completes | No transaction registration needed |
| Native commit holding predicate/partition guards, before WAL acceptance | Fails all 16 attempts | Completes | Fails with serialization conflict | One orphan registration remains |

The optional indexed-update guard applies to callers that participate in that coordination protocol. Its death does not, by itself, block every native transaction. The native commit case needs no such application guard: it exercises the engine's actual commit path.

For that case, the supervisor holds the WAL file's `flock`. Linux `/proc/locks` confirms the child is waiting for that file lock before it is killed. At this point `OccCommitter` has acquired predicate and partition guards and prepared index destinations. It has not accepted the transaction into WAL or published its row heads. The test confirms unchanged WAL bytes and both original row values. Affected predicates remain unusable after the death. Ranges protect every predicate bucket, so a range with a disjoint logical result also fails.

These are selected, deterministic failure points. A live survivor is present at the kill, but the probe does not claim continuous application throughput across a randomly timed crash.

## Why warm attach is insufficient

[`ProcArray`](../aerostore_core/src/procarray.rs) stores transaction IDs and snapshot horizons, without worker PID or automatic owner-death cleanup. A private-write victim leaves a pinned horizon: eight later updates complete, but vacuum reclaims none of their eight old versions. Once all workers are stopped, exclusive warm attach clears the orphan registration and vacuum can reclaim those eight versions. It does not rewind the allocator or recover the dead worker's unreachable private version.

[`ShmMutex`](../aerostore_core/src/shm_lock.rs) and the OCC partition locks have no owner-death handoff. Only the ProcArray lifecycle latch has an exclusive startup reset wired into [`open_boot_context`](../aerostore_core/src/bootloader.rs). The row-lock and indexed-update-guard probes confirm that exclusive warm attach clears registrations but does not make those abandoned locks usable. The native commit probe confirms the same limitation for affected index predicates.

`open_boot_context` clears **every** ProcArray registration on warm attach. Its caller must first exclude all previous workers. It must not be used to attach a replacement worker while survivors still run. The probes use ordinary shared mapping attachment for workers and call the boot path only after stopping the fixture's workers.

The existing [`tmpfs_warm_restart_chaos`](../aerostore_core/tests/tmpfs_warm_restart_chaos.rs) exits its writer after all commits complete. Its warm-boot timing remains useful for that case; it does not establish recovery after death inside a transaction or critical section.

## Timing observations

The archived run uses disposable 16 MiB `MAP_SHARED` file mappings and release builds. The survivor fixtures contain three rows; the recovery fixture contains two. These are functional failure probes, not HyperFeed-scale performance measurements.

- Supervisor `SIGKILL` plus reap took approximately **1.1 ms**, including its 1 ms polling interval.
- Idle-worker death required no recovery; the supervisor's first verified update completed in **0.014 ms**.
- Exclusive warm attach took approximately **0.02 ms**, but left abandoned row/index guards unusable.
- Creating a fresh arena/table, restoring deterministic bootstrap values, replaying one durable transaction with two writes, rebuilding/binding the index, and committing/querying the first replacement message took **1.86 ms**. [Recovery observation](worker_failure_data/2026-09-24/native_commit_before_wal_acceptance.json)

The recovery measurement excludes failure detection, stopping other workers, realistic checkpoint/WAL sizes, remote output reconciliation, and process restart. It is one small observed run, not an SLA or a claim that production interruption would take two milliseconds. The intact WAL is replayed into entirely new lock/allocator/index state. The failed mapping is never repaired in place.

## Limits and next acceptance tests

This probe does not kill a worker after WAL acceptance, between individual row publications, while updating allocator/skiplist metadata, or while holding the ProcArray lifecycle latch. It does not establish safety after arbitrary partial publication. The existing error-poisoning path is ordinary Rust control flow; `SIGKILL` does not run it or guard destructors. Warm registration clearing alone must not be assumed to restore atomicity after such a death.

The next ownership design must demonstrate survivor progress across those cuts, bounded orphan cleanup, complete queries, and an explicit outcome for in-doubt messages. Reclaiming a lock requires establishing which mutation completed and preserving survivor snapshots; blindly clearing a dead-looking lock or every registration is not that protocol.

A database-owned mutation path is a candidate for investigation if worker failure can be isolated at that boundary. These measurements demonstrate a requirement the present engine does not meet; they do not establish that a separate-process design is faster or that a specific replacement is necessary.

## Reproduce

Run from the repository root on Linux. The tests create disposable mappings under the system temporary directory and bound child checkpoints/completion by ten seconds. They kill only child processes they created. Use an absolute report path because Cargo executes integration tests from the package directory.

```sh
mkdir -p target/worker-failure-contract
AEROSTORE_WORKER_FAILURE_REPORT_DIR="$PWD/target/worker-failure-contract" \
  cargo test -p aerostore_core --release --test worker_failure_contract \
  -- --nocapture --test-threads=1
```

There are six parent characterization tests and one subprocess entry test. A successful test run means the observations match the declared current behavior. Architecture acceptance must separately require `required_worker_failure_availability_satisfied: true` in `survivor_availability.json`; the current report records `false`.

## Database-owned service prototype — 2026-09-25

An experimental service now executes every storage operation in the database
owner process. Application workers execute their message handlers and issue
interactive storage requests over Unix sockets or TCP; they have no arena,
transaction registration, private row allocation, or native guard. Each session
has its own executor. The coordinator still owns the native WAL and collector
processes. This changes the ownership boundary without changing production
engine source or placing the matching algorithm inside the database.

The [retained service evidence](bench_data/architecture_2026-09-25/service-availability/README.md)
passes selected application-client SIGKILL tests in debug and release builds.
A separate survivor process establishes its session before each death and
keeps that session while subsequently committing same-row and disjoint-family
transactions. All tests use the actual native contention adapter. No mapping,
lock, or registration is reset while the survivor runs.

| Client killed while owner is paused at this cut | Owner's result | Survivor result |
| --- | --- | --- |
| Request accepted | Open transaction aborts after disconnect | Both identities complete |
| Query returned | Transaction aborts and registration disappears | Both identities complete |
| Private write returned | Private write aborts and registration disappears | Both identities complete |
| Commit request accepted | Owner finishes commit; resolver reports Committed | Both identities complete |
| Committed outcome recorded, reply not sent | Resolver reports Committed | Both identities complete |

The service uses bounded frames, active sessions, transaction lifetimes,
operation counts, and retained commit outcomes. Disconnect and idle timeout
abort unfinished transactions. Once a commit request is accepted, client death
does not cancel the owner's native operation. An opaque session/sequence token
allows a replacement client to resolve a lost commit reply while its outcome
is retained. Pending, Committed, AbortedConflict, Indeterminate, and Unknown are
distinct. Expiry, acknowledgement, or a new service incarnation can make an
outcome Unknown; that result never authorizes automatic replay.

The campaign also checks savepoint rollback and 24 repeated private-write
disconnects. Native active registrations return to zero, indexes agree with the
table, and the last twelve allocation high-water samples are equal. In this
small release fixture, kill confirmation through both survivor commits took
2.26–3.39 ms with Unix sockets and 3.41–6.34 ms with loopback TCP. Those timings
include supervisor polling and protocol work. They are functional observations
on 16 rows and a 32 MiB mapping, not architecture throughput or latency claims.

Eight additional protocol regressions check bounded admission/outcome retention,
lost replies and expiry, malformed/replayed/partial requests, idle versus
transaction deadlines, backpressured replies, retention of an indeterminate
commit token, exhausted session identifiers, and failure after an executor
panic. An executor initialization failure, panic, or failed cleanup stops
admission and closes existing sessions; if a native operation cannot finish,
clean shutdown fails explicitly and the isolated owner must be terminated. That is an engine failure, distinct from the
application-worker deaths that these tests contain.

The [separate TLA+ model](../verification/service_protocol/README.md) checks a
finite two-client ownership/outcome protocol and rejects four intentional
mutations. Accepted commits are assumed successful; native conflict, fatal, and
indeterminate results are outside that model. It assumes atomic native
operations and weak fairness; it is not an implementation-refinement proof. These service cuts do not cover killing the
database owner or WAL/GC processes, arbitrary partial native publication, or
cross-transaction asynchronous recovery. The original direct-mode availability
failure remains unchanged, and the service still needs realistic workload and
performance qualification before an architectural decision.
