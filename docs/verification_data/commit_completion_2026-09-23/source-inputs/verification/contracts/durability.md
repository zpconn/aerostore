# Native WAL and checkpoint boundary

This phase repairs reproduced bugs in the existing `OccTable`/`OccCommitter`
path. Its regression tests and finite models are supporting evidence. They do
not close the `DURABILITY-IMPLEMENTATION` proof obligation.

## Publication

Commit copies the final private writes and encodes their complete WAL payload
before acquiring predicate and row-partition locks. Preparation returns a
one-shot acceptance callback inside the same commit call; callers cannot mutate
the transaction between preparation and validation. The transaction's active
registration retains its base versions during this preparation, under the
existing retention contract.

After acquiring the locks, commit validates its read and write dependencies.
A concurrent writer can make the prepared transaction stale; validation must
reject it without accepting its WAL payload. Commit allocates destination
postings while source postings and row heads still contain the old state. The
WAL callback then accepts the already encoded payload. Only afterward may commit
remove source postings, publish rows, deregister, stamp predicates and release
the locks. Ordinary in-memory commits use the same driver with the write-ahead
branch eliminated at compile time.

Synchronous acceptance includes `fdatasync`. Asynchronous acceptance means the
complete record reached the selected shared ring; it is not a durable
acknowledgement. A preparation/codec rejection aborts the unpublished transaction
before any destination allocation; unwinding runs that cleanup too. An acceptance
rejection rolls back prepared destinations and aborts the transaction. Caught
unwinding at acceptance runs the same rollback. Failed abort cleanup poisons the
table.
An I/O failure whose appended tail cannot be durably removed, or a publication
failure after WAL acceptance, poisons the table and reports an indeterminate
outcome. Recovery may include that transaction; callers must not automatically
retry it as an aborted serialization conflict.

Commit observes the table's poison flag again after acquiring publication
guards, before allocating destinations or accepting WAL. A transaction whose
preparation outlasted a detected failure is rejected and aborted. Native
synchronous append also checks the same table's health after acquiring the WAL
file lock; a detected indeterminate rollback publishes poison before releasing
that lock. Thus a later managed append for that table cannot pass the file-lock
health check after the detected failure. This does not cancel disjoint work
already admitted or accepted. Raw `SyncWalWriter::append_commit` has no table
health boundary and requires caller-managed stop/recovery discipline. Death in
the middle of a frame can release the OS lock without executing poison; owner
death and exclusive recovery remain open obligations, not a general valid-tail
protocol supplied by this check.

The asynchronous acceptance step rechecks the writer epoch used for encoding.
A mismatch rejects the prepared payload as a serialization failure, with no
enqueue or publication, allowing the existing clean-abort retry path. A later
attempt clears stale baseline knowledge and emits full records as required.
Successful full-baseline bookkeeping runs after the table commit releases its
guards; it never records a rejected enqueue. The epoch check is not an atomic
protocol for concurrent destructive reset: exclusive restart/reset authority
remains a lifecycle assumption.

Independent synchronous file handles lock complete frames; an inherited writer
reopens its descriptor after `fork` so the processes do not share flock
ownership. A ring admits one daemon until its prior owner is joined and retains
its file identity across restarts. Raw ring/reset APIs require exclusive owner
discipline. Warm attachment assumes an already consistent, quiescent arena;
it is not a substitute for cold recovery after interrupted publication. Full
lifecycle/owner-death enforcement remains the separate `LIFE-01` obligation.

The first WAL commit attempt binds each table to one WAL stream: either a synchronous file identified by
device/inode, or an asynchronous ring in its arena. Multiple worker committers
may use the same stream. A failed first attempt still leaves this conservative binding in place. Changing modes, files or rings after binding is
rejected, as are subsequent writes through the unlogged commit/bootstrap
routes. This closes the reproduced case where a synchronous acknowledgement
depended on a write still buffered in another stream. Live stream migration and
mixed synchronous/asynchronous acknowledgements through one ordered stream are
future protocol work. To change streams now, stop workers, drain the old writer,
and recover exclusively into a fresh mapping from the old durable inputs.

The initial seed/recovery state must itself be supplied by the recovery inputs.
Binding a WAL does not retroactively make arbitrary earlier in-memory work
durable. Physical seeding and recovery remain exclusive bootstrap operations.
WAL records belong to one table's row-ID namespace; sharing a file/ring across
independent tables is outside this native recovery contract.

## Checkpoint cut

The checkpoint holds the existing all-partition lock set from row capture
through durable checkpoint publication and WAL truncation. Transaction starts
remain possible. The checkpoint records both the allocated-ID cutoff and the
IDs active at capture. Replay includes records above the cutoff **or** belonging
to that active set. A start ID alone is not a commit/log position.

The temporary checkpoint is synced before rename; its containing directory is
synced before truncating and syncing the WAL. Whole-frame appends and truncation
coordinate with a file lock. Version 2 checkpoint files store the active set;
version 1 files remain readable, but reading an old file cannot recover data
already lost by its original unsafe checkpoint cut.

This checkpoint API accepts an unbound bootstrap table or a table bound to the
same synchronous WAL file. It rejects an asynchronous stream, whose ring/file
cut requires a separate coordinated API. It does not infer a safe transition
from a process-local configuration change.

Shared-memory layout 5 and boot metadata 7 reject older mappings. Preserve
existing WAL/checkpoint files and use exclusive cold recovery for an upgrade.
The per-table header also validates its format on direct attachment.

## Evidence and remaining obligations

`wal_protocol_regressions.rs` contains deterministic production regressions;
the retained evidence records original failures and corrected results.
`CheckpointCut.tla` separately checks the partition-excluded active-set cut,
with crash actions between persistence stages, deliberate mutants and positive
witnesses. The conditional Verus commit proof checks the actual driver's call
ordering and cleanup, assuming the named primitive contracts.

Still open: refinement of those primitives, general dependency/history
composition, filesystem and process-failure semantics, all codec payloads,
resource ownership, arbitrary owner death, counter exhaustion, and safe live
stream transitions. Complete durable append/checkpoint are trusted persistence
steps in the finite model. Torn/corrupt WAL input remains a reported recovery
error rather than a proved recoverable prefix. Legacy WAL/recovery APIs have
separate coverage; they do not inherit these native-path repairs or proofs.
