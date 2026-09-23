# Production durability regressions

Five deterministic schedules fail against commit
`a382ce3d06d4fb690ba09632d7b3a38b8902c65a` and pass against the repaired production
engine. [baseline.json](baseline.json) records the immutable source archive hash,
compiler, exact final test-source hash, commands, and individual failure logs.
The baseline archive was changed only by adding the regression test file.
[fixed.json](fixed.json) and [fixed.log](fixed.log) record all 19 new passing tests
and the tested source hashes. These are executable implementation regressions,
not proofs of the full persistence protocol.

| Schedule | Baseline result | Repair |
| --- | --- | --- |
| WAL codec fails during commit | Changed row remains published without WAL | Encode before taking commit guards, then validate, prepare destinations, accept WAL, and publish; reject without leaving published rows or destination postings |
| Another transaction reads that unlogged row and receives synchronous success | Recovery retains output `3` but restores its input to `0`; required relation was output = input + 2 | WAL acceptance precedes visibility, so a rejected predecessor cannot supply a committed input |
| Transaction starts before checkpoint and commits afterward | Its start ID is below the checkpoint cut, so its durable value `7` is discarded | Checkpoint v2 records the active transaction IDs at the cut and replays their later commits |
| Writer commits after checkpoint captures rows but before WAL truncation | Acknowledged value `9` is lost; recovered value is `0` | Keep the existing partition exclusion through checkpoint persistence and truncation |
| Synchronous transaction depends on a still-buffered asynchronous transaction | Its durable output survives without its asynchronous input | Bind a table to one immutable stream; reject incompatible mode/file/ring before publication |

The checkpoint race uses a custom serializer and channels to schedule real
checkpoint and commit APIs; it does not sleep to choose an interleaving. Recovery
uses fresh arenas and the actual checkpoint/WAL readers. It does not simulate
power failure or prove device/filesystem persistence behavior.

Additional tests cover prepared-index rollback, failed asynchronous enqueue and
baseline encoding, checkpoint v1 compatibility, incompatible WAL files/rings,
unlogged writes after stream binding, checkpoint refusal for incompatible or
undrained asynchronous streams, independent and fork-inherited file writers,
single-daemon ownership, joined crash replacement, and immutable daemon file
identity.

The normal in-memory commit path retains its existing partition/predicate locks.
WAL commits hold those locks through acceptance. Synchronous append serializes
complete frames with a file lock; an inherited writer reopens its file after
fork so the lock uses a distinct open-file description. Asynchronous file I/O
uses complete 1 MiB batches under the file lock. Daemon ownership is checked at
startup/join, without a new check on every transaction. Steady-state asynchronous
delta encoding retains one baseline-set lookup per row; failed attempts cannot
claim baselines that never entered the ring.

## Shorter commit critical section

The first repaired candidate encoded records while holding the commit guards.
The matched extended workloads exposed a repeatable slowdown. That candidate is
preserved separately from the original `a382ce3` baseline: the original engine
encoded after unlocking, but its WAL acceptance happened too late for durability.

[lock-scope-baseline.json](lock-scope-baseline.json) binds the preserved first
candidate archive and a test-only patch. Pausing its codec caused competing
indexed writers to fail in both synchronous and asynchronous modes; a third
test found that a changed writer epoch did not invalidate the prepared payload.
The existing panic-cleanup case passed. These three failures are attributed to
that first candidate, not to `a382ce3`.

The optimized implementation copies the final transaction record, encodes its
rows, and serializes the complete frame before acquiring commit guards. The
same commit call then acquires guards, revalidates the transaction, prepares
destinations and accepts that payload before publication. It rejects an epoch
mismatch without enqueue and uses the existing serialization retry path.
Successful baseline-set bookkeeping happens after the guards have been released.

[lock-scope-fixed.json](lock-scope-fixed.json) and its retained log record
42 passing focused tests with stable source hashes, including all 23 tests in
`wal_protocol_regressions.rs`. They cover competing indexed writers during
encoding, rejection of stale prepared values without WAL, panic cleanup,
epoch invalidation, and full-baseline/delta behavior after retry. Native unit
tests additionally check that preparation, acceptance and publication use the
same final-write record. Epoch rechecking does not establish concurrent raw
reset safety; exclusive restart authority remains required.

Scope and compatibility:

- A table binds on its first WAL commit attempt, including an attempt that later
  aborts. The binding permits independent handles to the same synchronous file
  or the same asynchronous ring. It rejects mixed streams, unlogged writes and
  direct recovery writes afterward; read-only commits remain allowed.
- This phase requires draining/stopping workers and exclusive cold recovery to
  change a bound stream or durability mode. A unified stream with synchronous
  barriers/group commit remains future work. The file checkpoint API refuses
  asynchronous-bound tables because it cannot establish their daemon/file cut.
- Checkpoint v1 remains readable, but missing active-transaction information in
  a previously inconsistent v1 checkpoint cannot be reconstructed. New writes
  use v2. The shared-memory layout is versioned separately; old mappings must
  not be reinterpreted as the changed table/ring layout.
- A daemon must be joined before replacement; reset of an abandoned ring is an
  exclusive restart operation after all old processes have stopped. Public raw
  ring pop/reset operations still require their stated ownership discipline.
- Truncated/corrupt WAL frames still cause explicit recovery failure. Automatic
  recovery of an acknowledged prefix past a torn final frame, full I/O fault
  exploration, and a complete implementation refinement proof remain open.

Performance conclusions belong to the matched engine/WAL campaign in the parent
directory. Passing these correctness tests does not establish a speedup.
