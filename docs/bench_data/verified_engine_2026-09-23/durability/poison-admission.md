# Poison admission regressions

Final review found three admission failures in the optimized candidate before
this repair. These failures were **not** attributed to the original `a382ce3`
baseline. [Before evidence](poison-before.json) retains commands, compiler/source
hashes, and the hash/file inventory of
`target/verification-next/poison-before-source.tar`. Only test code and test-only
phase hooks were added before that run; all three tests failed at their intended
assertions in [the log](poison-before.log).

| Native regression | Before repair | After repair |
| --- | --- | --- |
| Real row codec poisons an unindexed table during WAL preparation | Commit still returned `Ok(1)` | Rejected before accepting bytes or publishing rows; registration cleaned up |
| Managed synchronous writer passes table validation, then waits on an independently held file lock while its table is poisoned | Commit still returned `Ok(1)` | Health rechecked inside the acquired file lock; WAL and row unchanged |
| `/dev/full` causes actual write and rollback failures; an observer checks table admission immediately before file unlock | Poison was still absent at unlock | Detected indeterminate failure publishes poison before releasing the file lock |

[After evidence](poison-after.json) and [its log](poison-after.log) record all three
passing tests with stable source hashes. The tests live in
`wal_writer::poison_regressions` and execute the production commit/file paths.
The phase hooks are compiled only for unit tests. `/dev/full` testing is limited
to Linux.

The fix adds one shared poison observation after acquiring publication guards,
before destination allocation, and aborts rejected transactions. Managed
synchronous append checks the same table's health after acquiring its existing
file lock; detected indeterminate rollback sets poison before that lock releases.
No global commit lock, metadata layout change, or post-acceptance retry was added.

This boundary does not cancel disjoint transactions already admitted or accepted.
Raw `SyncWalWriter::append_commit` does not carry table identity and still requires
caller-managed stop/recovery discipline. Process death during a frame may release
the OS file lock without executing poison. Such owner-death and valid-tail
protocols remain outside this repair, as described in the
[durability contract](../../../../verification/contracts/durability.md).

The earlier performance captures describe earlier sources. These correctness
repairs do not confer performance acceptance on the current candidate.
