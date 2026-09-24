# Native complete-transaction scenarios

This campaign exercises real `OccTable` transactions and intended broken source
variants. It is deterministic native testing, not a formal history-refinement
proof or evidence that P1 is complete.

Two public-API tests add complete application outcomes to the existing cutpoint
regressions. In the creation test, two worker threads first query the same empty
key, repeat the query, stage logical creations in different preseeded physical
slots, and observe their own pending writes. Channels select either the older
or newer transaction as the winner. The loser must reject, leave its physical
row unchanged, and on a fresh transaction observe the single winner. Both
workers finish their retries; the arena has no leftover registrations.

In the key-move test, an older-active or later-starting writer moves a row after
the reader has captured old and empty predicates. The reader also stages a
different row and repeats a lookup. Its full write commit must reject without
leaking that staged row or a posting, and a fresh retry must commit successfully.
The new schedules use channels rather than sleeps and join worker threads before
checking semantic outcomes. Their ten-second waits detect broken test progress;
elapsed time never establishes a correctness result.

Seven existing cases cover the internal seams without adding duplicate test
bodies: actual candidate capture followed by key movement and vacuum before
materialization; crossed empty predicates under commit guards; real destination
allocation failure; rejected and panicking callbacks; rejected WAL acceptance;
poison observed during preparation; and both deregistration/stamping cuts.

Seven controls omit predicate validation, own-write candidate collection,
destination preparation, old-source removal, prepared-destination rollback or
poison publication, or substitute the writer's start ID for a fresh publication
stamp. Each must compile and fail the selected assertion in exactly one test.
Poison testing covers the native poison flag and admission behavior; it is not a
proof of every partial-publication or persistence failure.

```sh
source target/verification-tools/environment.sh
python3 -m unittest discover -s verification/p1_native -p 'test_*.py'
python3 verification/p1_native/run.py --output target/verification/p1-native
```

The runner checks the pinned native compiler, snapshots all four workspace
crates, overlays every current Rust/Cargo input, and builds each mutant in its
own source and target directory. It records native input, compiler, executable,
log and final freshness hashes. It rejects missing tests, unexpected assertion
failures, compilation failures, signals and timeouts. Sources must remain stable
throughout the campaign. Tests run with the default bucket implementation;
the regular transaction/index integration matrix exercises the new public
tests with the sort and bitmap variants too.
