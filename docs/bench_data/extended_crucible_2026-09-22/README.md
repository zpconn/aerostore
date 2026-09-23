# Extended Crucible validation — 2026-09-22

The extended benchmark is implemented and its bounded message replays pass. **The complete correctness gate fails on three Aerostore native contracts.** PostgreSQL passes all six counterpart contracts. These results establish the synthetic cases described in the [runbook](../../extended_crucible.md), not compatibility with proprietary HyperFeed code.

## Tested source and environment

The tested working tree starts at commit `013e7178a0c638343a8eb4e24deb8b64b706ada3` and includes the new extended benchmark and fallible secondary-index query API. It was not yet committed when these results were collected. [source-sha256.txt](source-sha256.txt) fingerprints the changed/new implementation files, manifests, and dependency lockfile; unchanged implementation files come from that base commit. Verify from the repository root with:

```sh
sha256sum -c docs/bench_data/extended_crucible_2026-09-22/source-sha256.txt
```

Host: Intel Core Ultra 9 285K, 24 visible CPUs, Linux `6.6.87.2-microsoft-standard-WSL2`, Rust `1.93.1 (01f6ddf75 2026-02-11)`. The disposable database was PostgreSQL `16.13 (Debian 16.13-1.pgdg13+1)`, with `fsync=on`, serializable transactions, session `synchronous_commit=off`, and `wal_writer_delay=10s`. The JSON records the remaining database settings. Timing includes the local transport and fixture coordination described in the runbook; it is not a production speed comparison.

## Results

| Check | Result | Evidence |
| --- | --- | --- |
| Both engines, 32 families, four cycles, eight workers | Each replay passes 3,840 deliveries and 108 phase comparisons; complete gate exits 2 on the three native Aerostore failures | [JSON](both_32f_4c_8w.json), [log](both_32f_4c_8w.log) |
| Aerostore replay, 64 families, 16 cycles, eight workers, 128 MiB arena | Passes 30,720 deliveries and 432 phase comparisons; exits 0; arena high-water mark 4,717,472 bytes | [JSON](aerostore_64f_16c_8w_128m.json), [log](aerostore_64f_16c_8w_128m.log) |
| Workspace release tests | All nonignored tests pass, including 170 core unit tests; two existing tests remain explicitly ignored | [log](workspace-tests.log) |
| Final focused model/adapter tests | All 21 pass, including four safety tests added after the workspace run | [log](model-tests.log) |
| Original Crucible, Aerostore only, 30 seconds, 128 MiB | Passes 11,539,937 operations, exact index comparison, allocation audit, and reclamation checks | [log](original_crucible_30s.log) |
| Undersized arena | Exits 2 with a failed replay and replaces a seeded stale success report | [JSON](undersized_arena_failure.json), [log](undersized_arena_failure.log) |
| Killed worker | Exits 2 promptly with a failed replay and replaces a seeded stale success report | [JSON](killed_worker_failure.json), [log](killed_worker_failure.log) |

Of the 3,840 deliveries per engine in the shared run, 3,712 commit and 128 deliberately abort. Each engine reports 640 duplicate outcomes, 128 rejected inputs, 128 savepoint rollbacks, and 10,368 expired records. All physical state and emitted outputs match the serial reference after every phase. Aerostore's final exact index and allocation ownership audit passes. The larger replay has 29,696 commits and 1,024 intentional aborts, with the same full-state/output checks and final audit.

The native failures are:

1. `serializable_absent_candidate_creation`: competing transactions can both commit after observing the same empty candidate range.
2. `committed_row_and_index_visibility`: a committed row can be visible before its changed index entry is published.
3. `index_candidates_respect_transaction_snapshot`: moving an index key can hide an older row version from a reader with an older snapshot.

The native savepoint/abort, multirow snapshot, and concrete-row write-skew contracts pass on both engines. The failing contracts were not suppressed or treated as expected success.

## Reproduce

The exact matrix commands and observed exit codes are also retained in [validation.json](validation.json). From the repository root:

```sh
cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine both --families 32 --cycles 4 --workers 8 --seed 20260922 \
  --output target/extended-both-32f-4c-8w.json

cargo bench --offline -p aerostore_core --bench hyperfeed_extended_crucible -- \
  --engine aerostore --mode replay --families 64 --cycles 16 --workers 8 \
  --shm-mib 128 --seed 8675309 --output target/extended-aero-64f-16c.json

cargo test --offline --workspace --release -- --test-threads=1

cargo test --offline -p aerostore_core --release --test extended_crucible_model -- \
  --test-threads=1

AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_DURATION_SECS=30 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
cargo bench --offline -p aerostore_core --bench hyperfeed_crucible
```

The first command currently returns a nonzero exit code; inspect its completed JSON rather than treating process completion as a correctness pass. Future engine repairs should make each contract pass without weakening its assertion.

The failure-path checks used the compiled benchmark directly. [failure-path-validation.json](failure-path-validation.json) records its exact arguments and exits. Each check first wrote a fake `passed: true` report with a `stale_success` marker, then verified a nonzero exit, `passed: false`, removal of the marker, and a concrete replay error. The allocation case requested 4,096 families in 32 MiB. The worker case requested 64 families and 64 cycles with eight workers, then sent `SIGKILL` to one `--internal-worker` found exclusively among that invocation's descendant processes. Both checks terminated promptly. This validates failure reporting and disposable-process cleanup, not recovery of an arena after owner death.

The 30-second original Crucible run is a regression check. Neither it nor the short, phased extended replays replace a multi-hour degradation or crash-recovery test.
