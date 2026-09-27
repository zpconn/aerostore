# Optional native retry observations

The `retry-diagnostics` Cargo feature records the first rejecting native branch
within one synchronous operation. It is off by default. Feature-disabled builds
contain no native observation calls; their public observation API returns no
events. Feature-enabled builds start disabled on every thread and can enable or
disable observations at runtime. This supports controlled comparisons within one
binary, but it does not make that binary a performance baseline for the default
build.

`set_enabled(bool)` resets pending state. An adapter calls `clear()` immediately
before an operation and `take()` immediately after its result, before cleanup or
another operation can reject. `take()` consumes the event. Events contain a
finite `Cause`, an optional index-header offset and an optional row ID. The
mechanism uses one thread-local `Cell` with a fixed-size first-event slot, no
allocation, no shared counters or engine writes, and `try_with` so inaccessible
TLS during destruction is harmless. It does not follow an async operation across
threads. Concurrent adapters on the same thread must coordinate their scopes.

Names identify branches, not inferred semantic causes. For example,
`lookup_post_snapshot_stamp` alone does not establish that the conflict was
unnecessary, nor that index versioning would solve it. The controlled schedules
in `aerostore_core/tests/contention_diagnostics.rs` separately demonstrate a
disjoint range update and a historical indexed read reaching that same branch.
Their existence does not establish either case's fraction of workload retries.
Commit retains its existing short-circuit order, so the observed branch is the
first decision encountered, not an inventory of every possible blocker.

| Native rejection | Observation |
| --- | --- |
| Index bucket acquisition exhausts its bounded attempts | `index_bucket_busy` with index |
| Lookup observes a stamp newer than its snapshot | `lookup_post_snapshot_stamp` with index |
| Repeated lookup observes a changed captured stamp | `lookup_changed_captured_stamp` with index |
| Commit observes a previously failed index dependency | `sticky_index_conflict` |
| Commit predicate stamp is changed or newer than snapshot | `predicate_validation_stamp` with index |
| `lock_for_update` finds another owner, or loses its ownership CAS | `lock_for_update_held`, `lock_for_update_race` with row |
| Read, ordinary write, or dirty-mask write finds another owner | `read_row_locked`, `write_row_locked`, `write_dirty_row_locked` with row |
| Commit finds another owner of a dependent row | `commit_row_locked` with row |
| Read validation observes changed identity or a later committed deletion | `read_version_identity_changed`, `read_version_deleted_after_snapshot` with row |
| Write validation observes changed head or nonzero base xmax | `write_base_head_changed`, `write_base_xmax_set` with row |
| Visible-version traversal reaches its existing safety bound | `visible_chain_limit` with row |
| Commit cannot acquire its row partitions within the budget | `partition_lock_busy` |
| Prepared asynchronous WAL payload belongs to an earlier writer epoch | `wal_writer_epoch_changed` |

Resource exhaustion and index/poison errors remain their existing error types.
Failed publication CAS operations become fatal poisoned-index errors rather than
ordinary serialization retries; they have no retry observation. Exclusive
recovery API precondition/CAS errors are outside this workload instrumentation.
An absent event must remain an unclassified origin, never an invented diagnosis.

## Proof boundary and change detection

The existing source adapters continue to claim only their **default feature
configuration**. They do not establish refinement of the feature-enabled code.
`normalize.py` erases only 18 finite, reviewed, exactly feature-guarded calls,
including their exact argument token patterns. An unknown cause, changed
arguments, another call, a block wrapper or an unguarded diagnostic call is
rejected. It does not erase arbitrary conditional Rust. The shared adapter
`verification/concurrent/generate.py` pins this normalizer's file hash, so the
existing per-component receipts' generator binding includes the dependency.
The standalone refinement receipt validator also reads that pin from the
already source-bound generator and checks the current normalizer's hash.
Changing only the normalizer therefore rejects a previously valid receipt,
without relying on a new proof run or on the encompassing pilot fingerprint.

The original instrumentation baseline is preserved byte-for-byte in
[`default_sources_4da551b.json`](default_sources_4da551b.json), tied to commit
`4da551bbc2a17614ae11795cfee7764cb252d56a`. It rejects the subsequent prior-prefix
capture optimization, as it should: that change affects the native default
program and must not disappear during diagnostic normalization.

[`default_sources.json`](default_sources.json) now records the
[explicitly reviewed capture transition](capture_prefix_review.md) from immutable
commit `94ad54bef275dd4db0ce382569bc239e931ada87`. Its
[archived OCC source](occ_partitioned_94ad54b.rs) is hash-bound and first checked
against the original instrumentation baseline. The checker then derives the
new expectation using exactly two native token edits: capture the prior read-set
length before the stamp loop, and choose the whole-vector search only while its
length still equals that prefix, otherwise the bounded slice search. This
choice is evaluated for each bucket. One exact `cfg(test)`
child-module declaration is also allowed at EOF. No other native change is
accepted merely by updating the expected digest. WAL's original projection is
retained. The optimization's tokens remain present after normalization.

The manifest also pins every observation's position and adjacent tokens and
requires one site for each of the 18 causes. The capture edits shift OCC offsets;
all observation arguments and rejecting branches remain unchanged. Two windows
include neighboring search syntax that changes; the checker pins their exact
old/new token sequences and keeps every other window unchanged.
Moving or duplicating a legal observation still fails even when erasing it would
leave the same default program. `check_default.py` uses preserved artifacts,
not mutable HEAD or a network checkout. The existing concurrent adapter unit
suite invokes this check; the frozen-boundary source set also includes this
directory. Baseline or site updates require explicit review.

This is lexical change detection and a deliberately narrow source abstraction,
not a Rust equivalence proof. The new capture transition relies on unique query
bucket IDs and preservation of the existing dependency prefix; its component
proofs, canonicalization premises and native regressions are separate evidence.
Old component receipts are stale after the native change, even where generated
proof bodies happen to remain identical. Native enabled/disabled tests support
the instrumentation's decision-preservation argument; they do not prove every
feature-enabled execution, TLS/platform behavior or timing noninterference.

## Focused checks

```sh
python3 verification/retry_diagnostics/check_default.py
python3 -m unittest discover -s verification/retry_diagnostics -p 'test_*.py'
python3 -m unittest discover -s verification/concurrent -p 'test_*.py'
source target/verification-tools/environment.sh
cargo test --offline --locked -p aerostore_core --test contention_diagnostics
cargo test --offline --locked -p aerostore_core --features retry-diagnostics --test contention_diagnostics
cargo test --offline --locked -p aerostore_core --lib retry_diagnostics
cargo test --offline --locked -p aerostore_core --features retry-diagnostics --lib retry_diagnostics
```

Native schedules assert precise index/row context for predicate rejection, row
validation, lock rejection, blind-write conflict and sticky failure. Additional
crate-local tests exhaust index/partition budgets and change the WAL writer epoch
during payload preparation; the WAL case asserts rejection before any enqueue or
publication. Observation tests cover first-event preservation, consumption,
clearing, runtime disablement and thread isolation. Rare defensive branches such
as changed version identity, the traversal cap and CAS ownership races remain
site-reviewed rather than individually forced by these tests. The checker tests
include invalid guards/calls/arguments, moved or duplicated sites, and a real
native predicate mutation. Capture-transition negatives additionally reject a
zero or shortened prefix, restoration of the growing-vector search, altered
full-vector arm conditions, identity/stamp checks, tampered historical artifacts,
and changed WAL tokens,
including attempts to approve a mutant by rewriting its expected digest.
Missing/null transition metadata and a rewritten manifest claiming the legacy
revision are also rejected; only the exact preserved legacy manifest can use
the historical path.
