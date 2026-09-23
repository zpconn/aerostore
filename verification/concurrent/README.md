# Native commit orchestration: conditional proof

This campaign checks the **existing production commit driver and its callback cleanup**, extracted afresh from `aerostore_core/src/occ_partitioned.rs`. It does not introduce another database implementation or execute proof state in the database. The default and write-ahead paths remain the same production code with a const policy parameter.

```sh
python3 verification/concurrent/generate.py
python3 verification/concurrent/test_generate.py
python3 verification/concurrent/run.py
```

`run.py` uses the already pinned Verus toolchain, rejects stale generated code, individually checks every required operation, and retains current source/tool hashes and negative-control logs under `target/verification/concurrent`. A missing tool, incomplete check, source change, unsupported adapter syntax, or wrong-kind mutation failure fails the campaign. The generated file is [commit.verus.rs](commit.verus.rs); its fixed event contracts are [contracts.rs](contracts.rs).

## What is proved

The extracted `commit_with_record_impl`, `prepare_before_publish`, `abort_preparation`, `invoke_before_publish`, and `rollback_prepared_commit` bodies satisfy their event contracts for **any implementation of the remaining primitive interface**, for both values of the write-ahead policy and arbitrary transaction/vector contents. This is a source-bound conditional control-flow proof, not a concurrent transaction-history refinement theorem.

The event invariant records lock acquisition, each validation, destination preparation, the write-ahead callback, destructive source removal, possible row publication, deregistration, stamping, poisoning, and explicit release. The proof establishes:

- Both requested guard sets have been acquired and remain held when validation and publication stages run; proving that those sets cover every required row/predicate is a separate primitive obligation. All four validation checks must succeed before preparing destinations or changing sources/rows.
- An ordinary commit with writes must check that the table is not durably bound, under the native binding-read contract. Read-only commits are unaffected.
- After acquiring both guard sets, every commit must recheck table health before destination preparation or WAL acceptance. Rejection releases its guards and invokes abort cleanup before returning. This records one admission observation; it does not assert instantaneous global exclusion against a different, already-running writer that poisons the table later.
- On the write-ahead branch, immutable record and payload preparation precede lock acquisition. Acceptance remains after validation and destination preparation under both guard sets, and precedes destructive source removal and row publication. The native call retains the same record and exclusive transaction borrow throughout; the event proof does not prove payload bytes correspond to that record.
- A successful commit response follows row publication, successful deregistration, successful stamping, and release of both classes of guards. Stamping cannot precede deregistration.
- Every normal return satisfies the condition for releasing scoped guards. After possible partial row publication, an error must have poisoned the table. Destination preparation errors must have undone additions or poisoned the table under the stated primitive contract.
- Callback errors execute the actual rollback/abort orchestration before returning. The caught-unwind branch executes that same cleanup before resuming the unwind; the abstraction of `resume_unwind` requires safe guard release. If abort fails during cleanup, the extracted body explicitly poisons the table.

- Record preparation errors and pre-lock codec errors or caught panics require abort before return/unwind, or poisoning if cleanup fails. No destination has been allocated yet. The preparation primitive contract forbids WAL acceptance and table publication.

The thirteen semantic negative controls remove predicate validation, bypass the bound-WAL write check, bypass the guarded health recheck, omit its failure cleanup, omit poisoning after partial row publication, bypass the write-ahead callback, omit acceptance error cleanup, omit acceptance caught-unwind cleanup, stamp before deregistration, omit record-copy error abort, omit pre-lock codec error abort, omit pre-lock codec caught-unwind abort, and move acceptance before validation. Each must reach Verus and fail a precondition or postcondition; a parser failure does not count. An adapter test also rejects a write-ahead wrapper incorrectly selecting the ordinary branch.

A supporting named lemma checks that the callback contract admits success, unchanged failure, and failure that poisons, for any initial event state. The latter permits the real WAL callback to poison the table after an indeterminate append whose rollback also failed. Error/caught-panic results may add poison but cannot clear it; successful callbacks preserve poison. This small non-vacuity check does not establish that the entire native primitive interface is implemented correctly.

No runtime locks, atomic orderings, allocations, or proof bookkeeping are introduced by this campaign. The proof alone does not establish the performance of the durability repair that it checks.

## Exact source relationship and trusted adaptation

The adapter preserves executable body tokens, including branches, `?` propagation, short-circuit validation, and local-set cleanup, subject to this explicit abstraction list:

1. Native row/index/guard/error types become abstract representations and an event view. The actual `&self` receiver becomes a mutable proof-side event driver; this represents observations of one operation and does **not** claim exclusive ownership of the real shared database. Row contents, index contents, record payloads, and the generic error conversion are not proved.
2. Explicit `drop(locks)` and `drop(index_locks)` calls become release events. Implicit Rust/RAII drops on scope exit are not modeled as executing verified code; instead the postcondition proves that release is allowed at every normal exit. Native guard ownership, actual Drop code, and process-shared acquire/release visibility remain separate obligations.
3. Each application closure's `catch_unwind(AssertUnwindSafe(...))` expression becomes a primitive returning success, application error, or caught panic. Preparation cannot accept WAL; acceptance has its separate guarded contract. The actual three-way matches and cleanup branches are retained. `resume_unwind` becomes a diverging primitive requiring safe release. Native unwinding, abort-on-panic builds, allocation panic, and panics inside other primitives remain outside this proof.
4. The shared-arena recycle-cache call is projected to an opaque method with the same result flow. One formatted error payload becomes an abstract error variant; error/Ok control flow is retained. The one `cfg(test)` hook is excluded exactly as in a production build.

5. Native `P: FnOnce(...) -> Result<F, E>` fixes the returned closure type. Since the abstract interface omits `FnOnce`, its call spells out the same `P, F` type arguments; no executable operation is changed.

The three public/private wrapper bodies are checked against their exact policy bindings. Unknown executable calls remain in the generated operation and must typecheck against a declared contract; the adapter does not silently discard them. The adapter and event contracts belong to the reviewed frozen verification boundary. This is a deliberately restricted trusted transformation, not Charon/Aeneas extraction of the concurrent module.

## Remaining primitive obligations

The trait declarations are assumptions, not certified implementations. No native algorithm is labeled `external_body` or hidden behind `assume`; the generic theorem explicitly quantifies over implementations meeting these contracts. That distinction still leaves real work:

| Boundary | Required native justification |
| --- | --- |
| Acquisition/release | Actual guard ownership and lock coverage; row and predicate exclusion; lock lifetime through all exceptional exits; weak-memory and mmap visibility |
| Validation | Predicate capture completeness, row/base conflict decisions and their connection to a legal serialization history |
| Health admission | Native poison visibility and abort cleanup; synchronous WAL recheck under its file lock. The event theorem proves the guarded table recheck occurs, not global cancellation or absence of later responses from previously admitted writers |
| Destination allocation/rollback | Failed preparation leaves no visible addition, or poisons; rollback removes exactly owned additions without losing unrelated postings |
| Source removal and row publication | Correct postings/rows, all-or-poison failure semantics, and accurate prepared WAL record contents; cached skiplist removal must preserve the guarded search window and confirm all-lane detachment before retirement |
| Record/payload preparation | Immutable and retained row values; the same prepared transaction record determines the encoded bytes and eventual publication; preparation does not accept WAL or publish |
| Callback invocation | The WAL callback's successful return really establishes the selected durability condition; this proof records that it returned successfully |
| ProcArray and stamps | Correct deregistration, snapshot membership, fresh monotone publication stamps, and exhaustion handling; the event proof establishes call ordering, not atomic clock correctness |
| Memory/runtime | Safe accesses and allocation/reclamation, absence of unmodeled panics, compiler/RAII behavior, and successful allocation where required |

The [posting campaign](../postings/README.md) now separately checks the actual
`prepare_index_destinations`, `rollback_index_destinations` and
`remove_index_sources` algorithms against posting-set and poison contracts.
Destination absence/ownership, primitive insertion/removal, and actual row
publication remain obligations. This event interface has not been fully
instantiated by those data proofs. The generated receipts therefore keep both
`native_primitive_refinement_proved` and `transaction_history_refinement_proved`
**false**. The full plan's P1 concurrent slice and P2 history theorem remain open.

The accepted cached removal-window change is inside that source-removal
primitive boundary. Its [storage contract](../contracts/transactions.md#storage-and-progress)
requires uninterrupted mutation exclusion, attached predecessors, all-lane
detachment or a complete fallback search, and unchanged pinned-reader lifetime.
[Native evidence](../../docs/bench_data/performance_repair_2026-09-23/remove-window/README.md)
covers maximum-height removal, partial postings, pinned retirement, shorter
reuse, fallback failures, and an upper-lane omission mutant. Separate test-only
instrumentation confirms one ordinary removal search instead of two. The
bounded Loom campaign exercises the actual lock, not this pointer algorithm.
Neither those tests nor rechecking the unchanged conditional driver theorem
establish native refinement or approve the candidate's performance.
The [detachment campaign](../skiplist_detach/README.md) now proves the actual
cached/fallback loop's retirement precondition under guarded lane/search/epoch
contracts. Those native pointer and reclamation contracts remain unproved;
engineering acceptance of the implementation is recorded separately in the
performance archive.

The [native predicate campaign](../predicate/README.md) now proves the actual
lock-key calculation, read validation and stamp publication against data-bearing
contracts, and [dependency capture](../predicate_capture/README.md) checks the
actual lookup loop, including empty predicates. This narrows the named primitive
obligations, but does not automatically instantiate this entire event interface
or establish native concurrent history refinement. The remaining work joins
those results to storage, snapshot and interference invariants. A passing
orchestration campaign must not be used to authorize changes to unproved
atomic, allocator, snapshot or WAL primitives.
