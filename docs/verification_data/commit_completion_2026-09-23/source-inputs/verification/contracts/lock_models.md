# Bounded production-lock checks

`scripts/check_lock_models.py` is a mandatory `pilot`/`full` step. It compiles
`aerostore_core/tests/shm_mutation_model.rs`, which imports the actual production
`shm_lock.rs` and substitutes Loom atomics under `--cfg aerostore_loom`. The
runtime lock retains its original CAS and backoff protocol; the added tests
exist only under `cfg(all(test, aerostore_loom))`.

The seven required cases cover:

- Abstract index reachability and allocation ownership during insertion,
  deletion, reads and collection.
- Row-guard ordering between row and index publication.
- Returning failed prepublication allocations before unlocking.
- Expected counterexamples for the unprotected predecessor race and omitted
  row guard.
- Handoff of a protected non-atomic value through the actual lock.
- Priority admission ahead of an ordinary contender after observing actual
  priority registration. The original owner becomes the ordinary contender,
  keeping three roles in a two-thread schedule without injecting protocol state.

Every case uses preemption bound **2** and maximum branches **10,000**, with no
permutation or duration cutoff. Branch exhaustion, timeout, missing cases and
compilation failure are failures, never successful exploration. Earlier
three-thread priority exploration exhausted the branch bound for both the
original lock and a diagnostic variant; that incomplete evidence is retained
with the performance diagnostics, not counted as a passing result.

A mandatory negative control changes only successful acquisition from Acquire
to Relaxed. Its protected-value handoff must fail with Loom's UnsafeCell
causality violation. An arbitrary nonzero exit or unrelated panic is insufficient.
Production and mutant builds use separate fresh source copies and target
directories; the runner requires fresh compiler artifacts and distinct binary
hashes. Commands, flags, inputs, logs, binaries and the mutation are retained.
The composed gate validates their hashes and exact named outcomes against the
current source, rather than accepting only a success boolean. Receipts begin
incomplete, so interrupted runs cannot leave a prior success at that path.

These are finite model checks of the imported lock and an abstract index
protocol. Loom uses a modeled yield in the retry loop rather than the native
spin/yield/sleep timing. The results do not prove all weak-memory executions,
cross-process mmap semantics, the complete skiplist, crash recovery, fairness,
bounded waiting, or a general reclamation-progress theorem. The compiler,
Loom model, frozen runner and trusted workflow remain explicit boundaries.
Neither diagnostic lock optimization was adopted on the strength of these
checks; correctness evidence does not replace workload performance acceptance.

After sourcing the pinned production tool environment, run:

```sh
python3 scripts/check_lock_models.py
```

The receipt and retained artifacts default to `target/verification/lock-models`.
